use std::convert::TryInto;

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use log::{debug, error, info, warn};
use serde_json;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    client::IdentifyData,
    server::{Server2ClientMessage, ServerMessage},
};

pub enum FrameType {
    Response = 0,
    Error = 1,
    Message = 2,
}

// Misc
pub const SEPARATOR: &str = " ";
pub const HEARTBEAT: &str = "_heartbeat_";

// Response codes ok
pub const OK: &str = "OK";
pub const CLOSE_WAIT: &str = "CLOSE_WAIT";

// Response codes faile
pub const E_INVALID: &str = "E_INVALID";
pub const E_BAD_PROTOCOL: &str = "E_BAD_PROTOCOL";
pub const E_BAD_TOPIC: &str = "E_BAD_TOPIC";
pub const E_BAD_CHANNEL: &str = "E_BAD_CHANNEL";
pub const E_BAD_MESSAGE: &str = "E_BAD_MESSAGE";
pub const E_BAD_BODY: &str = "E_BAD_BODY";
pub const E_PUB_FAILED: &str = "E_PUB_FAILED";
pub const E_MPUB_FAILED: &str = "E_MPUB_FAILED";
pub const E_DPUB_FAILED: &str = "E_DPUB_FAILED";
pub const E_FIN_FAILED: &str = "E_FIN_FAILED";
pub const E_REQ_FAILED: &str = "E_REQ_FAILED";
pub const E_TOUCH_FAILED: &str = "E_TOUCH_FAILED";
pub const E_AUTH_FAILED: &str = "E_AUTH_FAILED";
pub const E_ANAUTHORIZED: &str = "E_ANAUTHORIZED";

// Protocol RPC commands as found here https://nsq.io/clients/tcp_protocol_spec.html
pub const IDENTIFY: &str = "IDENTIFY";
pub const SUB: &str = "SUB";
pub const PUB: &str = "PUB";
pub const MPUB: &str = "MPUB";
pub const DPUB: &str = "DPUB";
pub const RDY: &str = "RDY";
pub const FIN: &str = "FIN";
pub const REQ: &str = "REQ";
pub const TOUCH: &str = "TOUCH";
pub const CLS: &str = "CLS";
pub const NOP: &str = "NOP";
pub const AUTH: &str = "AUTH";

pub const FRAME_SIZE_HEADER_SIZE: usize = 4;
pub const FRAME_TYPE_HEADER_SIZE: usize = 4;

const LINE_BUFFER_PREALLOCATE_SIZE: usize = 32;

#[derive(Debug)]
enum WriterCommand {
    RespondOk { response: String },
    RespondErr { error: String },
}

#[derive(Debug)]
enum Command {
    Nop,
    WriterCommand(WriterCommand),
    ServerCommand(crate::server::ServerMessage),
}

// [x][x][x][x][x][x][x][x][x][x][x][x]...
// |  (int32) ||  (int32) || (binary)
// |  4-byte  ||  4-byte  || N-byte
// ------------------------------------...
//     size     frame type     data
async fn compose_and_write_frame<W, P>(
    writer: &mut W,
    frame_type: FrameType,
    payload: P,
) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
    P: AsRef<[u8]>,
{
    writer
        .write_all(&{
            let payload = payload.as_ref();
            let mut buffer_to_push = BytesMut::with_capacity(
                FRAME_SIZE_HEADER_SIZE + FRAME_TYPE_HEADER_SIZE + payload.len(),
            );
            buffer_to_push.put_u32((4 + payload.len()) as u32);
            buffer_to_push.put_u32(frame_type as u32);
            buffer_to_push.put_slice(payload);
            buffer_to_push
        })
        .await?;
    Ok(())
}

pub async fn write_error_frame<W, M>(writer: &mut W, error_binary: M) -> Result<()>
where
    M: AsRef<[u8]>,
    W: AsyncWrite + Unpin + ?Sized,
{
    compose_and_write_frame(writer, FrameType::Error, error_binary).await
}

pub async fn write_response_frame<W, M>(writer: &mut W, response_binary: M) -> Result<()>
where
    M: AsRef<[u8]>,
    W: AsyncWrite + Unpin + ?Sized,
{
    compose_and_write_frame(writer, FrameType::Response, response_binary).await
}

pub async fn write_message_frame<W, M>(writer: &mut W, message_binary: M) -> Result<()>
where
    M: AsRef<[u8]>,
    W: AsyncWrite + Unpin + ?Sized,
{
    compose_and_write_frame(writer, FrameType::Message, message_binary).await
}

// IDENTIFY\n
// [ 4-byte size in bytes ][ N-byte JSON data ]
async fn exec_identify_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Send + Sync + Unpin + 'static,
{
    let &["IDENTIFY"] = parts else {
        warn!("IDENTIFY command is broken");
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {error: format!(
            "{E_INVALID}: IDENTIFY command can't have any arguments"
        )})]);
    };

    let payload_expected_size = client.reader.read_u32().await? as usize;
    let mut identify_payload_buffer = BytesMut::with_capacity(payload_expected_size);
    identify_payload_buffer.resize(payload_expected_size, 0);
    if client
        .reader
        .read_exact(&mut identify_payload_buffer)
        .await?
        == 0
    {
        return Ok(vec![Command::ServerCommand(ServerMessage::Disconnect {
            address: client.address,
        })]);
    }

    let identify_data: IdentifyData = serde_json::from_slice(identify_payload_buffer.as_ref())?;

    // If feature_negotiation flag is raised, the client would expect to get a json back
    if identify_data.feature_negotiation == Some(true) {
        return Ok(vec![
            Command::ServerCommand(ServerMessage::Identify {
                address: client.address,
                data: identify_data,
            }),
            Command::WriterCommand(WriterCommand::RespondOk {
                response: serde_json::to_string(&IdentifyData::default()).unwrap(),
            }),
        ]);
    }

    // Ok(CommandExecResult::Response(OK.to_string()))
    Ok(vec![Command::WriterCommand(WriterCommand::RespondOk {
        response: OK.to_string(),
    })])
}

// SUB <topic_name> <channel_name>\n
async fn exec_sub_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Send + Sync + Unpin + 'static,
{
    let &["SUB", topic_name, channel_name] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {error: format!(
            "{E_INVALID}: SUB command must have exactly two arguments: topic_name & channel_name"
        )})]);
    };

    Ok(vec![
        Command::ServerCommand(ServerMessage::Subscribe {
            address: client.address,
            topic_name: topic_name.to_string(),
            channel_name: channel_name.to_string(),
            send_back: client.send_back_channel.clone().unwrap(),
        }),
        Command::WriterCommand(WriterCommand::RespondOk {
            response: OK.to_string(),
        }),
    ])
}

// PUB <topic_name>\n
// [ 4-byte size in bytes ][ N-byte binary data ]
async fn exec_pub_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite,
{
    let &["PUB", topic_name] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {error: format!(
            "{E_INVALID}: PUB command should have exactly one argument"
        )})]);
    };

    let message_body_size = client.reader.read_u32().await? as usize;
    let mut message_body_buffer = BytesMut::with_capacity(message_body_size);
    message_body_buffer.resize(message_body_size, 0);
    if client.reader.read_exact(&mut message_body_buffer).await? == 0 {
        return Ok(vec![Command::ServerCommand(ServerMessage::Disconnect {
            address: client.address,
        })]);
    }

    Ok(vec![
        Command::ServerCommand(ServerMessage::Publish {
            address: client.address,
            topic_name: topic_name.to_string(),
            message: crate::message::ProtocolMessage::from_body(message_body_buffer),
        }),
        Command::WriterCommand(WriterCommand::RespondOk {
            response: OK.to_string(),
        }),
    ])
}

// // MPUB <topic_name>\n
// // [ 4-byte body size ]
// // [ 4-byte num messages ]
// // [ 4-byte message #1 size ][ N-byte binary data ]
// //       ... (repeated <num_messages> times)
async fn exec_mpub_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Send + Sync + Unpin + 'static,
{
    let &["MPUB", topic_name] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {error: format!(
            "{E_INVALID}: PUB command should have exactly one argument"
        )})]);
    };
    // No actual use for the overall payload size
    let _ = client.reader.read_u32().await?;
    // How many messages are sent
    let batch_size = client.reader.read_u32().await?;

    // It is important to be sure the messages are all fine before sending it to the queue
    let mut messages = Vec::with_capacity(batch_size as usize);
    for _ in 0..batch_size {
        let current_message_size = client.reader.read_u32().await? as usize;
        let mut current_message_buffer = BytesMut::with_capacity(current_message_size);
        current_message_buffer.resize(current_message_size, 0);
        // Read the body or handle the client's sudden death
        if client
            .reader
            .read_exact(&mut current_message_buffer)
            .await?
            == 0
        {
            return Ok(vec![Command::ServerCommand(ServerMessage::Disconnect {
                address: client.address,
            })]);
        }
        messages.push(crate::message::ProtocolMessage::from_body(
            current_message_buffer,
        ));
    }

    let mut commands: Vec<Command> = messages
        .into_iter()
        .map(|message| {
            Command::ServerCommand(ServerMessage::Publish {
                address: client.address,
                topic_name: topic_name.to_string(),
                message,
            })
        })
        .collect();

    commands.push(Command::WriterCommand(WriterCommand::RespondOk {
        response: OK.to_string(),
    }));

    Ok(commands)
}

// RDY <count>\n
// <count> - a string representation of integer N where 0 < N <= configured_max
async fn exec_rdy_command<R, W>(
    _: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + 'static,
{
    let &["RDY", _] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!("{E_INVALID}: RDY command should have exactly one argument"),
        })]);
    };

    Ok(vec![Command::Nop])
}

// // FIN <message_id>\n
// // <message_id> - message id as 16-byte hex string
async fn exec_fin_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + 'static,
{
    let &["FIN", message_id] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!("{E_INVALID}: FIN command should have exactly one argument"),
        })]);
    };

    match TryInto::<[u8; 16]>::try_into(message_id.as_bytes()) {
        Ok(buffer) => Ok(vec![Command::ServerCommand(ServerMessage::Finalize {
            address: client.address,
            message_id: buffer,
        })]),
        Err(reason) => Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!(
                "{E_BAD_MESSAGE}: message id should be exactly 16 bytes long, error: {reason}"
            ),
        })]),
    }
}

// REQ <message_id> <timeout>\n
// <message_id> - message id as 16-byte hex string
// <timeout> - a string representation of integer N where N <= configured max timeout
//     timeout == 0 - requeue a message immediately
//     timeout  > 0 - defer requeue for timeout milliseconds
async fn exec_req_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + 'static,
{
    // For now we'll ommit the requeue timeout
    let &["REQ", message_id, _] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!("{E_INVALID}: REQ command should have exactly two arguments"),
        })]);
    };
    match TryInto::<[u8; 16]>::try_into(message_id.as_bytes()) {
        Ok(buffer) => Ok(vec![Command::ServerCommand(ServerMessage::Requeue {
            address: client.address,
            message_id: buffer,
        })]),
        Err(reason) => Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!(
                "{E_BAD_MESSAGE}: message id should be exactly 16 bytes long, error: {reason}"
            ),
        })]),
    }
}

// CLS\n
async fn exec_cls_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + 'static,
{
    let &["CLS"] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!("{E_INVALID}: RDY command should have exactly one argument"),
        })]);
    };

    Ok(vec![Command::ServerCommand(ServerMessage::Disconnect {
        address: client.address,
    })])
}

async fn exec_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    info!(
        "{address} is calling {parts:?}",
        address = client.address.to_string()
    );

    if let Some(command) = parts.first() {
        return match *command {
            NOP => Ok(vec![Command::Nop]),
            CLS => exec_cls_command(client, parts).await,
            IDENTIFY => exec_identify_command(client, parts).await,
            SUB => exec_sub_command(client, parts).await,
            PUB => exec_pub_command(client, parts).await,
            RDY => exec_rdy_command(client, parts).await,
            FIN => exec_fin_command(client, parts).await,
            REQ => exec_req_command(client, parts).await,
            MPUB => exec_mpub_command(client, parts).await,
            something_else => {
                error!("Got unknown command: {something_else}");
                Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {error: format!(
                    "{E_INVALID}: does this look like a valid protocol command to you: {something_else}?",
                )})])
            }
        };
    }
    Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
        error: E_INVALID.to_string(),
    })])
}

async fn parse_single_command_and_exec<R, W>(
    client: &mut crate::client::Client<R, W>,
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    let mut current_line_buffer = String::with_capacity(LINE_BUFFER_PREALLOCATE_SIZE);
    if client.reader.read_line(&mut current_line_buffer).await? == 0 {
        return Ok(vec![Command::ServerCommand(ServerMessage::Disconnect {
            address: client.address,
        })]);
    }

    if current_line_buffer.ends_with('\n') {
        current_line_buffer.truncate(current_line_buffer.len() - 1);
    }

    exec_command(
        client,
        current_line_buffer
            .split(SEPARATOR)
            .collect::<Vec<&str>>()
            .as_slice(),
    )
    .await
}

enum AfterCommand {
    Disconnect { message_to_log: String },
    Proceed,
}

async fn handle_server_command(
    command: crate::server::ServerMessage,
    to_server_sender: &tokio::sync::mpsc::Sender<crate::server::ServerMessage>,
) -> AfterCommand {
    match command {
        // Disconnect commands are handled somewwhere else
        ServerMessage::Disconnect { address, .. } => AfterCommand::Disconnect {
            message_to_log: format!("{address}: will soon be disconnected"),
        },
        other_command => to_server_sender
            .send(other_command)
            .await
            .map(|_| AfterCommand::Proceed)
            .unwrap_or_else(|_| AfterCommand::Disconnect {
                message_to_log: "Server crashed".to_string(),
            }),
    }
}

async fn handle_writer_command<R, W>(
    command: WriterCommand,
    client: &mut crate::client::Client<R, W>,
) -> AfterCommand
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    let address = client.address.to_string();
    match command {
        WriterCommand::RespondOk { response } => {
            match write_response_frame(&mut client.writer, response).await {
                Ok(()) => AfterCommand::Proceed,
                Err(e) =>
                    AfterCommand::Disconnect {
                        message_to_log:
                            format!(
                                "{address}: error occurred while pushing response bytes to the client, reason: {e}",
                            )
                    }

            }
        }
        WriterCommand::RespondErr { error } => {
            match write_error_frame(&mut client.writer, error).await {
                Ok(()) =>
                    AfterCommand::Disconnect {
                        message_to_log:
                            format!("{address}: error occurred in protocol logic, disconnecting...")
                    },
                Err(e) =>
                    AfterCommand::Disconnect {
                        message_to_log:
                            format!(
                                "{address}: error occurred while pushing response bytes to the client, reason: {e}"
                            )
                    }
            }
        }
    }
}

pub async fn run_socket_mainloop<R, W>(
    mut client: crate::client::Client<R, W>,
    to_server_sender: tokio::sync::mpsc::Sender<crate::server::ServerMessage>,
) where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    let (from_server_sender, mut from_server_receiver) =
        tokio::sync::mpsc::channel::<Server2ClientMessage>(128);
    client.send_back_channel = Some(from_server_sender);

    let address = client.address.to_string();
    'mainloop: loop {
        tokio::select! {
            Some(message_from_server) = from_server_receiver.recv() => {
                info!("{address}: got a new internal message");
                match message_from_server {
                    Server2ClientMessage::SendToClient {message} => {
                        info!("{address}: got a new message for a the client");
                        if let Err(reason) = write_message_frame(&mut client.writer, message.as_bytes()).await {
                            error!(
                                "{address}: looks like the client got disconnected before the message was pushed, reason was: {reason}"
                            );
                            break 'mainloop;
                        }
                    }
                }
            }
            Ok(messages_from_client) = parse_single_command_and_exec(&mut client) => {
                for command in messages_from_client {
                    let whats_next = match command {
                        Command::Nop => AfterCommand::Proceed,
                        Command::ServerCommand(serve_command) =>
                            handle_server_command(serve_command, &to_server_sender).await,
                        Command::WriterCommand(writer_command) =>
                            handle_writer_command(writer_command, &mut client).await,
                    };
                    if let AfterCommand::Disconnect {message_to_log} = whats_next {
                        warn!("{address}: {message_to_log}");
                        break 'mainloop
                    }
                };
            }
            else => {
                debug!("{address}: both input and output channels are closed");
                break 'mainloop
            }
        };
        if client.writer.flush().await.is_err() {
            warn!("{address}: abnormally disconnected");
            break 'mainloop;
        };
    }
    // Additional cleanup on the server side
    if to_server_sender
        .send(ServerMessage::Disconnect {
            address: client.address,
        })
        .await
        .is_err()
    {
        error!("{address}: The server crashed while client was trying to disconnect");
    };
}

#[cfg(test)]
mod test_writers {
    use rstest;
    use std::io::Cursor;

    use super::{write_error_frame, write_response_frame};

    #[rstest::rstest]
    #[case("".to_string())]
    #[case("helloworld".to_string())]
    #[case("1 2 3 \\n \\r more stuff ????????????????\n\n\n".to_string())]
    #[tokio::test]
    async fn test_error_writer(#[case] message: String) {
        let mut buffer_to_write_in = Cursor::new(Vec::new());
        let write_result = write_error_frame(&mut buffer_to_write_in, message.as_bytes()).await;

        assert!(write_result.is_ok());
        assert!(buffer_to_write_in.get_ref().len() == 2 * (i32::BITS / 8) as usize + message.len());
    }

    #[rstest::rstest]
    #[case("".to_string())]
    #[case("helloworld".to_string())]
    #[case("1 2 3 \\n \\r more stuff ????????????????\n\n\n".to_string())]
    #[tokio::test]
    async fn test_response_wirter(#[case] message: String) {
        let mut buffer_to_write_in = Cursor::new(Vec::new());
        let write_result = write_response_frame(&mut buffer_to_write_in, message.as_bytes()).await;

        assert!(write_result.is_ok());
        assert!(buffer_to_write_in.get_ref().len() == 2 * (i32::BITS / 8) as usize + message.len());
    }
}
