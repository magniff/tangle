use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use serde_json;
use std::convert::TryInto;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::client::{IdentifyData, IdentifyResponse};

#[derive(Debug)]
enum WriterCommand {
    RespondOk { response: String },
    RespondErr { error: String },
}

#[derive(Debug)]
enum Command {
    Nop,
    WriterCommand(WriterCommand),
    ServerCommand(crate::components::server::Message),
}

// IDENTIFY\n
// [ 4-byte size in bytes ][ N-byte JSON data ]
async fn exec_identify_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite,
{
    let &[super::constants::IDENTIFY] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {error: format!(
            "{invalid}: IDENTIFY command can't have any arguments", invalid=super::constants::E_INVALID
        )})]);
    };

    let payload_expected_size = client.reader.read_u32().await? as usize;
    let mut identify_payload_buffer = BytesMut::with_capacity(payload_expected_size);
    unsafe { identify_payload_buffer.set_len(payload_expected_size) };

    if client
        .reader
        .read_exact(&mut identify_payload_buffer)
        .await?
        == 0
    {
        return Ok(vec![Command::ServerCommand(
            crate::components::server::Message::Disconnect {
                address: client.address,
            },
        )]);
    }

    let identify_data: IdentifyData = serde_json::from_slice(identify_payload_buffer.as_ref())?;
    log::trace!("Identify data from the client: {identify_data:?}");

    // If feature_negotiation flag is raised, the client would expect to get a json back
    if identify_data.feature_negotiation {
        return Ok(vec![
            Command::ServerCommand(crate::components::server::Message::Identify {
                address: client.address,
                data: identify_data,
            }),
            Command::WriterCommand(WriterCommand::RespondOk {
                response: serde_json::to_string(&IdentifyResponse::default()).unwrap(),
            }),
        ]);
    }

    // Ok(CommandExecResult::Response(OK.to_string()))
    Ok(vec![Command::WriterCommand(WriterCommand::RespondOk {
        response: super::constants::OK.to_string(),
    })])
}

// SUB <topic_name> <channel_name>\n
async fn exec_sub_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite,
{
    let &[super::constants::SUB, topic_name, channel_name] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {error: format!(
            "{invalid}: SUB command must have exactly two arguments: topic_name & channel_name",
            invalid=super::constants::E_INVALID
        )})]);
    };

    Ok(vec![
        Command::ServerCommand(crate::components::server::Message::Subscribe {
            address: client.address,
            topic_name: topic_name.to_string(),
            channel_name: channel_name.to_string(),
            back_to_client: client.send_back_channel.clone().unwrap(),
        }),
        Command::WriterCommand(WriterCommand::RespondOk {
            response: super::constants::OK.to_string(),
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
    let &[super::constants::PUB, topic_name] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {error: format!(
            "{invalid}: PUB command should have exactly one argument", invalid=super::constants::E_INVALID,
        )})]);
    };

    let message_body_size = client.reader.read_u32().await? as usize;
    let mut message_body_buffer = BytesMut::with_capacity(message_body_size);
    unsafe { message_body_buffer.set_len(message_body_size) };

    if client.reader.read_exact(&mut message_body_buffer).await? == 0 {
        return Ok(vec![Command::ServerCommand(
            crate::components::server::Message::Disconnect {
                address: client.address,
            },
        )]);
    }

    Ok(vec![
        Command::ServerCommand(crate::components::server::Message::Publish {
            address: client.address,
            topic_name: topic_name.to_string(),
            message: Arc::new(message_body_buffer.freeze()),
        }),
        Command::WriterCommand(WriterCommand::RespondOk {
            response: super::constants::OK.to_string(),
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
    W: AsyncWrite,
{
    let &[super::constants::MPUB, topic_name] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {error: format!(
            "{invalid}: PUB command should have exactly one argument", invalid=super::constants::E_INVALID
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
        unsafe { current_message_buffer.set_len(current_message_size) };

        // Read the body or handle the client's sudden death
        if client
            .reader
            .read_exact(&mut current_message_buffer)
            .await?
            == 0
        {
            return Ok(vec![Command::ServerCommand(
                crate::components::server::Message::Disconnect {
                    address: client.address,
                },
            )]);
        }
        messages.push(Arc::new(current_message_buffer.freeze()));
    }

    let mut commands: Vec<Command> = messages
        .into_iter()
        .map(|message| {
            Command::ServerCommand(crate::components::server::Message::Publish {
                address: client.address,
                topic_name: topic_name.to_string(),
                message,
            })
        })
        .collect();

    commands.push(Command::WriterCommand(WriterCommand::RespondOk {
        response: super::constants::OK.to_string(),
    }));

    Ok(commands)
}

// RDY <count>\n
// <count> - a string representation of integer N where 0 < N <= configured_max
async fn exec_rdy_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite,
{
    let &[super::constants::RDY, count_string] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!(
                "{invalid}: RDY command should have exactly one argument",
                invalid=super::constants::E_INVALID
            ),
        })]);
    };

    let Ok(capacity) = count_string.parse::<usize>()else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!(
                "{invalid}: RDY command should have a numeric count argument",
                invalid=super::constants::E_INVALID
            ),
        })]);
    };

    Ok(vec![Command::ServerCommand(
        crate::components::server::Message::SetCapacity {
            address: client.address,
            capacity,
        },
    )])
}

// // FIN <message_id>\n
// // <message_id> - message id as 16-byte hex string
async fn exec_fin_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite,
{
    let &[super::constants::FIN, message_id] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!(
                "{invalid}: FIN command should have exactly one argument",
                invalid=super::constants::E_INVALID
            ),
        })]);
    };

    match TryInto::<[u8; 16]>::try_into(message_id.as_bytes()) {
        Ok(buffer) => Ok(vec![Command::ServerCommand(
            crate::components::server::Message::Finalize {
                address: client.address,
                message_id: buffer,
            },
        )]),
        Err(reason) => Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!(
                "{bad_message}: message id should be exactly 16 bytes long, error: {reason}",
                bad_message = super::constants::E_BAD_MESSAGE
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
    W: AsyncWrite,
{
    // For now we'll ommit the requeue timeout
    let &[super::constants::REQ, message_id, _] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!(
                "{invalid}: REQ command should have exactly two arguments",
                invalid=super::constants::E_INVALID),
        })]);
    };
    match TryInto::<[u8; 16]>::try_into(message_id.as_bytes()) {
        Ok(buffer) => Ok(vec![Command::ServerCommand(
            crate::components::server::Message::Requeue {
                address: client.address,
                message_id: buffer,
            },
        )]),
        Err(reason) => Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!(
                "{bad_message}: message id should be exactly 16 bytes long, error: {reason}",
                bad_message = super::constants::E_BAD_MESSAGE
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
    W: AsyncWrite,
{
    let &[super::constants::CLS] = parts else {
        return Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
            error: format!(
                "{invalid}: RDY command should have exactly one argument",
                invalid=super::constants::E_INVALID,
            ),
        })]);
    };

    Ok(vec![Command::ServerCommand(
        crate::components::server::Message::Disconnect {
            address: client.address,
        },
    )])
}

async fn exec_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    parts: &[&str],
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    log::trace!(
        "{address} is calling {parts:?}",
        address = client.address.to_string()
    );

    if let &[command, ..] = parts {
        return match command {
            super::constants::NOP => Ok(vec![Command::Nop]),
            super::constants::CLS => exec_cls_command(client, parts).await,
            super::constants::IDENTIFY => exec_identify_command(client, parts).await,
            super::constants::SUB => exec_sub_command(client, parts).await,
            super::constants::PUB => exec_pub_command(client, parts).await,
            super::constants::RDY => exec_rdy_command(client, parts).await,
            super::constants::FIN => exec_fin_command(client, parts).await,
            super::constants::REQ => exec_req_command(client, parts).await,
            super::constants::MPUB => exec_mpub_command(client, parts).await,
            something_else => {
                log::error!(
                    "{address}: Got unknown command: {something_else}",
                    address = client.address
                );
                Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {error: format!(
                    "{invalid}: does this look like a valid protocol command to you: {something_else}?",
                    invalid=super::constants::E_INVALID,
                )})])
            }
        };
    }
    Ok(vec![Command::WriterCommand(WriterCommand::RespondErr {
        error: super::constants::E_INVALID.to_string(),
    })])
}

async fn parse_single_command_and_exec<R, W>(
    client: &mut crate::client::Client<R, W>,
) -> Result<Vec<Command>>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    let mut current_line_buffer =
        String::with_capacity(super::constants::LINE_BUFFER_PREALLOCATE_SIZE);
    if client.reader.read_line(&mut current_line_buffer).await? == 0 {
        return Ok(vec![Command::ServerCommand(
            crate::components::server::Message::Disconnect {
                address: client.address,
            },
        )]);
    }

    if current_line_buffer.ends_with('\n') {
        current_line_buffer.truncate(current_line_buffer.len() - 1);
    }

    exec_command(
        client,
        current_line_buffer
            .split(super::constants::SEPARATOR)
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
    command: crate::components::server::Message,
    to_server_sender: &tokio::sync::mpsc::UnboundedSender<crate::components::server::Message>,
) -> AfterCommand {
    match command {
        // Disconnect commands are handled somewwhere else
        crate::components::server::Message::Disconnect { address, .. } => {
            AfterCommand::Disconnect {
                message_to_log: format!("{address}: will soon be disconnected"),
            }
        }
        other_command => to_server_sender
            .send(other_command)
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
    W: AsyncWrite + Unpin,
{
    let address = client.address.to_string();
    match command {
        WriterCommand::RespondOk { response } => {
            match super::writer::write_response_frame(&mut client.writer, response).await {
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
            match super::writer::write_error_frame(&mut client.writer, error).await {
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
    to_server_sender: tokio::sync::mpsc::UnboundedSender<crate::components::server::Message>,
) where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    let (from_server_sender, mut from_server_receiver) =
        tokio::sync::mpsc::unbounded_channel::<crate::components::client::Message>();
    client.send_back_channel = Some(from_server_sender);

    let address = client.address.to_string();
    'mainloop: loop {
        tokio::select! {
            Some(message_from_server) = from_server_receiver.recv() => {
                match message_from_server {
                    crate::components::client::Message::PushResponse {message} => {
                        if super::writer::write_response_frame(&mut client.writer, message).await.is_err() {
                            break 'mainloop;
                        }
                    }
                    crate::components::client::Message::PushMessage {message} => {
                        if super::writer::write_message_frame(&mut client.writer, message).await.is_err() {
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
                        log::trace!("{address}: {message_to_log}");
                        break 'mainloop
                    }
                };
            }
            else => {
                log::debug!("{address}: both input and output channels are closed");
                break 'mainloop
            }
        };
        if client.writer.flush().await.is_err() {
            log::trace!("{address}: abnormally disconnected");
            break 'mainloop;
        };
    }
    // Additional cleanup on the server side
    if to_server_sender
        .send(crate::components::server::Message::Disconnect {
            address: client.address,
        })
        .is_err()
    {
        log::error!("{address}: The server crashed while client was trying to disconnect");
    };
}

#[cfg(test)]
mod test_writers {
    use rstest;
    use std::io::Cursor;

    use super::{super::writer::write_error_frame, super::writer::write_response_frame};

    #[rstest::rstest]
    #[case("".to_string())]
    #[case("helloworld".to_string())]
    #[case("1 2 3 \\n \\r more stuff приветик\n\n\n".to_string())]
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
    #[case("1 2 3 \\n \\r more stuff приветик\n\n\n".to_string())]
    #[tokio::test]
    async fn test_response_wirter(#[case] message: String) {
        let mut buffer_to_write_in = Cursor::new(Vec::new());
        let write_result = write_response_frame(&mut buffer_to_write_in, message.as_bytes()).await;

        assert!(write_result.is_ok());
        assert!(buffer_to_write_in.get_ref().len() == 2 * (i32::BITS / 8) as usize + message.len());
    }
}
