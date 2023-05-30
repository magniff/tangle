use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use bytes::BytesMut;
use serde_json;
use std::convert::TryInto;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::{
    client::{IdentifyData, IdentifyResponse},
    protocol::constants::E_INVALID,
};

async fn get_topic_queue(
    mainloop_state: &mut MainloopState,
    topic_name: String,
    client_address: std::net::SocketAddr,
) -> Result<UnboundedSender<crate::components::topic::TopicMessage>> {
    log::trace!("{client_address}: get_topic_queue");
    Ok(match mainloop_state.topics.get(&topic_name) {
        Some(topic_queue) => topic_queue.clone(),
        None => {
            let (resposne_sender, response_receiver) = oneshot::channel();
            mainloop_state.server_sender.send(
                crate::components::server::Message::GetTopicInlet {
                    topic_name: topic_name.to_string(),
                    respond_to: crate::components::server::RespondTo {
                        address: client_address,
                        queue: resposne_sender,
                    },
                },
            )?;
            let topic_queue = response_receiver.await?;
            mainloop_state
                .topics
                .insert(topic_name, topic_queue.clone());
            topic_queue
        }
    })
}

// IDENTIFY\n
// [ 4-byte size in bytes ][ N-byte JSON data ]
async fn exec_identify_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    mainloop_state: &mut MainloopState,
    parts: &[&str],
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    log::trace!("{address}: IDENTIFY", address = client.address);
    let &[super::constants::IDENTIFY] = parts else {
        bail!(
            "{invalid}: IDENTIFY command can't have any arguments",
            invalid=super::constants::E_INVALID
        )
    };

    let payload_expected_size = client.socker_reader.read_u32().await? as usize;
    let mut identify_payload_buffer = BytesMut::with_capacity(payload_expected_size);
    unsafe { identify_payload_buffer.set_len(payload_expected_size) };

    if client
        .socker_reader
        .read_exact(&mut identify_payload_buffer)
        .await?
        != identify_payload_buffer.len()
    {
        bail!(super::constants::E_BAD_BODY)
    }

    let identify_data: IdentifyData = serde_json::from_slice(identify_payload_buffer.as_ref())?;
    log::trace!("Identify data from the client: {identify_data:?}");

    // If feature_negotiation flag is raised, the client would expect to get a json back
    if identify_data.feature_negotiation {
        mainloop_state
            .server_sender
            .send(crate::components::server::Message::Identify {
                address: client.address,
                data: identify_data,
            })?;

        return super::writer::write_response_frame(
            &mut client.socker_writer,
            serde_json::to_string(&IdentifyResponse::default()).unwrap(),
        )
        .await;
    }

    super::writer::write_response_frame(&mut client.socker_writer, super::constants::OK).await
}

// SUB <topic_name> <channel_name>\n
async fn exec_sub_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    mainloop_state: &mut MainloopState,
    parts: &[&str],
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    log::trace!("{address}: SUB", address = client.address);
    let &[super::constants::SUB, topic_name, channel_name] = parts else {
        bail!(
            "{invalid}: SUB command must have exactly two arguments: topic_name & channel_name",
            invalid=super::constants::E_INVALID
        )
    };

    let topic_queue =
        get_topic_queue(mainloop_state, topic_name.to_string(), client.address).await?;

    topic_queue.send(crate::components::topic::TopicMessage::Subscribe {
        address: client.address,
        channel_name: channel_name.to_string(),
        back_to_client: mainloop_state.client_sender.clone(),
    })?;

    super::writer::write_response_frame(&mut client.socker_writer, super::constants::OK).await
}

// PUB <topic_name>\n
// [ 4-byte size in bytes ][ N-byte binary data ]
async fn exec_pub_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    mainloop_state: &mut MainloopState,
    parts: &[&str],
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    log::trace!("{address}: PUB", address = client.address);
    let &[super::constants::PUB, topic_name] = parts else {
        bail!(
            "{invalid}: PUB command should have exactly one argument",
            invalid=super::constants::E_INVALID
        )
    };

    let topic_queue =
        get_topic_queue(mainloop_state, topic_name.to_string(), client.address).await?;

    let message_body_size = client.socker_reader.read_u32().await? as usize;
    let mut message_body_buffer = BytesMut::with_capacity(message_body_size);
    unsafe { message_body_buffer.set_len(message_body_size) };

    log::trace!(
        "{address}: reading the PUB body from the socket",
        address = client.address
    );

    if client
        .socker_reader
        .read_exact(&mut message_body_buffer)
        .await?
        != message_body_size
    {
        bail!(super::constants::E_BAD_BODY)
    }

    topic_queue.send(crate::components::topic::TopicMessage::Publish {
        message: Arc::new(message_body_buffer.freeze()),
    })?;

    super::writer::write_response_frame(&mut client.socker_writer, super::constants::OK).await
}

// MPUB <topic_name>\n
// [ 4-byte body size ]
// [ 4-byte num messages ]
// [ 4-byte message #1 size ][ N-byte binary data ]
//       ... (repeated <num_messages> times)
async fn exec_mpub_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    mainloop_state: &mut MainloopState,
    parts: &[&str],
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    log::trace!("{address}: MPUB", address = client.address);
    let &[super::constants::MPUB, topic_name] = parts else {
        bail!(
            "{invalid}: PUB command should have exactly one argument",
            invalid=super::constants::E_INVALID
        )
    };
    log::trace!("{address}: MPUB, {topic_name}", address = client.address);

    // There's no actual use for the overall payload size
    let _ = client.socker_reader.read_u32().await?;
    // How many messages are sent
    let batch_size = client.socker_reader.read_u32().await?;

    // It is important to be sure the messages are all fine before sending it to the queue
    let mut messages = Vec::with_capacity(batch_size as usize);
    for _ in 0..batch_size {
        let current_message_size = client.socker_reader.read_u32().await? as usize;
        let mut current_message_buffer = BytesMut::with_capacity(current_message_size);
        unsafe { current_message_buffer.set_len(current_message_size) };

        // Read the body or handle the client's sudden death
        if client
            .socker_reader
            .read_exact(&mut current_message_buffer)
            .await?
            != current_message_size
        {
            bail!(super::constants::E_BAD_BODY)
        }
        messages.push(Arc::new(current_message_buffer.freeze()));
    }

    let topic_queue =
        get_topic_queue(mainloop_state, topic_name.to_string(), client.address).await?;

    for message in messages {
        if topic_queue
            .send(crate::components::topic::TopicMessage::Publish { message })
            .is_err()
        {
            bail!("Topic {topic_name} has died unexpectedly");
        }
    }

    super::writer::write_response_frame(&mut client.socker_writer, super::constants::OK).await
}

// RDY <count>\n
// <count> - a string representation of integer N where 0 < N <= configured_max
async fn exec_rdy_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    mainloop_state: &mut MainloopState,
    parts: &[&str],
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    log::trace!("{address}: RDY", address = client.address);
    let &[super::constants::RDY, capacity] = parts else {
        bail!(
            "{invalid}: RDY command should have exactly one argument",
            invalid=super::constants::E_INVALID
        )
    };

    let Ok(capacity) = capacity.parse::<usize>() else {
        bail!(
            "{invalid}: RDY command should have a numeric count argument",
            invalid=super::constants::E_INVALID
        )
    };

    for topic_queue in mainloop_state.topics.values() {
        topic_queue.send(crate::components::topic::TopicMessage::SetCapacity {
            address: client.address,
            capacity,
        })?;
    }

    Ok(())
}

// // FIN <message_id>\n
// // <message_id> - message id as 16-byte hex string
async fn exec_fin_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    mainloop_state: &mut MainloopState,
    parts: &[&str],
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    log::trace!("{address}: FIN", address = client.address);
    let &[super::constants::FIN, message_id] = parts else {
        bail!(
            "{invalid}: FIN command should have exactly one argument",
            invalid=super::constants::E_INVALID
        )
    };

    let Ok(message_id) = TryInto::<[u8; 16]>::try_into(message_id.as_bytes()) else {
        bail!("Could not construct message id from the bin string {message_id}")
    };

    let channel_to_notify = mainloop_state
        .notifiers
        .remove(&message_id)
        .ok_or_else(|| anyhow!("Message with id <{message_id:?}> is not bound to any channel"))?;

    channel_to_notify
        .send(crate::components::topic::ChannelNotification::MessageAcked { message_id })?;

    Ok(())
}

// REQ <message_id> <timeout>\n
// <message_id> - message id as 16-byte hex string
// <timeout> - a string representation of integer N where N <= configured max timeout
//     timeout == 0 - requeue a message immediately
//     timeout  > 0 - defer requeue for timeout milliseconds
async fn exec_req_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    mainloop_state: &mut MainloopState,
    parts: &[&str],
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    log::trace!("{address}: REQ", address = client.address);
    // For now we'll ommit the requeue timeout
    let &[super::constants::REQ, message_id, _] = parts else {
        bail!(
            "{invalid}: REQ command should have exactly two arguments",
            invalid=super::constants::E_INVALID
        );
    };

    let Ok(message_id) = TryInto::<[u8; 16]>::try_into(message_id.as_bytes()) else {
        bail!("Could not construct message id from the bin string {message_id}")
    };

    let channel_to_notify = mainloop_state
        .notifiers
        .get(&message_id)
        .ok_or_else(|| anyhow!("Message with id <{message_id:?}> is not bound to any channel"))?;

    channel_to_notify
        .send(crate::components::topic::ChannelNotification::MessageRequeued { message_id })?;

    Ok(())
}

// CLS\n
async fn exec_cls_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    mainloop_state: &mut MainloopState,
    parts: &[&str],
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite,
{
    log::trace!("{address}: CLS", address = client.address);
    let &[super::constants::CLS] = parts else {
        bail!(
            "{invalid}: CLS command should have exactly zero argument",
            invalid=super::constants::E_INVALID,
        )
    };

    for (topic_name, topic_channel) in mainloop_state.topics.iter() {
        if topic_channel
            .send(crate::components::topic::TopicMessage::Disconnect {
                address: client.address,
            })
            .is_err()
        {
            log::warn!(
                "Failed to disconnect {address} from the topic {topic_name}",
                address = client.address
            )
        }
    }

    bail!(
        "Client {address} decided to hop off, fine",
        address = client.address
    );
}

async fn exec_command<R, W>(
    client: &mut crate::client::Client<R, W>,
    mainloop_state: &mut MainloopState,
    parts: &[&str],
) -> Result<()>
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
            super::constants::NOP => Ok(()),
            super::constants::CLS => exec_cls_command(client, mainloop_state, parts).await,
            super::constants::IDENTIFY => {
                exec_identify_command(client, mainloop_state, parts).await
            }
            super::constants::SUB => exec_sub_command(client, mainloop_state, parts).await,
            super::constants::PUB => exec_pub_command(client, mainloop_state, parts).await,
            super::constants::RDY => exec_rdy_command(client, mainloop_state, parts).await,
            super::constants::FIN => exec_fin_command(client, mainloop_state, parts).await,
            super::constants::REQ => exec_req_command(client, mainloop_state, parts).await,
            super::constants::MPUB => exec_mpub_command(client, mainloop_state, parts).await,
            something_else => {
                log::error!(
                    "{address}: Got unknown command: {something_else}",
                    address = client.address
                );
                Err(
                    anyhow!(
                        "{invalid}: does this look like a valid protocol command to you: {something_else}?",
                        invalid=super::constants::E_INVALID,
                    )
                )
            }
        };
    }
    Err(anyhow!(E_INVALID))
}

async fn parse_single_command_and_exec<R, W>(
    client: &mut crate::client::Client<R, W>,
    mainloop_state: &mut MainloopState,
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    let mut current_line_buffer =
        String::with_capacity(super::constants::LINE_BUFFER_PREALLOCATE_SIZE);

    if client
        .socker_reader
        .read_line(&mut current_line_buffer)
        .await?
        == 0
    {
        bail!(super::constants::E_INVALID)
    }

    if current_line_buffer.ends_with('\n') {
        current_line_buffer.truncate(current_line_buffer.len() - 1);
    }

    exec_command(
        client,
        mainloop_state,
        current_line_buffer
            .split(super::constants::SEPARATOR)
            .collect::<Vec<&str>>()
            .as_slice(),
    )
    .await
}

struct MainloopState {
    topics: HashMap<String, UnboundedSender<crate::components::topic::TopicMessage>>,
    notifiers: HashMap<[u8; 16], UnboundedSender<crate::components::topic::ChannelNotification>>,
    server_sender: UnboundedSender<crate::components::server::Message>,
    client_sender: UnboundedSender<crate::client::Message>,
}

pub async fn run_socket_mainloop<R, W>(
    mut client: crate::client::Client<R, W>,
    server_sender: UnboundedSender<crate::components::server::Message>,
) where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    let (client_sender, mut client_receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut mainloop_state = MainloopState {
        topics: HashMap::new(),
        notifiers: HashMap::new(),
        server_sender,
        client_sender,
    };

    let address = client.address;

    'mainloop: loop {
        tokio::select! {
            Some(message) = client_receiver.recv() => {
                match message {
                    crate::client::Message::Send { payload, notify_back } => {
                        mainloop_state.notifiers.insert(payload.id, notify_back);
                        if super::writer::write_message_frame(&mut client.socker_writer, payload).await.is_err() {
                            break 'mainloop;
                        }
                    }
                }
            }
            protocol_raw_result = parse_single_command_and_exec(&mut client, &mut mainloop_state) => {
                if protocol_raw_result.is_err() {
                    log::error!("{address}: {protocol_raw_result:?}");
                    for topic_queue in mainloop_state.topics.values() {
                        if topic_queue
                            .send(
                                crate::components::topic::TopicMessage::Disconnect { address: client.address }
                            )
                            .is_err() {
                            log::warn!("Client {address} died unexpectedly");
                        }
                    }
                    break 'mainloop
                }
            }
        };
        if client.socker_writer.flush().await.is_err() {
            break 'mainloop;
        };
    }
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
