use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, Sender, UnboundedReceiver, UnboundedSender};

use anyhow::Result;
use futures::future::try_join_all;
use log::{error, info, warn};
use rand::seq::IteratorRandom;
use rand::{self, SeedableRng};
use tokio;
use tokio::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::mpsc::Receiver;

use crate::client::Client;
use crate::message::ProtocolMessage;
use crate::protocol::{run_socket_mainloop, write_error_frame, E_BAD_PROTOCOL};

#[derive(Debug)]
pub enum Server2ClientMessage {
    SendToClient {
        message: crate::message::ProtocolMessage,
    },
}

// Client2serverMessage is suppose to be a message origin
#[derive(Debug)]
pub enum ServerMessage {
    Disconnect {
        address: std::net::SocketAddr,
    },
    Identify {
        address: std::net::SocketAddr,
        data: crate::client::IdentifyData,
    },
    Publish {
        address: std::net::SocketAddr,
        topic_name: String,
        message: crate::message::ProtocolMessage,
    },
    Subscribe {
        address: std::net::SocketAddr,
        topic_name: String,
        channel_name: String,
        send_back: Sender<Server2ClientMessage>,
    },
    Finalize {
        address: std::net::SocketAddr,
        message_id: [u8; 16],
    },
    Requeue {
        address: std::net::SocketAddr,
        message_id: [u8; 16],
    },
}

async fn run_network_listener(
    listener: TcpListener,
    client2server_channel: tokio::sync::mpsc::Sender<ServerMessage>,
) {
    info!("Spinning up a new network listener task...");
    while let Ok((tcp_stream, address)) = listener.accept().await {
        let (stream_reader, stream_writer) = tcp_stream.into_split();

        let mut reader = BufReader::new(stream_reader);
        let mut writer = BufWriter::new(stream_writer);

        // Check the protocol version first, NSQ itself has support for the V2 only
        let mut magic_buffer = [0; 4];
        if let Err(message) = reader.read_exact(&mut magic_buffer).await {
            error!("{address}: Unable to read the magic bytes from the socket, reason: {message}");
            continue;
        }

        if magic_buffer.as_slice() != b"  V2".as_slice() {
            error!("{address}: client is using the wrong magic number -- {magic_buffer:?}");
            if let Err(message) = write_error_frame(&mut writer, E_BAD_PROTOCOL).await {
                error!(
                    "{address}: unable to send the error message to the client, reason: {message}"
                );
            }
            continue;
        }

        tokio::spawn(run_socket_mainloop(
            Client::from_reader_writer(address, reader, writer),
            client2server_channel.clone(),
        ));

        info!("Client {address} is now connected");
    }
}

#[derive(Debug)]
enum TopicMessage {
    Disconnect {
        address: std::net::SocketAddr,
    },
    Subscribe {
        address: std::net::SocketAddr,
        channel_name: String,
        send_back: Sender<Server2ClientMessage>,
    },
    Publish {
        address: std::net::SocketAddr,
        message: crate::message::ProtocolMessage,
    },
    Finalize {
        address: std::net::SocketAddr,
        message_id: [u8; 16],
    },
    Requeue {
        address: std::net::SocketAddr,
        message_id: [u8; 16],
    },
}

#[derive(Debug)]
enum ChannelMessage {
    Disconnect {
        address: std::net::SocketAddr,
    },
    Publish {
        address: std::net::SocketAddr,
        message: crate::message::ProtocolMessage,
    },
    Subscribe {
        address: std::net::SocketAddr,
        send_back: Sender<Server2ClientMessage>,
    },
    Finalize {
        address: std::net::SocketAddr,
        message_id: [u8; 16],
    },
    Requeue {
        address: std::net::SocketAddr,
        message_id: [u8; 16],
    },
}

#[derive(Debug)]
struct ClientDescriptor {
    sender: Sender<Server2ClientMessage>,
    capacity: u8,
}

fn check_clients_capacity(
    descriptor_map: &HashMap<std::net::SocketAddr, ClientDescriptor>,
) -> bool {
    for (_, descriptor) in descriptor_map.iter() {
        if descriptor.capacity > 0 {
            return true;
        }
    }
    false
}

async fn run_channel(
    name: String,
    mut inlet: UnboundedReceiver<ChannelMessage>,
    config: Arc<crate::settings::TangleArguments>,
) {
    info!("Spinning up a new channel {name}");
    let mut rng = rand::rngs::StdRng::from_entropy();
    let mut clients: HashMap<std::net::SocketAddr, ClientDescriptor> = HashMap::new();
    let mut inflight_messages: HashMap<[u8; 16], crate::message::ProtocolMessage> = HashMap::new();

    let (internal_buf_send, mut internal_buffer) =
        tokio::sync::mpsc::unbounded_channel::<(std::net::SocketAddr, ProtocolMessage)>();

    loop {
        tokio::select! {
            Some((_, message)) = internal_buffer.recv(), if check_clients_capacity(&clients) => {
                // According to the precondition above, there must be at least one ready client
                let (address, descriptor)  =
                    clients
                        .iter_mut()
                        .filter(|(_, desciptor)| {desciptor.capacity > 0})
                        .choose(&mut rng)
                        .unwrap();
                let send_result =
                    descriptor
                        .sender
                        .send(
                            Server2ClientMessage::SendToClient { message: message.clone() }
                        )
                        .await;

                descriptor.capacity -= 1;
                inflight_messages.insert(message.id, message);

                if send_result.is_err() {
                    warn!("Client {address} seems to be disconnected", address=address.to_string())
                };
            }
            Some(message_from_topic) = inlet.recv() => {
                match message_from_topic {
                    ChannelMessage::Disconnect {address} => {
                        clients.remove(&address);
                    }
                    ChannelMessage::Publish {
                        message: message_to_publish,
                        address,
                    } => {
                        internal_buf_send
                            .send((address, message_to_publish))
                            .unwrap();
                    }
                    ChannelMessage::Subscribe { send_back, address, .. } => {
                        clients
                            .entry(address)
                            .or_insert(
                                ClientDescriptor {sender: send_back, capacity: config.max_in_flight}
                            );
                    }
                    ChannelMessage::Finalize { address, message_id } => {
                        if inflight_messages.remove(&message_id).is_some() {
                            // NOTE: Client could have disconnected at this point, match for safety
                            if let Some(descriptor) = clients.get_mut(&address) {
                                descriptor.capacity += 1;
                            }
                        }
                    }
                    ChannelMessage::Requeue { address, message_id } => {
                        if let Some(message) = inflight_messages.get_mut(&message_id) {
                            message.attempts += 1;
                            internal_buf_send.send((address, message.clone())).unwrap();
                            // NOTE: Client could have disconnected at this point, match for safety
                            if let Some(descriptor) = clients.get_mut(&address) {
                                descriptor.capacity += 1;
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn run_topic(
    name: String,
    mut inlet: UnboundedReceiver<TopicMessage>,
    config: Arc<crate::settings::TangleArguments>,
) {
    info!("Spinning up a new topic {name}");
    let mut channels: HashMap<String, UnboundedSender<ChannelMessage>> = HashMap::new();
    let (internal_buf_send, mut internal_buffer) =
        tokio::sync::mpsc::unbounded_channel::<(std::net::SocketAddr, ProtocolMessage)>();

    loop {
        tokio::select! {
            Some((address, message)) = internal_buffer.recv(), if !channels.is_empty() => {
                for (_, inlet) in channels.iter() {
                    inlet
                        .send(ChannelMessage::Publish {
                            address,
                            message: message.clone(),
                        })
                        .unwrap()
                }
            }
            Some(message) = inlet.recv() => {
                match message {
                    TopicMessage::Disconnect {address} => {
                        for (_, inlet) in channels.iter() {
                            inlet
                                .send(ChannelMessage::Disconnect { address })
                                .unwrap()
                        }
                    }
                    TopicMessage::Subscribe {
                        address,
                        channel_name,
                        send_back,
                        ..
                    } => {
                        let channel_inlet = channels.entry(channel_name.clone()).or_insert_with(|| {
                            let (sender, receiver) = unbounded_channel::<ChannelMessage>();
                            tokio::spawn(run_channel(channel_name.clone(), receiver, config.clone()));
                            sender
                        });
                        channel_inlet
                            .send(ChannelMessage::Subscribe {
                                address,
                                send_back,
                            })
                            .unwrap();
                    }
                    TopicMessage::Finalize {address, message_id} => {
                        for (_, inlet) in channels.iter() {
                            inlet
                                .send(ChannelMessage::Finalize { address, message_id })
                                .unwrap()
                        }
                    }
                    TopicMessage::Requeue {address, message_id} => {
                        for (_, inlet) in channels.iter() {
                            inlet
                                .send(ChannelMessage::Requeue { address, message_id })
                                .unwrap()
                        }
                    }
                    TopicMessage::Publish { address, message } => {
                        internal_buf_send.send((address, message)).unwrap();
                    }
                }
            }
            else => break
        }
    }
}

async fn run_server(
    config: Arc<crate::settings::TangleArguments>,
    mut inlet: Receiver<ServerMessage>,
) {
    let mut topics: HashMap<String, UnboundedSender<TopicMessage>> = HashMap::new();

    while let Some(to_server_message) = inlet.recv().await {
        match to_server_message {
            ServerMessage::Finalize {
                address,
                message_id,
            } => {
                for (_, topic_sender) in topics.iter() {
                    topic_sender
                        .send(TopicMessage::Finalize {
                            address,
                            message_id,
                        })
                        .unwrap()
                }
            }
            ServerMessage::Requeue {
                address,
                message_id,
            } => {
                for (_, topic_sender) in topics.iter() {
                    topic_sender
                        .send(TopicMessage::Requeue {
                            address,
                            message_id,
                        })
                        .unwrap()
                }
            }
            ServerMessage::Publish {
                address,
                topic_name,
                message,
            } => {
                let topic_inlet_channel = topics.entry(topic_name.clone()).or_insert_with(|| {
                    let (sender, receiver) = unbounded_channel::<TopicMessage>();
                    tokio::spawn(run_topic(topic_name, receiver, config.clone()));
                    sender
                });
                topic_inlet_channel
                    .send(TopicMessage::Publish { address, message })
                    .unwrap();
            }
            ServerMessage::Subscribe {
                address,
                topic_name,
                channel_name,
                send_back,
            } => {
                let topic_inlet_channel = topics.entry(topic_name.clone()).or_insert_with(|| {
                    let (sender, receiver) = unbounded_channel::<TopicMessage>();
                    tokio::spawn(run_topic(topic_name.clone(), receiver, config.clone()));
                    sender
                });
                topic_inlet_channel
                    .send(TopicMessage::Subscribe {
                        address,
                        channel_name,
                        send_back,
                    })
                    .unwrap();
            }
            ServerMessage::Identify { .. } => {
                info!("IDENTIFY command is yet to be implemented")
            }
            ServerMessage::Disconnect { address } => {
                for (_, topic_sender) in topics.iter() {
                    topic_sender
                        .send(TopicMessage::Disconnect { address })
                        .unwrap()
                }
            }
        }
    }
}

pub async fn run_tangled<S>(config: Arc<crate::settings::TangleArguments>, stopper: S) -> Result<()>
where
    S: Future<Output = std::io::Result<()>>,
{
    let listener = tokio::net::TcpListener::bind(config.server_address).await?;
    let (server_channel_sender, server_channel_receiver) =
        tokio::sync::mpsc::channel::<ServerMessage>(128);

    tokio::select! {
        _ = stopper => {
            info!("Ctrl+C signal: Will now stop the server...")
        }
        server_result = try_join_all(vec![
            tokio::spawn(run_network_listener(listener, server_channel_sender)),
            tokio::spawn(run_server(config.clone(), server_channel_receiver)),
        ]) => {
            info!("Fatal: Server exited with this result: {server_result:?}");
        }
    };
    Ok(())
}
