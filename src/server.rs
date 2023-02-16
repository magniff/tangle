use std::collections::HashMap;
use std::future::Future;
use tokio::sync::mpsc::{unbounded_channel, Sender, UnboundedReceiver, UnboundedSender};

use anyhow::Result;
use futures::future::try_join_all;
use log::{error, info, warn};
use tokio;
use tokio::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::TcpListener;

use crate::client::Client;
use crate::message::Message;
use crate::protocol::{socket_mainloop, write_error_frame, E_BAD_PROTOCOL};

#[derive(Debug)]
pub enum Server2ClientMessage {
    SendToClient { message: crate::message::Message },
}

#[derive(Debug)]
pub enum Client2ServerMessage {
    Disconnect {
        address: std::net::SocketAddr,
    },
    NotifyReady {
        address: std::net::SocketAddr,
        count: u32,
    },
    Identify {
        address: std::net::SocketAddr,
        data: crate::client::IdentifyData,
    },
    Publish {
        address: std::net::SocketAddr,
        topic_name: String,
        message: crate::message::Message,
    },
    Subscribe {
        address: std::net::SocketAddr,
        topic_name: String,
        channel_name: String,
        send_back: Sender<Server2ClientMessage>,
    },
    Fin {
        address: std::net::SocketAddr,
        message_id: [u8; 16],
    },
}

pub enum FromServerMessage {}

async fn run_network_listener(
    listener: TcpListener,
    to_server_sender: tokio::sync::mpsc::Sender<Client2ServerMessage>,
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

        tokio::spawn(socket_mainloop(
            Client::from_reader_writer(address, reader, writer),
            to_server_sender.clone(),
        ));

        info!("Client {address} is now connected");
    }
}

#[derive(Debug)]
enum Server2TopicMessage {
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
        message: crate::message::Message,
    },
}

#[derive(Debug)]
enum Topic2ChannelMessage {
    Disconnect {
        address: std::net::SocketAddr,
    },
    Publish {
        address: std::net::SocketAddr,
        message: crate::message::Message,
    },
    Subscribe {
        address: std::net::SocketAddr,
        send_back: Sender<Server2ClientMessage>,
    },
}

async fn run_channel(
    name: String,
    mut topic2channel_channel: UnboundedReceiver<Topic2ChannelMessage>,
) {
    info!("Spinning up a new channel {name}");
    let mut clients: HashMap<std::net::SocketAddr, Sender<Server2ClientMessage>> = HashMap::new();
    let (internal_buf_send, mut internal_buf_recv) =
        tokio::sync::mpsc::unbounded_channel::<(std::net::SocketAddr, Message)>();

    loop {
        tokio::select! {
            Some((_, message)) = internal_buf_recv.recv(), if clients.len() > 0 => {
                for (ref address, ref client_channel) in clients.iter() {
                    let send_result =
                        client_channel
                            .send(
                                Server2ClientMessage::SendToClient { message: message.clone() }
                            )
                            .await;
                    if let Err(_) = send_result {
                        warn!("Client {address} seems to be disconnecred", address=address.to_string())
                    }
                }
            }
            Some(message) = topic2channel_channel.recv() => {
                match message {
                    Topic2ChannelMessage::Disconnect {address} => {
                        clients.remove(&address);
                    }
                    Topic2ChannelMessage::Publish {
                        message: message_to_publish,
                        address,
                    } => {
                        internal_buf_send
                            .send((address, message_to_publish))
                            .unwrap();
                    }
                    Topic2ChannelMessage::Subscribe { send_back, address, .. } => {
                        clients.insert(address, send_back);
                    }
                }
            }
        }
    }
}

async fn run_topic(name: String, mut server2topic_channel: UnboundedReceiver<Server2TopicMessage>) {
    info!("Spinning up a new topic {name}");
    let mut channels: HashMap<String, UnboundedSender<Topic2ChannelMessage>> = HashMap::new();
    let (internal_buf_send, mut internal_buf_recv) =
        tokio::sync::mpsc::unbounded_channel::<(std::net::SocketAddr, Message)>();

    loop {
        tokio::select! {
            Some((address, message)) = internal_buf_recv.recv(), if channels.len() > 0 => {
                for (_, inlet) in channels.iter() {
                    inlet
                        .send(Topic2ChannelMessage::Publish {
                            address: address,
                            message: message.clone(),
                        })
                        .unwrap()
                }
            }
            Some(message) = server2topic_channel.recv() => {
                match message {
                    // Forward a clients disconnection event to the actual queues
                    Server2TopicMessage::Disconnect {address} => {
                        for (_, inlet) in channels.iter() {
                            inlet
                                .send(Topic2ChannelMessage::Disconnect { address: address })
                                .unwrap()
                        }
                    }
                    Server2TopicMessage::Subscribe {
                        address,
                        channel_name,
                        send_back,
                        ..
                    } => {
                        let channel_inlet = channels.entry(channel_name.clone()).or_insert_with(|| {
                            let (sender, receiver) = unbounded_channel::<Topic2ChannelMessage>();
                            tokio::spawn(run_channel(channel_name.clone(), receiver));
                            sender
                        });
                        channel_inlet
                            .send(Topic2ChannelMessage::Subscribe {
                                address: address,
                                send_back,
                            })
                            .unwrap();
                    }
                    Server2TopicMessage::Publish { address, message } => {
                        internal_buf_send.send((address, message)).unwrap();
                    }
                }
            }
            else => break
        }
    }
}

async fn run_server(mut to_server_receiver: tokio::sync::mpsc::Receiver<Client2ServerMessage>) {
    let mut topics: HashMap<String, UnboundedSender<Server2TopicMessage>> = HashMap::new();

    while let Some(to_server_message) = to_server_receiver.recv().await {
        match to_server_message {
            Client2ServerMessage::Fin { .. } => {}
            Client2ServerMessage::Publish {
                address,
                topic_name,
                message,
            } => {
                let topic_inlet_channel = topics.entry(topic_name.clone()).or_insert_with(|| {
                    let (sender, receiver) = unbounded_channel::<Server2TopicMessage>();
                    tokio::spawn(run_topic(topic_name, receiver));
                    sender
                });
                topic_inlet_channel
                    .send(Server2TopicMessage::Publish { address, message })
                    .unwrap();
            }
            Client2ServerMessage::Subscribe {
                address,
                topic_name,
                channel_name,
                send_back,
            } => {
                let topic_inlet_channel = topics.entry(topic_name.clone()).or_insert_with(|| {
                    let (sender, receiver) = unbounded_channel::<Server2TopicMessage>();
                    tokio::spawn(run_topic(topic_name.clone(), receiver));
                    sender
                });
                topic_inlet_channel
                    .send(Server2TopicMessage::Subscribe {
                        address,
                        channel_name,
                        send_back,
                    })
                    .unwrap();
            }
            Client2ServerMessage::NotifyReady { .. } => {
                info!("RDY command is yet to be implemented")
            }
            Client2ServerMessage::Identify { .. } => {
                info!("IDENTIFY command is yet to be implemented")
            }
            Client2ServerMessage::Disconnect { address } => {
                for (_, ref channel) in topics.iter() {
                    channel
                        .send(Server2TopicMessage::Disconnect { address })
                        .unwrap()
                }
            }
        }
    }
}

pub async fn run(
    listener: TcpListener,
    stopper: impl Future<Output = std::io::Result<()>>,
) -> Result<()> {
    info!("Spinning up the server, {listener:?}");
    let (server_channel_sender, server_channel_receiver) =
        tokio::sync::mpsc::channel::<Client2ServerMessage>(128);

    tokio::select! {
        _ = stopper => {
            info!("Ctrl+C signal: Will now stop the server...")
        }
        server_result = try_join_all(vec![
            tokio::spawn(run_network_listener(listener, server_channel_sender)),
            tokio::spawn(run_server(server_channel_receiver)),
        ]) => {
            info!("Fatal: Server exited with this result: {server_result:?}");
        }
    };
    Ok(())
}
