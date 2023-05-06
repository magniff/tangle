use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use tokio;

// Client2serverMessage is suppose to be a message origin
#[derive(Debug)]
pub enum Message {
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
        message: Arc<Bytes>,
    },
    Subscribe {
        address: std::net::SocketAddr,
        topic_name: String,
        channel_name: String,
        back_to_client: UnboundedSender<super::client::Message>,
    },
    Finalize {
        address: std::net::SocketAddr,
        message_id: [u8; 16],
    },
    Requeue {
        address: std::net::SocketAddr,
        message_id: [u8; 16],
    },
    SetCapacity {
        address: std::net::SocketAddr,
        capacity: usize,
    },
}

pub async fn run_server(
    config: Arc<crate::settings::TangleArguments>,
    mut messages: UnboundedReceiver<Message>,
) {
    // Maps topic's names to their input channels
    let mut topics: HashMap<String, UnboundedSender<super::topic::Message>> = HashMap::new();

    while let Some(to_server_message) = messages.recv().await {
        match to_server_message {
            Message::SetCapacity { address, capacity } => {
                for (_, topic_sender) in topics.iter() {
                    topic_sender
                        .send(super::topic::Message::SetCapacity { address, capacity })
                        .unwrap()
                }
            }
            Message::Finalize {
                address,
                message_id,
            } => {
                for (_, topic_sender) in topics.iter() {
                    topic_sender
                        .send(super::topic::Message::Finalize {
                            address,
                            message_id,
                        })
                        .unwrap()
                }
            }
            Message::Requeue {
                address,
                message_id,
            } => {
                for (_, topic_sender) in topics.iter() {
                    topic_sender
                        .send(super::topic::Message::Requeue {
                            address,
                            message_id,
                        })
                        .unwrap()
                }
            }
            Message::Publish {
                address,
                topic_name,
                message,
            } => {
                let topic_messages_channel =
                    topics.entry(topic_name.clone()).or_insert_with(|| {
                        let (sender, receiver) = unbounded_channel::<super::topic::Message>();
                        tokio::spawn(super::topic::run_topic(
                            topic_name,
                            receiver,
                            config.clone(),
                        ));
                        sender
                    });
                topic_messages_channel
                    .send(super::topic::Message::Publish { address, message })
                    .unwrap();
            }
            Message::Subscribe {
                address,
                topic_name,
                channel_name,
                back_to_client,
            } => {
                let topic_incomming_messages =
                    topics.entry(topic_name.clone()).or_insert_with(|| {
                        let (sender, receiver) = unbounded_channel::<super::topic::Message>();
                        tokio::spawn(super::topic::run_topic(
                            topic_name.clone(),
                            receiver,
                            config.clone(),
                        ));
                        sender
                    });
                topic_incomming_messages
                    .send(super::topic::Message::Subscribe {
                        address,
                        channel_name,
                        back_to_client,
                    })
                    .unwrap();
            }
            Message::Identify { .. } => {
                log::info!("IDENTIFY command is yet to be implemented")
            }
            Message::Disconnect { address } => {
                for (_, topic_sender) in topics.iter() {
                    topic_sender
                        .send(super::topic::Message::Disconnect { address })
                        .unwrap()
                }
            }
        }
    }
}
