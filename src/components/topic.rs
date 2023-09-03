use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use log;
use rand::{self, prelude::IteratorRandom, SeedableRng};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Notify,
};

use crate::message::NSQMessage;

#[derive(Debug)]
pub enum TopicMessage {
    Disconnect {
        address: std::net::SocketAddr,
    },
    Subscribe {
        address: std::net::SocketAddr,
        channel_name: String,
        back_to_client: UnboundedSender<crate::client::Message>,
    },
    Publish {
        message: Bytes,
    },
    SetCapacity {
        address: std::net::SocketAddr,
        capacity: usize,
    },
}

#[derive(Debug)]
pub enum ChannelMessage {
    Publish {
        address: std::net::SocketAddr,
        message: crate::message::NSQMessage,
    },
}

#[derive(Debug)]
pub enum ChannelNotification {
    ClientConnect {
        address: std::net::SocketAddr,
        send_back: UnboundedSender<crate::client::Message>,
    },
    ClientDisconnect {
        address: std::net::SocketAddr,
    },
    ClientRdy {
        address: SocketAddr,
        value: usize,
    },
    MessageAcked {
        message_id: [u8; 16],
    },
    MessageRequeued {
        message_id: [u8; 16],
    },
}

#[derive(Debug)]
struct ClientDescriptor {
    queue: UnboundedSender<crate::client::Message>,
    capacity: usize,
}

async fn channel_worker(
    channel_name: String,
    messages_receiver: super::pq::PQReceiver<NSQMessage>,
    messages_sender: super::pq::PQSender<NSQMessage>,
    mut notifications_receiver: UnboundedReceiver<ChannelNotification>,
    notifications_sender: UnboundedSender<ChannelNotification>,
) {
    log::info!("Spinning up a channel worker: {channel_name}");

    let mut rng = rand::rngs::StdRng::from_entropy();
    let mut clients = HashMap::<SocketAddr, ClientDescriptor>::with_capacity(256);
    let mut pending_messages = HashMap::<[u8; 16], NSQMessage>::with_capacity(1024);
    let mut pending_message_to_address = HashMap::<[u8; 16], SocketAddr>::with_capacity(1024);
    let someone_is_waiting = Notify::new();

    loop {
        tokio::select! {
            Some(notification) = notifications_receiver.recv() => {
                match notification {
                    ChannelNotification::MessageAcked {message_id} => {
                        pending_messages.remove(&message_id);
                        if let Some(address) = pending_message_to_address.remove(&message_id) {
                            if let Some(descriptor) = clients.get_mut(&address) {
                                descriptor.capacity += 1;
                                someone_is_waiting.notify_one();
                            }
                        }
                    },
                    ChannelNotification::MessageRequeued {message_id} => {
                        // The requeued message may be processed by someone else, so remove the mapping endtry
                        if let Some(client_address) = pending_message_to_address.remove(&message_id) {
                            if let Some(descriptor) = clients.get_mut(&client_address) {
                                descriptor.capacity += 1;
                                someone_is_waiting.notify_one();
                            }
                        }
                        if let Some(pending_message) = pending_messages.get_mut(&message_id) {
                            pending_message.attempts += 1;
                            messages_sender.send(pending_message.clone());
                        }
                    },
                    ChannelNotification::ClientConnect {address, send_back} => {
                        clients.insert(address, ClientDescriptor {queue: send_back, capacity: 0});
                    }
                    ChannelNotification::ClientDisconnect {address} => {
                        clients.remove(&address);
                    }
                    ChannelNotification::ClientRdy {address, value} => {
                        if let Some(descriptor) = clients.get_mut(&address) {
                            descriptor.capacity = value;
                            someone_is_waiting.notify_one();
                        }
                    }
                }
            }
            _ = someone_is_waiting.notified() => {
               while let Some((&address, descriptor)) = clients
                    .iter_mut()
                    .filter(|(_, descriptor)| descriptor.capacity > 0)
                    .choose(&mut rng) {
                        while descriptor.capacity > 0 {
                            let message = messages_receiver.recv().await.unwrap();
                            let message_id = message.id;
                            descriptor.capacity -= 1;
                            // Trying to push the message to the client's io task
                            if descriptor.queue
                                .send(
                                    crate::client::Message::Send {
                                        payload: message.clone(),
                                        notify_back: notifications_sender.clone(),
                                    }
                                )
                                .is_err()
                            {
                                notifications_sender
                                    .send(ChannelNotification::ClientDisconnect {address})
                                    .unwrap();
                                messages_sender.send(message);
                                break;
                            }
                            // Registring message as in-flight only if the client was alive
                            pending_messages.insert(message_id, message);
                            pending_message_to_address.insert(message_id, address);
                        }
                    }
            }
            else => break
        }
    }
}

struct ChannelIO {
    messages: super::pq::PQSender<NSQMessage>,
    notifications: UnboundedSender<ChannelNotification>,
}

pub async fn run_topic(
    topic_name: String,
    mut messages: UnboundedReceiver<TopicMessage>,
    _config: Arc<crate::settings::TangleArguments>,
) -> anyhow::Result<()> {
    log::info!("Spinning up a topic worker: {topic_name}");

    let (buffer_sender, mut buffer_receiver) = unbounded_channel::<Bytes>();
    let mut channels_io = HashMap::<String, ChannelIO>::new();

    loop {
        tokio::select! {
            Some(message) = buffer_receiver.recv(), if !channels_io.is_empty() => {
                for io_pair in channels_io.values() {
                    io_pair.messages.send(NSQMessage::from_body(message.clone()));
                }
            }
            Some(message) = messages.recv() => {
                match message {
                    TopicMessage::SetCapacity {address, capacity} => {
                        for channel_io in channels_io.values() {
                            channel_io.notifications
                                .send(ChannelNotification::ClientRdy { address, value: capacity })
                                .unwrap();
                        }
                    }
                    TopicMessage::Disconnect {address} => {
                        for channel_io in channels_io.values() {
                            channel_io
                                .notifications
                                .send(ChannelNotification::ClientDisconnect {address})
                                .unwrap()
                        }
                    },
                    TopicMessage::Subscribe {
                        address,
                        channel_name,
                        back_to_client,
                    } => {
                        let channel_io_pair = channels_io.entry(channel_name.clone()).or_insert_with(|| {
                            let (message_sender, message_receiver) = super::pq::pq_channel::<NSQMessage>();
                            let (notification_sender, notification_receiver) =
                                unbounded_channel::<ChannelNotification>();
                            tokio::spawn(
                                channel_worker(
                                    channel_name,
                                    message_receiver,
                                    message_sender.clone(),
                                    notification_receiver,
                                    notification_sender.clone(),
                                )
                            );
                            ChannelIO { messages: message_sender, notifications: notification_sender }
                        });
                        channel_io_pair
                            .notifications
                            .send(ChannelNotification::ClientConnect {address, send_back: back_to_client})
                            .unwrap();
                    }
                    TopicMessage::Publish { message } => { buffer_sender.send(message).unwrap(); }
                }
             }
             else => break
        }
    }
    Ok(())
}
