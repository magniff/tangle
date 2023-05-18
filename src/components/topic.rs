use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use log;
use rand::{self, prelude::IteratorRandom, SeedableRng};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use super::client::Message as ClientMessage;
use crate::message::NSQMessage;

#[derive(Debug)]
pub enum Message {
    Disconnect {
        address: std::net::SocketAddr,
    },
    Subscribe {
        address: std::net::SocketAddr,
        channel_name: String,
        back_to_client: UnboundedSender<super::client::Message>,
    },
    Publish {
        address: std::net::SocketAddr,
        message: Arc<Bytes>,
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

struct ClientDescriptor {
    queue: UnboundedSender<ClientMessage>,
    capacity: usize,
}

#[derive(Debug)]
enum WorkerNotification {
    ClientConnected(SocketAddr, UnboundedSender<ClientMessage>),
    ClientDisconnected(SocketAddr),
    SetCapacity(SocketAddr, usize),
    MessageAcked([u8; 16]),
    MessageRequeued([u8; 16]),
}

async fn channel_worker(
    channel_name: String,
    messages_receiver: super::pq::PQReceiver<NSQMessage>,
    messages_sender: super::pq::PQSender<NSQMessage>,
    mut notifications: UnboundedReceiver<WorkerNotification>,
) {
    log::trace!("Spinning up a channel worker: {channel_name}");
    let mut rng = rand::rngs::StdRng::from_entropy();

    let mut clients = HashMap::<SocketAddr, ClientDescriptor>::with_capacity(256);
    // Messages that havent been acked yet
    let mut pending_messages = HashMap::<[u8; 16], NSQMessage>::with_capacity(1024);
    // Map from the unacked message to its original consumer
    let mut pending_message_to_address = HashMap::<[u8; 16], SocketAddr>::with_capacity(1024);

    loop {
        tokio::select! {
            Some(notification) = notifications.recv() => {
                match notification {
                    WorkerNotification::MessageAcked(message_id) => {
                        pending_messages.remove(&message_id);
                        if let Some(address) = pending_message_to_address.remove(&message_id) {
                            if let Some(descriptor) = clients.get_mut(&address) {
                                descriptor.capacity += 1;
                            }
                        }
                    },
                    WorkerNotification::MessageRequeued(message_id) => {
                        // The requeued message may be processed by someone else, so remove the mapping endtry
                        if let Some(client_address) = pending_message_to_address.remove(&message_id) {
                            if let Some(descriptor) = clients.get_mut(&client_address) {
                                descriptor.capacity += 1;
                            }
                        }
                        if let Some(pending_message) = pending_messages.get_mut(&message_id) {
                            pending_message.attempts += 1;
                            messages_sender.send(pending_message.clone()).await;
                        }
                    },
                    WorkerNotification::SetCapacity(address, read_value) => {
                        if let Some(descriptor) = clients.get_mut(&address) {
                            descriptor.capacity = read_value;
                        }
                    }
                    WorkerNotification::ClientDisconnected(address) => {
                        clients.remove(&address);
                    }
                    WorkerNotification::ClientConnected(address, queue) => {
                        clients.insert(address, ClientDescriptor {queue, capacity: 0});
                    }
                }
            }
            Some(nsq_message) = messages_receiver.recv(),
                if clients.values().map(|descriptor| descriptor.capacity).any(|value| value > 0) =>
            {
                let (&address, descriptor) = clients
                    .iter_mut()
                    .filter(|(_, descriptor)| descriptor.capacity > 0)
                    .choose(&mut rng)
                    .unwrap();

                descriptor.capacity -= 1;
                pending_message_to_address.insert(nsq_message.id, address);
                pending_messages.insert(nsq_message.id, nsq_message.clone());

                if descriptor.queue.send(ClientMessage::PushMessage { message: nsq_message }).is_err() {
                    clients.remove(&address);
                }
            }
            else => break
        }
    }
}

struct ChannelIO {
    messages: super::pq::PQSender<NSQMessage>,
    notifications: UnboundedSender<WorkerNotification>,
}

pub async fn run_topic(
    topic_name: String,
    mut messages: UnboundedReceiver<Message>,
    _config: Arc<crate::settings::TangleArguments>,
) -> anyhow::Result<()> {
    log::trace!("Spinning up a topic worker: {topic_name}");

    let (buffer_sender, mut buffer_receiver) = unbounded_channel::<Arc<Bytes>>();
    let mut channels_io = HashMap::<String, ChannelIO>::new();
    let mut mid_to_channel_mapping =
        HashMap::<[u8; 16], UnboundedSender<WorkerNotification>>::new();

    loop {
        tokio::select! {
            Some(message) = buffer_receiver.recv(), if !channels_io.is_empty() => {
                for io_pair in channels_io.values() {
                    let nsq_message = NSQMessage::from_body(message.clone());
                    mid_to_channel_mapping.insert(nsq_message.id, io_pair.notifications.clone());
                    io_pair.messages.send(nsq_message).await;
                }
            }
            Some(message) = messages.recv() => {
                match message {
                    Message::SetCapacity { address, capacity } => {
                        for channel_io in channels_io.values() {
                            channel_io.notifications.send(WorkerNotification::SetCapacity(address, capacity)).unwrap()
                        }
                    }
                    Message::Disconnect {address} => {
                        for channel_io in channels_io.values() {
                            channel_io.notifications.send(WorkerNotification::ClientDisconnected(address)).unwrap()
                        }
                    },
                    Message::Subscribe {
                        address,
                        channel_name,
                        back_to_client,
                    } => {
                        let channel_io_pair = channels_io.entry(channel_name.clone()).or_insert_with(|| {
                            let (message_sender, message_receiver) =
                                super::pq::pq_channel::<NSQMessage>();
                            let (notification_sender, notification_receiver) =
                                unbounded_channel::<WorkerNotification>();
                            tokio::spawn(
                                channel_worker(
                                    channel_name, message_receiver, message_sender.clone(), notification_receiver
                                )
                            );
                            ChannelIO { messages: message_sender, notifications: notification_sender }
                        });
                        channel_io_pair
                            .notifications
                            .send(WorkerNotification::ClientConnected(address, back_to_client))
                            .unwrap();
                    }
                    Message::Finalize { message_id, .. } => {
                        if let Some(notifications_queue) = mid_to_channel_mapping.remove(&message_id) {
                            notifications_queue.send(WorkerNotification::MessageAcked(message_id)).unwrap();
                        }
                    }
                    Message::Requeue { message_id, .. } => {
                        if let Some(notifications_queue) = mid_to_channel_mapping.get(&message_id) {
                            notifications_queue.send(WorkerNotification::MessageRequeued(message_id)).unwrap();
                        }
                    }
                    Message::Publish {message, .. } => { buffer_sender.send(message).unwrap(); }
                }
             }
             else => break
        }
    }
    Ok(())
}
