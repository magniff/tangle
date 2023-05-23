use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use tokio;

#[derive(Debug)]
pub struct RespondTo {
    pub queue: oneshot::Sender<UnboundedSender<crate::components::topic::TopicMessage>>,
    pub address: std::net::SocketAddr,
}

#[derive(Debug)]
pub enum Message {
    Identify {
        address: std::net::SocketAddr,
        data: crate::client::IdentifyData,
    },
    GetTopicInlet {
        topic_name: String,
        respond_to: RespondTo,
    },
}

pub async fn run_server(
    config: Arc<crate::settings::TangleArguments>,
    mut requests: UnboundedReceiver<Message>,
) {
    let mut topics: HashMap<String, UnboundedSender<super::topic::TopicMessage>> = HashMap::new();
    while let Some(to_server_message) = requests.recv().await {
        match to_server_message {
            Message::Identify {
                address,
                data: _data,
            } => {
                log::info!("{address}: IDENTIFY command is not implemented yet..");
            }
            Message::GetTopicInlet {
                topic_name,
                respond_to,
            } => {
                let topic_inlet = topics.entry(topic_name.clone()).or_insert_with(|| {
                    let (sender, receiver) = unbounded_channel::<super::topic::TopicMessage>();
                    tokio::spawn(super::topic::run_topic(
                        topic_name,
                        receiver,
                        config.clone(),
                    ));
                    sender
                });
                if respond_to.queue.send(topic_inlet.clone()).is_err() {
                    log::warn!(
                        "Client {address} has suddenly disconnected",
                        address = respond_to.address
                    );
                }
            }
        }
    }
    log::info!("Server: \"requests\" channel is now closed")
}
