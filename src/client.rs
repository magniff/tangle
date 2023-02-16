use std::marker::Unpin;

use tokio::io::{AsyncBufRead, AsyncWrite};

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone)]
pub struct IdentifyData {
    pub client_id: Option<String>,
    pub hostname: Option<String>,
    pub heartbeat_interval: Option<i32>,
    pub output_buffer_size: Option<i32>,
    pub output_buffer_timeout: Option<i32>,
    pub feature_negotiation: Option<bool>,
    pub tls_v1: Option<bool>,
    pub deflate: Option<bool>,
    pub deflate_level: Option<i32>,
    pub snappy: Option<bool>,
    pub sample_rate: Option<i32>,
    pub user_agent: Option<String>,
    pub msg_timeout: Option<i32>,
}

#[derive(Default)]
pub struct ClientDetails {}

#[derive(PartialEq, Eq)]
pub enum ClientState {
    UnInitialized,
    Connected,
    Subscribed,
    Closing,
}

pub struct Client<R, W> {
    pub address: std::net::SocketAddr,
    pub reader: R,
    pub writer: W,
    pub state: ClientState,
    pub send_back_channel: Option<tokio::sync::mpsc::Sender<crate::server::Server2ClientMessage>>,
    pub details: Option<ClientDetails>,
}

impl<R, W> Client<R, W> {
    pub fn from_reader_writer(address: std::net::SocketAddr, reader: R, writer: W) -> Self
    where
        R: AsyncBufRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        Self {
            address,
            reader,
            writer,
            details: None,
            state: ClientState::UnInitialized,
            send_back_channel: None,
        }
    }
}
