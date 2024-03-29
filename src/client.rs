use std::marker::Unpin;

use tokio::io::{AsyncBufRead, AsyncWrite};
use tokio::sync::mpsc::UnboundedSender;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone)]
pub struct IdentifyData {
    pub client_id: Option<String>,
    pub hostname: Option<String>,
    pub user_agent: Option<String>,
    pub feature_negotiation: bool,
    pub tls_v1: bool,
    pub deflate: bool,
    pub snappy: bool,
    pub sample_rate: Option<u32>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct IdentifyResponse {
    max_rdy_count: u16,
    version: String,
    max_msg_timeout: u32,
    msg_timeout: u32,
    tls_v1: bool,
    deflate: bool,
    deflate_level: u8,
    max_deflate_level: u8,
    snappy: bool,
    sample_rate: u8,
    auth_required: bool,
    output_buffer_size: u32,
    output_buffer_timeout: u32,
}

impl Default for IdentifyResponse {
    fn default() -> Self {
        Self {
            max_rdy_count: 1000,
            version: "0.0.1".to_string(),
            max_msg_timeout: 200,
            msg_timeout: 0,
            tls_v1: false,
            deflate: false,
            deflate_level: 0,
            max_deflate_level: 0,
            snappy: false,
            sample_rate: 0,
            auth_required: false,
            output_buffer_size: 16 * 1024,
            output_buffer_timeout: 0,
        }
    }
}

#[derive(Default)]
pub struct ClientDetails {}

pub struct Client<R, W> {
    pub details: Option<ClientDetails>,
    pub address: std::net::SocketAddr,
    pub socker_reader: R,
    pub socker_writer: W,
}

impl<R, W> Client<R, W> {
    pub fn from_reader_writer(address: std::net::SocketAddr, reader: R, writer: W) -> Self
    where
        R: AsyncBufRead,
        W: AsyncWrite + Unpin,
    {
        Self {
            address,
            socker_reader: reader,
            socker_writer: writer,
            details: None,
        }
    }
}

pub enum Message {
    Send {
        payload: crate::message::NSQMessage,
        notify_back: UnboundedSender<crate::components::topic::ChannelNotification>,
    },
}
