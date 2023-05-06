use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use rand::{distributions::Alphanumeric, Rng};

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct NSQMessage {
    pub id: [u8; 16],
    pub timestamp: u64,
    pub attempts: u16,
    pub body: Arc<Bytes>,
}

impl NSQMessage {
    pub fn from_body(body: Arc<Bytes>) -> Self {
        Self {
            id: rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(16)
                .collect::<Vec<u8>>()
                .try_into()
                .unwrap(),
            body,
            attempts: 1,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
        }
    }
}

impl std::cmp::Ord for NSQMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl std::cmp::PartialOrd for NSQMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
