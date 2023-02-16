use std::{
    cmp::Ordering,
    ops::Deref,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::{BufMut, Bytes, BytesMut};
use rand::{distributions::Alphanumeric, Rng};

#[derive(Default, Eq, Debug, Clone)]
pub struct Message {
    id: [u8; 16],
    timestamp: u64,
    attempts: u16,
    body: Vec<u8>,
}

impl Deref for Message {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

impl Ord for Message {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for Message {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

impl Message {
    /// # Constructs a new message instance from a slice buffer
    /// ## Example
    /// ```rust
    /// use tangle::message::Message;
    /// let my_message = Message::from_body(vec![1,2,3]);
    /// assert_eq!(*my_message, vec![1,2,3]);
    /// ```
    pub fn from_body<T>(body: T) -> Self
    where
        T: AsRef<[u8]>,
    {
        Self {
            id: rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(16)
                .collect::<Vec<u8>>()
                .try_into()
                .unwrap(),
            body: Vec::from(body.as_ref()),
            attempts: 1,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
        }
    }
    // From the NSQD docs:
    //
    //	[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
    //	|       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
    //	|       8-byte         ||    ||                 16-byte                      || N-byte
    //	------------------------------------------------------------------------------------------...
    //	  nanosecond timestamp    ^^                   message ID                       message body
    //	                       (uint16)
    //	                        2-byte
    //	                       attempts
    pub fn as_bytes(&self) -> Bytes {
        let mut output = BytesMut::with_capacity(8 + 2 + 16 + self.body.len());
        output.put_u64(self.timestamp);
        output.put_u16(self.attempts);
        output.put_slice(self.id.as_slice());
        output.put_slice(self.body.as_slice());
        output.freeze()
    }
}
