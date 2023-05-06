use crate::message::NSQMessage;

#[derive(Debug)]
pub enum Message {
    PushMessage { message: NSQMessage },
    PushResponse { message: &'static str },
}
