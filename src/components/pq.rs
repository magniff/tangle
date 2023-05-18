use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

struct Inner<T> {
    queue: BinaryHeap<Reverse<T>>,
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
    available: Notify,
}

#[derive(Clone)]
pub struct PQSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> PQSender<T>
where
    T: Ord,
{
    pub fn send(&self, value: T) {
        self.shared.inner.lock().unwrap().queue.push(Reverse(value));
        self.shared.available.notify_one();
    }
}

pub struct PQReceiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> PQReceiver<T>
where
    T: Ord,
{
    pub async fn recv(&self) -> Option<T> {
        loop {
            // Get the value and release the lock ASAP
            let poped_value = self.shared.inner.lock().unwrap().queue.pop();
            match poped_value {
                None => self.shared.available.notified().await,
                Some(value) => break Some(value.0),
            }
        }
    }
}

pub fn pq_channel<T>() -> (PQSender<T>, PQReceiver<T>)
where
    T: Ord,
{
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner {
            queue: BinaryHeap::<Reverse<T>>::with_capacity(128),
        }),
        available: Notify::new(),
    });
    (
        PQSender {
            shared: shared.clone(),
        },
        PQReceiver { shared },
    )
}
