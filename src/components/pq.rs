use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

struct Inner<T> {
    queue: BinaryHeap<Reverse<T>>,
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
    available: Notify,
}

pub struct PQSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> PQSender<T>
where
    T: Ord,
{
    pub async fn send(&self, value: T) {
        let mut inner_guard = self.shared.inner.lock().await;
        inner_guard.queue.push(Reverse(value));
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
    pub async fn recv(&self) -> T {
        let mut inner_lock = self.shared.inner.lock().await;
        loop {
            match inner_lock.queue.pop() {
                None => self.shared.available.notified().await,
                Some(value) => break value.0,
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
