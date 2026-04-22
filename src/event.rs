//! Internal event fanout primitives used by the client, session, and mocks.
//!
//! Unlike `tokio::sync::broadcast`, this fanout does not drop messages when a
//! receiver falls behind. Each subscriber gets its own unbounded queue.

use std::collections::HashMap;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use tokio::sync::mpsc;

/// Receive-side error for [`EventReceiver::recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// The sender side has been dropped.
    Closed,
}

/// Receive-side error for [`EventReceiver::try_recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// No event is currently available.
    Empty,
    /// The sender side has been dropped.
    Closed,
}

/// Non-dropping multi-subscriber event hub.
#[derive(Debug, Clone)]
pub struct EventHub<T> {
    inner: Arc<EventHubInner<T>>,
}

#[derive(Debug)]
struct EventHubInner<T> {
    subscribers: Mutex<HashMap<usize, mpsc::UnboundedSender<T>>>,
    next_subscriber_id: AtomicUsize,
}

/// Receiver returned by [`EventHub::subscribe`].
#[derive(Debug)]
pub struct EventReceiver<T> {
    receiver: mpsc::UnboundedReceiver<T>,
    hub: Weak<EventHubInner<T>>,
    subscriber_id: usize,
}

impl<T> EventHub<T> {
    /// Creates an empty event hub.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(EventHubInner {
                subscribers: Mutex::new(HashMap::new()),
                next_subscriber_id: AtomicUsize::new(1),
            }),
        }
    }

    /// Registers a new subscriber.
    pub fn subscribe(&self) -> EventReceiver<T> {
        let subscriber_id = self
            .inner
            .next_subscriber_id
            .fetch_add(1, Ordering::Relaxed);
        let (tx, receiver) = mpsc::unbounded_channel();
        self.inner
            .subscribers
            .lock()
            .expect("event hub subscribers mutex poisoned")
            .insert(subscriber_id, tx);
        EventReceiver {
            receiver,
            hub: Arc::downgrade(&self.inner),
            subscriber_id,
        }
    }

    /// Delivers an event to all current subscribers.
    pub fn send(&self, event: T)
    where
        T: Clone,
    {
        let mut stale = Vec::new();
        let mut subscribers = self
            .inner
            .subscribers
            .lock()
            .expect("event hub subscribers mutex poisoned");
        for (subscriber_id, tx) in subscribers.iter() {
            if tx.send(event.clone()).is_err() {
                stale.push(*subscriber_id);
            }
        }
        for subscriber_id in stale {
            subscribers.remove(&subscriber_id);
        }
    }
}

impl<T> Default for EventHub<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> EventReceiver<T> {
    /// Waits for the next event.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        self.receiver.recv().await.ok_or(RecvError::Closed)
    }

    /// Tries to receive an event immediately.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.receiver.try_recv().map_err(|error| match error {
            mpsc::error::TryRecvError::Empty => TryRecvError::Empty,
            mpsc::error::TryRecvError::Disconnected => TryRecvError::Closed,
        })
    }
}

impl<T> Drop for EventReceiver<T> {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            hub.subscribers
                .lock()
                .expect("event hub subscribers mutex poisoned")
                .remove(&self.subscriber_id);
        }
    }
}
