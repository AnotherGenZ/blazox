//! Internal event fanout primitives used by the client, session, and mocks.
//!
//! Unlike a per-subscriber channel fanout, this hub keeps one shared backlog
//! and advances subscribers with independent cursors. That avoids cloning every
//! event into every subscriber queue while preserving ordered delivery.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use tokio::sync::Notify;

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

/// Metrics hook invoked when the shared backlog grows or shrinks.
pub trait EventHubMetrics: Send + Sync {
    /// Called after one event was added to the shared backlog.
    fn on_buffered_event(&self);

    /// Called after one event was released from the shared backlog.
    fn on_released_event(&self);
}

/// Shared multi-subscriber event hub.
pub struct EventHub<T> {
    inner: Arc<EventHubInner<T>>,
}

struct EventHubInner<T> {
    state: Mutex<EventHubState<T>>,
    metrics: Mutex<Option<Arc<dyn EventHubMetrics>>>,
    notify: Notify,
    next_subscriber_id: AtomicUsize,
    hub_count: AtomicUsize,
}

struct EventHubState<T> {
    base_sequence: u64,
    buffer: VecDeque<T>,
    subscribers: HashMap<usize, u64>,
}

/// Receiver returned by [`EventHub::subscribe`].
pub struct EventReceiver<T> {
    inner: Arc<EventHubInner<T>>,
    subscriber_id: usize,
}

impl<T> fmt::Debug for EventHub<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventHub").finish_non_exhaustive()
    }
}

impl<T> Default for EventHubState<T> {
    fn default() -> Self {
        Self {
            base_sequence: 0,
            buffer: VecDeque::new(),
            subscribers: HashMap::new(),
        }
    }
}

impl<T> EventHub<T> {
    /// Creates an empty event hub.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(EventHubInner {
                state: Mutex::new(EventHubState::default()),
                metrics: Mutex::new(None),
                notify: Notify::new(),
                next_subscriber_id: AtomicUsize::new(1),
                hub_count: AtomicUsize::new(1),
            }),
        }
    }

    /// Installs or replaces the metrics hook for this hub.
    pub fn set_metrics(&self, metrics: Arc<dyn EventHubMetrics>) {
        let mut slot = self.inner.metrics.lock().expect("event hub metrics mutex poisoned");
        *slot = Some(metrics);
    }

    /// Registers a new subscriber.
    pub fn subscribe(&self) -> EventReceiver<T> {
        let subscriber_id = self
            .inner
            .next_subscriber_id
            .fetch_add(1, Ordering::Relaxed);
        let mut state = self
            .inner
            .state
            .lock()
            .expect("event hub state mutex poisoned");
        let next_sequence = state.base_sequence + state.buffer.len() as u64;
        state.subscribers.insert(subscriber_id, next_sequence);
        drop(state);
        EventReceiver {
            inner: self.inner.clone(),
            subscriber_id,
        }
    }

    /// Delivers an event to all current subscribers.
    pub fn send(&self, event: T) {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("event hub state mutex poisoned");
            if state.subscribers.is_empty() {
                return;
            }
            state.buffer.push_back(event);
        }
        if let Some(metrics) = self
            .inner
            .metrics
            .lock()
            .expect("event hub metrics mutex poisoned")
            .clone()
        {
            metrics.on_buffered_event();
        }
        self.inner.notify.notify_waiters();
    }
}

impl<T> Clone for EventHub<T> {
    fn clone(&self) -> Self {
        self.inner.hub_count.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Default for EventHub<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for EventHub<T> {
    fn drop(&mut self) {
        if self.inner.hub_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.notify.notify_waiters();
        }
    }
}

impl<T> EventReceiver<T>
where
    T: Clone,
{
    /// Waits for the next event.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            if let Some(event) = self.try_take_next().await {
                return Ok(event);
            }
            if self.inner.hub_count.load(Ordering::Acquire) == 0 {
                return Err(RecvError::Closed);
            }
            self.inner.notify.notified().await;
        }
    }

    /// Tries to receive an event immediately.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let metrics = self
            .inner
            .metrics
            .lock()
            .expect("event hub metrics mutex poisoned");
        let metrics = metrics.clone();
        if let Some(event) = self
            .inner
            .state
            .lock()
            .expect("event hub state mutex poisoned")
            .try_take_next(self.subscriber_id, metrics.as_ref())
        {
            return Ok(event);
        }
        if self.inner.hub_count.load(Ordering::Acquire) == 0 {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    async fn try_take_next(&mut self) -> Option<T> {
        let metrics = self
            .inner
            .metrics
            .lock()
            .expect("event hub metrics mutex poisoned")
            .clone();
        self.inner
            .state
            .lock()
            .expect("event hub state mutex poisoned")
            .try_take_next(self.subscriber_id, metrics.as_ref())
    }
}

impl<T> Drop for EventReceiver<T> {
    fn drop(&mut self) {
        let metrics = self
            .inner
            .metrics
            .lock()
            .expect("event hub metrics mutex poisoned");
        let metrics = metrics.clone();
        let mut state = self
            .inner
            .state
            .lock()
            .expect("event hub state mutex poisoned");
        state.subscribers.remove(&self.subscriber_id);
        state.release_consumed_prefix(metrics.as_ref());
        drop(state);
        drop(metrics);
        self.inner.notify.notify_waiters();
    }
}

impl<T> EventHubState<T> {
    fn release_consumed_prefix(&mut self, metrics: Option<&Arc<dyn EventHubMetrics>>) {
        let Some(min_sequence) = self.subscribers.values().min().copied() else {
            let released = self.buffer.len();
            self.buffer.clear();
            self.base_sequence = 0;
            if let Some(metrics) = metrics {
                for _ in 0..released {
                    metrics.on_released_event();
                }
            }
            return;
        };

        while self.base_sequence < min_sequence {
            if self.buffer.pop_front().is_none() {
                break;
            }
            self.base_sequence += 1;
            if let Some(metrics) = metrics {
                metrics.on_released_event();
            }
        }
    }
}

impl<T> EventHubState<T>
where
    T: Clone,
{
    fn try_take_next(
        &mut self,
        subscriber_id: usize,
        metrics: Option<&Arc<dyn EventHubMetrics>>,
    ) -> Option<T> {
        let next_sequence = *self.subscribers.get(&subscriber_id)?;
        let offset = usize::try_from(next_sequence.saturating_sub(self.base_sequence)).ok()?;
        let event = self.buffer.get(offset)?.clone();
        if let Some(sequence) = self.subscribers.get_mut(&subscriber_id) {
            *sequence = next_sequence + 1;
        }
        self.release_consumed_prefix(metrics);
        Some(event)
    }
}
