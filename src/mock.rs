//! In-memory mocks for session- and queue-level tests.

use crate::error::{Error, Result};
use crate::session::{
    Acknowledgement, CloseQueueStatus, ConfigureQueueStatus, QueueEvent, SessionEvent,
};
use crate::types::{
    ConfirmBatch, ConfirmMessage, MessageConfirmationCookie, PostBatch, PostMessage, QueueOptions,
    Uri,
};
use crate::wire::MessageGuid;
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use tokio::sync::{Mutex, broadcast};

/// Recorded queue open call.
#[derive(Debug, Clone)]
pub struct RecordedOpenQueue {
    /// Queue URI.
    pub uri: Uri,
    /// Options used for the open.
    pub options: QueueOptions,
    /// Queue id assigned by the mock session.
    pub queue_id: u32,
}

/// Recorded queue reconfigure call.
#[derive(Debug, Clone)]
pub struct RecordedReconfigureQueue {
    /// Queue URI.
    pub uri: Uri,
    /// Options supplied during reconfiguration.
    pub options: QueueOptions,
    /// Queue id affected by the reconfiguration.
    pub queue_id: u32,
}

/// Recorded queue close call.
#[derive(Debug, Clone)]
pub struct RecordedCloseQueue {
    /// Queue URI.
    pub uri: Uri,
    /// Queue id that was closed.
    pub queue_id: u32,
}

/// Recorded post call.
#[derive(Debug, Clone)]
pub struct RecordedPost {
    /// Queue URI.
    pub uri: Uri,
    /// Queue id targeted by the post.
    pub queue_id: u32,
    /// Messages posted in the batch.
    pub messages: Vec<PostMessage>,
}

/// Recorded confirm call.
#[derive(Debug, Clone)]
pub struct RecordedConfirm {
    /// Queue URI.
    pub uri: Uri,
    /// Queue id targeted by the confirmations.
    pub queue_id: u32,
    /// Confirmations sent in the batch.
    pub messages: Vec<ConfirmMessage>,
}

/// In-memory session mock with recorded side effects.
#[derive(Clone)]
pub struct MockSession {
    inner: Arc<MockSessionInner>,
}

/// In-memory queue mock returned by [`MockSession`].
#[derive(Clone)]
pub struct MockQueue {
    session: MockSession,
    state: Arc<MockQueueState>,
}

struct MockSessionInner {
    started: AtomicBool,
    next_queue_id: AtomicU32,
    events: broadcast::Sender<SessionEvent>,
    queues: Mutex<HashMap<u32, Arc<MockQueueState>>>,
    queue_uris: Mutex<HashMap<String, u32>>,
    opens: Mutex<Vec<RecordedOpenQueue>>,
    reconfigures: Mutex<Vec<RecordedReconfigureQueue>>,
    closes: Mutex<Vec<RecordedCloseQueue>>,
    posts: Mutex<Vec<RecordedPost>>,
    confirms: Mutex<Vec<RecordedConfirm>>,
}

struct MockQueueState {
    uri: Uri,
    queue_id: u32,
    options: Mutex<QueueOptions>,
    events: broadcast::Sender<QueueEvent>,
    closed: AtomicBool,
}

impl MockSession {
    /// Creates a started mock session.
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            inner: Arc::new(MockSessionInner {
                started: AtomicBool::new(true),
                next_queue_id: AtomicU32::new(1),
                events,
                queues: Mutex::new(HashMap::new()),
                queue_uris: Mutex::new(HashMap::new()),
                opens: Mutex::new(Vec::new()),
                reconfigures: Mutex::new(Vec::new()),
                closes: Mutex::new(Vec::new()),
                posts: Mutex::new(Vec::new()),
                confirms: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Returns `true` when the mock session is started.
    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::SeqCst)
    }

    /// Marks the mock session started and emits `Connected`.
    pub async fn start(&self) {
        self.inner.started.store(true, Ordering::SeqCst);
        let _ = self.inner.events.send(SessionEvent::Connected);
    }

    /// Marks the mock session stopped and emits `Disconnected`.
    pub async fn stop(&self) {
        self.inner.started.store(false, Ordering::SeqCst);
        let _ = self.inner.events.send(SessionEvent::Disconnected);
    }

    /// Subscribes to mock session events.
    pub fn events(&self) -> broadcast::Receiver<SessionEvent> {
        self.inner.events.subscribe()
    }

    /// Waits for the next session event.
    pub async fn next_event(&self) -> Result<SessionEvent> {
        self.events()
            .recv()
            .await
            .map_err(|_| Error::RequestCanceled)
    }

    /// Injects a session event into subscribers.
    pub async fn push_session_event(&self, event: SessionEvent) {
        let _ = self.inner.events.send(event);
    }

    /// Opens or looks up a mock queue.
    pub async fn open_queue(
        &self,
        uri: impl AsRef<str>,
        options: QueueOptions,
    ) -> Result<MockQueue> {
        if !self.is_started() {
            return Err(Error::WriterClosed);
        }
        let uri = Uri::parse(uri.as_ref())?;
        if let Some(existing) = self.get_queue(uri.as_str()).await {
            return Ok(existing);
        }
        let queue_id = self.inner.next_queue_id.fetch_add(1, Ordering::Relaxed);
        let (events, _) = broadcast::channel(256);
        let state = Arc::new(MockQueueState {
            uri: uri.clone(),
            queue_id,
            options: Mutex::new(options.clone()),
            events,
            closed: AtomicBool::new(false),
        });
        self.inner
            .queues
            .lock()
            .await
            .insert(queue_id, state.clone());
        self.inner
            .queue_uris
            .lock()
            .await
            .insert(uri.to_string(), queue_id);
        self.inner.opens.lock().await.push(RecordedOpenQueue {
            uri: uri.clone(),
            options,
            queue_id,
        });
        let _ = self.inner.events.send(SessionEvent::QueueOpened {
            uri: uri.clone(),
            queue_id,
        });
        let _ = state.events.send(QueueEvent::Opened { queue_id });
        Ok(MockQueue {
            session: self.clone(),
            state,
        })
    }

    /// Looks up an open mock queue by URI.
    pub async fn get_queue(&self, uri: impl AsRef<str>) -> Option<MockQueue> {
        let uri = Uri::parse(uri.as_ref()).ok()?;
        let queue_id = self
            .inner
            .queue_uris
            .lock()
            .await
            .get(uri.as_str())
            .copied()?;
        let state = self.inner.queues.lock().await.get(&queue_id).cloned()?;
        Some(MockQueue {
            session: self.clone(),
            state,
        })
    }

    /// Looks up an open mock queue id by URI.
    pub async fn get_queue_id(&self, uri: impl AsRef<str>) -> Option<u32> {
        self.get_queue(uri).await.map(|queue| queue.queue_id())
    }

    /// Returns all recorded open calls.
    pub async fn recorded_opens(&self) -> Vec<RecordedOpenQueue> {
        self.inner.opens.lock().await.clone()
    }

    /// Returns all recorded reconfigure calls.
    pub async fn recorded_reconfigures(&self) -> Vec<RecordedReconfigureQueue> {
        self.inner.reconfigures.lock().await.clone()
    }

    /// Returns all recorded close calls.
    pub async fn recorded_closes(&self) -> Vec<RecordedCloseQueue> {
        self.inner.closes.lock().await.clone()
    }

    /// Returns all recorded post calls.
    pub async fn recorded_posts(&self) -> Vec<RecordedPost> {
        self.inner.posts.lock().await.clone()
    }

    /// Returns all recorded confirm calls.
    pub async fn recorded_confirms(&self) -> Vec<RecordedConfirm> {
        self.inner.confirms.lock().await.clone()
    }
}

impl Default for MockSession {
    fn default() -> Self {
        Self::new()
    }
}

impl MockQueue {
    /// Returns the queue URI.
    pub fn uri(&self) -> &Uri {
        &self.state.uri
    }

    /// Returns the queue id.
    pub fn queue_id(&self) -> u32 {
        self.state.queue_id
    }

    /// Subscribes to queue-local mock events.
    pub fn events(&self) -> broadcast::Receiver<QueueEvent> {
        self.state.events.subscribe()
    }

    /// Waits for the next queue event.
    pub async fn next_event(&self) -> Result<QueueEvent> {
        self.events()
            .recv()
            .await
            .map_err(|_| Error::RequestCanceled)
    }

    /// Injects a queue event into subscribers.
    pub async fn push_event(&self, event: QueueEvent) {
        let _ = self.state.events.send(event);
    }

    /// Records a single post call.
    pub async fn post(&self, message: PostMessage) -> Result<()> {
        let mut batch = PostBatch::new();
        batch.push(message);
        self.post_batch(batch).await
    }

    /// Records a batched post call.
    pub async fn post_batch(&self, batch: PostBatch) -> Result<()> {
        if self.state.closed.load(Ordering::SeqCst) {
            return Err(Error::WriterClosed);
        }
        self.session.inner.posts.lock().await.push(RecordedPost {
            uri: self.state.uri.clone(),
            queue_id: self.state.queue_id,
            messages: batch.into_messages(),
        });
        Ok(())
    }

    /// Records a single confirmation.
    pub async fn confirm(&self, message_guid: MessageGuid, sub_queue_id: u32) -> Result<()> {
        let mut batch = ConfirmBatch::new();
        batch.push(message_guid, sub_queue_id);
        self.confirm_batch(batch).await
    }

    /// Records a confirmation derived from a cookie.
    pub async fn confirm_cookie(&self, cookie: MessageConfirmationCookie) -> Result<()> {
        self.confirm(cookie.message_guid, cookie.sub_queue_id).await
    }

    /// Records a batched confirm call.
    pub async fn confirm_batch(&self, batch: ConfirmBatch) -> Result<()> {
        if self.state.closed.load(Ordering::SeqCst) {
            return Err(Error::WriterClosed);
        }
        self.session
            .inner
            .confirms
            .lock()
            .await
            .push(RecordedConfirm {
                uri: self.state.uri.clone(),
                queue_id: self.state.queue_id,
                messages: batch.into_messages(),
            });
        Ok(())
    }

    /// Injects an acknowledgement event into queue subscribers.
    pub async fn acknowledge(&self, ack: Acknowledgement) {
        let _ = self.state.events.send(QueueEvent::Ack(ack));
    }

    /// Records a queue reconfiguration and updates stored options.
    pub async fn reconfigure(&self, options: QueueOptions) -> Result<ConfigureQueueStatus> {
        *self.state.options.lock().await = options.clone();
        self.session
            .inner
            .reconfigures
            .lock()
            .await
            .push(RecordedReconfigureQueue {
                uri: self.state.uri.clone(),
                options,
                queue_id: self.state.queue_id,
            });
        Ok(ConfigureQueueStatus {
            uri: self.state.uri.clone(),
            queue_id: self.state.queue_id,
            suspended: false,
        })
    }

    /// Records a queue close and removes it from session lookups.
    pub async fn close(&self) -> Result<CloseQueueStatus> {
        self.state.closed.store(true, Ordering::SeqCst);
        self.session
            .inner
            .queues
            .lock()
            .await
            .remove(&self.state.queue_id);
        self.session
            .inner
            .queue_uris
            .lock()
            .await
            .remove(self.state.uri.as_str());
        self.session
            .inner
            .closes
            .lock()
            .await
            .push(RecordedCloseQueue {
                uri: self.state.uri.clone(),
                queue_id: self.state.queue_id,
            });
        let _ = self.state.events.send(QueueEvent::Closed);
        let _ = self.session.inner.events.send(SessionEvent::QueueClosed {
            uri: self.state.uri.clone(),
        });
        Ok(CloseQueueStatus {
            uri: self.state.uri.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_session_records_posts_and_confirms() {
        let session = MockSession::new();
        let queue = session
            .open_queue(
                "bmq://bmq.test.mem.priority/mock",
                QueueOptions::read_write(),
            )
            .await
            .unwrap();
        queue.post(PostMessage::new("hello")).await.unwrap();
        queue.confirm(MessageGuid([7; 16]), 0).await.unwrap();

        assert_eq!(session.recorded_posts().await.len(), 1);
        assert_eq!(session.recorded_confirms().await.len(), 1);
        assert_eq!(
            session
                .get_queue_id("bmq://bmq.test.mem.priority/mock")
                .await,
            Some(queue.queue_id())
        );
    }
}
