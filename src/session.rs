//! High-level async session and queue APIs.
//!
//! This module is the crate's "SDK layer".  It sits above
//! [`crate::client::Client`] and assembles the protocol pieces into the
//! behavior application code usually expects from a BlazingMQ client library:
//!
//! - automatic reconnect after transport loss
//! - queue reopen and reconfigure on reconnect
//! - queue-local event streams built from transport-level `PUSH` and `ACK`
//!   events
//! - host-health-driven queue suspension and resumption
//! - message confirmation helpers and batching
//! - optional operation tracing and distributed trace integration
//!
//! The design follows the intent of the official
//! [High Availability in Client Libraries](https://bloomberg.github.io/blazingmq/docs/architecture/high_availability_sdk/)
//! and
//! [Client/Broker Protocol](https://bloomberg.github.io/blazingmq/docs/architecture/client_broker_protocol/)
//! documentation, while exposing an async Rust interface instead of the
//! callback- and event-handler-heavy style used in the C++ and Java SDKs.

use crate::client::{
    Client, InboundSchemaEvent, OutboundPut, QueueHandle, SessionEvent as TransportEvent,
};
use crate::error::{Error, Result};
use crate::schema::{AuthenticationRequest, AuthenticationResponse};
use crate::types::{
    ConfirmBatch, ConfirmMessage, CorrelationId, DistributedTraceScope, HostHealthState,
    MessageConfirmationCookie, PackedPostBatch, PostBatch, PostMessage, QueueOptions,
    SessionOptions, TraceBaggage, TraceOperation, TraceOperationKind, Uri,
};
use crate::wire::{AckMessage, MessageGuid, MessageGuidGenerator, PushMessage};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::{Mutex, broadcast, mpsc, watch};
use tokio::time::{sleep, timeout};
use tracing::info;

/// Session-level events emitted by [`Session`].
#[derive(Debug, Clone)]
pub enum SessionEvent {
    /// Transport negotiation completed and the session is connected.
    Connected,
    /// Authentication completed successfully.
    Authenticated,
    /// The session is retrying after a transport failure.
    Reconnecting {
        /// Reconnect attempt number starting at 1.
        attempt: u32,
        /// Error that triggered the reconnect cycle.
        error: String,
    },
    /// A reconnect completed and queue state was restored.
    Reconnected,
    /// The session disconnected.
    Disconnected,
    /// Transport-level error surfaced from the underlying client.
    TransportError(String),
    /// Host health changed.
    HostHealthChanged(HostHealthState),
    /// A queue was opened.
    QueueOpened {
        /// Queue URI.
        uri: Uri,
        /// Queue id assigned by the transport layer.
        queue_id: u32,
    },
    /// A queue was reopened during reconnect.
    QueueReopened {
        /// Queue URI.
        uri: Uri,
        /// Queue id assigned after reconnect.
        queue_id: u32,
    },
    /// A queue was closed.
    QueueClosed {
        /// Queue URI.
        uri: Uri,
    },
    /// A queue suspended because the host became unhealthy.
    QueueSuspended {
        /// Queue URI.
        uri: Uri,
    },
    /// A queue resumed after host health was restored.
    QueueResumed {
        /// Queue URI.
        uri: Uri,
    },
    /// Event dispatch backlog crossed the configured high watermark.
    SlowConsumerHighWatermark {
        /// Number of pending events when the watermark was crossed.
        pending: usize,
    },
    /// Event dispatch backlog fell back below the low watermark.
    SlowConsumerNormal {
        /// Number of pending events when the queue returned to normal.
        pending: usize,
    },
    /// Raw schema event from the underlying client.
    Schema(InboundSchemaEvent),
}

/// Result returned after a successful queue open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenQueueStatus {
    /// Queue URI that was opened.
    pub uri: Uri,
    /// Queue id assigned by the transport layer.
    pub queue_id: u32,
}

/// Result returned after a successful queue reconfiguration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigureQueueStatus {
    /// Queue URI that was reconfigured.
    pub uri: Uri,
    /// Queue id currently associated with the queue.
    pub queue_id: u32,
    /// Whether the queue remains suspended after reconfiguration.
    pub suspended: bool,
}

/// Result returned after a successful queue close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseQueueStatus {
    /// Queue URI that was closed.
    pub uri: Uri,
}

/// High-level view of a pushed message routed to a queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMessage {
    /// Queue id that received the message.
    pub queue_id: u32,
    /// Primary sub-queue id associated with the message.
    pub sub_queue_id: u32,
    /// Redelivery-attempt metadata carried by the broker.
    pub rda_info: crate::wire::RdaInfo,
    /// Globally unique message identifier.
    pub message_guid: MessageGuid,
    /// Application payload.
    pub payload: bytes::Bytes,
    /// Message properties delivered with the payload.
    pub properties: crate::wire::MessageProperties,
    /// Whether the properties used the legacy encoded representation.
    pub properties_are_old_style: bool,
    /// All advertised sub-queue infos for the message.
    pub sub_queue_infos: Vec<crate::wire::SubQueueInfo>,
    /// Optional message group id.
    pub message_group_id: Option<String>,
    /// Compression applied to the payload.
    pub compression: crate::wire::CompressionAlgorithm,
    /// Raw push-header flags.
    pub flags: u8,
    /// Schema wire id attached to the payload.
    pub schema_wire_id: i16,
}

impl From<PushMessage> for ReceivedMessage {
    fn from(value: PushMessage) -> Self {
        let primary = value.sub_queue_infos.first().copied().unwrap_or_default();
        Self {
            queue_id: value.header.queue_id,
            sub_queue_id: primary.sub_queue_id,
            rda_info: primary.rda_info,
            message_guid: value.header.message_guid,
            payload: value.payload,
            properties: value.properties,
            properties_are_old_style: value.properties_are_old_style,
            sub_queue_infos: value.sub_queue_infos,
            message_group_id: value.message_group_id,
            compression: value.header.compression,
            flags: value.header.flags,
            schema_wire_id: value.header.schema_wire_id,
        }
    }
}

impl ReceivedMessage {
    /// Returns a cookie suitable for later confirmation.
    pub fn confirmation_cookie(&self) -> MessageConfirmationCookie {
        MessageConfirmationCookie::new(self.queue_id, self.message_guid, self.sub_queue_id)
    }
}

/// Producer acknowledgement routed to a queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acknowledgement {
    /// Queue id acknowledged by the broker.
    pub queue_id: u32,
    /// Raw acknowledgement status code.
    pub status: u8,
    /// Correlation id associated with the original post, if any.
    pub correlation_id: CorrelationId,
    /// GUID associated with the original post.
    pub message_guid: MessageGuid,
}

impl From<AckMessage> for Acknowledgement {
    fn from(value: AckMessage) -> Self {
        Self {
            queue_id: value.queue_id,
            status: value.status,
            correlation_id: CorrelationId::new(value.correlation_id.into()),
            message_guid: value.message_guid,
        }
    }
}

/// Queue-local events emitted by [`Queue`].
#[derive(Debug, Clone)]
pub enum QueueEvent {
    /// Queue was opened.
    Opened {
        /// Queue id assigned by the transport layer.
        queue_id: u32,
    },
    /// Queue was reopened during reconnect.
    Reopened {
        /// Queue id assigned after reconnect.
        queue_id: u32,
    },
    /// Queue was closed.
    Closed,
    /// Queue suspended because the host became unhealthy.
    Suspended,
    /// Queue resumed after host health was restored.
    Resumed,
    /// Message pushed to the queue.
    Message(ReceivedMessage),
    /// Acknowledgement for a posted message.
    Ack(Acknowledgement),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Connecting,
    Connected,
    Reconnecting,
    Closed,
}

/// High-level reconnecting session API.
///
/// [`Session`] is the main entry point for application code that wants a
/// "real" client rather than a raw protocol connection.  A session owns the
/// underlying transport, manages reconnect attempts, reopens queues after
/// reconnect, restores flow-control and subscription configuration, routes
/// incoming transport events to the relevant queues, and surfaces lifecycle
/// transitions through [`SessionEvent`].
///
/// In terms of the upstream protocol docs, a session coordinates:
///
/// - transport startup and negotiation
/// - optional authentication
/// - the two-step queue open / configure workflow
/// - data-plane posting, pushing, acknowledgement, and confirmation
/// - graceful disconnect
///
/// Most applications should create one session per broker connection target,
/// then open one or more [`Queue`] handles from it.
#[derive(Clone)]
pub struct Session {
    inner: Arc<SessionInner>,
}

/// Queue handle tied to a [`Session`].
///
/// A [`Queue`] remembers the options it was opened with and is therefore able
/// to participate in reconnect-driven restoration.  It exposes a focused API
/// for the producer and consumer flows described in the protocol docs:
///
/// - post messages or post batches
/// - receive pushed messages or queue events
/// - confirm delivered messages
/// - reconfigure flow-control or subscription settings
/// - close the queue
#[derive(Clone)]
pub struct Queue {
    session: Session,
    state: Arc<QueueState>,
}

/// Builder for batching post requests on a queue.
///
/// This mirrors the "message event builder" idea from the C++ and Java SDKs,
/// but in a cheaper Rust form: the builder is just an owned batch that can be
/// posted immediately or packed for later replay.
#[derive(Clone)]
pub struct PutBuilder {
    queue: Queue,
    batch: PostBatch,
}

/// Builder for batching confirmation requests on a queue.
///
/// Consumer applications typically accumulate one or more confirmations after
/// processing messages and then send them together.  This helper keeps that
/// workflow explicit without exposing wire-level confirm event details.
#[derive(Clone)]
pub struct ConfirmBuilder {
    queue: Queue,
    batch: ConfirmBatch,
}

struct SessionInner {
    options: SessionOptions,
    client: Mutex<Option<Client>>,
    guid_generator: MessageGuidGenerator,
    queues: Mutex<HashMap<u64, Arc<QueueState>>>,
    queue_ids: Mutex<HashMap<u32, u64>>,
    queue_uris: Mutex<HashMap<String, u64>>,
    next_queue_key: AtomicU64,
    host_unhealthy: AtomicBool,
    reconnecting: AtomicBool,
    closed: AtomicBool,
    started: AtomicBool,
    events: broadcast::Sender<SessionEvent>,
    event_tx: mpsc::UnboundedSender<SessionEvent>,
    connection: watch::Sender<ConnectionState>,
    message_dumping: Mutex<MessageDumpState>,
    watermark_monitor: Arc<EventWatermarkMonitor>,
}

struct QueueState {
    key: u64,
    uri: Uri,
    options: Mutex<QueueOptions>,
    handle: Mutex<Option<QueueHandle>>,
    pending_posts: Mutex<VecDeque<PostMessage>>,
    last_queue_id: AtomicU32,
    suspended: AtomicBool,
    closed: AtomicBool,
    events: broadcast::Sender<QueueEvent>,
    event_tx: mpsc::UnboundedSender<QueueEvent>,
    watermark_monitor: Arc<EventWatermarkMonitor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MessageDumpState {
    Off,
    On,
    Remaining(usize),
    Until(tokio::time::Instant),
}

struct EventWatermarkMonitor {
    pending: AtomicUsize,
    in_high_watermark: AtomicBool,
    low_watermark: usize,
    high_watermark: usize,
    session_events: broadcast::Sender<SessionEvent>,
}

impl EventWatermarkMonitor {
    fn new(
        low_watermark: usize,
        high_watermark: usize,
        session_events: broadcast::Sender<SessionEvent>,
    ) -> Self {
        Self {
            pending: AtomicUsize::new(0),
            in_high_watermark: AtomicBool::new(false),
            low_watermark,
            high_watermark: high_watermark.max(1),
            session_events,
        }
    }

    fn on_enqueue(&self) {
        let pending = self.pending.fetch_add(1, Ordering::SeqCst) + 1;
        if pending < self.high_watermark {
            return;
        }
        if !self.in_high_watermark.swap(true, Ordering::SeqCst) {
            let _ = self
                .session_events
                .send(SessionEvent::SlowConsumerHighWatermark { pending });
        }
    }

    fn on_dequeue(&self) {
        let previous = self.pending.fetch_sub(1, Ordering::SeqCst);
        let pending = previous.saturating_sub(1);
        if !self.in_high_watermark.load(Ordering::SeqCst) || pending > self.low_watermark {
            return;
        }
        if self.in_high_watermark.swap(false, Ordering::SeqCst) {
            let _ = self
                .session_events
                .send(SessionEvent::SlowConsumerNormal { pending });
        }
    }
}

impl Session {
    /// Alias for [`Session::connect`].
    ///
    /// This exists for parity with other BlazingMQ SDKs, where "start" is the
    /// common name for session establishment.
    pub async fn start(options: SessionOptions) -> Result<Self> {
        Self::connect(options).await
    }

    /// Connects to the broker and starts the reconnecting session runtime.
    ///
    /// This performs the initial TCP connect, negotiation, optional
    /// authentication, event-dispatch setup, host-health subscription, and
    /// periodic stats tasks described by [`SessionOptions`].  On success the
    /// returned session is ready for queue opens and will continue handling
    /// reconnects internally until [`Session::disconnect`] is called.
    ///
    /// Unlike [`crate::client::Client::connect`], this method is not merely a
    /// transport bootstrap.  It creates the long-lived runtime needed for a
    /// client library that behaves like the official SDKs.
    pub async fn connect(options: SessionOptions) -> Result<Self> {
        let event_capacity = options.event_queue_capacity();
        let guid_generator = options.to_client_config().message_guid_generator();
        let (events, _) = broadcast::channel(event_capacity);
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let watermark_monitor = Arc::new(EventWatermarkMonitor::new(
            options.event_queue_low_watermark,
            options.event_queue_high_watermark,
            events.clone(),
        ));
        spawn_session_event_dispatcher(event_rx, events.clone(), watermark_monitor.clone());
        let (connection, _) = watch::channel(ConnectionState::Connecting);
        let inner = Arc::new(SessionInner {
            options,
            client: Mutex::new(None),
            guid_generator,
            queues: Mutex::new(HashMap::new()),
            queue_ids: Mutex::new(HashMap::new()),
            queue_uris: Mutex::new(HashMap::new()),
            next_queue_key: AtomicU64::new(1),
            host_unhealthy: AtomicBool::new(false),
            reconnecting: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            started: AtomicBool::new(false),
            events,
            event_tx,
            connection,
            message_dumping: Mutex::new(MessageDumpState::Off),
            watermark_monitor,
        });

        let client = inner.establish_client().await?;
        inner.finish_connect(client, false).await?;
        inner.spawn_host_health_monitor();
        inner.spawn_stats_dumper();

        Ok(Self { inner })
    }

    /// Returns `true` when the session currently has an active transport.
    ///
    /// A `false` value usually means either the session has not connected yet,
    /// is in the middle of reconnecting, or has been explicitly disconnected.
    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::SeqCst)
    }

    /// Subscribes to session-level events.
    ///
    /// The returned receiver observes lifecycle, queue, host-health, and
    /// schema events for the whole session.  Use this when application logic
    /// wants a central stream of operational state changes instead of
    /// per-queue handling.
    pub fn events(&self) -> broadcast::Receiver<SessionEvent> {
        self.inner.events.subscribe()
    }

    /// Spawns a task that consumes session events until the channel closes.
    ///
    /// This is a convenience wrapper around [`Session::events`] for the common
    /// "drive a background event loop" style.  Lagged broadcast messages are
    /// skipped so a slow handler does not permanently wedge the session.
    pub fn spawn_event_handler<F, Fut>(&self, mut handler: F) -> tokio::task::JoinHandle<()>
    where
        F: FnMut(SessionEvent) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut events = self.events();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => handler(event).await,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        })
    }

    /// Returns the next locally generated message GUID.
    ///
    /// Applications rarely need this because [`Queue::post`], [`Queue::post_batch`],
    /// and related helpers generate GUIDs automatically when none are supplied.
    /// It is exposed for interoperability with external state tracking or
    /// custom publish pipelines.
    pub fn next_message_guid(&self) -> MessageGuid {
        self.inner.guid_generator.next()
    }

    /// Waits for the next session event.
    ///
    /// This is useful for synchronous-style integration tests or small tools
    /// that prefer to "pull" session events one at a time instead of managing
    /// a spawned event task.
    pub async fn next_event(&self) -> Result<SessionEvent> {
        let mut events = self.events();
        timeout(self.inner.options.request_timeout, events.recv())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::RequestCanceled)
    }

    /// Sends an anonymous authentication request and emits `Authenticated`.
    ///
    /// Use this when the broker expects the standard `ANONYMOUS` mechanism but
    /// authentication is not being driven automatically through
    /// [`SessionOptions::authenticate_anonymous`] or
    /// [`SessionOptions::auth_provider`].
    pub async fn authenticate_anonymous(&self) -> Result<()> {
        self.authenticate(AuthenticationRequest {
            mechanism: "ANONYMOUS".to_string(),
            data: None,
        })
        .await
    }

    /// Sends an explicit authentication request and emits `Authenticated`.
    ///
    /// This is the manual authentication entry point.  It is primarily useful
    /// when applications need to choose a mechanism dynamically or when they
    /// want to defer authentication until after the session has already been
    /// established.
    pub async fn authenticate(&self, request: AuthenticationRequest) -> Result<()> {
        let client = self.inner.wait_for_client().await?;
        self.inner
            .with_trace(TraceOperationKind::Authenticate, None, async {
                client.authenticate(request).await.map(|_| ())
            })
            .await?;
        self.inner.emit_session(SessionEvent::Authenticated);
        Ok(())
    }

    /// Sends an explicit authentication request and returns the full response.
    ///
    /// Compared with [`Session::authenticate`], this keeps broker-returned
    /// metadata such as `lifetime_ms`.  Use it when the authentication layer
    /// needs that extra information for token refresh or lease management.
    pub async fn authenticate_with_response(
        &self,
        request: AuthenticationRequest,
    ) -> Result<AuthenticationResponse> {
        let client = self.inner.wait_for_client().await?;
        let response = self
            .inner
            .with_trace(
                TraceOperationKind::Authenticate,
                None,
                client.authenticate(request),
            )
            .await?;
        self.inner.emit_session(SessionEvent::Authenticated);
        Ok(response)
    }

    /// Executes an administrative command.
    ///
    /// Admin commands are control-plane operations interpreted directly by the
    /// broker.  They are useful for diagnostics and broker introspection, not
    /// for normal publish/consume traffic.
    pub async fn admin_command(&self, command: impl Into<String>) -> Result<String> {
        let client = self.inner.wait_for_client().await?;
        self.inner
            .with_trace(
                TraceOperationKind::AdminCommand,
                None,
                client.admin_command(command),
            )
            .await
    }

    /// Opens a queue and returns the queue handle.
    ///
    /// The supplied [`QueueOptions`] determine whether the queue behaves as a
    /// producer, consumer, or mixed handle, and they also supply the flow
    /// control, subscription, and host-health semantics that will be restored
    /// after reconnect.
    pub async fn open_queue(&self, uri: impl AsRef<str>, options: QueueOptions) -> Result<Queue> {
        Ok(self.open_queue_with_status(uri, options).await?.0)
    }

    /// Opens a queue and returns both the handle and status metadata.
    ///
    /// This method performs the full protocol-defined queue attach sequence:
    ///
    /// 1. send `OpenQueue`
    /// 2. send `ConfigureQueueStream`
    /// 3. optionally send `ConfigureStream` when application id or
    ///    subscriptions are configured
    ///
    /// The resulting [`Queue`] is then registered with the session so that
    /// reconnect, host-health transitions, and inbound message routing can all
    /// operate on it automatically.
    pub async fn open_queue_with_status(
        &self,
        uri: impl AsRef<str>,
        options: QueueOptions,
    ) -> Result<(Queue, OpenQueueStatus)> {
        let uri = Uri::parse(uri.as_ref())?;
        let handle = self.inner.open_queue_with_retry(&uri, &options).await?;
        let (events, _) = broadcast::channel(self.inner.options.event_queue_capacity());
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        spawn_queue_event_dispatcher(
            event_rx,
            events.clone(),
            self.inner.watermark_monitor.clone(),
        );
        let state = Arc::new(QueueState {
            key: self.inner.next_queue_key.fetch_add(1, Ordering::Relaxed),
            uri: uri.clone(),
            options: Mutex::new(options),
            handle: Mutex::new(Some(handle.clone())),
            pending_posts: Mutex::new(VecDeque::new()),
            last_queue_id: AtomicU32::new(handle.queue_id),
            suspended: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            events,
            event_tx,
            watermark_monitor: self.inner.watermark_monitor.clone(),
        });

        {
            let mut queues = self.inner.queues.lock().await;
            queues.insert(state.key, state.clone());
        }
        {
            let mut queue_ids = self.inner.queue_ids.lock().await;
            queue_ids.insert(handle.queue_id, state.key);
        }
        self.inner
            .queue_uris
            .lock()
            .await
            .insert(uri.to_string(), state.key);

        if self.inner.is_host_unhealthy() && state.should_suspend_on_bad_host_health().await {
            self.inner.suspend_queue_for_host_health(&state).await?;
        }

        state.enqueue_event(QueueEvent::Opened {
            queue_id: handle.queue_id,
        });
        self.inner.emit_session(SessionEvent::QueueOpened {
            uri: uri.clone(),
            queue_id: handle.queue_id,
        });

        let queue = Queue {
            session: self.clone(),
            state,
        };
        Ok((
            queue,
            OpenQueueStatus {
                uri,
                queue_id: handle.queue_id,
            },
        ))
    }

    /// Looks up an open queue id by URI.
    ///
    /// This only succeeds for queues currently tracked by the session.  It
    /// does not query the broker.
    pub async fn get_queue_id(&self, uri: impl AsRef<str>) -> Option<u32> {
        self.get_queue(uri).await.and_then(|queue| queue.queue_id())
    }

    /// Looks up an open queue by URI.
    ///
    /// The lookup uses the session's local registry of open queues and returns
    /// a cheap clone of the queue handle when found.
    pub async fn get_queue(&self, uri: impl AsRef<str>) -> Option<Queue> {
        let uri = Uri::parse(uri.as_ref()).ok()?;
        let queue_key = self
            .inner
            .queue_uris
            .lock()
            .await
            .get(uri.as_str())
            .copied()?;
        let state = self.inner.queues.lock().await.get(&queue_key).cloned()?;
        Some(Queue {
            session: self.clone(),
            state,
        })
    }

    /// Looks up an open queue by queue id.
    ///
    /// This is mainly useful when application state stores broker-facing queue
    /// ids or when processing confirmation cookies.
    pub async fn get_queue_by_id(&self, queue_id: u32) -> Option<Queue> {
        let state = self.inner.lookup_queue(queue_id).await?;
        Some(Queue {
            session: self.clone(),
            state,
        })
    }

    /// Returns an empty message-properties collection.
    ///
    /// This is a convenience constructor for code that wants parity with the
    /// "load message properties" builder style from the C++ SDK.
    pub fn load_message_properties(&self) -> crate::wire::MessageProperties {
        crate::wire::MessageProperties::default()
    }

    /// Creates a put builder for the supplied queue.
    ///
    /// Equivalent to calling [`Queue::put_builder`] directly.
    pub fn load_put_builder(&self, queue: &Queue) -> PutBuilder {
        queue.put_builder()
    }

    /// Creates a confirm builder for the supplied queue.
    ///
    /// Equivalent to calling [`Queue::confirm_builder`] directly.
    pub fn load_confirm_builder(&self, queue: &Queue) -> ConfirmBuilder {
        queue.confirm_builder()
    }

    /// Confirms a message identified by a previously captured cookie.
    ///
    /// Cookies are the safest way to confirm consumed messages because they
    /// preserve the exact queue id, GUID, and sub-queue id tuple the broker
    /// expects for the `CONFIRM` operation.
    pub async fn confirm_message(&self, cookie: MessageConfirmationCookie) -> Result<()> {
        let queue = self.get_queue_by_id(cookie.queue_id).await.ok_or_else(|| {
            Error::InvalidConfiguration("unknown queue id in confirmation cookie".to_string())
        })?;
        queue
            .confirm(cookie.message_guid, cookie.sub_queue_id)
            .await
    }

    /// Configures debug dumping of inbound and outbound protocol messages.
    ///
    /// Accepted values are `"on"`, `"off"`, a positive count such as `"10"`,
    /// or a positive duration in seconds such as `"5s"`.
    pub async fn configure_message_dumping(&self, command: impl AsRef<str>) -> Result<()> {
        let state = parse_message_dump_command(command.as_ref())?;
        *self.inner.message_dumping.lock().await = state;
        Ok(())
    }

    /// Alias for [`Session::disconnect`].
    ///
    /// The naming mirrors the official SDKs, where "stop" is the normal way to
    /// terminate a session.
    pub async fn stop(&self) -> Result<()> {
        self.disconnect().await
    }

    /// Attempts a graceful disconnect within `linger_timeout`.
    ///
    /// This is a bounded graceful shutdown: the session will try to send the
    /// broker `Disconnect` request and clear local state, but the caller will
    /// regain control once `linger_timeout` elapses even if the transport does
    /// not complete cleanly.
    pub async fn linger(&self) -> Result<()> {
        timeout(self.inner.options.linger_timeout, self.disconnect())
            .await
            .map_err(|_| Error::Timeout)?
    }

    /// Disconnects the session and clears queue handles.
    ///
    /// After this returns, the session no longer participates in reconnect and
    /// all tracked queues are detached from their underlying transport handles.
    /// Existing [`Queue`] values become inert wrappers around closed state and
    /// must not be treated as live broker attachments.
    pub async fn disconnect(&self) -> Result<()> {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.started.store(false, Ordering::SeqCst);
        let _ = self.inner.connection.send(ConnectionState::Closed);
        let client = self.inner.client.lock().await.take();

        {
            let queues = self.inner.queue_snapshot().await;
            self.inner.queue_ids.lock().await.clear();
            self.inner.queue_uris.lock().await.clear();
            for queue in queues {
                queue.handle.lock().await.take();
            }
        }

        if let Some(client) = client {
            let _ = self
                .inner
                .with_trace(TraceOperationKind::Disconnect, None, client.disconnect())
                .await;
        }
        self.inner.emit_session(SessionEvent::Disconnected);
        Ok(())
    }
}

impl Queue {
    /// Returns the queue URI.
    ///
    /// This is the stable application-facing identifier for the queue.  Queue
    /// ids may change across reconnect, but the URI does not.
    pub fn uri(&self) -> &Uri {
        &self.state.uri
    }

    /// Returns the current queue id, if the queue is presently attached.
    ///
    /// Queue ids are transport-local identifiers chosen by the SDK when
    /// opening a queue.  They are useful for protocol correlation and may be
    /// reassigned when the queue is reopened after reconnect.
    pub fn queue_id(&self) -> Option<u32> {
        let value = self.state.last_queue_id.load(Ordering::Relaxed);
        (value != 0).then_some(value)
    }

    /// Subscribes to queue-local events.
    ///
    /// Queue events are derived from session transport events after the session
    /// has already filtered them for the relevant queue id.
    pub fn events(&self) -> broadcast::Receiver<QueueEvent> {
        self.state.events.subscribe()
    }

    /// Spawns a task that consumes queue events until the channel closes.
    ///
    /// This is the easiest way to attach per-queue logic in a long-running
    /// consumer or producer process.
    pub fn spawn_event_handler<F, Fut>(&self, mut handler: F) -> tokio::task::JoinHandle<()>
    where
        F: FnMut(QueueEvent) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut events = self.events();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => handler(event).await,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        })
    }

    /// Waits for the next queue event.
    ///
    /// This helper is intended for pull-style workflows and tests.  Long-lived
    /// applications usually prefer [`Queue::events`] or
    /// [`Queue::spawn_event_handler`].
    pub async fn next_event(&self) -> Result<QueueEvent> {
        let mut events = self.events();
        timeout(self.session.inner.options.request_timeout, events.recv())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::RequestCanceled)
    }

    /// Waits for the next pushed message on the queue.
    ///
    /// Only `Message` queue events are returned; lifecycle events such as
    /// reopen, suspend, or resume are skipped internally.
    pub async fn next_message(&self) -> Result<ReceivedMessage> {
        let mut events = self.events();
        loop {
            let event = timeout(self.session.inner.options.request_timeout, events.recv())
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::RequestCanceled)?;
            if let QueueEvent::Message(message) = event {
                return Ok(message);
            }
        }
    }

    /// Waits for the next producer acknowledgement on the queue.
    ///
    /// This is mainly useful on producer queues opened with the ACK flag set.
    /// Other queue event kinds are skipped internally until an acknowledgement
    /// arrives or the request timeout elapses.
    pub async fn next_ack(&self) -> Result<Acknowledgement> {
        let mut events = self.events();
        loop {
            let event = timeout(self.session.inner.options.request_timeout, events.recv())
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::RequestCanceled)?;
            if let QueueEvent::Ack(ack) = event {
                return Ok(ack);
            }
        }
    }

    /// Posts a single message.
    ///
    /// This is the simplest producer entry point.  The message is validated
    /// against the queue's current state, packed into the wire format, and then
    /// written through the underlying session transport.
    pub async fn post(&self, message: PostMessage) -> Result<()> {
        let mut batch = PostBatch::new();
        batch.push(message);
        self.post_batch(batch).await
    }

    /// Posts a batch of messages.
    ///
    /// Batching is the natural way to take advantage of the binary `PUT` event
    /// format, which allows multiple messages to share a single event header.
    pub async fn post_batch(&self, batch: PostBatch) -> Result<()> {
        let packed = self.pack_batch(batch).await?;
        self.post_packed_batch(packed).await
    }

    /// Creates a builder for batched post requests.
    ///
    /// Use this when application code wants to accumulate a batch
    /// incrementally, possibly across multiple helper functions, before
    /// packing or posting it.
    pub fn put_builder(&self) -> PutBuilder {
        PutBuilder {
            queue: self.clone(),
            batch: PostBatch::new(),
        }
    }

    /// Validates and packs a single message for later posting.
    ///
    /// Packing performs the queue-state validation that would normally happen
    /// just before a post.  This is particularly useful when a producer wants
    /// to separate "prepare work while healthy" from "write to the broker when
    /// capacity is available".
    pub async fn pack_message(&self, message: PostMessage) -> Result<PackedPostBatch> {
        let mut batch = PostBatch::new();
        batch.push(message);
        self.pack_batch(batch).await
    }

    /// Validates and packs a batch for later posting.
    ///
    /// Packed batches participate in the session's reconnect behavior the same
    /// way as directly posted batches once they are eventually submitted with
    /// [`Queue::post_packed_batch`].
    pub async fn pack_batch(&self, batch: PostBatch) -> Result<PackedPostBatch> {
        self.session
            .inner
            .pack_post_batch(&self.state, batch.into_messages())
            .await
    }

    /// Posts a previously packed batch.
    ///
    /// Use this when message preparation happened earlier than the eventual
    /// write, for example after local batching or after a temporary host-health
    /// suspension has cleared.
    pub async fn post_packed_batch(&self, batch: PackedPostBatch) -> Result<()> {
        self.session
            .inner
            .post_packed_batch(&self.state, batch.into_messages())
            .await
    }

    /// Confirms a single delivered message.
    ///
    /// Confirmations tell the broker that a pushed message has been processed
    /// successfully and may be removed according to queue semantics.
    pub async fn confirm(&self, message_guid: MessageGuid, sub_queue_id: u32) -> Result<()> {
        let mut batch = ConfirmBatch::new();
        batch.push(message_guid, sub_queue_id);
        self.confirm_batch(batch).await
    }

    /// Confirms a batch of delivered messages.
    ///
    /// This is the most efficient confirmation path for consumers that process
    /// messages in groups.
    pub async fn confirm_batch(&self, batch: ConfirmBatch) -> Result<()> {
        self.session
            .inner
            .confirm_batch(&self.state, batch.into_messages())
            .await
    }

    /// Confirms a message identified by a cookie.
    ///
    /// Prefer this over manually threading queue id, GUID, and sub-queue id
    /// around your application.
    pub async fn confirm_cookie(&self, cookie: MessageConfirmationCookie) -> Result<()> {
        self.confirm(cookie.message_guid, cookie.sub_queue_id).await
    }

    /// Creates a builder for batched confirm requests.
    ///
    /// Use this when the consumer pipeline wants to accumulate confirms
    /// gradually before flushing them together.
    pub fn confirm_builder(&self) -> ConfirmBuilder {
        ConfirmBuilder {
            queue: self.clone(),
            batch: ConfirmBatch::new(),
        }
    }

    /// Reconfigures the queue in place.
    ///
    /// Reconfiguration updates the broker-facing flow-control, priority,
    /// application-id, and subscription settings tracked by this queue.
    pub async fn reconfigure(&self, options: QueueOptions) -> Result<()> {
        self.reconfigure_with_status(options).await.map(|_| ())
    }

    /// Reconfigures the queue and returns the resulting status.
    ///
    /// The returned [`ConfigureQueueStatus`] is useful when the caller needs to
    /// know whether host-health logic left the queue suspended after the
    /// reconfiguration finished.
    pub async fn reconfigure_with_status(
        &self,
        options: QueueOptions,
    ) -> Result<ConfigureQueueStatus> {
        self.session
            .inner
            .reconfigure_queue(&self.state, options)
            .await?;
        Ok(ConfigureQueueStatus {
            uri: self.state.uri.clone(),
            queue_id: self.state.queue_id().unwrap_or_default(),
            suspended: self.state.suspended.load(Ordering::SeqCst),
        })
    }

    /// Closes the queue.
    ///
    /// Closing a queue detaches the application from that broker-side handle;
    /// it does not delete the underlying queue in the cluster.
    pub async fn close(&self) -> Result<()> {
        self.close_with_status().await.map(|_| ())
    }

    /// Closes the queue and returns the resulting status.
    ///
    /// Under the hood the session follows the protocol's close workflow by
    /// first clearing stream configuration and then sending `CloseQueue`.
    pub async fn close_with_status(&self) -> Result<CloseQueueStatus> {
        self.session.inner.close_queue(&self.state).await?;
        Ok(CloseQueueStatus {
            uri: self.state.uri.clone(),
        })
    }
}

impl PutBuilder {
    /// Appends a message to the batch.
    ///
    /// The message is stored as-is; validation happens when the batch is
    /// packed or posted.
    pub fn push(&mut self, message: PostMessage) -> &mut Self {
        self.batch.push(message);
        self
    }

    /// Returns `true` when the builder contains no messages.
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Validates and packs the accumulated batch.
    ///
    /// This is the builder equivalent of [`Queue::pack_batch`].
    pub async fn pack(self) -> Result<PackedPostBatch> {
        self.queue.pack_batch(self.batch).await
    }

    /// Posts the accumulated batch.
    ///
    /// This is the builder equivalent of [`Queue::post_batch`].
    pub async fn post(self) -> Result<()> {
        self.queue.post_batch(self.batch).await
    }
}

impl ConfirmBuilder {
    /// Appends a confirmation item.
    ///
    /// The caller is responsible for providing the GUID and sub-queue id taken
    /// from the delivered message.
    pub fn push(&mut self, message_guid: MessageGuid, sub_queue_id: u32) -> &mut Self {
        self.batch.push(message_guid, sub_queue_id);
        self
    }

    /// Appends a confirmation derived from a cookie.
    ///
    /// This is usually less error-prone than unpacking the cookie manually.
    pub fn push_cookie(&mut self, cookie: MessageConfirmationCookie) -> &mut Self {
        self.batch.push_cookie(cookie);
        self
    }

    /// Sends the accumulated confirmations.
    ///
    /// This is the builder equivalent of [`Queue::confirm_batch`].
    pub async fn send(self) -> Result<()> {
        self.queue.confirm_batch(self.batch).await
    }
}

impl SessionInner {
    async fn establish_client(&self) -> Result<Client> {
        let config = self.options.to_client_config();
        let client = self
            .with_trace(TraceOperationKind::Connect, None, async {
                timeout(
                    self.options.connect_timeout,
                    Client::connect(&self.options.broker_addr, config),
                )
                .await
                .map_err(|_| Error::Timeout)?
            })
            .await?;

        self.emit_session(SessionEvent::Connected);

        if let Some(provider) = &self.options.auth_provider {
            let request = provider.authentication_request().await?;
            self.with_trace(TraceOperationKind::Authenticate, None, async {
                client.authenticate(request).await.map(|_| ())
            })
            .await?;
            self.emit_session(SessionEvent::Authenticated);
        } else if self.options.authenticate_anonymous {
            self.with_trace(TraceOperationKind::Authenticate, None, async {
                client.authenticate_anonymous().await.map(|_| ())
            })
            .await?;
            self.emit_session(SessionEvent::Authenticated);
        }

        Ok(client)
    }

    async fn finish_connect(self: &Arc<Self>, client: Client, reconnected: bool) -> Result<()> {
        if reconnected {
            self.restore_queues(&client).await?;
        }

        *self.client.lock().await = Some(client.clone());
        self.started.store(true, Ordering::SeqCst);
        let _ = self.connection.send(ConnectionState::Connected);
        if reconnected {
            self.emit_session(SessionEvent::Reconnected);
        }
        tokio::spawn(transport_loop(self.clone(), client));
        Ok(())
    }

    fn spawn_host_health_monitor(self: &Arc<Self>) {
        let Some(monitor) = self.options.host_health_monitor.as_ref() else {
            return;
        };
        let mut health = monitor.subscribe();
        let inner = self.clone();
        tokio::spawn(async move {
            let initial = *health.borrow_and_update();
            inner.handle_host_health(initial).await;
            while health.changed().await.is_ok() {
                let state = *health.borrow_and_update();
                inner.handle_host_health(state).await;
            }
        });
    }

    fn spawn_stats_dumper(self: &Arc<Self>) {
        let Some(interval) = self.options.stats_dump_interval else {
            return;
        };
        let inner = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if inner.closed.load(Ordering::SeqCst) {
                    return;
                }
                inner.dump_stats().await;
            }
        });
    }

    async fn handle_host_health(self: &Arc<Self>, state: HostHealthState) {
        self.host_unhealthy.store(
            matches!(state, HostHealthState::Unhealthy),
            Ordering::SeqCst,
        );
        self.emit_session(SessionEvent::HostHealthChanged(state));
        let unhealthy = matches!(state, HostHealthState::Unhealthy);
        let queues = self.queue_snapshot().await;
        for queue in queues {
            if !queue.should_suspend_on_bad_host_health().await {
                continue;
            }
            let result = if unhealthy {
                self.suspend_queue_for_host_health(&queue).await
            } else {
                self.resume_queue_from_host_health(&queue).await
            };
            if result.is_err() && unhealthy {
                self.start_reconnect("host health suspension reconfiguration failed".to_string());
            }
        }
    }

    fn is_host_unhealthy(&self) -> bool {
        self.host_unhealthy.load(Ordering::SeqCst)
    }

    async fn with_trace<T>(
        &self,
        kind: TraceOperationKind,
        queue: Option<&Uri>,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let operation = TraceOperation {
            kind,
            queue: queue.cloned(),
        };
        let _distributed_trace_scope = self.activate_distributed_trace(kind, queue);
        if let Some(trace) = &self.options.trace_sink {
            trace.operation_started(&operation);
        }
        match future.await {
            Ok(value) => {
                if let Some(trace) = &self.options.trace_sink {
                    trace.operation_succeeded(&operation);
                }
                Ok(value)
            }
            Err(error) => {
                if let Some(trace) = &self.options.trace_sink {
                    trace.operation_failed(&operation, &error);
                }
                Err(error)
            }
        }
    }

    async fn wait_for_client(&self) -> Result<Client> {
        let mut connection = self.connection.subscribe();
        timeout(self.options.request_timeout, async {
            loop {
                if let Some(client) = self.client.lock().await.clone() {
                    return Ok(client);
                }
                if matches!(*connection.borrow(), ConnectionState::Closed) {
                    return Err(Error::WriterClosed);
                }
                connection
                    .changed()
                    .await
                    .map_err(|_| Error::WriterClosed)?;
            }
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    async fn queue_snapshot(&self) -> Vec<Arc<QueueState>> {
        self.queues.lock().await.values().cloned().collect()
    }

    fn activate_distributed_trace(
        &self,
        kind: TraceOperationKind,
        queue: Option<&Uri>,
    ) -> Option<Box<dyn DistributedTraceScope>> {
        let context = self.options.trace_context.as_ref()?;
        let tracer = self.options.tracer.as_ref()?;
        let operation = distributed_trace_operation_name(kind);
        let baggage = distributed_trace_baggage(kind, queue);
        let span = tracer.create_child_span(context.current_span(), operation, &baggage)?;
        Some(context.activate_span(span))
    }

    async fn route_push(&self, message: PushMessage) {
        self.maybe_dump_message("inbound_push", &message).await;
        if let Some(queue) = self.lookup_queue(message.header.queue_id).await {
            queue.enqueue_event(QueueEvent::Message(message.into()));
        }
    }

    async fn route_ack(&self, ack: AckMessage) {
        self.maybe_dump_message("inbound_ack", &ack).await;
        if let Some(queue) = self.lookup_queue(ack.queue_id).await {
            queue.remove_pending_post(ack.message_guid).await;
            queue.enqueue_event(QueueEvent::Ack(ack.into()));
        }
    }

    async fn lookup_queue(&self, queue_id: u32) -> Option<Arc<QueueState>> {
        let queue_key = self.queue_ids.lock().await.get(&queue_id).copied()?;
        self.queues.lock().await.get(&queue_key).cloned()
    }

    async fn handle_transport_event(self: &Arc<Self>, event: TransportEvent) {
        match event {
            TransportEvent::Schema(message) => {
                self.maybe_dump_message("inbound_schema", &message).await;
                self.emit_session(SessionEvent::Schema(message));
            }
            TransportEvent::Ack(messages) => {
                for message in messages {
                    self.route_ack(message).await;
                }
            }
            TransportEvent::Push(messages) => {
                for message in messages {
                    self.route_push(message).await;
                }
            }
            TransportEvent::HeartbeatRequest | TransportEvent::HeartbeatResponse => {}
            TransportEvent::TransportClosed => {
                self.emit_session(SessionEvent::Disconnected);
                self.start_reconnect("transport closed".to_string());
            }
            TransportEvent::TransportError(error) => {
                self.emit_session(SessionEvent::TransportError(error.clone()));
                self.start_reconnect(error);
            }
        }
    }

    fn start_reconnect(self: &Arc<Self>, reason: String) {
        if self.closed.load(Ordering::SeqCst) || self.reconnecting.swap(true, Ordering::SeqCst) {
            return;
        }

        let inner = self.clone();
        tokio::spawn(async move {
            inner.prepare_reconnect(reason).await;
        });
    }

    async fn prepare_reconnect(self: Arc<Self>, reason: String) {
        *self.client.lock().await = None;
        self.started.store(false, Ordering::SeqCst);
        self.queue_ids.lock().await.clear();
        let queues = self.queue_snapshot().await;
        for queue in &queues {
            queue.handle.lock().await.take();
        }
        let _ = self.connection.send(ConnectionState::Reconnecting);
        self.reconnect_loop(reason).await;
    }

    async fn reconnect_loop(self: Arc<Self>, mut last_error: String) {
        let mut attempt = 1_u32;
        loop {
            if self.closed.load(Ordering::SeqCst) {
                self.reconnecting.store(false, Ordering::SeqCst);
                return;
            }

            self.emit_session(SessionEvent::Reconnecting {
                attempt,
                error: last_error.clone(),
            });

            match self.establish_client().await {
                Ok(client) => match self.finish_connect(client, true).await {
                    Ok(()) => {
                        self.reconnecting.store(false, Ordering::SeqCst);
                        return;
                    }
                    Err(error) => {
                        last_error = error.to_string();
                    }
                },
                Err(error) => {
                    last_error = error.to_string();
                }
            }

            sleep(reconnect_backoff(attempt)).await;
            attempt = attempt.saturating_add(1);
        }
    }

    async fn restore_queues(self: &Arc<Self>, client: &Client) -> Result<()> {
        let queues = self.queue_snapshot().await;
        self.queue_ids.lock().await.clear();
        self.queue_uris.lock().await.clear();

        for queue in queues {
            if queue.closed.load(Ordering::SeqCst) {
                continue;
            }
            let options = queue.options.lock().await.clone();
            let handle = self
                .with_trace(
                    TraceOperationKind::OpenQueue,
                    Some(&queue.uri),
                    client.open_queue(queue.uri.to_string(), options.to_open_queue_options()),
                )
                .await?;

            let queue_id = handle.queue_id;
            queue.last_queue_id.store(queue_id, Ordering::Relaxed);
            *queue.handle.lock().await = Some(handle);
            self.queue_ids.lock().await.insert(queue_id, queue.key);
            self.queue_uris
                .lock()
                .await
                .insert(queue.uri.to_string(), queue.key);

            queue.enqueue_event(QueueEvent::Reopened { queue_id });
            self.emit_session(SessionEvent::QueueReopened {
                uri: queue.uri.clone(),
                queue_id,
            });

            if !queue.suspended.load(Ordering::SeqCst) {
                self.flush_queue(&queue).await?;
            } else {
                self.apply_suspended_queue_stream(&queue).await?;
            }
        }
        Ok(())
    }

    async fn pack_post_batch(
        self: &Arc<Self>,
        queue: &Arc<QueueState>,
        messages: Vec<PostMessage>,
    ) -> Result<PackedPostBatch> {
        if messages.is_empty() {
            return Ok(PackedPostBatch::new());
        }
        let options = queue.options.lock().await.clone();
        validate_outbound_posts(
            &queue.uri,
            &options,
            queue.closed.load(Ordering::SeqCst),
            queue.suspended.load(Ordering::SeqCst),
            &messages,
        )?;

        let messages = messages
            .into_iter()
            .map(|mut message| {
                if message.message_guid.is_none() {
                    message.message_guid = Some(self.guid_generator.next());
                }
                message
            })
            .collect::<Vec<_>>();

        Ok(PackedPostBatch { messages })
    }

    async fn post_packed_batch(
        self: &Arc<Self>,
        queue: &Arc<QueueState>,
        messages: Vec<PostMessage>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let options = queue.options.lock().await.clone();
        validate_post_queue_state(&queue.uri, &options, queue.closed.load(Ordering::SeqCst))?;
        queue.buffer_pending_posts(&messages).await;
        self.maybe_dump_message("outbound_put", &messages).await;
        let outbound = messages
            .iter()
            .cloned()
            .map(post_to_outbound)
            .collect::<Vec<_>>();

        if let Some(handle) = queue.handle.lock().await.clone() {
            let result = self
                .with_trace(
                    TraceOperationKind::Publish,
                    Some(&queue.uri),
                    handle.publish_all(outbound),
                )
                .await;
            match result {
                Ok(()) => return Ok(()),
                Err(Error::WriterClosed) => {}
                Err(error) => return Err(error),
            }
        }

        self.start_reconnect("publish buffered while disconnected".to_string());
        Ok(())
    }

    async fn confirm_batch(
        self: &Arc<Self>,
        queue: &Arc<QueueState>,
        messages: Vec<ConfirmMessage>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        if let Some(handle) = queue.handle.lock().await.clone() {
            let wire = messages
                .iter()
                .map(|message| (message.message_guid, message.sub_queue_id))
                .collect::<Vec<_>>();
            self.maybe_dump_message("outbound_confirm", &wire).await;
            let result = self
                .with_trace(
                    TraceOperationKind::Confirm,
                    Some(&queue.uri),
                    handle.confirm_all(wire),
                )
                .await;
            match result {
                Ok(()) => return Ok(()),
                Err(Error::WriterClosed) => {
                    self.start_reconnect("confirm attempted while disconnected".to_string());
                    return Err(Error::WriterClosed);
                }
                Err(error) => return Err(error),
            }
        }

        self.start_reconnect("confirm attempted while disconnected".to_string());
        Err(Error::WriterClosed)
    }

    async fn flush_queue(self: &Arc<Self>, queue: &Arc<QueueState>) -> Result<()> {
        if queue.suspended.load(Ordering::SeqCst) {
            return Ok(());
        }
        let handle = match queue.handle.lock().await.clone() {
            Some(handle) => handle,
            None => return Ok(()),
        };

        let pending_posts = {
            let pending = queue.pending_posts.lock().await;
            pending.iter().cloned().collect::<Vec<_>>()
        };
        if !pending_posts.is_empty() {
            let outbound = pending_posts
                .into_iter()
                .map(post_to_outbound)
                .collect::<Vec<_>>();
            self.with_trace(
                TraceOperationKind::Publish,
                Some(&queue.uri),
                handle.publish_all(outbound),
            )
            .await?;
        }

        Ok(())
    }

    async fn reconfigure_queue(
        self: &Arc<Self>,
        queue: &Arc<QueueState>,
        options: QueueOptions,
    ) -> Result<()> {
        if queue.closed.load(Ordering::SeqCst) {
            return Err(Error::WriterClosed);
        }

        *queue.options.lock().await = options.clone();
        if options.suspends_on_bad_host_health && self.is_host_unhealthy() {
            self.suspend_queue_for_host_health(queue).await?;
            return Ok(());
        }

        if queue.suspended.load(Ordering::SeqCst) && !options.suspends_on_bad_host_health {
            self.resume_queue_from_host_health(queue).await?;
            return Ok(());
        }

        if queue.suspended.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.apply_queue_configuration(queue, &options).await?;
        self.flush_queue(queue).await?;
        Ok(())
    }

    async fn close_queue(&self, queue: &Arc<QueueState>) -> Result<()> {
        queue.closed.store(true, Ordering::SeqCst);
        let handle = queue.handle.lock().await.take();
        let options = queue.options.lock().await.clone();
        self.queues.lock().await.remove(&queue.key);
        self.queue_uris.lock().await.remove(queue.uri.as_str());
        if let Some(queue_id) = queue.queue_id() {
            self.queue_ids.lock().await.remove(&queue_id);
        }

        if let Some(handle) = handle {
            if let Ok(client) = self.wait_for_client().await {
                if let Some(params) = options.suspended_queue_stream_parameters() {
                    let _ = self
                        .with_trace(
                            TraceOperationKind::ConfigureQueue,
                            Some(&queue.uri),
                            client.configure_queue_stream(handle.queue_id, params),
                        )
                        .await;
                }
            }
            let _ = self
                .with_trace(
                    TraceOperationKind::CloseQueue,
                    Some(&queue.uri),
                    handle.close(true),
                )
                .await;
        }

        queue.enqueue_event(QueueEvent::Closed);
        self.emit_session(SessionEvent::QueueClosed {
            uri: queue.uri.clone(),
        });
        Ok(())
    }

    fn emit_session(&self, event: SessionEvent) {
        self.watermark_monitor.on_enqueue();
        if self.event_tx.send(event).is_err() {
            self.watermark_monitor.on_dequeue();
        }
    }

    async fn open_queue_with_retry(
        self: &Arc<Self>,
        uri: &Uri,
        options: &QueueOptions,
    ) -> Result<QueueHandle> {
        loop {
            let client = self.wait_for_client().await?;
            let result = self
                .with_trace(
                    TraceOperationKind::OpenQueue,
                    Some(uri),
                    client.open_queue(uri.to_string(), options.to_open_queue_options()),
                )
                .await;
            match result {
                Ok(handle) => return Ok(handle),
                Err(Error::WriterClosed) => {
                    self.start_reconnect("open queue interrupted by disconnect".to_string());
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn apply_queue_configuration(
        &self,
        queue: &Arc<QueueState>,
        options: &QueueOptions,
    ) -> Result<()> {
        let Some(handle) = queue.handle.lock().await.clone() else {
            return Ok(());
        };
        let client = self.wait_for_client().await?;
        if let Some(params) = options.default_queue_stream_parameters() {
            self.with_trace(
                TraceOperationKind::ConfigureQueue,
                Some(&queue.uri),
                client.configure_queue_stream(handle.queue_id, params),
            )
            .await?;
        }
        if let Some(params) = options.stream_parameters() {
            self.with_trace(
                TraceOperationKind::ConfigureQueue,
                Some(&queue.uri),
                client.configure_stream(handle.queue_id, params),
            )
            .await?;
        }
        Ok(())
    }

    async fn apply_suspended_queue_stream(&self, queue: &Arc<QueueState>) -> Result<()> {
        let Some(handle) = queue.handle.lock().await.clone() else {
            return Ok(());
        };
        let options = queue.options.lock().await.clone();
        let Some(params) = options.suspended_queue_stream_parameters() else {
            return Ok(());
        };
        let client = self.wait_for_client().await?;
        self.with_trace(
            TraceOperationKind::ConfigureQueue,
            Some(&queue.uri),
            client.configure_queue_stream(handle.queue_id, params),
        )
        .await
    }

    async fn suspend_queue_for_host_health(
        self: &Arc<Self>,
        queue: &Arc<QueueState>,
    ) -> Result<()> {
        if !queue.set_suspended(true) {
            return Ok(());
        }
        self.apply_suspended_queue_stream(queue).await?;
        queue.enqueue_event(QueueEvent::Suspended);
        self.emit_session(SessionEvent::QueueSuspended {
            uri: queue.uri.clone(),
        });
        Ok(())
    }

    async fn resume_queue_from_host_health(
        self: &Arc<Self>,
        queue: &Arc<QueueState>,
    ) -> Result<()> {
        if !queue.set_suspended(false) {
            return Ok(());
        }
        let options = queue.options.lock().await.clone();
        self.apply_queue_configuration(queue, &options).await?;
        queue.enqueue_event(QueueEvent::Resumed);
        self.emit_session(SessionEvent::QueueResumed {
            uri: queue.uri.clone(),
        });
        self.flush_queue(queue).await?;
        Ok(())
    }

    async fn maybe_dump_message<T: std::fmt::Debug>(&self, direction: &str, value: &T) {
        if !self.should_dump_message().await {
            return;
        }
        info!(target: "blazox::messages", direction, payload = ?value);
    }

    async fn should_dump_message(&self) -> bool {
        let mut state = self.message_dumping.lock().await;
        match &mut *state {
            MessageDumpState::Off => false,
            MessageDumpState::On => true,
            MessageDumpState::Remaining(remaining) => {
                if *remaining == 0 {
                    *state = MessageDumpState::Off;
                    false
                } else {
                    *remaining -= 1;
                    if *remaining == 0 {
                        *state = MessageDumpState::Off;
                    }
                    true
                }
            }
            MessageDumpState::Until(deadline) => {
                if tokio::time::Instant::now() <= *deadline {
                    true
                } else {
                    *state = MessageDumpState::Off;
                    false
                }
            }
        }
    }

    async fn dump_stats(&self) {
        let queues = self.queue_snapshot().await;
        let mut pending_posts = 0usize;
        for queue in &queues {
            pending_posts += queue.pending_posts.lock().await.len();
        }
        info!(
            target: "blazox::stats",
            started = self.started.load(Ordering::SeqCst),
            reconnecting = self.reconnecting.load(Ordering::SeqCst),
            unhealthy = self.host_unhealthy.load(Ordering::SeqCst),
            queue_count = queues.len(),
            pending_posts,
            processing_threads = self.options.num_processing_threads,
            blob_buffer_size = self.options.blob_buffer_size,
            channel_high_watermark = self.options.channel_high_watermark,
            event_queue_low_watermark = self.options.event_queue_low_watermark,
            event_queue_high_watermark = self.options.event_queue_high_watermark,
            "session_stats"
        );
    }
}

impl QueueState {
    async fn should_suspend_on_bad_host_health(&self) -> bool {
        self.options.lock().await.suspends_on_bad_host_health
    }

    fn set_suspended(&self, suspended: bool) -> bool {
        self.suspended.swap(suspended, Ordering::SeqCst) != suspended
    }

    fn queue_id(&self) -> Option<u32> {
        let value = self.last_queue_id.load(Ordering::Relaxed);
        (value != 0).then_some(value)
    }

    fn enqueue_event(&self, event: QueueEvent) {
        self.watermark_monitor.on_enqueue();
        if self.event_tx.send(event).is_err() {
            self.watermark_monitor.on_dequeue();
        }
    }

    async fn buffer_pending_posts(&self, messages: &[PostMessage]) {
        let mut pending = self.pending_posts.lock().await;
        for message in messages {
            let Some(message_guid) = message.message_guid else {
                continue;
            };
            if pending
                .iter()
                .any(|existing| existing.message_guid == Some(message_guid))
            {
                continue;
            }
            pending.push_back(message.clone());
        }
    }

    async fn remove_pending_post(&self, message_guid: MessageGuid) {
        let mut pending = self.pending_posts.lock().await;
        if let Some(index) = pending
            .iter()
            .position(|message| message.message_guid == Some(message_guid))
        {
            pending.remove(index);
        }
    }
}

fn validate_post_queue_state(uri: &Uri, options: &QueueOptions, closed: bool) -> Result<()> {
    if closed {
        return Err(Error::QueueClosed(uri.to_string()));
    }
    if !options.flags.contains(crate::types::QueueFlags::WRITE) {
        return Err(Error::QueueReadOnly(uri.to_string()));
    }
    Ok(())
}

fn validate_outbound_posts(
    uri: &Uri,
    options: &QueueOptions,
    closed: bool,
    suspended: bool,
    messages: &[PostMessage],
) -> Result<()> {
    validate_post_queue_state(uri, options, closed)?;
    if suspended {
        return Err(Error::QueueSuspended(uri.to_string()));
    }
    if messages.iter().any(|message| message.payload.is_empty()) {
        return Err(Error::EmptyPayload);
    }
    if options.flags.contains(crate::types::QueueFlags::ACK)
        && messages
            .iter()
            .any(|message| message.correlation_id.is_none())
    {
        return Err(Error::MissingCorrelationId(uri.to_string()));
    }
    Ok(())
}

fn spawn_session_event_dispatcher(
    mut rx: mpsc::UnboundedReceiver<SessionEvent>,
    events: broadcast::Sender<SessionEvent>,
    monitor: Arc<EventWatermarkMonitor>,
) {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            monitor.on_dequeue();
            let _ = events.send(event);
        }
    });
}

fn spawn_queue_event_dispatcher(
    mut rx: mpsc::UnboundedReceiver<QueueEvent>,
    events: broadcast::Sender<QueueEvent>,
    monitor: Arc<EventWatermarkMonitor>,
) {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            monitor.on_dequeue();
            let _ = events.send(event);
        }
    });
}

async fn transport_loop(session: Arc<SessionInner>, client: Client) {
    let mut events = client.subscribe();
    loop {
        match events.recv().await {
            Ok(event) => {
                let terminal = matches!(
                    event,
                    TransportEvent::TransportClosed | TransportEvent::TransportError(_)
                );
                session.handle_transport_event(event).await;
                if terminal {
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                session.start_reconnect("transport receiver closed".to_string());
                return;
            }
        }
    }
}

fn reconnect_backoff(attempt: u32) -> Duration {
    Duration::from_millis((u64::from(attempt.min(10))) * 250)
}

fn distributed_trace_operation_name(kind: TraceOperationKind) -> &'static str {
    match kind {
        TraceOperationKind::Connect => "connect",
        TraceOperationKind::Authenticate => "authenticate",
        TraceOperationKind::OpenQueue => "open_queue",
        TraceOperationKind::ConfigureQueue => "configure_queue",
        TraceOperationKind::CloseQueue => "close_queue",
        TraceOperationKind::Disconnect => "disconnect",
        TraceOperationKind::Publish => "publish",
        TraceOperationKind::Confirm => "confirm",
        TraceOperationKind::AdminCommand => "admin_command",
    }
}

fn distributed_trace_baggage(kind: TraceOperationKind, queue: Option<&Uri>) -> TraceBaggage {
    let mut baggage = vec![(
        "bmq.operation".to_string(),
        distributed_trace_operation_name(kind).to_string(),
    )];
    if let Some(queue) = queue {
        baggage.push(("bmq.queue_uri".to_string(), queue.to_string()));
        baggage.push(("bmq.queue_domain".to_string(), queue.domain().to_string()));
        baggage.push(("bmq.queue_name".to_string(), queue.queue().to_string()));
    }
    baggage
}

fn parse_message_dump_command(command: &str) -> Result<MessageDumpState> {
    let trimmed = command.trim();
    if trimmed.eq_ignore_ascii_case("OFF") {
        return Ok(MessageDumpState::Off);
    }
    if trimmed.eq_ignore_ascii_case("ON") {
        return Ok(MessageDumpState::On);
    }
    if let Some(seconds) = trimmed.strip_suffix(['s', 'S']) {
        let seconds = seconds.trim().parse::<u64>().map_err(|_| {
            Error::InvalidConfiguration(format!("invalid message dump duration: {trimmed}"))
        })?;
        if seconds == 0 {
            return Err(Error::InvalidConfiguration(
                "message dump duration must be positive".to_string(),
            ));
        }
        return Ok(MessageDumpState::Until(
            tokio::time::Instant::now() + Duration::from_secs(seconds),
        ));
    }
    let remaining = trimmed.parse::<usize>().map_err(|_| {
        Error::InvalidConfiguration(format!(
            "invalid message dumping command: expected ON, OFF, <count>, or <seconds>s, got {trimmed}"
        ))
    })?;
    if remaining == 0 {
        return Err(Error::InvalidConfiguration(
            "message dump count must be positive".to_string(),
        ));
    }
    Ok(MessageDumpState::Remaining(remaining))
}

fn post_to_outbound(message: PostMessage) -> OutboundPut {
    let mut outbound = OutboundPut::new(message.payload)
        .with_properties(message.properties)
        .with_compression(message.compression);
    if let Some(correlation_id) = message.correlation_id {
        outbound = outbound.with_correlation_id(correlation_id.get() as u32);
    }
    if let Some(message_guid) = message.message_guid {
        outbound = outbound.with_message_guid(message_guid);
    }
    outbound
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_outbound_posts_rejects_closed_readonly_empty_and_missing_correlation_id() {
        let uri = Uri::parse("bmq://bmq.test.mem.priority/example").unwrap();

        let err = validate_outbound_posts(
            &uri,
            &QueueOptions::writer(),
            true,
            false,
            &[PostMessage::new("payload")],
        )
        .unwrap_err();
        assert!(matches!(err, Error::QueueClosed(_)));

        let err = validate_outbound_posts(
            &uri,
            &QueueOptions::reader(),
            false,
            false,
            &[PostMessage::new("payload")],
        )
        .unwrap_err();
        assert!(matches!(err, Error::QueueReadOnly(_)));

        let err = validate_outbound_posts(
            &uri,
            &QueueOptions::writer(),
            false,
            false,
            &[PostMessage::new(bytes::Bytes::new())],
        )
        .unwrap_err();
        assert!(matches!(err, Error::EmptyPayload));

        let err = validate_outbound_posts(
            &uri,
            &QueueOptions::writer()
                .flags(crate::types::QueueFlags::WRITE | crate::types::QueueFlags::ACK),
            false,
            false,
            &[PostMessage::new("payload")],
        )
        .unwrap_err();
        assert!(matches!(err, Error::MissingCorrelationId(_)));
    }

    #[test]
    fn validate_outbound_posts_allows_ack_messages_with_correlation_ids() {
        let uri = Uri::parse("bmq://bmq.test.mem.priority/example").unwrap();
        let message = PostMessage::new("payload").correlation_id(CorrelationId::new(7));
        validate_outbound_posts(
            &uri,
            &QueueOptions::writer()
                .flags(crate::types::QueueFlags::WRITE | crate::types::QueueFlags::ACK),
            false,
            false,
            &[message],
        )
        .unwrap();
    }

    #[test]
    fn watermark_monitor_emits_high_and_normal_transitions() {
        let (events, _) = broadcast::channel(16);
        let monitor = EventWatermarkMonitor::new(1, 3, events.clone());
        let mut rx = events.subscribe();

        monitor.on_enqueue();
        monitor.on_enqueue();
        monitor.on_enqueue();
        let event = rx.try_recv().unwrap();
        match event {
            SessionEvent::SlowConsumerHighWatermark { pending } => assert_eq!(pending, 3),
            other => panic!("unexpected event: {other:?}"),
        }

        monitor.on_dequeue();
        monitor.on_dequeue();
        let event = rx.try_recv().unwrap();
        match event {
            SessionEvent::SlowConsumerNormal { pending } => assert_eq!(pending, 1),
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
