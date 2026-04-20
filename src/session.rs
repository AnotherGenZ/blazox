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
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::sync::{Mutex, broadcast, watch};
use tokio::time::{sleep, timeout};
use tracing::info;

#[derive(Debug, Clone)]
pub enum SessionEvent {
    Connected,
    Authenticated,
    Reconnecting { attempt: u32, error: String },
    Reconnected,
    Disconnected,
    TransportError(String),
    HostHealthChanged(HostHealthState),
    QueueOpened { uri: Uri, queue_id: u32 },
    QueueReopened { uri: Uri, queue_id: u32 },
    QueueClosed { uri: Uri },
    QueueSuspended { uri: Uri },
    QueueResumed { uri: Uri },
    Schema(InboundSchemaEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenQueueStatus {
    pub uri: Uri,
    pub queue_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigureQueueStatus {
    pub uri: Uri,
    pub queue_id: u32,
    pub suspended: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseQueueStatus {
    pub uri: Uri,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMessage {
    pub queue_id: u32,
    pub sub_queue_id: u32,
    pub rda_info: crate::wire::RdaInfo,
    pub message_guid: MessageGuid,
    pub payload: bytes::Bytes,
    pub properties: crate::wire::MessageProperties,
    pub properties_are_old_style: bool,
    pub sub_queue_infos: Vec<crate::wire::SubQueueInfo>,
    pub message_group_id: Option<String>,
    pub compression: crate::wire::CompressionAlgorithm,
    pub flags: u8,
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
    pub fn confirmation_cookie(&self) -> MessageConfirmationCookie {
        MessageConfirmationCookie::new(self.queue_id, self.message_guid, self.sub_queue_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acknowledgement {
    pub queue_id: u32,
    pub status: u8,
    pub correlation_id: CorrelationId,
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

#[derive(Debug, Clone)]
pub enum QueueEvent {
    Opened { queue_id: u32 },
    Reopened { queue_id: u32 },
    Closed,
    Suspended,
    Resumed,
    Message(ReceivedMessage),
    Ack(Acknowledgement),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Connecting,
    Connected,
    Reconnecting,
    Closed,
}

#[derive(Clone)]
pub struct Session {
    inner: Arc<SessionInner>,
}

#[derive(Clone)]
pub struct Queue {
    session: Session,
    state: Arc<QueueState>,
}

#[derive(Clone)]
pub struct PutBuilder {
    queue: Queue,
    batch: PostBatch,
}

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
    connection: watch::Sender<ConnectionState>,
    message_dumping: Mutex<MessageDumpState>,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MessageDumpState {
    Off,
    On,
    Remaining(usize),
    Until(tokio::time::Instant),
}

impl Session {
    pub async fn start(options: SessionOptions) -> Result<Self> {
        Self::connect(options).await
    }

    pub async fn connect(options: SessionOptions) -> Result<Self> {
        let event_capacity = options.event_queue_capacity();
        let guid_generator = options.to_client_config().message_guid_generator();
        let (events, _) = broadcast::channel(event_capacity);
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
            connection,
            message_dumping: Mutex::new(MessageDumpState::Off),
        });

        let client = inner.establish_client().await?;
        inner.finish_connect(client, false).await?;
        inner.spawn_host_health_monitor();
        inner.spawn_stats_dumper();

        Ok(Self { inner })
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::SeqCst)
    }

    pub fn events(&self) -> broadcast::Receiver<SessionEvent> {
        self.inner.events.subscribe()
    }

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

    pub fn next_message_guid(&self) -> MessageGuid {
        self.inner.guid_generator.next()
    }

    pub async fn next_event(&self) -> Result<SessionEvent> {
        let mut events = self.events();
        timeout(self.inner.options.request_timeout, events.recv())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::RequestCanceled)
    }

    pub async fn authenticate_anonymous(&self) -> Result<()> {
        self.authenticate(AuthenticationRequest {
            mechanism: "ANONYMOUS".to_string(),
            data: None,
        })
        .await
    }

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

    pub async fn open_queue(&self, uri: impl AsRef<str>, options: QueueOptions) -> Result<Queue> {
        Ok(self.open_queue_with_status(uri, options).await?.0)
    }

    pub async fn open_queue_with_status(
        &self,
        uri: impl AsRef<str>,
        options: QueueOptions,
    ) -> Result<(Queue, OpenQueueStatus)> {
        let uri = Uri::parse(uri.as_ref())?;
        let handle = self.inner.open_queue_with_retry(&uri, &options).await?;
        let (events, _) = broadcast::channel(self.inner.options.event_queue_capacity());
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

        let _ = state.events.send(QueueEvent::Opened {
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

    pub async fn get_queue_id(&self, uri: impl AsRef<str>) -> Option<u32> {
        self.get_queue(uri).await.and_then(|queue| queue.queue_id())
    }

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

    pub async fn get_queue_by_id(&self, queue_id: u32) -> Option<Queue> {
        let state = self.inner.lookup_queue(queue_id).await?;
        Some(Queue {
            session: self.clone(),
            state,
        })
    }

    pub fn load_message_properties(&self) -> crate::wire::MessageProperties {
        crate::wire::MessageProperties::default()
    }

    pub fn load_put_builder(&self, queue: &Queue) -> PutBuilder {
        queue.put_builder()
    }

    pub fn load_confirm_builder(&self, queue: &Queue) -> ConfirmBuilder {
        queue.confirm_builder()
    }

    pub async fn confirm_message(&self, cookie: MessageConfirmationCookie) -> Result<()> {
        let queue = self.get_queue_by_id(cookie.queue_id).await.ok_or_else(|| {
            Error::InvalidConfiguration("unknown queue id in confirmation cookie".to_string())
        })?;
        queue
            .confirm(cookie.message_guid, cookie.sub_queue_id)
            .await
    }

    pub async fn configure_message_dumping(&self, command: impl AsRef<str>) -> Result<()> {
        let state = parse_message_dump_command(command.as_ref())?;
        *self.inner.message_dumping.lock().await = state;
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        self.disconnect().await
    }

    pub async fn linger(&self) -> Result<()> {
        timeout(self.inner.options.linger_timeout, self.disconnect())
            .await
            .map_err(|_| Error::Timeout)?
    }

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
    pub fn uri(&self) -> &Uri {
        &self.state.uri
    }

    pub fn queue_id(&self) -> Option<u32> {
        let value = self.state.last_queue_id.load(Ordering::Relaxed);
        (value != 0).then_some(value)
    }

    pub fn events(&self) -> broadcast::Receiver<QueueEvent> {
        self.state.events.subscribe()
    }

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

    pub async fn next_event(&self) -> Result<QueueEvent> {
        let mut events = self.events();
        timeout(self.session.inner.options.request_timeout, events.recv())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::RequestCanceled)
    }

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

    pub async fn post(&self, message: PostMessage) -> Result<()> {
        let mut batch = PostBatch::new();
        batch.push(message);
        self.post_batch(batch).await
    }

    pub async fn post_batch(&self, batch: PostBatch) -> Result<()> {
        let packed = self.pack_batch(batch).await?;
        self.post_packed_batch(packed).await
    }

    pub fn put_builder(&self) -> PutBuilder {
        PutBuilder {
            queue: self.clone(),
            batch: PostBatch::new(),
        }
    }

    pub async fn pack_message(&self, message: PostMessage) -> Result<PackedPostBatch> {
        let mut batch = PostBatch::new();
        batch.push(message);
        self.pack_batch(batch).await
    }

    pub async fn pack_batch(&self, batch: PostBatch) -> Result<PackedPostBatch> {
        self.session
            .inner
            .pack_post_batch(&self.state, batch.into_messages())
            .await
    }

    pub async fn post_packed_batch(&self, batch: PackedPostBatch) -> Result<()> {
        self.session
            .inner
            .post_packed_batch(&self.state, batch.into_messages())
            .await
    }

    pub async fn confirm(&self, message_guid: MessageGuid, sub_queue_id: u32) -> Result<()> {
        let mut batch = ConfirmBatch::new();
        batch.push(message_guid, sub_queue_id);
        self.confirm_batch(batch).await
    }

    pub async fn confirm_batch(&self, batch: ConfirmBatch) -> Result<()> {
        self.session
            .inner
            .confirm_batch(&self.state, batch.into_messages())
            .await
    }

    pub async fn confirm_cookie(&self, cookie: MessageConfirmationCookie) -> Result<()> {
        self.confirm(cookie.message_guid, cookie.sub_queue_id).await
    }

    pub fn confirm_builder(&self) -> ConfirmBuilder {
        ConfirmBuilder {
            queue: self.clone(),
            batch: ConfirmBatch::new(),
        }
    }

    pub async fn reconfigure(&self, options: QueueOptions) -> Result<()> {
        self.reconfigure_with_status(options).await.map(|_| ())
    }

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

    pub async fn close(&self) -> Result<()> {
        self.close_with_status().await.map(|_| ())
    }

    pub async fn close_with_status(&self) -> Result<CloseQueueStatus> {
        self.session.inner.close_queue(&self.state).await?;
        Ok(CloseQueueStatus {
            uri: self.state.uri.clone(),
        })
    }
}

impl PutBuilder {
    pub fn push(&mut self, message: PostMessage) -> &mut Self {
        self.batch.push(message);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    pub async fn pack(self) -> Result<PackedPostBatch> {
        self.queue.pack_batch(self.batch).await
    }

    pub async fn post(self) -> Result<()> {
        self.queue.post_batch(self.batch).await
    }
}

impl ConfirmBuilder {
    pub fn push(&mut self, message_guid: MessageGuid, sub_queue_id: u32) -> &mut Self {
        self.batch.push(message_guid, sub_queue_id);
        self
    }

    pub fn push_cookie(&mut self, cookie: MessageConfirmationCookie) -> &mut Self {
        self.batch.push_cookie(cookie);
        self
    }

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
            let _ = queue.events.send(QueueEvent::Message(message.into()));
        }
    }

    async fn route_ack(&self, ack: AckMessage) {
        self.maybe_dump_message("inbound_ack", &ack).await;
        if let Some(queue) = self.lookup_queue(ack.queue_id).await {
            queue.remove_pending_post(ack.message_guid).await;
            let _ = queue.events.send(QueueEvent::Ack(ack.into()));
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

            let _ = queue.events.send(QueueEvent::Reopened { queue_id });
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
        if queue.suspended.load(Ordering::SeqCst) {
            return Err(Error::QueueSuspended(queue.uri.to_string()));
        }

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

        let _ = queue.events.send(QueueEvent::Closed);
        self.emit_session(SessionEvent::QueueClosed {
            uri: queue.uri.clone(),
        });
        Ok(())
    }

    fn emit_session(&self, event: SessionEvent) {
        let _ = self.events.send(event);
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
        let _ = queue.events.send(QueueEvent::Suspended);
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
        let _ = queue.events.send(QueueEvent::Resumed);
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
