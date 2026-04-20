use crate::client::{ClientConfig, OpenQueueOptions, QueueHandleConfig, queue_flags};
use crate::error::{Error, Result};
use crate::schema::AuthenticationRequest;
use crate::schema::{QueueStreamParameters, StreamParameters, Subscription};
use crate::wire::{CompressionAlgorithm, MessageGuid, MessageProperties};
use bytes::Bytes;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::watch;

pub type AuthRequestFuture = Pin<Box<dyn Future<Output = Result<AuthenticationRequest>> + Send>>;
pub type TraceBaggage = Vec<(String, String)>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Uri {
    raw: String,
    domain: String,
    queue: String,
}

impl Uri {
    pub fn parse(input: impl AsRef<str>) -> Result<Self> {
        let raw = input.as_ref().trim().to_string();
        let Some(rest) = raw.strip_prefix("bmq://") else {
            return Err(Error::InvalidUri(raw));
        };
        let mut parts = rest.splitn(2, '/');
        let domain = parts.next().unwrap_or_default().trim();
        let queue = parts.next().unwrap_or_default().trim();
        if domain.is_empty() || queue.is_empty() {
            return Err(Error::InvalidUri(raw));
        }
        let domain = domain.to_string();
        let queue = queue.to_string();
        Ok(Self { raw, domain, queue })
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }
}

impl fmt::Display for Uri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.raw)
    }
}

impl TryFrom<&str> for Uri {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

impl TryFrom<String> for Uri {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        Self::parse(value)
    }
}

#[derive(Debug, Clone, Default)]
pub struct UriBuilder {
    domain: Option<String>,
    queue: Option<String>,
}

impl UriBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn domain(mut self, value: impl Into<String>) -> Self {
        self.domain = Some(value.into());
        self
    }

    pub fn queue(mut self, value: impl Into<String>) -> Self {
        self.queue = Some(value.into());
        self
    }

    pub fn build(self) -> Result<Uri> {
        let domain = self.domain.unwrap_or_default();
        let queue = self.queue.unwrap_or_default();
        Uri::parse(format!("bmq://{domain}/{queue}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct CorrelationId(u64);

impl CorrelationId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u32> for CorrelationId {
    fn from(value: u32) -> Self {
        Self(value as u64)
    }
}

#[derive(Debug)]
pub struct CorrelationIdGenerator(AtomicU64);

impl Default for CorrelationIdGenerator {
    fn default() -> Self {
        Self(AtomicU64::new(1))
    }
}

impl CorrelationIdGenerator {
    pub fn next(&self) -> CorrelationId {
        CorrelationId(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostHealthState {
    Healthy,
    Unhealthy,
}

pub trait HostHealthMonitor: Send + Sync {
    fn subscribe(&self) -> watch::Receiver<HostHealthState>;
}

#[derive(Debug)]
pub struct ManualHostHealthMonitor {
    tx: watch::Sender<HostHealthState>,
}

impl Default for ManualHostHealthMonitor {
    fn default() -> Self {
        let (tx, _) = watch::channel(HostHealthState::Healthy);
        Self { tx }
    }
}

impl ManualHostHealthMonitor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_state(&self, state: HostHealthState) {
        let _ = self.tx.send(state);
    }

    pub fn set_healthy(&self) {
        self.set_state(HostHealthState::Healthy);
    }

    pub fn set_unhealthy(&self) {
        self.set_state(HostHealthState::Unhealthy);
    }
}

impl HostHealthMonitor for ManualHostHealthMonitor {
    fn subscribe(&self) -> watch::Receiver<HostHealthState> {
        self.tx.subscribe()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceOperationKind {
    Connect,
    Authenticate,
    OpenQueue,
    ConfigureQueue,
    CloseQueue,
    Disconnect,
    Publish,
    Confirm,
    AdminCommand,
}

#[derive(Debug, Clone)]
pub struct TraceOperation {
    pub kind: TraceOperationKind,
    pub queue: Option<Uri>,
}

pub trait TraceSink: Send + Sync {
    fn operation_started(&self, operation: &TraceOperation);
    fn operation_succeeded(&self, operation: &TraceOperation);
    fn operation_failed(&self, operation: &TraceOperation, error: &Error);
}

pub trait DistributedTraceSpan: Send + Sync {
    fn operation(&self) -> &str;

    fn baggage(&self) -> TraceBaggage {
        TraceBaggage::default()
    }
}

pub trait DistributedTraceScope: Send {}

pub trait DistributedTraceContext: Send + Sync {
    fn current_span(&self) -> Option<Arc<dyn DistributedTraceSpan>>;
    fn activate_span(&self, span: Arc<dyn DistributedTraceSpan>) -> Box<dyn DistributedTraceScope>;
}

pub trait DistributedTracer: Send + Sync {
    fn create_child_span(
        &self,
        parent: Option<Arc<dyn DistributedTraceSpan>>,
        operation: &str,
        baggage: &TraceBaggage,
    ) -> Option<Arc<dyn DistributedTraceSpan>>;
}

pub trait AuthProvider: Send + Sync {
    fn authentication_request(&self) -> AuthRequestFuture;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueFlags(u64);

impl QueueFlags {
    pub const ADMIN: Self = Self(queue_flags::ADMIN);
    pub const READ: Self = Self(queue_flags::READ);
    pub const WRITE: Self = Self(queue_flags::WRITE);
    pub const ACK: Self = Self(queue_flags::ACK);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn read_write() -> Self {
        Self(queue_flags::READ | queue_flags::WRITE)
    }

    pub const fn bits(self) -> u64 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for QueueFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for QueueFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueOptions {
    pub flags: QueueFlags,
    pub read_count: i32,
    pub write_count: i32,
    pub admin_count: i32,
    pub max_unconfirmed_messages: Option<i64>,
    pub max_unconfirmed_bytes: Option<i64>,
    pub consumer_priority: Option<i32>,
    pub consumer_priority_count: Option<i32>,
    pub app_id: Option<String>,
    pub subscriptions: Vec<Subscription>,
    pub suspends_on_bad_host_health: bool,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            flags: QueueFlags::WRITE,
            read_count: 0,
            write_count: 1,
            admin_count: 0,
            max_unconfirmed_messages: None,
            max_unconfirmed_bytes: None,
            consumer_priority: None,
            consumer_priority_count: None,
            app_id: None,
            subscriptions: Vec::new(),
            suspends_on_bad_host_health: false,
        }
    }
}

impl QueueOptions {
    pub fn reader() -> Self {
        Self {
            flags: QueueFlags::READ | QueueFlags::ACK,
            read_count: 1,
            write_count: 0,
            ..Self::default()
        }
    }

    pub fn writer() -> Self {
        Self::default()
    }

    pub fn read_write() -> Self {
        Self {
            flags: QueueFlags::READ | QueueFlags::WRITE | QueueFlags::ACK,
            read_count: 1,
            write_count: 1,
            ..Self::default()
        }
    }

    pub fn flags(mut self, flags: QueueFlags) -> Self {
        self.flags = flags;
        self
    }

    pub fn read_count(mut self, value: i32) -> Self {
        self.read_count = value;
        self
    }

    pub fn write_count(mut self, value: i32) -> Self {
        self.write_count = value;
        self
    }

    pub fn admin_count(mut self, value: i32) -> Self {
        self.admin_count = value;
        self
    }

    pub fn max_unconfirmed_messages(mut self, value: i64) -> Self {
        self.max_unconfirmed_messages = Some(value);
        self
    }

    pub fn max_unconfirmed_bytes(mut self, value: i64) -> Self {
        self.max_unconfirmed_bytes = Some(value);
        self
    }

    pub fn consumer_priority(mut self, value: i32) -> Self {
        self.consumer_priority = Some(value);
        self
    }

    pub fn consumer_priority_count(mut self, value: i32) -> Self {
        self.consumer_priority_count = Some(value);
        self
    }

    pub fn app_id(mut self, value: impl Into<String>) -> Self {
        self.app_id = Some(value.into());
        self
    }

    pub fn subscriptions(mut self, value: Vec<Subscription>) -> Self {
        self.subscriptions = value;
        self
    }

    pub fn suspends_on_bad_host_health(mut self, value: bool) -> Self {
        self.suspends_on_bad_host_health = value;
        self
    }

    pub(crate) fn suspended_queue_stream_parameters(&self) -> Option<QueueStreamParameters> {
        if self.read_count <= 0 && !self.flags.contains(QueueFlags::READ) {
            return None;
        }

        Some(QueueStreamParameters {
            sub_id_info: None,
            max_unconfirmed_messages: 0,
            max_unconfirmed_bytes: 0,
            consumer_priority: i32::MIN,
            consumer_priority_count: 0,
        })
    }

    pub(crate) fn default_queue_stream_parameters(&self) -> Option<QueueStreamParameters> {
        if self.read_count <= 0
            && self.max_unconfirmed_messages.is_none()
            && self.max_unconfirmed_bytes.is_none()
            && self.consumer_priority.is_none()
            && self.consumer_priority_count.is_none()
        {
            return None;
        }

        Some(QueueStreamParameters {
            sub_id_info: None,
            max_unconfirmed_messages: self.max_unconfirmed_messages.unwrap_or_default(),
            max_unconfirmed_bytes: self.max_unconfirmed_bytes.unwrap_or_default(),
            consumer_priority: self.consumer_priority.unwrap_or(i32::MIN),
            consumer_priority_count: self.consumer_priority_count.unwrap_or_default(),
        })
    }

    pub(crate) fn initial_queue_stream_parameters(&self) -> QueueStreamParameters {
        QueueStreamParameters {
            sub_id_info: None,
            max_unconfirmed_messages: self.max_unconfirmed_messages.unwrap_or_default(),
            max_unconfirmed_bytes: self.max_unconfirmed_bytes.unwrap_or_default(),
            consumer_priority: self.consumer_priority.unwrap_or(i32::MIN),
            consumer_priority_count: self.consumer_priority_count.unwrap_or_default(),
        }
    }

    pub(crate) fn stream_parameters(&self) -> Option<StreamParameters> {
        if self.app_id.is_none() && self.subscriptions.is_empty() {
            return None;
        }

        Some(StreamParameters {
            app_id: self
                .app_id
                .clone()
                .unwrap_or_else(|| "__default".to_string()),
            subscriptions: self.subscriptions.clone(),
        })
    }

    pub(crate) fn to_open_queue_options(&self) -> OpenQueueOptions {
        OpenQueueOptions {
            handle: QueueHandleConfig {
                flags: self.flags.bits(),
                read_count: self.read_count,
                write_count: self.write_count,
                admin_count: self.admin_count,
            },
            configure_queue_stream: Some(self.initial_queue_stream_parameters()),
            configure_stream: self.stream_parameters(),
        }
    }
}

#[derive(Clone)]
pub struct SessionOptions {
    pub broker_addr: String,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub open_queue_timeout: Duration,
    pub configure_queue_timeout: Duration,
    pub close_queue_timeout: Duration,
    pub disconnect_timeout: Duration,
    pub channel_write_timeout: Duration,
    pub linger_timeout: Duration,
    pub authenticate_anonymous: bool,
    pub auth_provider: Option<Arc<dyn AuthProvider>>,
    pub process_name_override: Option<String>,
    pub host_name_override: Option<String>,
    pub session_id: i32,
    pub features: String,
    pub user_agent: Option<String>,
    pub num_processing_threads: usize,
    pub blob_buffer_size: usize,
    pub channel_high_watermark: Option<u64>,
    pub event_queue_low_watermark: usize,
    pub event_queue_high_watermark: usize,
    pub stats_dump_interval: Option<Duration>,
    pub host_health_monitor: Option<Arc<dyn HostHealthMonitor>>,
    pub trace_sink: Option<Arc<dyn TraceSink>>,
    pub trace_context: Option<Arc<dyn DistributedTraceContext>>,
    pub tracer: Option<Arc<dyn DistributedTracer>>,
}

impl fmt::Debug for SessionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionOptions")
            .field("broker_addr", &self.broker_addr)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("open_queue_timeout", &self.open_queue_timeout)
            .field("configure_queue_timeout", &self.configure_queue_timeout)
            .field("close_queue_timeout", &self.close_queue_timeout)
            .field("disconnect_timeout", &self.disconnect_timeout)
            .field("channel_write_timeout", &self.channel_write_timeout)
            .field("linger_timeout", &self.linger_timeout)
            .field("authenticate_anonymous", &self.authenticate_anonymous)
            .field("process_name_override", &self.process_name_override)
            .field("host_name_override", &self.host_name_override)
            .field("session_id", &self.session_id)
            .field("features", &self.features)
            .field("user_agent", &self.user_agent)
            .field("num_processing_threads", &self.num_processing_threads)
            .field("blob_buffer_size", &self.blob_buffer_size)
            .field("channel_high_watermark", &self.channel_high_watermark)
            .field("event_queue_low_watermark", &self.event_queue_low_watermark)
            .field(
                "event_queue_high_watermark",
                &self.event_queue_high_watermark,
            )
            .field("stats_dump_interval", &self.stats_dump_interval)
            .finish_non_exhaustive()
    }
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            broker_addr: "127.0.0.1:30114".to_string(),
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
            open_queue_timeout: Duration::from_secs(10),
            configure_queue_timeout: Duration::from_secs(10),
            close_queue_timeout: Duration::from_secs(10),
            disconnect_timeout: Duration::from_secs(5),
            channel_write_timeout: Duration::from_secs(5),
            linger_timeout: Duration::from_secs(5),
            authenticate_anonymous: false,
            auth_provider: None,
            process_name_override: None,
            host_name_override: None,
            session_id: 1,
            features: "PROTOCOL_ENCODING:JSON;MPS:MESSAGE_PROPERTIES_EX".to_string(),
            user_agent: None,
            num_processing_threads: 1,
            blob_buffer_size: 64 * 1024,
            channel_high_watermark: None,
            event_queue_low_watermark: 128,
            event_queue_high_watermark: 512,
            stats_dump_interval: None,
            host_health_monitor: None,
            trace_sink: None,
            trace_context: None,
            tracer: None,
        }
    }
}

impl SessionOptions {
    pub fn broker_addr(mut self, value: impl Into<String>) -> Self {
        self.broker_addr = value.into();
        self
    }

    pub fn request_timeout(mut self, value: Duration) -> Self {
        self.request_timeout = value;
        self
    }

    pub fn connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = value;
        self
    }

    pub fn open_queue_timeout(mut self, value: Duration) -> Self {
        self.open_queue_timeout = value;
        self
    }

    pub fn configure_queue_timeout(mut self, value: Duration) -> Self {
        self.configure_queue_timeout = value;
        self
    }

    pub fn close_queue_timeout(mut self, value: Duration) -> Self {
        self.close_queue_timeout = value;
        self
    }

    pub fn disconnect_timeout(mut self, value: Duration) -> Self {
        self.disconnect_timeout = value;
        self
    }

    pub fn channel_write_timeout(mut self, value: Duration) -> Self {
        self.channel_write_timeout = value;
        self
    }

    pub fn linger_timeout(mut self, value: Duration) -> Self {
        self.linger_timeout = value;
        self
    }

    pub fn authenticate_anonymous(mut self, value: bool) -> Self {
        self.authenticate_anonymous = value;
        self
    }

    pub fn auth_provider(mut self, value: Arc<dyn AuthProvider>) -> Self {
        self.auth_provider = Some(value);
        self
    }

    pub fn process_name_override(mut self, value: impl Into<String>) -> Self {
        self.process_name_override = Some(value.into());
        self
    }

    pub fn host_name_override(mut self, value: impl Into<String>) -> Self {
        self.host_name_override = Some(value.into());
        self
    }

    pub fn session_id(mut self, value: i32) -> Self {
        self.session_id = value;
        self
    }

    pub fn user_agent(mut self, value: impl Into<String>) -> Self {
        self.user_agent = Some(value.into());
        self
    }

    pub fn num_processing_threads(mut self, value: usize) -> Self {
        self.num_processing_threads = value.max(1);
        self
    }

    pub fn blob_buffer_size(mut self, value: usize) -> Self {
        self.blob_buffer_size = value.max(1);
        self
    }

    pub fn channel_high_watermark(mut self, value: u64) -> Self {
        self.channel_high_watermark = Some(value);
        self
    }

    pub fn event_queue_watermarks(mut self, low: usize, high: usize) -> Self {
        let high = high.max(1);
        self.event_queue_low_watermark = low.min(high);
        self.event_queue_high_watermark = high;
        self
    }

    pub fn stats_dump_interval(mut self, value: Duration) -> Self {
        self.stats_dump_interval = Some(value);
        self
    }

    pub fn host_health_monitor(mut self, value: Arc<dyn HostHealthMonitor>) -> Self {
        self.host_health_monitor = Some(value);
        self
    }

    pub fn trace_sink(mut self, value: Arc<dyn TraceSink>) -> Self {
        self.trace_sink = Some(value);
        self
    }

    pub fn trace_context(mut self, value: Arc<dyn DistributedTraceContext>) -> Self {
        self.trace_context = Some(value);
        self
    }

    pub fn tracer(mut self, value: Arc<dyn DistributedTracer>) -> Self {
        self.tracer = Some(value);
        self
    }

    pub(crate) fn to_client_config(&self) -> ClientConfig {
        let mut config = ClientConfig {
            request_timeout: self.request_timeout,
            open_queue_timeout: self.open_queue_timeout,
            configure_queue_timeout: self.configure_queue_timeout,
            close_queue_timeout: self.close_queue_timeout,
            disconnect_timeout: self.disconnect_timeout,
            channel_write_timeout: self.channel_write_timeout,
            channel_high_watermark: self.channel_high_watermark,
            blob_buffer_size: self.blob_buffer_size,
            session_id: self.session_id,
            features: self.features.clone(),
            ..ClientConfig::default()
        };
        if let Some(process_name) = &self.process_name_override {
            config.process_name = process_name.clone();
        }
        if let Some(host_name) = &self.host_name_override {
            config.host_name = host_name.clone();
        }
        if let Some(user_agent) = &self.user_agent {
            config.user_agent = user_agent.clone();
        }
        config
    }

    pub(crate) fn event_queue_capacity(&self) -> usize {
        self.event_queue_high_watermark.max(1)
    }
}

#[derive(Debug, Clone)]
pub struct PostMessage {
    pub payload: Bytes,
    pub properties: MessageProperties,
    pub correlation_id: Option<CorrelationId>,
    pub message_guid: Option<MessageGuid>,
    pub compression: CompressionAlgorithm,
}

impl PostMessage {
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
            properties: MessageProperties::default(),
            correlation_id: None,
            message_guid: None,
            compression: CompressionAlgorithm::None,
        }
    }

    pub fn properties(mut self, value: MessageProperties) -> Self {
        self.properties = value;
        self
    }

    pub fn correlation_id(mut self, value: CorrelationId) -> Self {
        self.correlation_id = Some(value);
        self
    }

    pub fn message_guid(mut self, value: MessageGuid) -> Self {
        self.message_guid = Some(value);
        self
    }

    pub fn compression(mut self, value: CompressionAlgorithm) -> Self {
        self.compression = value;
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostBatch {
    messages: Vec<PostMessage>,
}

impl PostBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, message: PostMessage) {
        self.messages.push(message);
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn into_messages(self) -> Vec<PostMessage> {
        self.messages
    }
}

#[derive(Debug, Clone, Default)]
pub struct PackedPostBatch {
    pub(crate) messages: Vec<PostMessage>,
}

impl PackedPostBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, message: PostMessage) {
        self.messages.push(message);
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn into_messages(self) -> Vec<PostMessage> {
        self.messages
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfirmMessage {
    pub message_guid: MessageGuid,
    pub sub_queue_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageConfirmationCookie {
    pub queue_id: u32,
    pub message_guid: MessageGuid,
    pub sub_queue_id: u32,
}

impl MessageConfirmationCookie {
    pub const fn new(queue_id: u32, message_guid: MessageGuid, sub_queue_id: u32) -> Self {
        Self {
            queue_id,
            message_guid,
            sub_queue_id,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConfirmBatch {
    messages: Vec<ConfirmMessage>,
}

impl ConfirmBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, message_guid: MessageGuid, sub_queue_id: u32) {
        self.messages.push(ConfirmMessage {
            message_guid,
            sub_queue_id,
        });
    }

    pub fn push_cookie(&mut self, cookie: MessageConfirmationCookie) {
        self.push(cookie.message_guid, cookie.sub_queue_id);
    }

    pub fn into_messages(self) -> Vec<ConfirmMessage> {
        self.messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_parse_extracts_domain_and_queue() {
        let uri = Uri::parse("bmq://bmq.test.mem.priority/example").unwrap();
        assert_eq!(uri.domain(), "bmq.test.mem.priority");
        assert_eq!(uri.queue(), "example");
    }

    #[test]
    fn uri_parse_rejects_invalid_values() {
        assert!(Uri::parse("example").is_err());
        assert!(Uri::parse("bmq://domain").is_err());
    }

    #[test]
    fn queue_options_map_to_open_options() {
        let options = QueueOptions::reader()
            .max_unconfirmed_messages(16)
            .max_unconfirmed_bytes(1024)
            .consumer_priority(1)
            .app_id("consumer-a");
        let mapped = options.to_open_queue_options();
        assert_eq!(
            mapped.handle.flags,
            (QueueFlags::READ | QueueFlags::ACK).bits()
        );
        assert_eq!(
            mapped
                .configure_queue_stream
                .as_ref()
                .unwrap()
                .max_unconfirmed_messages,
            16
        );
        assert_eq!(
            mapped.configure_stream.as_ref().unwrap().app_id,
            "consumer-a"
        );
    }

    #[test]
    fn writer_open_options_still_include_initial_queue_stream_config() {
        let mapped = QueueOptions::writer().to_open_queue_options();
        let stream = mapped.configure_queue_stream.unwrap();
        assert_eq!(stream.max_unconfirmed_messages, 0);
        assert_eq!(stream.max_unconfirmed_bytes, 0);
        assert_eq!(stream.consumer_priority, i32::MIN);
        assert_eq!(stream.consumer_priority_count, 0);
    }
}
