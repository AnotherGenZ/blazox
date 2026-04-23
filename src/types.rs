//! User-facing helper types, configuration builders, and extension traits.
//!
//! This module is where the crate's public API surface is shaped into concepts
//! application developers actually care about:
//!
//! - queue identity via [`Uri`]
//! - producer correlation and confirmation handles
//! - queue open and reconfigure behavior via [`QueueOptions`]
//! - session runtime behavior via [`SessionOptions`]
//! - integration points for authentication, host health, and tracing
//!
//! In other words, [`schema`] and [`wire`] describe what the broker speaks,
//! while this module describes how an application configures and interacts with
//! a client built on top of that protocol.
//!
//! [`schema`]: crate::schema
//! [`wire`]: crate::wire

use crate::client::{ClientConfig, OpenQueueOptions, QueueHandleConfig, queue_flags};
use crate::error::{Error, Result};
use crate::schema::AuthenticationRequest;
use crate::schema::{
    ConsumerInfo, QueueStreamParameters, StreamParameters, SubQueueIdInfo, Subscription,
};
use crate::wire::{CompressionAlgorithm, MessageGuid, MessageProperties};
use bytes::Bytes;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::watch;

/// Boxed future returned by an [`AuthProvider`].
pub type AuthRequestFuture = Pin<Box<dyn Future<Output = Result<AuthenticationRequest>> + Send>>;

/// Parsed BlazingMQ queue URI.
///
/// Queue URIs are the stable identifiers used throughout the client APIs and
/// in the upstream BlazingMQ documentation.  Queue ids are transport-local and
/// may change across reconnect, but a URI remains the same.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Uri {
    raw: String,
    domain: String,
    queue: String,
}

impl Uri {
    /// Parses a `bmq://<domain>/<queue>` URI.
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

    /// Returns the original URI string.
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Returns the URI domain segment.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Returns the queue name segment.
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

/// Builder for [`Uri`] values.
///
/// This is primarily a convenience for examples and tests; applications that
/// already have a broker-provided URI string can use [`Uri::parse`] directly.
#[derive(Debug, Clone, Default)]
pub struct UriBuilder {
    domain: Option<String>,
    queue: Option<String>,
}

impl UriBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the URI domain.
    pub fn domain(mut self, value: impl Into<String>) -> Self {
        self.domain = Some(value.into());
        self
    }

    /// Sets the queue name.
    pub fn queue(mut self, value: impl Into<String>) -> Self {
        self.queue = Some(value.into());
        self
    }

    /// Builds the final [`Uri`].
    pub fn build(self) -> Result<Uri> {
        let domain = self.domain.unwrap_or_default();
        let queue = self.queue.unwrap_or_default();
        Uri::parse(format!("bmq://{domain}/{queue}"))
    }
}

/// Producer-defined identifier echoed back in acknowledgement messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct CorrelationId(u64);

impl CorrelationId {
    /// Creates a new correlation id from a raw numeric value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw numeric value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u32> for CorrelationId {
    fn from(value: u32) -> Self {
        Self(value as u64)
    }
}

/// Monotonic generator for [`CorrelationId`] values.
#[derive(Debug)]
pub struct CorrelationIdGenerator(AtomicU64);

impl Default for CorrelationIdGenerator {
    fn default() -> Self {
        Self(AtomicU64::new(1))
    }
}

impl CorrelationIdGenerator {
    /// Returns the next correlation id.
    pub fn next(&self) -> CorrelationId {
        CorrelationId(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

/// Host health state observed by a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostHealthState {
    /// The host is healthy and queues may operate normally.
    Healthy,
    /// The host is unhealthy and opted-in queues should suspend.
    Unhealthy,
}

/// Source of host health transitions for a session.
///
/// Host health is an optional BlazingMQ feature documented at
/// <https://bloomberg.github.io/blazingmq/docs/features/host_health_monitoring/>.
/// A monitor does not itself suspend queues; instead, it reports process or
/// machine health transitions and the session applies the appropriate queue
/// reconfiguration for handles that opted in through [`QueueOptions`].
pub trait HostHealthMonitor: Send + Sync {
    /// Returns a watch channel carrying the current and future health state.
    fn subscribe(&self) -> watch::Receiver<HostHealthState>;
}

/// Manual host health monitor useful for tests and custom integrations.
///
/// The official C++ SDK ships a similar helper for unit tests and for
/// environments where another subsystem is already deciding host health.
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
    /// Creates a monitor with an initial healthy state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the current host health state.
    pub fn set_state(&self, state: HostHealthState) {
        let _ = self.tx.send(state);
    }

    /// Marks the host healthy.
    pub fn set_healthy(&self) {
        self.set_state(HostHealthState::Healthy);
    }

    /// Marks the host unhealthy.
    pub fn set_unhealthy(&self) {
        self.set_state(HostHealthState::Unhealthy);
    }
}

impl HostHealthMonitor for ManualHostHealthMonitor {
    fn subscribe(&self) -> watch::Receiver<HostHealthState> {
        self.tx.subscribe()
    }
}

/// Provider that constructs authentication requests on demand.
pub trait AuthProvider: Send + Sync {
    /// Produces the next authentication request to send to the broker.
    fn authentication_request(&self) -> AuthRequestFuture;
}

/// Queue mode flags mirrored from the BlazingMQ protocol.
///
/// These flags determine whether a queue handle behaves as a producer,
/// consumer, administrative handle, or some combination of those roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueFlags(u64);

impl QueueFlags {
    /// Administrative access to the queue.
    pub const ADMIN: Self = Self(queue_flags::ADMIN);
    /// Read access to receive pushed messages.
    pub const READ: Self = Self(queue_flags::READ);
    /// Write access to post messages.
    pub const WRITE: Self = Self(queue_flags::WRITE);
    /// Request producer acknowledgements.
    pub const ACK: Self = Self(queue_flags::ACK);

    /// Returns a flag set with no bits enabled.
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Returns the common read/write combination.
    pub const fn read_write() -> Self {
        Self(queue_flags::READ | queue_flags::WRITE)
    }

    /// Returns the raw protocol bits.
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Returns `true` when all bits from `other` are present.
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

/// High-level queue configuration used by the session API.
///
/// [`QueueOptions`] collect the settings that make a queue behave like a
/// specific kind of client attachment:
///
/// - producer vs consumer vs mixed mode
/// - consumer flow control
/// - consumer routing priority
/// - fanout application id
/// - subscriptions
/// - host-health suspension semantics
///
/// These map to the protocol's `OpenQueue`, `ConfigureQueueStream`, and
/// `ConfigureStream` requests.  See the official
/// [Consumer Flow Control](https://bloomberg.github.io/blazingmq/docs/features/consumer_flow_control/),
/// [Subscriptions](https://bloomberg.github.io/blazingmq/docs/features/subscriptions/),
/// and
/// [Host Health Monitoring](https://bloomberg.github.io/blazingmq/docs/features/host_health_monitoring/)
/// documents for the broker-side semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueOptions {
    /// Requested queue access flags.
    pub flags: QueueFlags,
    /// Number of reader handles requested from the broker.
    pub read_count: i32,
    /// Number of writer handles requested from the broker.
    pub write_count: i32,
    /// Number of administrative handles requested from the broker.
    pub admin_count: i32,
    /// Maximum number of outstanding pushed messages before delivery pauses.
    pub max_unconfirmed_messages: Option<i64>,
    /// Maximum number of outstanding pushed bytes before delivery pauses.
    pub max_unconfirmed_bytes: Option<i64>,
    /// Consumer priority advertised to the broker.
    pub consumer_priority: Option<i32>,
    /// Relative weight among consumers sharing a priority.
    pub consumer_priority_count: Option<i32>,
    /// Fanout application identifier, when applicable.
    pub app_id: Option<String>,
    /// Subscription expressions attached to the stream.
    pub subscriptions: Vec<Subscription>,
    /// Whether the queue should suspend while the host is unhealthy.
    pub suspends_on_bad_host_health: bool,
    subscriptions_set: bool,
    suspends_on_bad_host_health_set: bool,
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
            subscriptions_set: false,
            suspends_on_bad_host_health_set: false,
        }
    }
}

impl QueueOptions {
    const DEFAULT_MAX_UNCONFIRMED_MESSAGES: i64 = 1000;
    const DEFAULT_MAX_UNCONFIRMED_BYTES: i64 = 33_554_432;
    const DEFAULT_CONSUMER_PRIORITY: i32 = 0;
    const DEFAULT_CONSUMER_PRIORITY_COUNT: i32 = 1;
    const INITIAL_FANOUT_SUBQUEUE_ID: u32 = 1;

    /// Returns default options for a reader queue.
    ///
    /// This enables the protocol `READ` and `ACK` flags and requests one read
    /// handle, which is the most common shape for a consumer attachment.
    pub fn reader() -> Self {
        Self {
            flags: QueueFlags::READ | QueueFlags::ACK,
            read_count: 1,
            write_count: 0,
            ..Self::default()
        }
    }

    /// Returns default options for a writer queue.
    ///
    /// This is the most common producer attachment: write enabled, one writer
    /// handle requested, and no consumer-side flow-control settings.
    pub fn writer() -> Self {
        Self::default()
    }

    /// Returns default options for a queue opened for both reading and writing.
    ///
    /// Mixed-mode handles are less common than pure producers or consumers, but
    /// they are supported by the protocol and useful for test tools and some
    /// broker utility workflows.
    pub fn read_write() -> Self {
        Self {
            flags: QueueFlags::READ | QueueFlags::WRITE | QueueFlags::ACK,
            read_count: 1,
            write_count: 1,
            ..Self::default()
        }
    }

    /// Replaces the queue flags.
    ///
    /// Most callers should prefer [`QueueOptions::reader`],
    /// [`QueueOptions::writer`], or [`QueueOptions::read_write`] rather than
    /// building flag sets from scratch.
    pub fn flags(mut self, flags: QueueFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Sets the requested reader handle count.
    ///
    /// Advanced callers may request counts other than `1`, but most
    /// application-facing code should leave the default alone.
    pub fn read_count(mut self, value: i32) -> Self {
        self.read_count = value;
        self
    }

    /// Sets the requested writer handle count.
    ///
    /// Advanced callers may request counts other than `1`, but most
    /// application-facing code should leave the default alone.
    pub fn write_count(mut self, value: i32) -> Self {
        self.write_count = value;
        self
    }

    /// Sets the requested administrative handle count.
    ///
    /// Administrative handles are uncommon in normal producer/consumer
    /// applications and are mostly relevant to broker tooling.
    pub fn admin_count(mut self, value: i32) -> Self {
        self.admin_count = value;
        self
    }

    /// Sets `max_unconfirmed_messages`.
    ///
    /// This is one half of BlazingMQ consumer flow control.  Once the broker
    /// has delivered this many unconfirmed messages to the consumer, it pauses
    /// further delivery until confirmations free capacity.
    pub fn max_unconfirmed_messages(mut self, value: i64) -> Self {
        self.max_unconfirmed_messages = Some(value);
        self
    }

    /// Sets `max_unconfirmed_bytes`.
    ///
    /// This complements [`QueueOptions::max_unconfirmed_messages`] by limiting
    /// outstanding delivery based on payload volume rather than message count.
    pub fn max_unconfirmed_bytes(mut self, value: i64) -> Self {
        self.max_unconfirmed_bytes = Some(value);
        self
    }

    /// Sets the consumer priority.
    ///
    /// Consumer priority participates in broker-side routing decisions when
    /// multiple readers are attached to the same queue.
    pub fn consumer_priority(mut self, value: i32) -> Self {
        self.consumer_priority = Some(value);
        self
    }

    /// Sets the consumer priority count.
    ///
    /// This is the secondary weighting within a priority tier.
    pub fn consumer_priority_count(mut self, value: i32) -> Self {
        self.consumer_priority_count = Some(value);
        self
    }

    /// Sets the application id used for fanout routing.
    ///
    /// For non-fanout queues the broker typically treats `"__default"` as the
    /// default stream identity.
    pub fn app_id(mut self, value: impl Into<String>) -> Self {
        self.app_id = Some(value.into());
        self
    }

    /// Replaces the subscription list.
    ///
    /// Subscription expressions let consumers declare which messages they are
    /// prepared to receive based on message properties.
    pub fn subscriptions(mut self, value: Vec<Subscription>) -> Self {
        self.subscriptions = value;
        self.subscriptions_set = true;
        self
    }

    /// Explicitly clears all subscriptions during queue reconfiguration.
    ///
    /// This is distinct from simply omitting subscriptions from a
    /// reconfigure request, which preserves the broker-side subscription
    /// state.
    pub fn clear_subscriptions(mut self) -> Self {
        self.subscriptions.clear();
        self.subscriptions_set = true;
        self
    }

    /// Opts the queue into host-health-based suspension.
    ///
    /// When enabled and a [`HostHealthMonitor`] reports that the host is
    /// unhealthy, the session reconfigures the queue into a paused consumer
    /// state as described by the official host-health documentation.
    pub fn suspends_on_bad_host_health(mut self, value: bool) -> Self {
        self.suspends_on_bad_host_health = value;
        self.suspends_on_bad_host_health_set = true;
        self
    }

    pub(crate) fn suspended_queue_stream_parameters(&self) -> Option<QueueStreamParameters> {
        if !self.is_reader() {
            return None;
        }

        Some(QueueStreamParameters {
            sub_id_info: self.sub_queue_id_info(),
            max_unconfirmed_messages: 0,
            max_unconfirmed_bytes: 0,
            consumer_priority: i32::MIN,
            consumer_priority_count: 0,
        })
    }

    pub(crate) fn default_queue_stream_parameters(&self) -> Option<QueueStreamParameters> {
        if !self.is_reader() {
            return None;
        }

        Some(QueueStreamParameters {
            sub_id_info: self.sub_queue_id_info(),
            max_unconfirmed_messages: self
                .max_unconfirmed_messages
                .unwrap_or(Self::DEFAULT_MAX_UNCONFIRMED_MESSAGES),
            max_unconfirmed_bytes: self
                .max_unconfirmed_bytes
                .unwrap_or(Self::DEFAULT_MAX_UNCONFIRMED_BYTES),
            consumer_priority: self
                .consumer_priority
                .unwrap_or(Self::DEFAULT_CONSUMER_PRIORITY),
            consumer_priority_count: self
                .consumer_priority_count
                .unwrap_or(Self::DEFAULT_CONSUMER_PRIORITY_COUNT),
        })
    }

    fn build_stream_parameters(&self, allow_empty_subscriptions: bool) -> Option<StreamParameters> {
        if self.subscriptions.is_empty() && !allow_empty_subscriptions {
            return None;
        }
        let default_consumer = ConsumerInfo {
            max_unconfirmed_messages: self
                .max_unconfirmed_messages
                .unwrap_or(Self::DEFAULT_MAX_UNCONFIRMED_MESSAGES),
            max_unconfirmed_bytes: self
                .max_unconfirmed_bytes
                .unwrap_or(Self::DEFAULT_MAX_UNCONFIRMED_BYTES),
            consumer_priority: self
                .consumer_priority
                .unwrap_or(Self::DEFAULT_CONSUMER_PRIORITY),
            consumer_priority_count: self
                .consumer_priority_count
                .unwrap_or(Self::DEFAULT_CONSUMER_PRIORITY_COUNT),
        };

        Some(StreamParameters {
            app_id: self
                .app_id
                .clone()
                .unwrap_or_else(|| "__default".to_string()),
            subscriptions: self
                .subscriptions
                .iter()
                .cloned()
                .map(|mut subscription| {
                    if subscription.consumers.is_empty() {
                        subscription.consumers.push(default_consumer.clone());
                    }
                    subscription
                })
                .collect(),
        })
    }

    pub(crate) fn open_stream_parameters(&self) -> Option<StreamParameters> {
        self.build_stream_parameters(false)
    }

    pub(crate) fn configure_stream_parameters(&self) -> Option<StreamParameters> {
        if !self.has_explicit_subscriptions() {
            return None;
        }
        self.build_stream_parameters(true)
    }

    pub(crate) fn to_open_queue_options(&self) -> OpenQueueOptions {
        OpenQueueOptions {
            handle: QueueHandleConfig {
                sub_id_info: self.sub_queue_id_info(),
                flags: self.flags.bits(),
                read_count: self.read_count,
                write_count: self.write_count,
                admin_count: self.admin_count,
            },
            configure_queue_stream: self.default_queue_stream_parameters(),
            configure_stream: self.open_stream_parameters(),
        }
    }

    pub(crate) fn merged_for_reconfigure(&self, patch: &Self) -> Result<Self> {
        if patch.has_explicit_suspends_on_bad_host_health()
            && patch.suspends_on_bad_host_health != self.suspends_on_bad_host_health
        {
            return Err(Error::InvalidConfiguration(
                "suspends_on_bad_host_health cannot be reconfigured after open".to_string(),
            ));
        }

        let mut merged = self.clone();
        merged.max_unconfirmed_messages = patch
            .max_unconfirmed_messages
            .or(merged.max_unconfirmed_messages);
        merged.max_unconfirmed_bytes = patch.max_unconfirmed_bytes.or(merged.max_unconfirmed_bytes);
        merged.consumer_priority = patch.consumer_priority.or(merged.consumer_priority);
        merged.consumer_priority_count = patch
            .consumer_priority_count
            .or(merged.consumer_priority_count);
        if let Some(app_id) = &patch.app_id {
            merged.app_id = Some(app_id.clone());
        }
        if patch.has_explicit_subscriptions() {
            merged.subscriptions = patch.subscriptions.clone();
            merged.subscriptions_set = true;
        }
        Ok(merged)
    }

    fn is_reader(&self) -> bool {
        self.flags.contains(QueueFlags::READ) || self.read_count > 0
    }

    fn has_explicit_subscriptions(&self) -> bool {
        self.subscriptions_set || !self.subscriptions.is_empty()
    }

    fn has_explicit_suspends_on_bad_host_health(&self) -> bool {
        self.suspends_on_bad_host_health_set || self.suspends_on_bad_host_health
    }

    fn sub_queue_id_info(&self) -> Option<SubQueueIdInfo> {
        self.app_id.as_ref().map(|app_id| SubQueueIdInfo {
            sub_id: if self.is_reader() {
                Self::INITIAL_FANOUT_SUBQUEUE_ID
            } else {
                0
            },
            app_id: app_id.clone(),
        })
    }
}

/// Session-wide configuration for [`crate::session::Session`].
///
/// The settings here answer three different questions:
///
/// - how should the client connect and time out?
/// - what extra runtime features should be enabled?
/// - how should the session expose observability and backpressure?
///
/// Most applications start from [`SessionOptions::default`] and then override
/// the broker address plus whichever optional features are relevant for their
/// environment.
#[derive(Clone)]
pub struct SessionOptions {
    /// Broker socket address, typically `host:port`.
    pub broker_addr: String,
    /// Timeout for generic request/response operations.
    pub request_timeout: Duration,
    /// Timeout for transport connection and negotiation.
    pub connect_timeout: Duration,
    /// Timeout for queue open requests.
    pub open_queue_timeout: Duration,
    /// Timeout for queue configure requests.
    pub configure_queue_timeout: Duration,
    /// Timeout for queue close requests.
    pub close_queue_timeout: Duration,
    /// Timeout for broker disconnect requests.
    pub disconnect_timeout: Duration,
    /// Timeout for individual frame writes.
    pub channel_write_timeout: Duration,
    /// Maximum time spent waiting for graceful disconnect completion.
    pub linger_timeout: Duration,
    /// Whether to attempt anonymous authentication automatically after connect.
    pub authenticate_anonymous: bool,
    /// Authentication provider used after connect.
    pub auth_provider: Option<Arc<dyn AuthProvider>>,
    /// Optional process name advertised during negotiation.
    pub process_name_override: Option<String>,
    /// Optional host name advertised during negotiation.
    pub host_name_override: Option<String>,
    /// Session id advertised to the broker and used in GUID generation.
    pub session_id: i32,
    /// Feature string advertised during negotiation.
    pub features: String,
    /// Optional user agent advertised during negotiation.
    pub user_agent: Option<String>,
    /// Advisory processing thread count retained for parity with other SDKs.
    pub num_processing_threads: usize,
    /// Buffer size used while reading frames from the broker.
    pub blob_buffer_size: usize,
    /// Optional maximum frame size accepted by the write path.
    pub channel_high_watermark: Option<u64>,
    /// Low watermark for slow-consumer event notifications.
    pub event_queue_low_watermark: usize,
    /// High watermark and broadcast capacity for event notifications.
    pub event_queue_high_watermark: usize,
    /// Optional interval for periodic internal stats logging.
    pub stats_dump_interval: Option<Duration>,
    /// Optional host health monitor installed on the session.
    pub host_health_monitor: Option<Arc<dyn HostHealthMonitor>>,
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
        }
    }
}

impl SessionOptions {
    /// Sets the broker socket address.
    ///
    /// This is the TCP endpoint the session will connect to for negotiation and
    /// all later traffic.
    pub fn broker_addr(mut self, value: impl Into<String>) -> Self {
        self.broker_addr = value.into();
        self
    }

    /// Sets the generic request timeout.
    ///
    /// This timeout is used for operations that wait on broker responses or on
    /// session/queue event streams.
    pub fn request_timeout(mut self, value: Duration) -> Self {
        self.request_timeout = value;
        self
    }

    /// Sets the transport connect timeout.
    ///
    /// This bounds the initial `TcpStream::connect` plus negotiation phase.
    pub fn connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = value;
        self
    }

    /// Sets the queue open timeout.
    ///
    /// This applies to the broker response for the `OpenQueue` request itself;
    /// follow-up configure requests use the configure timeout below.
    pub fn open_queue_timeout(mut self, value: Duration) -> Self {
        self.open_queue_timeout = value;
        self
    }

    /// Sets the queue configure timeout.
    ///
    /// This is used for both `ConfigureQueueStream` and `ConfigureStream`
    /// requests.
    pub fn configure_queue_timeout(mut self, value: Duration) -> Self {
        self.configure_queue_timeout = value;
        self
    }

    /// Sets the queue close timeout.
    ///
    /// This bounds the final `CloseQueue` request in the close workflow.
    pub fn close_queue_timeout(mut self, value: Duration) -> Self {
        self.close_queue_timeout = value;
        self
    }

    /// Sets the broker disconnect timeout.
    ///
    /// This is the graceful shutdown timeout for the broker-side `Disconnect`
    /// request.
    pub fn disconnect_timeout(mut self, value: Duration) -> Self {
        self.disconnect_timeout = value;
        self
    }

    /// Sets the per-frame write timeout.
    ///
    /// If the socket stays blocked longer than this while writing a frame, the
    /// client surfaces [`crate::Error::BandwidthLimit`].
    pub fn channel_write_timeout(mut self, value: Duration) -> Self {
        self.channel_write_timeout = value;
        self
    }

    /// Sets the timeout used by [`crate::session::Session::linger`].
    pub fn linger_timeout(mut self, value: Duration) -> Self {
        self.linger_timeout = value;
        self
    }

    /// Enables or disables automatic anonymous authentication after connect.
    ///
    /// If [`SessionOptions::auth_provider`] is also configured, the provider
    /// takes precedence and this flag is ignored.
    pub fn authenticate_anonymous(mut self, value: bool) -> Self {
        self.authenticate_anonymous = value;
        self
    }

    /// Installs an authentication provider.
    ///
    /// The provider is invoked after negotiation and before the session begins
    /// normal queue operations.
    pub fn auth_provider(mut self, value: Arc<dyn AuthProvider>) -> Self {
        self.auth_provider = Some(value);
        self
    }

    /// Overrides the process name advertised during negotiation.
    pub fn process_name_override(mut self, value: impl Into<String>) -> Self {
        self.process_name_override = Some(value.into());
        self
    }

    /// Overrides the host name advertised during negotiation.
    pub fn host_name_override(mut self, value: impl Into<String>) -> Self {
        self.host_name_override = Some(value.into());
        self
    }

    /// Sets the advertised session id.
    ///
    /// This value also participates in locally generated message GUIDs.
    pub fn session_id(mut self, value: i32) -> Self {
        self.session_id = value;
        self
    }

    /// Sets the advertised user agent.
    pub fn user_agent(mut self, value: impl Into<String>) -> Self {
        self.user_agent = Some(value.into());
        self
    }

    /// Sets the advisory processing thread count.
    ///
    /// The Rust client is Tokio-based and does not literally spawn a matching
    /// number of queue-processing threads, but the field is retained for parity
    /// with the concepts exposed by the other SDKs.
    pub fn num_processing_threads(mut self, value: usize) -> Self {
        self.num_processing_threads = value.max(1);
        self
    }

    /// Sets the frame read buffer size.
    ///
    /// Larger values reduce reallocations for large broker frames at the cost
    /// of more baseline memory reservation.
    pub fn blob_buffer_size(mut self, value: usize) -> Self {
        self.blob_buffer_size = value.max(1);
        self
    }

    /// Sets the maximum frame size accepted by the write path.
    ///
    /// This is a client-side backpressure guard, not a broker protocol limit.
    pub fn channel_high_watermark(mut self, value: u64) -> Self {
        self.channel_high_watermark = Some(value);
        self
    }

    /// Sets the low and high event queue watermarks.
    ///
    /// These control when the session emits `SlowConsumerHighWatermark` and
    /// `SlowConsumerNormal` notifications.  The high watermark also determines
    /// the broadcast channel capacity used for session events.
    pub fn event_queue_watermarks(mut self, low: usize, high: usize) -> Self {
        let high = high.max(1);
        self.event_queue_low_watermark = low.min(high);
        self.event_queue_high_watermark = high;
        self
    }

    /// Enables periodic internal stats dumping.
    ///
    /// This is intended for diagnostics and development rather than normal
    /// application behavior.
    pub fn stats_dump_interval(mut self, value: Duration) -> Self {
        self.stats_dump_interval = Some(value);
        self
    }

    /// Installs a host health monitor.
    ///
    /// Installing a monitor alone does not suspend any queues.  Individual
    /// queues must also opt in through
    /// [`QueueOptions::suspends_on_bad_host_health`].
    pub fn host_health_monitor(mut self, value: Arc<dyn HostHealthMonitor>) -> Self {
        self.host_health_monitor = Some(value);
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
}

/// Message posted through a [`crate::session::Queue`].
///
/// This is the high-level producer message type.  In addition to raw payload,
/// it carries the pieces BlazingMQ uses for richer routing and observation:
/// message properties, an optional producer correlation id, an optional
/// explicit GUID, and optional compression.
#[derive(Debug, Clone)]
pub struct PostMessage {
    /// Application payload.
    pub payload: Bytes,
    /// Message properties delivered alongside the payload.
    pub properties: MessageProperties,
    /// Optional producer correlation id echoed in acknowledgements.
    pub correlation_id: Option<CorrelationId>,
    /// Optional explicit message GUID. When absent, one is generated.
    pub message_guid: Option<MessageGuid>,
    /// Compression applied to the payload.
    pub compression: CompressionAlgorithm,
}

impl PostMessage {
    /// Creates a message with the provided payload.
    ///
    /// The payload may be any byte source accepted by [`Bytes`].  Properties,
    /// correlation id, explicit GUID, and compression can be added with the
    /// builder-style methods below.
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
            properties: MessageProperties::default(),
            correlation_id: None,
            message_guid: None,
            compression: CompressionAlgorithm::None,
        }
    }

    /// Replaces the message properties.
    ///
    /// Properties are the broker-visible metadata used by subscriptions and
    /// other routing logic.
    pub fn properties(mut self, value: MessageProperties) -> Self {
        self.properties = value;
        self
    }

    /// Sets the producer correlation id.
    ///
    /// When the queue is opened with the ACK flag, the session requires every
    /// outbound post to carry a correlation id so later acknowledgements can be
    /// matched back to application state.
    pub fn correlation_id(mut self, value: CorrelationId) -> Self {
        self.correlation_id = Some(value);
        self
    }

    /// Sets an explicit message GUID.
    ///
    /// Most applications should let the SDK generate GUIDs automatically unless
    /// they are coordinating with an external deduplication or tracing system.
    pub fn message_guid(mut self, value: MessageGuid) -> Self {
        self.message_guid = Some(value);
        self
    }

    /// Sets the payload compression algorithm.
    ///
    /// Compression affects only the payload encoding, not the control-plane
    /// request flow.
    pub fn compression(mut self, value: CompressionAlgorithm) -> Self {
        self.compression = value;
        self
    }
}

/// Mutable batch of messages to post together.
///
/// Batching is a first-class concept in the BlazingMQ wire protocol, so it is
/// also a first-class concept in this client surface.
#[derive(Debug, Clone, Default)]
pub struct PostBatch {
    messages: Vec<PostMessage>,
}

impl PostBatch {
    /// Creates an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a message to the batch.
    pub fn push(&mut self, message: PostMessage) {
        self.messages.push(message);
    }

    /// Returns `true` when the batch contains no messages.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Consumes the batch and returns the contained messages.
    pub fn into_messages(self) -> Vec<PostMessage> {
        self.messages
    }
}

/// Prevalidated batch ready to post later.
///
/// A packed batch represents "messages that were accepted by the queue's local
/// validation rules at packing time".  It is useful when an application wants
/// to separate payload preparation from the eventual network write.
#[derive(Debug, Clone, Default)]
pub struct PackedPostBatch {
    pub(crate) messages: Vec<PostMessage>,
}

impl PackedPostBatch {
    /// Creates an empty packed batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a message to the packed batch.
    pub fn push(&mut self, message: PostMessage) {
        self.messages.push(message);
    }

    /// Returns `true` when the batch contains no messages.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Consumes the batch and returns the contained messages.
    pub fn into_messages(self) -> Vec<PostMessage> {
        self.messages
    }
}

/// Single consumer confirmation item.
///
/// Confirmations are the consumer-side acknowledgement path described in the
/// protocol docs: they tell the broker a pushed message was processed
/// successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfirmMessage {
    /// Message GUID being confirmed.
    pub message_guid: MessageGuid,
    /// Sub-queue identifier associated with the delivered message.
    pub sub_queue_id: u32,
}

/// Compact handle for confirming a delivered message later.
///
/// Applications usually obtain this from
/// [`crate::session::ReceivedMessage::confirmation_cookie`] and treat it as the
/// canonical confirmation token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageConfirmationCookie {
    /// Queue id on which the message was delivered.
    pub queue_id: u32,
    /// Message GUID to confirm.
    pub message_guid: MessageGuid,
    /// Sub-queue id to confirm against.
    pub sub_queue_id: u32,
}

impl MessageConfirmationCookie {
    /// Creates a cookie from queue, GUID, and sub-queue identifiers.
    pub const fn new(queue_id: u32, message_guid: MessageGuid, sub_queue_id: u32) -> Self {
        Self {
            queue_id,
            message_guid,
            sub_queue_id,
        }
    }
}

/// Mutable batch of consumer confirmations.
///
/// This exists because the binary `CONFIRM` event format is batch-oriented in
/// the same way the `PUT` event format is.
#[derive(Debug, Clone, Default)]
pub struct ConfirmBatch {
    messages: Vec<ConfirmMessage>,
}

impl ConfirmBatch {
    /// Creates an empty confirmation batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a confirmation item.
    pub fn push(&mut self, message_guid: MessageGuid, sub_queue_id: u32) {
        self.messages.push(ConfirmMessage {
            message_guid,
            sub_queue_id,
        });
    }

    /// Appends a confirmation derived from a cookie.
    pub fn push_cookie(&mut self, cookie: MessageConfirmationCookie) {
        self.push(cookie.message_guid, cookie.sub_queue_id);
    }

    /// Consumes the batch and returns the contained confirmations.
    pub fn into_messages(self) -> Vec<ConfirmMessage> {
        self.messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Expression, ExpressionVersion};

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
            mapped.handle.sub_id_info.as_ref().unwrap().app_id,
            "consumer-a"
        );
        assert_eq!(mapped.handle.sub_id_info.as_ref().unwrap().sub_id, 1);
        assert_eq!(
            mapped
                .configure_queue_stream
                .as_ref()
                .unwrap()
                .sub_id_info
                .as_ref()
                .unwrap()
                .app_id,
            "consumer-a"
        );
        assert_eq!(
            mapped
                .configure_queue_stream
                .as_ref()
                .unwrap()
                .sub_id_info
                .as_ref()
                .unwrap()
                .sub_id,
            1
        );
        assert!(mapped.configure_stream.is_none());
    }

    #[test]
    fn writer_open_options_do_not_include_queue_stream_config() {
        let mapped = QueueOptions::writer().to_open_queue_options();
        assert!(mapped.configure_queue_stream.is_none());
    }

    #[test]
    fn subscriptions_still_emit_configure_stream_for_app_id_consumers() {
        let mapped = QueueOptions::reader()
            .app_id("consumer-a")
            .subscriptions(vec![Subscription {
                s_id: 7,
                expression: Expression {
                    version: ExpressionVersion::Version1,
                    text: "x == 1".to_string(),
                },
                consumers: Vec::new(),
            }])
            .to_open_queue_options();

        let stream = mapped.configure_stream.expect("configure_stream");
        assert_eq!(stream.app_id, "consumer-a");
        assert_eq!(stream.subscriptions.len(), 1);
        assert_eq!(stream.subscriptions[0].s_id, 7);
    }

    #[test]
    fn reader_open_options_include_sdk_defaults() {
        let mapped = QueueOptions::reader().to_open_queue_options();
        let stream = mapped.configure_queue_stream.unwrap();
        assert_eq!(stream.max_unconfirmed_messages, 1000);
        assert_eq!(stream.max_unconfirmed_bytes, 33_554_432);
        assert_eq!(stream.consumer_priority, 0);
        assert_eq!(stream.consumer_priority_count, 1);
    }

    #[test]
    fn subscriptions_inherit_consumer_settings_when_missing_consumer_info() {
        let mapped = QueueOptions::reader()
            .max_unconfirmed_messages(16)
            .max_unconfirmed_bytes(2048)
            .consumer_priority(7)
            .consumer_priority_count(3)
            .subscriptions(vec![Subscription {
                s_id: 42,
                expression: Expression {
                    version: ExpressionVersion::Version1,
                    text: "x > 0".to_string(),
                },
                consumers: Vec::new(),
            }])
            .to_open_queue_options();

        let subscription = &mapped.configure_stream.unwrap().subscriptions[0];
        assert_eq!(subscription.consumers.len(), 1);
        assert_eq!(subscription.consumers[0].max_unconfirmed_messages, 16);
        assert_eq!(subscription.consumers[0].max_unconfirmed_bytes, 2048);
        assert_eq!(subscription.consumers[0].consumer_priority, 7);
        assert_eq!(subscription.consumers[0].consumer_priority_count, 3);
    }

    #[test]
    fn explicit_subscription_clear_emits_empty_configure_stream() {
        let stream = QueueOptions::reader()
            .clear_subscriptions()
            .configure_stream_parameters()
            .expect("configure_stream");

        assert_eq!(stream.app_id, "__default");
        assert!(stream.subscriptions.is_empty());
    }

    #[test]
    fn reconfigure_merge_preserves_unspecified_fields_and_allows_subscription_clear() {
        let current = QueueOptions::reader()
            .max_unconfirmed_messages(16)
            .consumer_priority(7)
            .subscriptions(vec![Subscription {
                s_id: 42,
                expression: Expression {
                    version: ExpressionVersion::Version1,
                    text: "x > 0".to_string(),
                },
                consumers: Vec::new(),
            }]);
        let patch = QueueOptions::reader().clear_subscriptions();

        let merged = current.merged_for_reconfigure(&patch).expect("merged");
        assert_eq!(merged.max_unconfirmed_messages, Some(16));
        assert_eq!(merged.consumer_priority, Some(7));
        assert!(merged.subscriptions.is_empty());
        assert!(merged.configure_stream_parameters().is_some());
    }

    #[test]
    fn reconfigure_merge_rejects_host_health_mode_change() {
        let current = QueueOptions::reader().suspends_on_bad_host_health(true);
        let patch = QueueOptions::reader().suspends_on_bad_host_health(false);

        let err = current.merged_for_reconfigure(&patch).unwrap_err();
        assert!(matches!(err, Error::InvalidConfiguration(_)));
    }
}
