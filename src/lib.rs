//! Async Rust client primitives for building a full-featured
//! [BlazingMQ](https://bloomberg.github.io/blazingmq/) client.
//!
//! # Architecture
//!
//! The crate is intentionally split along the same boundary described by the
//! official
//! [Client/Broker Protocol](https://bloomberg.github.io/blazingmq/docs/architecture/client_broker_protocol/)
//! document:
//!
//! - [`client`] is the low-level protocol layer. It performs negotiation,
//!   emits decoded transport events, sends control requests, and encodes the
//!   binary `PUT` / `CONFIRM` data path directly.
//! - [`session`] is the higher-level client library layer. It builds the
//!   operational behavior expected from a production SDK: reconnect,
//!   queue-state restoration, queue-local event routing, host-health-aware
//!   suspension, message confirmation helpers, and tracing hooks.
//! - [`schema`] models the control-plane messages (`OpenQueue`,
//!   `ConfigureQueueStream`, `Disconnect`, authentication, negotiation, and
//!   related responses).
//! - [`wire`] models the binary data-plane structures (`PUT`, `PUSH`, `ACK`,
//!   and `CONFIRM`) together with the event header and message-property codec.
//! - [`types`] contains the user-facing option builders and extension traits
//!   that tie everything together.
//!
//! This split is useful because some applications only need a protocol engine,
//! while others want a batteries-included SDK surface.  The higher-level
//! [`Session`] and [`Queue`] types are what most applications should start
//! with.
//!
//! # How the Pieces Fit Together
//!
//! A typical end-to-end flow looks like this:
//!
//! 1. Construct [`SessionOptions`] to describe transport timeouts,
//!    authentication, host-health monitoring, event backpressure, and tracing.
//! 2. Call [`Session::connect`] or [`Session::start`] to establish TCP,
//!    negotiate protocol features, and optionally authenticate.
//! 3. Call [`Session::open_queue`] with [`QueueOptions`].  Under the hood this
//!    performs the protocol-defined two-step open sequence: `OpenQueue`
//!    followed by `ConfigureQueueStream`, and optionally `ConfigureStream`
//!    when application id or subscriptions are configured.
//! 4. Use [`Queue::post`], [`Queue::post_batch`], or [`Queue::put_builder`] on
//!    producer queues, and [`Queue::next_message`], [`Queue::events`], or
//!    [`Queue::confirm`] on consumer queues.
//! 5. Let the session layer handle reconnects and queue restoration in a way
//!    that follows the official
//!    [High Availability in Client Libraries](https://bloomberg.github.io/blazingmq/docs/architecture/high_availability_sdk/)
//!    guidance.
//!
//! The crate also exposes the lower-level pieces required for advanced use
//! cases: custom reconnect orchestration, protocol debugging, schema or wire
//! inspection, or test doubles that avoid a live broker.
//!
//! # Feature Mapping
//!
//! Several BlazingMQ feature documents map directly to types in this crate:
//!
//! - [Consumer Flow Control](https://bloomberg.github.io/blazingmq/docs/features/consumer_flow_control/)
//!   maps to [`QueueOptions::max_unconfirmed_messages`] and
//!   [`QueueOptions::max_unconfirmed_bytes`].
//! - [Subscriptions](https://bloomberg.github.io/blazingmq/docs/features/subscriptions/)
//!   map to [`Subscription`], [`Expression`], and [`QueueOptions::subscriptions`].
//! - [Host Health Monitoring](https://bloomberg.github.io/blazingmq/docs/features/host_health_monitoring/)
//!   maps to [`HostHealthMonitor`], [`ManualHostHealthMonitor`], and
//!   [`QueueOptions::suspends_on_bad_host_health`].
//! - [Distributed Trace](https://bloomberg.github.io/blazingmq/docs/features/distributed_trace/)
//!   maps to [`TraceSink`], [`DistributedTracer`], and related traits.
//! - [Compression](https://bloomberg.github.io/blazingmq/docs/features/compression/)
//!   maps to [`CompressionAlgorithm`].
#![warn(missing_docs)]

mod ber;
pub mod event;

/// Low-level asynchronous client for direct protocol access.
///
/// Choose this module when you want to work directly with queue ids, transport
/// events, and control/data-plane requests.
pub mod client;
/// Error and result types shared across the crate.
///
/// These are used by both the low-level protocol layer and the higher-level
/// session API.
pub mod error;
/// In-memory mocks for queue- and session-level tests.
///
/// These are intended for application tests that want the high-level client
/// shape without a live broker.
pub mod mock;
/// Control-plane schema types mirrored from the BlazingMQ protocol.
///
/// This module contains the typed representation of negotiation,
/// authentication, queue open/close, and configure requests and responses.
pub mod schema;
/// High-level session and queue APIs built on top of the transport client.
///
/// Most applications should start here.
pub mod session;
/// User-facing helper types, options, and extension traits.
///
/// This module contains queue/session options plus the extension traits used
/// for host health, authentication, and tracing.
pub mod types;
/// Data-plane wire structures and codec helpers.
///
/// This module models the binary `PUT`, `PUSH`, `ACK`, and `CONFIRM` traffic
/// used on the message data path.
pub mod wire;

pub use client::{
    Client, ClientConfig, InboundSchemaEvent, OpenQueueOptions, OutboundPut, QueueHandle,
    QueueHandleConfig, SessionEvent as TransportEvent, queue_flags,
};
pub use error::{Error, Result};
pub use event::{EventReceiver, RecvError as EventRecvError, TryRecvError as EventTryRecvError};
pub use mock::{
    MockQueue, MockSession, RecordedCloseQueue, RecordedConfirm, RecordedOpenQueue, RecordedPost,
    RecordedReconfigureQueue,
};
pub use schema::{
    AdminCommand, AdminCommandResponse, AuthenticationMessage, AuthenticationPayload,
    AuthenticationRequest, AuthenticationResponse, BrokerResponse, ClientIdentity, ClientLanguage,
    ClientType, ClusterMessage, ConfigureQueueStream, ConfigureQueueStreamResponse,
    ConfigureStream, ConfigureStreamResponse, ConsumerInfo, ControlMessage, ControlPayload, Empty,
    Expression, ExpressionVersion, GuidInfo, NegotiationMessage, NegotiationPayload, OpenQueue,
    OpenQueueResponse, QueueHandleParameters, QueueStreamParameters, RoutingConfiguration, Status,
    StatusCategory, StreamParameters, SubQueueIdInfo, Subscription,
};
pub use session::{
    Acknowledgement, CloseQueueStatus, ConfigureQueueStatus, ConfirmBuilder, OpenQueueStatus,
    PutBuilder, Queue, QueueEvent, ReceivedMessage, Session, SessionEvent,
};
pub use types::{
    AuthProvider, AuthRequestFuture, ConfirmBatch, ConfirmMessage, CorrelationId,
    CorrelationIdGenerator, DistributedTraceContext, DistributedTraceScope, DistributedTraceSpan,
    DistributedTracer, HostHealthMonitor, HostHealthState, ManualHostHealthMonitor,
    MessageConfirmationCookie, PackedPostBatch, PostBatch, PostMessage, QueueFlags, QueueOptions,
    SessionOptions, TraceBaggage, TraceOperation, TraceOperationKind, TraceSink, Uri, UriBuilder,
};
pub use wire::{
    AckHeader, AckMessage, CompressionAlgorithm, ConfirmHeader,
    ConfirmMessage as WireConfirmMessage, EncodingType, EventHeader, EventType, MessageGuid,
    MessageGuidGenerator, MessageProperties, MessageProperty, MessagePropertyType,
    MessagePropertyValue, PushHeader, PushHeaderFlags, PushMessage, PutHeader, PutHeaderFlags,
    RdaInfo, SubQueueInfo,
};
