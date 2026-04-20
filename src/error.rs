use crate::schema::Status;
use thiserror::Error;

/// Convenient result alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors returned by transport, protocol, and session operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Wrapper for lower-level I/O failures.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Wrapper for JSON serialization and deserialization failures.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// A broker request or event wait exceeded the configured timeout.
    #[error("timed out waiting for broker response")]
    Timeout,

    /// A waiting task lost the corresponding request or event source.
    #[error("request was canceled before a broker response arrived")]
    RequestCanceled,

    /// The received frame violated an invariant with a static explanation.
    #[error("invalid BlazingMQ frame: {0}")]
    Protocol(&'static str),

    /// The received frame violated an invariant with a dynamic explanation.
    #[error("invalid BlazingMQ frame: {0}")]
    ProtocolMessage(String),

    /// The broker returned a non-success status payload.
    #[error("broker returned an error status: {0:?}")]
    BrokerStatus(Status),

    /// A schema event was decoded successfully but was not the expected shape.
    #[error("unexpected schema event: {0}")]
    UnexpectedSchema(&'static str),

    /// The write side of the transport is no longer available.
    #[error("writer task is closed")]
    WriterClosed,

    /// A write exceeded the configured backpressure limit or timeout.
    #[error("channel write exceeded the configured backpressure timeout")]
    BandwidthLimit,

    /// A queue URI did not match the `bmq://<domain>/<queue>` form.
    #[error("invalid queue uri: {0}")]
    InvalidUri(String),

    /// Session or queue configuration was internally inconsistent.
    #[error("invalid session configuration: {0}")]
    InvalidConfiguration(String),

    /// The queue is temporarily suspended, typically due to host health.
    #[error("queue is suspended: {0}")]
    QueueSuspended(String),

    /// The queue has already been closed and cannot accept new work.
    #[error("queue is closed: {0}")]
    QueueClosed(String),

    /// The requested queue operation requires write access.
    #[error("queue is not opened for writing: {0}")]
    QueueReadOnly(String),

    /// BlazingMQ does not accept empty message payloads.
    #[error("message payload must not be empty")]
    EmptyPayload,

    /// The queue requires correlation ids for every posted message.
    #[error("queue requires a correlation id for every message: {0}")]
    MissingCorrelationId(String),
}
