use crate::schema::Status;
use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("timed out waiting for broker response")]
    Timeout,

    #[error("request was canceled before a broker response arrived")]
    RequestCanceled,

    #[error("invalid BlazingMQ frame: {0}")]
    Protocol(&'static str),

    #[error("invalid BlazingMQ frame: {0}")]
    ProtocolMessage(String),

    #[error("broker returned an error status: {0:?}")]
    BrokerStatus(Status),

    #[error("unexpected schema event: {0}")]
    UnexpectedSchema(&'static str),

    #[error("writer task is closed")]
    WriterClosed,

    #[error("channel write exceeded the configured backpressure timeout")]
    BandwidthLimit,

    #[error("invalid queue uri: {0}")]
    InvalidUri(String),

    #[error("invalid session configuration: {0}")]
    InvalidConfiguration(String),

    #[error("queue is suspended: {0}")]
    QueueSuspended(String),

    #[error("queue is closed: {0}")]
    QueueClosed(String),

    #[error("queue is not opened for writing: {0}")]
    QueueReadOnly(String),

    #[error("message payload must not be empty")]
    EmptyPayload,

    #[error("queue requires a correlation id for every message: {0}")]
    MissingCorrelationId(String),
}
