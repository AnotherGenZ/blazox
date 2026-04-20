//! Control-plane schema types mirrored from the BlazingMQ client/broker
//! protocol.
//!
//! BlazingMQ uses a schema-based control plane for infrequent operations such
//! as negotiation, authentication, queue open/close, stream configuration, and
//! administrative commands.  The official protocol guide describes these as
//! "schema messages" to distinguish them from the binary data-plane message
//! families used for `PUT`, `PUSH`, `ACK`, and `CONFIRM`.
//!
//! This module is the typed representation of that control plane.  Most
//! application code will interact with these concepts indirectly through
//! [`crate::client::Client`] or [`crate::session::Session`], but the concrete
//! message types are still useful for:
//!
//! - protocol debugging
//! - custom control-plane tooling
//! - BER/JSON codec tests
//! - reasoning about how queue options map to wire requests

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Empty schema payload.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Empty {}

/// Standard broker status categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusCategory {
    /// Operation succeeded.
    #[serde(rename = "E_SUCCESS")]
    Success,
    /// Broker returned an unspecified error.
    #[serde(rename = "E_UNKNOWN")]
    Unknown,
    /// Operation timed out.
    #[serde(rename = "E_TIMEOUT")]
    Timeout,
    /// Session was not connected.
    #[serde(rename = "E_NOT_CONNECTED")]
    NotConnected,
    /// Operation was canceled.
    #[serde(rename = "E_CANCELED")]
    Canceled,
    /// Operation is unsupported by the broker.
    #[serde(rename = "E_NOT_SUPPORTED")]
    NotSupported,
    /// Broker refused the request.
    #[serde(rename = "E_REFUSED")]
    Refused,
    /// Request arguments were invalid.
    #[serde(rename = "E_INVALID_ARGUMENT")]
    InvalidArgument,
    /// Operation could not be performed yet.
    #[serde(rename = "E_NOT_READY")]
    NotReady,
}

/// Broker status object carried by control responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    /// High-level status category.
    pub category: StatusCategory,
    /// Broker-defined numeric status code.
    pub code: i32,
    /// Optional human-readable message.
    #[serde(default)]
    pub message: String,
}

impl Status {
    /// Returns a success status.
    pub fn success() -> Self {
        Self {
            category: StatusCategory::Success,
            code: 0,
            message: String::new(),
        }
    }

    /// Returns `true` when the status represents success.
    pub fn is_success(&self) -> bool {
        matches!(self.category, StatusCategory::Success)
    }
}

/// Top-level control-plane message wrapper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlMessage {
    /// Request id echoed by the broker, or `None` for notifications.
    #[serde(rename = "rId", skip_serializing_if = "Option::is_none")]
    pub r_id: Option<i32>,
    /// Concrete control payload.
    #[serde(flatten)]
    pub payload: ControlPayload,
}

impl ControlMessage {
    /// Creates a request message with a request id.
    pub fn request(r_id: i32, payload: ControlPayload) -> Self {
        Self {
            r_id: Some(r_id),
            payload,
        }
    }

    /// Creates a notification without a request id.
    pub fn notification(payload: ControlPayload) -> Self {
        Self {
            r_id: None,
            payload,
        }
    }

    /// Converts a `status` payload into an error when it is not successful.
    pub fn expect_success(self) -> std::result::Result<Self, Status> {
        match &self.payload {
            ControlPayload::Status(status) if !status.is_success() => Err(status.clone()),
            _ => Ok(self),
        }
    }
}

/// All control-plane payload variants exchanged with the broker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ControlPayload {
    /// Generic status response.
    Status(Status),
    /// Disconnect request.
    Disconnect(Empty),
    /// Disconnect response.
    DisconnectResponse(Empty),
    /// Administrative command request.
    AdminCommand(AdminCommand),
    /// Administrative command response.
    AdminCommandResponse(AdminCommandResponse),
    /// Opaque cluster message payload.
    ClusterMessage(ClusterMessage),
    /// Queue open request.
    OpenQueue(OpenQueue),
    /// Queue open response.
    OpenQueueResponse(OpenQueueResponse),
    /// Queue close request.
    CloseQueue(CloseQueue),
    /// Queue close response.
    CloseQueueResponse(CloseQueueResponse),
    /// Queue stream configure request.
    ConfigureQueueStream(ConfigureQueueStream),
    /// Queue stream configure response.
    ConfigureQueueStreamResponse(ConfigureQueueStreamResponse),
    /// Subscription/application stream configure request.
    ConfigureStream(ConfigureStream),
    /// Subscription/application stream configure response.
    ConfigureStreamResponse(ConfigureStreamResponse),
}

/// Opaque cluster-message payload transported as base64 in JSON mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterMessage {
    /// Raw schema bytes for cluster messages not modeled by this crate.
    #[serde(
        default,
        serialize_with = "serialize_base64",
        deserialize_with = "deserialize_base64"
    )]
    pub raw: Vec<u8>,
}

/// Top-level negotiation message wrapper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NegotiationMessage {
    /// Concrete negotiation payload.
    #[serde(flatten)]
    pub payload: NegotiationPayload,
}

/// Negotiation payload variants exchanged during session setup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NegotiationPayload {
    /// Client identity sent by the SDK.
    ClientIdentity(ClientIdentity),
    /// Broker response sent after successful negotiation.
    BrokerResponse(BrokerResponse),
    /// Placeholder payload retained for protocol compatibility.
    PlaceHolder(Empty),
}

/// Top-level authentication message wrapper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthenticationMessage {
    /// Concrete authentication payload.
    #[serde(flatten)]
    pub payload: AuthenticationPayload,
}

/// Authentication request/response payload variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AuthenticationPayload {
    /// Authentication request sent by the client.
    AuthenticationRequest(AuthenticationRequest),
    /// Authentication response sent by the broker.
    AuthenticationResponse(AuthenticationResponse),
}

/// Administrative command request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminCommand {
    /// Command text interpreted by the broker.
    pub command: String,
}

/// Administrative command response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminCommandResponse {
    /// Human-readable command output.
    pub text: String,
}

/// Queue open request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenQueue {
    /// Queue handle parameters requested by the client.
    pub handle_parameters: QueueHandleParameters,
}

/// Queue open response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenQueueResponse {
    /// Original request echoed by the broker.
    pub original_request: OpenQueue,
    /// Broker-advertised routing configuration for the queue.
    pub routing_configuration: RoutingConfiguration,
    /// Deduplication retention window for unacknowledged puts, in milliseconds.
    #[serde(default = "default_deduplication_time_ms")]
    pub deduplication_time_ms: i32,
}

const fn default_deduplication_time_ms() -> i32 {
    300_000
}

/// Queue close request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseQueue {
    /// Queue handle parameters identifying the queue to close.
    pub handle_parameters: QueueHandleParameters,
    /// Whether this is the final close for the queue handle.
    #[serde(default)]
    pub is_final: bool,
}

/// Queue close response.
pub type CloseQueueResponse = Empty;

/// Queue stream configuration request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureQueueStream {
    /// Queue id being configured.
    #[serde(rename = "qId")]
    pub q_id: u32,
    /// Stream parameters such as flow control and consumer priority.
    pub stream_parameters: QueueStreamParameters,
}

/// Queue stream configuration response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureQueueStreamResponse {
    /// Original request echoed by the broker.
    pub request: ConfigureQueueStream,
}

/// Application/subscription stream configuration request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureStream {
    /// Queue id being configured.
    #[serde(rename = "qId")]
    pub q_id: u32,
    /// Stream parameters such as app id and subscriptions.
    pub stream_parameters: StreamParameters,
}

/// Application/subscription stream configuration response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureStreamResponse {
    /// Original request echoed by the broker.
    pub request: ConfigureStream,
}

/// Queue handle description carried by open and close requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueHandleParameters {
    /// Full queue URI.
    pub uri: String,
    /// Queue id chosen by the client.
    #[serde(rename = "qId")]
    pub q_id: u32,
    /// Optional sub-queue identification for fanout streams.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_id_info: Option<SubQueueIdInfo>,
    /// Raw queue flags.
    pub flags: u64,
    /// Requested reader handle count.
    #[serde(default)]
    pub read_count: i32,
    /// Requested writer handle count.
    #[serde(default)]
    pub write_count: i32,
    /// Requested administrative handle count.
    #[serde(default)]
    pub admin_count: i32,
}

/// Flow-control and priority parameters for a queue stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueStreamParameters {
    /// Optional sub-queue identification for fanout streams.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_id_info: Option<SubQueueIdInfo>,
    /// Maximum number of outstanding pushed messages.
    #[serde(default)]
    pub max_unconfirmed_messages: i64,
    /// Maximum number of outstanding pushed bytes.
    #[serde(default)]
    pub max_unconfirmed_bytes: i64,
    /// Consumer priority used by broker-side routing.
    #[serde(default = "default_consumer_priority")]
    pub consumer_priority: i32,
    /// Relative weight among consumers with the same priority.
    #[serde(default)]
    pub consumer_priority_count: i32,
}

/// Sub-queue identification used for fanout queues.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubQueueIdInfo {
    /// Numeric sub-queue identifier.
    #[serde(default)]
    pub sub_id: u32,
    /// Application id attached to the sub-queue.
    #[serde(default = "default_app_id")]
    pub app_id: String,
}

const fn default_consumer_priority() -> i32 {
    i32::MIN
}

fn default_app_id() -> String {
    "__default".to_string()
}

/// Application/subscription stream parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamParameters {
    /// Fanout application id, or `"__default"` for non-fanout queues.
    #[serde(default = "default_app_id")]
    pub app_id: String,
    /// Consumer subscriptions applied to the stream.
    #[serde(default)]
    pub subscriptions: Vec<Subscription>,
}

/// Version of the subscription expression language.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpressionVersion {
    /// Undefined or broker-default version.
    #[serde(rename = "E_UNDEFINED")]
    Undefined,
    /// First published expression language version.
    #[serde(rename = "E_VERSION_1")]
    Version1,
}

/// Subscription expression used by consumer routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Expression {
    /// Expression language version.
    #[serde(default = "default_expression_version")]
    pub version: ExpressionVersion,
    /// Expression text.
    pub text: String,
}

const fn default_expression_version() -> ExpressionVersion {
    ExpressionVersion::Undefined
}

/// Consumer-specific flow-control and priority settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerInfo {
    /// Maximum number of outstanding pushed messages.
    #[serde(default)]
    pub max_unconfirmed_messages: i64,
    /// Maximum number of outstanding pushed bytes.
    #[serde(default)]
    pub max_unconfirmed_bytes: i64,
    /// Consumer priority used by broker-side routing.
    #[serde(default = "default_consumer_priority")]
    pub consumer_priority: i32,
    /// Relative weight among consumers with the same priority.
    #[serde(default)]
    pub consumer_priority_count: i32,
}

/// Subscription definition attached to a stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    /// Subscription identifier.
    #[serde(rename = "sId")]
    pub s_id: u32,
    /// Expression that must match for delivery.
    pub expression: Expression,
    /// Optional per-consumer overrides for this subscription.
    #[serde(default)]
    pub consumers: Vec<ConsumerInfo>,
}

/// Broker-advertised routing configuration for a queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutingConfiguration {
    /// Raw routing flag bits.
    pub flags: u64,
}

/// Kind of client participating in negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientType {
    /// Unknown client type.
    #[serde(rename = "E_UNKNOWN")]
    Unknown,
    /// Regular TCP client.
    #[serde(rename = "E_TCPCLIENT")]
    TcpClient,
    /// Broker peer.
    #[serde(rename = "E_TCPBROKER")]
    TcpBroker,
    /// Administrative client.
    #[serde(rename = "E_TCPADMIN")]
    TcpAdmin,
}

/// SDK language advertised during negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientLanguage {
    /// Unknown language.
    #[serde(rename = "E_UNKNOWN")]
    Unknown,
    /// C++ SDK.
    #[serde(rename = "E_CPP")]
    Cpp,
    /// Java SDK.
    #[serde(rename = "E_JAVA")]
    Java,
}

/// Extra information used while constructing client GUIDs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuidInfo {
    /// Client identifier incorporated into GUID generation.
    #[serde(default)]
    pub client_id: String,
    /// Timestamp seed used for GUID generation.
    #[serde(default)]
    pub nano_seconds_from_epoch: i64,
}

/// Client identity exchanged during negotiation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientIdentity {
    /// BlazingMQ protocol version.
    pub protocol_version: i32,
    /// SDK version advertised by the client.
    #[serde(default = "default_sdk_version")]
    pub sdk_version: i32,
    /// Client type.
    pub client_type: ClientType,
    /// Process name running the SDK.
    #[serde(default)]
    pub process_name: String,
    /// Process id.
    #[serde(default)]
    pub pid: i32,
    /// Session identifier chosen by the client.
    #[serde(default = "default_session_id")]
    pub session_id: i32,
    /// Host name running the SDK.
    #[serde(default)]
    pub host_name: String,
    /// Feature string advertised during negotiation.
    #[serde(default)]
    pub features: String,
    /// Broker cluster name, typically empty for clients.
    #[serde(default)]
    pub cluster_name: String,
    /// Broker cluster node id, typically `-1` for clients.
    #[serde(default = "default_cluster_node_id")]
    pub cluster_node_id: i32,
    /// Language of the SDK.
    #[serde(default = "default_sdk_language")]
    pub sdk_language: ClientLanguage,
    /// GUID generation metadata.
    #[serde(default)]
    pub guid_info: GuidInfo,
    /// User agent string.
    #[serde(default)]
    pub user_agent: String,
}

const fn default_sdk_version() -> i32 {
    999_999
}

const fn default_session_id() -> i32 {
    1
}

const fn default_cluster_node_id() -> i32 {
    -1
}

const fn default_sdk_language() -> ClientLanguage {
    ClientLanguage::Cpp
}

/// Broker response returned during negotiation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerResponse {
    /// Negotiation result.
    pub result: Status,
    /// Protocol version supported by the broker.
    pub protocol_version: i32,
    /// Broker software version.
    pub broker_version: i32,
    /// Whether the broker considers the client SDK deprecated.
    #[serde(default)]
    pub is_deprecated_sdk: bool,
    /// Identity information for the broker.
    pub broker_identity: ClientIdentity,
    /// Heartbeat interval, in milliseconds.
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: i32,
    /// Number of missed heartbeats tolerated before disconnect.
    #[serde(default = "default_max_missed_heartbeats")]
    pub max_missed_heartbeats: i32,
}

const fn default_heartbeat_interval_ms() -> i32 {
    3_000
}

const fn default_max_missed_heartbeats() -> i32 {
    10
}

/// Authentication request sent to the broker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationRequest {
    /// Authentication mechanism name.
    pub mechanism: String,
    /// Optional mechanism-specific payload, base64 encoded in JSON mode.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_base64",
        deserialize_with = "deserialize_optional_base64"
    )]
    pub data: Option<Vec<u8>>,
}

/// Authentication response returned by the broker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationResponse {
    /// Result of the authentication attempt.
    pub status: Status,
    /// Optional credential lifetime, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifetime_ms: Option<i32>,
}

fn serialize_optional_base64<S>(value: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(bytes) => serializer.serialize_some(&STANDARD.encode(bytes)),
        None => serializer.serialize_none(),
    }
}

fn serialize_base64<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&STANDARD.encode(value))
}

fn deserialize_optional_base64<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    value
        .map(|encoded| STANDARD.decode(encoded).map_err(serde::de::Error::custom))
        .transpose()
}

fn deserialize_base64<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    STANDARD.decode(value).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_open_queue_matches_expected_shape() {
        let message = ControlMessage::request(
            7,
            ControlPayload::OpenQueue(OpenQueue {
                handle_parameters: QueueHandleParameters {
                    uri: "bmq://bmq.test.mem.priority/example".into(),
                    q_id: 42,
                    sub_id_info: Some(SubQueueIdInfo {
                        sub_id: 0,
                        app_id: "__default".into(),
                    }),
                    flags: 0b1110,
                    read_count: 1,
                    write_count: 1,
                    admin_count: 0,
                },
            }),
        );

        let value = serde_json::to_value(&message).unwrap();
        let expected = serde_json::json!({
            "rId": 7,
            "openQueue": {
                "handleParameters": {
                    "uri": "bmq://bmq.test.mem.priority/example",
                    "qId": 42,
                    "subIdInfo": {
                        "subId": 0,
                        "appId": "__default"
                    },
                    "flags": 14,
                    "readCount": 1,
                    "writeCount": 1,
                    "adminCount": 0
                }
            }
        });

        assert_eq!(value, expected);
    }

    #[test]
    fn serialize_negotiation_message_matches_expected_shape() {
        let message = NegotiationMessage {
            payload: NegotiationPayload::ClientIdentity(ClientIdentity {
                protocol_version: 1,
                sdk_version: 999_999,
                client_type: ClientType::TcpClient,
                process_name: "blazox-test".into(),
                pid: 1234,
                session_id: 1,
                host_name: "localhost".into(),
                features: "PROTOCOL_ENCODING:JSON".into(),
                cluster_name: String::new(),
                cluster_node_id: -1,
                sdk_language: ClientLanguage::Cpp,
                guid_info: GuidInfo {
                    client_id: "blazox-test".into(),
                    nano_seconds_from_epoch: 0,
                },
                user_agent: "blazox/0.1.0".into(),
            }),
        };

        let value = serde_json::to_value(&message).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "clientIdentity": {
                    "protocolVersion": 1,
                    "sdkVersion": 999999,
                    "clientType": "E_TCPCLIENT",
                    "processName": "blazox-test",
                    "pid": 1234,
                    "sessionId": 1,
                    "hostName": "localhost",
                    "features": "PROTOCOL_ENCODING:JSON",
                    "clusterName": "",
                    "clusterNodeId": -1,
                    "sdkLanguage": "E_CPP",
                    "guidInfo": {
                        "clientId": "blazox-test",
                        "nanoSecondsFromEpoch": 0
                    },
                    "userAgent": "blazox/0.1.0"
                }
            })
        );
    }
}
