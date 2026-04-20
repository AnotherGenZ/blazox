use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Empty {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusCategory {
    #[serde(rename = "E_SUCCESS")]
    Success,
    #[serde(rename = "E_UNKNOWN")]
    Unknown,
    #[serde(rename = "E_TIMEOUT")]
    Timeout,
    #[serde(rename = "E_NOT_CONNECTED")]
    NotConnected,
    #[serde(rename = "E_CANCELED")]
    Canceled,
    #[serde(rename = "E_NOT_SUPPORTED")]
    NotSupported,
    #[serde(rename = "E_REFUSED")]
    Refused,
    #[serde(rename = "E_INVALID_ARGUMENT")]
    InvalidArgument,
    #[serde(rename = "E_NOT_READY")]
    NotReady,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub category: StatusCategory,
    pub code: i32,
    #[serde(default)]
    pub message: String,
}

impl Status {
    pub fn success() -> Self {
        Self {
            category: StatusCategory::Success,
            code: 0,
            message: String::new(),
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self.category, StatusCategory::Success)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlMessage {
    #[serde(rename = "rId", skip_serializing_if = "Option::is_none")]
    pub r_id: Option<i32>,
    #[serde(flatten)]
    pub payload: ControlPayload,
}

impl ControlMessage {
    pub fn request(r_id: i32, payload: ControlPayload) -> Self {
        Self {
            r_id: Some(r_id),
            payload,
        }
    }

    pub fn notification(payload: ControlPayload) -> Self {
        Self {
            r_id: None,
            payload,
        }
    }

    pub fn expect_success(self) -> std::result::Result<Self, Status> {
        match &self.payload {
            ControlPayload::Status(status) if !status.is_success() => Err(status.clone()),
            _ => Ok(self),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ControlPayload {
    Status(Status),
    Disconnect(Empty),
    DisconnectResponse(Empty),
    AdminCommand(AdminCommand),
    AdminCommandResponse(AdminCommandResponse),
    ClusterMessage(ClusterMessage),
    OpenQueue(OpenQueue),
    OpenQueueResponse(OpenQueueResponse),
    CloseQueue(CloseQueue),
    CloseQueueResponse(CloseQueueResponse),
    ConfigureQueueStream(ConfigureQueueStream),
    ConfigureQueueStreamResponse(ConfigureQueueStreamResponse),
    ConfigureStream(ConfigureStream),
    ConfigureStreamResponse(ConfigureStreamResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterMessage {
    #[serde(
        default,
        serialize_with = "serialize_base64",
        deserialize_with = "deserialize_base64"
    )]
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NegotiationMessage {
    #[serde(flatten)]
    pub payload: NegotiationPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NegotiationPayload {
    ClientIdentity(ClientIdentity),
    BrokerResponse(BrokerResponse),
    PlaceHolder(Empty),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthenticationMessage {
    #[serde(flatten)]
    pub payload: AuthenticationPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AuthenticationPayload {
    AuthenticationRequest(AuthenticationRequest),
    AuthenticationResponse(AuthenticationResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminCommand {
    pub command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminCommandResponse {
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenQueue {
    pub handle_parameters: QueueHandleParameters,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenQueueResponse {
    pub original_request: OpenQueue,
    pub routing_configuration: RoutingConfiguration,
    #[serde(default = "default_deduplication_time_ms")]
    pub deduplication_time_ms: i32,
}

const fn default_deduplication_time_ms() -> i32 {
    300_000
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseQueue {
    pub handle_parameters: QueueHandleParameters,
    #[serde(default)]
    pub is_final: bool,
}

pub type CloseQueueResponse = Empty;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureQueueStream {
    #[serde(rename = "qId")]
    pub q_id: u32,
    pub stream_parameters: QueueStreamParameters,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureQueueStreamResponse {
    pub request: ConfigureQueueStream,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureStream {
    #[serde(rename = "qId")]
    pub q_id: u32,
    pub stream_parameters: StreamParameters,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureStreamResponse {
    pub request: ConfigureStream,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueHandleParameters {
    pub uri: String,
    #[serde(rename = "qId")]
    pub q_id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_id_info: Option<SubQueueIdInfo>,
    pub flags: u64,
    #[serde(default)]
    pub read_count: i32,
    #[serde(default)]
    pub write_count: i32,
    #[serde(default)]
    pub admin_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueStreamParameters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_id_info: Option<SubQueueIdInfo>,
    #[serde(default)]
    pub max_unconfirmed_messages: i64,
    #[serde(default)]
    pub max_unconfirmed_bytes: i64,
    #[serde(default = "default_consumer_priority")]
    pub consumer_priority: i32,
    #[serde(default)]
    pub consumer_priority_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubQueueIdInfo {
    #[serde(default)]
    pub sub_id: u32,
    #[serde(default = "default_app_id")]
    pub app_id: String,
}

const fn default_consumer_priority() -> i32 {
    i32::MIN
}

fn default_app_id() -> String {
    "__default".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamParameters {
    #[serde(default = "default_app_id")]
    pub app_id: String,
    #[serde(default)]
    pub subscriptions: Vec<Subscription>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpressionVersion {
    #[serde(rename = "E_UNDEFINED")]
    Undefined,
    #[serde(rename = "E_VERSION_1")]
    Version1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Expression {
    #[serde(default = "default_expression_version")]
    pub version: ExpressionVersion,
    pub text: String,
}

const fn default_expression_version() -> ExpressionVersion {
    ExpressionVersion::Undefined
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerInfo {
    #[serde(default)]
    pub max_unconfirmed_messages: i64,
    #[serde(default)]
    pub max_unconfirmed_bytes: i64,
    #[serde(default = "default_consumer_priority")]
    pub consumer_priority: i32,
    #[serde(default)]
    pub consumer_priority_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    #[serde(rename = "sId")]
    pub s_id: u32,
    pub expression: Expression,
    #[serde(default)]
    pub consumers: Vec<ConsumerInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutingConfiguration {
    pub flags: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientType {
    #[serde(rename = "E_UNKNOWN")]
    Unknown,
    #[serde(rename = "E_TCPCLIENT")]
    TcpClient,
    #[serde(rename = "E_TCPBROKER")]
    TcpBroker,
    #[serde(rename = "E_TCPADMIN")]
    TcpAdmin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientLanguage {
    #[serde(rename = "E_UNKNOWN")]
    Unknown,
    #[serde(rename = "E_CPP")]
    Cpp,
    #[serde(rename = "E_JAVA")]
    Java,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuidInfo {
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub nano_seconds_from_epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientIdentity {
    pub protocol_version: i32,
    #[serde(default = "default_sdk_version")]
    pub sdk_version: i32,
    pub client_type: ClientType,
    #[serde(default)]
    pub process_name: String,
    #[serde(default)]
    pub pid: i32,
    #[serde(default = "default_session_id")]
    pub session_id: i32,
    #[serde(default)]
    pub host_name: String,
    #[serde(default)]
    pub features: String,
    #[serde(default)]
    pub cluster_name: String,
    #[serde(default = "default_cluster_node_id")]
    pub cluster_node_id: i32,
    #[serde(default = "default_sdk_language")]
    pub sdk_language: ClientLanguage,
    #[serde(default)]
    pub guid_info: GuidInfo,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerResponse {
    pub result: Status,
    pub protocol_version: i32,
    pub broker_version: i32,
    #[serde(default)]
    pub is_deprecated_sdk: bool,
    pub broker_identity: ClientIdentity,
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: i32,
    #[serde(default = "default_max_missed_heartbeats")]
    pub max_missed_heartbeats: i32,
}

const fn default_heartbeat_interval_ms() -> i32 {
    3_000
}

const fn default_max_missed_heartbeats() -> i32 {
    10
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationRequest {
    pub mechanism: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_base64",
        deserialize_with = "deserialize_optional_base64"
    )]
    pub data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationResponse {
    pub status: Status,
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
