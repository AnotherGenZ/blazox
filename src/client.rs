use crate::error::{Error, Result};
use crate::schema::{
    AdminCommand, AuthenticationMessage, AuthenticationPayload, AuthenticationRequest,
    AuthenticationResponse, BrokerResponse, ClientIdentity, ClientLanguage, ClientType, CloseQueue,
    ConfigureQueueStream, ConfigureStream, ControlMessage, ControlPayload, Empty, GuidInfo,
    NegotiationMessage, NegotiationPayload, OpenQueue, QueueHandleParameters,
    QueueStreamParameters, StreamParameters,
};
use crate::wire::{
    AckMessage, CompressionAlgorithm, EncodingType, EventHeader, EventType, MessageGuid,
    MessageGuidGenerator, MessageProperties, OutboundPutFrame, PushMessage, decode_ack_event,
    decode_control_event, decode_frame, decode_push_event, encode_confirm_event,
    encode_control_event, encode_heartbeat_response, encode_put_event, encode_schema_event,
};
use bytes::Bytes;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicI32, AtomicU32, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::time::timeout;

pub mod queue_flags {
    pub const ADMIN: u64 = 1 << 0;
    pub const READ: u64 = 1 << 1;
    pub const WRITE: u64 = 1 << 2;
    pub const ACK: u64 = 1 << 3;

    pub const fn admin(flags: u64) -> u64 {
        flags | ADMIN
    }

    pub const fn read(flags: u64) -> u64 {
        flags | READ
    }

    pub const fn write(flags: u64) -> u64 {
        flags | WRITE
    }

    pub const fn ack(flags: u64) -> u64 {
        flags | ACK
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub request_timeout: Duration,
    pub open_queue_timeout: Duration,
    pub configure_queue_timeout: Duration,
    pub close_queue_timeout: Duration,
    pub disconnect_timeout: Duration,
    pub channel_write_timeout: Duration,
    pub channel_high_watermark: Option<u64>,
    pub blob_buffer_size: usize,
    pub client_type: ClientType,
    pub process_name: String,
    pub host_name: String,
    pub sdk_version: i32,
    pub session_id: i32,
    pub features: String,
    pub user_agent: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        let process_name = std::env::args()
            .next()
            .and_then(|path| {
                std::path::Path::new(&path)
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| "blazox".to_string());

        Self {
            request_timeout: Duration::from_secs(10),
            open_queue_timeout: Duration::from_secs(10),
            configure_queue_timeout: Duration::from_secs(10),
            close_queue_timeout: Duration::from_secs(10),
            disconnect_timeout: Duration::from_secs(5),
            channel_write_timeout: Duration::from_secs(5),
            channel_high_watermark: None,
            blob_buffer_size: 64 * 1024,
            client_type: ClientType::TcpClient,
            process_name,
            host_name: gethostname::gethostname().to_string_lossy().into_owned(),
            sdk_version: 999_999,
            session_id: 1,
            features: "PROTOCOL_ENCODING:BER,JSON;MPS:MESSAGE_PROPERTIES_EX".to_string(),
            user_agent: format!("blazox/{}", env!("CARGO_PKG_VERSION")),
        }
    }
}

impl ClientConfig {
    fn client_identity(&self) -> ClientIdentity {
        let pid = std::process::id() as i32;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos() as i64)
            .unwrap_or_default();

        ClientIdentity {
            protocol_version: 1,
            sdk_version: self.sdk_version,
            client_type: self.client_type,
            process_name: self.process_name.clone(),
            pid,
            session_id: self.session_id,
            host_name: self.host_name.clone(),
            features: self.features.clone(),
            cluster_name: String::new(),
            cluster_node_id: -1,
            sdk_language: ClientLanguage::Cpp,
            guid_info: GuidInfo {
                client_id: self.user_agent.clone(),
                nano_seconds_from_epoch: nanos,
            },
            user_agent: self.user_agent.clone(),
        }
    }

    pub(crate) fn message_guid_generator(&self) -> MessageGuidGenerator {
        let identity = self.client_identity();
        MessageGuidGenerator::new(
            format!(
                "{}|{}|{}|{}",
                identity.host_name,
                identity.process_name,
                identity.user_agent,
                identity.guid_info.client_id
            ),
            identity.session_id,
            identity.pid,
            identity.guid_info.nano_seconds_from_epoch,
        )
    }
}

#[derive(Debug, Clone)]
pub struct QueueHandleConfig {
    pub flags: u64,
    pub read_count: i32,
    pub write_count: i32,
    pub admin_count: i32,
}

impl Default for QueueHandleConfig {
    fn default() -> Self {
        Self {
            flags: queue_flags::write(0),
            read_count: 0,
            write_count: 1,
            admin_count: 0,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OpenQueueOptions {
    pub handle: QueueHandleConfig,
    pub configure_queue_stream: Option<QueueStreamParameters>,
    pub configure_stream: Option<StreamParameters>,
}

#[derive(Debug, Clone)]
pub struct OutboundPut {
    pub payload: Bytes,
    pub properties: MessageProperties,
    pub correlation_id: Option<u32>,
    pub message_guid: Option<MessageGuid>,
    pub compression: CompressionAlgorithm,
}

impl OutboundPut {
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
            properties: MessageProperties::default(),
            correlation_id: None,
            message_guid: None,
            compression: CompressionAlgorithm::None,
        }
    }

    pub fn with_properties(mut self, properties: MessageProperties) -> Self {
        self.properties = properties;
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: u32) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_message_guid(mut self, message_guid: MessageGuid) -> Self {
        self.message_guid = Some(message_guid);
        self
    }

    pub fn with_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.compression = compression;
        self
    }
}

#[derive(Debug, Clone)]
pub enum InboundSchemaEvent {
    Control(ControlMessage),
    Negotiation(NegotiationMessage),
    Authentication(AuthenticationMessage),
    UnknownJson(Value),
}

#[derive(Debug, Clone)]
pub enum SessionEvent {
    Schema(InboundSchemaEvent),
    Ack(Vec<AckMessage>),
    Push(Vec<PushMessage>),
    HeartbeatRequest,
    HeartbeatResponse,
    TransportClosed,
    TransportError(String),
}

#[derive(Debug, Clone)]
pub struct QueueHandle {
    client: Client,
    pub uri: String,
    pub queue_id: u32,
    pub flags: u64,
}

impl QueueHandle {
    pub fn subscribe(&self) -> broadcast::Receiver<SessionEvent> {
        self.client.subscribe()
    }

    pub async fn publish(&self, message: OutboundPut) -> Result<()> {
        self.client
            .publish_to_queue(self.queue_id, vec![message])
            .await
    }

    pub async fn publish_all(&self, messages: Vec<OutboundPut>) -> Result<()> {
        self.client.publish_to_queue(self.queue_id, messages).await
    }

    pub async fn configure_queue_stream(&self, params: QueueStreamParameters) -> Result<()> {
        self.client
            .configure_queue_stream(self.queue_id, params)
            .await
    }

    pub async fn configure_stream(&self, params: StreamParameters) -> Result<()> {
        self.client.configure_stream(self.queue_id, params).await
    }

    pub async fn confirm(&self, message_guid: MessageGuid, sub_queue_id: u32) -> Result<()> {
        self.client
            .confirm(self.queue_id, message_guid, sub_queue_id)
            .await
    }

    pub async fn confirm_all(
        &self,
        messages: impl IntoIterator<Item = (MessageGuid, u32)>,
    ) -> Result<()> {
        self.client.confirm_many(self.queue_id, messages).await
    }

    pub async fn next_push(&self) -> Result<PushMessage> {
        let mut events = self.subscribe();
        loop {
            let event = timeout(self.client.inner.request_timeout, events.recv())
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::RequestCanceled)?;
            if let SessionEvent::Push(messages) = event {
                if let Some(message) = messages
                    .into_iter()
                    .find(|message| message.header.queue_id == self.queue_id)
                {
                    return Ok(message);
                }
            }
        }
    }

    pub async fn next_ack(&self) -> Result<AckMessage> {
        let mut events = self.subscribe();
        loop {
            let event = timeout(self.client.inner.request_timeout, events.recv())
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::RequestCanceled)?;
            if let SessionEvent::Ack(messages) = event {
                if let Some(message) = messages
                    .into_iter()
                    .find(|message| message.queue_id == self.queue_id)
                {
                    return Ok(message);
                }
            }
        }
    }

    pub async fn close(&self, is_final: bool) -> Result<()> {
        self.client.close_queue(self, is_final).await
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    writer: Mutex<OwnedWriteHalf>,
    pending_requests: Mutex<HashMap<i32, oneshot::Sender<ControlMessage>>>,
    pending_correlation_ids: Mutex<HashMap<MessageGuid, u32>>,
    events: broadcast::Sender<SessionEvent>,
    next_request_id: AtomicI32,
    next_queue_id: AtomicU32,
    request_timeout: Duration,
    open_queue_timeout: Duration,
    configure_queue_timeout: Duration,
    close_queue_timeout: Duration,
    disconnect_timeout: Duration,
    channel_write_timeout: Duration,
    channel_high_watermark: Option<u64>,
    blob_buffer_size: usize,
    negotiated: BrokerResponse,
    encoding: EncodingType,
    guid_generator: MessageGuidGenerator,
}

impl Client {
    pub async fn connect(addr: impl AsRef<str>, config: ClientConfig) -> Result<Self> {
        let stream = TcpStream::connect(addr.as_ref()).await?;
        let (mut reader, mut writer) = stream.into_split();

        let client_identity = config.client_identity();
        let negotiation = NegotiationMessage {
            payload: NegotiationPayload::ClientIdentity(client_identity),
        };
        let frame = encode_control_event(&negotiation, EncodingType::Json)?;
        writer.write_all(&frame).await?;

        let response_frame = read_frame(&mut reader, config.blob_buffer_size).await?;
        let (header, payload) = decode_frame(&response_frame)?;
        if header.event_type != EventType::Control {
            return Err(Error::UnexpectedSchema(
                "expected negotiation response as a CONTROL event",
            ));
        }
        let negotiation_response: NegotiationMessage = decode_control_event(&header, payload)?;
        let broker_response = match negotiation_response.payload {
            NegotiationPayload::BrokerResponse(response) => response,
            _ => {
                return Err(Error::UnexpectedSchema(
                    "expected brokerResponse during negotiation",
                ));
            }
        };
        if !broker_response.result.is_success() {
            return Err(Error::BrokerStatus(broker_response.result));
        }
        let encoding = best_schema_encoding(&broker_response.broker_identity.features);

        let (events, _) = broadcast::channel(256);
        let client = Self {
            inner: Arc::new(Inner {
                writer: Mutex::new(writer),
                pending_requests: Mutex::new(HashMap::new()),
                pending_correlation_ids: Mutex::new(HashMap::new()),
                events,
                next_request_id: AtomicI32::new(1),
                next_queue_id: AtomicU32::new(1),
                request_timeout: config.request_timeout,
                open_queue_timeout: config.open_queue_timeout,
                configure_queue_timeout: config.configure_queue_timeout,
                close_queue_timeout: config.close_queue_timeout,
                disconnect_timeout: config.disconnect_timeout,
                channel_write_timeout: config.channel_write_timeout,
                channel_high_watermark: config.channel_high_watermark,
                blob_buffer_size: config.blob_buffer_size,
                negotiated: broker_response,
                encoding,
                guid_generator: config.message_guid_generator(),
            }),
        };

        tokio::spawn(read_loop(client.clone(), reader));
        Ok(client)
    }

    pub fn broker_response(&self) -> &BrokerResponse {
        &self.inner.negotiated
    }

    pub fn negotiated_encoding(&self) -> EncodingType {
        self.inner.encoding
    }

    pub fn next_message_guid(&self) -> MessageGuid {
        self.inner.guid_generator.next()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SessionEvent> {
        self.inner.events.subscribe()
    }

    pub async fn authenticate(
        &self,
        request: AuthenticationRequest,
    ) -> Result<AuthenticationResponse> {
        let mut events = self.subscribe();
        let frame = encode_schema_event(
            &AuthenticationMessage {
                payload: AuthenticationPayload::AuthenticationRequest(request),
            },
            EventType::Authentication,
            self.inner.encoding,
        )?;
        self.write_frame(frame).await?;
        loop {
            let event = timeout(self.inner.request_timeout, events.recv())
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::RequestCanceled)?;
            if let SessionEvent::Schema(InboundSchemaEvent::Authentication(message)) = event {
                if let AuthenticationPayload::AuthenticationResponse(response) = message.payload {
                    if response.status.is_success() {
                        return Ok(response);
                    }
                    return Err(Error::BrokerStatus(response.status));
                }
            }
        }
    }

    pub async fn authenticate_anonymous(&self) -> Result<AuthenticationResponse> {
        self.authenticate(AuthenticationRequest {
            mechanism: "ANONYMOUS".to_string(),
            data: None,
        })
        .await
    }

    pub async fn admin_command(&self, command: impl Into<String>) -> Result<String> {
        let response = self
            .send_control_request(
                ControlPayload::AdminCommand(AdminCommand {
                    command: command.into(),
                }),
                self.inner.request_timeout,
            )
            .await?;
        match response.payload {
            ControlPayload::AdminCommandResponse(response) => Ok(response.text),
            ControlPayload::Status(status) => Err(Error::BrokerStatus(status)),
            _ => Err(Error::UnexpectedSchema(
                "admin command did not return adminCommandResponse",
            )),
        }
    }

    pub async fn open_queue(
        &self,
        uri: impl Into<String>,
        options: OpenQueueOptions,
    ) -> Result<QueueHandle> {
        let uri = uri.into();
        let queue_id = self.inner.next_queue_id.fetch_add(1, Ordering::Relaxed);
        let handle_parameters = QueueHandleParameters {
            uri: uri.clone(),
            q_id: queue_id,
            sub_id_info: None,
            flags: options.handle.flags,
            read_count: options.handle.read_count,
            write_count: options.handle.write_count,
            admin_count: options.handle.admin_count,
        };

        let response = self
            .send_control_request(
                ControlPayload::OpenQueue(OpenQueue {
                    handle_parameters: handle_parameters.clone(),
                }),
                self.inner.open_queue_timeout,
            )
            .await?;
        match response.payload {
            ControlPayload::OpenQueueResponse(_) => {}
            ControlPayload::Status(status) => return Err(Error::BrokerStatus(status)),
            _ => {
                return Err(Error::UnexpectedSchema(
                    "open queue did not return openQueueResponse",
                ));
            }
        }

        if let Some(params) = options.configure_queue_stream {
            self.configure_queue_stream(queue_id, params).await?;
        }
        if let Some(params) = options.configure_stream {
            self.configure_stream(queue_id, params).await?;
        }

        Ok(QueueHandle {
            client: self.clone(),
            uri,
            queue_id,
            flags: handle_parameters.flags,
        })
    }

    pub async fn close_queue(&self, handle: &QueueHandle, is_final: bool) -> Result<()> {
        let response = self
            .send_control_request(
                ControlPayload::CloseQueue(CloseQueue {
                    handle_parameters: QueueHandleParameters {
                        uri: handle.uri.clone(),
                        q_id: handle.queue_id,
                        sub_id_info: None,
                        flags: handle.flags,
                        read_count: i32::from((handle.flags & queue_flags::READ) != 0),
                        write_count: i32::from((handle.flags & queue_flags::WRITE) != 0),
                        admin_count: i32::from((handle.flags & queue_flags::ADMIN) != 0),
                    },
                    is_final,
                }),
                self.inner.close_queue_timeout,
            )
            .await?;
        match response.payload {
            ControlPayload::CloseQueueResponse(Empty {}) => Ok(()),
            ControlPayload::Status(status) => Err(Error::BrokerStatus(status)),
            _ => Err(Error::UnexpectedSchema(
                "close queue did not return closeQueueResponse",
            )),
        }
    }

    pub async fn configure_queue_stream(
        &self,
        queue_id: u32,
        params: QueueStreamParameters,
    ) -> Result<()> {
        let response = self
            .send_control_request(
                ControlPayload::ConfigureQueueStream(ConfigureQueueStream {
                    q_id: queue_id,
                    stream_parameters: params,
                }),
                self.inner.configure_queue_timeout,
            )
            .await?;
        match response.payload {
            ControlPayload::ConfigureQueueStreamResponse(_) => Ok(()),
            ControlPayload::Status(status) => Err(Error::BrokerStatus(status)),
            _ => Err(Error::UnexpectedSchema(
                "configureQueueStream did not return configureQueueStreamResponse",
            )),
        }
    }

    pub async fn configure_stream(&self, queue_id: u32, params: StreamParameters) -> Result<()> {
        let response = self
            .send_control_request(
                ControlPayload::ConfigureStream(ConfigureStream {
                    q_id: queue_id,
                    stream_parameters: params,
                }),
                self.inner.configure_queue_timeout,
            )
            .await?;
        match response.payload {
            ControlPayload::ConfigureStreamResponse(_) => Ok(()),
            ControlPayload::Status(status) => Err(Error::BrokerStatus(status)),
            _ => Err(Error::UnexpectedSchema(
                "configureStream did not return configureStreamResponse",
            )),
        }
    }

    pub async fn disconnect(&self) -> Result<()> {
        let response = self
            .send_control_request(
                ControlPayload::Disconnect(Empty {}),
                self.inner.disconnect_timeout,
            )
            .await?;
        match response.payload {
            ControlPayload::DisconnectResponse(Empty {}) => Ok(()),
            ControlPayload::Status(status) => Err(Error::BrokerStatus(status)),
            _ => Err(Error::UnexpectedSchema(
                "disconnect did not return disconnectResponse",
            )),
        }
    }

    pub async fn publish_to_queue(&self, queue_id: u32, messages: Vec<OutboundPut>) -> Result<()> {
        let mut correlation_ids = Vec::new();
        let wire_messages = messages
            .into_iter()
            .map(|message| {
                let message_guid = message
                    .message_guid
                    .unwrap_or_else(|| self.inner.guid_generator.next());
                if let Some(correlation_id) = message.correlation_id {
                    correlation_ids.push((message_guid, correlation_id));
                }
                OutboundPutFrame {
                    queue_id,
                    payload: message.payload,
                    properties: message.properties,
                    correlation_id: message.correlation_id,
                    message_guid: Some(message_guid),
                    compression: message.compression,
                }
            })
            .collect::<Vec<_>>();

        if !correlation_ids.is_empty() {
            let mut pending = self.inner.pending_correlation_ids.lock().await;
            for (message_guid, correlation_id) in &correlation_ids {
                pending.insert(*message_guid, *correlation_id);
            }
        }
        let frame = encode_put_event(&wire_messages)?;
        if let Err(error) = self.write_frame(frame).await {
            if !correlation_ids.is_empty() {
                let mut pending = self.inner.pending_correlation_ids.lock().await;
                for (message_guid, _) in correlation_ids {
                    pending.remove(&message_guid);
                }
            }
            return Err(error);
        }
        Ok(())
    }

    pub async fn confirm(
        &self,
        queue_id: u32,
        message_guid: MessageGuid,
        sub_queue_id: u32,
    ) -> Result<()> {
        let frame = encode_confirm_event(&[crate::wire::ConfirmMessage {
            queue_id,
            message_guid,
            sub_queue_id,
        }])?;
        self.write_frame(frame).await
    }

    pub async fn confirm_many(
        &self,
        queue_id: u32,
        messages: impl IntoIterator<Item = (MessageGuid, u32)>,
    ) -> Result<()> {
        let messages = messages
            .into_iter()
            .map(|(message_guid, sub_queue_id)| crate::wire::ConfirmMessage {
                queue_id,
                message_guid,
                sub_queue_id,
            })
            .collect::<Vec<_>>();
        if messages.is_empty() {
            return Ok(());
        }
        let frame = encode_confirm_event(&messages)?;
        self.write_frame(frame).await
    }

    async fn send_control_request(
        &self,
        payload: ControlPayload,
        request_timeout: Duration,
    ) -> Result<ControlMessage> {
        let request_id = self.inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        let message = ControlMessage::request(request_id, payload);
        let frame = encode_control_event(&message, self.inner.encoding)?;
        let (tx, rx) = oneshot::channel();
        self.inner
            .pending_requests
            .lock()
            .await
            .insert(request_id, tx);

        if let Err(error) = self.write_frame(frame).await {
            self.inner.pending_requests.lock().await.remove(&request_id);
            return Err(error);
        }

        let response = timeout(request_timeout, rx)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::RequestCanceled)?;
        response.expect_success().map_err(Error::BrokerStatus)
    }

    async fn write_frame(&self, frame: Bytes) -> Result<()> {
        if let Some(high_watermark) = self.inner.channel_high_watermark {
            if frame.len() as u64 > high_watermark {
                return Err(Error::BandwidthLimit);
            }
        }
        let mut writer = self.inner.writer.lock().await;
        timeout(self.inner.channel_write_timeout, writer.write_all(&frame))
            .await
            .map_err(|_| Error::BandwidthLimit)?
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionReset => Error::WriterClosed,
                _ => Error::Io(error),
            })
    }
}

fn best_schema_encoding(features: &str) -> EncodingType {
    let Some(values) = feature_values(features, "PROTOCOL_ENCODING") else {
        return EncodingType::Json;
    };
    if values.iter().any(|value| *value == "JSON") {
        return EncodingType::Json;
    }
    if values.iter().any(|value| *value == "BER") {
        return EncodingType::Ber;
    }
    EncodingType::Json
}

fn feature_values<'a>(features: &'a str, field_name: &str) -> Option<Vec<&'a str>> {
    for field in features.split(';').filter(|field| !field.is_empty()) {
        let mut parts = field.splitn(2, ':');
        let key = parts.next()?;
        let values = parts.next();
        if key != field_name {
            continue;
        }
        return Some(
            values
                .map(|values| {
                    values
                        .split(',')
                        .filter(|value| !value.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
        );
    }
    None
}

async fn read_loop(client: Client, mut reader: OwnedReadHalf) {
    loop {
        let frame = match read_frame(&mut reader, client.inner.blob_buffer_size).await {
            Ok(frame) => frame,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                let _ = client.inner.events.send(SessionEvent::TransportClosed);
                break;
            }
            Err(error) => {
                let _ = client
                    .inner
                    .events
                    .send(SessionEvent::TransportError(error.to_string()));
                break;
            }
        };

        match handle_frame(&client, &frame).await {
            Ok(()) => {}
            Err(error) => {
                let _ = client
                    .inner
                    .events
                    .send(SessionEvent::TransportError(error.to_string()));
            }
        }
    }
}

async fn handle_frame(client: &Client, frame: &[u8]) -> Result<()> {
    let (header, payload) = decode_frame(frame)?;
    match header.event_type {
        EventType::Control => handle_control_frame(client, header, payload).await,
        EventType::Ack => {
            let mut messages = decode_ack_event(payload)?;
            if !messages.is_empty() {
                let mut pending = client.inner.pending_correlation_ids.lock().await;
                for message in &mut messages {
                    if let Some(correlation_id) = pending.remove(&message.message_guid) {
                        message.correlation_id = correlation_id;
                    }
                }
            }
            let _ = client.inner.events.send(SessionEvent::Ack(messages));
            Ok(())
        }
        EventType::Push => {
            let _ = client
                .inner
                .events
                .send(SessionEvent::Push(decode_push_event(payload)?));
            Ok(())
        }
        EventType::HeartbeatReq => {
            client.write_frame(encode_heartbeat_response()).await?;
            let _ = client.inner.events.send(SessionEvent::HeartbeatRequest);
            Ok(())
        }
        EventType::HeartbeatRsp => {
            let _ = client.inner.events.send(SessionEvent::HeartbeatResponse);
            Ok(())
        }
        EventType::Authentication => {
            let message = decode_control_event::<AuthenticationMessage>(&header, payload)?;
            let _ =
                client
                    .inner
                    .events
                    .send(SessionEvent::Schema(InboundSchemaEvent::Authentication(
                        message,
                    )));
            Ok(())
        }
        _ => {
            let _ = client
                .inner
                .events
                .send(SessionEvent::TransportError(format!(
                    "received unsupported event type {:?}",
                    header.event_type
                )));
            Ok(())
        }
    }
}

async fn handle_control_frame(client: &Client, header: EventHeader, payload: &[u8]) -> Result<()> {
    if let Ok(message) = decode_control_event::<ControlMessage>(&header, payload) {
        if let Some(request_id) = message.r_id {
            if let Some(sender) = client
                .inner
                .pending_requests
                .lock()
                .await
                .remove(&request_id)
            {
                let _ = sender.send(message);
                return Ok(());
            }
        }
        let _ = client
            .inner
            .events
            .send(SessionEvent::Schema(InboundSchemaEvent::Control(message)));
        return Ok(());
    }

    if let Ok(message) = decode_control_event::<NegotiationMessage>(&header, payload) {
        let _ = client
            .inner
            .events
            .send(SessionEvent::Schema(InboundSchemaEvent::Negotiation(
                message,
            )));
        return Ok(());
    }

    if let Ok(message) = decode_control_event::<AuthenticationMessage>(&header, payload) {
        let _ = client
            .inner
            .events
            .send(SessionEvent::Schema(InboundSchemaEvent::Authentication(
                message,
            )));
        return Ok(());
    }

    if header.schema_encoding()? != EncodingType::Json {
        return Err(Error::UnexpectedSchema(
            "unrecognized BER schema payload for control event",
        ));
    }
    let trimmed = payload
        .iter()
        .rposition(|byte| *byte != 0)
        .map(|idx| &payload[..=idx])
        .unwrap_or(payload);
    let value = serde_json::from_slice::<Value>(trimmed)?;
    let _ = client
        .inner
        .events
        .send(SessionEvent::Schema(InboundSchemaEvent::UnknownJson(value)));
    Ok(())
}

async fn read_frame(
    reader: &mut OwnedReadHalf,
    blob_buffer_size: usize,
) -> std::io::Result<Vec<u8>> {
    let mut prefix = [0_u8; 4];
    reader.read_exact(&mut prefix).await?;
    let length = u32::from_be_bytes(prefix) & 0x7fff_ffff;
    if length < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "packet length is smaller than minimum header size",
        ));
    }
    let mut frame = Vec::with_capacity((length as usize).max(blob_buffer_size));
    frame.resize(length as usize, 0);
    frame[..4].copy_from_slice(&prefix);
    reader.read_exact(&mut frame[4..]).await?;
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ControlPayload, Status};

    #[test]
    fn default_client_identity_advertises_protocol_encodings() {
        let config = ClientConfig::default();
        let identity = config.client_identity();
        assert_eq!(identity.protocol_version, 1);
        assert!(identity.features.contains("PROTOCOL_ENCODING:BER,JSON"));
        assert!(identity.features.contains("JSON"));
    }

    #[test]
    fn best_schema_encoding_prefers_json_and_defaults_to_it() {
        assert_eq!(
            best_schema_encoding("PROTOCOL_ENCODING:BER,JSON;MPS:MESSAGE_PROPERTIES_EX"),
            EncodingType::Json
        );
        assert_eq!(
            best_schema_encoding("PROTOCOL_ENCODING:JSON"),
            EncodingType::Json
        );
        assert_eq!(
            best_schema_encoding("PROTOCOL_ENCODING:BER"),
            EncodingType::Ber
        );
        assert_eq!(best_schema_encoding(""), EncodingType::Json);
    }

    #[test]
    fn queue_handle_config_defaults_to_writer() {
        let config = QueueHandleConfig::default();
        assert_eq!(config.flags, queue_flags::WRITE);
        assert_eq!(config.write_count, 1);
    }

    #[test]
    fn open_queue_response_error_is_promoted_to_broker_status() {
        let status = Status {
            category: crate::schema::StatusCategory::Refused,
            code: -6,
            message: "refused".into(),
        };
        let response = ControlMessage {
            r_id: Some(1),
            payload: ControlPayload::Status(status.clone()),
        };
        let err = response.expect_success().unwrap_err();
        assert_eq!(err, status);
    }
}
