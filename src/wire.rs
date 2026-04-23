//! Data-plane wire structures and codec helpers.
//!
//! This module models the part of the BlazingMQ protocol that is optimized for
//! frequent traffic: binary event headers plus the `PUT`, `PUSH`, `ACK`, and
//! `CONFIRM` message families.
//!
//! The important mental model is:
//!
//! - [`crate::schema`] is the control plane, used for infrequent structured
//!   operations such as opening queues and authenticating.
//! - [`wire`] is the data plane, used for message traffic that needs compact,
//!   fast binary encoding.
//!
//! Most application code will never construct these headers directly, but the
//! types are the source of truth for how the low-level [`crate::client::Client`]
//! turns producer and consumer operations into bytes on the wire.

use crate::ber::BerCodec;
use crate::error::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};
use crc32c::crc32c;
use flate2::Compression as ZlibCompression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

/// Word size used by the BlazingMQ binary protocol.
pub const WORD_SIZE: usize = 4;
/// Size of the fixed event header in bytes.
pub const EVENT_HEADER_SIZE: usize = 8;
/// Schema wire id used when no schema id is present.
pub const INVALID_SCHEMA_WIRE_ID: i16 = 1;

const MESSAGE_PROPERTIES_HEADER_SIZE: usize = 6;
const MESSAGE_PROPERTY_HEADER_SIZE: usize = 6;

/// BlazingMQ event types carried in the event header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EventType {
    /// Undefined event type.
    Undefined = 0,
    /// Control-plane event.
    Control = 1,
    /// Producer put event.
    Put = 2,
    /// Consumer confirm event.
    Confirm = 3,
    /// Broker push event.
    Push = 4,
    /// Producer acknowledgement event.
    Ack = 5,
    /// Cluster-state event.
    ClusterState = 6,
    /// Elector event.
    Elector = 7,
    /// Storage event.
    Storage = 8,
    /// Recovery event.
    Recovery = 9,
    /// Partition sync event.
    PartitionSync = 10,
    /// Heartbeat request event.
    HeartbeatReq = 11,
    /// Heartbeat response event.
    HeartbeatRsp = 12,
    /// Reject event.
    Reject = 13,
    /// Replication receipt event.
    ReplicationReceipt = 14,
    /// Authentication event.
    Authentication = 15,
}

impl EventType {
    /// Decodes an event type from its wire representation.
    pub fn from_u8(value: u8) -> Result<Self> {
        Ok(match value {
            0 => Self::Undefined,
            1 => Self::Control,
            2 => Self::Put,
            3 => Self::Confirm,
            4 => Self::Push,
            5 => Self::Ack,
            6 => Self::ClusterState,
            7 => Self::Elector,
            8 => Self::Storage,
            9 => Self::Recovery,
            10 => Self::PartitionSync,
            11 => Self::HeartbeatReq,
            12 => Self::HeartbeatRsp,
            13 => Self::Reject,
            14 => Self::ReplicationReceipt,
            15 => Self::Authentication,
            _ => {
                return Err(Error::ProtocolMessage(format!(
                    "unknown event type {value}"
                )));
            }
        })
    }
}

/// Encoding used for schema events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EncodingType {
    /// BER encoding.
    Ber = 0,
    /// JSON encoding.
    Json = 1,
}

impl EncodingType {
    /// Decodes a schema encoding from its wire representation.
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Ber),
            1 => Ok(Self::Json),
            _ => Err(Error::ProtocolMessage(format!(
                "unsupported control encoding {value}"
            ))),
        }
    }
}

/// Compression algorithm used for message payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionAlgorithm {
    /// No compression.
    None = 0,
    /// Zlib compression.
    Zlib = 1,
    /// Unknown or unsupported compression algorithm.
    Unknown = 255,
}

impl CompressionAlgorithm {
    /// Decodes a compression algorithm from its wire representation.
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::None,
            1 => Self::Zlib,
            _ => Self::Unknown,
        }
    }
}

/// BlazingMQ message GUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct MessageGuid(pub [u8; 16]);

impl MessageGuid {
    /// Creates a GUID from a 16-byte slice.
    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 16 {
            return Err(Error::Protocol("message guid must be 16 bytes"));
        }
        let mut guid = [0_u8; 16];
        guid.copy_from_slice(bytes);
        Ok(Self(guid))
    }

    /// Returns the GUID bytes.
    pub fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Locally generated message GUID source.
#[derive(Debug)]
pub struct MessageGuidGenerator {
    counter: AtomicU32,
    timer_base: Instant,
    client_id: [u8; 6],
}

impl MessageGuidGenerator {
    /// Creates a generator using the supplied client identity components.
    pub fn new(
        client_id_hint: impl AsRef<str>,
        session_id: i32,
        pid: i32,
        nano_seconds_from_epoch: i64,
    ) -> Self {
        let mut hasher = DefaultHasher::new();
        client_id_hint.as_ref().hash(&mut hasher);
        session_id.hash(&mut hasher);
        pid.hash(&mut hasher);
        nano_seconds_from_epoch.hash(&mut hasher);

        let digest = hasher.finish().to_be_bytes();
        let mut client_id = [0_u8; 6];
        client_id.copy_from_slice(&digest[2..8]);

        Self {
            counter: AtomicU32::new(0),
            timer_base: Instant::now(),
            client_id,
        }
    }

    /// Returns the next message GUID.
    pub fn next(&self) -> MessageGuid {
        let counter = self.counter.fetch_add(1, Ordering::Relaxed) & 0x003f_ffff;
        let timer_tick = (self.timer_base.elapsed().as_nanos() as u64) & 0x00ff_ffff_ffff_ffff;

        let version_and_counter = ((1_u32) << 30) | (counter << 8);
        let mut guid = [0_u8; 16];
        let version_and_counter = version_and_counter.to_be_bytes();
        guid[..3].copy_from_slice(&version_and_counter[..3]);

        let timer_tick = timer_tick.to_be_bytes();
        guid[3..10].copy_from_slice(&timer_tick[1..8]);
        guid[10..16].copy_from_slice(&self.client_id);

        MessageGuid(guid)
    }
}

/// Fixed header present at the start of every BlazingMQ event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventHeader {
    /// Total event length in bytes, including the header.
    pub length: u32,
    /// Protocol version.
    pub protocol_version: u8,
    /// Event type.
    pub event_type: EventType,
    /// Header length expressed in protocol words.
    pub header_words: u8,
    /// Type-specific header flags or schema encoding bits.
    pub type_specific: u8,
}

impl EventHeader {
    /// Creates a header for the given event type.
    pub fn new(event_type: EventType) -> Self {
        let mut header = Self {
            length: EVENT_HEADER_SIZE as u32,
            protocol_version: 1,
            event_type,
            header_words: (EVENT_HEADER_SIZE / WORD_SIZE) as u8,
            type_specific: 0,
        };
        if matches!(event_type, EventType::Control) {
            header.set_control_encoding(EncodingType::Ber);
        }
        header
    }

    /// Sets the schema encoding bits used by control/authentication events.
    pub fn set_control_encoding(&mut self, encoding: EncodingType) {
        self.type_specific &= !0b1110_0000;
        self.type_specific |= (encoding as u8) << 5;
    }

    /// Returns the schema encoding carried by the header.
    pub fn schema_encoding(&self) -> Result<EncodingType> {
        if !matches!(
            self.event_type,
            EventType::Control | EventType::Authentication
        ) {
            return Err(Error::Protocol("event does not carry a schema payload"));
        }
        EncodingType::from_u8((self.type_specific & 0b1110_0000) >> 5)
    }

    /// Alias for [`EventHeader::schema_encoding`].
    pub fn control_encoding(&self) -> Result<EncodingType> {
        self.schema_encoding()
    }

    /// Encodes the header to its 8-byte wire form.
    pub fn encode(self) -> [u8; EVENT_HEADER_SIZE] {
        let mut out = [0_u8; EVENT_HEADER_SIZE];
        let length_word = self.length & 0x7fff_ffff;
        out[..4].copy_from_slice(&length_word.to_be_bytes());
        out[4] = ((self.protocol_version & 0b11) << 6) | ((self.event_type as u8) & 0b0011_1111);
        out[5] = self.header_words;
        out[6] = self.type_specific;
        out[7] = 0;
        out
    }

    /// Decodes an event header from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < EVENT_HEADER_SIZE {
            return Err(Error::Protocol("event header is truncated"));
        }
        let length = u32::from_be_bytes(bytes[..4].try_into().unwrap()) & 0x7fff_ffff;
        let event_type = EventType::from_u8(bytes[4] & 0b0011_1111)?;
        Ok(Self {
            length,
            protocol_version: bytes[4] >> 6,
            event_type,
            header_words: bytes[5],
            type_specific: bytes[6],
        })
    }
}

/// Wire-level message property value discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessagePropertyType {
    /// Undefined type.
    Undefined = 0,
    /// Boolean value.
    Bool = 1,
    /// Signed 8-bit integer.
    Byte = 2,
    /// Signed 16-bit integer.
    Short = 3,
    /// Signed 32-bit integer.
    Int32 = 4,
    /// Signed 64-bit integer.
    Int64 = 5,
    /// ASCII string.
    String = 6,
    /// Opaque binary blob.
    Binary = 7,
}

impl MessagePropertyType {
    fn from_u8(value: u8) -> Result<Self> {
        Ok(match value {
            1 => Self::Bool,
            2 => Self::Byte,
            3 => Self::Short,
            4 => Self::Int32,
            5 => Self::Int64,
            6 => Self::String,
            7 => Self::Binary,
            _ => {
                return Err(Error::ProtocolMessage(format!(
                    "unknown message property type {value}"
                )));
            }
        })
    }
}

/// Value stored in a message property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessagePropertyValue {
    /// Boolean value.
    Bool(bool),
    /// Signed 8-bit integer.
    Byte(i8),
    /// Signed 16-bit integer.
    Short(i16),
    /// Signed 32-bit integer.
    Int32(i32),
    /// Signed 64-bit integer.
    Int64(i64),
    /// ASCII string.
    String(String),
    /// Opaque binary value.
    Binary(Bytes),
}

/// Single message property entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageProperty {
    /// Property name.
    pub name: String,
    /// Property value.
    pub value: MessagePropertyValue,
}

/// Collection of message properties.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MessageProperties {
    properties: Vec<MessageProperty>,
}

impl MessageProperties {
    /// Creates an empty property collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a fully constructed property.
    pub fn push(&mut self, property: MessageProperty) {
        self.properties.push(property);
    }

    /// Appends a property by name and value.
    pub fn insert(&mut self, name: impl Into<String>, value: MessagePropertyValue) {
        self.push(MessageProperty {
            name: name.into(),
            value,
        });
    }

    /// Inserts a property or replaces the existing value for that name.
    pub fn insert_or_replace(&mut self, name: impl Into<String>, value: MessagePropertyValue) {
        let name = name.into();
        if let Some(property) = self
            .properties
            .iter_mut()
            .find(|property| property.name == name)
        {
            property.value = value;
            return;
        }
        self.push(MessageProperty { name, value });
    }

    /// Returns the first property matching `name`.
    pub fn get(&self, name: &str) -> Option<&MessagePropertyValue> {
        self.properties
            .iter()
            .find(|property| property.name == name)
            .map(|property| &property.value)
    }

    /// Returns an iterator over properties in insertion order.
    pub fn iter(&self) -> std::slice::Iter<'_, MessageProperty> {
        self.properties.iter()
    }

    /// Returns the number of properties.
    pub fn len(&self) -> usize {
        self.properties.len()
    }

    /// Returns `true` when no properties are present.
    pub fn is_empty(&self) -> bool {
        self.properties.is_empty()
    }
}

impl IntoIterator for MessageProperties {
    type Item = MessageProperty;
    type IntoIter = std::vec::IntoIter<MessageProperty>;

    fn into_iter(self) -> Self::IntoIter {
        self.properties.into_iter()
    }
}

impl From<Vec<MessageProperty>> for MessageProperties {
    fn from(properties: Vec<MessageProperty>) -> Self {
        Self { properties }
    }
}

/// Validates and splits a complete event frame into header and payload.
pub fn decode_frame(frame: &[u8]) -> Result<(EventHeader, &[u8])> {
    let header = EventHeader::decode(frame)?;
    if header.length as usize != frame.len() {
        return Err(Error::ProtocolMessage(format!(
            "frame length mismatch: header={}, actual={}",
            header.length,
            frame.len()
        )));
    }
    let header_len = usize::from(header.header_words) * WORD_SIZE;
    if header_len < EVENT_HEADER_SIZE || header_len > frame.len() {
        return Err(Error::Protocol("invalid event header length"));
    }
    Ok((header, &frame[header_len..]))
}

fn zero_pad_to_word(bytes: usize) -> usize {
    let rem = bytes % WORD_SIZE;
    if rem == 0 { 0 } else { WORD_SIZE - rem }
}

fn strip_protocol_padding(data: &[u8]) -> &[u8] {
    let Some(&last) = data.last() else {
        return data;
    };
    let padding = usize::from(last);
    if padding == 0 || padding > WORD_SIZE || padding > data.len() {
        return data;
    }
    if data[data.len() - padding..]
        .iter()
        .all(|byte| usize::from(*byte) == padding)
    {
        &data[..data.len() - padding]
    } else {
        data
    }
}

fn appdata_padding(bytes: usize) -> usize {
    ((bytes + WORD_SIZE) / WORD_SIZE) * WORD_SIZE - bytes
}

fn appdata_padding_bytes(padding: usize) -> &'static [u8] {
    match padding {
        1 => &[1],
        2 => &[2, 2],
        3 => &[3, 3, 3],
        4 => &[4, 4, 4, 4],
        _ => &[],
    }
}

fn strip_appdata_padding(data: &[u8]) -> Result<&[u8]> {
    let Some(last) = data.last() else {
        return Ok(data);
    };
    let padding = usize::from(*last);
    if padding == 0 || padding > WORD_SIZE || padding > data.len() {
        return Err(Error::ProtocolMessage(format!(
            "invalid application data padding length {padding}"
        )));
    }
    if !data[data.len() - padding..]
        .iter()
        .all(|byte| usize::from(*byte) == padding)
    {
        return Err(Error::Protocol(
            "application data padding bytes are invalid",
        ));
    }
    let unpadded = &data[..data.len() - padding];
    if appdata_padding(unpadded.len()) != padding {
        return Err(Error::Protocol(
            "application data padding does not match payload",
        ));
    }
    Ok(unpadded)
}

fn compress_payload(payload: &[u8], compression: CompressionAlgorithm) -> Result<Vec<u8>> {
    match compression {
        CompressionAlgorithm::None => Ok(payload.to_vec()),
        CompressionAlgorithm::Zlib => {
            let mut encoder = ZlibEncoder::new(Vec::new(), ZlibCompression::default());
            encoder.write_all(payload).map_err(|error| {
                Error::ProtocolMessage(format!("zlib compression failed: {error}"))
            })?;
            encoder.finish().map_err(|error| {
                Error::ProtocolMessage(format!("zlib compression failed: {error}"))
            })
        }
        CompressionAlgorithm::Unknown => Err(Error::Protocol(
            "cannot encode payload with an unknown compression algorithm",
        )),
    }
}

fn decompress_payload(payload: &[u8], compression: CompressionAlgorithm) -> Result<Vec<u8>> {
    match compression {
        CompressionAlgorithm::None => Ok(payload.to_vec()),
        CompressionAlgorithm::Zlib => {
            let mut decoder = ZlibDecoder::new(payload);
            let mut out = Vec::new();
            decoder.read_to_end(&mut out).map_err(|error| {
                Error::ProtocolMessage(format!("zlib decompression failed: {error}"))
            })?;
            Ok(out)
        }
        CompressionAlgorithm::Unknown => Err(Error::Protocol(
            "cannot decode payload with an unknown compression algorithm",
        )),
    }
}

fn property_value_type(value: &MessagePropertyValue) -> MessagePropertyType {
    match value {
        MessagePropertyValue::Bool(_) => MessagePropertyType::Bool,
        MessagePropertyValue::Byte(_) => MessagePropertyType::Byte,
        MessagePropertyValue::Short(_) => MessagePropertyType::Short,
        MessagePropertyValue::Int32(_) => MessagePropertyType::Int32,
        MessagePropertyValue::Int64(_) => MessagePropertyType::Int64,
        MessagePropertyValue::String(_) => MessagePropertyType::String,
        MessagePropertyValue::Binary(_) => MessagePropertyType::Binary,
    }
}

fn encode_property_value(value: &MessagePropertyValue) -> Result<Vec<u8>> {
    Ok(match value {
        MessagePropertyValue::Bool(value) => vec![u8::from(*value)],
        MessagePropertyValue::Byte(value) => vec![*value as u8],
        MessagePropertyValue::Short(value) => value.to_be_bytes().to_vec(),
        MessagePropertyValue::Int32(value) => value.to_be_bytes().to_vec(),
        MessagePropertyValue::Int64(value) => value.to_be_bytes().to_vec(),
        MessagePropertyValue::String(value) => {
            if !value.is_ascii() {
                return Err(Error::Protocol("message property strings must be ASCII"));
            }
            value.as_bytes().to_vec()
        }
        MessagePropertyValue::Binary(value) => value.to_vec(),
    })
}

fn decode_property_value(
    property_type: MessagePropertyType,
    value_bytes: &[u8],
) -> Result<MessagePropertyValue> {
    Ok(match property_type {
        MessagePropertyType::Bool => {
            if value_bytes.len() != 1 {
                return Err(Error::Protocol("boolean properties must be one byte"));
            }
            MessagePropertyValue::Bool(value_bytes[0] != 0)
        }
        MessagePropertyType::Byte => {
            if value_bytes.len() != 1 {
                return Err(Error::Protocol("byte properties must be one byte"));
            }
            MessagePropertyValue::Byte(value_bytes[0] as i8)
        }
        MessagePropertyType::Short => {
            if value_bytes.len() != 2 {
                return Err(Error::Protocol("short properties must be two bytes"));
            }
            MessagePropertyValue::Short(i16::from_be_bytes(value_bytes.try_into().unwrap()))
        }
        MessagePropertyType::Int32 => {
            if value_bytes.len() != 4 {
                return Err(Error::Protocol("int32 properties must be four bytes"));
            }
            MessagePropertyValue::Int32(i32::from_be_bytes(value_bytes.try_into().unwrap()))
        }
        MessagePropertyType::Int64 => {
            if value_bytes.len() != 8 {
                return Err(Error::Protocol("int64 properties must be eight bytes"));
            }
            MessagePropertyValue::Int64(i64::from_be_bytes(value_bytes.try_into().unwrap()))
        }
        MessagePropertyType::String => {
            if !value_bytes.is_ascii() {
                return Err(Error::Protocol("message property strings must be ASCII"));
            }
            MessagePropertyValue::String(
                String::from_utf8(value_bytes.to_vec())
                    .map_err(|error| Error::ProtocolMessage(error.to_string()))?,
            )
        }
        MessagePropertyType::Binary => {
            MessagePropertyValue::Binary(Bytes::copy_from_slice(value_bytes))
        }
        MessagePropertyType::Undefined => {
            return Err(Error::Protocol(
                "undefined message property type is invalid",
            ));
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EncodedMessagePropertiesHeader {
    header_size: u8,
    property_header_size: u8,
    area_words: u32,
    num_properties: u8,
}

impl EncodedMessagePropertiesHeader {
    fn encode(self, buf: &mut BytesMut) {
        let combined = ((self.property_header_size / 2) << 3) | ((self.header_size / 2) & 0b111);
        buf.put_u8(combined);
        buf.put_u8(((self.area_words >> 16) & 0xff) as u8);
        buf.put_u16((self.area_words & 0xffff) as u16);
        buf.put_u8(0);
        buf.put_u8(self.num_properties);
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < MESSAGE_PROPERTIES_HEADER_SIZE {
            return Err(Error::Protocol("message properties header is truncated"));
        }
        let combined = bytes[0];
        let header_size = 2 * (combined & 0b111);
        let property_header_size = 2 * ((combined >> 3) & 0b111);
        if header_size == 0 || property_header_size == 0 {
            return Err(Error::Protocol(
                "message properties header sizes are invalid",
            ));
        }
        let area_words =
            (u32::from(bytes[1]) << 16) | u32::from(u16::from_be_bytes([bytes[2], bytes[3]]));
        Ok(Self {
            header_size,
            property_header_size,
            area_words,
            num_properties: bytes[5],
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EncodedMessagePropertyHeader {
    property_type: MessagePropertyType,
    value_field: u32,
    name_len: u16,
}

impl EncodedMessagePropertyHeader {
    fn encode(self, buf: &mut BytesMut) -> Result<()> {
        if self.value_field >= (1 << 26) {
            return Err(Error::Protocol("message property value field is too large"));
        }
        if usize::from(self.name_len) >= (1 << 12) {
            return Err(Error::Protocol("message property name is too large"));
        }
        let upper =
            ((self.property_type as u16) << 10) | ((self.value_field >> 16) as u16 & 0x03ff);
        buf.put_u16(upper);
        buf.put_u16((self.value_field & 0xffff) as u16);
        buf.put_u16(self.name_len & 0x0fff);
        Ok(())
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < MESSAGE_PROPERTY_HEADER_SIZE {
            return Err(Error::Protocol("message property header is truncated"));
        }
        let upper = u16::from_be_bytes(bytes[0..2].try_into().unwrap());
        let lower = u16::from_be_bytes(bytes[2..4].try_into().unwrap());
        let name_len = u16::from_be_bytes(bytes[4..6].try_into().unwrap()) & 0x0fff;
        let property_type = MessagePropertyType::from_u8(((upper >> 10) & 0x1f) as u8)?;
        let value_field = (u32::from(upper & 0x03ff) << 16) | u32::from(lower);
        Ok(Self {
            property_type,
            value_field,
            name_len,
        })
    }
}

fn encode_message_properties(properties: &MessageProperties) -> Result<Bytes> {
    if properties.is_empty() {
        return Ok(Bytes::new());
    }

    let mut headers = Vec::with_capacity(properties.len());
    let mut names_and_values = Vec::with_capacity(properties.len());
    let mut offset = 0_u32;
    let mut body_len = 0_usize;

    for property in properties.iter() {
        if !property.name.is_ascii() {
            return Err(Error::Protocol("message property names must be ASCII"));
        }
        if property.name.len() >= (1 << 12) {
            return Err(Error::Protocol("message property name is too large"));
        }
        let value_bytes = encode_property_value(&property.value)?;
        let property_len = property.name.len() + value_bytes.len();
        headers.push(EncodedMessagePropertyHeader {
            property_type: property_value_type(&property.value),
            value_field: offset,
            name_len: property.name.len() as u16,
        });
        names_and_values.push((property.name.as_bytes().to_vec(), value_bytes));
        offset = offset
            .checked_add(property_len as u32)
            .ok_or_else(|| Error::ProtocolMessage("message properties body is too large".into()))?;
        body_len += property_len;
    }

    let raw_len =
        MESSAGE_PROPERTIES_HEADER_SIZE + headers.len() * MESSAGE_PROPERTY_HEADER_SIZE + body_len;
    let padding = appdata_padding(raw_len);
    let area_len = raw_len + padding;
    let area_words = u32::try_from(area_len / WORD_SIZE)
        .map_err(|_| Error::Protocol("message properties area is too large"))?;

    let mut encoded = BytesMut::with_capacity(area_len);
    EncodedMessagePropertiesHeader {
        header_size: MESSAGE_PROPERTIES_HEADER_SIZE as u8,
        property_header_size: MESSAGE_PROPERTY_HEADER_SIZE as u8,
        area_words,
        num_properties: u8::try_from(headers.len())
            .map_err(|_| Error::Protocol("too many message properties"))?,
    }
    .encode(&mut encoded);

    for header in headers {
        header.encode(&mut encoded)?;
    }

    for (name, value) in names_and_values {
        encoded.extend_from_slice(&name);
        encoded.extend_from_slice(&value);
    }
    encoded.extend_from_slice(appdata_padding_bytes(padding));
    Ok(encoded.freeze())
}

fn decode_message_properties(data: &[u8], old_style: bool) -> Result<(MessageProperties, usize)> {
    let header = EncodedMessagePropertiesHeader::decode(data)?;
    let area_len = usize::try_from(header.area_words)
        .ok()
        .and_then(|words| words.checked_mul(WORD_SIZE))
        .ok_or(Error::Protocol("message properties area is too large"))?;
    if data.len() < area_len {
        return Err(Error::Protocol("message properties area is truncated"));
    }

    let area = &data[..area_len];
    let unpadded_area = strip_appdata_padding(area)?;
    let header_size = usize::from(header.header_size);
    let property_header_size = usize::from(header.property_header_size);
    let property_count = usize::from(header.num_properties);
    let properties_header_bytes = header_size
        .checked_add(property_count * property_header_size)
        .ok_or(Error::Protocol("message properties headers are too large"))?;
    if unpadded_area.len() < properties_header_bytes {
        return Err(Error::Protocol(
            "message properties header area is truncated",
        ));
    }

    let mut encoded_headers = Vec::with_capacity(property_count);
    let mut offset = header_size;
    for _ in 0..property_count {
        encoded_headers.push(EncodedMessagePropertyHeader::decode(
            &unpadded_area[offset..offset + property_header_size],
        )?);
        offset += property_header_size;
    }

    let body = &unpadded_area[properties_header_bytes..];
    let mut cursor = 0_usize;
    let mut properties = Vec::with_capacity(property_count);

    for (idx, encoded_header) in encoded_headers.iter().enumerate() {
        let name_len = usize::from(encoded_header.name_len);
        if cursor + name_len > body.len() {
            return Err(Error::Protocol("message property name exceeds body size"));
        }

        let expected_offset = if old_style {
            cursor
        } else {
            usize::try_from(encoded_header.value_field)
                .map_err(|_| Error::Protocol("message property offset is too large"))?
        };
        if expected_offset != cursor {
            return Err(Error::Protocol("message property offsets are inconsistent"));
        }

        let value_len = if old_style {
            usize::try_from(encoded_header.value_field)
                .map_err(|_| Error::Protocol("message property value length is too large"))?
        } else {
            let next_offset = if let Some(next) = encoded_headers.get(idx + 1) {
                usize::try_from(next.value_field)
                    .map_err(|_| Error::Protocol("message property offset is too large"))?
            } else {
                body.len()
            };
            next_offset
                .checked_sub(cursor)
                .and_then(|remaining| remaining.checked_sub(name_len))
                .ok_or(Error::Protocol("message property offsets are invalid"))?
        };

        if cursor + name_len + value_len > body.len() {
            return Err(Error::Protocol("message property value exceeds body size"));
        }

        let name_bytes = &body[cursor..cursor + name_len];
        if !name_bytes.is_ascii() {
            return Err(Error::Protocol("message property names must be ASCII"));
        }
        let name = String::from_utf8(name_bytes.to_vec())
            .map_err(|error| Error::ProtocolMessage(error.to_string()))?;
        cursor += name_len;

        let value_bytes = &body[cursor..cursor + value_len];
        let value = decode_property_value(encoded_header.property_type, value_bytes)?;
        cursor += value_len;

        properties.push(MessageProperty { name, value });
    }

    if cursor != body.len() {
        return Err(Error::Protocol(
            "message properties body contains trailing bytes",
        ));
    }

    Ok((MessageProperties::from(properties), area_len))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedApplicationData {
    payload: Bytes,
    properties: MessageProperties,
}

fn decode_application_data(
    data: &[u8],
    has_properties: bool,
    old_style_properties: bool,
    compression: CompressionAlgorithm,
) -> Result<DecodedApplicationData> {
    if data.is_empty() {
        return Ok(DecodedApplicationData {
            payload: Bytes::new(),
            properties: MessageProperties::default(),
        });
    }

    let unpadded = strip_appdata_padding(data)?;

    match compression {
        CompressionAlgorithm::None => {
            if has_properties {
                let (properties, consumed) =
                    decode_message_properties(unpadded, old_style_properties)?;
                Ok(DecodedApplicationData {
                    payload: Bytes::copy_from_slice(&unpadded[consumed..]),
                    properties,
                })
            } else {
                Ok(DecodedApplicationData {
                    payload: Bytes::copy_from_slice(unpadded),
                    properties: MessageProperties::default(),
                })
            }
        }
        CompressionAlgorithm::Zlib => {
            if has_properties && !old_style_properties {
                let (properties, consumed) = decode_message_properties(unpadded, false)?;
                let payload = decompress_payload(&unpadded[consumed..], compression)?;
                Ok(DecodedApplicationData {
                    payload: Bytes::from(payload),
                    properties,
                })
            } else {
                let decompressed = decompress_payload(unpadded, compression)?;
                if has_properties {
                    let (properties, consumed) = decode_message_properties(&decompressed, true)?;
                    Ok(DecodedApplicationData {
                        payload: Bytes::copy_from_slice(&decompressed[consumed..]),
                        properties,
                    })
                } else {
                    Ok(DecodedApplicationData {
                        payload: Bytes::from(decompressed),
                        properties: MessageProperties::default(),
                    })
                }
            }
        }
        CompressionAlgorithm::Unknown => Err(Error::Protocol(
            "cannot decode application data with an unknown compression algorithm",
        )),
    }
}

/// Encodes a schema payload into a complete event frame.
pub fn encode_schema_event<T: Serialize + BerCodec>(
    message: &T,
    event_type: EventType,
    encoding: EncodingType,
) -> Result<Bytes> {
    let payload = match encoding {
        EncodingType::Json => serde_json::to_vec(message)?,
        EncodingType::Ber => message.encode_ber()?,
    };
    let padding = zero_pad_to_word(payload.len());
    let mut header = EventHeader::new(event_type);
    if matches!(event_type, EventType::Control | EventType::Authentication) {
        header.set_control_encoding(encoding);
    }
    header.length = (EVENT_HEADER_SIZE + payload.len() + padding) as u32;

    let mut frame = BytesMut::with_capacity(header.length as usize);
    frame.extend_from_slice(&header.encode());
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(appdata_padding_bytes(padding));
    Ok(frame.freeze())
}

/// Encodes a control-plane payload into a complete control event frame.
pub fn encode_control_event<T: Serialize + BerCodec>(
    message: &T,
    encoding: EncodingType,
) -> Result<Bytes> {
    encode_schema_event(message, EventType::Control, encoding)
}

/// Decodes a control or authentication schema payload from an event frame.
pub fn decode_control_event<T: DeserializeOwned + BerCodec>(
    header: &EventHeader,
    payload: &[u8],
) -> Result<T> {
    match header.schema_encoding()? {
        EncodingType::Json => {
            let payload = strip_protocol_padding(payload);
            let trimmed = payload
                .iter()
                .rposition(|byte| *byte != 0)
                .map(|idx| &payload[..=idx])
                .unwrap_or(payload);
            Ok(serde_json::from_slice(trimmed)?)
        }
        EncodingType::Ber => T::decode_ber(payload),
    }
}

/// Encodes a heartbeat response event.
pub fn encode_heartbeat_response() -> Bytes {
    let mut header = EventHeader::new(EventType::HeartbeatRsp);
    header.length = EVENT_HEADER_SIZE as u32;
    Bytes::copy_from_slice(&header.encode())
}

/// Flag bits carried by [`PutHeader::flags`].
pub mod put_header_flags {
    /// Broker should send an acknowledgement for the message.
    pub const ACK_REQUESTED: u8 = 1 << 0;
    /// Application data contains encoded message properties.
    pub const MESSAGE_PROPERTIES: u8 = 1 << 1;
}

/// Flag bits carried by [`PushHeader::flags`].
pub mod push_header_flags {
    /// Message payload is implicit and omitted from the frame.
    pub const IMPLICIT_PAYLOAD: u8 = 1 << 0;
    /// Application data contains encoded message properties.
    pub const MESSAGE_PROPERTIES: u8 = 1 << 1;
    /// Broker marked the delivery out of order.
    pub const OUT_OF_ORDER: u8 = 1 << 2;
}

/// Re-export of [`push_header_flags`] using the protocol naming style.
pub use push_header_flags as PushHeaderFlags;
/// Re-export of [`put_header_flags`] using the protocol naming style.
pub use put_header_flags as PutHeaderFlags;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum OptionType {
    Undefined = 0,
    SubQueueIdsOld = 1,
    MsgGroupId = 2,
    SubQueueInfos = 3,
}

impl OptionType {
    fn from_u8(value: u8) -> Result<Self> {
        Ok(match value {
            1 => Self::SubQueueIdsOld,
            2 => Self::MsgGroupId,
            3 => Self::SubQueueInfos,
            0 => Self::Undefined,
            _ => return Err(Error::Protocol("unknown option type")),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OptionHeader {
    option_type: OptionType,
    packed: bool,
    type_specific: u8,
    words: u32,
}

impl OptionHeader {
    const SIZE: usize = WORD_SIZE;

    fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < Self::SIZE {
            return Err(Error::Protocol("option header is truncated"));
        }
        let raw = u32::from_be_bytes(src[..Self::SIZE].try_into().unwrap());
        Ok(Self {
            option_type: OptionType::from_u8(((raw >> 26) & 0x3f) as u8)?,
            packed: ((raw >> 25) & 0x1) != 0,
            type_specific: ((raw >> 21) & 0x0f) as u8,
            words: raw & 0x1f_ffff,
        })
    }

    fn encoded_len_bytes(self) -> Result<usize> {
        let words =
            usize::try_from(self.words).map_err(|_| Error::Protocol("option is too large"))?;
        words
            .checked_mul(WORD_SIZE)
            .ok_or(Error::Protocol("option is too large"))
    }
}

/// Redelivery-attempt metadata attached to a pushed message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RdaInfo {
    /// Redelivery counter.
    pub counter: u8,
    /// Whether the broker considers redelivery unlimited.
    pub is_unlimited: bool,
    /// Whether the message has been marked poisonous.
    pub is_poisonous: bool,
}

impl Default for RdaInfo {
    fn default() -> Self {
        Self {
            counter: 0,
            is_unlimited: true,
            is_poisonous: false,
        }
    }
}

impl RdaInfo {
    fn decode(byte: u8) -> Self {
        Self {
            counter: byte & 0x3f,
            is_unlimited: byte & 0x80 != 0,
            is_poisonous: byte & 0x40 != 0,
        }
    }
}

/// Sub-queue metadata attached to a pushed message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SubQueueInfo {
    /// Sub-queue identifier.
    pub sub_queue_id: u32,
    /// Redelivery-attempt metadata.
    pub rda_info: RdaInfo,
}

/// Per-message header inside a `PUT` event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutHeader {
    /// PUT flags such as acknowledgement requested or properties present.
    pub flags: u8,
    /// Total message size in protocol words.
    pub message_words: u32,
    /// Options area size in protocol words.
    pub options_words: u32,
    /// Payload compression algorithm.
    pub compression: CompressionAlgorithm,
    /// Header size in protocol words.
    pub header_words: u8,
    /// Queue id targeted by the message.
    pub queue_id: u32,
    /// Message GUID field used by the protocol.
    pub guid_or_correlation: MessageGuid,
    /// CRC32C over the uncompressed payload.
    pub crc32c: u32,
    /// Schema wire id attached to the payload.
    pub schema_wire_id: i16,
}

impl PutHeader {
    /// Size of the encoded PUT header.
    pub const SIZE: usize = 36;
    const HEADER_WORDS: u8 = (Self::SIZE / WORD_SIZE) as u8;

    /// Encodes the header into the provided buffer.
    pub fn encode(self, buf: &mut BytesMut) {
        let flags_and_words = (u32::from(self.flags) << 28) | (self.message_words & 0x0fff_ffff);
        let options_and_meta = ((self.options_words & 0x00ff_ffff) << 8)
            | ((self.compression as u32 & 0x7) << 5)
            | u32::from(self.header_words & 0x1f);
        buf.put_u32(flags_and_words);
        buf.put_u32(options_and_meta);
        buf.put_u32(self.queue_id);
        buf.extend_from_slice(&self.guid_or_correlation.0);
        buf.put_u32(self.crc32c);
        buf.put_i16(self.schema_wire_id);
        buf.put_u16(0);
    }

    /// Decodes a PUT header from bytes.
    pub fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < Self::SIZE {
            return Err(Error::Protocol("put header is truncated"));
        }
        let flags_and_words = u32::from_be_bytes(src[0..4].try_into().unwrap());
        let options_and_meta = u32::from_be_bytes(src[4..8].try_into().unwrap());
        Ok(Self {
            flags: (flags_and_words >> 28) as u8,
            message_words: flags_and_words & 0x0fff_ffff,
            options_words: (options_and_meta >> 8) & 0x00ff_ffff,
            compression: CompressionAlgorithm::from_u8(((options_and_meta >> 5) & 0x7) as u8),
            header_words: (options_and_meta & 0x1f) as u8,
            queue_id: u32::from_be_bytes(src[8..12].try_into().unwrap()),
            guid_or_correlation: MessageGuid::from_slice(&src[12..28])?,
            crc32c: u32::from_be_bytes(src[28..32].try_into().unwrap()),
            schema_wire_id: i16::from_be_bytes(src[32..34].try_into().unwrap()),
        })
    }
}

/// High-level description of a message to include in a `PUT` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundPutFrame {
    /// Queue id targeted by the message.
    pub queue_id: u32,
    /// Application payload.
    pub payload: Bytes,
    /// Message properties delivered alongside the payload.
    pub properties: MessageProperties,
    /// Optional producer correlation id.
    pub correlation_id: Option<u32>,
    /// Optional explicit message GUID.
    pub message_guid: Option<MessageGuid>,
    /// Payload compression algorithm.
    pub compression: CompressionAlgorithm,
}

/// Encodes one or more outbound messages into a complete `PUT` event.
pub fn encode_put_event(messages: &[OutboundPutFrame]) -> Result<Bytes> {
    if messages.is_empty() {
        return Err(Error::Protocol(
            "PUT event must contain at least one message",
        ));
    }

    let mut payload = BytesMut::new();
    for message in messages {
        let properties = encode_message_properties(&message.properties)?;
        let encoded_payload = compress_payload(&message.payload, message.compression)?;
        let appdata_len = properties.len() + encoded_payload.len();
        let padded_len = appdata_len + appdata_padding(appdata_len);
        let message_words = ((PutHeader::SIZE + padded_len) / WORD_SIZE) as u32;
        let mut flags = 0_u8;
        if !message.properties.is_empty() {
            flags |= PutHeaderFlags::MESSAGE_PROPERTIES;
        }
        if message.correlation_id.is_some() {
            flags |= PutHeaderFlags::ACK_REQUESTED;
        }
        let guid_or_correlation = message.message_guid.unwrap_or_default();

        PutHeader {
            flags,
            message_words,
            options_words: 0,
            compression: message.compression,
            header_words: PutHeader::HEADER_WORDS,
            queue_id: message.queue_id,
            guid_or_correlation,
            crc32c: crc32c(&message.payload),
            schema_wire_id: if message.properties.is_empty() {
                0
            } else {
                INVALID_SCHEMA_WIRE_ID
            },
        }
        .encode(&mut payload);

        payload.extend_from_slice(&properties);
        payload.extend_from_slice(&encoded_payload);
        payload.extend_from_slice(appdata_padding_bytes(appdata_padding(appdata_len)));
    }

    let mut header = EventHeader::new(EventType::Put);
    header.length = (EVENT_HEADER_SIZE + payload.len()) as u32;
    let mut frame = BytesMut::with_capacity(header.length as usize);
    frame.extend_from_slice(&header.encode());
    frame.extend_from_slice(&payload);
    Ok(frame.freeze())
}

/// Per-message header inside a `PUSH` event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushHeader {
    /// PUSH flags such as implicit payload or properties present.
    pub flags: u8,
    /// Total message size in protocol words.
    pub message_words: u32,
    /// Options area size in protocol words.
    pub options_words: u32,
    /// Payload compression algorithm.
    pub compression: CompressionAlgorithm,
    /// Header size in protocol words.
    pub header_words: u8,
    /// Queue id targeted by the message.
    pub queue_id: u32,
    /// Message GUID assigned by the producer.
    pub message_guid: MessageGuid,
    /// Schema wire id attached to the payload.
    pub schema_wire_id: i16,
}

impl PushHeader {
    /// Size of a push header when a schema id field is present.
    pub const SIZE_WITH_SCHEMA_ID: usize = 32;

    /// Decodes a PUSH header from bytes.
    pub fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < 28 {
            return Err(Error::Protocol("push header is truncated"));
        }
        let flags_and_words = u32::from_be_bytes(src[0..4].try_into().unwrap());
        let options_and_meta = u32::from_be_bytes(src[4..8].try_into().unwrap());
        let header_words = (options_and_meta & 0x1f) as u8;
        let header_bytes = usize::from(header_words) * WORD_SIZE;
        if src.len() < header_bytes || header_bytes < 28 {
            return Err(Error::Protocol("push header length is invalid"));
        }
        let schema_wire_id = if header_bytes >= 32 {
            i16::from_be_bytes(src[28..30].try_into().unwrap())
        } else {
            0
        };
        Ok(Self {
            flags: (flags_and_words >> 28) as u8,
            message_words: flags_and_words & 0x0fff_ffff,
            options_words: (options_and_meta >> 8) & 0x00ff_ffff,
            compression: CompressionAlgorithm::from_u8(((options_and_meta >> 5) & 0x7) as u8),
            header_words,
            queue_id: u32::from_be_bytes(src[8..12].try_into().unwrap()),
            message_guid: MessageGuid::from_slice(&src[12..28])?,
            schema_wire_id,
        })
    }
}

/// Fully decoded message from a `PUSH` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushMessage {
    /// Decoded push header.
    pub header: PushHeader,
    /// Application payload.
    pub payload: Bytes,
    /// Message properties delivered with the payload.
    pub properties: MessageProperties,
    /// Whether the properties used the legacy encoded representation.
    pub properties_are_old_style: bool,
    /// Sub-queue metadata carried in message options.
    pub sub_queue_infos: Vec<SubQueueInfo>,
    /// Optional message group id carried in message options.
    pub message_group_id: Option<String>,
}

impl PushMessage {
    /// Returns a span for application work triggered by this message.
    ///
    /// If the message carries propagated trace context and the process has a
    /// `tracing-opentelemetry` layer plus a configured text-map propagator,
    /// the returned span is created as a child of that remote context.
    pub fn handling_span(&self, operation: impl Into<String>) -> tracing::Span {
        let sub_queue_id = self
            .sub_queue_infos
            .first()
            .map(|info| info.sub_queue_id)
            .unwrap_or_default();
        crate::message_trace::handling_span(
            &self.properties,
            operation,
            self.header.queue_id,
            sub_queue_id,
            self.header.message_guid,
        )
    }
}

#[derive(Debug, Default)]
struct DecodedPushOptions {
    sub_queue_infos: Vec<SubQueueInfo>,
    message_group_id: Option<String>,
}

fn decode_sub_queue_infos(payload: &[u8], item_size: usize) -> Result<Vec<SubQueueInfo>> {
    if item_size < 8 || !payload.len().is_multiple_of(item_size) {
        return Err(Error::Protocol("sub-queue option payload is malformed"));
    }

    let mut out = Vec::with_capacity(payload.len() / item_size);
    for chunk in payload.chunks(item_size) {
        out.push(SubQueueInfo {
            sub_queue_id: u32::from_be_bytes(chunk[..4].try_into().unwrap()),
            rda_info: RdaInfo::decode(chunk[4]),
        });
    }
    Ok(out)
}

fn decode_push_options(data: &[u8]) -> Result<DecodedPushOptions> {
    let mut cursor = 0;
    let mut out = DecodedPushOptions::default();

    while cursor < data.len() {
        let header = OptionHeader::decode(&data[cursor..])?;
        let option_len = if header.packed {
            OptionHeader::SIZE
        } else {
            header.encoded_len_bytes()?
        };
        if option_len < OptionHeader::SIZE || cursor + option_len > data.len() {
            return Err(Error::Protocol("option length exceeds options area"));
        }

        let body = &data[cursor + OptionHeader::SIZE..cursor + option_len];
        match header.option_type {
            OptionType::Undefined => {}
            OptionType::SubQueueInfos => {
                if header.packed {
                    out.sub_queue_infos = vec![SubQueueInfo {
                        sub_queue_id: 0,
                        rda_info: RdaInfo::decode((header.words & 0xff) as u8),
                    }];
                } else {
                    out.sub_queue_infos = decode_sub_queue_infos(
                        body,
                        usize::from(header.type_specific) * WORD_SIZE,
                    )?;
                }
            }
            OptionType::SubQueueIdsOld => {
                if header.packed || !body.len().is_multiple_of(WORD_SIZE) {
                    return Err(Error::Protocol("legacy sub-queue option is malformed"));
                }
                out.sub_queue_infos = body
                    .chunks_exact(WORD_SIZE)
                    .map(|chunk| SubQueueInfo {
                        sub_queue_id: u32::from_be_bytes(chunk.try_into().unwrap()),
                        rda_info: RdaInfo::default(),
                    })
                    .collect();
            }
            OptionType::MsgGroupId => {
                if header.packed {
                    return Err(Error::Protocol(
                        "packed message group id options are invalid",
                    ));
                }
                let unpadded = strip_appdata_padding(body)?;
                out.message_group_id = Some(
                    String::from_utf8(unpadded.to_vec())
                        .map_err(|error| Error::ProtocolMessage(error.to_string()))?,
                );
            }
        }

        cursor += option_len;
    }

    if cursor != data.len() {
        return Err(Error::Protocol("options area contains trailing bytes"));
    }
    Ok(out)
}

/// Decodes a complete `PUSH` event payload.
pub fn decode_push_event(payload: &[u8]) -> Result<Vec<PushMessage>> {
    let mut offset = 0;
    let mut out = Vec::new();
    while offset < payload.len() {
        let header = PushHeader::decode(&payload[offset..])?;
        let header_bytes = usize::from(header.header_words) * WORD_SIZE;
        let message_bytes = usize::try_from(header.message_words).unwrap() * WORD_SIZE;
        if message_bytes < header_bytes || offset + message_bytes > payload.len() {
            return Err(Error::Protocol("push message size is invalid"));
        }
        let options_bytes = usize::try_from(header.options_words).unwrap() * WORD_SIZE;
        let data_start = offset + header_bytes + options_bytes;
        let data_end = offset + message_bytes;
        if data_start > data_end {
            return Err(Error::Protocol("push options exceed message size"));
        }
        let decoded_options = decode_push_options(&payload[offset + header_bytes..data_start])?;
        let has_properties = header.flags & PushHeaderFlags::MESSAGE_PROPERTIES != 0;
        let properties_are_old_style = has_properties && header.schema_wire_id == 0;
        let decoded =
            if header.flags & PushHeaderFlags::IMPLICIT_PAYLOAD != 0 && data_start == data_end {
                DecodedApplicationData {
                    payload: Bytes::new(),
                    properties: MessageProperties::default(),
                }
            } else {
                decode_application_data(
                    &payload[data_start..data_end],
                    has_properties,
                    properties_are_old_style,
                    header.compression,
                )?
            };
        out.push(PushMessage {
            header,
            payload: decoded.payload,
            properties: decoded.properties,
            properties_are_old_style,
            sub_queue_infos: decoded_options.sub_queue_infos,
            message_group_id: decoded_options.message_group_id,
        });
        offset = data_end;
    }
    Ok(out)
}

/// Header for an `ACK` event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckHeader {
    /// Header size in protocol words.
    pub header_words: u8,
    /// Size of each acknowledgement message in protocol words.
    pub per_message_words: u8,
    /// Event-level flags.
    pub flags: u8,
}

impl AckHeader {
    /// Size of the encoded ACK header.
    pub const SIZE: usize = 4;

    /// Decodes an ACK header from bytes.
    pub fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < Self::SIZE {
            return Err(Error::Protocol("ack header is truncated"));
        }
        let packed = src[0];
        Ok(Self {
            header_words: packed >> 4,
            per_message_words: packed & 0x0f,
            flags: src[1],
        })
    }
}

/// Single producer acknowledgement item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckMessage {
    /// Broker status code.
    pub status: u8,
    /// Producer correlation id.
    pub correlation_id: u32,
    /// GUID assigned to the original message.
    pub message_guid: MessageGuid,
    /// Queue id acknowledged by the broker.
    pub queue_id: u32,
}

impl AckMessage {
    /// Size of the encoded ACK message.
    pub const SIZE: usize = 24;

    /// Decodes an ACK message from bytes.
    pub fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < Self::SIZE {
            return Err(Error::Protocol("ack message is truncated"));
        }
        let status_and_id = u32::from_be_bytes(src[0..4].try_into().unwrap());
        Ok(Self {
            status: ((status_and_id >> 24) & 0x0f) as u8,
            correlation_id: status_and_id & 0x00ff_ffff,
            message_guid: MessageGuid::from_slice(&src[4..20])?,
            queue_id: u32::from_be_bytes(src[20..24].try_into().unwrap()),
        })
    }
}

/// Decodes a complete `ACK` event payload.
pub fn decode_ack_event(payload: &[u8]) -> Result<Vec<AckMessage>> {
    let header = AckHeader::decode(payload)?;
    let header_bytes = usize::from(header.header_words) * WORD_SIZE;
    let message_bytes = usize::from(header.per_message_words) * WORD_SIZE;
    if header_bytes < AckHeader::SIZE
        || message_bytes < AckMessage::SIZE
        || payload.len() < header_bytes
    {
        return Err(Error::Protocol("ack event layout is invalid"));
    }
    let mut offset = header_bytes;
    let mut out = Vec::new();
    while offset + message_bytes <= payload.len() {
        out.push(AckMessage::decode(
            &payload[offset..offset + message_bytes],
        )?);
        offset += message_bytes;
    }
    if offset != payload.len() {
        return Err(Error::Protocol("ack event contains trailing bytes"));
    }
    Ok(out)
}

/// Header for a `CONFIRM` event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfirmHeader {
    /// Header size in protocol words.
    pub header_words: u8,
    /// Size of each confirm message in protocol words.
    pub per_message_words: u8,
}

impl ConfirmHeader {
    /// Size of the encoded confirm header.
    pub const SIZE: usize = 4;

    /// Returns the standard client/broker confirm header.
    pub fn new() -> Self {
        Self {
            header_words: 1,
            per_message_words: 6,
        }
    }

    /// Decodes a confirm header from bytes.
    pub fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < Self::SIZE {
            return Err(Error::Protocol("confirm header is truncated"));
        }
        let packed = src[0];
        Ok(Self {
            header_words: packed >> 4,
            per_message_words: packed & 0x0f,
        })
    }

    /// Encodes the header into the provided buffer.
    pub fn encode(self, buf: &mut BytesMut) {
        buf.put_u8((self.header_words << 4) | (self.per_message_words & 0x0f));
        buf.put_u8(0);
        buf.put_u16(0);
    }
}

impl Default for ConfirmHeader {
    fn default() -> Self {
        Self::new()
    }
}

/// Single consumer confirm item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmMessage {
    /// Queue id being confirmed.
    pub queue_id: u32,
    /// GUID of the delivered message.
    pub message_guid: MessageGuid,
    /// Sub-queue id associated with the delivered message.
    pub sub_queue_id: u32,
}

impl ConfirmMessage {
    /// Size of the encoded confirm message.
    pub const SIZE: usize = 24;

    /// Encodes the message into the provided buffer.
    pub fn encode(self, buf: &mut BytesMut) {
        buf.put_u32(self.queue_id);
        buf.extend_from_slice(&self.message_guid.0);
        buf.put_u32(self.sub_queue_id);
    }
}

/// Encodes one or more confirmations into a complete `CONFIRM` event.
pub fn encode_confirm_event(messages: &[ConfirmMessage]) -> Result<Bytes> {
    if messages.is_empty() {
        return Err(Error::Protocol(
            "CONFIRM event must contain at least one message",
        ));
    }
    let mut payload =
        BytesMut::with_capacity(ConfirmHeader::SIZE + messages.len() * ConfirmMessage::SIZE);
    ConfirmHeader::new().encode(&mut payload);
    for message in messages.iter().cloned() {
        message.encode(&mut payload);
    }
    let mut header = EventHeader::new(EventType::Confirm);
    header.length = (EVENT_HEADER_SIZE + payload.len()) as u32;
    let mut frame = BytesMut::with_capacity(header.length as usize);
    frame.extend_from_slice(&header.encode());
    frame.extend_from_slice(&payload);
    Ok(frame.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        AuthenticationMessage, AuthenticationPayload, AuthenticationRequest, ClientIdentity,
        ClientLanguage, ClientType, ClusterMessage, ControlMessage, ControlPayload, GuidInfo,
        NegotiationMessage, NegotiationPayload, OpenQueue, QueueHandleParameters, Status,
        StatusCategory,
    };

    fn encode_test_option(
        option_type: OptionType,
        packed: bool,
        type_specific: u8,
        words: u32,
        payload: &[u8],
    ) -> Vec<u8> {
        let header = ((option_type as u32 & 0x3f) << 26)
            | (u32::from(packed) << 25)
            | (u32::from(type_specific & 0x0f) << 21)
            | (words & 0x1f_ffff);
        let mut out = Vec::new();
        out.extend_from_slice(&header.to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn encode_test_push_message(options: &[u8], appdata: &[u8], flags: u8) -> Vec<u8> {
        let header_words = 8_u8;
        let message_words =
            ((usize::from(header_words) * WORD_SIZE + options.len() + appdata.len()) / WORD_SIZE)
                as u32;
        let mut out = Vec::new();
        out.extend_from_slice(&((u32::from(flags) << 28) | message_words).to_be_bytes());
        out.extend_from_slice(
            &(((options.len() / WORD_SIZE) as u32) << 8 | u32::from(header_words)).to_be_bytes(),
        );
        out.extend_from_slice(&17_u32.to_be_bytes());
        out.extend_from_slice(&[9; 16]);
        out.extend_from_slice(&INVALID_SCHEMA_WIRE_ID.to_be_bytes());
        out.extend_from_slice(&0_u16.to_be_bytes());
        out.extend_from_slice(options);
        out.extend_from_slice(appdata);
        out
    }

    #[test]
    fn event_header_round_trip() {
        let mut header = EventHeader::new(EventType::Control);
        header.length = 36;
        header.set_control_encoding(EncodingType::Json);
        let bytes = header.encode();
        let decoded = EventHeader::decode(&bytes).unwrap();
        assert_eq!(decoded.length, 36);
        assert_eq!(decoded.event_type, EventType::Control);
        assert_eq!(decoded.control_encoding().unwrap(), EncodingType::Json);
    }

    #[test]
    fn control_event_round_trip() {
        let message = ControlMessage::request(
            3,
            ControlPayload::OpenQueue(OpenQueue {
                handle_parameters: QueueHandleParameters {
                    uri: "bmq://bmq.test.mem.priority/example".into(),
                    q_id: 1,
                    sub_id_info: None,
                    flags: 12,
                    read_count: 0,
                    write_count: 1,
                    admin_count: 0,
                },
            }),
        );

        let frame = encode_control_event(&message, EncodingType::Json).unwrap();
        let (header, payload) = decode_frame(&frame).unwrap();
        let decoded: ControlMessage = decode_control_event(&header, payload).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn control_event_round_trip_ber() {
        let message = ControlMessage::request(
            7,
            ControlPayload::Status(Status {
                category: StatusCategory::Refused,
                code: -6,
                message: "denied".into(),
            }),
        );

        let frame = encode_control_event(&message, EncodingType::Ber).unwrap();
        let (header, payload) = decode_frame(&frame).unwrap();
        assert_eq!(header.control_encoding().unwrap(), EncodingType::Ber);
        let decoded: ControlMessage = decode_control_event(&header, payload).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn control_event_round_trip_cluster_message_ber() {
        let message = ControlMessage::request(
            9,
            ControlPayload::ClusterMessage(ClusterMessage {
                raw: vec![0x9f, 0x81, 0x01, 0x01, 0xff],
            }),
        );

        let frame = encode_control_event(&message, EncodingType::Ber).unwrap();
        let (header, payload) = decode_frame(&frame).unwrap();
        let decoded: ControlMessage = decode_control_event(&header, payload).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn control_event_ber_uses_untagged_choice_encoding() {
        let message = ControlMessage::request(
            1,
            ControlPayload::OpenQueue(OpenQueue {
                handle_parameters: QueueHandleParameters {
                    uri: "bmq://bmq.test.mem.priority/hello-world".into(),
                    q_id: 1,
                    sub_id_info: None,
                    flags: 0b1110,
                    read_count: 1,
                    write_count: 1,
                    admin_count: 0,
                },
            }),
        );

        let frame = encode_control_event(&message, EncodingType::Ber).unwrap();
        let (_header, payload) = decode_frame(&frame).unwrap();
        assert_eq!(&payload[..7], &[0x30, 0x80, 0x80, 0x01, 0x01, 0xaa, 0x80]);
    }

    #[test]
    fn negotiation_and_authentication_round_trip_ber() {
        let negotiation = NegotiationMessage {
            payload: NegotiationPayload::ClientIdentity(ClientIdentity {
                protocol_version: 1,
                sdk_version: 999_999,
                client_type: ClientType::TcpClient,
                process_name: "blazox-test".into(),
                pid: 1234,
                session_id: 2,
                host_name: "localhost".into(),
                features: "PROTOCOL_ENCODING:BER,JSON".into(),
                cluster_name: String::new(),
                cluster_node_id: -1,
                sdk_language: ClientLanguage::Cpp,
                guid_info: GuidInfo {
                    client_id: "client".into(),
                    nano_seconds_from_epoch: 42,
                },
                user_agent: "blazox/test".into(),
            }),
        };
        let negotiation_frame = encode_control_event(&negotiation, EncodingType::Ber).unwrap();
        let (negotiation_header, negotiation_payload) = decode_frame(&negotiation_frame).unwrap();
        let decoded_negotiation: NegotiationMessage =
            decode_control_event(&negotiation_header, negotiation_payload).unwrap();
        assert_eq!(decoded_negotiation, negotiation);

        let authentication = AuthenticationMessage {
            payload: AuthenticationPayload::AuthenticationRequest(AuthenticationRequest {
                mechanism: "ANONYMOUS".into(),
                data: Some(vec![1, 2, 3, 4]),
            }),
        };
        let authentication_frame = encode_schema_event(
            &authentication,
            EventType::Authentication,
            EncodingType::Ber,
        )
        .unwrap();
        let (authentication_header, authentication_payload) =
            decode_frame(&authentication_frame).unwrap();
        assert_eq!(
            authentication_header.schema_encoding().unwrap(),
            EncodingType::Ber
        );
        let decoded_authentication: AuthenticationMessage =
            decode_control_event(&authentication_header, authentication_payload).unwrap();
        assert_eq!(decoded_authentication, authentication);
    }

    #[test]
    fn negotiation_and_authentication_decode_unwrapped_ber_payloads() {
        let negotiation = NegotiationMessage {
            payload: NegotiationPayload::BrokerResponse(crate::schema::BrokerResponse {
                result: Status::success(),
                protocol_version: 1,
                broker_version: 3,
                is_deprecated_sdk: false,
                broker_identity: ClientIdentity {
                    protocol_version: 1,
                    sdk_version: 999_999,
                    client_type: ClientType::TcpBroker,
                    process_name: "bmqbrkr".into(),
                    pid: 7,
                    session_id: 1,
                    host_name: "localhost".into(),
                    features: "PROTOCOL_ENCODING:BER,JSON".into(),
                    cluster_name: "primary".into(),
                    cluster_node_id: 1,
                    sdk_language: ClientLanguage::Cpp,
                    guid_info: GuidInfo::default(),
                    user_agent: "bmqbrkr/test".into(),
                },
                heartbeat_interval_ms: 3000,
                max_missed_heartbeats: 10,
            }),
        };
        let negotiation_wrapped = encode_control_event(&negotiation, EncodingType::Ber).unwrap();
        let (_, negotiation_payload) = decode_frame(&negotiation_wrapped).unwrap();
        let unwrapped_negotiation = match negotiation.payload {
            NegotiationPayload::BrokerResponse(_) => {
                let root = crate::ber::BerCodec::encode_ber(&negotiation).unwrap();
                let root = &root[2..root.len() - 2];
                root.to_vec()
            }
            _ => unreachable!(),
        };
        let mut header = EventHeader::new(EventType::Control);
        header.set_control_encoding(EncodingType::Ber);
        let decoded_unwrapped: NegotiationMessage =
            decode_control_event(&header, &unwrapped_negotiation).unwrap();
        assert_eq!(decoded_unwrapped, negotiation);
        let decoded_wrapped: NegotiationMessage =
            decode_control_event(&header, negotiation_payload).unwrap();
        assert_eq!(decoded_wrapped, negotiation);
        let choice_wrapped_negotiation = {
            let choice = crate::ber::BerCodec::encode_ber(&negotiation).unwrap();
            let mut bytes = vec![0x30, 0x80];
            bytes.extend_from_slice(&choice);
            bytes.extend_from_slice(&[0, 0]);
            bytes
        };
        let decoded_choice_wrapped: NegotiationMessage =
            decode_control_event(&header, &choice_wrapped_negotiation).unwrap();
        assert_eq!(decoded_choice_wrapped, negotiation);

        let authentication = AuthenticationMessage {
            payload: AuthenticationPayload::AuthenticationResponse(
                crate::schema::AuthenticationResponse {
                    status: Status::success(),
                    lifetime_ms: Some(60_000),
                },
            ),
        };
        let wrapped = encode_schema_event(
            &authentication,
            EventType::Authentication,
            EncodingType::Ber,
        )
        .unwrap();
        let (_, payload) = decode_frame(&wrapped).unwrap();
        let unwrapped = crate::ber::BerCodec::encode_ber(&authentication).unwrap();
        let unwrapped = unwrapped[2..unwrapped.len() - 2].to_vec();
        let mut auth_header = EventHeader::new(EventType::Authentication);
        auth_header.set_control_encoding(EncodingType::Ber);
        let decoded_auth_wrapped: AuthenticationMessage =
            decode_control_event(&auth_header, payload).unwrap();
        assert_eq!(decoded_auth_wrapped, authentication);
        let decoded_auth_unwrapped: AuthenticationMessage =
            decode_control_event(&auth_header, &unwrapped).unwrap();
        assert_eq!(decoded_auth_unwrapped, authentication);
        let choice_wrapped_auth = {
            let choice = crate::ber::BerCodec::encode_ber(&authentication).unwrap();
            let mut bytes = vec![0x30, 0x80];
            bytes.extend_from_slice(&choice);
            bytes.extend_from_slice(&[0, 0]);
            bytes
        };
        let decoded_auth_choice_wrapped: AuthenticationMessage =
            decode_control_event(&auth_header, &choice_wrapped_auth).unwrap();
        assert_eq!(decoded_auth_choice_wrapped, authentication);
    }

    #[test]
    fn negotiation_and_authentication_decode_standard_universal_ber_sequences() {
        let status = vec![
            0x30, 0x80, 0x0a, 0x01, 0x00, 0x02, 0x01, 0x00, 0x04, 0x00, 0x00, 0x00,
        ];
        let negotiation_payload = vec![
            0x30, 0x80, 0x30, 0x80, 0x0a, 0x01, 0x00, 0x02, 0x01, 0x00, 0x04, 0x00, 0x00, 0x00,
            0x02, 0x01, 0x01, 0x02, 0x01, 0x03, 0x01, 0x01, 0x00, 0x30, 0x80, 0x02, 0x01, 0x01,
            0x0a, 0x01, 0x02, 0x00, 0x00, 0x02, 0x02, 0x0b, 0xb8, 0x02, 0x01, 0x0a, 0x00, 0x00,
        ];
        let auth_payload = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&[0x30, 0x80]);
            bytes.extend_from_slice(&status);
            bytes.extend_from_slice(&[0x02, 0x03, 0x00, 0xea, 0x60, 0x00, 0x00]);
            bytes
        };

        let mut header = EventHeader::new(EventType::Control);
        header.set_control_encoding(EncodingType::Ber);
        let negotiation: NegotiationMessage =
            decode_control_event(&header, &negotiation_payload).unwrap();
        assert_eq!(
            negotiation,
            NegotiationMessage {
                payload: NegotiationPayload::BrokerResponse(crate::schema::BrokerResponse {
                    result: Status::success(),
                    protocol_version: 1,
                    broker_version: 3,
                    is_deprecated_sdk: false,
                    broker_identity: ClientIdentity {
                        protocol_version: 1,
                        sdk_version: 999_999,
                        client_type: ClientType::TcpBroker,
                        process_name: String::new(),
                        pid: 0,
                        session_id: 1,
                        host_name: String::new(),
                        features: String::new(),
                        cluster_name: String::new(),
                        cluster_node_id: -1,
                        sdk_language: ClientLanguage::Cpp,
                        guid_info: GuidInfo::default(),
                        user_agent: String::new(),
                    },
                    heartbeat_interval_ms: 3000,
                    max_missed_heartbeats: 10,
                }),
            }
        );

        let mut auth_header = EventHeader::new(EventType::Authentication);
        auth_header.set_control_encoding(EncodingType::Ber);
        let authentication: AuthenticationMessage =
            decode_control_event(&auth_header, &auth_payload).unwrap();
        assert_eq!(
            authentication,
            AuthenticationMessage {
                payload: AuthenticationPayload::AuthenticationResponse(
                    crate::schema::AuthenticationResponse {
                        status: Status::success(),
                        lifetime_ms: Some(60_000),
                    },
                ),
            }
        );
    }

    #[test]
    fn negotiation_and_authentication_decode_nested_choice_wrappers_with_padding() {
        let negotiation = NegotiationMessage {
            payload: NegotiationPayload::BrokerResponse(crate::schema::BrokerResponse {
                result: Status::success(),
                protocol_version: 1,
                broker_version: 3,
                is_deprecated_sdk: false,
                broker_identity: ClientIdentity {
                    protocol_version: 1,
                    sdk_version: 999_999,
                    client_type: ClientType::TcpBroker,
                    process_name: "bmqbrkr".into(),
                    pid: 1,
                    session_id: 7,
                    host_name: "earth".into(),
                    features: "PROTOCOL_ENCODING:BER,JSON".into(),
                    cluster_name: String::new(),
                    cluster_node_id: -1,
                    sdk_language: ClientLanguage::Cpp,
                    guid_info: GuidInfo::default(),
                    user_agent: "/usr/local/bin/bmqbrkr".into(),
                },
                heartbeat_interval_ms: 3000,
                max_missed_heartbeats: 10,
            }),
        };
        let broker_response_choice = crate::ber::BerCodec::encode_ber(&negotiation).unwrap();
        let nested_negotiation = {
            let mut bytes = vec![0x30, 0x80, 0xa0, 0x80];
            bytes.extend_from_slice(&broker_response_choice);
            bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x02, 0x02]);
            bytes
        };
        let mut header = EventHeader::new(EventType::Control);
        header.set_control_encoding(EncodingType::Ber);
        let decoded: NegotiationMessage =
            decode_control_event(&header, &nested_negotiation).unwrap();
        assert_eq!(decoded, negotiation);

        let authentication = AuthenticationMessage {
            payload: AuthenticationPayload::AuthenticationResponse(
                crate::schema::AuthenticationResponse {
                    status: Status::success(),
                    lifetime_ms: Some(60_000),
                },
            ),
        };
        let auth_choice = crate::ber::BerCodec::encode_ber(&authentication).unwrap();
        let nested_auth = {
            let mut bytes = vec![0x30, 0x80, 0xa0, 0x80];
            bytes.extend_from_slice(&auth_choice);
            bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x02, 0x02]);
            bytes
        };
        let mut auth_header = EventHeader::new(EventType::Authentication);
        auth_header.set_control_encoding(EncodingType::Ber);
        let decoded_auth: AuthenticationMessage =
            decode_control_event(&auth_header, &nested_auth).unwrap();
        assert_eq!(decoded_auth, authentication);
    }

    #[test]
    fn message_properties_round_trip_new_style() {
        let mut properties = MessageProperties::new();
        properties.insert("encoding", MessagePropertyValue::Int32(3));
        properties.insert("timestamp", MessagePropertyValue::Int64(1_234_567_890));
        properties.insert("id", MessagePropertyValue::String("myCoolId".into()));

        let encoded = encode_message_properties(&properties).unwrap();
        let (decoded, consumed) = decode_message_properties(&encoded, false).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, properties);
    }

    #[test]
    fn put_event_encodes_expected_header() {
        let frame = encode_put_event(&[OutboundPutFrame {
            queue_id: 9,
            payload: Bytes::from_static(b"hello"),
            properties: MessageProperties::default(),
            correlation_id: Some(0x00a1b2),
            message_guid: Some(MessageGuid([9; 16])),
            compression: CompressionAlgorithm::None,
        }])
        .unwrap();

        let (header, payload) = decode_frame(&frame).unwrap();
        assert_eq!(header.event_type, EventType::Put);
        let put_header = PutHeader::decode(payload).unwrap();
        assert_eq!(put_header.queue_id, 9);
        assert_eq!(put_header.flags & PutHeaderFlags::ACK_REQUESTED, 1);
        assert_eq!(put_header.message_words, 11);
        assert_eq!(put_header.guid_or_correlation, MessageGuid([9; 16]));
    }

    #[test]
    fn message_guid_generator_produces_unique_v1_guids() {
        let generator = MessageGuidGenerator::new("blazox-test", 7, 1234, 42);
        let first = generator.next();
        let second = generator.next();

        assert_ne!(first, second);
        assert_eq!(first.as_bytes()[0] >> 6, 1);
        assert_eq!(second.as_bytes()[0] >> 6, 1);
    }

    #[test]
    fn decode_application_data_handles_new_style_properties_and_compression() {
        let mut properties = MessageProperties::new();
        properties.insert("kind", MessagePropertyValue::String("greeting".into()));
        properties.insert("count", MessagePropertyValue::Int32(1));

        let properties_bytes = encode_message_properties(&properties).unwrap();
        let payload = Bytes::from_static(b"hello compressed world");
        let compressed = compress_payload(&payload, CompressionAlgorithm::Zlib).unwrap();

        let mut encoded = BytesMut::new();
        encoded.extend_from_slice(&properties_bytes);
        encoded.extend_from_slice(&compressed);
        encoded.extend_from_slice(appdata_padding_bytes(appdata_padding(encoded.len())));

        let decoded =
            decode_application_data(&encoded, true, false, CompressionAlgorithm::Zlib).unwrap();

        assert_eq!(decoded.payload, payload);
        assert_eq!(decoded.properties, properties);
    }

    #[test]
    fn confirm_event_encodes_messages() {
        let frame = encode_confirm_event(&[ConfirmMessage {
            queue_id: 7,
            message_guid: MessageGuid([1; 16]),
            sub_queue_id: 2,
        }])
        .unwrap();
        let (header, payload) = decode_frame(&frame).unwrap();
        assert_eq!(header.event_type, EventType::Confirm);
        let confirm_header = ConfirmHeader::decode(payload).unwrap();
        assert_eq!(confirm_header.header_words, 1);
        assert_eq!(confirm_header.per_message_words, 6);
    }

    #[test]
    fn decode_push_event_parses_options() {
        let group_id_payload = {
            let mut payload = b"blu".to_vec();
            payload.extend_from_slice(appdata_padding_bytes(appdata_padding(payload.len())));
            payload
        };
        let options = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&encode_test_option(
                OptionType::SubQueueInfos,
                false,
                2,
                3,
                &[0, 0, 0, 7, 0b0100_0011, 0, 0, 0],
            ));
            bytes.extend_from_slice(&encode_test_option(
                OptionType::MsgGroupId,
                false,
                0,
                2,
                &group_id_payload,
            ));
            bytes
        };

        let mut properties = MessageProperties::new();
        properties.insert("kind", MessagePropertyValue::String("hello".into()));
        let properties_bytes = encode_message_properties(&properties).unwrap();
        let mut appdata = BytesMut::new();
        appdata.extend_from_slice(&properties_bytes);
        appdata.extend_from_slice(b"payload");
        appdata.extend_from_slice(appdata_padding_bytes(appdata_padding(appdata.len())));

        let message =
            encode_test_push_message(&options, &appdata, PushHeaderFlags::MESSAGE_PROPERTIES);
        let decoded = decode_push_event(&message).unwrap();
        assert_eq!(decoded.len(), 1);
        let push = &decoded[0];
        assert_eq!(push.header.queue_id, 17);
        assert_eq!(push.payload, Bytes::from_static(b"payload"));
        assert_eq!(
            push.sub_queue_infos,
            vec![SubQueueInfo {
                sub_queue_id: 7,
                rda_info: RdaInfo {
                    counter: 3,
                    is_unlimited: false,
                    is_poisonous: true,
                },
            }]
        );
        assert_eq!(push.message_group_id.as_deref(), Some("blu"));
        assert_eq!(
            push.properties.get("kind"),
            Some(&MessagePropertyValue::String("hello".into()))
        );
    }

    #[test]
    fn decode_push_event_parses_packed_default_subqueue_info() {
        let options = encode_test_option(OptionType::SubQueueInfos, true, 0, 0b1000_0101, &[]);
        let mut appdata = BytesMut::new();
        appdata.extend_from_slice(b"hello");
        appdata.extend_from_slice(appdata_padding_bytes(appdata_padding(appdata.len())));

        let message = encode_test_push_message(&options, &appdata, 0);
        let decoded = decode_push_event(&message).unwrap();
        assert_eq!(decoded[0].sub_queue_infos.len(), 1);
        assert_eq!(decoded[0].sub_queue_infos[0].sub_queue_id, 0);
        assert_eq!(
            decoded[0].sub_queue_infos[0].rda_info,
            RdaInfo {
                counter: 5,
                is_unlimited: true,
                is_poisonous: false,
            }
        );
    }
}
