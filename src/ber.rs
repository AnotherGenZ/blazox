use crate::error::{Error, Result};
use crate::schema::{
    AdminCommand, AdminCommandResponse, AuthenticationMessage, AuthenticationPayload,
    AuthenticationRequest, AuthenticationResponse, BrokerResponse, ClientIdentity, ClientLanguage,
    ClientType, CloseQueue, ClusterMessage, ConfigureQueueStream, ConfigureQueueStreamResponse,
    ConfigureStream, ConfigureStreamResponse, ConsumerInfo, ControlMessage, ControlPayload, Empty,
    Expression, ExpressionVersion, GuidInfo, NegotiationMessage, NegotiationPayload, OpenQueue,
    OpenQueueResponse, QueueHandleParameters, QueueStreamParameters, RoutingConfiguration, Status,
    StatusCategory, StreamParameters, SubQueueIdInfo, Subscription,
};

pub trait BerCodec: Sized {
    fn encode_ber(&self) -> Result<Vec<u8>>;
    fn decode_ber(payload: &[u8]) -> Result<Self>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BerClass {
    Universal = 0,
    ContextSpecific = 2,
}

#[derive(Debug, Clone, Copy)]
struct BerElement<'a> {
    class: BerClass,
    constructed: bool,
    tag: u32,
    content: &'a [u8],
}

const UNIVERSAL_SEQUENCE_TAG: u32 = 16;
const UNIVERSAL_BOOLEAN_TAG: u32 = 1;
const UNIVERSAL_INTEGER_TAG: u32 = 2;
const UNIVERSAL_OCTET_STRING_TAG: u32 = 4;
const UNIVERSAL_ENUMERATED_TAG: u32 = 10;
const UNIVERSAL_UTF8_STRING_TAG: u32 = 12;
const UNIVERSAL_PRINTABLE_STRING_TAG: u32 = 19;
const UNIVERSAL_IA5_STRING_TAG: u32 = 22;
const UNIVERSAL_VISIBLE_STRING_TAG: u32 = 26;
const UNIVERSAL_GENERAL_STRING_TAG: u32 = 27;
const UNIVERSAL_BMP_STRING_TAG: u32 = 30;
const BER_EOC: [u8; 2] = [0, 0];

fn encode_tag(class: BerClass, constructed: bool, tag: u32, out: &mut Vec<u8>) -> Result<()> {
    if tag < 31 {
        out.push(((class as u8) << 6) | (u8::from(constructed) << 5) | tag as u8);
        return Ok(());
    }
    out.push(((class as u8) << 6) | (u8::from(constructed) << 5) | 0x1f);
    let mut stack = [0_u8; 5];
    let mut width = 0usize;
    let mut remaining = tag;
    loop {
        stack[width] = (remaining & 0x7f) as u8;
        width += 1;
        remaining >>= 7;
        if remaining == 0 {
            break;
        }
    }
    for index in (0..width).rev() {
        let mut byte = stack[index];
        if index != 0 {
            byte |= 0x80;
        }
        out.push(byte);
    }
    Ok(())
}

fn encode_length(len: usize, out: &mut Vec<u8>) -> Result<()> {
    if len < 0x80 {
        out.push(len as u8);
        return Ok(());
    }

    let bytes = len.to_be_bytes();
    let start = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len() - 1);
    let width = bytes.len() - start;
    if width > 8 {
        return Err(Error::Protocol("BER length is too large"));
    }
    out.push(0x80 | width as u8);
    out.extend_from_slice(&bytes[start..]);
    Ok(())
}

fn encode_primitive(class: BerClass, tag: u32, content: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(2 + content.len());
    encode_tag(class, false, tag, &mut out)?;
    encode_length(content.len(), &mut out)?;
    out.extend_from_slice(content);
    Ok(out)
}

fn encode_constructed(class: BerClass, tag: u32, content: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(4 + content.len());
    encode_tag(class, true, tag, &mut out)?;
    out.push(0x80);
    out.extend_from_slice(content);
    out.extend_from_slice(&BER_EOC);
    Ok(out)
}

fn wrap_sequence(content: &[u8]) -> Result<Vec<u8>> {
    encode_constructed(BerClass::Universal, UNIVERSAL_SEQUENCE_TAG, content)
}

fn parse_one(bytes: &[u8]) -> Result<(BerElement<'_>, usize)> {
    if bytes.len() < 2 {
        return Err(Error::Protocol("BER element is truncated"));
    }
    let ident = bytes[0];

    let class = match ident >> 6 {
        0 => BerClass::Universal,
        2 => BerClass::ContextSpecific,
        _ => return Err(Error::Protocol("unsupported BER tag class")),
    };
    let constructed = ident & 0x20 != 0;
    let mut header_len = 1usize;
    let tag = if ident & 0x1f == 0x1f {
        let mut tag = 0_u32;
        loop {
            if header_len >= bytes.len() {
                return Err(Error::Protocol("BER high-tag element is truncated"));
            }
            let byte = bytes[header_len];
            header_len += 1;
            tag = (tag << 7) | u32::from(byte & 0x7f);
            if byte & 0x80 == 0 {
                break tag;
            }
        }
    } else {
        u32::from(ident & 0x1f)
    };

    if header_len >= bytes.len() {
        return Err(Error::Protocol("BER element is truncated"));
    }
    let first_len = bytes[header_len];
    header_len += 1;
    let content;
    let total_len;

    if first_len == 0x80 {
        if !constructed {
            return Err(Error::Protocol(
                "primitive BER elements may not use indefinite lengths",
            ));
        }
        let mut cursor = header_len;
        loop {
            if cursor + 2 <= bytes.len() && bytes[cursor..cursor + 2] == BER_EOC {
                content = &bytes[header_len..cursor];
                total_len = cursor + 2;
                break;
            }
            let (_, consumed) = parse_one(&bytes[cursor..])?;
            cursor += consumed;
            if cursor > bytes.len() {
                return Err(Error::Protocol("BER indefinite element overruns payload"));
            }
        }
    } else if first_len & 0x80 == 0 {
        let len = usize::from(first_len);
        if bytes.len() < header_len + len {
            return Err(Error::Protocol("BER element is truncated"));
        }
        content = &bytes[header_len..header_len + len];
        total_len = header_len + len;
    } else {
        let width = usize::from(first_len & 0x7f);
        if width == 0 || width > 8 || bytes.len() < header_len + width {
            return Err(Error::Protocol("BER length is invalid"));
        }
        let mut len = 0usize;
        for byte in &bytes[header_len..header_len + width] {
            len = (len << 8) | usize::from(*byte);
        }
        header_len += width;
        if bytes.len() < header_len + len {
            return Err(Error::Protocol("BER element is truncated"));
        }
        content = &bytes[header_len..header_len + len];
        total_len = header_len + len;
    }

    Ok((
        BerElement {
            class,
            constructed,
            tag,
            content,
        },
        total_len,
    ))
}

fn parse_root(payload: &[u8]) -> Result<BerElement<'_>> {
    let (element, consumed) = parse_one(payload)?;
    let trailing = &payload[consumed..];
    let is_zero_padding = trailing.iter().all(|byte| *byte == 0);
    let is_protocol_padding = !trailing.is_empty()
        && trailing.len() <= 8
        && trailing.iter().all(|byte| *byte == trailing.len() as u8);
    if !trailing.is_empty() && !is_zero_padding && !is_protocol_padding {
        return Err(Error::Protocol(
            "BER payload contains non-padding trailing bytes",
        ));
    }
    Ok(element)
}

fn parse_children(content: &[u8]) -> Result<Vec<BerElement<'_>>> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while cursor < content.len() {
        let (element, consumed) = parse_one(&content[cursor..])?;
        out.push(element);
        cursor += consumed;
    }
    Ok(out)
}

fn sequence_children(content: &[u8]) -> Result<Vec<BerElement<'_>>> {
    let children = parse_children(content)?;
    if children.len() == 1 && is_universal_sequence(children[0]) {
        parse_children(children[0].content)
    } else {
        Ok(children)
    }
}

fn format_hex(bytes: &[u8], max_len: usize) -> String {
    let shown = bytes.len().min(max_len);
    let mut out = String::new();
    for (index, byte) in bytes[..shown].iter().enumerate() {
        if index > 0 {
            out.push(' ');
        }
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    if bytes.len() > max_len {
        out.push_str(" ...");
    }
    out
}

fn describe_root(payload: &[u8]) -> String {
    match parse_root(payload) {
        Ok(root) => {
            let children = parse_children(root.content)
                .map(|children| {
                    children
                        .into_iter()
                        .map(|child| {
                            format!(
                                "{:?}/constructed={}/tag={}/len={}",
                                child.class,
                                child.constructed,
                                child.tag,
                                child.content.len()
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|error| format!("children-error={error}"));
            format!(
                "root={:?}/constructed={}/tag={}/len={} children=[{}]",
                root.class,
                root.constructed,
                root.tag,
                root.content.len(),
                children
            )
        }
        Err(error) => format!("root-error={error}"),
    }
}

fn describe_segments(payload: &[u8]) -> String {
    let mut cursor = 0;
    let mut parts = Vec::new();
    while cursor < payload.len() {
        match parse_one(&payload[cursor..]) {
            Ok((element, consumed)) => {
                parts.push(format!(
                    "@{cursor}:{:?}/constructed={}/tag={}/len={}/consumed={}",
                    element.class,
                    element.constructed,
                    element.tag,
                    element.content.len(),
                    consumed
                ));
                cursor += consumed;
            }
            Err(error) => {
                parts.push(format!("@{cursor}:error={error}"));
                break;
            }
        }
    }
    parts.join(", ")
}

fn expect_sequence_root<'a>(payload: &'a [u8]) -> Result<&'a [u8]> {
    let root = parse_root(payload)?;
    if root.class != BerClass::Universal || !root.constructed || root.tag != UNIVERSAL_SEQUENCE_TAG
    {
        return Err(Error::Protocol("expected BER SEQUENCE root"));
    }
    Ok(root.content)
}

fn expect_context_constructed(element: BerElement<'_>, tag: u32) -> Result<&[u8]> {
    if element.class != BerClass::ContextSpecific || !element.constructed || element.tag != tag {
        return Err(Error::Protocol("unexpected BER constructed field"));
    }
    Ok(element.content)
}

fn expect_context_primitive(element: BerElement<'_>, tag: u32) -> Result<&[u8]> {
    if element.class != BerClass::ContextSpecific || element.constructed || element.tag != tag {
        return Err(Error::Protocol("unexpected BER primitive field"));
    }
    Ok(element.content)
}

fn is_universal_boolean(element: BerElement<'_>) -> bool {
    element.class == BerClass::Universal
        && !element.constructed
        && element.tag == UNIVERSAL_BOOLEAN_TAG
}

fn is_universal_integer(element: BerElement<'_>) -> bool {
    element.class == BerClass::Universal
        && !element.constructed
        && element.tag == UNIVERSAL_INTEGER_TAG
}

fn is_universal_enumerated(element: BerElement<'_>) -> bool {
    element.class == BerClass::Universal
        && !element.constructed
        && element.tag == UNIVERSAL_ENUMERATED_TAG
}

fn is_universal_string(element: BerElement<'_>) -> bool {
    element.class == BerClass::Universal
        && !element.constructed
        && matches!(
            element.tag,
            UNIVERSAL_OCTET_STRING_TAG
                | UNIVERSAL_UTF8_STRING_TAG
                | UNIVERSAL_PRINTABLE_STRING_TAG
                | UNIVERSAL_IA5_STRING_TAG
                | UNIVERSAL_VISIBLE_STRING_TAG
                | UNIVERSAL_GENERAL_STRING_TAG
                | UNIVERSAL_BMP_STRING_TAG
        )
}

fn is_universal_octet_string(element: BerElement<'_>) -> bool {
    element.class == BerClass::Universal
        && !element.constructed
        && element.tag == UNIVERSAL_OCTET_STRING_TAG
}

fn is_universal_sequence(element: BerElement<'_>) -> bool {
    element.class == BerClass::Universal
        && element.constructed
        && element.tag == UNIVERSAL_SEQUENCE_TAG
}

struct OrderedFields<'a> {
    children: &'a [BerElement<'a>],
    cursor: usize,
}

impl<'a> OrderedFields<'a> {
    fn new(children: &'a [BerElement<'a>]) -> Self {
        Self {
            children,
            cursor: 0,
        }
    }

    fn peek(&self) -> Option<BerElement<'a>> {
        self.children.get(self.cursor).copied()
    }

    fn take_if(
        &mut self,
        predicate: impl FnOnce(BerElement<'a>) -> bool,
    ) -> Option<BerElement<'a>> {
        let element = self.peek()?;
        if predicate(element) {
            self.cursor += 1;
            Some(element)
        } else {
            None
        }
    }

    fn require_if(
        &mut self,
        predicate: impl FnOnce(BerElement<'a>) -> bool,
        what: &'static str,
    ) -> Result<BerElement<'a>> {
        self.take_if(predicate)
            .ok_or_else(|| Error::ProtocolMessage(format!("missing or invalid BER field: {what}")))
    }

    fn optional_enum(&mut self) -> Option<&'a [u8]> {
        self.take_if(is_universal_enumerated)
            .map(|element| element.content)
    }

    fn required_enum(&mut self, what: &'static str) -> Result<&'a [u8]> {
        Ok(self.require_if(is_universal_enumerated, what)?.content)
    }

    fn optional_integer(&mut self) -> Option<&'a [u8]> {
        self.take_if(is_universal_integer)
            .map(|element| element.content)
    }

    fn required_integer(&mut self, what: &'static str) -> Result<&'a [u8]> {
        Ok(self.require_if(is_universal_integer, what)?.content)
    }

    fn optional_boolean(&mut self) -> Option<&'a [u8]> {
        self.take_if(is_universal_boolean)
            .map(|element| element.content)
    }

    fn optional_string(&mut self) -> Option<&'a [u8]> {
        self.take_if(is_universal_string)
            .map(|element| element.content)
    }

    fn required_string(&mut self, what: &'static str) -> Result<&'a [u8]> {
        Ok(self.require_if(is_universal_string, what)?.content)
    }

    fn optional_octet_string(&mut self) -> Option<&'a [u8]> {
        self.take_if(is_universal_octet_string)
            .map(|element| element.content)
    }

    fn optional_sequence(&mut self) -> Option<&'a [u8]> {
        self.take_if(is_universal_sequence)
            .map(|element| element.content)
    }

    fn required_sequence(&mut self, what: &'static str) -> Result<&'a [u8]> {
        Ok(self.require_if(is_universal_sequence, what)?.content)
    }

    fn finish(self, type_name: &'static str) -> Result<()> {
        if self.cursor == self.children.len() {
            Ok(())
        } else {
            Err(Error::ProtocolMessage(format!(
                "unexpected trailing BER fields in {type_name}"
            )))
        }
    }
}

fn decode_i64(bytes: &[u8]) -> Result<i64> {
    if bytes.is_empty() || bytes.len() > 8 {
        return Err(Error::Protocol("BER integer width is invalid"));
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut buf = if negative { [0xff; 8] } else { [0; 8] };
    buf[8 - bytes.len()..].copy_from_slice(bytes);
    Ok(i64::from_be_bytes(buf))
}

fn decode_i32(bytes: &[u8]) -> Result<i32> {
    decode_i64(bytes)?
        .try_into()
        .map_err(|_| Error::Protocol("BER integer does not fit in i32"))
}

fn decode_u64(bytes: &[u8]) -> Result<u64> {
    if bytes.is_empty() || bytes.len() > 9 {
        return Err(Error::Protocol("BER integer width is invalid"));
    }
    if bytes[0] & 0x80 != 0 {
        return Err(Error::Protocol("BER unsigned integer is negative"));
    }
    let bytes = if bytes.len() == 9 {
        if bytes[0] != 0 {
            return Err(Error::Protocol("BER unsigned integer is too wide"));
        }
        &bytes[1..]
    } else {
        bytes
    };
    let mut buf = [0; 8];
    buf[8 - bytes.len()..].copy_from_slice(bytes);
    Ok(u64::from_be_bytes(buf))
}

fn decode_u32(bytes: &[u8]) -> Result<u32> {
    decode_u64(bytes)?
        .try_into()
        .map_err(|_| Error::Protocol("BER integer does not fit in u32"))
}

fn encode_i64(value: i64) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut start = 0;
    while start < bytes.len() - 1 {
        let current = bytes[start];
        let next = bytes[start + 1];
        if (current == 0x00 && next & 0x80 == 0) || (current == 0xff && next & 0x80 != 0) {
            start += 1;
        } else {
            break;
        }
    }
    bytes[start..].to_vec()
}

fn encode_u64(value: u64) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let start = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len() - 1);
    let mut out = bytes[start..].to_vec();
    if out[0] & 0x80 != 0 {
        out.insert(0, 0);
    }
    out
}

fn decode_bool(bytes: &[u8]) -> Result<bool> {
    if bytes.len() != 1 {
        return Err(Error::Protocol("BER boolean must be exactly one byte"));
    }
    Ok(bytes[0] != 0)
}

fn encode_bool(value: bool) -> Vec<u8> {
    vec![if value { 0xff } else { 0x00 }]
}

fn decode_string(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|error| Error::ProtocolMessage(error.to_string()))
}

fn universal_i32(value: i32) -> Result<Vec<u8>> {
    encode_primitive(
        BerClass::Universal,
        UNIVERSAL_INTEGER_TAG,
        &encode_i64(i64::from(value)),
    )
}

fn context_i32(tag: u32, value: i32) -> Result<Vec<u8>> {
    encode_primitive(
        BerClass::ContextSpecific,
        tag,
        &encode_i64(i64::from(value)),
    )
}

fn universal_i64(value: i64) -> Result<Vec<u8>> {
    encode_primitive(
        BerClass::Universal,
        UNIVERSAL_INTEGER_TAG,
        &encode_i64(value),
    )
}

fn universal_u32(value: u32) -> Result<Vec<u8>> {
    encode_primitive(
        BerClass::Universal,
        UNIVERSAL_INTEGER_TAG,
        &encode_u64(u64::from(value)),
    )
}

fn universal_u64(value: u64) -> Result<Vec<u8>> {
    encode_primitive(
        BerClass::Universal,
        UNIVERSAL_INTEGER_TAG,
        &encode_u64(value),
    )
}

fn universal_bool(value: bool) -> Result<Vec<u8>> {
    encode_primitive(
        BerClass::Universal,
        UNIVERSAL_BOOLEAN_TAG,
        &encode_bool(value),
    )
}

fn universal_string(value: &str) -> Result<Vec<u8>> {
    encode_primitive(
        BerClass::Universal,
        UNIVERSAL_OCTET_STRING_TAG,
        value.as_bytes(),
    )
}

fn universal_bytes(value: &[u8]) -> Result<Vec<u8>> {
    encode_primitive(BerClass::Universal, UNIVERSAL_OCTET_STRING_TAG, value)
}

fn universal_enum(value: i64) -> Result<Vec<u8>> {
    encode_primitive(
        BerClass::Universal,
        UNIVERSAL_ENUMERATED_TAG,
        &encode_i64(value),
    )
}

fn universal_sequence(content: &[u8]) -> Result<Vec<u8>> {
    wrap_sequence(content)
}

fn decode_status_category(bytes: &[u8]) -> Result<StatusCategory> {
    Ok(match decode_i32(bytes)? {
        0 => StatusCategory::Success,
        -1 => StatusCategory::Unknown,
        -2 => StatusCategory::Timeout,
        -3 => StatusCategory::NotConnected,
        -4 => StatusCategory::Canceled,
        -5 => StatusCategory::NotSupported,
        -6 => StatusCategory::Refused,
        -7 => StatusCategory::InvalidArgument,
        -8 => StatusCategory::NotReady,
        _ => return Err(Error::Protocol("unknown StatusCategory enum value")),
    })
}

fn encode_status_category(value: StatusCategory) -> i64 {
    match value {
        StatusCategory::Success => 0,
        StatusCategory::Unknown => -1,
        StatusCategory::Timeout => -2,
        StatusCategory::NotConnected => -3,
        StatusCategory::Canceled => -4,
        StatusCategory::NotSupported => -5,
        StatusCategory::Refused => -6,
        StatusCategory::InvalidArgument => -7,
        StatusCategory::NotReady => -8,
    }
}

fn decode_expression_version(bytes: &[u8]) -> Result<ExpressionVersion> {
    Ok(match decode_i32(bytes)? {
        0 => ExpressionVersion::Undefined,
        1 => ExpressionVersion::Version1,
        _ => return Err(Error::Protocol("unknown ExpressionVersion enum value")),
    })
}

fn encode_expression_version(value: ExpressionVersion) -> i64 {
    match value {
        ExpressionVersion::Undefined => 0,
        ExpressionVersion::Version1 => 1,
    }
}

fn decode_client_type(bytes: &[u8]) -> Result<ClientType> {
    Ok(match decode_i32(bytes)? {
        0 => ClientType::Unknown,
        1 => ClientType::TcpClient,
        2 => ClientType::TcpBroker,
        3 => ClientType::TcpAdmin,
        _ => return Err(Error::Protocol("unknown ClientType enum value")),
    })
}

fn encode_client_type(value: ClientType) -> i64 {
    match value {
        ClientType::Unknown => 0,
        ClientType::TcpClient => 1,
        ClientType::TcpBroker => 2,
        ClientType::TcpAdmin => 3,
    }
}

fn decode_client_language(bytes: &[u8]) -> Result<ClientLanguage> {
    Ok(match decode_i32(bytes)? {
        0 => ClientLanguage::Unknown,
        1 => ClientLanguage::Cpp,
        2 => ClientLanguage::Java,
        _ => return Err(Error::Protocol("unknown ClientLanguage enum value")),
    })
}

fn encode_client_language(value: ClientLanguage) -> i64 {
    match value {
        ClientLanguage::Unknown => 0,
        ClientLanguage::Cpp => 1,
        ClientLanguage::Java => 2,
    }
}

fn decode_status(content: &[u8]) -> Result<Status> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let category = decode_status_category(fields.required_enum("Status.category")?)?;
        let code = decode_i32(fields.required_integer("Status.code")?)?;
        let message = fields
            .optional_string()
            .map(decode_string)
            .transpose()?
            .unwrap_or_default();
        fields.finish("Status")?;
        return Ok(Status {
            category,
            code,
            message,
        });
    }

    let mut category = None;
    let mut code = None;
    let mut message = String::new();

    for element in children {
        match element.tag {
            0 => {
                category = Some(decode_status_category(expect_context_primitive(
                    element, 0,
                )?)?)
            }
            1 => code = Some(decode_i32(expect_context_primitive(element, 1)?)?),
            2 => message = decode_string(expect_context_primitive(element, 2)?)?,
            _ => return Err(Error::Protocol("unexpected field in Status")),
        }
    }

    Ok(Status {
        category: category.ok_or(Error::Protocol("Status.category is missing"))?,
        code: code.ok_or(Error::Protocol("Status.code is missing"))?,
        message,
    })
}

fn encode_status(value: &Status) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_enum(encode_status_category(value.category))?);
    out.extend(universal_i32(value.code)?);
    out.extend(universal_string(&value.message)?);
    Ok(out)
}

fn decode_empty(content: &[u8]) -> Result<Empty> {
    let children = sequence_children(content)?;
    if children.is_empty() {
        return Ok(Empty::default());
    }
    if children.len() == 1
        && is_universal_sequence(children[0])
        && parse_children(children[0].content)?.is_empty()
    {
        return Ok(Empty::default());
    }
    if !children.is_empty() {
        return Err(Error::Protocol("empty BER type contains fields"));
    }
    Ok(Empty::default())
}

fn decode_admin_command(content: &[u8]) -> Result<AdminCommand> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let command = decode_string(fields.required_string("AdminCommand.command")?)?;
        fields.finish("AdminCommand")?;
        return Ok(AdminCommand { command });
    }

    let mut command = None;
    for element in children {
        match element.tag {
            0 => command = Some(decode_string(expect_context_primitive(element, 0)?)?),
            _ => return Err(Error::Protocol("unexpected field in AdminCommand")),
        }
    }
    Ok(AdminCommand {
        command: command.ok_or(Error::Protocol("AdminCommand.command is missing"))?,
    })
}

fn encode_admin_command(value: &AdminCommand) -> Result<Vec<u8>> {
    universal_string(&value.command)
}

fn decode_admin_command_response(content: &[u8]) -> Result<AdminCommandResponse> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let text = decode_string(fields.required_string("AdminCommandResponse.text")?)?;
        fields.finish("AdminCommandResponse")?;
        return Ok(AdminCommandResponse { text });
    }

    let mut text = None;
    for element in children {
        match element.tag {
            0 => text = Some(decode_string(expect_context_primitive(element, 0)?)?),
            _ => return Err(Error::Protocol("unexpected field in AdminCommandResponse")),
        }
    }
    Ok(AdminCommandResponse {
        text: text.ok_or(Error::Protocol("AdminCommandResponse.text is missing"))?,
    })
}

fn encode_admin_command_response(value: &AdminCommandResponse) -> Result<Vec<u8>> {
    universal_string(&value.text)
}

fn decode_sub_queue_id_info(content: &[u8]) -> Result<SubQueueIdInfo> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let sub_id = fields
            .optional_integer()
            .map(decode_u32)
            .transpose()?
            .unwrap_or(0);
        let app_id = fields
            .optional_string()
            .map(decode_string)
            .transpose()?
            .unwrap_or_else(|| "__default".to_string());
        fields.finish("SubQueueIdInfo")?;
        return Ok(SubQueueIdInfo { sub_id, app_id });
    }

    let mut sub_id = 0;
    let mut app_id = "__default".to_string();

    for element in children {
        match element.tag {
            0 => sub_id = decode_u32(expect_context_primitive(element, 0)?)?,
            1 => app_id = decode_string(expect_context_primitive(element, 1)?)?,
            _ => return Err(Error::Protocol("unexpected field in SubQueueIdInfo")),
        }
    }

    Ok(SubQueueIdInfo { sub_id, app_id })
}

fn encode_sub_queue_id_info(value: &SubQueueIdInfo) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_u32(value.sub_id)?);
    out.extend(universal_string(&value.app_id)?);
    Ok(out)
}

fn decode_queue_handle_parameters(content: &[u8]) -> Result<QueueHandleParameters> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let uri = decode_string(fields.required_string("QueueHandleParameters.uri")?)?;
        let q_id = decode_u32(fields.required_integer("QueueHandleParameters.qId")?)?;
        let sub_id_info = fields
            .optional_sequence()
            .map(decode_sub_queue_id_info)
            .transpose()?;
        let flags = decode_u64(fields.required_integer("QueueHandleParameters.flags")?)?;
        let read_count = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(0);
        let write_count = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(0);
        let admin_count = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(0);
        fields.finish("QueueHandleParameters")?;
        return Ok(QueueHandleParameters {
            uri,
            q_id,
            sub_id_info,
            flags,
            read_count,
            write_count,
            admin_count,
        });
    }

    let mut uri = None;
    let mut q_id = None;
    let mut sub_id_info = None;
    let mut flags = None;
    let mut read_count = 0;
    let mut write_count = 0;
    let mut admin_count = 0;

    for element in children {
        match element.tag {
            0 => uri = Some(decode_string(expect_context_primitive(element, 0)?)?),
            1 => q_id = Some(decode_u32(expect_context_primitive(element, 1)?)?),
            2 => {
                sub_id_info = Some(decode_sub_queue_id_info(expect_context_constructed(
                    element, 2,
                )?)?)
            }
            3 => flags = Some(decode_u64(expect_context_primitive(element, 3)?)?),
            4 => read_count = decode_i32(expect_context_primitive(element, 4)?)?,
            5 => write_count = decode_i32(expect_context_primitive(element, 5)?)?,
            6 => admin_count = decode_i32(expect_context_primitive(element, 6)?)?,
            _ => return Err(Error::Protocol("unexpected field in QueueHandleParameters")),
        }
    }

    Ok(QueueHandleParameters {
        uri: uri.ok_or(Error::Protocol("QueueHandleParameters.uri is missing"))?,
        q_id: q_id.ok_or(Error::Protocol("QueueHandleParameters.qId is missing"))?,
        sub_id_info,
        flags: flags.ok_or(Error::Protocol("QueueHandleParameters.flags is missing"))?,
        read_count,
        write_count,
        admin_count,
    })
}

fn encode_queue_handle_parameters(value: &QueueHandleParameters) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_string(&value.uri)?);
    out.extend(universal_u32(value.q_id)?);
    if let Some(sub_id_info) = &value.sub_id_info {
        out.extend(universal_sequence(&encode_sub_queue_id_info(sub_id_info)?)?);
    }
    out.extend(universal_u64(value.flags)?);
    out.extend(universal_i32(value.read_count)?);
    out.extend(universal_i32(value.write_count)?);
    out.extend(universal_i32(value.admin_count)?);
    Ok(out)
}

fn decode_queue_stream_parameters(content: &[u8]) -> Result<QueueStreamParameters> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let sub_id_info = fields
            .optional_sequence()
            .map(decode_sub_queue_id_info)
            .transpose()?;
        let max_unconfirmed_messages = fields
            .optional_integer()
            .map(decode_i64)
            .transpose()?
            .unwrap_or(0);
        let max_unconfirmed_bytes = fields
            .optional_integer()
            .map(decode_i64)
            .transpose()?
            .unwrap_or(0);
        let consumer_priority = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(i32::MIN);
        let consumer_priority_count = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(0);
        fields.finish("QueueStreamParameters")?;
        return Ok(QueueStreamParameters {
            sub_id_info,
            max_unconfirmed_messages,
            max_unconfirmed_bytes,
            consumer_priority,
            consumer_priority_count,
        });
    }

    let mut sub_id_info = None;
    let mut max_unconfirmed_messages = 0;
    let mut max_unconfirmed_bytes = 0;
    let mut consumer_priority = i32::MIN;
    let mut consumer_priority_count = 0;

    for element in children {
        match element.tag {
            0 => {
                sub_id_info = Some(decode_sub_queue_id_info(expect_context_constructed(
                    element, 0,
                )?)?)
            }
            1 => max_unconfirmed_messages = decode_i64(expect_context_primitive(element, 1)?)?,
            2 => max_unconfirmed_bytes = decode_i64(expect_context_primitive(element, 2)?)?,
            3 => consumer_priority = decode_i32(expect_context_primitive(element, 3)?)?,
            4 => consumer_priority_count = decode_i32(expect_context_primitive(element, 4)?)?,
            _ => return Err(Error::Protocol("unexpected field in QueueStreamParameters")),
        }
    }

    Ok(QueueStreamParameters {
        sub_id_info,
        max_unconfirmed_messages,
        max_unconfirmed_bytes,
        consumer_priority,
        consumer_priority_count,
    })
}

fn encode_queue_stream_parameters(value: &QueueStreamParameters) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    if let Some(sub_id_info) = &value.sub_id_info {
        out.extend(universal_sequence(&encode_sub_queue_id_info(sub_id_info)?)?);
    }
    out.extend(universal_i64(value.max_unconfirmed_messages)?);
    out.extend(universal_i64(value.max_unconfirmed_bytes)?);
    out.extend(universal_i32(value.consumer_priority)?);
    out.extend(universal_i32(value.consumer_priority_count)?);
    Ok(out)
}

fn decode_expression(content: &[u8]) -> Result<Expression> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let version = fields
            .optional_enum()
            .map(decode_expression_version)
            .transpose()?
            .unwrap_or(ExpressionVersion::Undefined);
        let text = decode_string(fields.required_string("Expression.text")?)?;
        fields.finish("Expression")?;
        return Ok(Expression { version, text });
    }

    let mut version = ExpressionVersion::Undefined;
    let mut text = None;

    for element in children {
        match element.tag {
            0 => version = decode_expression_version(expect_context_primitive(element, 0)?)?,
            1 => text = Some(decode_string(expect_context_primitive(element, 1)?)?),
            _ => return Err(Error::Protocol("unexpected field in Expression")),
        }
    }

    Ok(Expression {
        version,
        text: text.ok_or(Error::Protocol("Expression.text is missing"))?,
    })
}

fn encode_expression(value: &Expression) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_enum(encode_expression_version(value.version))?);
    out.extend(universal_string(&value.text)?);
    Ok(out)
}

fn decode_consumer_info(content: &[u8]) -> Result<ConsumerInfo> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let max_unconfirmed_messages = fields
            .optional_integer()
            .map(decode_i64)
            .transpose()?
            .unwrap_or(0);
        let max_unconfirmed_bytes = fields
            .optional_integer()
            .map(decode_i64)
            .transpose()?
            .unwrap_or(0);
        let consumer_priority = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(i32::MIN);
        let consumer_priority_count = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(0);
        fields.finish("ConsumerInfo")?;
        return Ok(ConsumerInfo {
            max_unconfirmed_messages,
            max_unconfirmed_bytes,
            consumer_priority,
            consumer_priority_count,
        });
    }

    let mut max_unconfirmed_messages = 0;
    let mut max_unconfirmed_bytes = 0;
    let mut consumer_priority = i32::MIN;
    let mut consumer_priority_count = 0;

    for element in children {
        match element.tag {
            0 => max_unconfirmed_messages = decode_i64(expect_context_primitive(element, 0)?)?,
            1 => max_unconfirmed_bytes = decode_i64(expect_context_primitive(element, 1)?)?,
            2 => consumer_priority = decode_i32(expect_context_primitive(element, 2)?)?,
            3 => consumer_priority_count = decode_i32(expect_context_primitive(element, 3)?)?,
            _ => return Err(Error::Protocol("unexpected field in ConsumerInfo")),
        }
    }

    Ok(ConsumerInfo {
        max_unconfirmed_messages,
        max_unconfirmed_bytes,
        consumer_priority,
        consumer_priority_count,
    })
}

fn encode_consumer_info(value: &ConsumerInfo) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_i64(value.max_unconfirmed_messages)?);
    out.extend(universal_i64(value.max_unconfirmed_bytes)?);
    out.extend(universal_i32(value.consumer_priority)?);
    out.extend(universal_i32(value.consumer_priority_count)?);
    Ok(out)
}

fn decode_subscription(content: &[u8]) -> Result<Subscription> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let s_id = decode_u32(fields.required_integer("Subscription.sId")?)?;
        let expression = decode_expression(fields.required_sequence("Subscription.expression")?)?;
        let mut consumers = Vec::new();
        while let Some(consumer) = fields.optional_sequence() {
            consumers.push(decode_consumer_info(consumer)?);
        }
        fields.finish("Subscription")?;
        return Ok(Subscription {
            s_id,
            expression,
            consumers,
        });
    }

    let mut s_id = None;
    let mut expression = None;
    let mut consumers = Vec::new();

    for element in children {
        match element.tag {
            0 => s_id = Some(decode_u32(expect_context_primitive(element, 0)?)?),
            1 => expression = Some(decode_expression(expect_context_constructed(element, 1)?)?),
            2 => consumers.push(decode_consumer_info(expect_context_constructed(
                element, 2,
            )?)?),
            _ => return Err(Error::Protocol("unexpected field in Subscription")),
        }
    }

    Ok(Subscription {
        s_id: s_id.ok_or(Error::Protocol("Subscription.sId is missing"))?,
        expression: expression.ok_or(Error::Protocol("Subscription.expression is missing"))?,
        consumers,
    })
}

fn encode_subscription(value: &Subscription) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_u32(value.s_id)?);
    out.extend(universal_sequence(&encode_expression(&value.expression)?)?);
    for consumer in &value.consumers {
        out.extend(universal_sequence(&encode_consumer_info(consumer)?)?);
    }
    Ok(out)
}

fn decode_stream_parameters(content: &[u8]) -> Result<StreamParameters> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let app_id = fields
            .optional_string()
            .map(decode_string)
            .transpose()?
            .unwrap_or_else(|| "__default".to_string());
        let mut subscriptions = Vec::new();
        while let Some(subscription) = fields.optional_sequence() {
            subscriptions.push(decode_subscription(subscription)?);
        }
        fields.finish("StreamParameters")?;
        return Ok(StreamParameters {
            app_id,
            subscriptions,
        });
    }

    let mut app_id = "__default".to_string();
    let mut subscriptions = Vec::new();

    for element in children {
        match element.tag {
            0 => app_id = decode_string(expect_context_primitive(element, 0)?)?,
            1 => subscriptions.push(decode_subscription(expect_context_constructed(
                element, 1,
            )?)?),
            _ => return Err(Error::Protocol("unexpected field in StreamParameters")),
        }
    }

    Ok(StreamParameters {
        app_id,
        subscriptions,
    })
}

fn encode_stream_parameters(value: &StreamParameters) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_string(&value.app_id)?);
    for subscription in &value.subscriptions {
        out.extend(universal_sequence(&encode_subscription(subscription)?)?);
    }
    Ok(out)
}

fn decode_routing_configuration(content: &[u8]) -> Result<RoutingConfiguration> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let flags = decode_u64(fields.required_integer("RoutingConfiguration.flags")?)?;
        fields.finish("RoutingConfiguration")?;
        return Ok(RoutingConfiguration { flags });
    }

    let mut flags = None;
    for element in children {
        match element.tag {
            0 => flags = Some(decode_u64(expect_context_primitive(element, 0)?)?),
            _ => return Err(Error::Protocol("unexpected field in RoutingConfiguration")),
        }
    }
    Ok(RoutingConfiguration {
        flags: flags.ok_or(Error::Protocol("RoutingConfiguration.flags is missing"))?,
    })
}

fn encode_routing_configuration(value: &RoutingConfiguration) -> Result<Vec<u8>> {
    universal_u64(value.flags)
}

fn decode_open_queue(content: &[u8]) -> Result<OpenQueue> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let handle_parameters = decode_queue_handle_parameters(
            fields.required_sequence("OpenQueue.handleParameters")?,
        )?;
        fields.finish("OpenQueue")?;
        return Ok(OpenQueue { handle_parameters });
    }

    let mut handle_parameters = None;
    for element in children {
        match element.tag {
            0 => {
                handle_parameters = Some(decode_queue_handle_parameters(
                    expect_context_constructed(element, 0)?,
                )?)
            }
            _ => return Err(Error::Protocol("unexpected field in OpenQueue")),
        }
    }
    Ok(OpenQueue {
        handle_parameters: handle_parameters
            .ok_or(Error::Protocol("OpenQueue.handleParameters is missing"))?,
    })
}

fn encode_open_queue(value: &OpenQueue) -> Result<Vec<u8>> {
    universal_sequence(&encode_queue_handle_parameters(&value.handle_parameters)?)
}

fn decode_open_queue_response(content: &[u8]) -> Result<OpenQueueResponse> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let original_request =
            decode_open_queue(fields.required_sequence("OpenQueueResponse.originalRequest")?)?;
        let routing_configuration = decode_routing_configuration(
            fields.required_sequence("OpenQueueResponse.routingConfiguration")?,
        )?;
        let deduplication_time_ms = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(300_000);
        fields.finish("OpenQueueResponse")?;
        return Ok(OpenQueueResponse {
            original_request,
            routing_configuration,
            deduplication_time_ms,
        });
    }

    let mut original_request = None;
    let mut routing_configuration = None;
    let mut deduplication_time_ms = 300_000;

    for element in children {
        match element.tag {
            0 => {
                original_request = Some(decode_open_queue(expect_context_constructed(element, 0)?)?)
            }
            1 => {
                routing_configuration = Some(decode_routing_configuration(
                    expect_context_constructed(element, 1)?,
                )?)
            }
            2 => deduplication_time_ms = decode_i32(expect_context_primitive(element, 2)?)?,
            _ => return Err(Error::Protocol("unexpected field in OpenQueueResponse")),
        }
    }

    Ok(OpenQueueResponse {
        original_request: original_request.ok_or(Error::Protocol(
            "OpenQueueResponse.originalRequest is missing",
        ))?,
        routing_configuration: routing_configuration.ok_or(Error::Protocol(
            "OpenQueueResponse.routingConfiguration is missing",
        ))?,
        deduplication_time_ms,
    })
}

fn encode_open_queue_response(value: &OpenQueueResponse) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_sequence(&encode_open_queue(
        &value.original_request,
    )?)?);
    out.extend(universal_sequence(&encode_routing_configuration(
        &value.routing_configuration,
    )?)?);
    out.extend(universal_i32(value.deduplication_time_ms)?);
    Ok(out)
}

fn decode_close_queue(content: &[u8]) -> Result<CloseQueue> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let handle_parameters = decode_queue_handle_parameters(
            fields.required_sequence("CloseQueue.handleParameters")?,
        )?;
        let is_final = fields
            .optional_boolean()
            .map(decode_bool)
            .transpose()?
            .unwrap_or(false);
        fields.finish("CloseQueue")?;
        return Ok(CloseQueue {
            handle_parameters,
            is_final,
        });
    }

    let mut handle_parameters = None;
    let mut is_final = false;

    for element in children {
        match element.tag {
            0 => {
                handle_parameters = Some(decode_queue_handle_parameters(
                    expect_context_constructed(element, 0)?,
                )?)
            }
            1 => is_final = decode_bool(expect_context_primitive(element, 1)?)?,
            _ => return Err(Error::Protocol("unexpected field in CloseQueue")),
        }
    }

    Ok(CloseQueue {
        handle_parameters: handle_parameters
            .ok_or(Error::Protocol("CloseQueue.handleParameters is missing"))?,
        is_final,
    })
}

fn encode_close_queue(value: &CloseQueue) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_sequence(&encode_queue_handle_parameters(
        &value.handle_parameters,
    )?)?);
    out.extend(universal_bool(value.is_final)?);
    Ok(out)
}

fn decode_configure_queue_stream(content: &[u8]) -> Result<ConfigureQueueStream> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let q_id = decode_u32(fields.required_integer("ConfigureQueueStream.qId")?)?;
        let stream_parameters = decode_queue_stream_parameters(
            fields.required_sequence("ConfigureQueueStream.streamParameters")?,
        )?;
        fields.finish("ConfigureQueueStream")?;
        return Ok(ConfigureQueueStream {
            q_id,
            stream_parameters,
        });
    }

    let mut q_id = None;
    let mut stream_parameters = None;

    for element in children {
        match element.tag {
            0 => q_id = Some(decode_u32(expect_context_primitive(element, 0)?)?),
            1 => {
                stream_parameters = Some(decode_queue_stream_parameters(
                    expect_context_constructed(element, 1)?,
                )?)
            }
            _ => return Err(Error::Protocol("unexpected field in ConfigureQueueStream")),
        }
    }

    Ok(ConfigureQueueStream {
        q_id: q_id.ok_or(Error::Protocol("ConfigureQueueStream.qId is missing"))?,
        stream_parameters: stream_parameters.ok_or(Error::Protocol(
            "ConfigureQueueStream.streamParameters is missing",
        ))?,
    })
}

fn encode_configure_queue_stream(value: &ConfigureQueueStream) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_u32(value.q_id)?);
    out.extend(universal_sequence(&encode_queue_stream_parameters(
        &value.stream_parameters,
    )?)?);
    Ok(out)
}

fn decode_configure_queue_stream_response(content: &[u8]) -> Result<ConfigureQueueStreamResponse> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let request = decode_configure_queue_stream(
            fields.required_sequence("ConfigureQueueStreamResponse.request")?,
        )?;
        fields.finish("ConfigureQueueStreamResponse")?;
        return Ok(ConfigureQueueStreamResponse { request });
    }

    let mut request = None;
    for element in children {
        match element.tag {
            0 => {
                request = Some(decode_configure_queue_stream(expect_context_constructed(
                    element, 0,
                )?)?)
            }
            _ => {
                return Err(Error::Protocol(
                    "unexpected field in ConfigureQueueStreamResponse",
                ));
            }
        }
    }
    Ok(ConfigureQueueStreamResponse {
        request: request.ok_or(Error::Protocol(
            "ConfigureQueueStreamResponse.request is missing",
        ))?,
    })
}

fn encode_configure_queue_stream_response(value: &ConfigureQueueStreamResponse) -> Result<Vec<u8>> {
    universal_sequence(&encode_configure_queue_stream(&value.request)?)
}

fn decode_configure_stream(content: &[u8]) -> Result<ConfigureStream> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let q_id = decode_u32(fields.required_integer("ConfigureStream.qId")?)?;
        let stream_parameters = decode_stream_parameters(
            fields.required_sequence("ConfigureStream.streamParameters")?,
        )?;
        fields.finish("ConfigureStream")?;
        return Ok(ConfigureStream {
            q_id,
            stream_parameters,
        });
    }

    let mut q_id = None;
    let mut stream_parameters = None;

    for element in children {
        match element.tag {
            0 => q_id = Some(decode_u32(expect_context_primitive(element, 0)?)?),
            1 => {
                stream_parameters = Some(decode_stream_parameters(expect_context_constructed(
                    element, 1,
                )?)?)
            }
            _ => return Err(Error::Protocol("unexpected field in ConfigureStream")),
        }
    }

    Ok(ConfigureStream {
        q_id: q_id.ok_or(Error::Protocol("ConfigureStream.qId is missing"))?,
        stream_parameters: stream_parameters.ok_or(Error::Protocol(
            "ConfigureStream.streamParameters is missing",
        ))?,
    })
}

fn encode_configure_stream(value: &ConfigureStream) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_u32(value.q_id)?);
    out.extend(universal_sequence(&encode_stream_parameters(
        &value.stream_parameters,
    )?)?);
    Ok(out)
}

fn decode_configure_stream_response(content: &[u8]) -> Result<ConfigureStreamResponse> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let request =
            decode_configure_stream(fields.required_sequence("ConfigureStreamResponse.request")?)?;
        fields.finish("ConfigureStreamResponse")?;
        return Ok(ConfigureStreamResponse { request });
    }

    let mut request = None;
    for element in children {
        match element.tag {
            0 => {
                request = Some(decode_configure_stream(expect_context_constructed(
                    element, 0,
                )?)?)
            }
            _ => {
                return Err(Error::Protocol(
                    "unexpected field in ConfigureStreamResponse",
                ));
            }
        }
    }
    Ok(ConfigureStreamResponse {
        request: request.ok_or(Error::Protocol(
            "ConfigureStreamResponse.request is missing",
        ))?,
    })
}

fn encode_configure_stream_response(value: &ConfigureStreamResponse) -> Result<Vec<u8>> {
    universal_sequence(&encode_configure_stream(&value.request)?)
}

fn decode_guid_info(content: &[u8]) -> Result<GuidInfo> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let client_id = fields
            .optional_string()
            .map(decode_string)
            .transpose()?
            .unwrap_or_default();
        let nano_seconds_from_epoch = fields
            .optional_integer()
            .map(decode_i64)
            .transpose()?
            .unwrap_or(0);
        fields.finish("GuidInfo")?;
        return Ok(GuidInfo {
            client_id,
            nano_seconds_from_epoch,
        });
    }

    let mut client_id = String::new();
    let mut nano_seconds_from_epoch = 0;

    for element in children {
        match element.tag {
            0 => client_id = decode_string(expect_context_primitive(element, 0)?)?,
            1 => nano_seconds_from_epoch = decode_i64(expect_context_primitive(element, 1)?)?,
            _ => return Err(Error::Protocol("unexpected field in GuidInfo")),
        }
    }

    Ok(GuidInfo {
        client_id,
        nano_seconds_from_epoch,
    })
}

fn encode_guid_info(value: &GuidInfo) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_string(&value.client_id)?);
    out.extend(universal_i64(value.nano_seconds_from_epoch)?);
    Ok(out)
}

fn decode_client_identity(content: &[u8]) -> Result<ClientIdentity> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let protocol_version =
            decode_i32(fields.required_integer("ClientIdentity.protocolVersion")?)?;
        let sdk_version = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(999_999);
        let client_type = decode_client_type(fields.required_enum("ClientIdentity.clientType")?)?;
        let process_name = fields
            .optional_string()
            .map(decode_string)
            .transpose()?
            .unwrap_or_default();
        let pid = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(0);
        let session_id = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(1);
        let host_name = fields
            .optional_string()
            .map(decode_string)
            .transpose()?
            .unwrap_or_default();
        let features = fields
            .optional_string()
            .map(decode_string)
            .transpose()?
            .unwrap_or_default();
        let cluster_name = fields
            .optional_string()
            .map(decode_string)
            .transpose()?
            .unwrap_or_default();
        let cluster_node_id = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(-1);
        let sdk_language = fields
            .optional_enum()
            .map(decode_client_language)
            .transpose()?
            .unwrap_or(ClientLanguage::Cpp);
        let guid_info = fields
            .optional_sequence()
            .map(decode_guid_info)
            .transpose()?
            .unwrap_or_default();
        let user_agent = fields
            .optional_string()
            .map(decode_string)
            .transpose()?
            .unwrap_or_default();
        fields.finish("ClientIdentity")?;
        return Ok(ClientIdentity {
            protocol_version,
            sdk_version,
            client_type,
            process_name,
            pid,
            session_id,
            host_name,
            features,
            cluster_name,
            cluster_node_id,
            sdk_language,
            guid_info,
            user_agent,
        });
    }

    let mut protocol_version = None;
    let mut sdk_version = 999_999;
    let mut client_type = None;
    let mut process_name = String::new();
    let mut pid = 0;
    let mut session_id = 1;
    let mut host_name = String::new();
    let mut features = String::new();
    let mut cluster_name = String::new();
    let mut cluster_node_id = -1;
    let mut sdk_language = ClientLanguage::Cpp;
    let mut guid_info = GuidInfo::default();
    let mut user_agent = String::new();

    for element in children {
        match element.tag {
            0 => protocol_version = Some(decode_i32(expect_context_primitive(element, 0)?)?),
            1 => sdk_version = decode_i32(expect_context_primitive(element, 1)?)?,
            2 => client_type = Some(decode_client_type(expect_context_primitive(element, 2)?)?),
            3 => process_name = decode_string(expect_context_primitive(element, 3)?)?,
            4 => pid = decode_i32(expect_context_primitive(element, 4)?)?,
            5 => session_id = decode_i32(expect_context_primitive(element, 5)?)?,
            6 => host_name = decode_string(expect_context_primitive(element, 6)?)?,
            7 => features = decode_string(expect_context_primitive(element, 7)?)?,
            8 => cluster_name = decode_string(expect_context_primitive(element, 8)?)?,
            9 => cluster_node_id = decode_i32(expect_context_primitive(element, 9)?)?,
            10 => sdk_language = decode_client_language(expect_context_primitive(element, 10)?)?,
            11 => guid_info = decode_guid_info(expect_context_constructed(element, 11)?)?,
            12 => user_agent = decode_string(expect_context_primitive(element, 12)?)?,
            _ => return Err(Error::Protocol("unexpected field in ClientIdentity")),
        }
    }

    Ok(ClientIdentity {
        protocol_version: protocol_version
            .ok_or(Error::Protocol("ClientIdentity.protocolVersion is missing"))?,
        sdk_version,
        client_type: client_type.ok_or(Error::Protocol("ClientIdentity.clientType is missing"))?,
        process_name,
        pid,
        session_id,
        host_name,
        features,
        cluster_name,
        cluster_node_id,
        sdk_language,
        guid_info,
        user_agent,
    })
}

fn encode_client_identity(value: &ClientIdentity) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_i32(value.protocol_version)?);
    out.extend(universal_i32(value.sdk_version)?);
    out.extend(universal_enum(encode_client_type(value.client_type))?);
    out.extend(universal_string(&value.process_name)?);
    out.extend(universal_i32(value.pid)?);
    out.extend(universal_i32(value.session_id)?);
    out.extend(universal_string(&value.host_name)?);
    out.extend(universal_string(&value.features)?);
    out.extend(universal_string(&value.cluster_name)?);
    out.extend(universal_i32(value.cluster_node_id)?);
    out.extend(universal_enum(encode_client_language(value.sdk_language))?);
    out.extend(universal_sequence(&encode_guid_info(&value.guid_info)?)?);
    out.extend(universal_string(&value.user_agent)?);
    Ok(out)
}

fn decode_broker_response(content: &[u8]) -> Result<BrokerResponse> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let result = decode_status(fields.required_sequence("BrokerResponse.result")?)?;
        let protocol_version =
            decode_i32(fields.required_integer("BrokerResponse.protocolVersion")?)?;
        let broker_version = decode_i32(fields.required_integer("BrokerResponse.brokerVersion")?)?;
        let is_deprecated_sdk = fields
            .optional_boolean()
            .map(decode_bool)
            .transpose()?
            .unwrap_or(false);
        let broker_identity =
            decode_client_identity(fields.required_sequence("BrokerResponse.brokerIdentity")?)?;
        let heartbeat_interval_ms = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(3_000);
        let max_missed_heartbeats = fields
            .optional_integer()
            .map(decode_i32)
            .transpose()?
            .unwrap_or(10);
        fields.finish("BrokerResponse")?;
        return Ok(BrokerResponse {
            result,
            protocol_version,
            broker_version,
            is_deprecated_sdk,
            broker_identity,
            heartbeat_interval_ms,
            max_missed_heartbeats,
        });
    }

    let mut result = None;
    let mut protocol_version = None;
    let mut broker_version = None;
    let mut is_deprecated_sdk = false;
    let mut broker_identity = None;
    let mut heartbeat_interval_ms = 3_000;
    let mut max_missed_heartbeats = 10;

    for element in children {
        match element.tag {
            0 => result = Some(decode_status(expect_context_constructed(element, 0)?)?),
            1 => protocol_version = Some(decode_i32(expect_context_primitive(element, 1)?)?),
            2 => broker_version = Some(decode_i32(expect_context_primitive(element, 2)?)?),
            3 => is_deprecated_sdk = decode_bool(expect_context_primitive(element, 3)?)?,
            4 => {
                broker_identity = Some(decode_client_identity(expect_context_constructed(
                    element, 4,
                )?)?)
            }
            5 => heartbeat_interval_ms = decode_i32(expect_context_primitive(element, 5)?)?,
            6 => max_missed_heartbeats = decode_i32(expect_context_primitive(element, 6)?)?,
            _ => return Err(Error::Protocol("unexpected field in BrokerResponse")),
        }
    }

    Ok(BrokerResponse {
        result: result.ok_or(Error::Protocol("BrokerResponse.result is missing"))?,
        protocol_version: protocol_version
            .ok_or(Error::Protocol("BrokerResponse.protocolVersion is missing"))?,
        broker_version: broker_version
            .ok_or(Error::Protocol("BrokerResponse.brokerVersion is missing"))?,
        is_deprecated_sdk,
        broker_identity: broker_identity
            .ok_or(Error::Protocol("BrokerResponse.brokerIdentity is missing"))?,
        heartbeat_interval_ms,
        max_missed_heartbeats,
    })
}

fn encode_broker_response(value: &BrokerResponse) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_sequence(&encode_status(&value.result)?)?);
    out.extend(universal_i32(value.protocol_version)?);
    out.extend(universal_i32(value.broker_version)?);
    out.extend(universal_bool(value.is_deprecated_sdk)?);
    out.extend(universal_sequence(&encode_client_identity(
        &value.broker_identity,
    )?)?);
    out.extend(universal_i32(value.heartbeat_interval_ms)?);
    out.extend(universal_i32(value.max_missed_heartbeats)?);
    Ok(out)
}

fn decode_authentication_request(content: &[u8]) -> Result<AuthenticationRequest> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let mechanism = decode_string(fields.required_string("AuthenticationRequest.mechanism")?)?;
        let data = fields.optional_octet_string().map(|bytes| bytes.to_vec());
        fields.finish("AuthenticationRequest")?;
        return Ok(AuthenticationRequest { mechanism, data });
    }

    let mut mechanism = None;
    let mut data = None;

    for element in children {
        match element.tag {
            0 => mechanism = Some(decode_string(expect_context_primitive(element, 0)?)?),
            1 => data = Some(expect_context_primitive(element, 1)?.to_vec()),
            _ => return Err(Error::Protocol("unexpected field in AuthenticationRequest")),
        }
    }

    Ok(AuthenticationRequest {
        mechanism: mechanism.ok_or(Error::Protocol(
            "AuthenticationRequest.mechanism is missing",
        ))?,
        data,
    })
}

fn encode_authentication_request(value: &AuthenticationRequest) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_string(&value.mechanism)?);
    if let Some(data) = &value.data {
        out.extend(universal_bytes(data)?);
    }
    Ok(out)
}

fn decode_authentication_response(content: &[u8]) -> Result<AuthenticationResponse> {
    let children = sequence_children(content)?;
    if children
        .first()
        .is_some_and(|element| element.class == BerClass::Universal)
    {
        let mut fields = OrderedFields::new(&children);
        let status = decode_status(fields.required_sequence("AuthenticationResponse.status")?)?;
        let lifetime_ms = fields.optional_integer().map(decode_i32).transpose()?;
        fields.finish("AuthenticationResponse")?;
        return Ok(AuthenticationResponse {
            status,
            lifetime_ms,
        });
    }

    let mut status = None;
    let mut lifetime_ms = None;

    for element in children {
        match element.tag {
            0 => status = Some(decode_status(expect_context_constructed(element, 0)?)?),
            1 => lifetime_ms = Some(decode_i32(expect_context_primitive(element, 1)?)?),
            _ => {
                return Err(Error::Protocol(
                    "unexpected field in AuthenticationResponse",
                ));
            }
        }
    }

    Ok(AuthenticationResponse {
        status: status.ok_or(Error::Protocol("AuthenticationResponse.status is missing"))?,
        lifetime_ms,
    })
}

fn encode_authentication_response(value: &AuthenticationResponse) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(universal_sequence(&encode_status(&value.status)?)?);
    if let Some(lifetime_ms) = value.lifetime_ms {
        out.extend(universal_i32(lifetime_ms)?);
    }
    Ok(out)
}

fn encode_selected_choice(choice_tag: u32, fields: &[u8]) -> Result<Vec<u8>> {
    let payload = wrap_sequence(fields)?;
    encode_constructed(BerClass::ContextSpecific, choice_tag, &payload)
}

fn encode_choice_wrapper(field_tag: u32, choice_tag: u32, fields: &[u8]) -> Result<Vec<u8>> {
    let selected = encode_selected_choice(choice_tag, fields)?;
    encode_constructed(BerClass::ContextSpecific, field_tag, &selected)
}

fn decode_control_choice(element: BerElement<'_>) -> Result<ControlPayload> {
    Ok(match element.tag {
        0 => ControlPayload::Status(decode_status(element.content)?),
        1 => ControlPayload::Disconnect(decode_empty(element.content)?),
        2 => ControlPayload::DisconnectResponse(decode_empty(element.content)?),
        7 => ControlPayload::AdminCommand(decode_admin_command(element.content)?),
        8 => ControlPayload::AdminCommandResponse(decode_admin_command_response(element.content)?),
        9 => ControlPayload::ClusterMessage(ClusterMessage {
            raw: element.content.to_vec(),
        }),
        10 => ControlPayload::OpenQueue(decode_open_queue(element.content)?),
        11 => ControlPayload::OpenQueueResponse(decode_open_queue_response(element.content)?),
        12 => ControlPayload::CloseQueue(decode_close_queue(element.content)?),
        13 => ControlPayload::CloseQueueResponse(decode_empty(element.content)?),
        14 => ControlPayload::ConfigureQueueStream(decode_configure_queue_stream(element.content)?),
        15 => ControlPayload::ConfigureQueueStreamResponse(decode_configure_queue_stream_response(
            element.content,
        )?),
        16 => ControlPayload::ConfigureStream(decode_configure_stream(element.content)?),
        17 => ControlPayload::ConfigureStreamResponse(decode_configure_stream_response(
            element.content,
        )?),
        _ => return Err(Error::Protocol("unknown ControlMessage choice")),
    })
}

impl BerCodec for ControlMessage {
    fn encode_ber(&self) -> Result<Vec<u8>> {
        let mut content = Vec::new();
        if let Some(r_id) = self.r_id {
            content.extend(context_i32(0, r_id)?);
        }

        let choice = match &self.payload {
            ControlPayload::Status(value) => encode_selected_choice(0, &encode_status(value)?)?,
            ControlPayload::Disconnect(_) => encode_selected_choice(1, &[])?,
            ControlPayload::DisconnectResponse(value) => {
                let _ = value;
                encode_selected_choice(2, &[])?
            }
            ControlPayload::AdminCommand(value) => {
                encode_selected_choice(7, &encode_admin_command(value)?)?
            }
            ControlPayload::AdminCommandResponse(value) => {
                encode_selected_choice(8, &encode_admin_command_response(value)?)?
            }
            ControlPayload::ClusterMessage(value) => {
                encode_constructed(BerClass::ContextSpecific, 9, &value.raw)?
            }
            ControlPayload::OpenQueue(value) => {
                encode_selected_choice(10, &encode_open_queue(value)?)?
            }
            ControlPayload::OpenQueueResponse(value) => {
                encode_selected_choice(11, &encode_open_queue_response(value)?)?
            }
            ControlPayload::CloseQueue(value) => {
                encode_selected_choice(12, &encode_close_queue(value)?)?
            }
            ControlPayload::CloseQueueResponse(_) => encode_selected_choice(13, &[])?,
            ControlPayload::ConfigureQueueStream(value) => {
                encode_selected_choice(14, &encode_configure_queue_stream(value)?)?
            }
            ControlPayload::ConfigureQueueStreamResponse(value) => {
                encode_selected_choice(15, &encode_configure_queue_stream_response(value)?)?
            }
            ControlPayload::ConfigureStream(value) => {
                encode_selected_choice(16, &encode_configure_stream(value)?)?
            }
            ControlPayload::ConfigureStreamResponse(value) => {
                encode_selected_choice(17, &encode_configure_stream_response(value)?)?
            }
        };
        content.extend(choice);

        wrap_sequence(&content)
    }

    fn decode_ber(payload: &[u8]) -> Result<Self> {
        let content = expect_sequence_root(payload)?;
        let mut r_id = None;
        let mut choice = None;

        for element in parse_children(content)? {
            match (element.class, element.tag) {
                (BerClass::Universal, UNIVERSAL_INTEGER_TAG) if !element.constructed => {
                    r_id = Some(decode_i32(element.content)?);
                }
                (BerClass::ContextSpecific, 0) if !element.constructed => {
                    r_id = Some(decode_i32(element.content)?);
                }
                (BerClass::ContextSpecific, 1) if !element.constructed => {
                    r_id = Some(decode_i32(element.content)?);
                }
                (BerClass::ContextSpecific, 1) if element.constructed => {
                    // Accept the legacy wrapped representation we previously
                    // emitted before aligning ControlMessage with the upstream
                    // untagged choice encoding.
                    let children = parse_children(element.content)?;
                    if children.len() == 1
                        && children[0].class == BerClass::ContextSpecific
                        && children[0].constructed
                    {
                        choice = Some(decode_control_choice(children[0])?);
                    } else {
                        choice = Some(ControlPayload::Disconnect(decode_empty(element.content)?));
                    }
                }
                (BerClass::ContextSpecific, tag)
                    if element.constructed
                        && matches!(
                            tag,
                            0 | 2 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17
                        ) =>
                {
                    choice = Some(decode_control_choice(element)?);
                }
                _ => return Err(Error::Protocol("unexpected BER field in ControlMessage")),
            }
        }

        Ok(ControlMessage {
            r_id,
            payload: choice.ok_or(Error::Protocol("ControlMessage payload is missing"))?,
        })
    }
}

impl BerCodec for NegotiationMessage {
    fn encode_ber(&self) -> Result<Vec<u8>> {
        let choice = match &self.payload {
            NegotiationPayload::ClientIdentity(value) => {
                encode_choice_wrapper(0, 0, &encode_client_identity(value)?)?
            }
            NegotiationPayload::BrokerResponse(value) => {
                encode_choice_wrapper(0, 1, &encode_broker_response(value)?)?
            }
            NegotiationPayload::PlaceHolder(_) => encode_choice_wrapper(0, 2, &[])?,
        };
        wrap_sequence(&choice)
    }

    fn decode_ber(payload: &[u8]) -> Result<Self> {
        if let Ok(root) = parse_root(payload) {
            if root.class == BerClass::ContextSpecific && root.constructed {
                return decode_negotiation_element(root);
            }

            if root.class == BerClass::Universal
                && root.constructed
                && root.tag == UNIVERSAL_SEQUENCE_TAG
            {
                let children = parse_children(root.content)?;
                if children.len() == 1
                    && children[0].class == BerClass::ContextSpecific
                    && children[0].constructed
                {
                    return decode_negotiation_element(children[0]);
                }
                return decode_negotiation_from_content(root.content);
            }
        }

        decode_negotiation_from_content(payload)
    }
}

impl BerCodec for AuthenticationMessage {
    fn encode_ber(&self) -> Result<Vec<u8>> {
        let choice = match &self.payload {
            AuthenticationPayload::AuthenticationRequest(value) => {
                encode_choice_wrapper(0, 0, &encode_authentication_request(value)?)?
            }
            AuthenticationPayload::AuthenticationResponse(value) => {
                encode_choice_wrapper(0, 1, &encode_authentication_response(value)?)?
            }
        };
        wrap_sequence(&choice)
    }

    fn decode_ber(payload: &[u8]) -> Result<Self> {
        if let Ok(root) = parse_root(payload) {
            if root.class == BerClass::ContextSpecific && root.constructed {
                return decode_authentication_element(root);
            }

            if root.class == BerClass::Universal
                && root.constructed
                && root.tag == UNIVERSAL_SEQUENCE_TAG
            {
                let children = parse_children(root.content)?;
                if children.len() == 1
                    && children[0].class == BerClass::ContextSpecific
                    && children[0].constructed
                {
                    return decode_authentication_element(children[0]);
                }
                return decode_authentication_from_content(root.content);
            }
        }

        decode_authentication_from_content(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn high_tag_identifier_round_trips() {
        let mut encoded = Vec::new();
        encode_tag(BerClass::ContextSpecific, true, 257, &mut encoded).unwrap();
        encoded.push(0x80);
        encoded.extend_from_slice(&BER_EOC);

        let (element, consumed) = parse_one(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(element.class, BerClass::ContextSpecific);
        assert!(element.constructed);
        assert_eq!(element.tag, 257);
    }
}

fn decode_negotiation_choice(element: BerElement<'_>) -> Result<NegotiationMessage> {
    let payload = match element.tag {
        0 => NegotiationPayload::ClientIdentity(decode_client_identity(element.content)?),
        1 => NegotiationPayload::BrokerResponse(decode_broker_response(element.content)?),
        2 => NegotiationPayload::PlaceHolder(decode_empty(element.content)?),
        _ => return Err(Error::Protocol("unknown NegotiationMessage choice")),
    };
    Ok(NegotiationMessage { payload })
}

fn decode_negotiation_element(element: BerElement<'_>) -> Result<NegotiationMessage> {
    if element.class == BerClass::ContextSpecific && element.constructed {
        let children = parse_children(element.content)?;
        if children.len() == 1
            && children[0].class == BerClass::ContextSpecific
            && children[0].constructed
        {
            return decode_negotiation_element(children[0]);
        }
        if let Ok(message) = decode_negotiation_from_content(element.content) {
            return Ok(message);
        }
    }
    decode_negotiation_choice(element)
}

fn decode_negotiation_from_content(content: &[u8]) -> Result<NegotiationMessage> {
    if let Ok(root) = parse_root(content) {
        if root.class == BerClass::ContextSpecific && root.constructed {
            return decode_negotiation_element(root);
        }
        if root.class == BerClass::Universal
            && root.constructed
            && root.tag == UNIVERSAL_SEQUENCE_TAG
        {
            let children = parse_children(root.content)?;
            if children.len() == 1
                && children[0].class == BerClass::ContextSpecific
                && children[0].constructed
            {
                return decode_negotiation_element(children[0]);
            }
        }
    }
    if let Ok(message) = decode_broker_response(content) {
        return Ok(NegotiationMessage {
            payload: NegotiationPayload::BrokerResponse(message),
        });
    }
    if let Ok(message) = decode_client_identity(content) {
        return Ok(NegotiationMessage {
            payload: NegotiationPayload::ClientIdentity(message),
        });
    }
    if let Ok(message) = decode_empty(content) {
        return Ok(NegotiationMessage {
            payload: NegotiationPayload::PlaceHolder(message),
        });
    }
    Err(Error::ProtocolMessage(format!(
        "unable to decode NegotiationMessage BER payload ({}, segments=[{}], hex={})",
        describe_root(content),
        describe_segments(content),
        format_hex(content, 128)
    )))
}

fn decode_authentication_choice(element: BerElement<'_>) -> Result<AuthenticationMessage> {
    let payload = match element.tag {
        0 => AuthenticationPayload::AuthenticationRequest(decode_authentication_request(
            element.content,
        )?),
        1 => AuthenticationPayload::AuthenticationResponse(decode_authentication_response(
            element.content,
        )?),
        _ => return Err(Error::Protocol("unknown AuthenticationMessage choice")),
    };
    Ok(AuthenticationMessage { payload })
}

fn decode_authentication_element(element: BerElement<'_>) -> Result<AuthenticationMessage> {
    if element.class == BerClass::ContextSpecific && element.constructed {
        let children = parse_children(element.content)?;
        if children.len() == 1
            && children[0].class == BerClass::ContextSpecific
            && children[0].constructed
        {
            return decode_authentication_element(children[0]);
        }
        if let Ok(message) = decode_authentication_from_content(element.content) {
            return Ok(message);
        }
    }
    decode_authentication_choice(element)
}

fn decode_authentication_from_content(content: &[u8]) -> Result<AuthenticationMessage> {
    if let Ok(root) = parse_root(content) {
        if root.class == BerClass::ContextSpecific && root.constructed {
            return decode_authentication_element(root);
        }
        if root.class == BerClass::Universal
            && root.constructed
            && root.tag == UNIVERSAL_SEQUENCE_TAG
        {
            let children = parse_children(root.content)?;
            if children.len() == 1
                && children[0].class == BerClass::ContextSpecific
                && children[0].constructed
            {
                return decode_authentication_element(children[0]);
            }
        }
    }
    if let Ok(message) = decode_authentication_response(content) {
        return Ok(AuthenticationMessage {
            payload: AuthenticationPayload::AuthenticationResponse(message),
        });
    }
    if let Ok(message) = decode_authentication_request(content) {
        return Ok(AuthenticationMessage {
            payload: AuthenticationPayload::AuthenticationRequest(message),
        });
    }
    Err(Error::ProtocolMessage(format!(
        "unable to decode AuthenticationMessage BER payload ({}, segments=[{}], hex={})",
        describe_root(content),
        describe_segments(content),
        format_hex(content, 128)
    )))
}
