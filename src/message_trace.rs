use crate::wire::{MessageGuid, MessageProperties, MessagePropertyValue};
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use tracing::{Span, info_span, trace};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) const PROPERTY_PREFIX: &str = "blazox.trace.";

pub(crate) fn inject_current_context(properties: &mut MessageProperties) {
    let context = Span::current().context();
    if !context.span().span_context().is_valid() {
        return;
    }

    global::get_text_map_propagator(|propagator| {
        let mut injector = MessagePropertiesInjector { properties };
        propagator.inject_context(&context, &mut injector);
    });
}

pub(crate) fn handling_span(
    properties: &MessageProperties,
    operation: impl Into<String>,
    queue_id: u32,
    sub_queue_id: u32,
    message_guid: MessageGuid,
) -> Span {
    let operation = operation.into();
    let span = info_span!(
        target: "blazox::messages::handler",
        "blazox.message.handle",
        operation = %operation,
        queue_id,
        sub_queue_id,
        message_guid = ?message_guid,
    );

    let context = extract_context(properties);
    if context.span().span_context().is_valid()
        && let Err(error) = span.set_parent(context)
    {
        trace!(
            target: "blazox::messages::trace",
            %error,
            queue_id,
            sub_queue_id,
            message_guid = ?message_guid,
            "failed to set message handling span parent"
        );
    }

    span
}

fn extract_context(properties: &MessageProperties) -> opentelemetry::Context {
    global::get_text_map_propagator(|propagator| {
        propagator.extract(&MessagePropertiesExtractor { properties })
    })
}

fn property_name(key: &str) -> String {
    format!("{PROPERTY_PREFIX}{}", key.to_ascii_lowercase())
}

struct MessagePropertiesInjector<'a> {
    properties: &'a mut MessageProperties,
}

impl Injector for MessagePropertiesInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.properties
            .insert_or_replace(property_name(key), MessagePropertyValue::String(value));
    }
}

struct MessagePropertiesExtractor<'a> {
    properties: &'a MessageProperties,
}

impl Extractor for MessagePropertiesExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        match self.properties.get(property_name(key).as_str()) {
            Some(MessagePropertyValue::String(value)) => Some(value.as_str()),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        self.properties
            .iter()
            .filter(|property| property.name.starts_with(PROPERTY_PREFIX))
            .map(|property| property.name.as_str())
            .collect()
    }
}
