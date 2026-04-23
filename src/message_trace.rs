#[cfg(feature = "trace-propagation")]
use crate::wire::MessagePropertyValue;
use crate::wire::{MessageGuid, MessageProperties};
#[cfg(feature = "trace-propagation")]
use opentelemetry::global;
#[cfg(feature = "trace-propagation")]
use opentelemetry::propagation::{Extractor, Injector};
#[cfg(feature = "trace-propagation")]
use opentelemetry::trace::TraceContextExt;
#[cfg(feature = "trace-propagation")]
use tracing::trace;
use tracing::{Span, info_span};
#[cfg(feature = "trace-propagation")]
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[cfg(feature = "trace-propagation")]
pub(crate) const PROPERTY_PREFIX: &str = "blazox.trace.";

pub(crate) fn inject_current_context(properties: &mut MessageProperties) {
    #[cfg(not(feature = "trace-propagation"))]
    {
        let _ = properties;
        return;
    }

    #[cfg(feature = "trace-propagation")]
    let context = Span::current().context();
    #[cfg(feature = "trace-propagation")]
    if !context.span().span_context().is_valid() {
        return;
    }

    #[cfg(feature = "trace-propagation")]
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
    let _ = properties;
    let span = info_span!(
        target: "blazox::messages::handler",
        "blazox.message.handle",
        operation = %operation,
        queue_id,
        sub_queue_id,
        message_guid = ?message_guid,
    );

    #[cfg(feature = "trace-propagation")]
    {
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
    }

    span
}

#[cfg(feature = "trace-propagation")]
fn extract_context(properties: &MessageProperties) -> opentelemetry::Context {
    global::get_text_map_propagator(|propagator| {
        propagator.extract(&MessagePropertiesExtractor { properties })
    })
}

#[cfg(feature = "trace-propagation")]
fn property_name(key: &str) -> String {
    format!("{PROPERTY_PREFIX}{}", key.to_ascii_lowercase())
}

#[cfg(feature = "trace-propagation")]
struct MessagePropertiesInjector<'a> {
    properties: &'a mut MessageProperties,
}

#[cfg(feature = "trace-propagation")]
impl Injector for MessagePropertiesInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.properties
            .insert_or_replace(property_name(key), MessagePropertyValue::String(value));
    }
}

#[cfg(feature = "trace-propagation")]
struct MessagePropertiesExtractor<'a> {
    properties: &'a MessageProperties,
}

#[cfg(feature = "trace-propagation")]
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
