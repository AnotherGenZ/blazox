use blazox::{
    CompressionAlgorithm, CorrelationIdGenerator, ManualHostHealthMonitor, MessageProperties,
    MessagePropertyValue, PostMessage, QueueEvent, QueueOptions, Session, SessionEvent,
    SessionOptions, TraceOperation, TraceSink, Uri, UriBuilder,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct LoggingTraceSink;

impl TraceSink for LoggingTraceSink {
    fn operation_started(&self, operation: &TraceOperation) {
        info!(
            kind = ?operation.kind,
            queue = display_queue(operation.queue.as_ref()),
            "trace started"
        );
    }

    fn operation_succeeded(&self, operation: &TraceOperation) {
        info!(
            kind = ?operation.kind,
            queue = display_queue(operation.queue.as_ref()),
            "trace ok"
        );
    }

    fn operation_failed(&self, operation: &TraceOperation, error: &blazox::Error) {
        error!(
            kind = ?operation.kind,
            queue = display_queue(operation.queue.as_ref()),
            %error,
            "trace failed"
        );
    }
}

fn display_queue(queue: Option<&Uri>) -> &str {
    queue.map(Uri::as_str).unwrap_or("-")
}

fn default_queue_uri() -> blazox::Result<Uri> {
    UriBuilder::new()
        .domain("bmq.test.mem.priority")
        .queue("hello-world")
        .build()
}

fn build_properties(kind: &str, source: &str, attempt: i32) -> MessageProperties {
    let mut properties = MessageProperties::new();
    properties.insert("kind", MessagePropertyValue::String(kind.into()));
    properties.insert("source", MessagePropertyValue::String(source.into()));
    properties.insert("attempt", MessagePropertyValue::Int32(attempt));
    properties
}

fn print_session_event(event: SessionEvent) {
    match event {
        SessionEvent::Connected => info!("session connected"),
        SessionEvent::Authenticated => info!("session authenticated"),
        SessionEvent::Reconnecting { attempt, error } => {
            warn!(attempt, %error, "session reconnecting")
        }
        SessionEvent::Reconnected => info!("session reconnected"),
        SessionEvent::Disconnected => info!("session disconnected"),
        SessionEvent::TransportError(error) => error!(%error, "transport error"),
        SessionEvent::HostHealthChanged(state) => {
            info!(state = ?state, "host health changed")
        }
        SessionEvent::QueueOpened { uri, queue_id } => {
            info!(%uri, queue_id, "queue opened")
        }
        SessionEvent::QueueReopened { uri, queue_id } => {
            info!(%uri, queue_id, "queue reopened")
        }
        SessionEvent::QueueClosed { uri } => info!(%uri, "queue closed"),
        SessionEvent::QueueSuspended { uri } => warn!(%uri, "queue suspended"),
        SessionEvent::QueueResumed { uri } => info!(%uri, "queue resumed"),
        SessionEvent::SlowConsumerHighWatermark { pending } => {
            warn!(pending, "session event queue reached high watermark")
        }
        SessionEvent::SlowConsumerNormal { pending } => {
            info!(pending, "session event queue returned to normal")
        }
        SessionEvent::Schema(schema) => info!(schema = ?schema, "session schema event"),
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| {
            EnvFilter::try_new(std::env::var("BLAZOX_LOG").unwrap_or_else(|_| "info".to_string()))
        })
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init();
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> blazox::Result<()> {
    init_tracing();

    let addr = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("BLAZOX_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:30114".to_string());
    let queue_uri = std::env::args()
        .nth(2)
        .map(Uri::parse)
        .transpose()?
        .or_else(|| {
            std::env::var("BLAZOX_QUEUE")
                .ok()
                .map(Uri::parse)
                .transpose()
                .ok()
                .flatten()
        })
        .unwrap_or(default_queue_uri()?);
    let skip_auth = std::env::var("BLAZOX_SKIP_AUTH")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let auth_mode = std::env::var("BLAZOX_AUTH")
        .unwrap_or_else(|_| {
            if skip_auth {
                "skip".to_string()
            } else {
                "skip".to_string()
            }
        })
        .to_ascii_lowercase();
    let auth_timeout = std::env::var("BLAZOX_AUTH_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_secs(2));
    let run_admin = std::env::var("BLAZOX_RUN_ADMIN")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let dump_messages = std::env::var("BLAZOX_DUMP_MESSAGES").ok();

    let host_health = Arc::new(ManualHostHealthMonitor::new());
    let trace_sink = Arc::new(LoggingTraceSink);
    let mut session_options = SessionOptions::default()
        .broker_addr(addr.clone())
        .request_timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10))
        .session_id(7)
        .process_name_override("blazox-hello-world")
        .host_name_override("localhost")
        .user_agent("blazox/examples/hello_world")
        .host_health_monitor(host_health.clone())
        .trace_sink(trace_sink);
    session_options.features = "PROTOCOL_ENCODING:BER,JSON;MPS:MESSAGE_PROPERTIES_EX".to_string();

    let session = Session::connect(session_options).await?;

    info!(%addr, started = session.is_started(), "connected");

    let session_task = session.spawn_event_handler(|event| async move {
        print_session_event(event);
    });

    match auth_mode.as_str() {
        "skip" => {
            info!(
                "skipping anonymous authentication (set BLAZOX_AUTH=try or BLAZOX_AUTH=always to enable it)"
            );
        }
        "always" | "require" => {
            session.authenticate_anonymous().await?;
        }
        "try" | "" => match timeout(auth_timeout, session.authenticate_anonymous()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(%error, "anonymous authentication failed, continuing without it");
            }
            Err(_) => {
                warn!(timeout = ?auth_timeout, "anonymous authentication timed out, continuing without it");
            }
        },
        other => {
            return Err(blazox::Error::ProtocolMessage(format!(
                "unsupported BLAZOX_AUTH mode '{other}', expected skip, try, or always"
            )));
        }
    }

    if let Some(command) = dump_messages.as_deref() {
        session.configure_message_dumping(command).await?;
        info!(command, "configured message dumping");
    }

    if run_admin {
        match session.admin_command("help").await {
            Ok(text) => {
                if let Some(line) = text.lines().next() {
                    info!(first_line = %line, "admin help");
                } else {
                    info!("admin help returned an empty response");
                }
            }
            Err(error) => error!(%error, "admin command failed"),
        }
    }

    let queue_options = QueueOptions::read_write()
        .max_unconfirmed_messages(16)
        .max_unconfirmed_bytes(1 << 20)
        .consumer_priority(1)
        .consumer_priority_count(1)
        .suspends_on_bad_host_health(true);
    let (queue, open_status) = session
        .open_queue_with_status(queue_uri.as_str(), queue_options.clone())
        .await?;
    info!(
        uri = %open_status.uri,
        queue_id = open_status.queue_id,
        "opened queue"
    );
    info!(
        lookup_queue_id = ?session.get_queue_id(queue_uri.as_str()).await,
        queue_found = session.get_queue(queue_uri.as_str()).await.is_some(),
        "verified queue lookup helpers"
    );

    let mut queue_events = queue.events();

    host_health.set_unhealthy();
    sleep(Duration::from_millis(100)).await;
    host_health.set_healthy();
    sleep(Duration::from_millis(100)).await;

    let configure_status = queue
        .reconfigure(
            queue_options
                .clone()
                .max_unconfirmed_messages(32)
                .max_unconfirmed_bytes(2 << 20)
                .consumer_priority_count(2),
        )
        .await;
    configure_status?;
    info!("reconfigured queue stream");

    let correlation_ids = CorrelationIdGenerator::default();
    let mut publisher = session.load_put_builder(&queue);
    publisher.push(
        PostMessage::new("hello world from blazox (compressed)")
            .properties(build_properties("hello", "blazox", 1))
            .correlation_id(correlation_ids.next())
            .compression(CompressionAlgorithm::Zlib),
    );
    publisher.push(
        PostMessage::new("hello world from blazox (explicit guid)")
            .properties(build_properties("hello-guid", "blazox", 2))
            .correlation_id(correlation_ids.next())
            .message_guid(session.next_message_guid())
            .compression(CompressionAlgorithm::None),
    );
    let packed = publisher.pack().await?;
    queue.post_packed_batch(packed).await?;
    info!("published batch");

    let mut acknowledgements = Vec::new();
    let mut deliveries = Vec::new();

    while acknowledgements.len() < 2 || deliveries.len() < 2 {
        let event = timeout(Duration::from_secs(10), queue_events.recv())
            .await
            .map_err(|_| blazox::Error::Timeout)?
            .map_err(|_| blazox::Error::RequestCanceled)?;

        match event {
            QueueEvent::Ack(ack) => {
                info!(
                    queue_id = ack.queue_id,
                    correlation_id = ack.correlation_id.get(),
                    status = ack.status,
                    guid = ?ack.message_guid.as_bytes(),
                    "ack"
                );
                acknowledgements.push(ack);
            }
            QueueEvent::Message(message) => {
                info!(
                    queue_id = message.queue_id,
                    sub_queue_id = message.sub_queue_id,
                    guid = ?message.message_guid.as_bytes(),
                    compression = ?message.compression,
                    schema_wire_id = message.schema_wire_id,
                    flags = format_args!("{:#x}", message.flags),
                    "push"
                );
                info!(payload = %String::from_utf8_lossy(&message.payload), "push payload");
                info!(
                    properties_old_style = message.properties_are_old_style,
                    rda = ?message.rda_info,
                    sub_queues = ?message.sub_queue_infos,
                    "push metadata"
                );
                if let Some(group_id) = &message.message_group_id {
                    info!(group_id = %group_id, "push group id");
                }
                for property in message.properties.iter() {
                    info!(name = %property.name, value = ?property.value, "push property");
                }
                deliveries.push(message);
            }
            QueueEvent::Suspended => warn!("queue suspended by host-health monitor"),
            QueueEvent::Resumed => info!("queue resumed by host-health monitor"),
            QueueEvent::Opened { queue_id } => info!(queue_id, "queue event opened"),
            QueueEvent::Reopened { queue_id } => info!(queue_id, "queue event reopened"),
            QueueEvent::Closed => return Err(blazox::Error::WriterClosed),
        }
    }

    let mut confirmer = session.load_confirm_builder(&queue);
    for message in &deliveries {
        confirmer.push_cookie(message.confirmation_cookie());
    }
    confirmer.send().await?;
    info!(count = deliveries.len(), "confirmed delivered messages");

    let close_status = queue.close_with_status().await?;
    info!(uri = %close_status.uri, "queue close status");
    session.stop().await?;
    session_task.abort();
    info!("session closed cleanly");
    Ok(())
}
