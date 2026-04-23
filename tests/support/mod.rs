#![allow(dead_code)]

use blazox::{
    Acknowledgement, Client, ClientConfig, Error, EventReceiver, Expression, ExpressionVersion,
    ManualHostHealthMonitor, MessageProperties, MessagePropertyValue, Queue, QueueEvent,
    QueueFlags, QueueOptions, ReceivedMessage, Session, SessionOptions, Subscription, Uri,
    UriBuilder,
};
use std::error::Error as StdError;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

pub type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

static NEXT_QUEUE_ID: AtomicU64 = AtomicU64::new(1);
static INIT_TRACING: Once = Once::new();

#[derive(Debug, Clone)]
pub struct LiveBrokerConfig {
    pub addr: String,
    pub priority_domain: String,
    pub fanout_domain: String,
    pub broadcast_domain: String,
    pub queue_prefix: String,
    pub request_timeout: Duration,
    pub cleanup_timeout: Duration,
    pub run_admin: bool,
    pub enable_anonymous_auth: bool,
    pub enable_subscriptions: bool,
    pub enable_fanout: bool,
    pub enable_broadcast: bool,
}

impl LiveBrokerConfig {
    pub fn from_env() -> Option<Self> {
        if !env_flag("BLAZOX_RUN_LIVE_TESTS") {
            return None;
        }

        Some(Self {
            addr: env_string("BLAZOX_TEST_ADDR").unwrap_or_else(|| "127.0.0.1:30114".to_string()),
            priority_domain: env_string("BLAZOX_TEST_PRIORITY_DOMAIN")
                .or_else(|| env_string("BLAZOX_TEST_DOMAIN"))
                .unwrap_or_else(|| "bmq.test.mem.priority".to_string()),
            fanout_domain: env_string("BLAZOX_TEST_FANOUT_DOMAIN")
                .unwrap_or_else(|| "bmq.test.mmap.fanout".to_string()),
            broadcast_domain: env_string("BLAZOX_TEST_BROADCAST_DOMAIN")
                .unwrap_or_else(|| "bmq.test.mem.broadcast".to_string()),
            queue_prefix: env_string("BLAZOX_TEST_QUEUE_PREFIX")
                .unwrap_or_else(|| "blazox-it".to_string()),
            request_timeout: Duration::from_millis(
                env_u64("BLAZOX_TEST_REQUEST_TIMEOUT_MS").unwrap_or(3_000),
            ),
            cleanup_timeout: Duration::from_millis(
                env_u64("BLAZOX_TEST_CLEANUP_TIMEOUT_MS").unwrap_or(500),
            ),
            run_admin: env_flag("BLAZOX_TEST_ENABLE_ADMIN"),
            enable_anonymous_auth: env_flag("BLAZOX_TEST_ENABLE_ANONYMOUS_AUTH"),
            enable_subscriptions: env_flag("BLAZOX_TEST_ENABLE_SUBSCRIPTIONS"),
            enable_fanout: env_flag("BLAZOX_TEST_ENABLE_FANOUT"),
            enable_broadcast: env_flag("BLAZOX_TEST_ENABLE_BROADCAST"),
        })
    }

    pub fn unique_uri(&self, label: &str) -> blazox::Result<Uri> {
        self.unique_uri_in_domain(&self.priority_domain, label)
    }

    pub fn unique_fanout_uri(&self, label: &str) -> blazox::Result<Uri> {
        self.unique_uri_in_domain(&self.fanout_domain, label)
    }

    pub fn unique_broadcast_uri(&self, label: &str) -> blazox::Result<Uri> {
        self.unique_uri_in_domain(&self.broadcast_domain, label)
    }

    pub fn unique_uri_in_domain(&self, domain: &str, label: &str) -> blazox::Result<Uri> {
        let unique = NEXT_QUEUE_ID.fetch_add(1, Ordering::Relaxed);
        UriBuilder::new()
            .domain(domain)
            .queue(format!("{}-{}-{}", self.queue_prefix, label, unique))
            .build()
    }

    pub fn session_options(&self) -> SessionOptions {
        SessionOptions::default()
            .broker_addr(self.addr.clone())
            .request_timeout(self.request_timeout)
            .connect_timeout(self.request_timeout)
            .open_queue_timeout(self.request_timeout)
            .configure_queue_timeout(self.request_timeout)
            .close_queue_timeout(self.request_timeout)
            .disconnect_timeout(self.request_timeout)
            .linger_timeout(self.request_timeout)
    }

    pub fn client_config(&self) -> ClientConfig {
        ClientConfig {
            request_timeout: self.request_timeout,
            open_queue_timeout: self.request_timeout,
            configure_queue_timeout: self.request_timeout,
            close_queue_timeout: self.request_timeout,
            disconnect_timeout: self.request_timeout,
            channel_write_timeout: self.request_timeout,
            ..ClientConfig::default()
        }
    }

    pub fn session_options_with_monitor(
        &self,
        monitor: Arc<ManualHostHealthMonitor>,
    ) -> SessionOptions {
        self.session_options().host_health_monitor(monitor)
    }
}

pub async fn connect_session(config: &LiveBrokerConfig) -> TestResult<Session> {
    Ok(Session::connect(config.session_options()).await?)
}

pub async fn connect_session_with_monitor(
    config: &LiveBrokerConfig,
    monitor: Arc<ManualHostHealthMonitor>,
) -> TestResult<Session> {
    Ok(Session::connect(config.session_options_with_monitor(monitor)).await?)
}

pub async fn disconnect_sessions(config: &LiveBrokerConfig, sessions: &[&Session]) {
    for session in sessions {
        let _ = timeout(config.cleanup_timeout, session.disconnect()).await;
    }
}

pub fn acking_writer_options() -> QueueOptions {
    QueueOptions::writer().flags(QueueFlags::WRITE | QueueFlags::ACK)
}

pub fn reader_options() -> QueueOptions {
    QueueOptions::reader()
        .max_unconfirmed_messages(16)
        .max_unconfirmed_bytes(1 << 20)
        .consumer_priority(1)
        .consumer_priority_count(1)
}

pub fn int_subscription(id: u32, expression: &str) -> Subscription {
    Subscription {
        s_id: id,
        expression: Expression {
            version: ExpressionVersion::Undefined,
            text: expression.to_string(),
        },
        consumers: Vec::new(),
    }
}

pub fn int_property(name: &str, value: i32) -> MessageProperties {
    let mut properties = MessageProperties::new();
    properties.insert(name, MessagePropertyValue::Int32(value));
    properties
}

pub fn string_and_int_properties(kind: &str, attempt: i32) -> MessageProperties {
    let mut properties = MessageProperties::new();
    properties.insert("kind", MessagePropertyValue::String(kind.to_string()));
    properties.insert("attempt", MessagePropertyValue::Int32(attempt));
    properties
}

pub async fn recv_and_confirm(queue: &Queue, count: usize) -> TestResult<Vec<String>> {
    let mut events = queue.events();
    recv_and_confirm_with_events(queue, &mut events, count).await
}

pub async fn assert_no_message(queue: &Queue, window: Duration) -> TestResult {
    let mut events = queue.events();
    assert_no_message_on_events(&mut events, window).await
}

pub async fn assert_no_message_on_events(
    events: &mut EventReceiver<QueueEvent>,
    window: Duration,
) -> TestResult {
    match timeout(window, next_queue_message(events)).await {
        Err(_) => Ok(()),
        Ok(Err(Error::Timeout)) => Ok(()),
        Ok(Ok(message)) => Err(format!(
            "unexpected message within {:?}: {:?}",
            window, message.payload
        )
        .into()),
        Ok(Err(error)) => Err(Box::new(error)),
    }
}

pub async fn recv_and_confirm_with_events(
    queue: &Queue,
    events: &mut EventReceiver<QueueEvent>,
    count: usize,
) -> TestResult<Vec<String>> {
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        let message = next_queue_message(events).await?;
        payloads.push(String::from_utf8(message.payload.to_vec())?);
        queue.confirm_cookie(message.confirmation_cookie()).await?;
    }
    Ok(payloads)
}

pub async fn next_queue_message(
    events: &mut EventReceiver<QueueEvent>,
) -> Result<ReceivedMessage, Error> {
    loop {
        match events.recv().await {
            Ok(QueueEvent::Message(message)) => return Ok(message),
            Ok(_) => continue,
            Err(blazox::EventRecvError::Closed) => {
                return Err(Error::RequestCanceled);
            }
        }
    }
}

pub async fn next_queue_ack(
    events: &mut EventReceiver<QueueEvent>,
) -> Result<Acknowledgement, Error> {
    loop {
        match events.recv().await {
            Ok(QueueEvent::Ack(ack)) => return Ok(ack),
            Ok(_) => continue,
            Err(blazox::EventRecvError::Closed) => {
                return Err(Error::RequestCanceled);
            }
        }
    }
}

pub async fn disconnect_client(config: &LiveBrokerConfig, client: &Client) {
    let _ = timeout(config.cleanup_timeout, client.disconnect()).await;
}

pub async fn blazox_timeout<T>(
    duration: Duration,
    label: &str,
    future: impl Future<Output = blazox::Result<T>>,
) -> TestResult<T> {
    match timeout(duration, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(Box::new(error)),
        Err(_) => Err(format!("{label} timed out after {duration:?}").into()),
    }
}

pub fn skip_unless_live() -> Option<LiveBrokerConfig> {
    let config = LiveBrokerConfig::from_env()?;
    init_test_tracing();
    Some(config)
}

pub fn init_test_tracing() {
    INIT_TRACING.call_once(|| {
        let filter = EnvFilter::try_from_default_env()
            .or_else(|_| {
                let value = std::env::var("BLAZOX_TEST_LOG")
                    .unwrap_or_else(|_| "blazox=debug,info".to_string());
                EnvFilter::try_new(value)
            })
            .unwrap_or_else(|_| EnvFilter::new("info"));

        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_test_writer()
            .try_init();
    });
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}
