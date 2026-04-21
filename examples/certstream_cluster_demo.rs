use blazox::{
    CorrelationIdGenerator, MessageProperties, MessagePropertyValue, PostMessage, Queue,
    QueueFlags, QueueOptions, Session, SessionOptions, Uri, UriBuilder,
};
use futures_util::{SinkExt, StreamExt};
use rustls::crypto::ring::default_provider;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const SHARED_TOPICS: [TopicDefinition; 3] = [
    TopicDefinition {
        label: "ticker",
        queue: "coinbase.ticker",
    },
    TopicDefinition {
        label: "matches",
        queue: "coinbase.matches",
    },
    TopicDefinition {
        label: "heartbeat",
        queue: "coinbase.heartbeat",
    },
];

const PRIORITY_QUEUE: QueueDefinition = QueueDefinition {
    label: "priority",
    queue: "coinbase.priority",
};

const FANOUT_QUEUE: QueueDefinition = QueueDefinition {
    label: "fanout",
    queue: "coinbase.fanout",
};

#[derive(Debug, Clone, Copy)]
struct TopicDefinition {
    label: &'static str,
    queue: &'static str,
}

#[derive(Clone, Copy)]
struct QueueDefinition {
    label: &'static str,
    queue: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Publisher,
    Worker,
}

impl Role {
    fn from_env() -> blazox::Result<Self> {
        let raw = std::env::var("BLAZOX_ROLE").unwrap_or_else(|_| "publisher".to_string());
        match raw.to_ascii_lowercase().as_str() {
            "publisher" => Ok(Self::Publisher),
            "worker" => Ok(Self::Worker),
            other => Err(blazox::Error::ProtocolMessage(format!(
                "unsupported BLAZOX_ROLE '{other}', expected publisher or worker"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    role: Role,
    broker_addr: String,
    priority_domain: String,
    fanout_domain: String,
    market_data_url: String,
    product_ids: Vec<String>,
    publish_every_n: u64,
    fanout_app_id: Option<String>,
    worker_name: String,
    worker_processing_delay: Duration,
    feature_processing_delay: Duration,
    priority_consumer_priority: i32,
    priority_reconfigure_to: Option<i32>,
    priority_reconfigure_after: Option<Duration>,
    request_timeout: Duration,
}

impl Config {
    fn from_env() -> blazox::Result<Self> {
        let role = Role::from_env()?;
        let broker_addr =
            std::env::var("BLAZOX_BROKER_ADDR").unwrap_or_else(|_| "127.0.0.1:30114".to_string());
        let priority_domain = std::env::var("BLAZOX_PRIORITY_DOMAIN")
            .or_else(|_| std::env::var("BLAZOX_DOMAIN"))
            .unwrap_or_else(|_| "bmq.demo.persistent.priority".to_string());
        let fanout_domain = std::env::var("BLAZOX_FANOUT_DOMAIN")
            .unwrap_or_else(|_| "bmq.demo.persistent.fanout".to_string());
        let market_data_url = std::env::var("BLAZOX_MARKET_DATA_URL")
            .unwrap_or_else(|_| "wss://ws-feed.exchange.coinbase.com".to_string());
        let product_ids = env_list("BLAZOX_PRODUCT_IDS", &["BTC-USD", "ETH-USD", "SOL-USD"]);
        if product_ids.is_empty() {
            return Err(blazox::Error::ProtocolMessage(
                "BLAZOX_PRODUCT_IDS must include at least one product".to_string(),
            ));
        }
        let publish_every_n = env_u64("BLAZOX_PUBLISH_EVERY_N", 10);
        let fanout_app_id = std::env::var("BLAZOX_FANOUT_APP_ID")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let worker_name =
            std::env::var("BLAZOX_WORKER_NAME").unwrap_or_else(|_| "worker".to_string());
        let worker_processing_delay = Duration::from_millis(env_u64("BLAZOX_WORKER_DELAY_MS", 750));
        let feature_processing_delay =
            Duration::from_millis(env_u64("BLAZOX_FEATURE_DELAY_MS", 0));
        let priority_consumer_priority = env_i32("BLAZOX_PRIORITY_CONSUMER_PRIORITY", 1);
        let priority_reconfigure_to = env_optional_i32("BLAZOX_PRIORITY_RECONFIGURE_TO");
        let priority_reconfigure_after =
            env_optional_u64("BLAZOX_PRIORITY_RECONFIGURE_AFTER_MS").map(Duration::from_millis);
        let request_timeout = Duration::from_millis(env_u64("BLAZOX_REQUEST_TIMEOUT_MS", 10_000));

        Ok(Self {
            role,
            broker_addr,
            priority_domain,
            fanout_domain,
            market_data_url,
            product_ids,
            publish_every_n,
            fanout_app_id,
            worker_name,
            worker_processing_delay,
            feature_processing_delay,
            priority_consumer_priority,
            priority_reconfigure_to,
            priority_reconfigure_after,
            request_timeout,
        })
    }

    fn queue_uri(&self, domain: &str, queue: &str) -> blazox::Result<Uri> {
        UriBuilder::new()
            .domain(domain.to_string())
            .queue(queue)
            .build()
    }

    fn priority_queue_uri(&self, queue: &str) -> blazox::Result<Uri> {
        self.queue_uri(&self.priority_domain, queue)
    }

    fn fanout_queue_uri(&self, queue: &str) -> blazox::Result<Uri> {
        self.queue_uri(&self.fanout_domain, queue)
    }

    fn fanout_app_queue_uri(&self, queue: &str, app_id: &str) -> blazox::Result<Uri> {
        let base_uri = self.fanout_queue_uri(queue)?;
        Uri::parse(format!("{}?id={app_id}", base_uri.as_str()))
    }

    fn websocket_url(&self) -> String {
        if let Some(rest) = self.market_data_url.strip_prefix("https://") {
            format!("wss://{rest}")
        } else if let Some(rest) = self.market_data_url.strip_prefix("http://") {
            format!("ws://{rest}")
        } else {
            self.market_data_url.clone()
        }
    }
}

#[derive(Debug, Clone)]
struct MarketEvent {
    shared_topic: TopicDefinition,
    message_type: String,
    product_id: String,
    sequence: Option<u64>,
    time: Option<String>,
    summary: String,
    payload: Value,
}

impl MarketEvent {
    fn routed(&self, pipeline: &'static str, topic: &'static str) -> RoutedMarketEvent {
        RoutedMarketEvent {
            pipeline: pipeline.to_string(),
            topic: topic.to_string(),
            message_type: self.message_type.clone(),
            product_id: self.product_id.clone(),
            sequence: self.sequence,
            time: self.time.clone(),
            summary: self.summary.clone(),
            payload: self.payload.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RoutedMarketEvent {
    pipeline: String,
    topic: String,
    message_type: String,
    product_id: String,
    sequence: Option<u64>,
    time: Option<String>,
    summary: String,
    payload: Value,
}

struct PublisherQueues {
    shared: Vec<(TopicDefinition, Queue)>,
    priority: Queue,
    fanout: Queue,
}

struct WorkerQueues {
    shared: Vec<(TopicDefinition, Queue)>,
    priority: Queue,
    fanout: Queue,
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init();
}

fn init_tls_crypto_provider() {
    let _ = default_provider().install_default();
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_i32(key: &str, default: i32) -> i32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(default)
}

fn env_optional_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

fn env_optional_i32(key: &str) -> Option<i32> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
}

fn env_list(key: &str, default: &[&str]) -> Vec<String> {
    std::env::var(key)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_else(|| default.iter().map(|value| (*value).to_string()).collect())
}

fn writer_queue_options() -> QueueOptions {
    QueueOptions::writer().flags(QueueFlags::WRITE | QueueFlags::ACK)
}

fn base_reader_queue_options() -> QueueOptions {
    QueueOptions::reader()
        .consumer_priority(1)
        .consumer_priority_count(1)
        .max_unconfirmed_messages(8)
        .max_unconfirmed_bytes(1 << 20)
}

fn priority_reader_queue_options(priority: i32) -> QueueOptions {
    base_reader_queue_options().consumer_priority(priority)
}

fn fanout_route_id(topic: &str) -> i64 {
    match topic {
        "ticker" => 1,
        "matches" => 2,
        "heartbeat" => 3,
        _ => 0,
    }
}

fn build_properties(event: &RoutedMarketEvent) -> MessageProperties {
    let mut properties = MessageProperties::new();
    properties.insert(
        "pipeline",
        MessagePropertyValue::String(event.pipeline.clone()),
    );
    properties.insert("topic", MessagePropertyValue::String(event.topic.clone()));
    properties.insert(
        "product_id",
        MessagePropertyValue::String(event.product_id.clone()),
    );
    properties.insert(
        "message_type",
        MessagePropertyValue::String(event.message_type.clone()),
    );
    properties.insert(
        "fanout_route",
        MessagePropertyValue::Int64(fanout_route_id(&event.topic)),
    );
    if let Some(sequence) = event.sequence {
        properties.insert("sequence", MessagePropertyValue::Int64(sequence as i64));
    }
    properties
}

fn string_field(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn u64_field(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str()?.parse::<u64>().ok())
    })
}

fn classify_shared_topic(message_type: &str) -> Option<TopicDefinition> {
    match message_type {
        "ticker" => Some(SHARED_TOPICS[0]),
        "match" | "last_match" => Some(SHARED_TOPICS[1]),
        "heartbeat" => Some(SHARED_TOPICS[2]),
        _ => None,
    }
}

fn summarize_event(message_type: &str, payload: &Value, product_id: &str) -> String {
    match message_type {
        "ticker" => {
            let price = string_field(payload, "price").unwrap_or_else(|| "n/a".to_string());
            let best_bid = string_field(payload, "best_bid").unwrap_or_else(|| "n/a".to_string());
            let best_ask = string_field(payload, "best_ask").unwrap_or_else(|| "n/a".to_string());
            format!("{product_id} price={price} bid={best_bid} ask={best_ask}")
        }
        "match" | "last_match" => {
            let side = string_field(payload, "side").unwrap_or_else(|| "n/a".to_string());
            let price = string_field(payload, "price").unwrap_or_else(|| "n/a".to_string());
            let size = string_field(payload, "size").unwrap_or_else(|| "n/a".to_string());
            format!("{product_id} side={side} size={size} price={price}")
        }
        "heartbeat" => {
            let last_trade_id = u64_field(payload, "last_trade_id").unwrap_or_default();
            format!("{product_id} heartbeat last_trade_id={last_trade_id}")
        }
        other => format!("{product_id} type={other}"),
    }
}

async fn connect_session_with_retry(config: &Config) -> Session {
    let options = SessionOptions::default()
        .broker_addr(config.broker_addr.clone())
        .request_timeout(config.request_timeout)
        .connect_timeout(config.request_timeout)
        .open_queue_timeout(config.request_timeout)
        .configure_queue_timeout(config.request_timeout)
        .close_queue_timeout(config.request_timeout)
        .disconnect_timeout(Duration::from_secs(5))
        .process_name_override("blazox-coinbase-compose")
        .user_agent("blazox/examples/coinbase_market_data_cluster_demo");
    let mut attempt = 0_u64;

    loop {
        attempt += 1;
        match Session::connect(options.clone()).await {
            Ok(session) => {
                info!(attempt, broker = %config.broker_addr, "session connected");
                return session;
            }
            Err(err) => {
                warn!(
                    attempt,
                    broker = %config.broker_addr,
                    error = %err,
                    "session connect failed; retrying"
                );
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

fn spawn_ack_listener(
    queue: Queue,
    pipeline: &'static str,
    queue_label: &'static str,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match queue.next_ack().await {
                Ok(ack) => {
                    info!(
                        pipeline,
                        queue = queue_label,
                        correlation_id = ack.correlation_id.get(),
                        status = ack.status,
                        "producer ack"
                    );
                }
                Err(err) => {
                    warn!(pipeline, queue = queue_label, error = %err, "producer ack listener stopped");
                    break;
                }
            }
        }
    })
}

async fn open_publisher_queues(
    config: &Config,
    session: &Session,
) -> blazox::Result<PublisherQueues> {
    let mut shared = Vec::with_capacity(SHARED_TOPICS.len());
    for topic in SHARED_TOPICS {
        let uri = config.priority_queue_uri(topic.queue)?;
        let queue = session
            .open_queue(uri.as_str(), writer_queue_options())
            .await?;
        info!(pipeline = "work-queue", topic = topic.label, uri = %uri, "writer queue opened");
        spawn_ack_listener(queue.clone(), "work-queue", topic.label);
        shared.push((topic, queue));
    }

    let priority_uri = config.priority_queue_uri(PRIORITY_QUEUE.queue)?;
    let priority = session
        .open_queue(priority_uri.as_str(), writer_queue_options())
        .await?;
    info!(
        pipeline = "consumer-priority",
        queue = PRIORITY_QUEUE.label,
        uri = %priority_uri,
        "writer queue opened"
    );
    spawn_ack_listener(priority.clone(), "consumer-priority", PRIORITY_QUEUE.label);

    let fanout_uri = config.fanout_queue_uri(FANOUT_QUEUE.queue)?;
    let fanout = session
        .open_queue(fanout_uri.as_str(), writer_queue_options())
        .await?;
    info!(
        pipeline = "fanout",
        queue = FANOUT_QUEUE.label,
        uri = %fanout_uri,
        "writer queue opened"
    );
    spawn_ack_listener(fanout.clone(), "fanout", FANOUT_QUEUE.label);

    Ok(PublisherQueues {
        shared,
        priority,
        fanout,
    })
}

async fn open_shared_reader_queues(
    config: &Config,
    session: &Session,
) -> blazox::Result<Vec<(TopicDefinition, Queue)>> {
    let mut queues = Vec::with_capacity(SHARED_TOPICS.len());
    for topic in SHARED_TOPICS {
        let uri = config.priority_queue_uri(topic.queue)?;
        let queue = session
            .open_queue(uri.as_str(), base_reader_queue_options())
            .await?;
        info!(pipeline = "work-queue", topic = topic.label, uri = %uri, "reader queue opened");
        queues.push((topic, queue));
    }
    Ok(queues)
}

async fn open_priority_reader_queue(config: &Config, session: &Session) -> blazox::Result<Queue> {
    let uri = config.priority_queue_uri(PRIORITY_QUEUE.queue)?;
    let options = priority_reader_queue_options(config.priority_consumer_priority);
    let queue = session.open_queue(uri.as_str(), options).await?;
    info!(
        pipeline = "consumer-priority",
        worker = %config.worker_name,
        queue = PRIORITY_QUEUE.label,
        consumer_priority = config.priority_consumer_priority,
        uri = %uri,
        "priority reader queue opened"
    );
    Ok(queue)
}

async fn open_fanout_reader_queue(config: &Config, session: &Session) -> blazox::Result<Queue> {
    let Some(app_id) = config.fanout_app_id.as_deref() else {
        return Err(blazox::Error::ProtocolMessage(
            "BLAZOX_FANOUT_APP_ID must be set for worker fanout readers".to_string(),
        ));
    };
    match app_id {
        "foo" | "bar" => {}
        other => {
            return Err(blazox::Error::ProtocolMessage(format!(
                "unsupported fanout app id '{other}', expected foo or bar"
            )));
        }
    }
    let uri = config.fanout_app_queue_uri(FANOUT_QUEUE.queue, app_id)?;
    let queue = session
        .open_queue(uri.as_str(), base_reader_queue_options().app_id(app_id))
        .await?;
    info!(
        pipeline = "fanout",
        worker = %config.worker_name,
        queue = FANOUT_QUEUE.label,
        app_id,
        uri = %uri,
        "fanout reader queue opened"
    );
    Ok(queue)
}

async fn connect_publisher_with_retry(config: &Config) -> (Session, PublisherQueues) {
    loop {
        let session = connect_session_with_retry(config).await;
        match open_publisher_queues(config, &session).await {
            Ok(queues) => return (session, queues),
            Err(err) => {
                warn!(
                    broker = %config.broker_addr,
                    error = %err,
                    "publisher queue setup failed; retrying"
                );
                let _ = session.disconnect().await;
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn connect_worker_with_retry(config: &Config) -> (Session, WorkerQueues) {
    loop {
        let session = connect_session_with_retry(config).await;
        let shared = match open_shared_reader_queues(config, &session).await {
            Ok(queues) => queues,
            Err(err) => {
                warn!(
                    broker = %config.broker_addr,
                    worker = %config.worker_name,
                    error = %err,
                    "shared reader queue setup failed; retrying"
                );
                let _ = session.disconnect().await;
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        let priority = match open_priority_reader_queue(config, &session).await {
            Ok(queue) => queue,
            Err(err) => {
                warn!(
                    broker = %config.broker_addr,
                    worker = %config.worker_name,
                    error = %err,
                    "priority reader queue setup failed; retrying"
                );
                let _ = session.disconnect().await;
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        let fanout = match open_fanout_reader_queue(config, &session).await {
            Ok(queue) => queue,
            Err(err) => {
                warn!(
                    broker = %config.broker_addr,
                    worker = %config.worker_name,
                    error = %err,
                    "fanout reader queue setup failed; retrying"
                );
                let _ = session.disconnect().await;
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        return (
            session,
            WorkerQueues {
                shared,
                priority,
                fanout,
            },
        );
    }
}

fn decode_market_event(payload: Value) -> blazox::Result<Option<MarketEvent>> {
    let Some(message_type) = payload.get("type").and_then(Value::as_str) else {
        return Err(blazox::Error::ProtocolMessage(
            "Coinbase payload missing type".to_string(),
        ));
    };

    match message_type {
        "subscriptions" => {
            info!(payload = %payload, "received Coinbase subscriptions confirmation");
            return Ok(None);
        }
        "error" => {
            warn!(payload = %payload, "received Coinbase websocket error");
            return Ok(None);
        }
        _ => {}
    }

    let Some(shared_topic) = classify_shared_topic(message_type) else {
        info!(message_type, "ignoring unsupported Coinbase message");
        return Ok(None);
    };

    let product_id = string_field(&payload, "product_id").unwrap_or_else(|| "unknown".to_string());
    let sequence = u64_field(&payload, "sequence");
    let time = string_field(&payload, "time");
    let summary = summarize_event(message_type, &payload, &product_id);

    Ok(Some(MarketEvent {
        shared_topic,
        message_type: message_type.to_string(),
        product_id,
        sequence,
        time,
        summary,
        payload,
    }))
}

async fn post_market_event(
    queue: &Queue,
    event: &RoutedMarketEvent,
    correlation_ids: &CorrelationIdGenerator,
) -> blazox::Result<()> {
    let payload = serde_json::to_string(event)
        .map_err(|err| blazox::Error::ProtocolMessage(err.to_string()))?;
    let properties = build_properties(event);
    queue
        .post(
            PostMessage::new(payload)
                .properties(properties)
                .correlation_id(correlation_ids.next()),
        )
        .await?;
    info!(
        pipeline = event.pipeline,
        topic = event.topic,
        product_id = event.product_id,
        message_type = event.message_type,
        summary = event.summary,
        "published market event"
    );
    Ok(())
}

async fn run_publisher(config: Config) -> blazox::Result<()> {
    let (_session, queues) = connect_publisher_with_retry(&config).await;
    let websocket_url = config.websocket_url();
    let sample_every = config.publish_every_n.max(1);
    let correlation_ids = CorrelationIdGenerator::default();
    let mut seen_market_events = 0_u64;
    let mut connect_attempt = 0_u64;

    loop {
        connect_attempt += 1;
        info!(
            attempt = connect_attempt,
            url = %websocket_url,
            products = ?config.product_ids,
            "connecting to Coinbase websocket"
        );

        let (mut websocket, _) = match connect_async(websocket_url.as_str()).await {
            Ok(stream) => stream,
            Err(err) => {
                warn!(
                    attempt = connect_attempt,
                    url = %websocket_url,
                    error = %err,
                    "failed to connect to Coinbase websocket; retrying"
                );
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        let subscribe_payload = json!({
            "type": "subscribe",
            "product_ids": config.product_ids,
            "channels": ["ticker", "matches", "heartbeat"],
        })
        .to_string();

        if let Err(err) = websocket
            .send(Message::Text(subscribe_payload.into()))
            .await
        {
            warn!(error = %err, "failed to send Coinbase subscribe message");
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        info!(
            attempt = connect_attempt,
            url = %websocket_url,
            "connected to Coinbase websocket and sent subscribe"
        );

        loop {
            match websocket.next().await {
                Some(Ok(Message::Text(text))) => {
                    if let Err(err) = publish_sampled_payload(
                        &queues,
                        text.as_bytes(),
                        sample_every,
                        &correlation_ids,
                        &mut seen_market_events,
                    )
                    .await
                    {
                        warn!(error = %err, "failed to publish Coinbase websocket payload");
                    }
                }
                Some(Ok(Message::Binary(payload))) => {
                    if let Err(err) = publish_sampled_payload(
                        &queues,
                        payload.as_ref(),
                        sample_every,
                        &correlation_ids,
                        &mut seen_market_events,
                    )
                    .await
                    {
                        warn!(error = %err, "failed to publish Coinbase websocket binary payload");
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    if let Err(err) = websocket.send(Message::Pong(payload)).await {
                        warn!(error = %err, "failed to respond to Coinbase ping");
                        break;
                    }
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Frame(_))) => {}
                Some(Ok(Message::Close(frame))) => {
                    info!(?frame, "Coinbase websocket closed; reconnecting");
                    break;
                }
                Some(Err(err)) => {
                    warn!(error = %err, "Coinbase websocket read failed; reconnecting");
                    break;
                }
                None => {
                    warn!("Coinbase websocket ended; reconnecting");
                    break;
                }
            }
        }

        sleep(Duration::from_secs(2)).await;
    }
}

async fn publish_sampled_payload(
    queues: &PublisherQueues,
    payload: &[u8],
    sample_every: u64,
    correlation_ids: &CorrelationIdGenerator,
    seen_market_events: &mut u64,
) -> blazox::Result<()> {
    let message = serde_json::from_slice::<Value>(payload)
        .map_err(|err| blazox::Error::ProtocolMessage(err.to_string()))?;
    publish_sampled_event(
        queues,
        message,
        sample_every,
        correlation_ids,
        seen_market_events,
    )
    .await
}

async fn publish_sampled_event(
    queues: &PublisherQueues,
    message: Value,
    sample_every: u64,
    correlation_ids: &CorrelationIdGenerator,
    seen_market_events: &mut u64,
) -> blazox::Result<()> {
    let Some(event) = decode_market_event(message)? else {
        return Ok(());
    };

    *seen_market_events += 1;
    if *seen_market_events <= 5 || *seen_market_events % 25 == 0 {
        info!(
            seen_market_events = *seen_market_events,
            shared_topic = event.shared_topic.label,
            product_id = %event.product_id,
            message_type = %event.message_type,
            summary = %event.summary,
            "decoded Coinbase market event"
        );
    }

    let sampled = *seen_market_events % sample_every == 0;

    if event.message_type == "heartbeat" || sampled {
        let shared_event = event.routed("work-queue", event.shared_topic.label);
        let Some((_, queue)) = queues
            .shared
            .iter()
            .find(|(candidate, _)| candidate.label == event.shared_topic.label)
        else {
            return Err(blazox::Error::ProtocolMessage(format!(
                "no shared queue configured for topic {}",
                event.shared_topic.label
            )));
        };
        post_market_event(queue, &shared_event, correlation_ids).await?;

        let fanout_event = event.routed("fanout", event.shared_topic.label);
        post_market_event(&queues.fanout, &fanout_event, correlation_ids).await?;
    }

    if sampled {
        let priority_event = event.routed("consumer-priority", PRIORITY_QUEUE.label);
        post_market_event(&queues.priority, &priority_event, correlation_ids).await?;
    }

    Ok(())
}

fn spawn_reader_task(
    worker_name: String,
    broker_addr: String,
    pipeline: &'static str,
    queue_label: &'static str,
    queue: Queue,
    delay: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match queue.next_message().await {
                Ok(message) => {
                    let cookie = message.confirmation_cookie();
                    match serde_json::from_slice::<RoutedMarketEvent>(&message.payload) {
                        Ok(event) => {
                            info!(
                                worker = %worker_name,
                                broker = %broker_addr,
                                pipeline,
                                queue = queue_label,
                                topic = %event.topic,
                                message_type = %event.message_type,
                                product_id = %event.product_id,
                                summary = %event.summary,
                                "processed queued market event"
                            );
                        }
                        Err(err) => {
                            warn!(
                                worker = %worker_name,
                                broker = %broker_addr,
                                pipeline,
                                queue = queue_label,
                                error = %err,
                                "failed to decode queued message"
                            );
                        }
                    }

                    if !delay.is_zero() {
                        sleep(delay).await;
                    }

                    if let Err(err) = queue.confirm_cookie(cookie).await {
                        warn!(
                            worker = %worker_name,
                            broker = %broker_addr,
                            pipeline,
                            queue = queue_label,
                            error = %err,
                            "message confirm failed"
                        );
                    }
                }
                Err(err) => {
                    let err_text = err.to_string();
                    if err_text.contains("timed out waiting for broker response") {
                        info!(
                            worker = %worker_name,
                            broker = %broker_addr,
                            pipeline,
                            queue = queue_label,
                            "queue idle; waiting for next message"
                        );
                    } else {
                        warn!(
                            worker = %worker_name,
                            broker = %broker_addr,
                            pipeline,
                            queue = queue_label,
                            error = %err_text,
                            "queue receive failed; retrying"
                        );
                    }
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
}

fn spawn_priority_reconfigure_task(
    worker_name: String,
    broker_addr: String,
    queue: Queue,
    after: Duration,
    new_priority: i32,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        sleep(after).await;
        let options = priority_reader_queue_options(new_priority);
        match queue.reconfigure(options).await {
            Ok(()) => {
                info!(
                    worker = %worker_name,
                    broker = %broker_addr,
                    pipeline = "consumer-priority",
                    queue = PRIORITY_QUEUE.label,
                    consumer_priority = new_priority,
                    "reconfigured priority consumer"
                );
            }
            Err(err) => {
                warn!(
                    worker = %worker_name,
                    broker = %broker_addr,
                    pipeline = "consumer-priority",
                    queue = PRIORITY_QUEUE.label,
                    consumer_priority = new_priority,
                    error = %err,
                    "priority consumer reconfigure failed"
                );
            }
        }
    })
}

async fn run_worker(config: Config) -> blazox::Result<()> {
    let (_session, queues) = connect_worker_with_retry(&config).await;
    let mut handles = Vec::with_capacity(queues.shared.len() + 3);

    for (topic, queue) in queues.shared {
        handles.push(spawn_reader_task(
            config.worker_name.clone(),
            config.broker_addr.clone(),
            "work-queue",
            topic.label,
            queue,
            config.worker_processing_delay,
        ));
    }

    handles.push(spawn_reader_task(
        config.worker_name.clone(),
        config.broker_addr.clone(),
        "consumer-priority",
        PRIORITY_QUEUE.label,
        queues.priority.clone(),
        config.feature_processing_delay,
    ));

    handles.push(spawn_reader_task(
        config.worker_name.clone(),
        config.broker_addr.clone(),
        "fanout",
        FANOUT_QUEUE.label,
        queues.fanout,
        config.feature_processing_delay,
    ));

    if let (Some(after), Some(new_priority)) = (
        config.priority_reconfigure_after,
        config.priority_reconfigure_to,
    ) {
        handles.push(spawn_priority_reconfigure_task(
            config.worker_name.clone(),
            config.broker_addr.clone(),
            queues.priority,
            after,
            new_priority,
        ));
    }

    for handle in handles {
        match handle.await {
            Ok(()) => {}
            Err(err) => error!(error = %err, "worker task crashed"),
        }
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> blazox::Result<()> {
    init_tracing();
    init_tls_crypto_provider();

    let config = Config::from_env()?;
    match config.role {
        Role::Publisher => run_publisher(config).await,
        Role::Worker => run_worker(config).await,
    }
}
