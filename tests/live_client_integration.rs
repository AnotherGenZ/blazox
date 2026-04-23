mod support;

use blazox::{
    AckMessage, Client, EventReceiver, OpenQueueOptions, OutboundPut, PushMessage,
    QueueHandleConfig, QueueStreamParameters, TransportEvent, queue_flags,
};
use std::time::Duration;
use support::{TestResult, blazox_timeout, disconnect_client, skip_unless_live};
use tracing::info;

#[tokio::test(flavor = "current_thread")]
async fn client_open_publish_push_confirm_close() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };

    let uri = config.unique_uri("client-round-trip")?;
    info!(%uri, "starting live client round-trip test");
    let producer = blazox_timeout(
        config.request_timeout,
        "producer Client::connect",
        Client::connect(&config.addr, config.client_config()),
    )
    .await?;
    info!("producer connected");
    let consumer = blazox_timeout(
        config.request_timeout,
        "consumer Client::connect",
        Client::connect(&config.addr, config.client_config()),
    )
    .await?;
    info!("consumer connected");

    let producer_handle = blazox_timeout(
        config.request_timeout,
        "producer.open_queue",
        producer.open_queue(uri.as_str(), producer_open_options()),
    )
    .await?;
    info!(queue_id = producer_handle.queue_id, "producer queue opened");
    let consumer_handle = blazox_timeout(
        config.request_timeout,
        "consumer.open_queue",
        consumer.open_queue(uri.as_str(), consumer_open_options()),
    )
    .await?;
    info!(queue_id = consumer_handle.queue_id, "consumer queue opened");
    let mut producer_events = producer_handle.subscribe();
    let mut consumer_events = consumer_handle.subscribe();

    blazox_timeout(
        config.request_timeout,
        "producer_handle.publish",
        producer_handle
            .publish(OutboundPut::new("hello from low-level client").with_correlation_id(7)),
    )
    .await?;
    info!("message published");

    let ack = next_client_ack(
        config.request_timeout,
        &mut producer_events,
        producer_handle.queue_id,
    )
    .await?;
    info!("producer acknowledgement received");
    assert_eq!(ack.correlation_id, 7);

    let push = next_client_push(
        config.request_timeout,
        &mut consumer_events,
        consumer_handle.queue_id,
    )
    .await?;
    info!("consumer push received");
    assert_eq!(
        String::from_utf8(push.payload.to_vec())?,
        "hello from low-level client"
    );
    blazox_timeout(
        config.request_timeout,
        "consumer_handle.confirm",
        consumer_handle.confirm(push.header.message_guid, 0),
    )
    .await?;
    info!("consumer confirmation sent");

    blazox_timeout(
        config.request_timeout,
        "producer_handle.close",
        producer_handle.close(true),
    )
    .await?;
    info!("producer queue closed");
    blazox_timeout(
        config.request_timeout,
        "consumer_handle.close",
        consumer_handle.close(true),
    )
    .await?;
    info!("consumer queue closed");
    disconnect_client(&config, &producer).await;
    disconnect_client(&config, &consumer).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn client_admin_help_round_trip() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.run_admin {
        return Ok(());
    }

    let client = blazox_timeout(
        config.request_timeout,
        "admin Client::connect",
        Client::connect(&config.addr, config.client_config()),
    )
    .await?;
    let response = client.admin_command("help").await?;
    assert!(!response.trim().is_empty());
    disconnect_client(&config, &client).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn client_anonymous_authentication_round_trip() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.enable_anonymous_auth {
        return Ok(());
    }

    let client = blazox_timeout(
        config.request_timeout,
        "auth Client::connect",
        Client::connect(&config.addr, config.client_config()),
    )
    .await?;
    let response = client.authenticate_anonymous().await?;
    assert!(response.status.is_success());
    disconnect_client(&config, &client).await;
    Ok(())
}

fn default_queue_stream_parameters() -> QueueStreamParameters {
    QueueStreamParameters {
        sub_id_info: None,
        max_unconfirmed_messages: 16,
        max_unconfirmed_bytes: 1 << 20,
        consumer_priority: 1,
        consumer_priority_count: 1,
    }
}

fn producer_open_options() -> OpenQueueOptions {
    OpenQueueOptions {
        handle: QueueHandleConfig {
            sub_id_info: None,
            flags: queue_flags::ack(queue_flags::write(0)),
            read_count: 0,
            write_count: 1,
            admin_count: 0,
        },
        configure_queue_stream: Some(default_queue_stream_parameters()),
        configure_stream: None,
    }
}

fn consumer_open_options() -> OpenQueueOptions {
    OpenQueueOptions {
        handle: QueueHandleConfig {
            sub_id_info: None,
            flags: queue_flags::ack(queue_flags::read(0)),
            read_count: 1,
            write_count: 0,
            admin_count: 0,
        },
        configure_queue_stream: Some(default_queue_stream_parameters()),
        configure_stream: None,
    }
}

async fn next_client_ack(
    duration: Duration,
    events: &mut EventReceiver<TransportEvent>,
    queue_id: u32,
) -> TestResult<AckMessage> {
    Ok(tokio::time::timeout(duration, async {
        loop {
            match events.recv().await {
                Ok(TransportEvent::Ack(messages)) => {
                    if let Some(message) = messages
                        .into_iter()
                        .find(|message| message.queue_id == queue_id)
                    {
                        return Ok(message);
                    }
                }
                Ok(_) => {}
                Err(blazox::EventRecvError::Closed) => {
                    return Err(blazox::Error::RequestCanceled);
                }
            }
        }
    })
    .await??)
}

async fn next_client_push(
    duration: Duration,
    events: &mut EventReceiver<TransportEvent>,
    queue_id: u32,
) -> TestResult<PushMessage> {
    Ok(tokio::time::timeout(duration, async {
        loop {
            match events.recv().await {
                Ok(TransportEvent::Push(messages)) => {
                    if let Some(message) = messages
                        .into_iter()
                        .find(|message| message.header.queue_id == queue_id)
                    {
                        return Ok(message);
                    }
                }
                Ok(_) => {}
                Err(blazox::EventRecvError::Closed) => {
                    return Err(blazox::Error::RequestCanceled);
                }
            }
        }
    })
    .await??)
}
