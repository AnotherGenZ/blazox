mod support;

use blazox::{
    CompressionAlgorithm, CorrelationIdGenerator, ManualHostHealthMonitor, MessagePropertyValue,
    EventReceiver, PostMessage, QueueEvent, QueueOptions, Session,
};
use std::sync::Arc;
use std::time::Duration;
use support::{
    TestResult, acking_writer_options, assert_no_message_on_events, blazox_timeout,
    connect_session, connect_session_with_monitor, disconnect_sessions, int_property,
    int_subscription, next_queue_ack, next_queue_message, reader_options,
    recv_and_confirm_with_events, skip_unless_live, string_and_int_properties,
};
use tokio::time::timeout;

#[tokio::test(flavor = "current_thread")]
async fn session_round_trip_ack_confirm_and_lookup() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };

    let uri = config.unique_uri("round-trip")?;
    let producer_session = connect_session(&config).await?;
    let consumer_session = connect_session(&config).await?;

    let producer = producer_session
        .open_queue(uri.as_str(), acking_writer_options())
        .await?;
    let consumer = consumer_session
        .open_queue(uri.as_str(), reader_options())
        .await?;
    let mut producer_events = producer.events();
    let mut consumer_events = consumer.events();

    let correlation_ids = CorrelationIdGenerator::default();
    let correlation_id = correlation_ids.next();
    producer
        .post(PostMessage::new("hello from rust").correlation_id(correlation_id))
        .await?;

    let ack = blazox_timeout(
        config.request_timeout,
        "producer queue ack",
        next_queue_ack(&mut producer_events),
    )
    .await?;
    assert_eq!(ack.correlation_id, correlation_id);

    let message = blazox_timeout(
        config.request_timeout,
        "consumer queue message",
        next_queue_message(&mut consumer_events),
    )
    .await?;
    assert_eq!(
        String::from_utf8(message.payload.to_vec())?,
        "hello from rust"
    );
    consumer
        .confirm_cookie(message.confirmation_cookie())
        .await?;

    let queue_id = producer_session.get_queue_id(uri.as_str()).await;
    assert_eq!(queue_id, producer.queue_id());
    assert!(producer_session.get_queue(uri.as_str()).await.is_some());

    blazox_timeout(config.request_timeout, "producer.close", producer.close()).await?;
    assert!(producer_session.get_queue(uri.as_str()).await.is_none());

    disconnect_sessions(&config, &[&producer_session, &consumer_session]).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_compression_and_properties_round_trip() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };

    let uri = config.unique_uri("compression")?;
    let producer_session = connect_session(&config).await?;
    let consumer_session = connect_session(&config).await?;

    let producer = producer_session
        .open_queue(uri.as_str(), QueueOptions::writer())
        .await?;
    let consumer = consumer_session
        .open_queue(uri.as_str(), reader_options())
        .await?;
    let mut consumer_events = consumer.events();

    let payload = "compressed-".repeat(500);
    producer
        .post(
            PostMessage::new(payload.clone())
                .properties(string_and_int_properties("compressed", 7))
                .compression(CompressionAlgorithm::Zlib),
        )
        .await?;

    let message = blazox_timeout(
        config.request_timeout,
        "compression consumer message",
        next_queue_message(&mut consumer_events),
    )
    .await?;
    assert_eq!(String::from_utf8(message.payload.to_vec())?, payload);
    assert_eq!(message.compression, CompressionAlgorithm::Zlib);
    assert_eq!(
        message.properties.get("kind"),
        Some(&MessagePropertyValue::String("compressed".to_string()))
    );
    assert_eq!(
        message.properties.get("attempt"),
        Some(&MessagePropertyValue::Int32(7))
    );
    consumer
        .confirm_cookie(message.confirmation_cookie())
        .await?;

    disconnect_sessions(&config, &[&producer_session, &consumer_session]).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_reconfigure_subscriptions_delivers_pending_messages() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.enable_subscriptions {
        return Ok(());
    }

    let uri = config.unique_uri("subscriptions")?;
    let producer_session = connect_session(&config).await?;
    let consumer_session = connect_session(&config).await?;

    let producer = producer_session
        .open_queue(uri.as_str(), QueueOptions::writer())
        .await?;
    let consumer = consumer_session
        .open_queue(
            uri.as_str(),
            reader_options().subscriptions(vec![int_subscription(1, "x >= 4")]),
        )
        .await?;
    let mut consumer_events = consumer.events();

    for value in 0..=5 {
        producer
            .post(PostMessage::new(format!("x: {value}")).properties(int_property("x", value)))
            .await?;
    }

    assert_eq!(
        recv_and_confirm_with_events(&consumer, &mut consumer_events, 2).await?,
        vec!["x: 4", "x: 5"]
    );

    consumer
        .reconfigure(QueueOptions::reader().subscriptions(vec![int_subscription(2, "x >= 0")]))
        .await?;

    assert_eq!(
        recv_and_confirm_with_events(&consumer, &mut consumer_events, 4).await?,
        vec!["x: 0", "x: 1", "x: 2", "x: 3"]
    );

    disconnect_sessions(&config, &[&producer_session, &consumer_session]).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_reconfigure_can_clear_subscriptions() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.enable_subscriptions {
        return Ok(());
    }

    let uri = config.unique_uri("subscriptions-clear")?;
    let producer_session = connect_session(&config).await?;
    let consumer_session = connect_session(&config).await?;

    let producer = producer_session
        .open_queue(uri.as_str(), QueueOptions::writer())
        .await?;
    let consumer = consumer_session
        .open_queue(
            uri.as_str(),
            reader_options().subscriptions(vec![int_subscription(1, "x >= 4")]),
        )
        .await?;
    let mut consumer_events = consumer.events();

    for value in 0..=5 {
        producer
            .post(PostMessage::new(format!("x: {value}")).properties(int_property("x", value)))
            .await?;
    }

    assert_eq!(
        recv_and_confirm_with_events(&consumer, &mut consumer_events, 2).await?,
        vec!["x: 4", "x: 5"]
    );

    consumer
        .reconfigure(QueueOptions::reader().clear_subscriptions())
        .await?;

    assert_eq!(
        recv_and_confirm_with_events(&consumer, &mut consumer_events, 4).await?,
        vec!["x: 0", "x: 1", "x: 2", "x: 3"]
    );

    disconnect_sessions(&config, &[&producer_session, &consumer_session]).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_max_unconfirmed_messages_blocks_additional_delivery_until_confirm() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };

    let uri = config.unique_uri("max-unconfirmed")?;
    let producer_session = connect_session(&config).await?;
    let consumer_session = connect_session(&config).await?;

    let producer = producer_session
        .open_queue(uri.as_str(), QueueOptions::writer())
        .await?;
    let consumer = consumer_session
        .open_queue(uri.as_str(), reader_options().max_unconfirmed_messages(1))
        .await?;
    let mut consumer_events = consumer.events();

    producer.post(PostMessage::new("first")).await?;
    producer.post(PostMessage::new("second")).await?;

    let first = blazox_timeout(
        config.request_timeout,
        "first consumer message",
        next_queue_message(&mut consumer_events),
    )
    .await?;
    assert_eq!(String::from_utf8(first.payload.to_vec())?, "first");
    assert_no_message_on_events(&mut consumer_events, Duration::from_secs(1)).await?;

    consumer.confirm_cookie(first.confirmation_cookie()).await?;

    let second = blazox_timeout(
        config.request_timeout,
        "second consumer message",
        next_queue_message(&mut consumer_events),
    )
    .await?;
    assert_eq!(String::from_utf8(second.payload.to_vec())?, "second");
    consumer
        .confirm_cookie(second.confirmation_cookie())
        .await?;

    disconnect_sessions(&config, &[&producer_session, &consumer_session]).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_priority_routing_prefers_highest_priority_consumers_regardless_of_open_order()
-> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };

    for consumers_first in [false, true] {
        let uri = config.unique_uri("priority-routing")?;
        let producer_session_1 = connect_session(&config).await?;
        let producer_session_2 = connect_session(&config).await?;
        let high_session_1 = connect_session(&config).await?;
        let high_session_2 = connect_session(&config).await?;
        let low_session_1 = connect_session(&config).await?;
        let low_session_2 = connect_session(&config).await?;

        let (
            producer_1,
            producer_2,
            high_consumer_1,
            high_consumer_2,
            low_consumer_1,
            low_consumer_2,
        ) = if consumers_first {
            let high_consumer_1 = high_session_1
                .open_queue(uri.as_str(), reader_options().consumer_priority(2))
                .await?;
            let high_consumer_2 = high_session_2
                .open_queue(uri.as_str(), reader_options().consumer_priority(2))
                .await?;
            let low_consumer_1 = low_session_1
                .open_queue(uri.as_str(), reader_options().consumer_priority(1))
                .await?;
            let low_consumer_2 = low_session_2
                .open_queue(uri.as_str(), reader_options().consumer_priority(1))
                .await?;
            let producer_2 = producer_session_2
                .open_queue(uri.as_str(), acking_writer_options())
                .await?;
            let producer_1 = producer_session_1
                .open_queue(uri.as_str(), acking_writer_options())
                .await?;
            (
                producer_1,
                producer_2,
                high_consumer_1,
                high_consumer_2,
                low_consumer_1,
                low_consumer_2,
            )
        } else {
            let producer_1 = producer_session_1
                .open_queue(uri.as_str(), acking_writer_options())
                .await?;
            let high_consumer_1 = high_session_1
                .open_queue(uri.as_str(), reader_options().consumer_priority(2))
                .await?;
            let producer_2 = producer_session_2
                .open_queue(uri.as_str(), acking_writer_options())
                .await?;
            let high_consumer_2 = high_session_2
                .open_queue(uri.as_str(), reader_options().consumer_priority(2))
                .await?;
            let low_consumer_1 = low_session_1
                .open_queue(uri.as_str(), reader_options().consumer_priority(1))
                .await?;
            let low_consumer_2 = low_session_2
                .open_queue(uri.as_str(), reader_options().consumer_priority(1))
                .await?;
            (
                producer_1,
                producer_2,
                high_consumer_1,
                high_consumer_2,
                low_consumer_1,
                low_consumer_2,
            )
        };

        let mut high_events_1 = high_consumer_1.events();
        let mut high_events_2 = high_consumer_2.events();
        let mut low_events_1 = low_consumer_1.events();
        let mut low_events_2 = low_consumer_2.events();
        let correlation_ids = CorrelationIdGenerator::default();

        producer_1
            .post(PostMessage::new("msg-1").correlation_id(correlation_ids.next()))
            .await?;
        producer_2
            .post(PostMessage::new("msg-2").correlation_id(correlation_ids.next()))
            .await?;

        let mut received = vec![
            recv_payload_and_confirm(
                &high_consumer_1,
                &mut high_events_1,
                config.request_timeout,
                "high_consumer_1",
            )
            .await?,
            recv_payload_and_confirm(
                &high_consumer_2,
                &mut high_events_2,
                config.request_timeout,
                "high_consumer_2",
            )
            .await?,
        ];
        received.sort();
        assert_eq!(received, vec!["msg-1".to_string(), "msg-2".to_string()]);

        assert_no_message_on_events(&mut low_events_1, Duration::from_secs(1)).await?;
        assert_no_message_on_events(&mut low_events_2, Duration::from_secs(1)).await?;

        disconnect_sessions(
            &config,
            &[
                &producer_session_1,
                &producer_session_2,
                &high_session_1,
                &high_session_2,
                &low_session_1,
                &low_session_2,
            ],
        )
        .await;
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_max_unconfirmed_bytes_blocks_additional_delivery_until_confirm() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };

    let uri = config.unique_uri("max-unconfirmed-bytes")?;
    let producer_session = connect_session(&config).await?;
    let consumer_session = connect_session(&config).await?;

    let producer = producer_session
        .open_queue(uri.as_str(), QueueOptions::writer())
        .await?;
    let consumer = consumer_session
        .open_queue(
            uri.as_str(),
            reader_options()
                .consumer_priority(3)
                .max_unconfirmed_messages(2)
                .max_unconfirmed_bytes(3),
        )
        .await?;
    let mut consumer_events = consumer.events();

    producer.post(PostMessage::new("123")).await?;

    let first = blazox_timeout(
        config.request_timeout,
        "first max_unconfirmed_bytes message",
        next_queue_message(&mut consumer_events),
    )
    .await?;
    assert_eq!(String::from_utf8(first.payload.to_vec())?, "123");

    producer.post(PostMessage::new("1")).await?;
    assert_no_message_on_events(&mut consumer_events, Duration::from_secs(1)).await?;

    consumer.confirm_cookie(first.confirmation_cookie()).await?;

    let second = blazox_timeout(
        config.request_timeout,
        "second max_unconfirmed_bytes message",
        next_queue_message(&mut consumer_events),
    )
    .await?;
    assert_eq!(String::from_utf8(second.payload.to_vec())?, "1");

    producer.post(PostMessage::new("2")).await?;
    let third = blazox_timeout(
        config.request_timeout,
        "third max_unconfirmed_bytes message",
        next_queue_message(&mut consumer_events),
    )
    .await?;
    assert_eq!(String::from_utf8(third.payload.to_vec())?, "2");

    producer.post(PostMessage::new("3")).await?;
    assert_no_message_on_events(&mut consumer_events, Duration::from_secs(1)).await?;

    consumer
        .confirm_cookie(second.confirmation_cookie())
        .await?;
    consumer.confirm_cookie(third.confirmation_cookie()).await?;

    let fourth = blazox_timeout(
        config.request_timeout,
        "fourth max_unconfirmed_bytes message",
        next_queue_message(&mut consumer_events),
    )
    .await?;
    assert_eq!(String::from_utf8(fourth.payload.to_vec())?, "3");
    consumer
        .confirm_cookie(fourth.confirmation_cookie())
        .await?;

    disconnect_sessions(&config, &[&producer_session, &consumer_session]).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_fanout_routes_messages_to_each_app_id_stream() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.enable_fanout {
        return Ok(());
    }

    let uri = config.unique_fanout_uri("fanout-routing")?;
    let producer_session = connect_session(&config).await?;
    let foo_session = connect_session(&config).await?;
    let bar_session = connect_session(&config).await?;

    let producer = producer_session
        .open_queue(uri.as_str(), acking_writer_options())
        .await?;
    let foo_consumer = foo_session
        .open_queue(uri.as_str(), reader_options().app_id("foo"))
        .await?;
    let bar_consumer = bar_session
        .open_queue(uri.as_str(), reader_options().app_id("bar"))
        .await?;
    let mut foo_events = foo_consumer.events();
    let mut bar_events = bar_consumer.events();

    let correlation_ids = CorrelationIdGenerator::default();
    producer
        .post(PostMessage::new("msg-1").correlation_id(correlation_ids.next()))
        .await?;
    producer
        .post(PostMessage::new("msg-2").correlation_id(correlation_ids.next()))
        .await?;

    let foo_payloads = recv_and_confirm_with_events(&foo_consumer, &mut foo_events, 2).await?;
    let bar_payloads = recv_and_confirm_with_events(&bar_consumer, &mut bar_events, 2).await?;
    assert_eq!(foo_payloads, vec!["msg-1", "msg-2"]);
    assert_eq!(bar_payloads, vec!["msg-1", "msg-2"]);

    disconnect_sessions(&config, &[&producer_session, &foo_session, &bar_session]).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_broadcast_delivers_each_message_to_each_active_consumer() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.enable_broadcast {
        return Ok(());
    }

    let uri = config.unique_broadcast_uri("broadcast-routing")?;
    let producer_session_1 = connect_session(&config).await?;
    let producer_session_2 = connect_session(&config).await?;
    let consumer_session_1 = connect_session(&config).await?;
    let consumer_session_2 = connect_session(&config).await?;

    let producer_1 = producer_session_1
        .open_queue(uri.as_str(), acking_writer_options())
        .await?;
    let producer_2 = producer_session_2
        .open_queue(uri.as_str(), acking_writer_options())
        .await?;
    let consumer_1 = consumer_session_1
        .open_queue(uri.as_str(), reader_options())
        .await?;
    let consumer_2 = consumer_session_2
        .open_queue(uri.as_str(), reader_options())
        .await?;
    let mut consumer_events_1 = consumer_1.events();
    let mut consumer_events_2 = consumer_2.events();

    let correlation_ids = CorrelationIdGenerator::default();
    producer_1
        .post(PostMessage::new("msg-1").correlation_id(correlation_ids.next()))
        .await?;
    producer_2
        .post(PostMessage::new("msg-2").correlation_id(correlation_ids.next()))
        .await?;

    assert_eq!(
        recv_and_confirm_with_events(&consumer_1, &mut consumer_events_1, 2).await?,
        vec!["msg-1", "msg-2"]
    );
    assert_eq!(
        recv_and_confirm_with_events(&consumer_2, &mut consumer_events_2, 2).await?,
        vec!["msg-1", "msg-2"]
    );

    disconnect_sessions(
        &config,
        &[
            &producer_session_1,
            &producer_session_2,
            &consumer_session_1,
            &consumer_session_2,
        ],
    )
    .await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_broadcast_resubscribe_does_not_replay_gap_messages() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.enable_broadcast {
        return Ok(());
    }

    let uri = config.unique_broadcast_uri("broadcast-resubscribe")?;
    let producer_session = connect_session(&config).await?;
    let consumer_session = connect_session(&config).await?;

    let producer = producer_session
        .open_queue(uri.as_str(), acking_writer_options())
        .await?;
    let consumer = consumer_session
        .open_queue(uri.as_str(), reader_options())
        .await?;
    let mut consumer_events = consumer.events();

    let correlation_ids = CorrelationIdGenerator::default();
    producer
        .post(PostMessage::new("msg-1").correlation_id(correlation_ids.next()))
        .await?;
    assert_eq!(
        recv_and_confirm_with_events(&consumer, &mut consumer_events, 1).await?,
        vec!["msg-1"]
    );

    consumer.close().await?;
    producer
        .post(PostMessage::new("msg-2").correlation_id(correlation_ids.next()))
        .await?;

    let reopened_consumer = consumer_session
        .open_queue(uri.as_str(), reader_options())
        .await?;
    let mut reopened_events = reopened_consumer.events();
    assert_no_message_on_events(&mut reopened_events, Duration::from_secs(1)).await?;

    producer
        .post(PostMessage::new("msg-3").correlation_id(correlation_ids.next()))
        .await?;
    assert_eq!(
        recv_and_confirm_with_events(&reopened_consumer, &mut reopened_events, 1).await?,
        vec!["msg-3"]
    );

    disconnect_sessions(&config, &[&producer_session, &consumer_session]).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_broadcast_new_consumers_only_receive_future_messages() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.enable_broadcast {
        return Ok(());
    }

    let uri = config.unique_broadcast_uri("broadcast-add-consumers")?;
    let producer_session = connect_session(&config).await?;
    let consumer_session_1 = connect_session(&config).await?;
    let consumer_session_2 = connect_session(&config).await?;

    let producer = producer_session
        .open_queue(uri.as_str(), acking_writer_options())
        .await?;

    let correlation_ids = CorrelationIdGenerator::default();
    producer
        .post(PostMessage::new("null-msg").correlation_id(correlation_ids.next()))
        .await?;

    let consumer_1 = consumer_session_1
        .open_queue(uri.as_str(), reader_options())
        .await?;
    let mut consumer_events_1 = consumer_1.events();
    assert_no_message_on_events(&mut consumer_events_1, Duration::from_secs(1)).await?;

    producer
        .post(PostMessage::new("msg-1").correlation_id(correlation_ids.next()))
        .await?;
    assert_eq!(
        recv_and_confirm_with_events(&consumer_1, &mut consumer_events_1, 1).await?,
        vec!["msg-1"]
    );

    let consumer_2 = consumer_session_2
        .open_queue(uri.as_str(), reader_options())
        .await?;
    let mut consumer_events_2 = consumer_2.events();
    assert_no_message_on_events(&mut consumer_events_2, Duration::from_secs(1)).await?;

    producer
        .post(PostMessage::new("msg-2").correlation_id(correlation_ids.next()))
        .await?;
    assert_eq!(
        recv_and_confirm_with_events(&consumer_1, &mut consumer_events_1, 1).await?,
        vec!["msg-2"]
    );
    assert_eq!(
        recv_and_confirm_with_events(&consumer_2, &mut consumer_events_2, 1).await?,
        vec!["msg-2"]
    );

    disconnect_sessions(
        &config,
        &[&producer_session, &consumer_session_1, &consumer_session_2],
    )
    .await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_broadcast_dynamic_priorities_route_only_to_highest_priority_tier() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.enable_broadcast {
        return Ok(());
    }

    let uri = config.unique_broadcast_uri("broadcast-priorities")?;
    let producer_session = connect_session(&config).await?;
    let consumer_session_1 = connect_session(&config).await?;
    let consumer_session_2 = connect_session(&config).await?;
    let consumer_session_3 = connect_session(&config).await?;

    let producer = producer_session
        .open_queue(uri.as_str(), acking_writer_options())
        .await?;
    let consumer_1 = consumer_session_1
        .open_queue(uri.as_str(), reader_options().consumer_priority(2))
        .await?;
    let consumer_2 = consumer_session_2
        .open_queue(uri.as_str(), reader_options().consumer_priority(2))
        .await?;
    let consumer_3 = consumer_session_3
        .open_queue(uri.as_str(), reader_options().consumer_priority(2))
        .await?;
    let mut consumer_events_1 = consumer_1.events();
    let mut consumer_events_2 = consumer_2.events();
    let mut consumer_events_3 = consumer_3.events();

    let correlation_ids = CorrelationIdGenerator::default();
    producer
        .post(PostMessage::new("msg-1").correlation_id(correlation_ids.next()))
        .await?;
    assert_eq!(
        recv_and_confirm_with_events(&consumer_1, &mut consumer_events_1, 1).await?,
        vec!["msg-1"]
    );
    assert_eq!(
        recv_and_confirm_with_events(&consumer_2, &mut consumer_events_2, 1).await?,
        vec!["msg-1"]
    );
    assert_eq!(
        recv_and_confirm_with_events(&consumer_3, &mut consumer_events_3, 1).await?,
        vec!["msg-1"]
    );

    consumer_2
        .reconfigure(reader_options().consumer_priority(1))
        .await?;

    producer
        .post(PostMessage::new("msg-2").correlation_id(correlation_ids.next()))
        .await?;
    assert_eq!(
        recv_and_confirm_with_events(&consumer_1, &mut consumer_events_1, 1).await?,
        vec!["msg-2"]
    );
    assert_eq!(
        recv_and_confirm_with_events(&consumer_3, &mut consumer_events_3, 1).await?,
        vec!["msg-2"]
    );
    assert_no_message_on_events(&mut consumer_events_2, Duration::from_secs(1)).await?;

    consumer_1
        .reconfigure(reader_options().consumer_priority(99))
        .await?;

    producer
        .post(PostMessage::new("msg-3").correlation_id(correlation_ids.next()))
        .await?;
    assert_eq!(
        recv_and_confirm_with_events(&consumer_1, &mut consumer_events_1, 1).await?,
        vec!["msg-3"]
    );
    assert_no_message_on_events(&mut consumer_events_2, Duration::from_secs(1)).await?;
    assert_no_message_on_events(&mut consumer_events_3, Duration::from_secs(1)).await?;

    disconnect_sessions(
        &config,
        &[
            &producer_session,
            &consumer_session_1,
            &consumer_session_2,
            &consumer_session_3,
        ],
    )
    .await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_host_health_suspends_and_resumes_writer_queue() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };

    let uri = config.unique_uri("host-health")?;
    let monitor = Arc::new(ManualHostHealthMonitor::new());
    let writer_session = connect_session_with_monitor(&config, monitor.clone()).await?;
    let reader_session = connect_session(&config).await?;

    let writer = writer_session
        .open_queue(
            uri.as_str(),
            QueueOptions::writer().suspends_on_bad_host_health(true),
        )
        .await?;
    let reader = reader_session
        .open_queue(uri.as_str(), reader_options())
        .await?;
    let mut writer_events = writer.events();
    let mut reader_events = reader.events();

    monitor.set_unhealthy();
    assert!(matches!(
        next_queue_event(&mut writer_events).await?,
        QueueEvent::Suspended
    ));
    assert!(matches!(
        writer.post(PostMessage::new("blocked")).await,
        Err(blazox::Error::QueueSuspended(_))
    ));

    monitor.set_healthy();
    assert!(matches!(
        next_queue_event(&mut writer_events).await?,
        QueueEvent::Resumed
    ));

    writer.post(PostMessage::new("after-resume")).await?;
    let message = blazox_timeout(
        config.request_timeout,
        "reader after resume",
        next_queue_message(&mut reader_events),
    )
    .await?;
    assert_eq!(String::from_utf8(message.payload.to_vec())?, "after-resume");
    reader.confirm_cookie(message.confirmation_cookie()).await?;

    disconnect_sessions(&config, &[&writer_session, &reader_session]).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_admin_help_round_trip() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.run_admin {
        return Ok(());
    }

    let session = connect_session(&config).await?;
    let response = session.admin_command("help").await?;
    assert!(!response.trim().is_empty());
    disconnect_sessions(&config, &[&session]).await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn session_anonymous_authentication_round_trip() -> TestResult {
    let Some(config) = skip_unless_live() else {
        return Ok(());
    };
    if !config.enable_anonymous_auth {
        return Ok(());
    }

    let options = config.session_options().authenticate_anonymous(true);
    let session = Session::connect(options).await?;
    let uri = config.unique_uri("auth")?;
    let queue = session
        .open_queue(uri.as_str(), QueueOptions::writer())
        .await?;
    queue.post(PostMessage::new("auth-ok")).await?;
    disconnect_sessions(&config, &[&session]).await;
    Ok(())
}

async fn next_queue_event(
    events: &mut EventReceiver<QueueEvent>,
) -> TestResult<QueueEvent> {
    match timeout(Duration::from_secs(2), events.recv()).await {
        Ok(Ok(event)) => Ok(event),
        Ok(Err(error)) => Err(Box::new(std::io::Error::other(format!(
            "queue event stream closed: {error:?}"
        )))),
        Err(error) => Err(Box::new(error)),
    }
}

async fn recv_payload_and_confirm(
    queue: &blazox::Queue,
    events: &mut EventReceiver<QueueEvent>,
    duration: Duration,
    label: &str,
) -> TestResult<String> {
    let message = blazox_timeout(duration, label, next_queue_message(events)).await?;
    let payload = String::from_utf8(message.payload.to_vec())?;
    queue.confirm_cookie(message.confirmation_cookie()).await?;
    Ok(payload)
}
