# BlazingMQ Rust SDK Parity Audit

This audit tracks `blazox` against:

- the official BlazingMQ feature documentation
- the published C++ client API
- the published Java client API

## Verdict

`blazox` covers the core publish/consume/client-runtime surface with async
Rust-native equivalents, but it does not yet fully match all documented SDK
semantics.

The public API is intentionally not a line-by-line port of the C++ or Java surface. It does expose Rust-native equivalents for most documented client capabilities:

- async session lifecycle
- queue open/configure/close operations
- publish/consume/confirm flows
- reconnect and queue state restoration
- host health suspension and restoration
- distributed-trace hooks
- builder-style batching and packed posting
- typed queue/session events and status objects
- BER and JSON control-plane support
- wire-level data-path compatibility, including compression, properties, and PUSH metadata

## Sources Reviewed

- BlazingMQ feature index: <https://bloomberg.github.io/blazingmq/features>
- Client/broker protocol: <https://bloomberg.github.io/blazingmq/docs/architecture/client_broker_protocol/>
- High availability in client libraries: <https://bloomberg.github.io/blazingmq/docs/architecture/high_availability_sdk/>
- Subscriptions: <https://bloomberg.github.io/blazingmq/docs/features/subscriptions/>
- Compression: <https://bloomberg.github.io/blazingmq/docs/features/compression/>
- Consumer flow control: <https://bloomberg.github.io/blazingmq/docs/features/consumer_flow_control/>
- Host health monitoring: <https://bloomberg.github.io/blazingmq/docs/features/host_health_monitoring/>
- Distributed trace: <https://bloomberg.github.io/blazingmq/docs/features/distributed_trace/>
- C++ `bmqa::AbstractSession`: <https://bloomberg.github.io/blazingmq/docs/apidocs/cpp_apidocs/classbmqa_1_1AbstractSession.html>
- C++ `bmqt::QueueOptions`: <https://bloomberg.github.io/blazingmq/docs/apidocs/cpp_apidocs/group__bmqt__queueoptions.html>
- Java `AbstractSession`: <https://bloomberg.github.io/blazingmq/docs/apidocs/java_apidocs/com/bloomberg/bmq/AbstractSession.html>
- Java `Queue`: <https://bloomberg.github.io/blazingmq/docs/apidocs/java_apidocs/com/bloomberg/bmq/Queue.html>
- Java `PutMessage`: <https://bloomberg.github.io/blazingmq/docs/apidocs/java_apidocs/com/bloomberg/bmq/PutMessage.html>
- Java `PushMessage`: <https://bloomberg.github.io/blazingmq/docs/apidocs/java_apidocs/com/bloomberg/bmq/PushMessage.html>
- Java `SessionOptions.Builder`: <https://bloomberg.github.io/blazingmq/docs/apidocs/java_apidocs/com/bloomberg/bmq/SessionOptions.Builder.html>
- Java SDK README: <https://github.com/bloomberg/blazingmq-sdk-java>

## Current Coverage

### Session lifecycle

`blazox` provides:

- `Session::start`, `Session::connect`, `Session::stop`, and `Session::linger`
- event-stream consumption via receivers or async handler tasks
- reconnect and reconnection events

This covers the documented lifecycle semantics while keeping the API idiomatic for async Rust.

### Queue and message APIs

`blazox` provides:

- `Queue::post`, `post_batch`, `pack_batch`, and `post_packed_batch`
- queue-bound `PutBuilder` and `ConfirmBuilder`
- `PostMessage`, `ReceivedMessage`, `ConfirmBatch`, and typed ACK/message events
- status-returning queue operations for open/configure/close

This is the Rust-native equivalent of the C++/Java builder and queue APIs.

### SessionOptions and QueueOptions

`blazox` now covers:

- broker address and user agent configuration
- connect, request, open, configure, close, disconnect, linger, and channel-write timeouts
- event-queue watermarks
- host health monitor installation
- distributed trace context and tracer installation
- consumer flow-control, consumer priority, app id, subscriptions, and host-health suspension policy

The remaining differences here are intentional async-native interpretations, not missing feature support:

- `blob_buffer_size`, `channel_high_watermark`, and `stats_dump_interval` are wired into runtime behavior
- `event_queue_high_watermark` drives async event-channel capacity
- `event_queue_low_watermark` and `num_processing_threads` remain advisory tuning knobs because the Rust client uses async tasks and bounded channels instead of the C++ synchronous event-queue/thread-pool model

### High availability

`blazox` now implements the documented client-library HA behavior:

- client-generated message GUIDs
- buffering of `PUT`s until `ACK`
- replay of unacked `PUT`s across reconnect
- queue reopen and state restoration
- local close completion while disconnected
- `CONFIRM` requests are not buffered across reconnect
- write-side backpressure timeout surfaced as a client error

### Host health monitoring

`blazox` now supports:

- installable host health monitors
- queue opt-in through `suspends_on_bad_host_health`
- queue suspension and restoration events
- broker-side stream reconfiguration during suspend and resume
- rejection of new packing/posting while suspended
- acceptance of confirms for previously delivered messages while suspended

### Distributed tracing

`blazox` now supports:

- operation-level tracing through `TraceSink`
- distributed trace integration through `DistributedTracer`, `DistributedTraceContext`, and `DistributedTraceSpan`

This now includes queue metadata baggage on child spans, which is the important feature-semantic match for the documented broker/session flows.

### Protocol support

`blazox` now supports:

- JSON and BER control-plane payloads
- high-tag BER identifiers
- opaque `clusterMessage` BER handling
- `CONTROL`, `PUT`, `PUSH`, `ACK`, `CONFIRM`, heartbeat, negotiation, and authentication events
- message properties, compression, and decoded PUSH options

## Remaining Differences

There are still a few material gaps and semantic differences:

1. `configureQueue` parity is not complete.
   - The Rust API now preserves unspecified flow-control and priority fields
     across reconfigure operations and supports explicit subscription clearing.
   - Remaining difference: the public `QueueOptions` shape is still Rust-first
     rather than a direct port of the C++/Java `QueueOptions` patch model.

2. Message-dumping uses Rust-native tracing rather than the upstream command API.
   - `blazox` emits structured tracing events on targets such as
     `blazox::messages::push`, `blazox::messages::ack`,
     `blazox::messages::put`, and `blazox::messages::confirm`.
   - It does not expose the upstream `configureMessageDumping` command parser.

3. Session lifecycle parity is intentionally async Rust-native rather than a
   line-by-line port.
   - Futures and event streams are used where C++ and Java expose separate
     synchronous and callback-heavy asynchronous entry points.
   - Convenience APIs such as `startAsync`, `stopAsync`, and `finalizeStop`
     are not exposed as distinct methods.

4. The public surface is async Rust-native rather than a literal C++/Java port.
   - Futures and event streams are used where C++ and Java expose callback-heavy async methods.
   - Session-owned builder loaders map to cheap Rust constructors and helper methods.

5. A few tuning knobs are advisory rather than exact mechanical ports.
   - `num_processing_threads` and `event_queue_low_watermark` do not force a direct thread-pool/event-queue implementation because that would be a poor fit for Tokio-based async execution.

6. The distributed-trace abstraction is intentionally smaller than the full C++ class hierarchy.
   - It preserves the important semantics: current-span lookup, child-span creation, activation scopes, and queue-operation baggage.

## Bottom Line

`blazox` is now a credible core Rust SDK for BlazingMQ, with working queue
open/configure/close, publish/consume/confirm flows, reconnect handling,
compression, flow control, fanout, broadcast, subscriptions, and host health.
It still has a few documented parity gaps relative to the C++/Java SDKs, most
notably in exact API-shape/lifecycle equivalence and other Rust-vs-upstream
surface differences.
