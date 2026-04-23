# blazox

`blazox` is an async Rust SDK for [BlazingMQ](https://bloomberg.github.io/blazingmq/), Bloomberg's open source distributed message queueing platform. This repository provides both a high-level session API for application developers and a lower-level protocol client for callers that need direct control over queue handles, transport events, and wire-level behavior.

If you are new to BlazingMQ, start with:

- [BlazingMQ home page](https://bloomberg.github.io/blazingmq/)
- [BlazingMQ overview](https://bloomberg.github.io/blazingmq/docs/introduction/overview/)
- [BlazingMQ feature index](https://bloomberg.github.io/blazingmq/features)
- [Client / broker protocol](https://bloomberg.github.io/blazingmq/docs/architecture/client_broker_protocol/)
- [High availability in client libraries](https://bloomberg.github.io/blazingmq/docs/architecture/high_availability_sdk/)

## What Is BlazingMQ?

BlazingMQ is a durable, highly available message queueing system designed for low-latency, high-throughput workloads. It supports multiple routing strategies and operational features that are useful in real distributed systems, including:

- work queues and priority-based consumption
- fanout and broadcast delivery patterns
- consumer flow control
- subscriptions driven by message properties
- payload compression
- host health monitoring
- structured tracing spans and events

At a system level, applications connect to BlazingMQ brokers and exchange data through named queues using producer and consumer clients. `blazox` is the Rust-side client implementation for those workflows.

## What This SDK Provides

This repository is split into two main layers.

| Layer | Use it when you want | Main entry points |
| --- | --- | --- |
| High-level SDK | a Rust-native application API with queue lifecycle management, reconnect behavior, queue restoration, confirmation helpers, and queue-local event streams | `Session`, `Queue`, `SessionOptions`, `QueueOptions` |
| Low-level protocol client | direct access to negotiation, control-plane requests, transport events, queue handles, and wire messages | `Client`, `OpenQueueOptions`, `OutboundPut`, `TransportEvent` |

The crate also exposes supporting modules for:

- control-plane schema types in `schema`
- binary data-path structures in `wire`
- event channels in `event`
- in-memory mocks for application tests in `mock`

## Getting Started

### Prerequisites

You need a reachable BlazingMQ broker. By default, the examples and tests assume `127.0.0.1:30114`.

For a quick introduction to running BlazingMQ itself, use the official documentation:

- [BlazingMQ getting started guide](https://bloomberg.github.io/blazingmq/getting_started)
- [BlazingMQ installation and docs](https://bloomberg.github.io/blazingmq/)

### Build The Crate

```bash
cargo build
```

### Run The Example

The fastest way to see the high-level API is the bundled hello world example:

```bash
cargo run --example hello_world -- 127.0.0.1:30114 bmq://bmq.test.mem.priority/hello-world
```

That example demonstrates:

- session connect and event handling
- optional anonymous authentication
- queue open and reconfigure flows
- posting messages with properties and compression
- consuming queue events and confirming deliveries
- host health suspension and resume behavior
- tracing hooks

See:

- [examples/hello_world.rs](examples/hello_world.rs)
- [examples/coinbase_market_data_cluster_demo.rs](examples/coinbase_market_data_cluster_demo.rs)
- [docker/coinbase-market-data-cluster/README.md](docker/coinbase-market-data-cluster/README.md)

### Minimal Session API Example

```rust
use blazox::{PostMessage, QueueOptions, Session, SessionOptions, UriBuilder};

#[tokio::main(flavor = "current_thread")]
async fn main() -> blazox::Result<()> {
    let queue_uri = UriBuilder::new()
        .domain("bmq.test.mem.priority")
        .queue("hello-world")
        .build()?;

    let session = Session::connect(
        SessionOptions::default().broker_addr("127.0.0.1:30114"),
    )
    .await?;

    let producer = session
        .open_queue(queue_uri.as_str(), QueueOptions::writer())
        .await?;

    producer.post(PostMessage::new("hello from blazox")).await?;
    session.stop().await?;
    Ok(())
}
```

## Supported Features

The short version: `blazox` already covers the core publish, consume, confirm, and runtime-management surface expected from a usable BlazingMQ SDK. The best source for exact parity notes is [docs/sdk-parity-audit.md](docs/sdk-parity-audit.md), but the table below is the practical overview.

| Area | Support | Notes |
| --- | --- | --- |
| Session lifecycle | Supported | `Session::start`, `Session::connect`, `Session::stop`, `Session::linger`, session event streams |
| Queue lifecycle | Supported | open, reconfigure, close, queue lookup, queue-local event streams |
| Publish / consume / confirm | Supported | `post`, batch posting, packed posting, `next_message`, confirm helpers |
| Producer acknowledgements | Supported | correlation ids and typed ACK events |
| Reconnect and queue restoration | Supported | session reconnects, queue reopen, replay of unacked puts |
| Consumer flow control | Supported | `max_unconfirmed_messages` and `max_unconfirmed_bytes` |
| Consumer priorities | Supported | priority and priority-count configuration, live reconfigure coverage |
| Fanout queues | Supported | app-id based stream routing |
| Broadcast queues | Supported | active-consumer broadcast and resubscribe coverage |
| Subscriptions | Supported | property-based subscription expressions and explicit subscription clearing |
| Message properties | Supported | property encoding, decoding, and subscription filtering |
| Compression | Supported | compressed payload posting and round-trip delivery |
| Host health monitoring | Supported | queue suspension and restoration driven by a host-health monitor |
| Distributed tracing | Supported | native `tracing` spans/events for SDK operations |
| Anonymous authentication | Supported | session-level and client-level anonymous auth flows |
| Admin commands | Supported | admin command round trips |
| Protocol encodings | Supported | JSON and BER control-plane support |
| Wire compatibility | Supported | `PUT`, `PUSH`, `ACK`, `CONFIRM`, properties, compression metadata |
| Testing support | Supported | in-memory `MockSession` / `MockQueue` plus live broker integration tests |

## Feature Coverage By API Surface

### High-level `Session` / `Queue` API

Use the high-level API if you want application-facing behavior rather than protocol plumbing. This layer includes:

- queue open, configure, close, and lookup helpers
- event-driven or pull-based message consumption
- confirmation builders and helpers
- reconnect handling and queue-state restoration
- queue suspension on bad host health
- structured `tracing` spans and events around queue operations

For most application code, this is the correct starting point.

### Low-level `Client` API

Use the low-level API if you want to work directly with the protocol and transport stream. This layer includes:

- raw queue-handle opens
- direct `OutboundPut` publishing
- transport event subscriptions
- authentication and admin commands
- access to schema and wire structures

This is useful for protocol tooling, debugging, specialized integrations, or situations where you want finer control than the session layer exposes.

## Current Gaps And Differences

`blazox` is intentionally Rust-first rather than a line-by-line port of the official C++, Java, or Python SDKs. The main gaps today are:

- queue reconfiguration parity is strong, but the public `QueueOptions` shape is Rust-native rather than a direct copy of upstream patch-style APIs
- message dumping is exposed through Rust tracing targets rather than the upstream `configureMessageDumping` command parser
- some tuning knobs, such as low-watermark and processing-thread settings, are advisory because the runtime model is Tokio-based instead of thread-pool based
- the async lifecycle is designed around futures and event streams instead of separate sync and callback-heavy async method families

Those differences are documented in more detail in [docs/sdk-parity-audit.md](docs/sdk-parity-audit.md).

## Examples And Supporting Docs

- [examples/hello_world.rs](examples/hello_world.rs): end-to-end session example with queue events, properties, compression, admin, auth, host health, and tracing
- [examples/coinbase_market_data_cluster_demo.rs](examples/coinbase_market_data_cluster_demo.rs): richer multi-queue demo using priority, fanout, and external market data
- [docker/coinbase-market-data-cluster/README.md](docker/coinbase-market-data-cluster/README.md): containerized cluster demo for the Coinbase example
- [docs/live-integration-tests.md](docs/live-integration-tests.md): how to run the live broker integration suite
- [docs/sdk-parity-audit.md](docs/sdk-parity-audit.md): feature and parity analysis against official BlazingMQ docs and SDKs

## Testing

The repository includes:

- unit-test-friendly in-memory mocks via `MockSession` and `MockQueue`
- live integration tests for client and session APIs against a real broker

Live tests are opt-in and documented in [docs/live-integration-tests.md](docs/live-integration-tests.md).

## Status

The crate already covers the core SDK workflows needed to build Rust producers and consumers on BlazingMQ, but it is still early-stage software and continues to close parity gaps with the official client libraries. If you need exact behavior details, treat the parity audit and the live integration suite as the source of truth for current support.

## Trademark Notice

BlazingMQ and the BlazingMQ logos are trademarks or service marks of Bloomberg L.P. This repository is an independent Rust SDK for working with BlazingMQ and is not affiliated with, endorsed by, or sponsored by Bloomberg.

Use of the BlazingMQ name in this repository is descriptive only, to identify compatibility with the upstream software. See Bloomberg's trademark guidance here:

- <https://github.com/bloomberg/blazingmq/blob/main/NOTICE/TRADEMARK.txt>

When referring to the upstream software, follow Bloomberg's published guidance, including the attribution text: `BLAZINGMQ, developed and published by Bloomberg L.P.`
