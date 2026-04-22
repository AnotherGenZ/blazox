# Coinbase Market Data Cluster Demo

This example starts:

- `broker-a`, `broker-b`, `broker-c`: three BlazingMQ brokers in one cluster
- `publisher`: a `blazox` client connected to `broker-a`
- `worker-a`: a `blazox` client connected to `broker-b`
- `worker-b`: a `blazox` client connected to `broker-c`

The publisher connects to Coinbase Exchange Market Data at `wss://ws-feed.exchange.coinbase.com`, subscribes to public channels for a small product set, and demonstrates BlazingMQ queueing plus a few operational features:

1. Shared work queues in the priority domain:

- `coinbase.ticker`
- `coinbase.matches`
- `coinbase.heartbeat`

2. A dedicated consumer-priority queue in the priority domain:

- `coinbase.priority`

3. A fanout queue in the fanout domain:

- `coinbase.fanout`

`worker-a` starts with consumer priority `10`, while `worker-b` starts with priority `5`. After startup, `worker-a` reconfigures itself down to priority `0`, which causes the broker to shift `coinbase.priority` traffic to `worker-b`. This makes the priority-routing behavior visible in live logs instead of leaving it as a static configuration detail.

The fanout lane follows the upstream BlazingMQ pattern used in `~/Coding/blazingmq/src/integration-tests`, where fanout readers attach through app-specific URIs such as `bmq://.../coinbase.fanout?id=foo`. In this demo:

- `worker-a` opens `...?id=foo`
- `worker-b` opens `...?id=bar`
- the fanout domain config sets `parameters.subscriptions`, following `test_app_subscription_fanout`, so:
- `foo` receives `ticker` and `heartbeat`
- `bar` receives `matches` and `heartbeat`

That means both workers receive the same `heartbeat` fanout copies, while `ticker` and `matches` are filtered per AppId by the broker.

The publisher also opens its writer queues with producer acknowledgements enabled, the dedicated priority consumer performs an in-place runtime reconfigure to demonstrate failover, and the client setup path retries queue-open timeouts during broker warm-up instead of exiting the container.

## Run

From the repository root:

```bash
docker compose -f docker/coinbase-market-data-cluster/docker-compose.yml up --build
```

The first run takes time because Compose builds:

- the official BlazingMQ broker image from `https://github.com/bloomberg/blazingmq.git`
- the local `blazox` client image from this repository

## What To Watch

- `publisher` logs show sampled Coinbase messages being published into the work-queue and consumer-priority pipelines, along with producer ACKs.
- `worker-a` and `worker-b` logs show:
- shared work-queue messages split across brokers
- consumer-priority traffic moving from `worker-a` to `worker-b` after the runtime reconfigure
- fanout deliveries arriving independently on `foo` and `bar`

- host ports `31114`, `31115`, and `31116` map to the three brokers if you want to inspect them manually.

## Tuning

Useful environment variables are already set in the compose file:

- `BLAZOX_MARKET_DATA_URL=wss://ws-feed.exchange.coinbase.com` points the publisher at the official Coinbase Exchange websocket feed.
- `BLAZOX_PRODUCT_IDS=BTC-USD,ETH-USD,SOL-USD` controls which products are subscribed on the `ticker`, `matches`, and `heartbeat` channels.
- `BLAZOX_PUBLISH_EVERY_N=10` samples the live market data while still producing enough traffic to observe work-queue distribution.
- `BLAZOX_FANOUT_DOMAIN=bmq.demo.persistent.fanout` controls the separate fanout domain used by `coinbase.fanout`.
- `BLAZOX_FANOUT_APP_ID=foo|bar` selects the worker's fanout AppId, which maps to the domain-configured application subscription.
- `BLAZOX_WORKER_DELAY_MS=750` slows each worker slightly so the work-queue distribution is visible in logs.
- `BLAZOX_FEATURE_DELAY_MS=0` keeps the dedicated fanout and priority demo lanes responsive so they do not stall behind the shared queue delay.
- `BLAZOX_PRIORITY_CONSUMER_PRIORITY` controls the dedicated `coinbase.priority` consumer tier for each worker.
- `BLAZOX_PRIORITY_RECONFIGURE_AFTER_MS` and `BLAZOX_PRIORITY_RECONFIGURE_TO` let a worker change its consumer priority at runtime to demonstrate broker-side failover.

You can change those values directly in [docker-compose.yml](/home/angz/Coding/blazox/docker/coinbase-market-data-cluster/docker-compose.yml).
