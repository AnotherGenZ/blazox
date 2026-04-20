# Live Integration Tests

The Rust integration suite in [`tests/live_client_integration.rs`](../tests/live_client_integration.rs)
and [`tests/live_session_integration.rs`](../tests/live_session_integration.rs) is intended to run
against a real BlazingMQ broker.

By default these tests are inert. They only execute when `BLAZOX_RUN_LIVE_TESTS=1` is set.

## Environment

Required:

- `BLAZOX_RUN_LIVE_TESTS=1`

Optional:

- `BLAZOX_TEST_ADDR`
  Default: `127.0.0.1:30114`
- `BLAZOX_TEST_DOMAIN`
  Legacy alias for `BLAZOX_TEST_PRIORITY_DOMAIN`.
- `BLAZOX_TEST_PRIORITY_DOMAIN`
  Default: `bmq.test.mmap.priority`
- `BLAZOX_TEST_FANOUT_DOMAIN`
  Default: `bmq.test.mmap.fanout`
- `BLAZOX_TEST_BROADCAST_DOMAIN`
  Default: `bmq.test.mem.broadcast`
- `BLAZOX_TEST_QUEUE_PREFIX`
  Default: `blazox-it`
- `BLAZOX_TEST_REQUEST_TIMEOUT_MS`
  Default: `3000`
- `BLAZOX_TEST_ENABLE_ADMIN=1`
  Enables admin-command round-trip tests.
- `BLAZOX_TEST_ENABLE_ANONYMOUS_AUTH=1`
  Enables anonymous-authentication tests.
- `BLAZOX_TEST_ENABLE_SUBSCRIPTIONS=1`
  Enables subscription and reconfiguration tests.
- `BLAZOX_TEST_ENABLE_FANOUT=1`
  Enables fanout app-id routing coverage. Requires a broker fanout domain,
  typically `bmq.test.mmap.fanout`, or an override via
  `BLAZOX_TEST_FANOUT_DOMAIN`.
- `BLAZOX_TEST_ENABLE_BROADCAST=1`
  Enables broadcast delivery, resubscribe, and dynamic-priority coverage.
  Requires a broker broadcast domain, typically `bmq.test.mem.broadcast`, or
  an override via `BLAZOX_TEST_BROADCAST_DOMAIN`.

## Running

Single-node example:

```bash
BLAZOX_RUN_LIVE_TESTS=1 cargo test --test live_client_integration --test live_session_integration
```

Broker with subscriptions, admin, and anonymous auth enabled:

```bash
BLAZOX_RUN_LIVE_TESTS=1 \
BLAZOX_TEST_ENABLE_ADMIN=1 \
BLAZOX_TEST_ENABLE_ANONYMOUS_AUTH=1 \
BLAZOX_TEST_ENABLE_SUBSCRIPTIONS=1 \
cargo test --test live_client_integration --test live_session_integration
```

Full parity run on a broker that exposes priority, fanout, and broadcast domains:

```bash
BLAZOX_RUN_LIVE_TESTS=1 \
BLAZOX_TEST_ENABLE_ADMIN=1 \
BLAZOX_TEST_ENABLE_ANONYMOUS_AUTH=1 \
BLAZOX_TEST_ENABLE_SUBSCRIPTIONS=1 \
BLAZOX_TEST_ENABLE_FANOUT=1 \
BLAZOX_TEST_ENABLE_BROADCAST=1 \
cargo test --test live_client_integration --test live_session_integration -- --nocapture
```

## Ported Coverage

These Rust tests target the SDK-owned behavior from the Python integration suite:

- `test_breathing.py`
  Ported basic open, post, ack, push, confirm, close, and queue lookup coverage.
- `test_breathing.py::test_verify_priority`
  Ported priority-tier routing, open-order independence, and
  `max_unconfirmed_messages` / `max_unconfirmed_bytes` delivery gating.
- `test_breathing.py::test_verify_fanout`
  Ported fanout delivery to distinct application-id streams behind explicit
  live-env gating.
- `test_breathing.py::test_verify_broadcast`
  Ported broadcast delivery to multiple active consumers behind explicit
  live-env gating.
- `test_compression.py`
  Ported compressed payload delivery coverage.
- `test_breathing.py::test_message_properties`
  Ported message-property round-trip coverage.
- `test_subscriptions.py`
  Ported subscription filtering and reconfigure-driven redelivery coverage.
- `test_broadcast.py`
  Ported live broadcast resubscribe, late-join consumer, and dynamic-priority
  routing behavior behind explicit live-env gating.
- `test_authn.py`
  Ported anonymous-authentication success coverage behind env gating.
- `test_admin_client.py`
  Ported basic admin-command round-trip coverage behind env gating.
- Host health monitoring coverage from the SDK parity work
  Ported queue suspension and resume behavior using the Rust `ManualHostHealthMonitor`.

## Intentionally Not Ported Here

The following Python areas are broker- or cluster-orchestration tests, not client-SDK tests:

- broker startup and shutdown behavior
- leader election and failover orchestration
- proxy / broker user-agent log assertions
- authorization and alarm-management flows that require domain reconfiguration
- partition wipe, rollover, redeploy, and domain lifecycle commands
- replica repair and FSM synchronization cases
- consumer redelivery scenarios that depend on process exit, disconnect, or node crash
- broker alarm, storage, and admin-routing internals

Those belong in broker integration coverage, even if the Rust SDK is one of the clients used during those runs.
