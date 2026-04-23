FROM rust:1.88-bookworm AS build

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples

RUN cargo build --release --example coinbase_market_data_cluster_demo --features "trace-propagation otel-exporter"

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/examples/coinbase_market_data_cluster_demo /usr/local/bin/coinbase-market-data-cluster-demo

ENTRYPOINT ["/usr/local/bin/coinbase-market-data-cluster-demo"]
