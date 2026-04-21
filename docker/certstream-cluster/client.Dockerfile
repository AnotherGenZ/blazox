FROM rust:1.88-bookworm AS build

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples

RUN cargo build --release --example certstream_cluster_demo

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/examples/certstream_cluster_demo /usr/local/bin/coinbase-market-data-cluster-demo

ENTRYPOINT ["/usr/local/bin/coinbase-market-data-cluster-demo"]
