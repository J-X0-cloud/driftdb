# syntax=docker/dockerfile:1

FROM rust:1.93-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY benches benches
COPY examples examples
RUN cargo build --release --locked --bin driftdb \
    && strip target/release/driftdb

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home-dir /data driftdb \
    && mkdir -p /data && chown driftdb /data
COPY --from=build /src/target/release/driftdb /usr/local/bin/driftdb
USER driftdb
ENV DRIFTDB_DATA_DIR=/data \
    DRIFTDB_BIND=0.0.0.0 \
    PORT=8086
VOLUME ["/data"]
EXPOSE 8086
# `serve` reads PORT from the environment, so platforms that inject PORT work
# without extra flags.
ENTRYPOINT ["driftdb"]
CMD ["serve"]
