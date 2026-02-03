FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev sqlite-dev

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY core ./core
COPY connectors ./connectors
COPY commons ./commons
COPY pipeline ./pipeline

ENV RUSTFLAGS="-C target-feature=-crt-static"
RUN cargo build --locked --release -p liquidium-pipeline

FROM alpine:latest

RUN apk add --no-cache ca-certificates libc6-compat openssl libgcc sqlite-libs
RUN adduser -D liquidator

RUN mkdir -p /data /secrets && chown liquidator:liquidator /data /secrets

COPY --from=builder /app/target/release/liquidator /usr/local/bin/liquidator
RUN chown liquidator:liquidator /usr/local/bin/liquidator

USER liquidator
WORKDIR /data

ENV DB_PATH=/data/wal.db \
    MNEMONIC_FILE=/secrets/key

ENTRYPOINT ["liquidator"]
