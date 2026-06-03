FROM rust:1.88-slim AS builder

WORKDIR /src

COPY chatmock-rs/Cargo.toml chatmock-rs/Cargo.lock /src/chatmock-rs/
COPY chatmock-rs/src /src/chatmock-rs/src

RUN cargo build --manifest-path /src/chatmock-rs/Cargo.toml --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

ENV CHATMOCK_RUNTIME=rust

WORKDIR /app

COPY --from=builder /src/chatmock-rs/target/release/chatmock-rs /usr/local/bin/chatmock-rs
COPY prompt.md prompt_gpt5_codex.md README.md /app/

RUN mkdir -p /data

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000 1455

ENTRYPOINT ["/entrypoint.sh"]
CMD ["serve"]