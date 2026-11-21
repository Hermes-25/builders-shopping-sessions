FROM rust:1.92-slim-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release
FROM debian:bookworm-slim
COPY --from=build /app/target/release/shopping-sessions /usr/local/bin/shopping-sessions
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]
