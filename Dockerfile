FROM rust:1.92-slim-bookworm AS build
WORKDIR /app
ENV CARGO_BUILD_JOBS=2
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tools/profile_input.rs /tmp/profile_input.rs
RUN rustup component add llvm-tools-preview \
    && rustc -O /tmp/profile_input.rs -o /tmp/generate-profile-input \
    && /tmp/generate-profile-input /tmp/profile-input \
    && mkdir /tmp/branch-counts \
    && cargo rustc --locked --release -- -Cprofile-generate=/tmp/branch-counts \
    && DATA_DIR=/tmp/profile-input OUTPUT_DIR=/tmp/profile-output NCPU=2 ./target/release/shopping-sessions \
    && "$(rustc --print sysroot)/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata" merge -o /tmp/branches.profdata /tmp/branch-counts \
    && cargo rustc --locked --release -- -Cprofile-use=/tmp/branches.profdata
FROM debian:bookworm-slim
COPY --from=build /app/target/release/shopping-sessions /usr/local/bin/shopping-sessions
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]
