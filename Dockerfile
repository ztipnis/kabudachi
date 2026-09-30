# syntax=docker/dockerfile:1
#
# Test/dev container for kabudachi. Not a production image — this exists so
# `bazel test //...` and `cargo test --workspace` work
# the same way on any host OS (see CONTRIBUTING.md), and so the same image
# doubles as the .devcontainer/ base for interactive VS Code development.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# build-essential/pkg-config: cargo/rustc. python3-dev: pyo3's build script
# (see CLAUDE.md's pyo3 note). git: Bazel module fetches. curl/unzip/ca-certificates:
# installing bazelisk/rustup/uv below.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    pkg-config \
    python3 \
    python3-dev \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Bazel, via bazelisk — respects this repo's .bazelversion (currently 9.2.0).
# Architecture-aware so the same Dockerfile works on amd64 and arm64 hosts.
RUN arch="$(dpkg --print-architecture)" \
    && case "$arch" in \
         amd64) bazelisk_arch=amd64 ;; \
         arm64) bazelisk_arch=arm64 ;; \
         *) echo "unsupported architecture: $arch" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /usr/local/bin/bazel \
         "https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-${bazelisk_arch}" \
    && chmod +x /usr/local/bin/bazel

# Rust, via rustup — for `cargo test --workspace` (fast local iteration only;
# Bazel's own Rust toolchain, via rules_rust, is separate and hermetic).
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal

# uv is available for interactive development. Python tests run through Bazel.
ENV PATH=/root/.local/bin:$PATH
RUN curl -fsSL https://astral.sh/uv/install.sh | sh

WORKDIR /workspace
COPY . .

CMD ["bash"]
