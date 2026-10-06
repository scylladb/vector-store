# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Cross-compilation image: x86_64 host -> aarch64-unknown-linux-gnu.
#
# Based on the image the release toolchain uses
# (scripts/run-with-release-toolchain: rust:<channel from rust-toolchain.toml>-bookworm),
# so rustc, gcc 12.2 and the glibc 2.36 headers match the official release build.
# It needs no qemu binfmt on the host: rustc and the Debian cross gcc run natively;
# qemu-user is only used to smoke-test the produced binaries.
#
# cmake/clang/libclang are not needed: aws-lc-sys builds with the cc builder and
# ships pre-generated aarch64-linux-gnu bindings.
#
# Built by `vsbench build` as vs-bench-cross:<rust version>.
ARG RUST_VERSION
FROM rust:${RUST_VERSION}-bookworm

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      gcc-aarch64-linux-gnu \
      g++-aarch64-linux-gnu \
      libc6-dev-arm64-cross \
      binutils-aarch64-linux-gnu \
      qemu-user \
      file \
 && rm -rf /var/lib/apt/lists/*

# The repo's rust-toolchain.toml also lists clippy and rustfmt; install them here
# or rustup would download them on every `docker run --rm`.
RUN rustup target add aarch64-unknown-linux-gnu \
 && rustup component add clippy rustfmt \
 && chmod -R a+w "$RUSTUP_HOME" "$CARGO_HOME"

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUNNER="qemu-aarch64 -L /usr/aarch64-linux-gnu" \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ \
    AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar \
    PKG_CONFIG_ALLOW_CROSS=1
