# The release toolchain image, built by scripts/run-with-release-toolchain: the Rust image of the
# channel pinned in rust-toolchain.toml on Debian bookworm, whose glibc 2.36 is the floor the
# redhat/ubi9-minimal runtime image (glibc 2.34 compatible) accepts, plus clang for the arm64
# build. numkong, the kernel library behind usearch's distance functions, needs gcc >= 13 or clang
# for its aarch64 kernels and bookworm ships gcc 12 (VECTOR-720). The amd64 build keeps gcc.
ARG RUST_VERSION
FROM rust:${RUST_VERSION}-bookworm
ARG TARGETARCH
RUN if [ "$TARGETARCH" = arm64 ]; then \
      apt-get update \
      && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends wget gnupg ca-certificates \
      && wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /usr/share/keyrings/llvm.gpg \
      && echo "deb [signed-by=/usr/share/keyrings/llvm.gpg] http://apt.llvm.org/bookworm/ llvm-toolchain-bookworm-19 main" > /etc/apt/sources.list.d/llvm.list \
      && apt-get update \
      && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends clang-19 \
      && rm -rf /var/lib/apt/lists/* ; \
    fi
# The cc crate reads the per-target variables, so the amd64 build is untouched by them.
ENV CC_aarch64_unknown_linux_gnu=clang-19 \
    CXX_aarch64_unknown_linux_gnu=clang++-19
