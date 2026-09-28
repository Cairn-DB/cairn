# Cairn server image. Build from the repository root:
#   podman build -f docker/Dockerfile -t cairn:dev .   (or docker build ...)
# Run one development node:
#   podman run -p 7200:7200 -p 7100:7100 -v cairn-data:/data cairn:dev
# HTTP/JSON API on 7200 (ADR 0023), binary protocol on 7100. The first start prints an admin
# API key in the logs (ADR 0030): podman logs <container> | grep cairn_
# The build stage runs on the builder's own platform and cross-compiles for the target one
# (linux/amd64 or linux/arm64), so a multi-arch build never emulates rustc.
FROM --platform=$BUILDPLATFORM docker.io/library/rust:1-bookworm AS build
ARG TARGETARCH
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar
RUN arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "$arch" in \
      amd64) echo x86_64-unknown-linux-gnu > /triple ;; \
      arm64) echo aarch64-unknown-linux-gnu > /triple \
             && apt-get update \
             && apt-get install -y --no-install-recommends gcc-aarch64-linux-gnu libc6-dev-arm64-cross \
             && rm -rf /var/lib/apt/lists/* ;; \
      *) echo "unsupported architecture: $arch" >&2; exit 1 ;; \
    esac
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    t="$(cat /triple)" \
 && rustup target add "$t" \
 && cargo build --release --locked --target "$t" -p cairn-server -p cairn-bench \
 && install -D "target/$t/release/cairn-server" /out/cairn-server \
 && install -D "target/$t/release/cairn-bench" /out/cairn-bench

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /data cairn \
 && mkdir -p /data /etc/cairn \
 && chown cairn /data
COPY --from=build /out/ /usr/local/bin/
COPY docker/entrypoint.sh /usr/local/bin/cairn-entrypoint
COPY docker/schema.json /etc/cairn/schema.json
USER cairn
VOLUME /data
EXPOSE 7100 7200
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/cairn-entrypoint"]
