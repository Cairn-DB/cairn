# Cairn server image. Build from the repository root:
#   podman build -f docker/Dockerfile -t cairn:dev .   (or docker build ...)
# Run one development node:
#   podman run -p 7100:7100 -v cairn-data:/data cairn:dev
FROM docker.io/library/rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p cairn-server -p cairn-bench \
 && install -D target/release/cairn-server /out/cairn-server \
 && install -D target/release/cairn-bench /out/cairn-bench

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
EXPOSE 7100
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/cairn-entrypoint"]
