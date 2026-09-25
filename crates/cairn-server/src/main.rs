//! Cairn node binary.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use anyhow::Context;
use cairn_core::{HashMap, NodeId, Schema};
use cairn_server::{Node, NodeConfig};
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "cairn-server", about = "A Cairn node")]
struct Cli {
    /// This node's id.
    #[arg(long)]
    node_id: u32,
    /// Listen address.
    #[arg(long)]
    listen: SocketAddr,
    /// Peers as `id=addr` (repeatable; include this node).
    #[arg(long = "peer")]
    peers: Vec<String>,
    /// Data directory.
    #[arg(long)]
    data: PathBuf,
    /// Schema JSON file.
    #[arg(long)]
    schema: PathBuf,
    /// Shards.
    #[arg(long, default_value_t = 4)]
    shards: u32,
    /// Replicas per shard (0: every node hosts every shard).
    #[arg(long, default_value_t = 0)]
    replication: usize,
    /// Executor threads.
    #[arg(long, default_value_t = 2)]
    cores: usize,
    /// Memtable flush threshold in bytes.
    #[arg(long, default_value_t = 64 << 20)]
    memtable_bytes: usize,
    /// Compact a shard once it holds more than this many segments.
    #[arg(long, default_value_t = 8)]
    max_segments: usize,
    /// Keep only SQ8 codes and graphs of loaded segments in memory (no exact f32 rerank).
    #[arg(long)]
    sq8_only: bool,
    /// Build disk-resident vector indexes (Vamana + PQ in RAM, blocks on disk; ADR 0014).
    #[arg(long)]
    disk_index: bool,
    /// Vamana build passes (1: faster build; 2: DiskANN default).
    #[arg(long, default_value_t = 2)]
    vamana_passes: u32,
    /// Tiered compaction target in live rows per merged segment (0: pairwise policy only).
    #[arg(long, default_value_t = 0)]
    target_segment_rows: u32,
    /// Concurrent index builds (flushes and compactions) on this node across all shards.
    #[arg(long, default_value_t = 2)]
    compaction_slots: usize,
    /// Flush an idle memtable after this many milliseconds without writes (0: never).
    #[arg(long, default_value_t = 5000)]
    idle_flush_ms: u64,
    /// Every replica builds every flushed segment itself instead of fetching the leader's
    /// (ADR 0016).
    #[arg(long)]
    no_ship_segments: bool,
    /// Cluster CA certificate (PEM). With --tls-cert and --tls-key, every connection uses
    /// mutual TLS (ADR 0018). Without them the node runs in plaintext (development only).
    #[arg(long)]
    tls_ca: Option<PathBuf>,
    /// This node's certificate chain (PEM), valid for `node-<id>.cairn`.
    #[arg(long)]
    tls_cert: Option<PathBuf>,
    /// This node's private key (PEM, PKCS#8).
    #[arg(long)]
    tls_key: Option<PathBuf>,
    /// Let application clients connect without a certificate (peers always need one).
    #[arg(long)]
    tls_anonymous_clients: bool,
    /// Threads per index build, flush or compaction (0: hardware threads / (2 x
    /// --compaction-slots), so builds use at most half the machine). Builds give the same
    /// segments whatever the count (ADR 0019).
    #[arg(long, default_value_t = 0)]
    build_threads: usize,
    /// Do not move shard leaderships to their preferred replicas (ADR 0020).
    #[arg(long)]
    no_leader_balancing: bool,
    /// Raft tick in milliseconds.
    #[arg(long, default_value_t = 50)]
    tick_ms: u64,
    /// Serve the HTTP/JSON API on this address (ADR 0023). Off by default.
    #[arg(long)]
    http_listen: Option<SocketAddr>,
    /// Allow the HTTP API on a node that uses mutual TLS. The HTTP port has no TLS and no
    /// authentication: bind it to a trusted interface or put an authenticating proxy in front.
    #[arg(long)]
    http_allow_plaintext: bool,
    /// Largest HTTP request body, in bytes.
    #[arg(long, default_value_t = 64 << 20)]
    http_max_body: usize,
    /// Drop this fraction of node messages (tests).
    #[arg(long, default_value_t = 0.0)]
    drop_prob: f64,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    let mut peers = HashMap::default();
    for p in &cli.peers {
        let (id, addr) = p.split_once('=').context("peer must be id=addr")?;
        peers.insert(NodeId(id.parse()?), addr.parse::<SocketAddr>()?);
    }
    let schema: Schema =
        serde_json::from_slice(&std::fs::read(&cli.schema).context("reading schema")?)
            .context("parsing schema")?;
    let tls = match (&cli.tls_ca, &cli.tls_cert, &cli.tls_key) {
        (Some(ca), Some(cert), Some(key)) => Some(
            cairn_runtime::tls::NodeTls::from_pem_files(ca, cert, key, cli.tls_anonymous_clients)
                .context("loading TLS material")?,
        ),
        (None, None, None) => {
            eprintln!(
                "WARNING: no --tls-ca/--tls-cert/--tls-key: node traffic and client traffic are \
                 plaintext and unauthenticated (development only)"
            );
            None
        }
        _ => anyhow::bail!("--tls-ca, --tls-cert and --tls-key go together"),
    };
    let http_tls = match (&cli.http_listen, &tls) {
        (Some(_), Some(_)) if !cli.http_allow_plaintext => anyhow::bail!(
            "--http-listen serves plaintext without authentication, which would bypass mutual \
             TLS: add --http-allow-plaintext to confirm (bind it to a trusted interface)"
        ),
        (Some(_), Some(_)) => Some(
            cairn_runtime::tls::ClientTls::from_pem_files(
                cli.tls_ca.as_deref().expect("checked"),
                Some((
                    cli.tls_cert.as_deref().expect("checked"),
                    cli.tls_key.as_deref().expect("checked"),
                )),
            )
            .context("loading TLS material for the HTTP API")?,
        ),
        _ => None,
    };
    let http_nodes = peers.clone();
    let http_schema = schema.clone();
    let node = Node::start(NodeConfig {
        id: NodeId(cli.node_id),
        listen: cli.listen,
        peers,
        data_dir: cli.data,
        shards: cli.shards,
        replication: cli.replication,
        cores: cli.cores,
        schema,
        memtable_max_bytes: cli.memtable_bytes,
        max_segments: cli.max_segments,
        sq8_only: cli.sq8_only,
        disk_index: cli.disk_index,
        vamana_passes: cli.vamana_passes,
        target_segment_rows: cli.target_segment_rows,
        compaction_slots: cli.compaction_slots,
        idle_flush_ms: cli.idle_flush_ms,
        ship_segments: !cli.no_ship_segments,
        tls,
        build_threads: cli.build_threads,
        leader_balancing: !cli.no_leader_balancing,
        tick_ms: cli.tick_ms,
        drop_prob: cli.drop_prob,
    })?;
    eprintln!(
        "cairn-server node {} listening on {} ({} cores)",
        node.id(),
        node.addr(),
        node.cores()
    );
    if let Some(listen) = cli.http_listen {
        let addr = cairn_server::http::start(cairn_server::http::HttpConfig {
            listen,
            nodes: http_nodes,
            schema: http_schema,
            tls: http_tls,
            max_body_bytes: cli.http_max_body,
        })?;
        eprintln!("cairn-server node {} serving HTTP on {addr}", node.id());
    }
    node.join();
    Ok(())
}
