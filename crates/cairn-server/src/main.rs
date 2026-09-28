//! Cairn node binary.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use anyhow::Context;

/// mimalloc instead of glibc malloc (ADR 0029): glibc's heap, fragmented by index builds, made
/// per-query allocations slow after a long ingest and kept freed memory.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
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
    /// Threads per index build, flush or compaction (0: hardware threads /
    /// --compaction-slots). Build threads run at a low priority, behind serving. Builds give
    /// the same segments whatever the count (ADR 0019).
    #[arg(long, default_value_t = 0)]
    build_threads: usize,
    /// Start with merges paused: no new merge starts until resumed (`POST /v1/admin/merges`
    /// or `cairn-bench merges resume`). Merges already committed still complete.
    #[arg(long)]
    merges_paused: bool,
    /// Do not move shard leaderships to their preferred replicas (ADR 0020).
    #[arg(long)]
    no_leader_balancing: bool,
    /// Raft tick in milliseconds.
    #[arg(long, default_value_t = 50)]
    tick_ms: u64,
    /// Serve the HTTP/JSON API on this address (ADR 0023). Off by default.
    #[arg(long)]
    http_listen: Option<SocketAddr>,
    /// Allow plain HTTP (no --http-tls-cert) on a node that uses mutual TLS between nodes.
    #[arg(long)]
    http_allow_plaintext: bool,
    /// API keys file (ADR 0030): `{"keys":[{"id","sha256","roles"}]}`, digests only. Create
    /// entries with `cairn-server keygen <id> <roles>`. The environment variable
    /// CAIRN_HTTP_ADMIN_KEY adds an admin key given in clear (same on every node).
    #[arg(long)]
    http_keys: Option<PathBuf>,
    /// If the --http-keys file does not exist, create it with a new admin key and print that key
    /// once on stderr (first start of a development node).
    #[arg(long)]
    http_generate_admin_key: bool,
    /// Serve the HTTP API without authentication (development only; refused otherwise).
    #[arg(long)]
    http_insecure_dev: bool,
    /// TLS certificate chain (PEM) for the HTTP port.
    #[arg(long)]
    http_tls_cert: Option<PathBuf>,
    /// TLS private key (PEM) for the HTTP port.
    #[arg(long)]
    http_tls_key: Option<PathBuf>,
    /// Largest HTTP request body, in bytes.
    #[arg(long, default_value_t = 64 << 20)]
    http_max_body: usize,
    /// Drop this fraction of node messages (tests).
    #[arg(long, default_value_t = 0.0)]
    drop_prob: f64,
}

/// `cairn-server keygen <id> <roles>`: prints a new API key and its keys-file entry.
fn keygen(args: &[String]) -> anyhow::Result<()> {
    let [id, roles] = args else {
        anyhow::bail!(
            "usage: cairn-server keygen <id> <roles, comma-separated: read,write,takedown,admin>"
        );
    };
    let roles: Vec<cairn_server::auth::Role> = roles
        .split(',')
        .map(cairn_server::auth::Role::parse)
        .collect::<anyhow::Result<_>>()?;
    let (secret, key) = cairn_server::auth::generate(id, &roles)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({ "key": secret, "entry": key.entry() }))?
    );
    eprintln!(
        "Store the key now: it is not saved anywhere. Add the entry to the --http-keys file."
    );
    Ok(())
}

/// The keys the HTTP API accepts, from --http-keys and CAIRN_HTTP_ADMIN_KEY.
fn http_keys(cli: &Cli) -> anyhow::Result<cairn_server::auth::ApiKeys> {
    use cairn_server::auth::{ApiKeys, Role, generate};
    let mut keys = match &cli.http_keys {
        Some(path) if path.exists() => ApiKeys::load(path)?,
        Some(path) if cli.http_generate_admin_key => {
            let (secret, key) = generate("admin", &[Role::Admin])?;
            let json = serde_json::to_string_pretty(&serde_json::json!({ "keys": [key.entry()] }))?;
            std::fs::write(path, json).with_context(|| format!("writing {}", path.display()))?;
            eprintln!(
                "Generated an admin API key (shown once, only its digest is kept in {}):\n  {secret}",
                path.display()
            );
            ApiKeys::load(path)?
        }
        Some(path) => anyhow::bail!("--http-keys {} does not exist", path.display()),
        None if cli.http_generate_admin_key => {
            anyhow::bail!("--http-generate-admin-key needs --http-keys <file>")
        }
        None => ApiKeys::default(),
    };
    if let Ok(secret) = std::env::var("CAIRN_HTTP_ADMIN_KEY")
        && !secret.is_empty()
    {
        keys.add_plain("admin-env", &secret, &[Role::Admin])?;
    }
    Ok(keys)
}

fn main() -> anyhow::Result<()> {
    // The takedown audit trail is on unless RUST_LOG says otherwise about it.
    let mut filter = tracing_subscriber::EnvFilter::from_default_env();
    if !std::env::var("RUST_LOG").is_ok_and(|v| v.contains("audit")) {
        filter = filter.add_directive("cairn_server::audit=info".parse()?);
    }
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("keygen") {
        return keygen(&args[2..]);
    }
    let cli = Cli::parse();
    if let Err(e) = cairn_runtime::raise_open_files_limit() {
        tracing::warn!("could not raise the open files limit: {e}");
    }
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
                "WARNING: no --tls-ca/--tls-cert/--tls-key: node traffic and binary-protocol \
                 clients are plaintext and unauthenticated (development only)"
            );
            None
        }
        _ => anyhow::bail!("--tls-ca, --tls-cert and --tls-key go together"),
    };
    let https = match (&cli.http_tls_cert, &cli.http_tls_key) {
        (Some(c), Some(k)) => Some(
            cairn_runtime::tls::https_server_config(c, k)
                .context("loading the HTTP TLS certificate")?,
        ),
        (None, None) => None,
        _ => anyhow::bail!("--http-tls-cert and --http-tls-key go together"),
    };
    let auth = http_keys(&cli)?;
    if cli.http_listen.is_some() {
        if auth.is_empty() && !cli.http_insecure_dev {
            anyhow::bail!(
                "the HTTP API needs API keys (ADR 0030): --http-keys <file> (see `cairn-server \
                 keygen`), --http-keys <file> --http-generate-admin-key on a first start, or \
                 CAIRN_HTTP_ADMIN_KEY; --http-insecure-dev serves it without authentication"
            );
        }
        if cli.http_insecure_dev {
            eprintln!(
                "WARNING: --http-insecure-dev: the HTTP API accepts every request without a key \
                 (development only)"
            );
        } else if https.is_none() {
            eprintln!(
                "WARNING: the HTTP API is plain HTTP: API keys travel in clear; add \
                 --http-tls-cert/--http-tls-key or put a TLS proxy in front"
            );
        }
    }
    let http_tls = match (&cli.http_listen, &tls) {
        (Some(_), Some(_)) if https.is_none() && !cli.http_allow_plaintext => anyhow::bail!(
            "--http-listen serves plain HTTP next to mutual TLS between nodes: add \
             --http-tls-cert/--http-tls-key, or --http-allow-plaintext to confirm (bind it to a \
             trusted interface)"
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
        merges_paused: cli.merges_paused,
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
            job_slots: node.job_slots(),
            auth: (!cli.http_insecure_dev).then(|| std::sync::Arc::new(auth)),
            https,
        })?;
        eprintln!("cairn-server node {} serving HTTP on {addr}", node.id());
    }
    node.join();
    Ok(())
}
