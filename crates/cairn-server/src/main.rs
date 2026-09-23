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
    /// Executor threads.
    #[arg(long, default_value_t = 2)]
    cores: usize,
    /// Memtable flush threshold in bytes.
    #[arg(long, default_value_t = 64 << 20)]
    memtable_bytes: usize,
    /// Compact a shard once it holds more than this many segments.
    #[arg(long, default_value_t = 8)]
    max_segments: usize,
    /// Raft tick in milliseconds.
    #[arg(long, default_value_t = 50)]
    tick_ms: u64,
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
    let node = Node::start(NodeConfig {
        id: NodeId(cli.node_id),
        listen: cli.listen,
        peers,
        data_dir: cli.data,
        shards: cli.shards,
        cores: cli.cores,
        schema,
        memtable_max_bytes: cli.memtable_bytes,
        max_segments: cli.max_segments,
        tick_ms: cli.tick_ms,
        drop_prob: cli.drop_prob,
    })?;
    eprintln!(
        "cairn-server node {} listening on {} ({} cores)",
        node.id(),
        node.addr(),
        node.cores()
    );
    node.join();
    Ok(())
}
