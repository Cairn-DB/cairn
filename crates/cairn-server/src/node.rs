//! Node assembly: core threads, shard replicas, node dispatcher, client coordinator.

use crate::catalog::{self, CATALOG_SHARD, CollectionDef};
use bytes::Bytes;
use cairn_core::{DocId, Document, Error, HashMap, NodeId, Result, Runtime as _, Schema, ShardId};
use cairn_proto::{Request, Response};
use cairn_query::{
    Applied, Consistency, EngineConfig, Hit, LegList, Query, Replica, ReplicaConfig, ReplicaHandle,
    ReplicaStatus, Token, fuse,
};
use cairn_runtime::pool::{PoolDisk, PoolRuntime, ThreadReactor, offload};
use cairn_runtime::tcp::ClientConn;
use cairn_runtime::{
    CrossQueue, CrossReceiver, CrossSender, Executor, TcpNetwork, TcpNetworkConfig, cross_oneshot,
};
use cairn_storage::{Command, DeleteScope, LogConfig, PatchOp, PatchTarget, StoreConfig};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

/// A deletion check's documents on one shard: ids, text ids, and their positions in the
/// request.
type CheckGroup = (Vec<DocId>, Vec<String>, Vec<usize>);

/// Serializes catalog changes on this node (the catalog leader).
struct CatalogLock;

static CATALOG_BUSY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

impl CatalogLock {
    async fn acquire(rt: &Runtime) -> CatalogLock {
        while CATALOG_BUSY.swap(true, std::sync::atomic::Ordering::AcqRel) {
            rt.sleep(cairn_core::Duration::from_millis(10)).await;
        }
        CatalogLock
    }
}

impl Drop for CatalogLock {
    fn drop(&mut self) {
        CATALOG_BUSY.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// The replicas one core runs, by shard; collections add and remove them at run time.
type Handles = Rc<RefCell<HashMap<ShardId, ReplicaHandle>>>;

/// Node configuration.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// This node.
    pub id: NodeId,
    /// Listen address.
    pub listen: SocketAddr,
    /// All nodes (including this one).
    pub peers: HashMap<NodeId, SocketAddr>,
    /// Data directory.
    pub data_dir: PathBuf,
    /// Shards per collection.
    pub shards: u32,
    /// Replicas per shard (0 or at least the node count: every node hosts every shard).
    /// Shard `s` lives on `replication` consecutive nodes (by id) starting at `s mod nodes`,
    /// so a cluster of N nodes holds `replication / N` of the data per node.
    pub replication: usize,
    /// Executor threads.
    pub cores: usize,
    /// Collection schema.
    pub schema: Schema,
    /// Memtable flush threshold.
    pub memtable_max_bytes: usize,
    /// Compact a shard once it holds more than this many segments.
    pub max_segments: usize,
    /// Keep only SQ8 codes and graphs of loaded segments in memory (no f32 rerank).
    pub sq8_only: bool,
    /// Build disk-resident vector indexes (Vamana + PQ, ADR 0014) for new segments.
    pub disk_index: bool,
    /// Vamana build passes (1 is about 1.5x faster; 2 is the DiskANN default).
    pub vamana_passes: u32,
    /// Tiered compaction target (live rows per merged segment; 0: pairwise policy only).
    pub target_segment_rows: u32,
    /// Concurrent compactions allowed on this node, across all its shards.
    pub compaction_slots: usize,
    /// Flush an idle memtable after this many milliseconds without writes (0: never).
    pub idle_flush_ms: u64,
    /// Followers fetch leader-built segments instead of building them (ADR 0016).
    pub ship_segments: bool,
    /// mTLS for all connections (ADR 0018); `None`: plaintext (development only).
    pub tls: Option<cairn_runtime::tls::NodeTls>,
    /// Threads per index build (ADR 0019); 0: [`auto_build_threads`].
    pub build_threads: usize,
    /// Hand each shard's leadership to its first host in the placement (ADR 0020).
    pub leader_balancing: bool,
    /// Raft tick in milliseconds.
    pub tick_ms: u64,
    /// Message drop probability (tests).
    pub drop_prob: f64,
    /// Start with merges paused ([`Node::set_merges_paused`]).
    pub merges_paused: bool,
    /// Retention field of the `default` collection (ADR 0031).
    pub expires_field: Option<String>,
    /// How often shard leaders delete expired documents, in milliseconds.
    pub retention_interval_ms: u64,
}

enum CoreRequest {
    Net {
        shard: ShardId,
        from: NodeId,
        bytes: Bytes,
    },
    Propose {
        shard: ShardId,
        cmd: Command,
        reply: CrossSender<Result<Applied>>,
    },
    Get {
        shard: ShardId,
        target: ReadTarget,
        consistency: Consistency,
        reply: CrossSender<Result<Option<Document>>>,
    },
    QueryLegs {
        shard: ShardId,
        query: Query,
        consistency: Consistency,
        reply: CrossSender<Result<Vec<LegList>>>,
    },
    Status {
        shard: ShardId,
        reply: CrossSender<Option<ReplicaStatus>>,
    },
    /// Reads every catalog entry from this node's catalog replica.
    CatalogRead {
        consistency: Consistency,
        reply: CrossSender<Result<Vec<Document>>>,
    },
    /// Starts this node's replica of a collection shard (idempotent): `true` once running.
    Start {
        shard: ShardId,
        def: Arc<CollectionDef>,
        reply: CrossSender<bool>,
    },
    /// Which of these documents this node's replica holds, once applied up to `min_index`.
    Check {
        shard: ShardId,
        targets: Vec<ReadTarget>,
        min_index: cairn_core::LogIndex,
        reply: CrossSender<Result<(cairn_core::LogIndex, Vec<bool>)>>,
    },
    /// Patches documents of a shard this node leads (ADR 0031).
    Patch {
        shard: ShardId,
        ops: Vec<PatchOp>,
        guard: Option<cairn_core::Predicate>,
        reply: CrossSender<Result<Applied>>,
    },
    /// Stops this node's replica of a shard, if it runs one; answers once its files are closed.
    Stop {
        shard: ShardId,
        reply: CrossSender<()>,
    },
}

/// What a point read targets: an internal id, or a text id resolved by the shard (ADR 0031).
#[derive(Debug, Clone)]
enum ReadTarget {
    Id(DocId),
    Key(String),
}

/// Checks documents before they are proposed (a document the log cannot apply is refused here,
/// with a message, rather than skipped by every replica). A client may send only the fields of
/// its own schema: the reserved ones (ADR 0031) are then appended, empty.
fn prepare_docs(schema: &Schema, docs: Vec<Document>, keyed: bool) -> Result<Vec<Document>> {
    let n = schema.fields.len();
    let user = schema
        .fields
        .iter()
        .filter(|f| !cairn_core::schema::is_reserved(&f.name))
        .count();
    let key_field = schema.index_of(cairn_core::schema::KEY_FIELD);
    docs.into_iter()
        .map(|mut d| {
            if d.values.len() == user && user < n {
                d.values.resize(n, None);
            }
            if !keyed {
                if d.id.is_keyed() {
                    return Err(Error::InvalidRequest(format!(
                        "document {}: integer ids are below 2^63",
                        d.id
                    )));
                }
                if key_field.is_some_and(|f| matches!(d.values.get(f), Some(Some(_)))) {
                    return Err(Error::InvalidRequest(format!(
                        "document {}: an integer-id document cannot set a text id",
                        d.id
                    )));
                }
            }
            d.validate(schema)
                .map_err(|e| Error::InvalidRequest(e.to_string()))?;
            Ok(d)
        })
        .collect()
}

/// Checks patches against the schema before they are sent (ADR 0031): known fields, values
/// of the field's kind, and no reserved field (a text id or a tenant is not patched).
fn check_patch(schema: &Schema, ops: &[PatchOp]) -> Result<()> {
    for op in ops {
        for (f, v) in &op.set {
            let field = schema
                .fields
                .get(*f as usize)
                .ok_or_else(|| Error::InvalidRequest(format!("patch: no field {f}")))?;
            if cairn_core::schema::is_reserved(&field.name) {
                return Err(Error::InvalidRequest(format!(
                    "patch: field {:?} is reserved",
                    field.name
                )));
            }
            if let Some(v) = v
                && !v.matches(&field.kind)
            {
                return Err(Error::InvalidRequest(format!(
                    "patch: wrong value for field {:?}",
                    field.name
                )));
            }
        }
    }
    Ok(())
}

type Runtime = PoolRuntime<TcpNetwork>;
type Queues = Arc<Vec<CrossQueue<CoreRequest>>>;

/// Connections to peers used to forward requests to shard leaders (one node per process).
static PEER_CONNS: std::sync::OnceLock<Mutex<HashMap<NodeId, Arc<ClientConn>>>> =
    std::sync::OnceLock::new();

/// The socket layer shared by the cores of this process (one node per process).
static SHARED_NET: std::sync::OnceLock<Mutex<Option<TcpNetwork>>> = std::sync::OnceLock::new();

fn peer_conns() -> &'static Mutex<HashMap<NodeId, Arc<ClientConn>>> {
    PEER_CONNS.get_or_init(|| Mutex::new(HashMap::default()))
}

fn shared_net() -> &'static Mutex<Option<TcpNetwork>> {
    SHARED_NET.get_or_init(|| Mutex::new(None))
}

/// A running node.
pub struct Node {
    cfg: NodeConfig,
    net: TcpNetwork,
    cores: usize,
    threads: Vec<std::thread::JoinHandle<()>>,
}

/// Threads per build when not set: all concurrent builds together can use the whole machine.
/// They run at a low priority ([`BUILD_NICE`]), so serving takes the CPU it needs first. At
/// normal priority, 4 slots of 8 threads on an 8-vCPU VM starved the Raft actors (GCP 50M run,
/// 2026-09-25); the half-machine cap that followed left builds, the ingest bottleneck, on half
/// the CPU (GCP run 7, 2026-09-26).
pub fn auto_build_threads(hardware_threads: usize, slots: usize) -> usize {
    (hardware_threads / slots.max(1)).max(1)
}

/// Nice value of build threads: under contention a normal-priority thread gets about ten
/// times their CPU share, and they still progress when serving is busy.
pub const BUILD_NICE: i32 = 10;

/// Compaction slots shared by every core of this process (one node per process).
fn job_slots(max: usize) -> std::sync::Arc<cairn_query::JobSlots> {
    static SLOTS: std::sync::OnceLock<std::sync::Arc<cairn_query::JobSlots>> =
        std::sync::OnceLock::new();
    SLOTS
        .get_or_init(|| std::sync::Arc::new(cairn_query::JobSlots::new(max)))
        .clone()
}

/// Nodes hosting `shard`, in placement order.
pub fn placement(cfg: &NodeConfig, shard: ShardId) -> Vec<NodeId> {
    let mut nodes: Vec<NodeId> = cfg.peers.keys().copied().collect();
    nodes.sort();
    let n = nodes.len();
    if n == 0 {
        return Vec::new();
    }
    let rf = if cfg.replication == 0 {
        n
    } else {
        cfg.replication.min(n)
    };
    if shard == CATALOG_SHARD {
        // The catalog lives on every node, so every node can read it locally.
        return nodes;
    }
    let start = shard.get() as usize % n;
    (0..rf).map(|j| nodes[(start + j) % n]).collect()
}

/// The collection given at startup.
fn default_collection(cfg: &NodeConfig) -> CollectionDef {
    CollectionDef {
        name: catalog::DEFAULT.into(),
        id: 0,
        base: 0,
        shards: cfg.shards,
        schema: cfg.schema.clone(),
        expires_field: cfg.expires_field.clone(),
    }
}

fn hosts(cfg: &NodeConfig, shard: ShardId) -> bool {
    placement(cfg, shard).contains(&cfg.id)
}

/// Index build threads for one replica (ADR 0019).
fn build_pool(cfg: &NodeConfig) -> std::sync::Arc<cairn_runtime::ThreadParallel> {
    std::sync::Arc::new(cairn_runtime::ThreadParallel::background(
        if cfg.build_threads == 0 {
            auto_build_threads(
                std::thread::available_parallelism().map_or(1, |n| n.get()),
                cfg.compaction_slots,
            )
        } else {
            cfg.build_threads
        },
        BUILD_NICE,
    ))
}

fn core_of(shard: ShardId, cores: usize) -> usize {
    shard.get() as usize % cores.max(1)
}

impl Node {
    /// Starts the node: core 0 binds the listener and publishes the socket layer; every core
    /// builds its own executor and replicas.
    pub fn start(cfg: NodeConfig) -> Result<Node> {
        std::fs::create_dir_all(&cfg.data_dir)
            .map_err(|e| Error::io(cairn_core::error::IoErrorKind::Other, e))?;
        if cfg.shards == 0 || cfg.shards > catalog::MAX_SHARD {
            return Err(Error::InvalidRequest(format!(
                "--shards must be between 1 and {}",
                catalog::MAX_SHARD
            )));
        }
        default_collection(&cfg).check_retention()?;
        catalog::install(catalog::from_entries(default_collection(&cfg), &[]));
        job_slots(cfg.compaction_slots).set_merges_paused(cfg.merges_paused);
        let cores_n = cfg.cores.max(1);
        let queues: Queues = Arc::new((0..cores_n).map(|_| CrossQueue::new()).collect());
        let (net_tx, net_rx) = std::sync::mpsc::channel::<Result<TcpNetwork>>();
        let mut threads = Vec::new();
        for core in 0..cores_n {
            let cfg2 = cfg.clone();
            let all_queues = queues.clone();
            let tx = net_tx.clone();
            let t = std::thread::Builder::new()
                .name(format!("core-{core}"))
                .spawn(move || Self::run_core(core, cfg2, all_queues, tx))
                .expect("spawn core thread");
            threads.push(t);
            if core == 0 {
                let net = net_rx
                    .recv()
                    .map_err(|_| Error::Internal("core 0 did not start".into()))??;
                *shared_net().lock().expect("net share") = Some(net);
            }
        }
        let net = shared_net()
            .lock()
            .expect("net share")
            .clone()
            .ok_or_else(|| Error::Internal("no network".into()))?;
        Ok(Node {
            cfg,
            net,
            cores: cores_n,
            threads,
        })
    }

    fn run_core(
        core: usize,
        cfg: NodeConfig,
        queues: Queues,
        tx: std::sync::mpsc::Sender<Result<TcpNetwork>>,
    ) {
        let mut ex = Executor::new(ThreadReactor::new());
        let net = if core == 0 {
            let r = TcpNetwork::start(
                TcpNetworkConfig {
                    id: cfg.id,
                    listen: cfg.listen,
                    peers: cfg.peers.iter().map(|(k, v)| (*k, *v)).collect(),
                    drop_prob: cfg.drop_prob,
                    tls: cfg.tls.clone(),
                    segment_version_max: cairn_storage::segment::SEGMENT_VERSION,
                },
                ex.reactor().completer(),
            );
            let _ = tx.send(
                r.as_ref()
                    .map(Clone::clone)
                    .map_err(|e| Error::Internal(e.to_string())),
            );
            match r {
                Ok(n) => n,
                Err(_) => return,
            }
        } else {
            match shared_net().lock().expect("net share").clone() {
                Some(n) => n,
                None => return,
            }
        };
        let disk = PoolDisk::new(&cfg.data_dir, ex.reactor().completer()).expect("data dir");
        let rt = PoolRuntime::new(ex.handle(), disk, net);
        let handles: Handles = Rc::new(RefCell::new(ex.block_on(Self::spawn_replicas(
            rt.clone(),
            cfg.clone(),
            core,
        ))));
        let queue = queues[core].clone();
        let (rt2, h2, cfg2) = (rt.clone(), handles.clone(), cfg.clone());
        rt.spawn(async move {
            while let Some(req) = queue.pop().await {
                Self::serve(&rt2, &cfg2, &h2, req);
            }
        });
        if core == 0 {
            Self::spawn_dispatcher(&rt, queues.clone(), cfg.cores.max(1));
            Self::spawn_reconciler(&rt, queues.clone(), cfg.clone());
            Self::spawn_retention(&rt, queues.clone(), cfg.clone());
            Self::spawn_coordinator(&rt, queues, cfg);
        }
        let _ = ex.run();
    }

    /// Replica settings for one shard, stored under `dir`.
    fn replica_config(cfg: &NodeConfig, shard: ShardId, dir: String) -> ReplicaConfig {
        ReplicaConfig {
            shard,
            id: cfg.id,
            peers: placement(cfg, shard),
            tick: cairn_core::Duration::from_millis(cfg.tick_ms),
            election_ticks: 10,
            heartbeat_ticks: 2,
            engine: EngineConfig {
                store: StoreConfig {
                    memtable_max_bytes: cfg.memtable_max_bytes,
                    max_segments: cfg.max_segments,
                    target_segment_rows: cfg.target_segment_rows,
                    log: LogConfig::default(),
                    shard: shard.get(),
                    ..StoreConfig::default()
                },
                vector: cairn_index::VectorIndexParams {
                    keep_f32: !cfg.sq8_only,
                    disk: cfg.disk_index,
                    vamana: cairn_index::diskann::VamanaParams {
                        passes: cfg.vamana_passes,
                        ..Default::default()
                    },
                    ..cairn_index::VectorIndexParams::default()
                },
            },
            dir,
            seed: u64::from(cfg.id.get()) * 1000 + u64::from(shard.get()),
            own_receiver: false,
            idle_flush_ticks: (cfg.idle_flush_ms / cfg.tick_ms.max(1)) as u32,
            compaction_slots: Some(job_slots(cfg.compaction_slots)),
            ship_segments: cfg.ship_segments,
            preferred_leader: cfg
                .leader_balancing
                .then(|| placement(cfg, shard).first().copied())
                .flatten(),
            build_parallel: Some(build_pool(cfg)),
        }
    }

    /// The replicas a core starts with: its shards of `default`, and the catalog. Other
    /// collections are started by the reconciler once the catalog is read.
    async fn spawn_replicas(
        rt: Runtime,
        cfg: NodeConfig,
        core: usize,
    ) -> HashMap<ShardId, ReplicaHandle> {
        let mut handles = HashMap::default();
        let default = default_collection(&cfg);
        let mut wanted: Vec<(ShardId, String, Schema)> = default
            .shard_ids()
            .map(|s| (s, default.shard_dir(s), cfg.schema.clone()))
            .collect();
        wanted.push((CATALOG_SHARD, "catalog".into(), catalog::catalog_schema()));
        for (shard, dir, schema) in wanted {
            if core_of(shard, cfg.cores) != core || !hosts(&cfg, shard) {
                continue;
            }
            let rc = Self::replica_config(&cfg, shard, dir);
            match Replica::spawn(rt.clone(), rc, schema).await {
                Ok(h) => {
                    handles.insert(shard, h);
                }
                Err(e) => tracing::error!(shard = %shard, "cannot start replica: {e}"),
            }
        }
        handles
    }

    fn serve(rt: &Runtime, cfg: &NodeConfig, handles: &Handles, req: CoreRequest) {
        let shard = match &req {
            CoreRequest::Net { shard, .. }
            | CoreRequest::Propose { shard, .. }
            | CoreRequest::Get { shard, .. }
            | CoreRequest::QueryLegs { shard, .. }
            | CoreRequest::Status { shard, .. }
            | CoreRequest::Start { shard, .. }
            | CoreRequest::Patch { shard, .. }
            | CoreRequest::Check { shard, .. }
            | CoreRequest::Stop { shard, .. } => *shard,
            CoreRequest::CatalogRead { .. } => CATALOG_SHARD,
        };
        let req = match req {
            CoreRequest::Start { shard, def, reply } => {
                if handles.borrow().contains_key(&shard) {
                    reply.send(true);
                    return;
                }
                let (rt2, cfg2, handles2) = (rt.clone(), cfg.clone(), handles.clone());
                rt.spawn(async move {
                    let rc = Self::replica_config(&cfg2, shard, def.shard_dir(shard));
                    match Replica::spawn(rt2, rc, def.schema.clone()).await {
                        Ok(h) => {
                            let mut m = handles2.borrow_mut();
                            // A concurrent start of the same shard won: keep it.
                            m.entry(shard).or_insert(h);
                            tracing::info!(node = %cfg2.id, %shard, collection = %def.name, "replica started");
                            reply.send(true);
                        }
                        Err(e) => {
                            tracing::error!(%shard, collection = %def.name, "cannot start replica: {e}");
                            reply.send(false);
                        }
                    }
                });
                return;
            }
            CoreRequest::Stop { shard, reply } => {
                let h = handles.borrow_mut().remove(&shard);
                rt.spawn(async move {
                    if let Some(h) = h {
                        h.stop().await;
                    }
                    reply.send(());
                });
                return;
            }
            other => other,
        };
        let found = handles.borrow().get(&shard).cloned();
        let Some(h) = found else {
            // Not hosted here: point the coordinator at a hosting node (rotating, so reads
            // that any replica may serve spread over the shard's replicas).
            static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let owners = placement(cfg, shard);
            let hint = (!owners.is_empty()).then(|| {
                owners[NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % owners.len()]
            });
            let e = || match hint {
                Some(h) if h != cfg.id => Error::NotLeader {
                    shard,
                    leader_hint: Some(h),
                },
                _ => Error::InvalidRequest(format!("no shard {shard}")),
            };
            match req {
                CoreRequest::Propose { reply, .. } => reply.send(Err(e())),
                CoreRequest::Get { reply, .. } => reply.send(Err(e())),
                CoreRequest::QueryLegs { reply, .. } => reply.send(Err(e())),
                CoreRequest::Status { reply, .. } => reply.send(None),
                CoreRequest::CatalogRead { reply, .. } => reply.send(Err(e())),
                CoreRequest::Patch { reply, .. } => reply.send(Err(e())),
                CoreRequest::Check { reply, .. } => reply.send(Err(e())),
                CoreRequest::Net { .. } | CoreRequest::Start { .. } | CoreRequest::Stop { .. } => {}
            }
            return;
        };
        match req {
            CoreRequest::Net { from, bytes, .. } => h.deliver(from, bytes),
            CoreRequest::Propose { cmd, reply, .. } => {
                rt.spawn(async move { reply.send(h.propose_applied(cmd).await) })
            }
            CoreRequest::Get {
                target,
                consistency,
                reply,
                ..
            } => rt.spawn(async move {
                reply.send(match target {
                    ReadTarget::Id(id) => h.get(id, consistency).await,
                    ReadTarget::Key(k) => h.get_key(k, consistency).await,
                })
            }),
            CoreRequest::QueryLegs {
                query,
                consistency,
                reply,
                ..
            } => rt.spawn(async move { reply.send(h.query_legs(query, consistency).await) }),
            CoreRequest::Status { reply, .. } => {
                rt.spawn(async move { reply.send(h.status().await) })
            }
            CoreRequest::CatalogRead { consistency, reply } => rt.spawn(async move {
                let mut q = Query::new(10_000);
                q.filter = cairn_core::Predicate::In {
                    field: 0,
                    values: vec![
                        cairn_core::Value::Enum("collection".into()),
                        cairn_core::Value::Enum("dropped".into()),
                    ],
                };
                q.with_documents = true;
                reply.send(
                    h.query(q, consistency)
                        .await
                        .map(|hits| hits.into_iter().filter_map(|hit| hit.document).collect()),
                )
            }),
            CoreRequest::Patch {
                ops, guard, reply, ..
            } => rt.spawn(async move { reply.send(h.patch(ops, guard).await) }),
            CoreRequest::Check {
                targets,
                min_index,
                reply,
                ..
            } => {
                let rt2 = rt.clone();
                rt.spawn(async move {
                    let r = async {
                        // Wait (a few seconds at most) for the takedown to reach this replica.
                        let t0 = rt2.now();
                        let mut applied = cairn_core::LogIndex(0);
                        loop {
                            if let Some(st) = h.status().await {
                                applied = st.applied;
                            }
                            if applied >= min_index
                                || rt2.now() - t0 > cairn_core::Duration::from_secs(5)
                            {
                                break;
                            }
                            rt2.sleep(cairn_core::Duration::from_millis(20)).await;
                        }
                        let mut present = Vec::with_capacity(targets.len());
                        for t in targets {
                            let d = match t {
                                ReadTarget::Id(id) => h.get(id, Consistency::Stale).await?,
                                ReadTarget::Key(k) => h.get_key(k, Consistency::Stale).await?,
                            };
                            present.push(d.is_some());
                        }
                        Ok((applied, present))
                    }
                    .await;
                    reply.send(r)
                })
            }
            CoreRequest::Start { .. } | CoreRequest::Stop { .. } => unreachable!("handled above"),
        }
    }

    /// Routes node-to-node frames to the core owning the frame's shard.
    fn spawn_dispatcher(rt: &Runtime, queues: Queues, cores: usize) {
        let rt2 = rt.clone();
        rt.spawn(async move {
            loop {
                let Ok((from, bytes)) = cairn_core::Network::recv(rt2.network()).await else {
                    break;
                };
                if bytes.len() < 4 {
                    continue;
                }
                let shard = ShardId(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
                queues[core_of(shard, cores)].push(CoreRequest::Net { shard, from, bytes });
            }
        });
    }

    /// Accepts client requests and answers them.
    fn spawn_coordinator(rt: &Runtime, queues: Queues, cfg: NodeConfig) {
        let rt2 = rt.clone();
        rt.spawn(async move {
            loop {
                let req = rt2.network().client_recv().await;
                let (q, c, net, rt3) = (
                    queues.clone(),
                    cfg.clone(),
                    rt2.network().clone(),
                    rt2.clone(),
                );
                rt2.spawn(async move {
                    let resp = match Request::from_bytes(&req.payload) {
                        Ok(Request::Forwarded(inner)) => {
                            Self::handle(&rt3, &q, &c, *inner, false).await
                        }
                        Ok(r) => Self::handle(&rt3, &q, &c, r, true).await,
                        Err(e) => Response::from_error(&e),
                    };
                    net.client_reply(req.conn, req.req, resp.to_bytes());
                });
            }
        });
    }

    fn send<T>(
        queues: &Queues,
        cores: usize,
        shard: ShardId,
        make: impl FnOnce(CrossSender<T>) -> CoreRequest,
    ) -> CrossReceiver<T> {
        let (tx, rx) = cross_oneshot();
        queues[core_of(shard, cores)].push(make(tx));
        rx
    }

    /// Forwards `req` for `shard` to `node`. Follows up to three redirections (the node may
    /// not host or lead the shard and answers with a hint), and on a transport failure (a dead
    /// or restarting node) tries the shard's other hosts before giving up.
    async fn forward(
        rt: &Runtime,
        cfg: &NodeConfig,
        node: NodeId,
        shard: ShardId,
        req: Request,
    ) -> Response {
        let mut tried: Vec<NodeId> = Vec::new();
        let mut target = Some(node);
        let mut last = Response::Error {
            message: format!("no reachable host for {shard}"),
            leader_hint: None,
        };
        let mut redirects = 0;
        while let Some(t) = target {
            tried.push(t);
            match Self::forward_once(rt, cfg, t, req.clone()).await {
                Ok(Response::Error {
                    message,
                    leader_hint: Some(h),
                }) if h != t && h != cfg.id && redirects < 3 && !tried.contains(&h) => {
                    redirects += 1;
                    last = Response::Error {
                        message,
                        leader_hint: Some(h),
                    };
                    target = Some(h);
                    continue;
                }
                Ok(resp) => return resp,
                Err(e) => last = Response::from_error(&e),
            }
            target = placement(cfg, shard)
                .into_iter()
                .find(|n| *n != cfg.id && !tried.contains(n));
        }
        last
    }

    /// One forwarded call to `node` over a peer connection (blocking call on a helper thread).
    async fn forward_once(
        rt: &Runtime,
        cfg: &NodeConfig,
        node: NodeId,
        req: Request,
    ) -> Result<Response> {
        let Some(addr) = cfg.peers.get(&node).copied() else {
            return Err(Error::InvalidRequest(format!("unknown node {node}")));
        };
        let bytes = Request::Forwarded(Box::new(req)).to_bytes();
        let tls = cfg.tls.as_ref().map(|t| t.client.clone());
        offload(&rt.completer(), move || {
            let conn = {
                let mut m = peer_conns().lock().expect("peer conns");
                match m.get(&node).filter(|c| !c.is_closed()) {
                    Some(c) => c.clone(),
                    None => {
                        let c = Arc::new(ClientConn::connect_with(
                            addr,
                            tls.as_ref().map(|t| (t, node)),
                        )?);
                        m.insert(node, c.clone());
                        c
                    }
                }
            };
            match conn.call(&bytes, std::time::Duration::from_secs(10)) {
                Ok(b) => Response::from_bytes(&b),
                Err(e) => {
                    peer_conns().lock().expect("peer conns").remove(&node);
                    Err(e)
                }
            }
        })
        .await
    }

    fn not_leader(shard: ShardId, message: &str, hint: Option<NodeId>) -> Error {
        if message.contains("not the leader") {
            Error::NotLeader {
                shard,
                leader_hint: hint,
            }
        } else {
            Error::Internal(message.to_owned())
        }
    }

    /// Proposes on the local replica; on `NotLeader` with a hint, forwards to that node once.
    async fn propose_routed(
        rt: &Runtime,
        queues: &Queues,
        cfg: &NodeConfig,
        coll: &CollectionDef,
        shard: ShardId,
        cmd: Command,
        may_forward: bool,
    ) -> Result<(Vec<Token>, u64)> {
        let cores = cfg.cores.max(1);
        let cmd2 = cmd.clone();
        match Self::send(queues, cores, shard, |reply| CoreRequest::Propose {
            shard,
            cmd: cmd2,
            reply,
        })
        .await
        {
            Some(Ok(a)) => Ok((vec![a.token], a.deleted)),
            Some(Err(Error::NotLeader {
                leader_hint: Some(l),
                ..
            })) if may_forward && l != cfg.id => {
                let req = match cmd {
                    Command::Upsert(docs) => Request::Upsert(docs),
                    Command::Delete(ids) => Request::Delete(ids),
                    Command::UpsertKeyed(docs) => Request::UpsertKeyed(docs),
                    Command::DeleteKeys(keys) => Request::DeleteKeys(keys),
                    Command::DeleteWhere { scope, filter } => Request::DeleteWhere {
                        shard: Some(shard),
                        scope,
                        filter,
                    },
                    // Upkeep entries are proposed by replicas themselves, never forwarded.
                    Command::Noop
                    | Command::FlushBegin
                    | Command::FlushCommit { .. }
                    | Command::CompactCommit { .. } => {
                        return Ok((Vec::new(), 0));
                    }
                };
                match Self::forward(rt, cfg, l, shard, Self::wrap(coll, req)).await {
                    Response::Ack(tokens) => Ok((tokens, 0)),
                    Response::Deleted { count, tokens } => Ok((tokens, count)),
                    Response::Error {
                        message,
                        leader_hint,
                    } => Err(Self::not_leader(shard, &message, leader_hint)),
                    other => Err(Error::Internal(format!(
                        "unexpected forward response {other:?}"
                    ))),
                }
            }
            Some(Err(e)) => Err(e),
            None => Err(Error::Internal("core stopped".into())),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_routed(
        rt: &Runtime,
        queues: &Queues,
        cfg: &NodeConfig,
        coll: &CollectionDef,
        shard: ShardId,
        target: ReadTarget,
        consistency: Consistency,
        may_forward: bool,
    ) -> Response {
        let cores = cfg.cores.max(1);
        let t2 = target.clone();
        match Self::send(queues, cores, shard, |reply| CoreRequest::Get {
            shard,
            target: t2,
            consistency,
            reply,
        })
        .await
        {
            Some(Ok(d)) => Response::Doc(d),
            Some(Err(Error::NotLeader {
                leader_hint: Some(l),
                ..
            })) if may_forward && l != cfg.id => {
                let req = match target {
                    ReadTarget::Id(id) => Request::Get {
                        id,
                        consistency,
                        tokens: Vec::new(),
                    },
                    ReadTarget::Key(key) => Request::GetKey {
                        key,
                        consistency,
                        tokens: Vec::new(),
                    },
                };
                Self::forward(rt, cfg, l, shard, Self::wrap(coll, req)).await
            }
            Some(Err(e)) => Response::from_error(&e),
            None => Response::Error {
                message: "core stopped".into(),
                leader_hint: None,
            },
        }
    }

    /// A request for a collection other than `default` is sent to another node wrapped with
    /// the collection's name, which that node resolves again.
    fn wrap(coll: &CollectionDef, req: Request) -> Request {
        if coll.id == 0 {
            req
        } else {
            Request::In {
                collection: coll.name.clone(),
                req: Box::new(req),
            }
        }
    }

    async fn handle(
        rt: &Runtime,
        queues: &Queues,
        cfg: &NodeConfig,
        req: Request,
        may_forward: bool,
    ) -> Response {
        match req {
            Request::Forwarded(inner) => {
                Box::pin(Self::handle(rt, queues, cfg, *inner, false)).await
            }
            Request::In { collection, req } => {
                match Self::resolve(rt, queues, cfg, &collection).await {
                    Ok(c) => {
                        Box::pin(Self::handle_in(rt, queues, cfg, &c, *req, may_forward)).await
                    }
                    Err(e) => Response::from_error(&e),
                }
            }
            req @ (Request::CreateCollection { .. }
            | Request::DropCollection { .. }
            | Request::ListCollections) => {
                Box::pin(Self::catalog_request(rt, queues, cfg, req, may_forward)).await
            }
            other => {
                let default = catalog::current()
                    .get(catalog::DEFAULT)
                    .expect("the default collection is always known");
                Self::handle_in(rt, queues, cfg, &default, other, may_forward).await
            }
        }
    }

    async fn handle_in(
        rt: &Runtime,
        queues: &Queues,
        cfg: &NodeConfig,
        coll: &CollectionDef,
        req: Request,
        may_forward: bool,
    ) -> Response {
        let cores = cfg.cores.max(1);
        macro_rules! route {
            ($e:expr) => {
                match $e {
                    Ok(s) => s,
                    Err(e) => return Response::from_error(&e),
                }
            };
        }
        match req {
            Request::ReplicaCheck {
                shard,
                ids,
                keys,
                min_index,
            } => {
                let targets: Vec<ReadTarget> = ids
                    .into_iter()
                    .map(ReadTarget::Id)
                    .chain(keys.into_iter().map(ReadTarget::Key))
                    .collect();
                match Self::send(queues, cores, shard, |reply| CoreRequest::Check {
                    shard,
                    targets,
                    min_index,
                    reply,
                })
                .await
                {
                    Some(Ok((applied, present))) => Response::Checked { applied, present },
                    Some(Err(e)) => Response::from_error(&e),
                    None => Response::from_error(&Error::Internal("core stopped".into())),
                }
            }
            Request::DeletionCheck { ids, keys, tokens } => {
                Self::deletion_check(rt, queues, cfg, coll, ids, keys, tokens).await
            }
            Request::Patch { shard, ops } => {
                if let Err(e) = check_patch(&coll.schema, &ops) {
                    return Response::from_error(&e);
                }
                let mut groups: HashMap<ShardId, Vec<PatchOp>> = HashMap::default();
                for op in ops {
                    let s = match (&shard, &op.target) {
                        (Some(s), _) => *s,
                        (None, PatchTarget::Id(id)) => route!(coll.route(*id)),
                        (None, PatchTarget::Key(k)) => coll.route_key(k),
                    };
                    groups.entry(s).or_default().push(op);
                }
                // Expired documents are gone for readers: a patch leaves them alone.
                let guard = coll
                    .expired(rt.unix_millis())
                    .map(|p| cairn_core::Predicate::Not(Box::new(p)));
                let (mut count, mut tokens) = (0, Vec::new());
                for (s, group) in groups {
                    let (g2, ops2) = (guard.clone(), group.clone());
                    let r = Self::send(queues, cores, s, |reply| CoreRequest::Patch {
                        shard: s,
                        ops: ops2,
                        guard: g2,
                        reply,
                    })
                    .await;
                    match r {
                        Some(Ok(a)) => {
                            count += a.deleted;
                            tokens.push(a.token);
                        }
                        Some(Err(Error::NotLeader {
                            leader_hint: Some(l),
                            ..
                        })) if may_forward && l != cfg.id => {
                            let req = Request::Patch {
                                shard: Some(s),
                                ops: group,
                            };
                            match Self::forward(rt, cfg, l, s, Self::wrap(coll, req)).await {
                                Response::Patched {
                                    count: n,
                                    tokens: t,
                                } => {
                                    count += n;
                                    tokens.extend(t);
                                }
                                Response::Error {
                                    message,
                                    leader_hint,
                                } => {
                                    return Response::from_error(&Self::not_leader(
                                        s,
                                        &message,
                                        leader_hint,
                                    ));
                                }
                                other => {
                                    return Response::from_error(&Error::Internal(format!(
                                        "unexpected forward response {other:?}"
                                    )));
                                }
                            }
                        }
                        Some(Err(e)) => return Response::from_error(&e),
                        None => {
                            return Response::from_error(&Error::Internal("core stopped".into()));
                        }
                    }
                }
                tokens.sort();
                Response::Patched { count, tokens }
            }
            Request::Forwarded(_)
            | Request::In { .. }
            | Request::CreateCollection { .. }
            | Request::DropCollection { .. }
            | Request::ListCollections => Response::from_error(&Error::InvalidRequest(
                "this request cannot target a collection".into(),
            )),
            Request::SetMergesPaused(paused) => {
                job_slots(cfg.compaction_slots).set_merges_paused(paused);
                tracing::info!(node = %cfg.id, paused, "merges paused set");
                Response::Ack(Vec::new())
            }
            Request::ShardLegs {
                shard,
                query,
                consistency,
            } => match Self::send(queues, cores, shard, |reply| CoreRequest::QueryLegs {
                shard,
                query,
                consistency,
                reply,
            })
            .await
            {
                Some(Ok(lists)) => Response::Legs(lists),
                Some(Err(e)) => Response::from_error(&e),
                None => Response::Error {
                    message: "core stopped".into(),
                    leader_hint: None,
                },
            },
            Request::Upsert(docs) => {
                let docs = match prepare_docs(&coll.schema, docs, false) {
                    Ok(d) => d,
                    Err(e) => return Response::from_error(&e),
                };
                let mut groups: HashMap<ShardId, Vec<Document>> = HashMap::default();
                for d in docs {
                    groups.entry(route!(coll.route(d.id))).or_default().push(d);
                }
                let mut tokens = Vec::new();
                for (shard, group) in groups {
                    match Self::propose_routed(
                        rt,
                        queues,
                        cfg,
                        coll,
                        shard,
                        Command::Upsert(group),
                        may_forward,
                    )
                    .await
                    {
                        Ok((t, _)) => tokens.extend(t),
                        Err(e) => return Response::from_error(&e),
                    }
                }
                tokens.sort();
                Response::Ack(tokens)
            }
            Request::Delete(ids) => {
                let mut groups: HashMap<ShardId, Vec<DocId>> = HashMap::default();
                for id in ids {
                    groups.entry(route!(coll.route(id))).or_default().push(id);
                }
                let mut tokens = Vec::new();
                for (shard, group) in groups {
                    match Self::propose_routed(
                        rt,
                        queues,
                        cfg,
                        coll,
                        shard,
                        Command::Delete(group),
                        may_forward,
                    )
                    .await
                    {
                        Ok((t, _)) => tokens.extend(t),
                        Err(e) => return Response::from_error(&e),
                    }
                }
                tokens.sort();
                Response::Ack(tokens)
            }
            Request::UpsertKeyed(docs) => {
                let docs = match prepare_docs(&coll.schema, docs, true) {
                    Ok(d) => d,
                    Err(e) => return Response::from_error(&e),
                };
                let Some(field) = coll.schema.index_of(cairn_core::schema::KEY_FIELD) else {
                    return Response::from_error(&Error::InvalidRequest(
                        "this collection has no text ids".into(),
                    ));
                };
                let mut groups: HashMap<ShardId, Vec<Document>> = HashMap::default();
                for d in docs {
                    let key = match d.values.get(field) {
                        Some(Some(cairn_core::Value::Blob(b))) => {
                            std::str::from_utf8(b).ok().map(str::to_owned)
                        }
                        _ => None,
                    };
                    let Some(key) = key.filter(|k| !k.is_empty()) else {
                        return Response::from_error(&Error::InvalidRequest(
                            "a text id must be a non-empty UTF-8 string".into(),
                        ));
                    };
                    groups.entry(coll.route_key(&key)).or_default().push(d);
                }
                let mut tokens = Vec::new();
                for (shard, group) in groups {
                    match Self::propose_routed(
                        rt,
                        queues,
                        cfg,
                        coll,
                        shard,
                        Command::UpsertKeyed(group),
                        may_forward,
                    )
                    .await
                    {
                        Ok((t, _)) => tokens.extend(t),
                        Err(e) => return Response::from_error(&e),
                    }
                }
                tokens.sort();
                Response::Ack(tokens)
            }
            Request::DeleteKeys(keys) => {
                let mut groups: HashMap<ShardId, Vec<String>> = HashMap::default();
                for k in keys {
                    groups.entry(coll.route_key(&k)).or_default().push(k);
                }
                let mut tokens = Vec::new();
                for (shard, group) in groups {
                    match Self::propose_routed(
                        rt,
                        queues,
                        cfg,
                        coll,
                        shard,
                        Command::DeleteKeys(group),
                        may_forward,
                    )
                    .await
                    {
                        Ok((t, _)) => tokens.extend(t),
                        Err(e) => return Response::from_error(&e),
                    }
                }
                tokens.sort();
                Response::Ack(tokens)
            }
            Request::DeleteWhere {
                shard,
                scope,
                filter,
            } => {
                if let Err(e) = filter.validate(&coll.schema) {
                    return Response::from_error(&Error::InvalidRequest(e.to_string()));
                }
                // One command per shard the scope reaches, each resolved by that shard's log.
                let parts: Vec<(ShardId, DeleteScope)> = match (shard, scope) {
                    (Some(s), _) if !coll.owns(s) => {
                        return Response::from_error(&Error::InvalidRequest(format!(
                            "no shard {s}"
                        )));
                    }
                    (Some(s), scope) => vec![(s, scope)],
                    (None, DeleteScope::All) => {
                        coll.shard_ids().map(|s| (s, DeleteScope::All)).collect()
                    }
                    (None, DeleteScope::Ids(ids)) => {
                        let mut groups: HashMap<ShardId, Vec<DocId>> = HashMap::default();
                        for id in ids {
                            groups.entry(route!(coll.route(id))).or_default().push(id);
                        }
                        groups
                            .into_iter()
                            .map(|(s, g)| (s, DeleteScope::Ids(g)))
                            .collect()
                    }
                    (None, DeleteScope::Keys(keys)) => {
                        let mut groups: HashMap<ShardId, Vec<String>> = HashMap::default();
                        for k in keys {
                            groups.entry(coll.route_key(&k)).or_default().push(k);
                        }
                        groups
                            .into_iter()
                            .map(|(s, g)| (s, DeleteScope::Keys(g)))
                            .collect()
                    }
                };
                let mut tokens = Vec::new();
                let mut count = 0;
                for (shard, scope) in parts {
                    let cmd = Command::DeleteWhere {
                        scope,
                        filter: filter.clone(),
                    };
                    match Self::propose_routed(rt, queues, cfg, coll, shard, cmd, may_forward).await
                    {
                        Ok((t, n)) => {
                            tokens.extend(t);
                            count += n;
                        }
                        Err(e) => return Response::from_error(&e),
                    }
                }
                tokens.sort();
                Response::Deleted { count, tokens }
            }
            Request::Get {
                id,
                consistency,
                tokens,
            } => {
                let shard = route!(coll.route(id));
                let consistency = Self::per_shard(consistency, &tokens, shard);
                Self::get_routed(
                    rt,
                    queues,
                    cfg,
                    coll,
                    shard,
                    ReadTarget::Id(id),
                    consistency,
                    may_forward,
                )
                .await
            }
            Request::GetKey {
                key,
                consistency,
                tokens,
            } => {
                let shard = coll.route_key(&key);
                let consistency = Self::per_shard(consistency, &tokens, shard);
                Self::get_routed(
                    rt,
                    queues,
                    cfg,
                    coll,
                    shard,
                    ReadTarget::Key(key),
                    consistency,
                    may_forward,
                )
                .await
            }
            Request::Query {
                query,
                consistency,
                tokens,
            } => {
                let mut receivers = Vec::new();
                let shard_list: Vec<ShardId> = coll.shard_ids().collect();
                for &shard in &shard_list {
                    let c = Self::per_shard(consistency, &tokens, shard);
                    let q = query.clone();
                    receivers.push(Self::send(queues, cores, shard, |reply| {
                        CoreRequest::QueryLegs {
                            shard,
                            query: q,
                            consistency: c,
                            reply,
                        }
                    }));
                }
                // Collect local answers first; shards led elsewhere are forwarded to their
                // leaders concurrently (one task per shard), not one round trip after another.
                let mut outcomes: Vec<Option<Result<Vec<LegList>>>> = Vec::new();
                let mut forwards: Vec<(usize, CrossReceiver<Response>)> = Vec::new();
                for (s, rx) in receivers.into_iter().enumerate() {
                    let shard = shard_list[s];
                    match rx.await {
                        Some(Err(Error::NotLeader {
                            leader_hint: Some(l),
                            ..
                        })) if may_forward && l != cfg.id => {
                            let c = Self::per_shard(consistency, &tokens, shard);
                            let (tx, frx) = cross_oneshot();
                            let (rt2, cfg2, q2) = (rt.clone(), cfg.clone(), query.clone());
                            rt.spawn(async move {
                                let resp = Self::forward(
                                    &rt2,
                                    &cfg2,
                                    l,
                                    shard,
                                    Request::ShardLegs {
                                        shard,
                                        query: q2,
                                        consistency: c,
                                    },
                                )
                                .await;
                                tx.send(resp);
                            });
                            forwards.push((s, frx));
                            outcomes.push(None);
                        }
                        Some(r) => outcomes.push(Some(r)),
                        None => outcomes.push(Some(Err(Error::Internal("core stopped".into())))),
                    }
                }
                for (s, frx) in forwards {
                    let shard = shard_list[s];
                    outcomes[s] = Some(match frx.await {
                        Some(Response::Legs(lists)) => Ok(lists),
                        Some(Response::Error {
                            message,
                            leader_hint,
                        }) => Err(Self::not_leader(shard, &message, leader_hint)),
                        Some(other) => Err(Error::Internal(format!(
                            "unexpected forward response {other:?}"
                        ))),
                        None => Err(Error::Internal("forward task dropped".into())),
                    });
                }
                let mut merged: Vec<LegList> = Vec::new();
                for outcome in outcomes {
                    match outcome.expect("every shard answered") {
                        Ok(lists) => {
                            if merged.is_empty() {
                                merged = lists;
                            } else {
                                for (m, l) in merged.iter_mut().zip(lists) {
                                    m.hits.extend(l.hits);
                                    m.keys.extend(l.keys);
                                }
                            }
                        }
                        Err(e) => return Response::from_error(&e),
                    }
                }
                let per_leg = query.per_leg();
                for m in &mut merged {
                    if m.higher_is_better {
                        m.hits
                            .sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                    } else {
                        m.hits
                            .sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                    }
                    m.hits.truncate(per_leg);
                }
                let keys: HashMap<DocId, String> = merged
                    .iter_mut()
                    .flat_map(|m| std::mem::take(&mut m.keys))
                    .collect();
                let fused = if merged.is_empty() {
                    Vec::new()
                } else if query.leg_count() == 0 {
                    // A pure filter: each shard sent its first matches in id order.
                    let mut ids: Vec<DocId> = merged
                        .into_iter()
                        .flat_map(|l| l.hits)
                        .map(|h| h.0)
                        .collect();
                    ids.sort_unstable();
                    ids.dedup();
                    ids.truncate(query.k);
                    ids.into_iter().map(|d| (d, 0.0, Vec::new())).collect()
                } else {
                    fuse(&query.fusion, &merged, query.k)
                };
                let mut hits = Vec::with_capacity(fused.len());
                for (doc_id, score, legs) in fused {
                    let document = if query.with_documents {
                        let Ok(shard) = coll.route(doc_id) else {
                            continue;
                        };
                        let c = Self::per_shard(consistency, &tokens, shard);
                        match Self::get_routed(
                            rt,
                            queues,
                            cfg,
                            coll,
                            shard,
                            ReadTarget::Id(doc_id),
                            c,
                            may_forward,
                        )
                        .await
                        {
                            Response::Doc(d) => d,
                            _ => None,
                        }
                    } else {
                        None
                    };
                    hits.push(Hit {
                        doc_id,
                        score,
                        legs,
                        document,
                        key: keys.get(&doc_id).cloned(),
                    });
                }
                Response::Hits(hits)
            }
            Request::Status => {
                let mut out = Vec::new();
                let all: Vec<ShardId> = catalog::current()
                    .live
                    .iter()
                    .flat_map(|c| c.shard_ids().collect::<Vec<_>>())
                    .collect();
                for shard in all {
                    if let Some(st) = Self::send(queues, cores, shard, |reply| {
                        CoreRequest::Status { shard, reply }
                    })
                    .await
                    .flatten()
                    {
                        out.push(st);
                    }
                }
                Response::Status(out)
            }
        }
    }

    /// Reads the catalog from this node's catalog replica.
    async fn read_catalog(
        queues: &Queues,
        cfg: &NodeConfig,
        consistency: Consistency,
    ) -> Result<catalog::Catalog> {
        let docs = Self::send(queues, cfg.cores.max(1), CATALOG_SHARD, |reply| {
            CoreRequest::CatalogRead { consistency, reply }
        })
        .await
        .ok_or_else(|| Error::Internal("core stopped".into()))??;
        Ok(catalog::from_entries(default_collection(cfg), &docs))
    }

    /// A live collection by name: from this node's view, or else from a linearizable read of
    /// the catalog (a collection just created through another node).
    async fn resolve(
        _rt: &Runtime,
        queues: &Queues,
        cfg: &NodeConfig,
        name: &str,
    ) -> Result<Arc<CollectionDef>> {
        if let Some(c) = catalog::current().get(name) {
            return Ok(c);
        }
        let fresh = Self::read_catalog(queues, cfg, Consistency::Linearizable).await?;
        catalog::observe(&fresh.live, &fresh.dropped, true);
        catalog::current()
            .get(name)
            .ok_or_else(|| Error::InvalidRequest(format!("no collection {name:?}")))
    }

    /// Keeps this node's replicas in line with the catalog: starts the shards it hosts of every
    /// live collection, stops those of dropped collections and deletes their files. Reads the
    /// local catalog replica (no leader needed), so a node restarting alone gets its
    /// collections back.
    fn spawn_reconciler(rt: &Runtime, queues: Queues, cfg: NodeConfig) {
        let rt2 = rt.clone();
        rt.spawn(async move {
            let mut started: std::collections::HashSet<ShardId> = Default::default();
            let mut removed: std::collections::HashSet<u32> = Default::default();
            let cores = cfg.cores.max(1);
            loop {
                if let Ok(cat) = Self::read_catalog(&queues, &cfg, Consistency::Stale).await {
                    catalog::observe(&cat.live, &cat.dropped, false);
                    let view = catalog::current();
                    for c in view.live.iter().filter(|c| c.id != 0) {
                        for shard in c.shard_ids() {
                            if started.contains(&shard) || !hosts(&cfg, shard) {
                                continue;
                            }
                            let def = c.clone();
                            let ok = Self::send(&queues, cores, shard, |reply| CoreRequest::Start {
                                shard,
                                def,
                                reply,
                            })
                            .await;
                            if ok == Some(true) {
                                started.insert(shard);
                            }
                        }
                    }
                    for d in &view.dropped {
                        if removed.contains(&d.id) {
                            continue;
                        }
                        for shard in d.shard_ids() {
                            let _ = Self::send(&queues, cores, shard, |reply| CoreRequest::Stop {
                                shard,
                                reply,
                            })
                            .await;
                            started.remove(&shard);
                        }
                        let dir = cfg.data_dir.join(catalog::collection_dir(d.id));
                        match std::fs::remove_dir_all(&dir) {
                            Ok(()) => {
                                tracing::info!(node = %cfg.id, collection = %d.name, id = d.id, "dropped collection's files deleted");
                                removed.insert(d.id);
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                removed.insert(d.id);
                            }
                            Err(e) => tracing::warn!(dir = %dir.display(), "cannot delete a dropped collection: {e}"),
                        }
                    }
                }
                rt2.sleep(cairn_core::Duration::from_millis(300)).await;
            }
        });
    }

    /// Retention (ADR 0031): every `retention_interval_ms`, for each collection with an
    /// `expires_field`, each shard this node leads deletes its expired documents. The time is
    /// read here and carried by the command, so every replica deletes the same documents.
    fn spawn_retention(rt: &Runtime, queues: Queues, cfg: NodeConfig) {
        if cfg.retention_interval_ms == 0 {
            return;
        }
        let rt2 = rt.clone();
        rt.spawn(async move {
            let cores = cfg.cores.max(1);
            loop {
                rt2.sleep(cairn_core::Duration::from_millis(cfg.retention_interval_ms))
                    .await;
                let now = rt2.unix_millis();
                for coll in catalog::current().live {
                    let Some(filter) = coll.expired(now) else {
                        continue;
                    };
                    for shard in coll.shard_ids() {
                        let leads = Self::send(&queues, cores, shard, |reply| {
                            CoreRequest::Status { shard, reply }
                        })
                        .await
                        .flatten()
                        .is_some_and(|st| st.role == cairn_raft::Role::Leader);
                        if !leads {
                            continue;
                        }
                        // Look before writing: a sweep that finds nothing adds nothing to the log.
                        let mut probe = Query::new(1);
                        probe.filter = filter.clone();
                        let found =
                            Self::send(&queues, cores, shard, |reply| CoreRequest::QueryLegs {
                                shard,
                                query: probe,
                                consistency: Consistency::Stale,
                                reply,
                            })
                            .await
                            .and_then(Result::ok)
                            .is_some_and(|lists| lists.iter().any(|l| !l.hits.is_empty()));
                        if !found {
                            continue;
                        }
                        let cmd = Command::DeleteWhere {
                            scope: DeleteScope::All,
                            filter: filter.clone(),
                        };
                        match Self::send(&queues, cores, shard, |reply| CoreRequest::Propose {
                            shard,
                            cmd,
                            reply,
                        })
                        .await
                        {
                            Some(Ok(a)) if a.deleted > 0 => tracing::info!(
                                target: "cairn_server::audit",
                                collection = %coll.name,
                                %shard,
                                count = a.deleted,
                                expired_before = now,
                                token = %format!("{}.{}", a.token.shard.get(), a.token.index.get()),
                                "expired documents deleted"
                            ),
                            Some(Ok(_)) => {}
                            Some(Err(e)) => tracing::debug!(%shard, "retention sweep: {e}"),
                            None => {}
                        }
                    }
                }
            }
        });
    }

    /// Creates, drops or lists collections. Creations and drops run on the catalog leader,
    /// one at a time, after a linearizable read: two of them never pick the same id or shards.
    async fn catalog_request(
        rt: &Runtime,
        queues: &Queues,
        cfg: &NodeConfig,
        req: Request,
        may_forward: bool,
    ) -> Response {
        let cores = cfg.cores.max(1);
        if matches!(req, Request::ListCollections) {
            return match Self::read_catalog(queues, cfg, Consistency::Linearizable).await {
                Ok(c) => {
                    catalog::observe(&c.live, &c.dropped, true);
                    Response::Collections(
                        c.live
                            .iter()
                            .map(|d| serde_json::to_string(d.as_ref()).expect("serializable"))
                            .collect(),
                    )
                }
                Err(e) => Response::from_error(&e),
            };
        }
        let status = Self::send(queues, cores, CATALOG_SHARD, |reply| CoreRequest::Status {
            shard: CATALOG_SHARD,
            reply,
        })
        .await
        .flatten();
        match status {
            Some(st) if st.role == cairn_raft::Role::Leader => {}
            Some(st) => {
                return match st.leader {
                    Some(l) if may_forward && l != cfg.id => {
                        Self::forward(rt, cfg, l, CATALOG_SHARD, req).await
                    }
                    hint => Response::from_error(&Error::NotLeader {
                        shard: CATALOG_SHARD,
                        leader_hint: hint,
                    }),
                };
            }
            None => {
                return Response::from_error(&Error::Internal("no catalog replica".into()));
            }
        }
        let _lock = CatalogLock::acquire(rt).await;
        let r = async {
            let cat = Self::read_catalog(queues, cfg, Consistency::Linearizable).await?;
            let default = default_collection(cfg);
            match req {
                Request::CreateCollection {
                    name,
                    schema,
                    shards,
                    expires_field,
                } => {
                    if !catalog::valid_name(&name) || name == catalog::DEFAULT {
                        return Err(Error::InvalidRequest(format!(
                            "a collection name is 1 to 64 of a-z, 0-9, '_' and '-', not {name:?} \
                             (\"default\" is taken)"
                        )));
                    }
                    if shards == 0 || shards > catalog::MAX_SHARDS {
                        return Err(Error::InvalidRequest(format!(
                            "shards must be between 1 and {}",
                            catalog::MAX_SHARDS
                        )));
                    }
                    let user: Schema = serde_json::from_str(&schema)
                        .map_err(|e| Error::InvalidRequest(format!("schema: {e}")))?;
                    let schema = Schema::new(user.fields)?.with_reserved()?;
                    if cat.get(&name).is_some() {
                        return Err(Error::InvalidRequest(format!(
                            "collection {name:?} already exists"
                        )));
                    }
                    let (id, base) = cat.next_ids();
                    if base + shards - 1 > catalog::MAX_SHARD {
                        return Err(Error::InvalidRequest("no shard ids left".into()));
                    }
                    let def = CollectionDef {
                        name: name.clone(),
                        id,
                        base,
                        shards,
                        schema,
                        expires_field: (!expires_field.is_empty()).then_some(expires_field),
                    };
                    def.check_retention()?;
                    let cmd = Command::UpsertKeyed(vec![catalog::entry_doc(&name, "collection", &def)]);
                    Self::propose_routed(rt, queues, cfg, &default, CATALOG_SHARD, cmd, false).await?;
                    catalog::observe(&[Arc::new(def.clone())], &[], false);
                    tracing::info!(node = %cfg.id, collection = %name, id, base, shards, "collection created");
                    // Answer once every shard answers a linearizable read (it has a leader).
                    let t0 = rt.now();
                    loop {
                        let probe = Request::Query {
                            query: Query::new(1),
                            consistency: Consistency::Linearizable,
                            tokens: Vec::new(),
                        };
                        match Box::pin(Self::handle_in(rt, queues, cfg, &def, probe, true)).await {
                            Response::Hits(_) => break,
                            _ if rt.now() - t0 > cairn_core::Duration::from_secs(20) => {
                                return Err(Error::Internal(format!(
                                    "collection {name:?} created, but its shards are not ready yet"
                                )));
                            }
                            _ => rt.sleep(cairn_core::Duration::from_millis(100)).await,
                        }
                    }
                    Ok(def)
                }
                Request::DropCollection { name } => {
                    if name == catalog::DEFAULT {
                        return Err(Error::InvalidRequest(
                            "the default collection cannot be dropped".into(),
                        ));
                    }
                    let def = cat
                        .get(&name)
                        .ok_or_else(|| Error::InvalidRequest(format!("no collection {name:?}")))?;
                    // The tombstone first: a crash before the second write leaves the collection
                    // dropped all the same.
                    let tomb = catalog::entry_doc(&catalog::tombstone_key(def.id), "dropped", &def);
                    Self::propose_routed(rt, queues, cfg, &default, CATALOG_SHARD, Command::UpsertKeyed(vec![tomb]), false).await?;
                    Self::propose_routed(rt, queues, cfg, &default, CATALOG_SHARD, Command::DeleteKeys(vec![name.clone()]), false).await?;
                    let fresh = Self::read_catalog(queues, cfg, Consistency::Linearizable).await?;
                    catalog::observe(&fresh.live, &fresh.dropped, true);
                    tracing::info!(node = %cfg.id, collection = %name, id = def.id, "collection dropped");
                    Ok(def.as_ref().clone())
                }
                _ => Err(Error::Internal("not a catalog request".into())),
            }
        }
        .await;
        match r {
            Ok(def) => {
                Response::Collections(vec![serde_json::to_string(&def).expect("serializable")])
            }
            Err(e) => Response::from_error(&e),
        }
    }

    /// Proof of deletion (ADR 0031): every replica of each document's shard says, once it has
    /// applied the takedown's token, whether it still holds the document. The report lists
    /// each replica's answer; a document is `deleted` only if every replica answered, had
    /// applied the token, and does not hold it.
    #[allow(clippy::too_many_arguments)]
    async fn deletion_check(
        rt: &Runtime,
        queues: &Queues,
        cfg: &NodeConfig,
        coll: &CollectionDef,
        ids: Vec<DocId>,
        keys: Vec<String>,
        tokens: Vec<Token>,
    ) -> Response {
        let cores = cfg.cores.max(1);
        // Documents in request order (ids, then text ids), grouped by shard.
        let mut groups: std::collections::BTreeMap<ShardId, CheckGroup> = Default::default();
        for (i, id) in ids.iter().enumerate() {
            let s = match coll.route(*id) {
                Ok(s) => s,
                Err(e) => return Response::from_error(&e),
            };
            let g = groups.entry(s).or_default();
            g.0.push(*id);
            g.2.push(i);
        }
        for (i, k) in keys.iter().enumerate() {
            let g = groups.entry(coll.route_key(k)).or_default();
            g.1.push(k.clone());
            g.2.push(ids.len() + i);
        }
        let total = ids.len() + keys.len();
        let mut docs: Vec<serde_json::Value> = vec![serde_json::Value::Null; total];
        let mut shards_out = Vec::new();
        for (shard, (gids, gkeys, positions)) in groups {
            let min_index = tokens
                .iter()
                .filter(|t| t.shard == shard)
                .map(|t| t.index)
                .max()
                .unwrap_or(cairn_core::LogIndex(0));
            let mut replicas = Vec::new();
            // Per document: reasons it is not proven deleted.
            let mut reasons: Vec<Vec<String>> = vec![Vec::new(); positions.len()];
            for node in placement(cfg, shard) {
                let answer = if node == cfg.id {
                    let targets: Vec<ReadTarget> = gids
                        .iter()
                        .copied()
                        .map(ReadTarget::Id)
                        .chain(gkeys.iter().cloned().map(ReadTarget::Key))
                        .collect();
                    match Self::send(queues, cores, shard, |reply| CoreRequest::Check {
                        shard,
                        targets,
                        min_index,
                        reply,
                    })
                    .await
                    {
                        Some(Ok(r)) => Ok(r),
                        Some(Err(e)) => Err(e.to_string()),
                        None => Err("core stopped".into()),
                    }
                } else {
                    let req = Request::ReplicaCheck {
                        shard,
                        ids: gids.clone(),
                        keys: gkeys.clone(),
                        min_index,
                    };
                    match Self::forward_once(rt, cfg, node, req).await {
                        Ok(Response::Checked { applied, present }) => Ok((applied, present)),
                        Ok(Response::Error { message, .. }) => Err(message),
                        Ok(other) => Err(format!("unexpected answer {other:?}")),
                        Err(e) => Err(e.to_string()),
                    }
                };
                match answer {
                    Ok((applied, present)) => {
                        for (j, p) in present.iter().enumerate().take(positions.len()) {
                            if applied < min_index {
                                reasons[j].push(format!(
                                    "node {} has applied {} < {}",
                                    node.get(),
                                    applied.get(),
                                    min_index.get()
                                ));
                            } else if *p {
                                reasons[j].push(format!("node {} still holds it", node.get()));
                            }
                        }
                        replicas.push(serde_json::json!({
                            "node": node.get(),
                            "applied": applied.get(),
                            "holds": positions.iter().zip(&present).filter(|(_, p)| **p).map(|(i, _)| *i).collect::<Vec<_>>(),
                        }));
                    }
                    Err(e) => {
                        for r in reasons.iter_mut() {
                            r.push(format!("node {} did not answer: {e}", node.get()));
                        }
                        replicas.push(serde_json::json!({ "node": node.get(), "error": e }));
                    }
                }
            }
            for (j, pos) in positions.iter().enumerate() {
                docs[*pos] = serde_json::json!({
                    "shard": shard.get(),
                    "verdict": if reasons[j].is_empty() { "deleted" } else { "not proven" },
                    "reasons": reasons[j],
                });
            }
            shards_out.push(serde_json::json!({
                "shard": shard.get(),
                "min_index": min_index.get(),
                "replicas": replicas,
            }));
        }
        Response::Proof(
            serde_json::json!({ "documents": docs, "shards": shards_out, "checked_by": cfg.id.get() })
                .to_string(),
        )
    }

    /// Consistency for one shard: read-your-writes uses that shard's token, or no bound.
    fn per_shard(c: Consistency, tokens: &[Token], shard: ShardId) -> Consistency {
        match c {
            Consistency::ReadYourWrites(t) => {
                let best = tokens
                    .iter()
                    .filter(|x| x.shard == shard)
                    .map(|x| x.index)
                    .max();
                match best {
                    Some(index) => Consistency::ReadYourWrites(Token { shard, index }),
                    None if t.shard == shard => Consistency::ReadYourWrites(t),
                    None => Consistency::Stale,
                }
            }
            other => other,
        }
    }

    /// Node id.
    pub fn id(&self) -> NodeId {
        self.cfg.id
    }

    /// Pauses or resumes merges on this node (see [`cairn_query::JobSlots::set_merges_paused`]).
    /// The build slots, and so this flag, are shared by every node of one process.
    pub fn set_merges_paused(&self, paused: bool) {
        job_slots(self.cfg.compaction_slots).set_merges_paused(paused);
    }

    /// The build slots shared by this node's replicas (merge pause flag included).
    pub fn job_slots(&self) -> std::sync::Arc<cairn_query::JobSlots> {
        job_slots(self.cfg.compaction_slots)
    }

    /// Listen address.
    pub fn addr(&self) -> SocketAddr {
        self.net.local_addr()
    }

    /// Number of cores.
    pub fn cores(&self) -> usize {
        self.cores
    }

    /// Blocks until the core threads exit (they do not, unless they panic).
    pub fn join(self) {
        for t in self.threads {
            let _ = t.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::auto_build_threads;

    #[test]
    fn concurrent_builds_share_the_machine() {
        assert_eq!(auto_build_threads(8, 4), 2);
        assert_eq!(auto_build_threads(8, 2), 4);
        assert_eq!(auto_build_threads(16, 2), 8);
        assert_eq!(auto_build_threads(2, 4), 1);
        assert_eq!(auto_build_threads(64, 0), 64);
    }
}
