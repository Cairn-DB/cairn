//! Node assembly: core threads, shard replicas, node dispatcher, client coordinator.

use bytes::Bytes;
use cairn_core::{DocId, Document, Error, HashMap, NodeId, Result, Runtime as _, Schema, ShardId};
use cairn_proto::{Request, Response, shard_of};
use cairn_query::{
    Consistency, EngineConfig, Hit, LegList, Query, Replica, ReplicaConfig, ReplicaHandle,
    ReplicaStatus, Token, fuse,
};
use cairn_runtime::pool::{PoolDisk, PoolRuntime, ThreadReactor, offload};
use cairn_runtime::tcp::ClientConn;
use cairn_runtime::{
    CrossQueue, CrossReceiver, CrossSender, Executor, TcpNetwork, TcpNetworkConfig, cross_oneshot,
};
use cairn_storage::{Command, LogConfig, StoreConfig};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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
    /// Shards per collection (every shard is replicated on every node).
    pub shards: u32,
    /// Executor threads.
    pub cores: usize,
    /// Collection schema.
    pub schema: Schema,
    /// Memtable flush threshold.
    pub memtable_max_bytes: usize,
    /// Raft tick in milliseconds.
    pub tick_ms: u64,
    /// Message drop probability (tests).
    pub drop_prob: f64,
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
        reply: CrossSender<Result<Token>>,
    },
    Get {
        shard: ShardId,
        id: DocId,
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

fn core_of(shard: ShardId, cores: usize) -> usize {
    shard.get() as usize % cores.max(1)
}

impl Node {
    /// Starts the node: core 0 binds the listener and publishes the socket layer; every core
    /// builds its own executor and replicas.
    pub fn start(cfg: NodeConfig) -> Result<Node> {
        std::fs::create_dir_all(&cfg.data_dir)
            .map_err(|e| Error::io(cairn_core::error::IoErrorKind::Other, e))?;
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
        let handles = ex.block_on(Self::spawn_replicas(rt.clone(), cfg.clone(), core));
        let queue = queues[core].clone();
        let (rt2, h2) = (rt.clone(), handles.clone());
        rt.spawn(async move {
            while let Some(req) = queue.pop().await {
                Self::serve(&rt2, &h2, req);
            }
        });
        if core == 0 {
            Self::spawn_dispatcher(&rt, queues.clone(), cfg.cores.max(1));
            Self::spawn_coordinator(&rt, queues, cfg);
        }
        let _ = ex.run();
    }

    async fn spawn_replicas(
        rt: Runtime,
        cfg: NodeConfig,
        core: usize,
    ) -> HashMap<ShardId, ReplicaHandle> {
        let mut handles = HashMap::default();
        for s in 0..cfg.shards {
            let shard = ShardId(s);
            if core_of(shard, cfg.cores) != core {
                continue;
            }
            let rc = ReplicaConfig {
                shard,
                id: cfg.id,
                peers: cfg.peers.keys().copied().collect(),
                tick: cairn_core::Duration::from_millis(cfg.tick_ms),
                election_ticks: 10,
                heartbeat_ticks: 2,
                engine: EngineConfig {
                    store: StoreConfig {
                        memtable_max_bytes: cfg.memtable_max_bytes,
                        log: LogConfig::default(),
                        ..StoreConfig::default()
                    },
                    ..EngineConfig::default()
                },
                dir: format!("shard{s}"),
                seed: u64::from(cfg.id.get()) * 1000 + u64::from(s),
                own_receiver: false,
            };
            match Replica::spawn(rt.clone(), rc, cfg.schema.clone()).await {
                Ok(h) => {
                    handles.insert(shard, h);
                }
                Err(e) => tracing::error!(shard = %shard, "cannot start replica: {e}"),
            }
        }
        handles
    }

    fn serve(rt: &Runtime, handles: &HashMap<ShardId, ReplicaHandle>, req: CoreRequest) {
        let shard = match &req {
            CoreRequest::Net { shard, .. }
            | CoreRequest::Propose { shard, .. }
            | CoreRequest::Get { shard, .. }
            | CoreRequest::QueryLegs { shard, .. }
            | CoreRequest::Status { shard, .. } => *shard,
        };
        let Some(h) = handles.get(&shard).cloned() else {
            let e = || Error::InvalidRequest(format!("no shard {shard}"));
            match req {
                CoreRequest::Propose { reply, .. } => reply.send(Err(e())),
                CoreRequest::Get { reply, .. } => reply.send(Err(e())),
                CoreRequest::QueryLegs { reply, .. } => reply.send(Err(e())),
                CoreRequest::Status { reply, .. } => reply.send(None),
                CoreRequest::Net { .. } => {}
            }
            return;
        };
        match req {
            CoreRequest::Net { from, bytes, .. } => h.deliver(from, bytes),
            CoreRequest::Propose { cmd, reply, .. } => {
                rt.spawn(async move { reply.send(h.propose(cmd).await) })
            }
            CoreRequest::Get {
                id,
                consistency,
                reply,
                ..
            } => rt.spawn(async move { reply.send(h.get(id, consistency).await) }),
            CoreRequest::QueryLegs {
                query,
                consistency,
                reply,
                ..
            } => rt.spawn(async move { reply.send(h.query_legs(query, consistency).await) }),
            CoreRequest::Status { reply, .. } => {
                rt.spawn(async move { reply.send(h.status().await) })
            }
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

    /// Forwards `req` to `node` over a peer connection (blocking call on a helper thread).
    async fn forward(rt: &Runtime, cfg: &NodeConfig, node: NodeId, req: Request) -> Response {
        let Some(addr) = cfg.peers.get(&node).copied() else {
            return Response::Error {
                message: format!("unknown node {node}"),
                leader_hint: None,
            };
        };
        let bytes = Request::Forwarded(Box::new(req)).to_bytes();
        let result = offload(&rt.completer(), move || {
            let conn = {
                let mut m = peer_conns().lock().expect("peer conns");
                match m.get(&node) {
                    Some(c) => c.clone(),
                    None => {
                        let c = Arc::new(ClientConn::connect(addr)?);
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
        .await;
        match result {
            Ok(r) => r,
            Err(e) => Response::from_error(&e),
        }
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
        shard: ShardId,
        cmd: Command,
        may_forward: bool,
    ) -> Result<Vec<Token>> {
        let cores = cfg.cores.max(1);
        let cmd2 = cmd.clone();
        match Self::send(queues, cores, shard, |reply| CoreRequest::Propose {
            shard,
            cmd: cmd2,
            reply,
        })
        .await
        {
            Some(Ok(t)) => Ok(vec![t]),
            Some(Err(Error::NotLeader {
                leader_hint: Some(l),
                ..
            })) if may_forward && l != cfg.id => {
                let req = match cmd {
                    Command::Upsert(docs) => Request::Upsert(docs),
                    Command::Delete(ids) => Request::Delete(ids),
                    Command::Noop => return Ok(Vec::new()),
                };
                match Self::forward(rt, cfg, l, req).await {
                    Response::Ack(tokens) => Ok(tokens),
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

    async fn get_routed(
        rt: &Runtime,
        queues: &Queues,
        cfg: &NodeConfig,
        shard: ShardId,
        id: DocId,
        consistency: Consistency,
        may_forward: bool,
    ) -> Response {
        let cores = cfg.cores.max(1);
        match Self::send(queues, cores, shard, |reply| CoreRequest::Get {
            shard,
            id,
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
                Self::forward(rt, cfg, l, Request::Get { id, consistency }).await
            }
            Some(Err(e)) => Response::from_error(&e),
            None => Response::Error {
                message: "core stopped".into(),
                leader_hint: None,
            },
        }
    }

    async fn handle(
        rt: &Runtime,
        queues: &Queues,
        cfg: &NodeConfig,
        req: Request,
        may_forward: bool,
    ) -> Response {
        let cores = cfg.cores.max(1);
        match req {
            Request::Forwarded(inner) => {
                Box::pin(Self::handle(rt, queues, cfg, *inner, false)).await
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
                let mut groups: HashMap<ShardId, Vec<Document>> = HashMap::default();
                for d in docs {
                    groups
                        .entry(shard_of(d.id, cfg.shards))
                        .or_default()
                        .push(d);
                }
                let mut tokens = Vec::new();
                for (shard, group) in groups {
                    match Self::propose_routed(
                        rt,
                        queues,
                        cfg,
                        shard,
                        Command::Upsert(group),
                        may_forward,
                    )
                    .await
                    {
                        Ok(t) => tokens.extend(t),
                        Err(e) => return Response::from_error(&e),
                    }
                }
                tokens.sort();
                Response::Ack(tokens)
            }
            Request::Delete(ids) => {
                let mut groups: HashMap<ShardId, Vec<DocId>> = HashMap::default();
                for id in ids {
                    groups.entry(shard_of(id, cfg.shards)).or_default().push(id);
                }
                let mut tokens = Vec::new();
                for (shard, group) in groups {
                    match Self::propose_routed(
                        rt,
                        queues,
                        cfg,
                        shard,
                        Command::Delete(group),
                        may_forward,
                    )
                    .await
                    {
                        Ok(t) => tokens.extend(t),
                        Err(e) => return Response::from_error(&e),
                    }
                }
                tokens.sort();
                Response::Ack(tokens)
            }
            Request::Get { id, consistency } => {
                let shard = shard_of(id, cfg.shards);
                let consistency = Self::per_shard(consistency, &[], shard);
                Self::get_routed(rt, queues, cfg, shard, id, consistency, may_forward).await
            }
            Request::Query {
                query,
                consistency,
                tokens,
            } => {
                let mut receivers = Vec::new();
                for s in 0..cfg.shards {
                    let shard = ShardId(s);
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
                let mut merged: Vec<LegList> = Vec::new();
                for (s, rx) in receivers.into_iter().enumerate() {
                    let shard = ShardId(s as u32);
                    let outcome = match rx.await {
                        Some(Err(Error::NotLeader {
                            leader_hint: Some(l),
                            ..
                        })) if may_forward && l != cfg.id => {
                            let c = Self::per_shard(consistency, &tokens, shard);
                            match Self::forward(
                                rt,
                                cfg,
                                l,
                                Request::ShardLegs {
                                    shard,
                                    query: query.clone(),
                                    consistency: c,
                                },
                            )
                            .await
                            {
                                Response::Legs(lists) => Ok(lists),
                                Response::Error {
                                    message,
                                    leader_hint,
                                } => Err(Self::not_leader(shard, &message, leader_hint)),
                                other => Err(Error::Internal(format!(
                                    "unexpected forward response {other:?}"
                                ))),
                            }
                        }
                        Some(r) => r,
                        None => Err(Error::Internal("core stopped".into())),
                    };
                    match outcome {
                        Ok(lists) => {
                            if merged.is_empty() {
                                merged = lists;
                            } else {
                                for (m, l) in merged.iter_mut().zip(lists) {
                                    m.hits.extend(l.hits);
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
                let fused = if merged.is_empty() {
                    Vec::new()
                } else {
                    fuse(&query.fusion, &merged, query.k)
                };
                let mut hits = Vec::with_capacity(fused.len());
                for (doc_id, score, legs) in fused {
                    let document = if query.with_documents {
                        let shard = shard_of(doc_id, cfg.shards);
                        let c = Self::per_shard(consistency, &tokens, shard);
                        match Self::get_routed(rt, queues, cfg, shard, doc_id, c, may_forward).await
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
                    });
                }
                Response::Hits(hits)
            }
            Request::Status => {
                let mut out = Vec::new();
                for s in 0..cfg.shards {
                    let shard = ShardId(s);
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
