//! A shard replica: the actor that owns one shard's engine and Raft state machine on one core.
//!
//! Everything reaches the replica through its inbox: ticks from a timer task, frames from a
//! network task, and requests from [`ReplicaHandle`]s. Each event is stepped into Raft, then the
//! [`cairn_raft::Ready`] is drained in the required order: persist hard state and entries, send
//! messages, apply committed entries, report progress. Flushes compact the Raft log to a
//! snapshot (the manifest); lagging followers fetch the manifest's files from the leader.

use crate::engine::{EngineConfig, ShardEngine};
use crate::query::{Hit, Query};
use crate::wire::{Frame, FrameBody};
use bytes::Bytes;
use cairn_core::codec::{Reader, Writer};
use cairn_core::sync::{LocalQueue, Sender, oneshot};
use cairn_core::{
    DocId, Document, Duration, Error, HashMap, LogIndex, Network, NodeId, Result, Runtime, Schema,
    ShardId, Term,
};
use cairn_index::DefaultIndexer;
use cairn_raft::{Entry, HardState, InitialState, Raft, Ready, Role, Snapshot};
use cairn_storage::manifest::{Manifest, ManifestStore};
use cairn_storage::{Command, CompactJob, FlushJob, LogEntry, Store};

/// Consistency level of a read (ADR 0010).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    /// Leader, after a ReadIndex round.
    Linearizable,
    /// Any replica that has applied at least this token.
    ReadYourWrites(Token),
    /// Any replica, whatever it has applied.
    Stale,
}

/// Acknowledgement of a write: where it sits in the shard's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Token {
    /// Shard.
    pub shard: ShardId,
    /// Log index.
    pub index: LogIndex,
}

/// Bounds how many compaction jobs run at once across the replicas that share it (one node).
/// A compaction holds all live rows of its input segments in memory while it rebuilds their
/// indexes, so an unbounded number of concurrent jobs can exhaust a node's RAM.
#[derive(Debug)]
pub struct JobSlots {
    used: std::sync::atomic::AtomicUsize,
    max: usize,
}

impl JobSlots {
    /// Allows `max` concurrent jobs.
    pub fn new(max: usize) -> Self {
        JobSlots {
            used: std::sync::atomic::AtomicUsize::new(0),
            max: max.max(1),
        }
    }

    fn try_acquire(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |u| {
                (u < self.max).then_some(u + 1)
            })
            .is_ok()
    }

    fn release(&self) {
        self.used.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Replica settings.
#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    /// Shard id.
    pub shard: ShardId,
    /// This node.
    pub id: NodeId,
    /// Group members.
    pub peers: Vec<NodeId>,
    /// Raft tick period.
    pub tick: Duration,
    /// Election timeout in ticks.
    pub election_ticks: u32,
    /// Heartbeat interval in ticks.
    pub heartbeat_ticks: u32,
    /// Engine settings.
    pub engine: EngineConfig,
    /// Shard directory.
    pub dir: String,
    /// Seed for Raft's election randomness.
    pub seed: u64,
    /// Spawn a task that reads the runtime's network inbox and delivers frames to this replica.
    /// A multi-shard node sets this to `false` and delivers through [`ReplicaHandle::deliver`].
    pub own_receiver: bool,
    /// Flush a non-empty memtable after this many ticks without applied writes (0: never).
    /// Queries scan the memtable without a graph, so a large idle memtable costs every query.
    pub idle_flush_ticks: u32,
    /// Shared bound on concurrent index builds, flushes and compactions (`None`: unbounded,
    /// one per replica).
    pub compaction_slots: Option<std::sync::Arc<JobSlots>>,
}

/// Snapshot of a replica's state for diagnostics and checkers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaStatus {
    /// Node.
    pub id: NodeId,
    /// Raft role.
    pub role: Role,
    /// Term.
    pub term: Term,
    /// Known leader.
    pub leader: Option<NodeId>,
    /// Commit index.
    pub commit: LogIndex,
    /// Applied index.
    pub applied: LogIndex,
    /// Live documents (approximate: memtable + segments minus deletions).
    pub live_docs: u64,
    /// Bytes in the active memtable (bounded by write backpressure).
    pub memtable_bytes: u64,
    /// Segment ids in manifest order.
    pub segments: Vec<u64>,
}

enum Event {
    Tick,
    Net(NodeId, Bytes),
    Propose(Command, Sender<Result<Token>>),
    Query(Query, Consistency, Sender<Result<Vec<Hit>>>),
    QueryLegs(
        Query,
        Consistency,
        Sender<Result<Vec<crate::fusion::LegList>>>,
    ),
    Get(DocId, Consistency, Sender<Result<Option<Document>>>),
    Status(Sender<ReplicaStatus>),
    FlushBuilt(FlushJob, Vec<Document>, Result<Vec<(String, Vec<u8>)>>),
    CompactBuilt(CompactJob, Result<Vec<(String, Vec<u8>)>>),
}

/// Handle to a running replica (cheap to clone).
#[derive(Clone)]
pub struct ReplicaHandle {
    inbox: LocalQueue<Event>,
    id: NodeId,
    alive: std::rc::Rc<std::cell::Cell<bool>>,
}

/// Marks the replica dead and fails queued requests when the actor future is dropped, which
/// is how a simulated crash (task cancellation) or a panic reaches waiting clients.
struct AliveGuard {
    inbox: LocalQueue<Event>,
    alive: std::rc::Rc<std::cell::Cell<bool>>,
}

impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.alive.set(false);
        while let Some(ev) = self.inbox.try_pop() {
            match ev {
                Event::Propose(_, done) => done.send(closed()),
                Event::Query(_, _, done) => done.send(closed()),
                Event::QueryLegs(_, _, done) => done.send(closed()),
                Event::Get(_, _, done) => done.send(closed()),
                Event::Status(_)
                | Event::Tick
                | Event::Net(..)
                | Event::FlushBuilt(..)
                | Event::CompactBuilt(..) => {}
            }
        }
    }
}

fn closed<T>() -> Result<T> {
    Err(Error::io(
        cairn_core::error::IoErrorKind::Shutdown,
        "replica stopped",
    ))
}

impl ReplicaHandle {
    /// Node this handle talks to.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Whether the replica is still running.
    pub fn is_alive(&self) -> bool {
        self.alive.get()
    }

    /// Proposes a command; resolves once applied here, or with `NotLeader`.
    pub async fn propose(&self, cmd: Command) -> Result<Token> {
        if !self.alive.get() {
            return closed();
        }
        let (tx, rx) = oneshot();
        self.inbox.push(Event::Propose(cmd, tx));
        rx.await.unwrap_or_else(closed)
    }

    /// Runs a hybrid query at the given consistency.
    pub async fn query(&self, q: Query, consistency: Consistency) -> Result<Vec<Hit>> {
        if !self.alive.get() {
            return closed();
        }
        let (tx, rx) = oneshot();
        self.inbox.push(Event::Query(q, consistency, tx));
        rx.await.unwrap_or_else(closed)
    }

    /// Runs the legs of a query at the given consistency, unfused (multi-shard coordinators).
    pub async fn query_legs(
        &self,
        q: Query,
        consistency: Consistency,
    ) -> Result<Vec<crate::fusion::LegList>> {
        if !self.alive.get() {
            return closed();
        }
        let (tx, rx) = oneshot();
        self.inbox.push(Event::QueryLegs(q, consistency, tx));
        rx.await.unwrap_or_else(closed)
    }

    /// Delivers a node-to-node frame (used by a node-level dispatcher that owns the socket).
    pub fn deliver(&self, from: NodeId, bytes: Bytes) {
        if self.alive.get() {
            self.inbox.push(Event::Net(from, bytes));
        }
    }

    /// Point read at the given consistency.
    pub async fn get(&self, id: DocId, consistency: Consistency) -> Result<Option<Document>> {
        if !self.alive.get() {
            return closed();
        }
        let (tx, rx) = oneshot();
        self.inbox.push(Event::Get(id, consistency, tx));
        rx.await.unwrap_or_else(closed)
    }

    /// Current status.
    pub async fn status(&self) -> Option<ReplicaStatus> {
        if !self.alive.get() {
            return None;
        }
        let (tx, rx) = oneshot();
        self.inbox.push(Event::Status(tx));
        rx.await
    }
}

/// Persisted Raft state beside the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct RaftState {
    hs: HardState,
    snapshot_term: Term,
}

impl Manifest for RaftState {
    fn encode(&self, w: &mut Writer) {
        w.u64(self.hs.term.get())
            .u64(self.hs.vote.map_or(u64::MAX, |v| u64::from(v.get())))
            .u64(self.hs.commit.get())
            .u64(self.snapshot_term.get());
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let term = Term(r.u64()?);
        let vote = match r.u64()? {
            u64::MAX => None,
            v => Some(NodeId(v as u32)),
        };
        let commit = LogIndex(r.u64()?);
        let snapshot_term = Term(r.u64()?);
        Ok(RaftState {
            hs: HardState { term, vote, commit },
            snapshot_term,
        })
    }
}

struct SnapshotFetch {
    manifest: Bytes,
    last_index: LogIndex,
    from: NodeId,
    needed: Vec<String>,
    got: HashMap<String, Vec<u8>>,
    partial: HashMap<String, (u64, Vec<u8>)>,
    req: u64,
}

const CHUNK: usize = 256 * 1024;

/// The actor. Create with [`Replica::spawn`].
pub struct Replica<R: Runtime> {
    rt: R,
    cfg: ReplicaConfig,
    schema: Schema,
    engine: ShardEngine<R>,
    raft: Raft,
    state_store: ManifestStore<R>,
    state: RaftState,
    inbox: LocalQueue<Event>,
    waiting_commit: HashMap<LogIndex, (Term, Sender<Result<Token>>)>,
    waiting_reads: HashMap<u64, Event>,
    waiting_applied: Vec<(LogIndex, Event)>,
    next_read: u64,
    fetch: Option<SnapshotFetch>,
    ticks: u64,
    last_apply_tick: u64,
    holds_slot: bool,
    /// Proposals held back while the memtable is over its hard limit (write backpressure).
    deferred: std::collections::VecDeque<(Command, Sender<Result<Token>>)>,
}

impl<R: Runtime> Replica<R> {
    /// Opens the engine and Raft state from disk.
    async fn open_state(
        rt: &R,
        cfg: &ReplicaConfig,
        schema: &Schema,
    ) -> Result<(ShardEngine<R>, Raft, RaftState, ManifestStore<R>)> {
        // Raft state first: only entries up to the persisted commit index may be replayed.
        let state_store = ManifestStore::new(rt.clone(), format!("{}/RAFT", cfg.dir));
        let state = state_store.load::<RaftState>().await?.unwrap_or_default();
        let engine = ShardEngine::open_with_limit(
            rt.clone(),
            &cfg.dir,
            schema.clone(),
            cfg.engine.clone(),
            Some(state.hs.commit),
        )
        .await?;
        let applied = engine.store().applied_index();
        let snapshot_index = engine.store().manifest().applied_index;
        let log = engine.store().log();
        let mut entries = Vec::new();
        if let Some(last) = log.last_index()
            && last > snapshot_index
        {
            let from = snapshot_index.next().max(log.first_index());
            if from == snapshot_index.next() {
                entries = log
                    .read_range(from, last)
                    .await?
                    .into_iter()
                    .map(|e| Entry {
                        index: e.index,
                        term: e.term,
                        payload: e.payload,
                    })
                    .collect();
            }
        }
        let raft_cfg = cairn_raft::Config {
            id: cfg.id,
            peers: cfg.peers.clone(),
            election_ticks: cfg.election_ticks,
            heartbeat_ticks: cfg.heartbeat_ticks,
            max_batch: 64,
            rng: cairn_core::SeededRng::from_seed(cfg.seed)
                .fork(&format!("raft-{}-{}", cfg.shard, cfg.id)),
        };
        let mut raft = Raft::new(
            raft_cfg,
            InitialState {
                hard_state: state.hs,
                entries,
                snapshot: (snapshot_index, state.snapshot_term),
                applied,
            },
        );
        if snapshot_index > LogIndex(0) {
            raft.compact(Snapshot {
                last_index: snapshot_index,
                last_term: state.snapshot_term,
                data: engine.store().manifest_bytes(),
            });
        }
        Ok((engine, raft, state, state_store))
    }

    /// Reopens everything from disk after a fatal error (the in-memory equivalent of a crash and
    /// restart); pending requests are failed.
    async fn reopen(&mut self) -> Result<()> {
        let (engine, raft, state, state_store) =
            Self::open_state(&self.rt, &self.cfg, &self.schema).await?;
        self.engine = engine;
        self.raft = raft;
        self.state = state;
        self.state_store = state_store;
        self.fetch = None;
        for (_, (_, done)) in self.waiting_commit.drain() {
            done.send(closed());
        }
        for (_, done) in self.deferred.drain(..) {
            done.send(closed());
        }
        for (_, ev) in self.waiting_reads.drain() {
            Self::fail(
                ev,
                Error::io(
                    cairn_core::error::IoErrorKind::Shutdown,
                    "replica restarted",
                ),
            );
        }
        for (_, ev) in self.waiting_applied.drain(..) {
            Self::fail(
                ev,
                Error::io(
                    cairn_core::error::IoErrorKind::Shutdown,
                    "replica restarted",
                ),
            );
        }
        Ok(())
    }

    /// Opens the shard (recovering) and spawns the actor, its ticker and its network receiver.
    /// The network receiver dispatches frames for this shard; `frames` receives frames for
    /// other shards (Phase 4 multiplexes several shards per node).
    pub async fn spawn(rt: R, cfg: ReplicaConfig, schema: Schema) -> Result<ReplicaHandle> {
        let (engine, raft, state, state_store) = Self::open_state(&rt, &cfg, &schema).await?;
        let inbox = LocalQueue::new();
        let alive = std::rc::Rc::new(std::cell::Cell::new(true));
        let handle = ReplicaHandle {
            inbox: inbox.clone(),
            id: cfg.id,
            alive: alive.clone(),
        };
        let mut replica = Replica {
            rt: rt.clone(),
            cfg: cfg.clone(),
            schema: schema.clone(),
            engine,
            raft,
            state_store,
            state,
            inbox: inbox.clone(),
            waiting_commit: HashMap::default(),
            waiting_reads: HashMap::default(),
            waiting_applied: Vec::new(),
            next_read: 1,
            fetch: None,
            ticks: 0,
            last_apply_tick: 0,
            holds_slot: false,
            deferred: std::collections::VecDeque::new(),
        };
        // Ticker.
        let (h, r2, tick) = (handle.clone(), rt.clone(), cfg.tick);
        rt.spawn(async move {
            while h.alive.get() {
                r2.sleep(tick).await;
                h.inbox.push(Event::Tick);
            }
        });
        // Network receiver.
        if cfg.own_receiver {
            let (h, r2) = (handle.clone(), rt.clone());
            rt.spawn(async move {
                while h.alive.get()
                    && let Ok((from, bytes)) = r2.network().recv().await
                {
                    h.deliver(from, bytes);
                }
            });
        }
        // The actor.
        let guard = AliveGuard {
            inbox: inbox.clone(),
            alive,
        };
        rt.spawn(async move {
            let _guard = guard;
            if let Err(e) = replica.run().await {
                tracing::error!(node = %cfg.id, shard = %cfg.shard, "replica stopped: {e}");
                // Keep answering so callers fail fast instead of waiting forever.
                loop {
                    let ev = replica.inbox.pop().await;
                    match ev {
                        Event::Propose(_, done) => done.send(closed()),
                        Event::Query(_, _, done) => done.send(closed()),
                        Event::QueryLegs(_, _, done) => done.send(closed()),
                        Event::Get(_, _, done) => done.send(closed()),
                        Event::Status(_)
                        | Event::Tick
                        | Event::Net(..)
                        | Event::FlushBuilt(..)
                        | Event::CompactBuilt(..) => {}
                    }
                }
            }
        });
        Ok(handle)
    }

    async fn run(&mut self) -> Result<()> {
        self.drain_ready().await?;
        loop {
            let ev = self.inbox.pop().await;
            let step = async {
                self.handle_event(ev).await?;
                self.drain_ready().await?;
                self.serve_waiting().await
            }
            .await;
            if let Err(e) = step {
                tracing::error!(node = %self.cfg.id, shard = %self.cfg.shard, "replica error, reopening from disk: {e}");
                self.reopen().await?;
                self.drain_ready().await?;
            }
        }
    }

    async fn handle_event(&mut self, ev: Event) -> Result<()> {
        match ev {
            Event::Tick => {
                self.ticks += 1;
                self.raft.tick();
                self.on_idle_tick().await?;
            }
            Event::Net(from, bytes) => match Frame::from_bytes(&bytes) {
                Ok(f) if f.shard == self.cfg.shard => self.handle_frame(from, f.body).await?,
                Ok(_) => {}
                Err(e) => tracing::warn!("bad frame from {from}: {e}"),
            },
            Event::Propose(cmd, done) => {
                if self.over_write_limit() {
                    self.deferred.push_back((cmd, done));
                } else {
                    self.propose(cmd, done);
                }
            }
            Event::Query(q, c, done) => self.read(Event::Query(q, c, done)).await?,
            Event::QueryLegs(q, c, done) => self.read(Event::QueryLegs(q, c, done)).await?,
            Event::Get(id, c, done) => self.read(Event::Get(id, c, done)).await?,
            Event::FlushBuilt(job, docs, sections) => {
                self.release_slot();
                let sections = sections?;
                self.engine
                    .store_mut()
                    .finish_flush(job, docs, sections)
                    .await?;
                self.after_publish().await?;
                self.release_deferred();
            }
            Event::CompactBuilt(job, sections) => {
                self.release_slot();
                let sections = sections?;
                self.engine
                    .store_mut()
                    .finish_compact(job, sections)
                    .await?;
                self.engine.refresh().await?;
                // Lagging followers must fetch the merged files, not the removed ones: refresh
                // the snapshot Raft offers them.
                let up_to = self.engine.store().manifest().applied_index;
                if up_to > LogIndex(0) {
                    let term = self
                        .raft
                        .term_at(up_to)
                        .unwrap_or(self.raft.snapshot_term());
                    self.raft.compact(Snapshot {
                        last_index: up_to,
                        last_term: term,
                        data: self.engine.store().manifest_bytes(),
                    });
                }
                self.maybe_compact_job().await?;
                self.release_deferred();
            }
            Event::Status(done) => {
                let st = self.engine.store();
                done.send(ReplicaStatus {
                    id: self.cfg.id,
                    role: self.raft.role(),
                    term: self.raft.term(),
                    leader: self.raft.leader(),
                    commit: self.raft.commit_index(),
                    applied: st.applied_index(),
                    live_docs: st.approx_live_docs(),
                    memtable_bytes: st.memtable_bytes() as u64,
                    segments: st.segments().map(|s| s.id.get()).collect(),
                });
            }
        }
        Ok(())
    }

    fn consistency_of(ev: &Event) -> Consistency {
        match ev {
            Event::Query(_, c, _) | Event::QueryLegs(_, c, _) | Event::Get(_, c, _) => *c,
            _ => Consistency::Stale,
        }
    }

    fn fail(ev: Event, e: Error) {
        match ev {
            Event::Query(_, _, done) => done.send(Err(e)),
            Event::QueryLegs(_, _, done) => done.send(Err(e)),
            Event::Get(_, _, done) => done.send(Err(e)),
            _ => {}
        }
    }

    /// Routes a read according to its consistency level.
    async fn read(&mut self, ev: Event) -> Result<()> {
        match Self::consistency_of(&ev) {
            Consistency::Stale => self.execute(ev).await,
            Consistency::ReadYourWrites(token) => {
                if token.shard != self.cfg.shard {
                    Self::fail(
                        ev,
                        Error::InvalidRequest("token is for another shard".into()),
                    );
                } else if self.engine.store().applied_index() >= token.index {
                    self.execute(ev).await?;
                } else if self.raft.leader().is_none() && self.raft.role() != Role::Leader {
                    // No leader known: a lagging replica cannot promise progress.
                    Self::fail(
                        ev,
                        Error::NotLeader {
                            shard: self.cfg.shard,
                            leader_hint: None,
                        },
                    );
                } else {
                    self.waiting_applied.push((token.index, ev));
                }
                Ok(())
            }
            Consistency::Linearizable => {
                let id = self.next_read;
                self.next_read += 1;
                match self.raft.read_index(id) {
                    Ok(()) => {
                        self.waiting_reads.insert(id, ev);
                    }
                    Err(e) => Self::fail(ev, e),
                }
                Ok(())
            }
        }
    }

    async fn execute(&mut self, ev: Event) -> Result<()> {
        match ev {
            Event::Query(q, _, done) => {
                let r = self.engine.query(&q).await;
                done.send(r);
            }
            Event::QueryLegs(q, _, done) => {
                let r = self.engine.query_legs(&q).await;
                done.send(r);
            }
            Event::Get(id, _, done) => {
                let r = self.engine.get(id).await;
                done.send(r);
            }
            _ => {}
        }
        Ok(())
    }

    /// Serves reads whose applied-index requirement is now met.
    async fn serve_waiting(&mut self) -> Result<()> {
        let applied = self.engine.store().applied_index();
        let mut ready = Vec::new();
        let mut i = 0;
        while i < self.waiting_applied.len() {
            if self.waiting_applied[i].0 <= applied {
                ready.push(self.waiting_applied.swap_remove(i).1);
            } else {
                i += 1;
            }
        }
        for ev in ready {
            self.execute(ev).await?;
        }
        Ok(())
    }

    async fn handle_frame(&mut self, from: NodeId, body: FrameBody) -> Result<()> {
        match body {
            FrameBody::Raft(m) => self.raft.step(from, m),
            FrameBody::FetchFile { req, path, offset } => {
                if !path.starts_with("segs/") || path.contains("..") {
                    return Ok(());
                }
                let data = self.engine.store().read_file(&path).await?;
                let (total, chunk) = match data {
                    None => (u64::MAX, Bytes::new()),
                    Some(d) => {
                        let end = (offset as usize + CHUNK).min(d.len());
                        (d.len() as u64, d.slice((offset as usize).min(d.len())..end))
                    }
                };
                self.send(
                    from,
                    FrameBody::FileChunk {
                        req,
                        path,
                        offset,
                        total,
                        data: chunk,
                    },
                )
                .await;
            }
            FrameBody::FileChunk {
                req,
                path,
                offset,
                total,
                data,
            } => {
                self.on_chunk(from, req, path, offset, total, data).await?;
            }
        }
        Ok(())
    }

    async fn send(&self, to: NodeId, body: FrameBody) {
        let f = Frame {
            shard: self.cfg.shard,
            body,
        };
        let _ = self.rt.network().send(to, f.to_bytes()).await;
    }

    /// Runs the persist → send → apply → advance cycle until Raft has nothing more.
    async fn drain_ready(&mut self) -> Result<()> {
        loop {
            let ready: Ready = self.raft.ready();
            if ready.is_empty() {
                return Ok(());
            }
            if let Some(hs) = ready.hard_state {
                self.state.hs = hs;
                self.state_store.store(&self.state).await?;
            }
            if let Some(t) = ready.truncate_from {
                self.engine.store_mut().log_mut().truncate_suffix(t).await?;
            }
            if !ready.entries.is_empty() {
                let entries: Vec<LogEntry> = ready
                    .entries
                    .iter()
                    .map(|e| LogEntry {
                        index: e.index,
                        term: e.term,
                        payload: e.payload.clone(),
                    })
                    .collect();
                let log = self.engine.store_mut().log_mut();
                let first = entries[0].index;
                if first < log.first_index() {
                    // Raft restarted below the on-disk log (crash mid snapshot install).
                    log.reset(first).await?;
                } else if log.next_index() != first {
                    // The on-disk log is ahead of Raft's view; align.
                    log.truncate_suffix(first).await?;
                }
                log.append(&entries).await?;
                log.sync().await?;
            }
            for (to, m) in ready.messages {
                self.send(to, FrameBody::Raft(m)).await;
            }
            if let Some(s) = ready.snapshot {
                tracing::info!(node = %self.cfg.id, last_index = %s.last_index, applied = %self.engine.store().applied_index(), "snapshot accepted");
                // Entries after the snapshot may arrive before its files: start the log there now.
                self.engine
                    .store_mut()
                    .log_mut()
                    .reset(s.last_index.next())
                    .await?;
                self.start_fetch(s).await?;
            }
            for e in &ready.committed {
                let cmd = Command::from_bytes(&e.payload)?;
                let store = self.engine.store_mut();
                if e.index > store.applied_index() {
                    store.apply(e.index, &cmd)?;
                }
                if let Some((term, done)) = self.waiting_commit.remove(&e.index) {
                    if term == e.term {
                        done.send(Ok(Token {
                            shard: self.cfg.shard,
                            index: e.index,
                        }));
                    } else {
                        done.send(Err(Error::NotLeader {
                            shard: self.cfg.shard,
                            leader_hint: self.raft.leader(),
                        }));
                    }
                }
            }
            if !ready.committed.is_empty() {
                self.last_apply_tick = self.ticks;
                self.engine.refresh().await?;
                self.maybe_flush().await?;
            }
            for (id, index) in ready.read_states {
                if let Some(ev) = self.waiting_reads.remove(&id) {
                    self.waiting_applied.push((index, ev));
                }
            }
            // Reads Raft gave up on (leadership changed, or the leader never answered a
            // follower read): fail them with a hint so the coordinator retries at the leader.
            for id in ready.failed_reads {
                if let Some(ev) = self.waiting_reads.remove(&id) {
                    Self::fail(
                        ev,
                        Error::NotLeader {
                            shard: self.cfg.shard,
                            leader_hint: self.raft.leader(),
                        },
                    );
                }
            }
            // Proposals from a lost leadership can never commit with their term.
            if self.raft.role() != Role::Leader && !self.waiting_commit.is_empty() {
                let hint = self.raft.leader();
                for (_, (_, done)) in self.waiting_commit.drain() {
                    done.send(Err(Error::NotLeader {
                        shard: self.cfg.shard,
                        leader_hint: hint,
                    }));
                }
            }
            let persisted = self
                .engine
                .store()
                .log()
                .last_index()
                .unwrap_or(self.engine.store().manifest().applied_index);
            let applied = self.engine.store().applied_index();
            self.raft.advance(persisted, applied);
        }
    }

    async fn maybe_flush(&mut self) -> Result<()> {
        if self.engine.store().memtable_bytes() < self.cfg.engine.store.memtable_max_bytes {
            return Ok(());
        }
        self.start_flush_job()?;
        self.release_deferred();
        Ok(())
    }

    /// Freezes the memtable and builds its indexes off the actor (`Runtime::offload`); the
    /// result comes back as an event. No-op when a job is already running.
    fn start_flush_job(&mut self) -> Result<()> {
        if self.engine.store().job_active() {
            return Ok(());
        }
        // Flush builds share the node's slots with compactions: each build holds its rows,
        // their vectors and the new graph in memory at once.
        if let Some(slots) = &self.cfg.compaction_slots {
            if !slots.try_acquire() {
                return Ok(());
            }
            self.holds_slot = true;
        }
        let Some(mut job) = self.engine.store_mut().begin_flush() else {
            self.release_slot();
            return Ok(());
        };
        // The build owns the documents and hands them back; `finish_flush` takes them as an
        // argument, so the job need not keep a second copy of the frozen memtable.
        let docs = std::mem::take(&mut job.docs);
        let schema = self.engine.schema().clone();
        let indexer = DefaultIndexer {
            vector: self.cfg.engine.vector,
        };
        let inbox = self.inbox.clone();
        let rt = self.rt.clone();
        self.rt.spawn(async move {
            let (docs, sections) = rt
                .offload(move || {
                    let sections = Store::<R>::build_sections(&schema, &docs, &indexer);
                    (docs, sections)
                })
                .await;
            inbox.push(Event::FlushBuilt(job, docs, sections));
        });
        Ok(())
    }

    /// After a segment was published: reload indexes, compact the Raft log to the manifest,
    /// and start a compaction if the policy asks for one.
    fn propose(&mut self, cmd: Command, done: Sender<Result<Token>>) {
        match self.raft.propose(cmd.to_bytes()) {
            Ok(index) => {
                self.waiting_commit.insert(index, (self.raft.term(), done));
            }
            Err(e) => done.send(Err(e)),
        }
    }

    /// Write backpressure: while a flush builds (or waits for a build slot), the active
    /// memtable keeps filling; past twice its threshold, new proposals wait. Without it, ingest
    /// faster than index builds grows the memtable (and the in-memory Raft log) without bound.
    fn over_write_limit(&self) -> bool {
        self.engine.store().memtable_bytes() >= 2 * self.cfg.engine.store.memtable_max_bytes
    }

    /// Releases held-back proposals while under the limit.
    fn release_deferred(&mut self) {
        while !self.over_write_limit() {
            let Some((cmd, done)) = self.deferred.pop_front() else {
                break;
            };
            self.propose(cmd, done);
        }
    }

    async fn after_publish(&mut self) -> Result<()> {
        self.engine.refresh().await?;
        let up_to = self.engine.store().manifest().applied_index;
        tracing::info!(node = %self.cfg.id, applied = %up_to, segments = ?self.engine.store().segments().map(|s| s.id.get()).collect::<Vec<_>>(), "flushed");
        if up_to > LogIndex(0)
            && let Some(term) = self.raft.term_at(up_to)
        {
            let snap = Snapshot {
                last_index: up_to,
                last_term: term,
                data: self.engine.store().manifest_bytes(),
            };
            self.raft.compact(snap);
            self.state.snapshot_term = term;
            self.state_store.store(&self.state).await?;
        }
        // A full memtable flushes before any compaction: compactions are long, and writes are
        // held back while the memtable is over its limit.
        if self.engine.store().memtable_bytes() >= self.cfg.engine.store.memtable_max_bytes {
            return self.start_flush_job();
        }
        self.maybe_compact_job().await
    }

    /// Background upkeep on ticks: flush an idle memtable, and retry a compaction that was
    /// waiting for a free slot.
    async fn on_idle_tick(&mut self) -> Result<()> {
        let idle = u64::from(self.cfg.idle_flush_ticks);
        if idle > 0
            && self.ticks - self.last_apply_tick >= idle
            && self.engine.store().memtable_bytes() > 0
            && !self.engine.store().job_active()
        {
            return self.start_flush_job();
        }
        if self.cfg.compaction_slots.is_some() && !self.engine.store().job_active() {
            // A flush may be waiting for a build slot.
            if self.engine.store().memtable_bytes() >= self.cfg.engine.store.memtable_max_bytes {
                self.start_flush_job()?;
                self.release_deferred();
                return Ok(());
            }
            if self.ticks % 20 == 0 {
                self.maybe_compact_job().await?;
            }
        }
        Ok(())
    }

    fn release_slot(&mut self) {
        if self.holds_slot {
            self.holds_slot = false;
            if let Some(slots) = &self.cfg.compaction_slots {
                slots.release();
            }
        }
    }

    async fn maybe_compact_job(&mut self) -> Result<()> {
        if self.engine.store().job_active() {
            return Ok(());
        }
        if let Some(slots) = &self.cfg.compaction_slots {
            if !slots.try_acquire() {
                return Ok(());
            }
            self.holds_slot = true;
        }
        let job = match self.engine.store_mut().begin_compact().await {
            Ok(Some(job)) => job,
            Ok(None) => {
                self.release_slot();
                return Ok(());
            }
            Err(e) => {
                self.release_slot();
                return Err(e);
            }
        };
        let schema = self.engine.schema().clone();
        let indexer = DefaultIndexer {
            vector: self.cfg.engine.vector,
        };
        let inbox = self.inbox.clone();
        let rt = self.rt.clone();
        self.rt.spawn(async move {
            let (job, sections) = rt
                .offload(move || {
                    let sections = Store::<R>::build_sections(&schema, &job.docs, &indexer);
                    (job, sections)
                })
                .await;
            inbox.push(Event::CompactBuilt(job, sections));
        });
        Ok(())
    }

    /// Flushes synchronously (tests): builds inline and publishes.
    pub async fn flush(&mut self) -> Result<()> {
        if self.engine.store().job_active() {
            return Ok(());
        }
        self.engine.flush().await?;
        self.after_publish().await
    }

    async fn start_fetch(&mut self, s: Snapshot) -> Result<()> {
        let m: cairn_storage::ShardManifest = ManifestStore::<R>::decode(&s.data)?;
        let needed = Store::<R>::snapshot_files(&m);
        let from = self.raft.leader().unwrap_or(self.cfg.id);
        tracing::info!(node = %self.cfg.id, %from, last_index = %s.last_index, segments = ?m.segments.iter().map(|x| x.id.get()).collect::<Vec<_>>(), "fetching snapshot");
        let req = self.next_read;
        self.next_read += 1;
        let mut fetch = SnapshotFetch {
            manifest: s.data.clone(),
            last_index: s.last_index,
            from,
            needed: needed.clone(),
            got: HashMap::default(),
            partial: HashMap::default(),
            req,
        };
        if needed.is_empty() {
            self.fetch = Some(fetch);
            return self.finish_fetch().await;
        }
        for p in &needed {
            fetch.partial.insert(p.clone(), (0, Vec::new()));
            self.send(
                from,
                FrameBody::FetchFile {
                    req,
                    path: p.clone(),
                    offset: 0,
                },
            )
            .await;
        }
        self.fetch = Some(fetch);
        Ok(())
    }

    async fn on_chunk(
        &mut self,
        from: NodeId,
        req: u64,
        path: String,
        offset: u64,
        total: u64,
        data: Bytes,
    ) -> Result<()> {
        let Some(f) = self.fetch.as_mut() else {
            return Ok(());
        };
        if f.req != req || f.from != from {
            return Ok(());
        }
        let Some((have, buf)) = f.partial.get_mut(&path) else {
            return Ok(());
        };
        if total == u64::MAX {
            if path.ends_with(".seg") {
                // The leader compacted that segment away: this snapshot is stale. Reopening from
                // disk makes the leader ship a fresh one.
                return Err(Error::Internal(format!(
                    "snapshot file {path} vanished on the leader"
                )));
            }
            // Missing deletion checkpoint: optional.
            f.partial.remove(&path);
            f.needed.retain(|p| *p != path);
        } else if offset == *have {
            buf.extend_from_slice(&data);
            *have += data.len() as u64;
            if *have >= total {
                let (_, buf) = f.partial.remove(&path).expect("present");
                f.got.insert(path.clone(), buf);
            } else {
                let next = *have;
                self.send(
                    from,
                    FrameBody::FetchFile {
                        req,
                        path,
                        offset: next,
                    },
                )
                .await;
                return Ok(());
            }
        }
        let done = self.fetch.as_ref().is_some_and(|f| f.partial.is_empty());
        if done {
            self.finish_fetch().await?;
        }
        Ok(())
    }

    async fn finish_fetch(&mut self) -> Result<()> {
        let Some(f) = self.fetch.take() else {
            return Ok(());
        };
        let files: Vec<(String, Bytes)> = f
            .needed
            .iter()
            .filter_map(|p| f.got.get(p).map(|b| (p.clone(), Bytes::from(b.clone()))))
            .collect();
        self.engine
            .store_mut()
            .install_snapshot(&f.manifest, files)
            .await?;
        self.engine.refresh().await?;
        let applied = self.engine.store().applied_index();
        debug_assert_eq!(applied, f.last_index);
        // Term of the snapshot point as Raft recorded it.
        self.state.snapshot_term = self.raft.snapshot_term();
        self.state_store.store(&self.state).await?;
        self.raft.advance(applied, applied);
        // Anyone waiting on an index at or below the snapshot can proceed.
        self.serve_waiting().await
    }
}
