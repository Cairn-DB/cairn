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
    Disk, DocId, Document, Duration, Error, HashMap, LogIndex, Network, NodeId, Result, Runtime,
    Schema, SegmentId, ShardId, Term,
};
use cairn_index::DefaultIndexer;
use cairn_raft::{Entry, HardState, InitialState, Raft, Ready, Role, Snapshot};
use cairn_storage::manifest::{Manifest, ManifestStore};
use cairn_storage::{Command, LogEntry, Store};

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

    /// Takes a slot if one is free.
    pub fn try_acquire(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |u| {
                (u < self.max).then_some(u + 1)
            })
            .is_ok()
    }

    /// Returns a slot.
    pub fn release(&self) {
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
    /// Followers fetch the segments the leader built instead of building them (ADR 0016). With
    /// `false`, every replica builds every flush itself (the flush still goes through the log).
    pub ship_segments: bool,
    /// Threads for index builds (flushes and compactions); `None`: the build thread alone.
    /// Builds are identical either way (ADR 0019).
    pub build_parallel: Option<std::sync::Arc<dyn cairn_core::Parallel>>,
    /// The replica that should lead this shard when it is up and caught up (ADR 0020). A
    /// leader that is not it hands leadership over. `None`: no balancing.
    pub preferred_leader: Option<NodeId>,
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
    /// Diagnostics: payload bytes of the in-memory Raft log.
    pub raft_log_bytes: u64,
    /// Diagnostics: proposals held back by backpressure, awaiting commit, events in the inbox.
    pub queued: [u32; 3],
    /// Diagnostics: this node's bytes queued for peers and messages not yet consumed.
    pub net: (u64, u64),
    /// Segment ids in manifest order.
    pub segments: Vec<u64>,
    /// Flushes built here, flushes fetched from the leader, frozen memtables not yet published.
    pub flushes: [u64; 3],
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
    /// A flushed segment was built and written outside the actor (ADR 0021).
    FlushWritten {
        id: SegmentId,
        generation: u64,
        file: Result<(u64, u64)>,
    },
    /// A merged segment was read, built and written outside the actor (ADR 0021).
    CompactWritten {
        id: SegmentId,
        inputs: Vec<SegmentId>,
        generation: u64,
        file: Result<(u64, u64)>,
    },
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
                | Event::FlushWritten { .. }
                | Event::CompactWritten { .. } => {}
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
    /// Reads wait until the applied index reaches this: an installed snapshot's files may
    /// reflect entries up to it (compactions and deletion checkpoints taken after the
    /// snapshot point), and the state is not a consistent prefix before they are replayed.
    read_floor: LogIndex,
    /// See `cairn_raft::InitialState::vote_barrier` (index, term).
    vote_barrier: LogIndex,
    vote_barrier_term: Term,
}

impl Manifest for RaftState {
    fn encode(&self, w: &mut Writer) {
        w.u64(self.hs.term.get())
            .u64(self.hs.vote.map_or(u64::MAX, |v| u64::from(v.get())))
            .u64(self.hs.commit.get())
            .u64(self.snapshot_term.get())
            .u64(self.read_floor.get())
            .u64(self.vote_barrier.get())
            .u64(self.vote_barrier_term.get());
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let term = Term(r.u64()?);
        let vote = match r.u64()? {
            u64::MAX => None,
            v => Some(NodeId(v as u32)),
        };
        let commit = LogIndex(r.u64()?);
        let snapshot_term = Term(r.u64()?);
        // Absent in state files written before the read floor existed.
        let read_floor = LogIndex(if r.remaining() > 0 { r.u64()? } else { 0 });
        let vote_barrier = LogIndex(if r.remaining() > 0 { r.u64()? } else { 0 });
        let vote_barrier_term = Term(if r.remaining() > 0 { r.u64()? } else { 0 });
        Ok(RaftState {
            hs: HardState { term, vote, commit },
            snapshot_term,
            read_floor,
            vote_barrier,
            vote_barrier_term,
        })
    }
}

/// A snapshot accepted from the leader and not yet installed, persisted before the log is
/// reset past it: after a restart, Raft resumes from it (the entries after it were acknowledged
/// and may be committed) and the fetch starts again. Without it, a restart dropped those
/// entries (chaos seed 14440).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct PendingSnapshot {
    last_index: LogIndex,
    last_term: Term,
    data: Bytes,
    /// The node that sent it: its files are fetched from it, since since ADR 0021 the same
    /// segment id may hold different bytes on different replicas (local fallback builds).
    from: Option<NodeId>,
}

impl Manifest for PendingSnapshot {
    fn encode(&self, w: &mut Writer) {
        w.u64(self.last_index.get())
            .u64(self.last_term.get())
            .bytes(&self.data)
            .u32(self.from.map_or(u32::MAX, |n| n.get()));
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let last_index = LogIndex(r.u64()?);
        let last_term = Term(r.u64()?);
        let data = Bytes::copy_from_slice(r.bytes()?);
        let from = if r.remaining() > 0 {
            Some(r.u32()?).filter(|n| *n != u32::MAX).map(NodeId)
        } else {
            None
        };
        Ok(PendingSnapshot {
            last_index,
            last_term,
            data,
            from,
        })
    }
}

struct SnapshotFetch {
    manifest: Bytes,
    last_index: LogIndex,
    last_term: Term,
    from: NodeId,
    needed: Vec<String>,
    /// Files completely fetched (streamed to their staging files and synced).
    got: Vec<String>,
    /// Bytes received so far per file still being fetched.
    partial: HashMap<String, u64>,
    req: u64,
    /// Tick of the last chunk (or of the requests).
    progress_tick: u64,
    /// Stalls so far (diagnostics; each stall re-requests the missing chunks).
    stalls: u32,
    /// Highest applied index reported by the file server with a chunk.
    floor: LogIndex,
}

const CHUNK: usize = 256 * 1024;

/// `FileChunk::total` for a deletion checkpoint the server does not have although it still
/// has the segment (no deletions to ship). `u64::MAX` means the file is gone.
const NO_CHECKPOINT: u64 = u64::MAX - 1;

/// A follower fetching a leader-built segment (ADR 0016).
struct SegFetch {
    /// A merged segment (ADR 0021) rather than a flushed one.
    compaction: bool,
    from: NodeId,
    req: u64,
    /// File length, known from the first chunk.
    total: Option<u64>,
    /// Next offset to request.
    next: u64,
    /// Offsets requested and not received yet.
    inflight: std::collections::BTreeSet<u64>,
    /// Bytes received so far.
    got: u64,
    /// Tick of the last chunk (or of the first request).
    progress_tick: u64,
    /// Tick of the last re-request of the chunks in flight.
    retry_tick: u64,
}

/// Segment files fetched at once (flushed or merged, ADR 0021): a replica that comes back
/// late fetches what it missed in parallel instead of one file at a time.
const MAX_SEG_FETCHES: usize = 4;
/// Chunk requests in flight per fetched file (256 KiB each): no round trip per chunk.
const FETCH_WINDOW: usize = 8;

/// Files of segments replaced by a compaction are kept this many election timeouts (30 s at
/// the server's 50 ms tick), so fetches and snapshot installs in flight can finish.
const RETIRED_GRACE_ELECTIONS: u64 = 60;

/// A segment fetch that gets no chunk for this many election timeouts is abandoned for a
/// local build.
const FETCH_STALL_ELECTIONS: u64 = 4;
/// A follower that waited this many election timeouts for the `FlushCommit` of a freeze builds
/// it itself (the leader that would announce it may have crashed after publishing it).
const COMMIT_WAIT_ELECTIONS: u64 = 200;

/// A leader keeps log entries a follower has not matched yet, up to this many payload bytes,
/// instead of compacting them at a flush: a follower one flush behind then catches up from
/// the log rather than through a snapshot (which ships segment files).
const RETAIN_LOG_BYTES: usize = 256 << 20;

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
    /// Flush state (ADR 0016), reset when the replica reopens.
    flush: FlushState,
}

/// Per-replica flush bookkeeping (ADR 0016). Not persisted: after a restart the pending
/// freezes come back from log replay and are built or fetched again.
#[derive(Default)]
struct FlushState {
    /// Leader: a `FlushBegin` proposed and not applied yet (term, index).
    begin_proposed: Option<(Term, LogIndex)>,
    /// Freeze whose segment builds here now.
    building: Option<SegmentId>,
    /// Compaction whose merged segment builds here now (ADR 0021).
    compacting: Option<SegmentId>,
    /// Leader: a merged segment built here and proposed, not yet committed: its id, file and
    /// the term of the proposal.
    own_compaction: Option<(SegmentId, (u64, u64), Term)>,
    /// Leader: the last merge built here (inputs, id, file), re-proposed instead of rebuilt if
    /// its proposal is lost.
    last_merge: Option<(Vec<SegmentId>, SegmentId, (u64, u64))>,
    /// Segments replaced by a compaction, with the tick they were retired: their files are
    /// purged after a grace period (ADR 0021).
    retired: std::collections::VecDeque<(SegmentId, u64)>,
    /// Segments being fetched, by id (at most `MAX_SEG_FETCHES`).
    fetches: std::collections::BTreeMap<SegmentId, SegFetch>,
    /// Freezes to build here although the leader ships them (fetch failed or never announced).
    fallback: std::collections::BTreeSet<SegmentId>,
    /// Tick at which each freeze was first seen without a commit.
    waiting_since: HashMap<SegmentId, u64>,
    /// Segments built here whose `FlushCommit` has not been applied yet: `(len, hash)` and the
    /// term in which this replica last proposed the commit.
    announce: std::collections::BTreeMap<SegmentId, ((u64, u64), Option<Term>)>,
    built: u64,
    fetched: u64,
    /// Leader balancing (ADR 0020): tick since which this replica leads, and the earliest tick
    /// for the next handover attempt.
    lead_since: Option<u64>,
    next_handover: u64,
    /// Handovers started by this replica (diagnostics).
    handovers: u64,
}

impl<R: Runtime> Replica<R> {
    /// Opens the engine and Raft state from disk. Also returns an accepted snapshot whose
    /// installation must resume.
    #[allow(clippy::type_complexity)]
    async fn open_state(
        rt: &R,
        cfg: &ReplicaConfig,
        schema: &Schema,
    ) -> Result<(
        ShardEngine<R>,
        Raft,
        RaftState,
        ManifestStore<R>,
        Option<(Snapshot, Option<NodeId>)>,
    )> {
        // Raft state first: only entries up to the persisted commit index may be replayed.
        let state_store = ManifestStore::new(rt.clone(), format!("{}/RAFT", cfg.dir));
        let state = state_store.load::<RaftState>().await?.unwrap_or_default();
        let mut engine = ShardEngine::open_with_limit(
            rt.clone(),
            &cfg.dir,
            schema.clone(),
            cfg.engine.clone(),
            Some(state.hs.commit),
        )
        .await?;
        engine
            .store_mut()
            .set_compaction_namespace(u64::from(cfg.id.get()));
        let applied = engine.store().applied_index();
        let manifest_index = engine.store().manifest().applied_index;
        // An accepted snapshot beyond the manifest: Raft restarts on it.
        let pending_path = format!("{}/SNAPSHOT", cfg.dir);
        let pending = ManifestStore::<R>::new(rt.clone(), pending_path.clone())
            .load::<PendingSnapshot>()
            .await?
            .filter(|p| p.last_index > manifest_index);
        // The manifest records the term of its index; older manifests fall back to the state.
        let manifest_term = match engine.store().manifest().applied_term {
            Term(0) => state.snapshot_term,
            t => t,
        };
        let (snapshot_index, snapshot_term) = match &pending {
            Some(p) => (p.last_index, p.last_term),
            None => (manifest_index, manifest_term),
        };
        let log = engine.store_mut().log_mut();
        if pending.is_some() && log.first_index() != snapshot_index.next() {
            // Crash between persisting the snapshot and resetting the log.
            log.reset(snapshot_index.next()).await?;
        }
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
                snapshot: (snapshot_index, snapshot_term),
                applied,
                installing: pending.is_some(),
                vote_barrier: (state.vote_barrier, state.vote_barrier_term),
            },
        );
        if pending.is_none() && snapshot_index > LogIndex(0) {
            raft.compact(Snapshot {
                last_index: snapshot_index,
                last_term: manifest_term,
                data: engine.store().manifest_bytes(),
            });
        }
        let resume = pending.map(|p| {
            (
                Snapshot {
                    last_index: p.last_index,
                    last_term: p.last_term,
                    data: p.data,
                },
                p.from,
            )
        });
        Ok((engine, raft, state, state_store, resume))
    }

    /// Reopens everything from disk after a fatal error (the in-memory equivalent of a crash and
    /// restart); pending requests are failed.
    async fn reopen(&mut self) -> Result<()> {
        let (engine, raft, state, state_store, resume) =
            Self::open_state(&self.rt, &self.cfg, &self.schema).await?;
        self.engine = engine;
        self.raft = raft;
        self.state = state;
        self.state_store = state_store;
        self.fetch = None;
        self.flush = FlushState::default();
        if let Some((s, from)) = resume {
            self.start_fetch(s, from).await?;
        }
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
        let (engine, raft, state, state_store, resume) =
            Self::open_state(&rt, &cfg, &schema).await?;
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
            flush: FlushState::default(),
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
            if let Err(e) = replica.run(resume).await {
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
                        | Event::FlushWritten { .. }
                        | Event::CompactWritten { .. } => {}
                    }
                }
            }
        });
        Ok(handle)
    }

    async fn run(&mut self, resume: Option<(Snapshot, Option<NodeId>)>) -> Result<()> {
        if let Some((s, from)) = resume {
            self.start_fetch(s, from).await?;
        }
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
            Event::FlushWritten {
                id,
                generation,
                file,
            } => {
                self.release_slot();
                self.flush.building = None;
                match file {
                    Err(e) => {
                        // Retried by the next drive: the freeze is still pending without a file.
                        tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "flush build failed: {e}");
                    }
                    Ok(file) => {
                        let wanted = self.engine.store().flush_wanted(id, generation);
                        if wanted {
                            self.engine.store().adopt_built_file(id).await?;
                        } else {
                            self.engine.store().discard_built_file(id).await?;
                        }
                        if wanted && self.engine.store_mut().set_flush_file(id, generation, file) {
                            self.flush.built += 1;
                            self.flush.fallback.remove(&id);
                            let committed = self
                                .engine
                                .store()
                                .pending_flushes()
                                .iter()
                                .any(|p| p.id == id && p.commit.is_some());
                            if !committed {
                                self.flush.announce.insert(id, (file, None));
                            }
                        }
                    }
                }
                self.drive_flushes().await?;
            }
            Event::CompactWritten {
                id,
                inputs,
                generation,
                file,
            } => {
                self.release_slot();
                self.flush.compacting = None;
                let file = match file {
                    Ok(f) => f,
                    Err(e) => {
                        // Inputs retired or replaced meanwhile, or an I/O error: dropped; a
                        // pending compaction is retried by the next drive.
                        tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "merge build failed: {e}");
                        return Ok(());
                    }
                };
                let pending = self.engine.store().compaction_pending(id);
                let has_file = self
                    .engine
                    .store()
                    .pending_compactions()
                    .iter()
                    .any(|(c, f, _)| c.id == id && *f);
                let installed = self.engine.store().segments().any(|m| m.id == id);
                let wanted = self.fetch.is_none()
                    && self.engine.store().job_is_current(generation)
                    && !installed
                    && !has_file
                    && (pending || self.raft.role() == Role::Leader);
                if !wanted {
                    // A snapshot was installed, or the merge was fetched or installed meanwhile:
                    // never replace a file that is in use.
                    self.engine.store().discard_built_file(id).await?;
                    return Ok(());
                }
                self.engine.store().adopt_built_file(id).await?;
                self.flush.built += 1;
                if pending {
                    self.engine.store_mut().set_compaction_file(id, file);
                    self.flush.fallback.remove(&id);
                } else {
                    self.propose_merge(id, inputs.clone(), file);
                }
                // Kept for a re-proposal if this one is lost with a leadership change.
                self.flush.last_merge = Some((inputs, id, file));
                self.drive_flushes().await?;
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
                    raft_log_bytes: self.raft.log_bytes() as u64,
                    queued: [
                        self.deferred.len() as u32,
                        self.waiting_commit.len() as u32,
                        self.inbox.len() as u32,
                    ],
                    net: self.rt.network().queue_stats(),
                    segments: st.segments().map(|s| s.id.get()).collect(),
                    flushes: [
                        self.flush.built,
                        self.flush.fetched,
                        (st.pending_flushes().len() + st.pending_compactions().len()) as u64,
                    ],
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
        let floor = self.state.read_floor;
        match Self::consistency_of(&ev) {
            Consistency::Stale if self.engine.store().applied_index() < floor => {
                // Freshly installed snapshot: not a consistent state until the floor.
                self.waiting_applied.push((floor, ev));
                Ok(())
            }
            Consistency::Stale => self.execute(ev).await,
            Consistency::ReadYourWrites(token) => {
                if token.shard != self.cfg.shard {
                    Self::fail(
                        ev,
                        Error::InvalidRequest("token is for another shard".into()),
                    );
                } else if self.engine.store().applied_index() >= token.index.max(floor) {
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
                    self.waiting_applied.push((token.index.max(floor), ev));
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
        if applied < self.state.read_floor {
            // A snapshot installed since these reads queued: wait for its floor too.
            return Ok(());
        }
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
                // One chunk read per request: never the whole (possibly very large) file.
                let data = self
                    .engine
                    .store()
                    .read_file_range(&path, offset, CHUNK)
                    .await?;
                let missing = if path.ends_with(".del")
                    && data.is_none()
                    && self
                        .engine
                        .store()
                        .read_file_range(&path.replace(".del", ".seg"), 0, 0)
                        .await?
                        .is_some()
                {
                    // No deletion checkpoint, but the segment is here: nothing to mask.
                    NO_CHECKPOINT
                } else {
                    u64::MAX
                };
                let (total, chunk) = data.unwrap_or((missing, Bytes::new()));
                let applied = self.engine.store().applied_index().get();
                self.send(
                    from,
                    FrameBody::FileChunk {
                        req,
                        path,
                        offset,
                        total,
                        applied,
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
                applied,
                data,
            } => {
                if let Some(f) = self.fetch.as_mut()
                    && f.req == req
                    && f.from == from
                {
                    f.floor = f.floor.max(LogIndex(applied));
                }
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
            // Entries before the hard state: the hard state carries the commit index, and a
            // crash between the two writes must leave a commit index that is too low (harmless:
            // Raft re-sends), never one that covers a stale entry still on disk. Writing the hard
            // state first let a restart replay a conflicting entry from an earlier term as
            // committed (chaos seed 2227).
            // An accepted snapshot first: it is persisted, and the log restarts after it,
            // before any entry that follows it is appended or acknowledged.
            if let Some(snap) = ready.snapshot {
                tracing::info!(node = %self.cfg.id, last_index = %snap.last_index, applied = %self.engine.store().applied_index(), "snapshot accepted");
                // Raft records the sender as leader when it accepts a snapshot.
                let source = self.raft.leader();
                ManifestStore::<R>::new(self.rt.clone(), format!("{}/SNAPSHOT", self.cfg.dir))
                    .store(&PendingSnapshot {
                        last_index: snap.last_index,
                        last_term: snap.last_term,
                        data: snap.data.clone(),
                        from: source,
                    })
                    .await?;
                self.engine
                    .store_mut()
                    .log_mut()
                    .reset(snap.last_index.next())
                    .await?;
                self.start_fetch(snap, source).await?;
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
            if let Some(hs) = ready.hard_state {
                self.state.hs = hs;
                self.state_store.store(&self.state).await?;
            }
            for (to, m) in ready.messages {
                self.send(to, FrameBody::Raft(m)).await;
            }
            for e in &ready.committed {
                let cmd = Command::from_bytes(&e.payload)?;
                if let Command::FlushCommit { id, .. } = &cmd {
                    self.flush.announce.remove(id);
                }
                let compact_id = match &cmd {
                    Command::CompactCommit { id, .. } => Some(*id),
                    _ => None,
                };
                let store = self.engine.store_mut();
                if e.index > store.applied_index() {
                    store.apply(e.index, &cmd)?;
                }
                if let Some(id) = compact_id
                    && self.flush.own_compaction.is_some_and(|o| o.0 == id)
                    && !self.engine.store().compaction_accepted(id)
                {
                    // Our merge lost to another one of the same inputs: nothing to install.
                    tracing::info!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "own compaction rejected");
                    self.flush.own_compaction = None;
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
                // Upkeep entries (flush markers) do not count as write activity for the idle
                // flush; everything else does.
                if ready.committed.iter().any(|e| {
                    !matches!(
                        Command::from_bytes(&e.payload),
                        Ok(Command::FlushBegin
                            | Command::FlushCommit { .. }
                            | Command::CompactCommit { .. }
                            | Command::Noop)
                    )
                }) {
                    self.last_apply_tick = self.ticks;
                }
                self.engine.refresh().await?;
                self.maybe_flush().await?;
            }
            for (id, index) in ready.read_states {
                if let Some(ev) = self.waiting_reads.remove(&id) {
                    self.waiting_applied
                        .push((index.max(self.state.read_floor), ev));
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
        if self.engine.store().memtable_bytes() >= self.cfg.engine.store.memtable_max_bytes {
            self.propose_flush_begin();
        }
        self.drive_flushes().await?;
        self.release_deferred();
        Ok(())
    }

    /// Leader only: proposes a `FlushBegin`, unless one is in flight or a freeze of this
    /// replica is still unpublished (one flush at a time, ADR 0016).
    fn propose_flush_begin(&mut self) {
        if self.raft.role() != Role::Leader
            || !self.engine.store().pending_flushes().is_empty()
            || self.engine.store().memtable().log_range().is_none()
        {
            return;
        }
        if let Some((term, index)) = self.flush.begin_proposed
            && term == self.raft.term()
            && index > self.engine.store().applied_index()
        {
            return;
        }
        if let Ok(index) = self.raft.propose(Command::FlushBegin.to_bytes()) {
            self.flush.begin_proposed = Some((self.raft.term(), index));
        }
    }

    /// Whether an index build (flush or compaction) runs on this replica.
    fn busy(&self) -> bool {
        self.flush.building.is_some() || self.flush.compacting.is_some()
    }

    /// Moves frozen memtables towards publication (ADR 0016): publishes what is ready, in
    /// order; the leader (or a follower falling back, or every replica when shipping is off)
    /// builds the oldest freeze without a file; a follower fetches the oldest committed one.
    async fn drive_flushes(&mut self) -> Result<()> {
        if self.fetch.is_some() {
            // Installing a snapshot: the store must not change under it.
            return Ok(());
        }
        let leader = self.raft.role() == Role::Leader;
        // Announce segments built here once leader (the old leader may have died before).
        if leader {
            let term = self.raft.term();
            let todo: Vec<(SegmentId, (u64, u64))> = self
                .flush
                .announce
                .iter()
                .filter(|(_, (_, t))| *t != Some(term))
                .map(|(id, (f, _))| (*id, *f))
                .collect();
            for (id, (len, hash)) in todo {
                if self
                    .raft
                    .propose(
                        Command::FlushCommit {
                            id,
                            len,
                            hash,
                            from: self.cfg.id,
                        }
                        .to_bytes(),
                    )
                    .is_ok()
                    && let Some(e) = self.flush.announce.get_mut(&id)
                {
                    e.1 = Some(term);
                }
            }
        }
        if self.engine.store_mut().publish_ready().await? > 0 {
            self.after_publish().await?;
        }
        let pending = self.engine.store().pending_flushes();
        let mut live: std::collections::BTreeSet<SegmentId> =
            pending.iter().map(|p| p.id).collect();
        // Fallbacks of committed compactions are kept too (dropping them made a follower
        // re-fetch a missing file on every tick).
        live.extend(
            self.engine
                .store()
                .pending_compactions()
                .iter()
                .map(|(c, _, _)| c.id),
        );
        self.flush.fallback.retain(|id| live.contains(id));
        self.flush.waiting_since.retain(|id, _| live.contains(id));
        // Fetches of items published or replaced meanwhile are dropped.
        self.flush.fetches.retain(|id, _| live.contains(id));
        let election = u64::from(self.cfg.election_ticks.max(1));
        let mut to_build = None;
        let mut to_fetch: Vec<(SegmentId, NodeId)> = Vec::new();
        let me = self.cfg.id;
        for p in &pending {
            if p.has_file || p.empty {
                continue;
            }
            let fetching = self.flush.fetches.contains_key(&p.id);
            // A committed file is fetched from the node that built it, whoever leads now (a
            // new leader included: rebuilding it was wasted work, and its followers then
            // asked it for a file it did not have yet). Built here: no commit yet and this
            // replica leads, its own file, no shipping, or a failed fetch.
            let local = !self.cfg.ship_segments
                || self.flush.fallback.contains(&p.id)
                || p.commit_from == Some(me)
                || (p.commit.is_none() && leader);
            if local && !fetching {
                if to_build.is_none() && self.flush.building != Some(p.id) {
                    to_build = Some(p.id);
                }
                continue;
            }
            if let (Some(_), Some(src)) = (p.commit, p.commit_from) {
                if !fetching {
                    to_fetch.push((p.id, src));
                }
            } else if p.commit.is_none() {
                let since = *self.flush.waiting_since.entry(p.id).or_insert(self.ticks);
                if self.ticks - since >= COMMIT_WAIT_ELECTIONS * election {
                    tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, id = %p.id, "no commit for a freeze: building it here");
                    self.flush.fallback.insert(p.id);
                    if to_build.is_none() {
                        to_build = Some(p.id);
                    }
                }
            }
        }
        if let Some(id) = to_build
            && !self.busy()
        {
            self.start_flush_build(id)?;
        }
        for (id, from) in to_fetch {
            if self.flush.fetches.len() >= MAX_SEG_FETCHES {
                break;
            }
            self.start_seg_fetch(id, false, from).await?;
        }
        self.drive_compactions().await
    }

    /// A snapshot fetch whose requests or chunks were lost would wait forever (chaos campaign,
    /// 3% drops): after a stall, re-request each missing file at its current offset.
    async fn retry_stalled_snapshot_fetch(&mut self) {
        let stall = FETCH_STALL_ELECTIONS * u64::from(self.cfg.election_ticks.max(1));
        let ticks = self.ticks;
        let Some(f) = self.fetch.as_mut() else {
            return;
        };
        if ticks - f.progress_tick < stall {
            return;
        }
        f.progress_tick = ticks;
        f.stalls += 1;
        // Same source: another replica cannot have the source's compaction outputs (their ids
        // are per replica). A source whose files are gone answers so, and the fetch restarts.
        let (from, req) = (f.from, f.req);
        let todo: Vec<(String, u64)> = f.partial.iter().map(|(p, o)| (p.clone(), *o)).collect();
        for (path, offset) in todo {
            self.send(from, FrameBody::FetchFile { req, path, offset })
                .await;
        }
    }

    /// Leader balancing (ADR 0020): a leader that is not the shard's preferred leader hands
    /// leadership to it once it is caught up. Not while this replica builds or holds an
    /// unpublished freeze (the new leader would redo the build), not within a few election
    /// timeouts of winning leadership, and at most once per cooldown.
    fn maybe_hand_over_leadership(&mut self) {
        let leader = self.raft.role() == Role::Leader;
        match (leader, self.flush.lead_since) {
            (true, None) => self.flush.lead_since = Some(self.ticks),
            (false, Some(_)) => self.flush.lead_since = None,
            _ => {}
        }
        let Some(pref) = self.cfg.preferred_leader else {
            return;
        };
        let election = u64::from(self.cfg.election_ticks.max(1));
        let settled = self
            .flush
            .lead_since
            .is_some_and(|t| self.ticks - t >= 3 * election);
        if !leader
            || !settled
            || pref == self.cfg.id
            || !self.cfg.peers.contains(&pref)
            || self.raft.transferring_to().is_some()
            || self.ticks < self.flush.next_handover
            || self.busy()
            || self.fetch.is_some()
            || self.flush.own_compaction.is_some()
            || !self.engine.store().pending_flushes().is_empty()
            || !self.engine.store().pending_compactions().is_empty()
        {
            return;
        }
        // The preferred replica must hold everything committed: a replica that is down or
        // lagging is not handed the shard (it gets it once it has caught up).
        if self
            .raft
            .matched_index(pref)
            .is_none_or(|m| m < self.raft.commit_index())
        {
            return;
        }
        self.flush.next_handover = self.ticks + 10 * election;
        if self.raft.transfer_leadership(pref).is_ok() {
            self.flush.handovers += 1;
            tracing::info!(node = %self.cfg.id, shard = %self.cfg.shard, to = %pref, "handing leadership to the preferred replica");
        }
    }

    /// Abandons the fetch of `id`: it is built here instead.
    fn fetch_failed(&mut self, id: SegmentId, why: &str) {
        if self.flush.fetches.remove(&id).is_some() {
            tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "segment fetch failed ({why}): building it here");
            self.flush.fallback.insert(id);
        }
    }

    /// Starts fetching segment `id` from `from`: the first chunk tells the length, then up to
    /// `FETCH_WINDOW` chunks stay in flight.
    async fn start_seg_fetch(
        &mut self,
        id: SegmentId,
        compaction: bool,
        from: NodeId,
    ) -> Result<()> {
        self.engine
            .store()
            .reset_fetch_staging(&Store::<R>::ship_staging(id))
            .await?;
        let req = self.next_read;
        self.next_read += 1;
        self.flush.fetches.insert(
            id,
            SegFetch {
                compaction,
                from,
                req,
                total: None,
                next: CHUNK as u64,
                inflight: [0u64].into_iter().collect(),
                got: 0,
                progress_tick: self.ticks,
                retry_tick: self.ticks,
            },
        );
        self.send(
            from,
            FrameBody::FetchFile {
                req,
                path: seg_rel(id),
                offset: 0,
            },
        )
        .await;
        Ok(())
    }

    /// Builds the indexes of the freeze `id` off the actor (`Runtime::offload`); the result
    /// comes back as an event.
    fn start_flush_build(&mut self, id: SegmentId) -> Result<()> {
        // Flush builds share the node's slots with compactions: each build holds its rows,
        // their vectors and the new graph in memory at once.
        if let Some(slots) = &self.cfg.compaction_slots {
            if !slots.try_acquire() {
                return Ok(());
            }
            self.holds_slot = true;
        }
        let Some(job) = self.engine.store().flush_job(id) else {
            self.release_slot();
            return Ok(());
        };
        self.flush.building = Some(id);
        let (path, schema, version) = self.engine.store().segment_write_params(id);
        let indexer = DefaultIndexer {
            vector: self.cfg.engine.vector,
            parallel: self.cfg.build_parallel.clone(),
        };
        let inbox = self.inbox.clone();
        let rt = self.rt.clone();
        let generation = job.generation;
        // Build on a helper thread, write from a task of its own: the actor keeps serving Raft
        // (a long write on the actor delayed heartbeats and cost leaderships, ADR 0021).
        self.rt.spawn(async move {
            let docs = job.docs;
            let schema2 = schema.clone();
            let (docs, sections) = rt
                .offload(move || {
                    let sections = Store::<R>::build_sections(&schema2, &docs, &indexer);
                    (docs, sections)
                })
                .await;
            let file = match sections {
                Ok(sections) => {
                    cairn_storage::store::write_segment_file(
                        rt.clone(),
                        &path,
                        &schema,
                        version,
                        &docs,
                        &sections,
                    )
                    .await
                }
                Err(e) => Err(e),
            };
            inbox.push(Event::FlushWritten {
                id,
                generation,
                file,
            });
        });
        Ok(())
    }

    /// Proposes a merge built here (leader).
    fn propose_merge(&mut self, id: SegmentId, inputs: Vec<SegmentId>, file: (u64, u64)) {
        let cmd = Command::CompactCommit {
            id,
            inputs: inputs.clone(),
            len: file.0,
            hash: file.1,
            from: self.cfg.id,
        };
        if self.raft.propose(cmd.to_bytes()).is_ok() {
            self.flush.own_compaction = Some((id, file, self.raft.term()));
            tracing::info!(node = %self.cfg.id, shard = %self.cfg.shard, %id, ?inputs, "compaction proposed");
        }
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

    /// Compacts the in-memory Raft log to `snap`, unless this leader still has a follower
    /// behind it and the log is under [`RETAIN_LOG_BYTES`]: then it only offers the new
    /// snapshot and keeps the entries.
    fn compact_raft_log(&mut self, snap: Snapshot) {
        let keep = self
            .raft
            .min_follower_matched()
            .is_some_and(|m| m < snap.last_index)
            && self.raft.log_bytes() < RETAIN_LOG_BYTES;
        if keep {
            self.raft.offer_snapshot(snap);
        } else {
            self.raft.compact(snap);
        }
    }

    /// Term of the entry at the manifest's applied index: recorded in the manifest itself,
    /// else (older manifests) Raft's view of it, else the persisted state.
    fn manifest_term(&self) -> Term {
        let m = self.engine.store().manifest();
        match m.applied_term {
            Term(0) => self
                .raft
                .term_at(m.applied_index)
                .unwrap_or(self.state.snapshot_term),
            t => t,
        }
    }

    async fn after_publish(&mut self) -> Result<()> {
        self.engine.refresh().await?;
        let up_to = self.engine.store().manifest().applied_index;
        tracing::info!(node = %self.cfg.id, applied = %up_to, segments = ?self.engine.store().segments().map(|s| s.id.get()).collect::<Vec<_>>(), "flushed");
        if up_to > LogIndex(0) {
            let term = self.manifest_term();
            let snap = Snapshot {
                last_index: up_to,
                last_term: term,
                data: self.engine.store().manifest_bytes(),
            };
            self.compact_raft_log(snap);
            self.state.snapshot_term = term;
            self.state_store.store(&self.state).await?;
        }
        // A full memtable flushes before any compaction: compactions are long, and writes are
        // held back while the memtable is over its limit.
        if self.engine.store().memtable_bytes() >= self.cfg.engine.store.memtable_max_bytes {
            self.propose_flush_begin();
            self.release_deferred();
            return Ok(());
        }
        if self.engine.store().pending_flushes().is_empty() {
            self.maybe_compact_job().await?;
        }
        self.release_deferred();
        Ok(())
    }

    /// Background upkeep on ticks: flush an idle memtable, and retry a compaction that was
    /// waiting for a free slot.
    async fn on_idle_tick(&mut self) -> Result<()> {
        // Write only a segment format every peer reads (ADR 0018): peers announce theirs when
        // they connect, and a rolling upgrade switches formats once the last one did.
        let v = cairn_storage::segment::negotiated_segment_version(
            self.rt.network().peer_segment_version(),
        );
        self.engine.store_mut().set_segment_version(v);
        // Held-back proposals are retried on every tick, not only after a flush: a replica that
        // lost leadership while holding them would otherwise keep clients waiting until their
        // timeout (seen on GCP). On a follower, `propose` fails them with a leader hint.
        if !self.deferred.is_empty() {
            if self.raft.role() != Role::Leader {
                for (cmd, done) in self.deferred.drain(..).collect::<Vec<_>>() {
                    self.propose(cmd, done);
                }
            } else {
                self.release_deferred();
            }
        }
        let idle = u64::from(self.cfg.idle_flush_ticks);
        if idle > 0
            && self.ticks - self.last_apply_tick >= idle
            && self.engine.store().memtable_bytes() > 0
        {
            self.propose_flush_begin();
        }
        if self.engine.store().memtable_bytes() >= self.cfg.engine.store.memtable_max_bytes {
            self.propose_flush_begin();
        }
        self.retry_stalled_snapshot_fetch().await;
        self.maybe_hand_over_leadership();
        // Files of segments replaced by a compaction outlive them for a while: a follower or a
        // snapshot install may still be fetching them.
        let grace = RETIRED_GRACE_ELECTIONS * u64::from(self.cfg.election_ticks.max(1));
        while let Some(&(id, at)) = self.flush.retired.front() {
            if self.ticks - at < grace {
                break;
            }
            self.flush.retired.pop_front();
            self.engine.store().purge_segment_files(id).await?;
        }
        // Segment fetches: chunks lost in flight are asked again after an election timeout; a
        // fetch with no chunk for several falls back to a local build.
        let election = u64::from(self.cfg.election_ticks.max(1));
        let stall = FETCH_STALL_ELECTIONS * election;
        let ticks = self.ticks;
        let stalled: Vec<SegmentId> = self
            .flush
            .fetches
            .iter()
            .filter(|(_, f)| ticks - f.progress_tick >= stall)
            .map(|(id, _)| *id)
            .collect();
        for id in stalled {
            self.fetch_failed(id, "no data");
        }
        let mut resend: Vec<(NodeId, u64, SegmentId, u64)> = Vec::new();
        for (id, f) in self.flush.fetches.iter_mut() {
            if ticks - f.progress_tick.max(f.retry_tick) >= election {
                f.retry_tick = ticks;
                resend.extend(f.inflight.iter().map(|off| (f.from, f.req, *id, *off)));
            }
        }
        for (from, req, id, offset) in resend {
            self.send(
                from,
                FrameBody::FetchFile {
                    req,
                    path: seg_rel(id),
                    offset,
                },
            )
            .await;
        }
        // Builds may be waiting for a slot, fetches for a leader, freezes for a commit.
        self.drive_flushes().await?;
        if self.cfg.compaction_slots.is_some()
            && !self.busy()
            && self.ticks % 20 == 0
            && self.engine.store().pending_flushes().is_empty()
        {
            self.maybe_compact_job().await?;
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

    /// Leader only (ADR 0021): picks a compaction per policy and builds it; the result is
    /// proposed as a `CompactCommit` and installed everywhere when applied. One compaction at
    /// a time: none while another is committed but not installed, or proposed but not
    /// committed.
    async fn maybe_compact_job(&mut self) -> Result<()> {
        if self.raft.role() != Role::Leader
            || self.busy()
            || self.fetch.is_some()
            || self.flush.own_compaction.is_some()
            || !self.engine.store().pending_compactions().is_empty()
            // A new leader waits until every entry of earlier terms is applied: a merge the old
            // leader committed is then pending here and fetched, not merged again.
            || self.engine.store().applied_index() < self.raft.lead_start()
        {
            return Ok(());
        }
        let Some(inputs) = self.engine.store().select_compaction() else {
            return Ok(());
        };
        // The same merge built here before and lost with a leadership change: propose that file
        // again (a commit with a known id is ignored, so a late duplicate is harmless).
        if let Some((last_inputs, id, file)) = self.flush.last_merge.clone()
            && last_inputs == inputs
            && !self.engine.store().compaction_accepted(id)
        {
            let (built, _, _) = self.engine.store().segment_write_params(id);
            let path = built.trim_end_matches(".built").to_owned();
            if self.rt.disk().exists(&path).await? {
                self.propose_merge(id, inputs, file);
                return Ok(());
            }
        }
        let id = self.engine.store_mut().reserve_compaction_id().await?;
        self.start_compaction_build(id, inputs)
    }

    /// Builds the merged segment `id` from `inputs` (their rows live now): rows are read, the
    /// indexes built and the file written outside the actor.
    fn start_compaction_build(&mut self, id: SegmentId, inputs: Vec<SegmentId>) -> Result<()> {
        let Some((sources, _)) = self.engine.store().compaction_sources(&inputs) else {
            return Ok(());
        };
        if let Some(slots) = &self.cfg.compaction_slots {
            if !slots.try_acquire() {
                return Ok(());
            }
            self.holds_slot = true;
        }
        tracing::info!(node = %self.cfg.id, shard = %self.cfg.shard, %id, ?inputs, "compaction build started");
        self.flush.compacting = Some(id);
        let (path, schema, version) = self.engine.store().segment_write_params(id);
        let generation = self.engine.store().generation();
        let indexer = DefaultIndexer {
            vector: self.cfg.engine.vector,
            parallel: self.cfg.build_parallel.clone(),
        };
        let inbox = self.inbox.clone();
        let rt = self.rt.clone();
        self.rt.spawn(async move {
            let file = async {
                let docs = cairn_storage::store::read_compaction_rows(rt.clone(), &sources).await?;
                let schema2 = schema.clone();
                let (docs, sections) = rt
                    .offload(move || {
                        let sections = Store::<R>::build_sections(&schema2, &docs, &indexer);
                        (docs, sections)
                    })
                    .await;
                cairn_storage::store::write_segment_file(
                    rt.clone(),
                    &path,
                    &schema,
                    version,
                    &docs,
                    &sections?,
                )
                .await
            }
            .await;
            inbox.push(Event::CompactWritten {
                id,
                inputs,
                generation,
                file,
            });
        });
        Ok(())
    }

    /// Moves committed compactions towards installation (ADR 0021), oldest first: installs
    /// when the file is here and the inputs are published; otherwise the leader (or a
    /// follower falling back, or every replica without shipping) builds it, and a follower
    /// fetches it from the leader.
    async fn drive_compactions(&mut self) -> Result<()> {
        if let Some((_, _, term)) = self.flush.own_compaction
            && term != self.raft.term()
        {
            // Leadership changed: the proposal may never commit. If it does, the file is
            // fetched or rebuilt like any other.
            self.flush.own_compaction = None;
        }
        loop {
            let pending = self.engine.store().pending_compactions();
            let Some((c, has_file, inputs_ready)) = pending.first().cloned() else {
                return Ok(());
            };
            if !has_file
                && let Some((id, file, _)) = self.flush.own_compaction
                && id == c.id
            {
                self.engine.store_mut().set_compaction_file(id, file);
                self.flush.own_compaction = None;
                continue;
            }
            if has_file && inputs_ready {
                let n = self.engine.store_mut().install_compactions().await?;
                if n > 0 {
                    self.after_compaction_install().await?;
                    continue;
                }
                return Ok(());
            }
            // Merges are fetched ahead of their turn, several at once (installation stays in
            // log order), even while the oldest waits for flushes to publish here.
            for (p, p_has_file, _) in &pending {
                if self.flush.fetches.len() >= MAX_SEG_FETCHES {
                    break;
                }
                let local = !self.cfg.ship_segments
                    || self.flush.fallback.contains(&p.id)
                    || p.from == self.cfg.id;
                if !p_has_file && !local && !self.flush.fetches.contains_key(&p.id) {
                    self.start_seg_fetch(p.id, true, p.from).await?;
                }
            }
            if !inputs_ready || has_file {
                // Earlier flushes still to publish here.
                return Ok(());
            }
            let fetching = self.flush.fetches.contains_key(&c.id);
            // From the node that built it, whoever leads now; here if it is ours (a restart
            // lost the file), without shipping, or after a failed fetch.
            let local = !self.cfg.ship_segments
                || self.flush.fallback.contains(&c.id)
                || c.from == self.cfg.id;
            if local && !fetching && !self.busy() && self.flush.compacting != Some(c.id) {
                self.start_compaction_build(c.id, c.inputs.clone())?;
            }
            return Ok(());
        }
    }

    /// After merged segments were installed: reload indexes, and offer lagging followers a
    /// snapshot that references the new files.
    async fn after_compaction_install(&mut self) -> Result<()> {
        let ticks = self.ticks;
        let retired = self.engine.store_mut().take_retired();
        self.flush
            .retired
            .extend(retired.into_iter().map(|id| (id, ticks)));
        self.engine.refresh().await?;
        let up_to = self.engine.store().manifest().applied_index;
        if up_to > LogIndex(0) {
            let term = self.manifest_term();
            self.compact_raft_log(Snapshot {
                last_index: up_to,
                last_term: term,
                data: self.engine.store().manifest_bytes(),
            });
        }
        self.maybe_compact_job().await
    }

    async fn start_fetch(&mut self, s: Snapshot, source: Option<NodeId>) -> Result<()> {
        let m: cairn_storage::ShardManifest = ManifestStore::<R>::decode(&s.data)?;
        let needed = self.engine.store().snapshot_files_needed(&m);
        // The node that sent the snapshot: its files match its manifest (the same segment id
        // may hold other bytes elsewhere). Older pending snapshots name no sender: the
        // leader, or another peer; a mismatch then makes the snapshot stale.
        let me = self.cfg.id;
        let from = source
            .filter(|l| *l != me)
            .or_else(|| self.raft.leader().filter(|l| *l != me))
            .or_else(|| self.cfg.peers.iter().copied().find(|p| *p != me))
            .unwrap_or(me);
        tracing::info!(node = %self.cfg.id, %from, last_index = %s.last_index, segments = ?m.segments.iter().map(|x| x.id.get()).collect::<Vec<_>>(), "fetching snapshot");
        let req = self.next_read;
        self.next_read += 1;
        let mut fetch = SnapshotFetch {
            manifest: s.data.clone(),
            last_index: s.last_index,
            last_term: s.last_term,
            from,
            needed: needed.clone(),
            got: Vec::new(),
            partial: HashMap::default(),
            req,
            progress_tick: self.ticks,
            stalls: 0,
            floor: s.last_index,
        };
        if needed.is_empty() {
            self.fetch = Some(fetch);
            return self.finish_fetch().await;
        }
        for p in &needed {
            fetch.partial.insert(p.clone(), 0);
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
        if let Some(id) = self
            .flush
            .fetches
            .iter()
            .find(|(_, f)| f.req == req)
            .map(|(id, _)| *id)
        {
            return self
                .on_segment_chunk(id, from, path, offset, total, data)
                .await;
        }
        let Some(f) = self.fetch.as_mut() else {
            return Ok(());
        };
        if f.req != req || f.from != from {
            return Ok(());
        }
        let Some(have) = f.partial.get(&path).copied() else {
            return Ok(());
        };
        f.progress_tick = self.ticks;
        if total == NO_CHECKPOINT && path.ends_with(".del") {
            // No deletions on the server for a segment it still has.
            f.partial.remove(&path);
            f.needed.retain(|p| *p != path);
        } else if total == u64::MAX {
            // A segment, or the deletion checkpoint of a segment, that is gone on the server
            // (compacted away): the snapshot is stale. A local checkpoint for a reused file
            // would miss deletions, so none may be assumed.
            return self
                .drop_stale_snapshot(format!("snapshot file {path} vanished on the leader"))
                .await;
        } else if offset == have {
            // Stream to the staging file: a catch-up never holds whole segments in memory.
            let len = data.len() as u64;
            self.engine
                .store()
                .write_fetch_chunk(&path, offset, data)
                .await?;
            let have = have + len;
            if have >= total || len == 0 {
                self.engine.store().sync_fetched(&path).await?;
                if let Some(f) = self.fetch.as_mut() {
                    f.partial.remove(&path);
                    f.got.push(path.clone());
                }
            } else {
                if let Some(f) = self.fetch.as_mut() {
                    f.partial.insert(path.clone(), have);
                }
                self.send(
                    from,
                    FrameBody::FetchFile {
                        req,
                        path,
                        offset: have,
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

    /// One chunk of a leader-built segment (ADR 0016): streamed to the staging file; the
    /// complete file is checked against the commit and installed, or the freeze falls back to a
    /// local build.
    async fn on_segment_chunk(
        &mut self,
        id: SegmentId,
        from: NodeId,
        path: String,
        offset: u64,
        total: u64,
        data: Bytes,
    ) -> Result<()> {
        let Some(f) = self.flush.fetches.get(&id) else {
            return Ok(());
        };
        let (req, compaction) = (f.req, f.compaction);
        if f.from != from || path != seg_rel(id) || !f.inflight.contains(&offset) {
            // Another source, or a duplicate of a chunk already written.
            return Ok(());
        }
        let expected = if compaction {
            self.engine
                .store()
                .pending_compactions()
                .into_iter()
                .find(|(c, _, _)| c.id == id)
                .map(|(c, _, _)| (c.len, c.hash))
        } else {
            self.engine
                .store()
                .pending_flushes()
                .into_iter()
                .find(|p| p.id == id)
                .and_then(|p| p.commit)
        };
        let Some((len, _)) = expected else {
            self.flush.fetches.remove(&id);
            return Ok(());
        };
        if total == u64::MAX {
            // Gone on the source (compacted away, or it never had this file).
            self.fetch_failed(id, "missing on the source");
            return self.drive_flushes().await;
        }
        if total != len {
            self.fetch_failed(id, "length differs from the commit");
            return self.drive_flushes().await;
        }
        let n = data.len() as u64;
        let staging = Store::<R>::ship_staging(id);
        self.engine
            .store()
            .write_fetch_chunk_at(&staging, offset, data)
            .await?;
        let ticks = self.ticks;
        let mut requests = Vec::new();
        let done = {
            let Some(f) = self.flush.fetches.get_mut(&id) else {
                return Ok(());
            };
            f.inflight.remove(&offset);
            f.got += n;
            f.total = Some(total);
            f.progress_tick = ticks;
            while f.inflight.len() < FETCH_WINDOW && f.next < total {
                requests.push(f.next);
                f.inflight.insert(f.next);
                f.next += CHUNK as u64;
            }
            f.got >= total && f.inflight.is_empty()
        };
        for offset in requests {
            self.send(
                from,
                FrameBody::FetchFile {
                    req,
                    path: path.clone(),
                    offset,
                },
            )
            .await;
        }
        if !done {
            return Ok(());
        }
        self.flush.fetches.remove(&id);
        self.engine.store().sync_fetched(&staging).await?;
        let installed = if compaction {
            self.engine
                .store_mut()
                .install_fetched_compaction(id)
                .await?
        } else {
            self.engine.store_mut().install_fetched_flush(id).await?
        };
        if installed {
            self.flush.fetched += 1;
            tracing::debug!(node = %self.cfg.id, shard = %self.cfg.shard, %id, bytes = total, "segment fetched");
        } else {
            tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "fetched segment does not match its commit: building it here");
            self.flush.fallback.insert(id);
        }
        self.drive_flushes().await
    }

    /// Drops the accepted snapshot and the log after it, and reopens from disk so the leader
    /// ships a fresh one. The entries after it were acknowledged: until the log is back to
    /// them, this node must not vote for a candidate without them (Raft's vote barrier).
    async fn drop_stale_snapshot(&mut self, why: String) -> Result<()> {
        if self.raft.last_index() > self.state.vote_barrier {
            self.state.vote_barrier = self.raft.last_index();
            self.state.vote_barrier_term = self.raft.last_log_term();
        }
        self.state_store.store(&self.state).await?;
        let pending = format!("{}/SNAPSHOT", self.cfg.dir);
        if self.rt.disk().exists(&pending).await? {
            self.rt.disk().remove(&pending).await?;
        }
        Err(Error::Internal(why))
    }

    async fn finish_fetch(&mut self) -> Result<()> {
        let Some(f) = self.fetch.take() else {
            return Ok(());
        };
        // Persist the read floor and the snapshot's term before the files become the shard
        // state: a crash right after the install must neither serve reads early nor restart
        // Raft with the term of an older snapshot point at the new one (chaos seed 19929: a
        // follower then refused every append at that index, forever).
        self.state.read_floor = self.state.read_floor.max(f.floor);
        self.state.snapshot_term = f.last_term;
        self.state_store.store(&self.state).await?;
        let files: Vec<String> = f
            .needed
            .iter()
            .filter(|p| f.got.contains(p))
            .cloned()
            .collect();
        match self
            .engine
            .store_mut()
            .install_snapshot(&f.manifest, files)
            .await
        {
            Err(Error::Internal(m)) if m.starts_with(cairn_storage::store::SNAPSHOT_MISMATCH) => {
                return self.drop_stale_snapshot(m).await;
            }
            r => r?,
        }
        // Installed: the manifest now holds it.
        let pending = format!("{}/SNAPSHOT", self.cfg.dir);
        if self.rt.disk().exists(&pending).await? {
            self.rt.disk().remove(&pending).await?;
        }
        self.engine.refresh().await?;
        let applied = self.engine.store().applied_index();
        debug_assert_eq!(applied, f.last_index);
        // Term of the snapshot point as Raft recorded it.
        self.state.snapshot_term = self.raft.snapshot_term();
        self.state_store.store(&self.state).await?;
        // Offer the installed state to lagging followers should this replica become leader:
        // without it, a leader whose log starts after a follower's next index sent that
        // follower nothing at all, not even heartbeats (chaos seed 1431, found once the
        // campaign flushed).
        self.raft.compact(Snapshot {
            last_index: applied,
            last_term: self.raft.snapshot_term(),
            data: self.engine.store().manifest_bytes(),
        });
        self.raft.advance(applied, applied);
        // Anyone waiting on an index at or below the snapshot can proceed.
        self.serve_waiting().await
    }
}

fn seg_rel(id: SegmentId) -> String {
    format!("segs/{:016x}.seg", id.get())
}
