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
use cairn_core::sync::{LocalQueue, Receiver, Sender, oneshot};
use cairn_core::{
    Disk, DocId, Document, Duration, Error, HashMap, LogIndex, Network, NodeId, Result, Runtime,
    Schema, SegmentId, ShardId, Term,
};
use cairn_index::DefaultIndexer;
use cairn_raft::{Entry, HardState, InitialState, Message, Raft, Ready, Role, Snapshot};
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

/// A command committed and applied by the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    /// Its consistency token.
    pub token: Token,
    /// Documents a deletion by filter removed (0 for other commands).
    pub deleted: u64,
}

/// Bounds how many compaction jobs run at once across the replicas that share it (one node).
/// A compaction holds all live rows of its input segments in memory while it rebuilds their
/// indexes, so an unbounded number of concurrent jobs can exhaust a node's RAM.
#[derive(Debug)]
pub struct JobSlots {
    used: std::sync::atomic::AtomicUsize,
    max: usize,
    /// No new merge starts while set (merges already committed still complete).
    merges_paused: std::sync::atomic::AtomicBool,
}

impl JobSlots {
    /// Allows `max` concurrent jobs.
    pub fn new(max: usize) -> Self {
        JobSlots {
            used: std::sync::atomic::AtomicUsize::new(0),
            max: max.max(1),
            merges_paused: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Pauses or resumes merges on the replicas sharing these slots (one node). A paused node
    /// starts no new merge; merges it already runs, and merges committed in a shard's log,
    /// still complete, so a shard never waits on a decided merge. Queries can then be
    /// measured on an idle node, and an operator can hold merges off during peak hours.
    pub fn set_merges_paused(&self, paused: bool) {
        self.merges_paused
            .store(paused, std::sync::atomic::Ordering::Release);
    }

    /// Whether merges are paused.
    pub fn merges_paused(&self) -> bool {
        self.merges_paused
            .load(std::sync::atomic::Ordering::Acquire)
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

    /// Takes a slot for a merge, which may hold it for minutes: never the last free one when
    /// there are two or more, so a flush always finds one. Without this, two merges on a
    /// 2-slot node held off every flush, memtables filled, and writes stalled for up to 15
    /// minutes (GCP 50M run, 2026-09-25).
    pub fn try_acquire_merge(&self) -> bool {
        use std::sync::atomic::Ordering;
        let limit = if self.max >= 2 { self.max - 1 } else { 1 };
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |u| {
                (u < limit).then_some(u + 1)
            })
            .is_ok()
    }

    /// Returns a slot.
    pub fn release(&self) {
        self.used.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Returns the node build slot recorded in `held`, if any.
fn release_slot(held: &mut bool, cfg: &ReplicaConfig) {
    if std::mem::take(held)
        && let Some(slots) = &cfg.compaction_slots
    {
        slots.release();
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
    /// Merges building here (0 or 1), merges committed and not yet installed here.
    pub merges: [u64; 2],
    /// Whether this node starts no new merge ([`JobSlots::set_merges_paused`]).
    pub merges_paused: bool,
}

/// What a point read targets: an internal id, or a text id (ADR 0031).
enum DocRef {
    Id(DocId),
    Key(String),
}

enum Event {
    Tick,
    Net(NodeId, Bytes),
    Propose(Command, Sender<Result<Applied>>),
    Query(Query, Consistency, Sender<Result<Vec<Hit>>>),
    QueryLegs(
        Query,
        Consistency,
        Sender<Result<Vec<crate::fusion::LegList>>>,
    ),
    Get(DocRef, Consistency, Sender<Result<Option<Document>>>),
    Status(Sender<ReplicaStatus>),
    /// Stop the replica: its actor exits and closes its files, then answers.
    Stop(Sender<()>),
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
    /// The persistence task with this sequence number finished (ADR 0027).
    Persisted(u64),
    /// A fetched segment file was synced and checked outside the actor (ADR 0027).
    FetchVerified {
        id: SegmentId,
        compaction: bool,
        expected: (u64, u64),
        ok: Result<bool>,
    },
    /// A chunk of the fetch `req` of segment `id` was written outside the actor (ADR 0027).
    ChunkWritten {
        id: SegmentId,
        req: u64,
        ok: Result<()>,
    },
    /// A segment's indexes were decoded outside the actor, ahead of its publication (ADR 0026).
    IndexesReady {
        id: SegmentId,
        indexes: Result<crate::engine::PreparedIndexes>,
        /// Renames made durable by the directory sync this preparation ran first.
        synced: Vec<SegmentId>,
    },
}

impl Event {
    fn kind(&self) -> &'static str {
        match self {
            Event::Tick => "tick",
            Event::Net(..) => "net",
            Event::Propose(..) => "propose",
            Event::Query(..) => "query",
            Event::QueryLegs(..) => "query_legs",
            Event::Get(..) => "get",
            Event::Status(..) => "status",
            Event::Stop(_) => "stop",
            Event::FlushWritten { .. } => "flush_written",
            Event::CompactWritten { .. } => "compact_written",
            Event::IndexesReady { .. } => "indexes_ready",
            Event::Persisted(_) => "persisted",
            Event::FetchVerified { .. } => "fetch_verified",
            Event::ChunkWritten { .. } => "chunk_written",
        }
    }
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
                Event::Stop(done) => done.send(()),
                Event::Status(_)
                | Event::Tick
                | Event::Net(..)
                | Event::FlushWritten { .. }
                | Event::CompactWritten { .. }
                | Event::Persisted(_)
                | Event::FetchVerified { .. }
                | Event::ChunkWritten { .. }
                | Event::IndexesReady { .. } => {}
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

    /// Stops the replica: resolves once its actor has exited and closed its files. Requests
    /// sent afterwards fail as for a stopped replica.
    pub async fn stop(&self) {
        if !self.alive.get() {
            return;
        }
        let (tx, rx) = oneshot();
        self.inbox.push(Event::Stop(tx));
        let _ = rx.await;
    }

    /// Proposes a command; resolves once applied here, or with `NotLeader`.
    pub async fn propose(&self, cmd: Command) -> Result<Token> {
        self.propose_applied(cmd).await.map(|a| a.token)
    }

    /// Like [`ReplicaHandle::propose`], with what applying the command did.
    pub async fn propose_applied(&self, cmd: Command) -> Result<Applied> {
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
        self.get_ref(DocRef::Id(id), consistency).await
    }

    /// Point read by text id (ADR 0031), resolved on the replica when the read is served.
    pub async fn get_key(&self, key: String, consistency: Consistency) -> Result<Option<Document>> {
        self.get_ref(DocRef::Key(key), consistency).await
    }

    async fn get_ref(&self, target: DocRef, consistency: Consistency) -> Result<Option<Document>> {
        if !self.alive.get() {
            return closed();
        }
        let (tx, rx) = oneshot();
        self.inbox.push(Event::Get(target, consistency, tx));
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
    /// Chunks received and still being written outside the actor. They count towards
    /// `FETCH_WINDOW`: a disk stalled behind builds must slow the fetch down, not pile up
    /// writes (each holds a file descriptor and a chunk; GCP run 7 ran out of descriptors).
    writing: usize,
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
/// A replica step at least this long is logged with its parts ("slow replica step").
const SLOW_STEP: cairn_core::Duration = cairn_core::Duration::from_millis(500);

/// A follower that waited this many election timeouts for the `FlushCommit` of a freeze, with
/// no sign of progress from the same leader meanwhile, builds it itself. This is a safety net
/// for a lost announcement, not a failure detector: a leader that fails is replaced, and the
/// new leader builds or re-announces what is pending. A slow leader is not a failed one. With
/// 200 (100 s in production), a single 256 MB build on the GCP 50M run outlasted the wait and
/// every follower rebuilt every segment. 2400 is 20 minutes with 500 ms election timeouts.
const COMMIT_WAIT_ELECTIONS: u64 = 2400;

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
    waiting_commit: HashMap<LogIndex, (Term, Sender<Result<Applied>>)>,
    waiting_reads: HashMap<u64, Event>,
    waiting_applied: Vec<(LogIndex, Event)>,
    next_read: u64,
    fetch: Option<SnapshotFetch>,
    ticks: u64,
    last_apply_tick: u64,
    /// Query timing since the last report (`cairn_query::stats` at info, every 200 ticks):
    /// searches, microseconds preparing on the actor, microseconds from submission to result
    /// on the search pool (queue wait included).
    search_stats: std::rc::Rc<std::cell::Cell<(u64, u64, u64)>>,
    /// Node build slots held by this replica's flush build and merge build. A flush may run
    /// while this replica's merge runs, each in a slot of its own.
    flush_slot: bool,
    merge_slot: bool,
    /// Proposals held back while the memtable is over its hard limit (write backpressure).
    deferred: std::collections::VecDeque<(Command, Sender<Result<Applied>>)>,
    /// Flush state (ADR 0016), reset when the replica reopens.
    flush: FlushState,
    persist: Persistence,
    /// Set by [`ReplicaHandle::stop`]: answered once the actor has closed everything.
    stopped: Option<Sender<()>>,
}

/// What a persistence task reports: its sequence, the log index it made durable, the outcome.
type PersistOutcome = (u64, Option<LogIndex>, Result<()>);

/// Asynchronous persistence of the Raft log and state (ADR 0027). At most one task runs; work
/// written meanwhile goes to the next one, so one sync covers many batches.
#[derive(Default)]
struct Persistence {
    /// Sequence number of the running task.
    running: Option<u64>,
    /// The running task's outcome.
    done: Option<Receiver<PersistOutcome>>,
    /// Sequence the next task will carry.
    next_seq: u64,
    /// Written log entries or a state change not covered by any task yet.
    work: bool,
    /// `state` changed since it was last stored.
    state_dirty: bool,
    /// The log is durable through this index.
    durable: LogIndex,
    /// Messages waiting for durability, with the task sequence that makes them durable.
    held: std::collections::VecDeque<(u64, Vec<(NodeId, Message)>)>,
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
    /// Tick from which each freeze has waited for its commit: first seen without one, or
    /// the last sign of the leader's progress (a `FlushCommit` applied, a new leader).
    waiting_since: HashMap<SegmentId, u64>,
    /// Leader when `waiting_since` was last reset.
    waiting_leader: Option<NodeId>,
    /// Segments built here whose `FlushCommit` has not been applied yet: `(len, hash)` and the
    /// term in which this replica last proposed the commit.
    announce: std::collections::BTreeMap<SegmentId, ((u64, u64), Option<Term>)>,
    built: u64,
    fetched: u64,
    /// Flushes never built or fetched here because a merge consumed them (diagnostics).
    skipped: u64,
    /// Segments whose indexes are being decoded outside the actor (ADR 0026).
    preparing: std::collections::BTreeSet<SegmentId>,
    /// Fetched segments being synced and checked outside the actor (ADR 0027): not fetched
    /// again meanwhile.
    verifying: std::collections::BTreeSet<SegmentId>,
    /// Segments whose index preparation failed: published anyway, loading at publication.
    prepare_failed: std::collections::BTreeSet<SegmentId>,
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
        // A running persistence task must not write the state file after it is reread.
        if let Some(rx) = self.persist.done.take() {
            let _ = rx.await;
        }
        let (engine, raft, state, state_store, resume) =
            Self::open_state(&self.rt, &self.cfg, &self.schema).await?;
        self.engine = engine;
        self.raft = raft;
        self.state = state;
        self.state_store = state_store;
        self.fetch = None;
        self.flush = FlushState::default();
        self.persist = Persistence {
            next_seq: self.persist.next_seq + 1,
            ..Persistence::default()
        };
        self.persist.durable = self.log_durable_now();
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
            search_stats: Default::default(),
            flush_slot: false,
            merge_slot: false,
            deferred: std::collections::VecDeque::new(),
            flush: FlushState::default(),
            persist: Persistence::default(),
            stopped: None,
        };
        replica.persist.durable = replica.log_durable_now();
        replica.persist.next_seq = 1;
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
            let r = replica.run(resume).await;
            if r.is_ok() {
                // Stopped on request: close the engine's files before answering.
                let done = replica.stopped.take();
                drop(replica);
                drop(_guard);
                if let Some(done) = done {
                    done.send(());
                }
                return;
            }
            if let Err(e) = r {
                tracing::error!(node = %cfg.id, shard = %cfg.shard, "replica stopped: {e}");
                // Keep answering so callers fail fast instead of waiting forever.
                loop {
                    let ev = replica.inbox.pop().await;
                    match ev {
                        Event::Propose(_, done) => done.send(closed()),
                        Event::Query(_, _, done) => done.send(closed()),
                        Event::QueryLegs(_, _, done) => done.send(closed()),
                        Event::Get(_, _, done) => done.send(closed()),
                        Event::Stop(done) => done.send(()),
                        Event::Status(_)
                        | Event::Tick
                        | Event::Net(..)
                        | Event::FlushWritten { .. }
                        | Event::CompactWritten { .. }
                        | Event::Persisted(_)
                        | Event::FetchVerified { .. }
                        | Event::ChunkWritten { .. }
                        | Event::IndexesReady { .. } => {}
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
            if let Event::Stop(done) = ev {
                self.stopped = Some(done);
                return Ok(());
            }
            let kind = ev.kind();
            let t0 = self.rt.now();
            let step = async {
                self.handle_event(ev).await?;
                let t1 = self.rt.now();
                self.drain_ready().await?;
                let t2 = self.rt.now();
                self.serve_waiting().await?;
                Ok::<_, Error>((t1, t2))
            }
            .await;
            // A step that holds the actor this long delays this shard's heartbeats, writes
            // and reads: say which part did.
            if let Ok((t1, t2)) = &step {
                let t3 = self.rt.now();
                if t3 - t0 >= SLOW_STEP {
                    tracing::warn!(
                        node = %self.cfg.id,
                        shard = %self.cfg.shard,
                        event = kind,
                        handle_ms = (*t1 - t0).as_millis() as u64,
                        persist_ms = (*t2 - *t1).as_millis() as u64,
                        serve_ms = (t3 - *t2).as_millis() as u64,
                        "slow replica step"
                    );
                }
            }
            let step = step.map(|_| ());
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
                release_slot(&mut self.flush_slot, &self.cfg);
                self.flush.building = None;
                match file {
                    Err(e) => {
                        // Retried by the next drive: the freeze is still pending without a file.
                        tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "flush build failed: {e}");
                    }
                    Ok(file) => {
                        let wanted = self.engine.store().flush_wanted(id, generation);
                        if wanted {
                            self.engine.store_mut().adopt_built_file(id).await?;
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
            Event::Persisted(seq) => {
                if self.persist.running == Some(seq)
                    && let Some(rx) = self.persist.done.take()
                {
                    let (seq, up_to, r) = rx.await.unwrap_or((
                        seq,
                        None,
                        Err(Error::Internal("persistence task dropped".into())),
                    ));
                    self.finish_persistence(seq, up_to, r).await?;
                }
            }
            Event::FetchVerified {
                id,
                compaction,
                expected,
                ok,
            } => {
                self.flush.verifying.remove(&id);
                let installed = match ok {
                    Ok(true) => match self
                        .engine
                        .store_mut()
                        .install_verified(id, compaction, expected)
                        .await
                    {
                        Ok(i) => i,
                        Err(e) => {
                            tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "installing a fetched segment failed: {e}");
                            false
                        }
                    },
                    _ => false,
                };
                if installed {
                    self.flush.fetched += 1;
                    tracing::debug!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "segment fetched");
                } else if self
                    .engine
                    .store()
                    .fetched_expected(id, compaction)
                    .is_some()
                {
                    tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "fetched segment does not match its commit: building it here");
                    self.flush.fallback.insert(id);
                }
                self.drive_flushes().await?;
                self.drive_compactions().await?;
            }
            Event::ChunkWritten { id, req, ok } => {
                let Some(f) = self.flush.fetches.get_mut(&id).filter(|f| f.req == req) else {
                    return Ok(());
                };
                f.writing -= 1;
                f.progress_tick = self.ticks;
                if let Err(e) = ok {
                    self.fetch_failed(id, &format!("writing a chunk: {e}"));
                    return self.drive_flushes().await;
                }
                self.advance_seg_fetch(id).await?;
            }
            Event::IndexesReady {
                id,
                indexes,
                synced,
            } => {
                self.flush.preparing.remove(&id);
                self.engine.store_mut().renames_synced(&synced);
                match indexes {
                    Ok(p) => self.engine.adopt_indexes(id, p),
                    Err(e) => {
                        tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "preparing indexes failed ({e}): loading them at publication");
                        self.flush.prepare_failed.insert(id);
                    }
                }
                self.drive_flushes().await?;
                self.drive_compactions().await?;
            }
            Event::CompactWritten {
                id,
                inputs,
                generation,
                file,
            } => {
                release_slot(&mut self.merge_slot, &self.cfg);
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
                self.engine.store_mut().adopt_built_file(id).await?;
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
            // Handled by `run` before dispatch.
            Event::Stop(done) => done.send(()),
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
                    merges: [
                        u64::from(
                            self.flush.compacting.is_some() || self.flush.own_compaction.is_some(),
                        ),
                        st.pending_compactions().len() as u64,
                    ],
                    merges_paused: self
                        .cfg
                        .compaction_slots
                        .as_ref()
                        .is_some_and(|s| s.merges_paused()),
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
                // The snapshot is taken here, when the read is released (after ReadIndex or
                // the token's index), and the search runs on a helper thread (ADR 0025):
                // searches of one shard overlap, and never hold up its Raft work.
                let t0 = self.rt.now();
                match self.engine.prepare_legs(&q).await {
                    Ok(job) => {
                        let rt = self.rt.clone();
                        let stats = self.search_stats.clone();
                        let names = self.engine.store().key_names();
                        let prepare = (self.rt.now() - t0).as_micros() as u64;
                        self.rt.spawn(async move {
                            let t1 = rt.now();
                            let mut r = rt.offload_search(move || job.run()).await;
                            // Text ids of the keyed hits, for the coordinator (ADR 0031). A key
                            // never changes while its id is live, so reading them now is safe.
                            if let Ok(lists) = &mut r {
                                let names = names.borrow();
                                for l in lists.iter_mut() {
                                    l.keys = l
                                        .hits
                                        .iter()
                                        .filter(|(id, _)| id.is_keyed())
                                        .filter_map(|(id, _)| {
                                            names.get(id).map(|k| (*id, k.clone()))
                                        })
                                        .collect();
                                }
                            }
                            let (n, p, w) = stats.get();
                            stats.set((n + 1, p + prepare, w + (rt.now() - t1).as_micros() as u64));
                            done.send(r);
                        });
                    }
                    Err(e) => done.send(Err(e)),
                }
            }
            Event::Get(target, _, done) => {
                let id = match target {
                    DocRef::Id(id) => Some(id),
                    DocRef::Key(k) => self.engine.store().key_to_id(&k),
                };
                let r = match id {
                    Some(id) => self.engine.get(id).await,
                    None => Ok(None),
                };
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
                // Served by a task of its own: a disk read can stall for seconds while builds
                // write, and the actor must not wait on it (ADR 0027). One chunk read per
                // request: never the whole (possibly very large) file.
                let (rt, dir, shard) = (
                    self.rt.clone(),
                    self.engine.store().shard_dir().to_owned(),
                    self.cfg.shard,
                );
                let applied = self.engine.store().applied_index().get();
                self.rt.spawn(async move {
                    let read = |rel: String, len| {
                        let (rt, dir) = (rt.clone(), dir.clone());
                        async move {
                            cairn_storage::store::read_shard_file(rt, &dir, &rel, offset, len)
                                .await
                                .ok()
                                .flatten()
                        }
                    };
                    let data = read(path.clone(), CHUNK).await;
                    let missing = if path.ends_with(".del")
                        && data.is_none()
                        && read(path.replace(".del", ".seg"), 0).await.is_some()
                    {
                        // No deletion checkpoint, but the segment is here: nothing to mask.
                        NO_CHECKPOINT
                    } else {
                        u64::MAX
                    };
                    let (total, chunk) = data.unwrap_or((missing, Bytes::new()));
                    let f = Frame {
                        shard,
                        body: FrameBody::FileChunk {
                            req,
                            path,
                            offset,
                            total,
                            applied,
                            data: chunk,
                        },
                    };
                    let _ = rt.network().send(from, f.to_bytes()).await;
                });
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
    /// The log's last index, or the applied index when the log holds nothing after it.
    fn log_durable_now(&self) -> LogIndex {
        self.engine
            .store()
            .log()
            .last_index()
            .unwrap_or(self.engine.store().manifest().applied_index)
    }

    /// Starts a persistence task if there is work and none is running (ADR 0027). It syncs
    /// the log files written since the last sync, then stores the Raft state, in that order (a
    /// commit index must never be durable before the entries it covers, chaos seed 2227; it is
    /// also capped at the synced index). Its end comes back as `Event::Persisted`.
    fn start_persistence(&mut self) {
        if self.persist.running.is_some() || !self.persist.work {
            return;
        }
        let seq = self.persist.next_seq;
        self.persist.next_seq += 1;
        self.persist.work = false;
        let plan = self.engine.store_mut().log_mut().sync_plan();
        let up_to = plan.as_ref().and_then(|p| p.up_to);
        let state = if self.persist.state_dirty {
            self.persist.state_dirty = false;
            let mut st = self.state;
            let cap = up_to
                .unwrap_or(self.persist.durable)
                .max(self.persist.durable);
            st.hs.commit = st.hs.commit.min(cap);
            Some(st)
        } else {
            None
        };
        let store = ManifestStore::<R>::new(self.rt.clone(), self.state_store.path().to_owned());
        let (tx, rx) = oneshot();
        let inbox = self.inbox.clone();
        self.persist.running = Some(seq);
        self.persist.done = Some(rx);
        self.rt.spawn(async move {
            let mut r = Ok(());
            if let Some(plan) = plan {
                r = plan.run().await;
            }
            if r.is_ok()
                && let Some(st) = state
            {
                r = store.store(&st).await;
            }
            tx.send((seq, up_to, r));
            inbox.push(Event::Persisted(seq));
        });
    }

    /// Records a finished persistence task: what it made durable, the messages that waited
    /// for it, and Raft's durable index. Starts the next task if work accumulated.
    async fn finish_persistence(
        &mut self,
        seq: u64,
        up_to: Option<LogIndex>,
        r: Result<()>,
    ) -> Result<()> {
        self.persist.running = None;
        self.persist.done = None;
        r?;
        if let Some(i) = up_to {
            self.persist.durable = self.persist.durable.max(i);
            self.engine.store_mut().log_mut().mark_synced(Some(i));
        }
        while self.persist.held.front().is_some_and(|(s, _)| *s <= seq) {
            let (_, msgs) = self.persist.held.pop_front().expect("checked");
            for (to, m) in msgs {
                self.send(to, FrameBody::Raft(m)).await;
            }
        }
        let applied = self.engine.store().applied_index();
        self.raft.advance(self.persist.durable, applied);
        self.start_persistence();
        Ok(())
    }

    /// Waits for the running persistence task, and for any it chains, to finish.
    async fn settle_persistence(&mut self) -> Result<()> {
        while let Some(rx) = self.persist.done.take() {
            let seq = self.persist.running.unwrap_or_default();
            let (seq, up_to, r) = rx.await.unwrap_or((
                seq,
                None,
                Err(Error::Internal("persistence task dropped".into())),
            ));
            self.finish_persistence(seq, up_to, r).await?;
        }
        Ok(())
    }

    /// Makes everything written durable and stores the Raft state now, on the actor: for the
    /// rare writes (vote barrier, snapshot term, read floor) that must not race a persistence
    /// task storing an older state.
    async fn store_state_now(&mut self) -> Result<()> {
        self.settle_persistence().await?;
        self.engine.store_mut().log_mut().sync().await?;
        self.persist.durable = self.persist.durable.max(self.log_durable_now());
        self.state_store.store(&self.state).await?;
        self.persist.state_dirty = false;
        self.persist.work = false;
        self.release_all_held().await;
        Ok(())
    }

    /// Everything is durable: every held message may leave.
    async fn release_all_held(&mut self) {
        while let Some((_, msgs)) = self.persist.held.pop_front() {
            for (to, m) in msgs {
                self.send(to, FrameBody::Raft(m)).await;
            }
        }
    }

    async fn drain_ready(&mut self) -> Result<()> {
        loop {
            let ready: Ready = self.raft.ready();
            if ready.is_empty() {
                return Ok(());
            }
            // Snapshots, truncations and a log out of line with Raft are rare: they wait for the
            // running persistence task, then persist synchronously as before. Everything else
            // is appended here and synced by a persistence task (ADR 0027).
            let realign = ready.entries.first().is_some_and(|e| {
                let log = self.engine.store().log();
                e.index < log.first_index() || log.next_index() != e.index
            });
            if ready.snapshot.is_some() || ready.truncate_from.is_some() || realign {
                self.settle_persistence().await?;
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
                    let t = self.rt.now();
                    log.append(&entries).await?;
                    log.sync().await?;
                    let took = self.rt.now() - t;
                    if took >= SLOW_STEP / 2 {
                        tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, entries = entries.len(), ms = took.as_millis() as u64, "slow Raft log append and sync");
                    }
                }
                if let Some(hs) = ready.hard_state {
                    self.state.hs = hs;
                    let t = self.rt.now();
                    self.state_store.store(&self.state).await?;
                    let took = self.rt.now() - t;
                    if took >= SLOW_STEP / 2 {
                        tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, ms = took.as_millis() as u64, "slow hard-state store");
                    }
                }
                self.persist.state_dirty = false;
                self.persist.work = false;
                self.persist.durable = self.persist.durable.max(self.log_durable_now());
                self.release_all_held().await;
                for (to, m) in ready.messages.into_iter().chain(ready.persisted_messages) {
                    self.send(to, FrameBody::Raft(m)).await;
                }
            } else {
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
                    self.engine.store_mut().log_mut().append(&entries).await?;
                    self.persist.work = true;
                }
                if let Some(hs) = ready.hard_state {
                    self.state.hs = hs;
                    self.persist.state_dirty = true;
                    self.persist.work = true;
                }
                // A leader's appends leave before its own copy is durable (ADR 0027).
                for (to, m) in ready.messages {
                    self.send(to, FrameBody::Raft(m)).await;
                }
                // The rest waits for the task that makes this ready durable: the next one if
                // this ready wrote something, else the running one, else nothing.
                if !ready.persisted_messages.is_empty() {
                    let after = if self.persist.work {
                        Some(self.persist.next_seq)
                    } else {
                        self.persist.running
                    };
                    match after {
                        Some(seq) => self.persist.held.push_back((seq, ready.persisted_messages)),
                        None => {
                            for (to, m) in ready.persisted_messages {
                                self.send(to, FrameBody::Raft(m)).await;
                            }
                        }
                    }
                }
                self.start_persistence();
            }
            for e in &ready.committed {
                let cmd = Command::from_bytes(&e.payload)?;
                if let Command::FlushCommit { id, .. } = &cmd {
                    self.flush.announce.remove(id);
                    // The leader is making progress on this shard's builds: freezes still
                    // waiting for their commit start their wait again (it builds in order,
                    // and may be busy with a queue of them).
                    let now = self.ticks;
                    self.flush.waiting_since.values_mut().for_each(|t| *t = now);
                }
                let compact_id = match &cmd {
                    Command::CompactCommit { id, .. } => Some(*id),
                    _ => None,
                };
                let store = self.engine.store_mut();
                let mut deleted = 0;
                if e.index > store.applied_index() {
                    deleted = store.apply(e.index, &cmd).await?;
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
                        done.send(Ok(Applied {
                            token: Token {
                                shard: self.cfg.shard,
                                index: e.index,
                            },
                            deleted,
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
            let applied = self.engine.store().applied_index();
            self.raft.advance(self.persist.durable, applied);
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
        // A segment is published only once its indexes were decoded outside the actor, so
        // publishing loads nothing here (ADR 0026).
        let publishable = self.engine.store().publishable_flushes();
        let ready = self.indexes_ready(&publishable).await?;
        if self
            .engine
            .store_mut()
            .publish_ready_where(|id| ready.contains(&id))
            .await?
            > 0
        {
            self.after_publish().await?;
        }
        // Flushes a pending merge already consumes are not built or fetched: the merge is
        // installed over them once its file is here (ADR 0021).
        let skip = self.subsume_flushes().await?;
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
        self.flush.prepare_failed.retain(|id| live.contains(id));
        self.engine.retain_prepared(|id| live.contains(&id));
        self.flush.waiting_since.retain(|id, _| live.contains(id));
        // A new leader rebuilds or re-announces what is pending: the wait starts again.
        let leader_now = self.raft.leader();
        if leader_now != self.flush.waiting_leader {
            self.flush.waiting_leader = leader_now;
            let now = self.ticks;
            self.flush.waiting_since.values_mut().for_each(|t| *t = now);
        }
        // Fetches of items published or replaced meanwhile are dropped.
        self.flush.fetches.retain(|id, _| live.contains(id));
        let election = u64::from(self.cfg.election_ticks.max(1));
        let mut to_build = None;
        let mut to_fetch: Vec<(SegmentId, NodeId)> = Vec::new();
        let me = self.cfg.id;
        for p in &pending {
            if p.has_file || p.empty || skip.contains(&p.id) {
                continue;
            }
            let fetching =
                self.flush.fetches.contains_key(&p.id) || self.flush.verifying.contains(&p.id);
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
        // Not held back by a merge of this replica: a merge of 3M rows takes minutes, and
        // meanwhile the memtable reaches its write limit and the whole ingest waits for this
        // shard (every client batch spans all shards).
        if let Some(id) = to_build
            && self.flush.building.is_none()
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

    /// Of `ids` (segments whose files are in place, about to be published or installed), those
    /// whose indexes are ready. Starts decoding the others outside the actor (ADR 0026); each
    /// comes back as `IndexesReady`. A preparation that failed counts as ready: that segment
    /// loads its indexes at publication, as before.
    async fn indexes_ready(
        &mut self,
        ids: &[SegmentId],
    ) -> Result<std::collections::BTreeSet<SegmentId>> {
        let mut ready = std::collections::BTreeSet::new();
        for &id in ids {
            if self.engine.indexes_ready(id) || self.flush.prepare_failed.contains(&id) {
                ready.insert(id);
                continue;
            }
            if self.flush.preparing.contains(&id) {
                continue;
            }
            match self.engine.index_job(id).await {
                Ok(job) => {
                    self.flush.preparing.insert(id);
                    let (inbox, rt) = (self.inbox.clone(), self.rt.clone());
                    // The segment directory is synced here too, off the actor: the file was
                    // renamed into place without it, and publication waits for this task.
                    let dir = self.engine.store().segs_dir();
                    let covered = self.engine.store().unsynced_renames();
                    self.rt.spawn(async move {
                        let synced = match rt.disk().sync_dir(&dir).await {
                            Ok(()) => covered,
                            Err(_) => Vec::new(),
                        };
                        let indexes = rt.offload(move || job.run()).await;
                        inbox.push(Event::IndexesReady {
                            id,
                            indexes,
                            synced,
                        });
                    });
                }
                Err(e) => {
                    tracing::warn!(node = %self.cfg.id, shard = %self.cfg.shard, %id, "preparing indexes failed ({e}): loading them at publication");
                    self.flush.prepare_failed.insert(id);
                    ready.insert(id);
                }
            }
        }
        Ok(ready)
    }

    /// Installs merges that consume the oldest pending flushes as soon as their files are here,
    /// and returns the flushes to leave alone meanwhile: those consumed by a merge still being
    /// fetched. A merge whose fetch failed, or our own whose file was lost, gives way to the
    /// normal path (build or fetch the flushes, then the merge).
    async fn subsume_flushes(&mut self) -> Result<std::collections::BTreeSet<SegmentId>> {
        loop {
            let candidates = self.engine.store().subsumption_candidates();
            if candidates.is_empty() {
                return Ok(Default::default());
            }
            let files: std::collections::BTreeSet<SegmentId> = self
                .engine
                .store()
                .pending_compactions()
                .into_iter()
                .filter(|(_, has_file, _)| *has_file)
                .map(|(c, _, _)| c.id)
                .collect();
            if let Some((j, id, consumed)) = candidates.iter().find(|(_, id, _)| files.contains(id))
            {
                if !self.indexes_ready(&[*id]).await?.contains(id) {
                    // Installed once its indexes are decoded; its flushes stay untouched.
                    return Ok(consumed.iter().copied().collect());
                }
                tracing::info!(node = %self.cfg.id, shard = %self.cfg.shard, %id, flushes = consumed.len(), "merge installed over flushes not built here");
                self.flush.skipped += consumed.len() as u64;
                self.engine.store_mut().install_subsumed(*j).await?;
                // Fetches of the consumed flushes are moot: drop them (late chunks are ignored).
                for f in consumed {
                    if self.flush.fetches.remove(f).is_some() {
                        self.engine
                            .store()
                            .reset_fetch_staging(&Store::<R>::ship_staging(*f))
                            .await?;
                    }
                }
                self.after_publish().await?;
                self.after_compaction_install().await?;
                continue;
            }
            let me = self.cfg.id;
            let origin: std::collections::BTreeMap<SegmentId, NodeId> = self
                .engine
                .store()
                .pending_compactions()
                .into_iter()
                .map(|(c, _, _)| (c.id, c.from))
                .collect();
            let viable = candidates.iter().find(|(_, id, _)| {
                self.cfg.ship_segments
                    && !self.flush.fallback.contains(id)
                    && origin.get(id).is_some_and(|f| *f != me)
            });
            return Ok(viable
                .map(|(_, _, consumed)| consumed.iter().copied().collect())
                .unwrap_or_default());
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
                writing: 0,
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
            self.flush_slot = true;
        }
        let Some(job) = self.engine.store().flush_job(id) else {
            release_slot(&mut self.flush_slot, &self.cfg);
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
    fn propose(&mut self, cmd: Command, done: Sender<Result<Applied>>) {
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
            // Stored by the next persistence task, not here: a restart takes the term from the
            // manifest just stored (`applied_term`, written with its index since ADR 0020),
            // and this copy only serves manifests older than that. Storing it on the actor
            // waited for the log sync after every publication (332 ms on average, ADR 0027).
            self.state.snapshot_term = term;
            self.persist.state_dirty = true;
            self.persist.work = true;
            self.start_persistence();
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
        // Every 200 ticks: 10 s at the server's 50 ms tick.
        if self.ticks % 200 == 0 {
            let (n, p, w) = self.search_stats.take();
            if n > 0 {
                tracing::info!(target: "cairn_query::stats", node = %self.cfg.id, shard = %self.cfg.shard, searches = n, prepare_us = p / n, search_us = w / n, segments = self.engine.store().segments().count(), "search timing");
            }
        }
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
            .filter(|(_, f)| f.writing == 0 && ticks - f.progress_tick >= stall)
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

    /// Leader only (ADR 0021): picks a compaction per policy and builds it; the result is
    /// proposed as a `CompactCommit` and installed everywhere when applied. One compaction at
    /// a time: none while another is committed but not installed, or proposed but not
    /// committed.
    async fn maybe_compact_job(&mut self) -> Result<()> {
        if self
            .cfg
            .compaction_slots
            .as_ref()
            .is_some_and(|s| s.merges_paused())
        {
            return Ok(());
        }
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
            if !slots.try_acquire_merge() {
                return Ok(());
            }
            self.merge_slot = true;
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
                let ready = self.indexes_ready(&[c.id]).await?;
                if !ready.contains(&c.id) {
                    // Installed when its indexes are decoded (`IndexesReady`).
                    return Ok(());
                }
                let n = self
                    .engine
                    .store_mut()
                    .install_compactions_where(|id| ready.contains(&id))
                    .await?;
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
                if !p_has_file
                    && !local
                    && !self.flush.fetches.contains_key(&p.id)
                    && !self.flush.verifying.contains(&p.id)
                {
                    self.start_seg_fetch(p.id, true, p.from).await?;
                }
            }
            if !inputs_ready || has_file {
                // Earlier flushes still to publish here.
                return Ok(());
            }
            let fetching =
                self.flush.fetches.contains_key(&c.id) || self.flush.verifying.contains(&c.id);
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
        // Written by a task of its own, like serving (a write can stall behind builds).
        let (rt, inbox) = (self.rt.clone(), self.inbox.clone());
        let staged = self.engine.store().staged_fetch_path(id);
        self.rt.spawn(async move {
            let ok = cairn_storage::store::write_staged_chunk(rt, staged, offset, data).await;
            inbox.push(Event::ChunkWritten { id, req, ok });
        });
        let ticks = self.ticks;
        if let Some(f) = self.flush.fetches.get_mut(&id) {
            f.inflight.remove(&offset);
            f.got += n;
            f.writing += 1;
            f.total = Some(total);
            f.progress_tick = ticks;
        }
        self.advance_seg_fetch(id).await
    }

    /// Keeps up to `FETCH_WINDOW` chunks of fetch `id` requested or being written, and once
    /// every chunk is written, checks the file outside the actor.
    async fn advance_seg_fetch(&mut self, id: SegmentId) -> Result<()> {
        let mut requests = Vec::new();
        let (from, req, compaction, done) = {
            let Some(f) = self.flush.fetches.get_mut(&id) else {
                return Ok(());
            };
            let total = f.total.unwrap_or(0);
            while f.inflight.len() + f.writing < FETCH_WINDOW && f.next < total {
                requests.push(f.next);
                f.inflight.insert(f.next);
                f.next += CHUNK as u64;
            }
            let done =
                f.total.is_some() && f.got >= total && f.inflight.is_empty() && f.writing == 0;
            (f.from, f.req, f.compaction, done)
        };
        for offset in requests {
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
        if !done {
            return Ok(());
        }
        self.flush.fetches.remove(&id);
        let Some(expected) = self.engine.store().fetched_expected(id, compaction) else {
            // Published or merged away meanwhile.
            return self.drive_flushes().await;
        };
        // Syncing the file and checking its hash read hundreds of megabytes: outside the
        // actor, which only renames the file into place once `FetchVerified` says it matches.
        self.flush.verifying.insert(id);
        let (rt, inbox) = (self.rt.clone(), self.inbox.clone());
        let path = self.engine.store().staged_fetch_path(id);
        self.rt.spawn(async move {
            let ok = cairn_storage::store::verify_staged_file(rt, path, expected).await;
            inbox.push(Event::FetchVerified {
                id,
                compaction,
                expected,
                ok,
            });
        });
        Ok(())
    }

    /// Drops the accepted snapshot and the log after it, and reopens from disk so the leader
    /// ships a fresh one. The entries after it were acknowledged: until the log is back to
    /// them, this node must not vote for a candidate without them (Raft's vote barrier).
    async fn drop_stale_snapshot(&mut self, why: String) -> Result<()> {
        if self.raft.last_index() > self.state.vote_barrier {
            self.state.vote_barrier = self.raft.last_index();
            self.state.vote_barrier_term = self.raft.last_log_term();
        }
        self.store_state_now().await?;
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
        self.store_state_now().await?;
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
        self.store_state_now().await?;
        // Offer the installed state to lagging followers should this replica become leader:
        // without it, a leader whose log starts after a follower's next index sent that
        // follower nothing at all, not even heartbeats (chaos seed 1431, found once the
        // campaign flushed).
        self.raft.compact(Snapshot {
            last_index: applied,
            last_term: self.raft.snapshot_term(),
            data: self.engine.store().manifest_bytes(),
        });
        self.persist.durable = self.persist.durable.max(applied);
        self.raft.advance(applied, applied);
        // Anyone waiting on an index at or below the snapshot can proceed.
        self.serve_waiting().await
    }
}

fn seg_rel(id: SegmentId) -> String {
    format!("segs/{:016x}.seg", id.get())
}

#[cfg(test)]
mod slot_tests {
    use super::JobSlots;

    #[test]
    fn merges_leave_a_slot_for_flushes() {
        let s = JobSlots::new(2);
        assert!(s.try_acquire_merge());
        assert!(
            !s.try_acquire_merge(),
            "a second merge would take the last slot"
        );
        assert!(s.try_acquire(), "a flush still finds one");
        assert!(!s.try_acquire());
        s.release();
        s.release();
        // A flush holds one: the other is the last free slot, so no merge starts.
        assert!(s.try_acquire());
        assert!(!s.try_acquire_merge());
        s.release();
        // One slot: no reservation is possible, merges still run.
        let one = JobSlots::new(1);
        assert!(one.try_acquire_merge());
        assert!(!one.try_acquire());
        let four = JobSlots::new(4);
        assert!((0..3).all(|_| four.try_acquire_merge()));
        assert!(!four.try_acquire_merge());
        assert!(four.try_acquire());
    }
}
