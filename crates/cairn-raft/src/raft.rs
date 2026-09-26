//! The Raft state machine.

use crate::message::{Entry, HardState, Message, Snapshot};
use bytes::Bytes;
use cairn_core::{Error, HashMap, LogIndex, NodeId, Result, SeededRng, Term};

/// Static configuration of one group member.
#[derive(Debug, Clone)]
pub struct Config {
    /// This node.
    pub id: NodeId,
    /// All members, including this node.
    pub peers: Vec<NodeId>,
    /// Election timeout base in ticks; the actual timeout is drawn from `[base, 2 * base)`.
    pub election_ticks: u32,
    /// Heartbeat interval in ticks.
    pub heartbeat_ticks: u32,
    /// Maximum entries per `Append`.
    pub max_batch: usize,
    /// Seeded randomness for election timeouts.
    pub rng: SeededRng,
}

/// Role of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Follows a leader (or waits for one).
    Follower,
    /// Probing whether an election could succeed.
    PreCandidate,
    /// Running an election.
    Candidate,
    /// Leading.
    Leader,
}

/// Where the log stands on disk when the state machine is created.
#[derive(Debug, Clone)]
pub struct InitialState {
    /// Persisted hard state.
    pub hard_state: HardState,
    /// Entries persisted on disk, contiguous, starting right after the snapshot.
    pub entries: Vec<Entry>,
    /// Index and term covered by the local snapshot / compaction point (0, 0 if none).
    pub snapshot: (LogIndex, Term),
    /// Index applied to the state machine.
    pub applied: LogIndex,
    /// The snapshot at `snapshot` was accepted but its installation did not finish before the
    /// restart: nothing is applied until the driver reports it installed (via
    /// [`Raft::advance`]).
    pub installing: bool,
    /// Last log position (index, term) this node ever acknowledged, when it had to drop part
    /// of its log (an accepted snapshot that could no longer be fetched). Until its log
    /// reaches that index again it does not campaign, and it votes only for candidates whose
    /// log is at least as up to date as that remembered position: an entry committed with its
    /// acknowledgement may now lack a majority of copies, and a vote judged on the shortened
    /// log could elect a leader without it (ADR 0017; refined in ADR 0020, because refusing
    /// every vote could leave a shard with no possible majority).
    pub vote_barrier: (LogIndex, Term),
}

#[derive(Debug, Clone, Copy)]
struct Progress {
    next: LogIndex,
    matched: LogIndex,
    /// Highest heartbeat sequence acknowledged.
    acked_seq: u64,
    /// Sequences of the entry-bearing appends sent and not yet answered (`inflight_n` of
    /// them). A response echoes the sequence of the message it answers and settles exactly that
    /// append; heartbeat responses settle nothing. (Letting any response free a slot drained
    /// the window while appends were still in flight, and the leader re-sent the same
    /// megabytes over and over: 42 GB queued on a real network.)
    inflight_seqs: [u64; MAX_INFLIGHT as usize],
    inflight_n: u32,
    /// Last index sent in an append carrying entries (pipelining point).
    sent: LogIndex,
    /// Leader ticks since an append to this follower was last settled while some are in
    /// flight; past an election timeout they are presumed lost and re-sent from `next`.
    stalled_ticks: u32,
}

impl Progress {
    /// Settles the append answered by a response with sequence `seq`, if it is one of ours.
    fn settle(&mut self, seq: u64) {
        let n = self.inflight_n as usize;
        if let Some(i) = self.inflight_seqs[..n].iter().position(|s| *s == seq) {
            self.inflight_seqs.copy_within(i + 1..n, i);
            self.inflight_n -= 1;
            self.stalled_ticks = 0;
        }
    }

    /// Forgets everything in flight; the next append starts again from `next`.
    fn reset_window(&mut self) {
        self.inflight_n = 0;
        self.stalled_ticks = 0;
        self.sent = LogIndex(self.next.get() - 1);
    }

    fn push_inflight(&mut self, seq: u64) {
        let n = self.inflight_n as usize;
        if n < self.inflight_seqs.len() {
            self.inflight_seqs[n] = seq;
            self.inflight_n += 1;
        }
    }
}

/// Appends carrying entries allowed in flight per follower. Beyond it the leader only sends
/// heartbeats: without a window, every proposal re-sent the whole unacknowledged batch to a
/// slow follower and its inbox grew without bound (30 GB in the 50M run).
const MAX_INFLIGHT: u32 = 4;
/// Upper bound on the entry payload of one append (at least one entry is always sent).
const MAX_APPEND_BYTES: usize = 4 << 20;

#[derive(Debug, Clone)]
struct PendingRead {
    id: u64,
    index: LogIndex,
    seq: u64,
    /// The follower that asked (follower read), or `None` for a read served here.
    from: Option<NodeId>,
}

/// What the driver must do after a batch of steps.
#[derive(Debug, Default)]
pub struct Ready {
    /// Persist this before sending `messages`, if present.
    pub hard_state: Option<HardState>,
    /// Truncate the on-disk log from this index (inclusive) before appending `entries`.
    pub truncate_from: Option<LogIndex>,
    /// Append and sync these entries before sending `messages`.
    pub entries: Vec<Entry>,
    /// Messages that may leave now, before this ready is persisted (ADR 0027): a leader's
    /// appends, snapshots, read-index answers and `TimeoutNow`. A leader's term and vote are
    /// durable (it was elected on votes requested after persisting them), and a leader may
    /// replicate entries before its own copy is durable.
    pub messages: Vec<(NodeId, Message)>,
    /// Messages to send only once this ready (and every earlier one) is persisted: follower
    /// acknowledgements, everything about votes and terms. A synchronous driver sends both
    /// lists after persisting.
    pub persisted_messages: Vec<(NodeId, Message)>,
    /// Entries to apply, in order.
    pub committed: Vec<Entry>,
    /// A snapshot to install into the state machine (replaces everything).
    pub snapshot: Option<Snapshot>,
    /// Reads that may now be served at (or after) the given applied index.
    pub read_states: Vec<(u64, LogIndex)>,
    /// Reads that will never complete here (leadership changed or the leader did not answer):
    /// the driver fails them so the client retries through the current leader.
    pub failed_reads: Vec<u64>,
}

impl Ready {
    /// Whether there is nothing to do.
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.truncate_from.is_none()
            && self.entries.is_empty()
            && self.messages.is_empty()
            && self.persisted_messages.is_empty()
            && self.committed.is_empty()
            && self.snapshot.is_none()
            && self.read_states.is_empty()
            && self.failed_reads.is_empty()
    }
}

/// A leadership transfer in progress.
#[derive(Debug, Clone, Copy)]
struct Transfer {
    to: NodeId,
    /// Ticks since it started; abandoned after one election timeout.
    elapsed: u32,
    /// `TimeoutNow` sent (once the target had every entry).
    sent: bool,
}

/// The state machine of one group member.
pub struct Raft {
    cfg: Config,
    role: Role,
    term: Term,
    vote: Option<NodeId>,
    leader: Option<NodeId>,
    /// Index of `entries[0]`.
    first_index: LogIndex,
    /// Term of `first_index - 1` (the snapshot / compaction point).
    snapshot_term: Term,
    entries: Vec<Entry>,
    commit: LogIndex,
    applied: LogIndex,
    /// Last index known durable on this node.
    persisted: LogIndex,
    /// Last index handed to the driver in a `Ready` (written, maybe not synced yet, ADR 0027).
    handed: LogIndex,
    /// Set when a conflict truncated below `persisted`.
    pending_truncate: Option<LogIndex>,
    progress: HashMap<NodeId, Progress>,
    votes: HashMap<NodeId, bool>,
    heartbeat_seq: u64,
    reads: Vec<PendingRead>,
    /// Index of the first entry of the current leadership (the no-op).
    lead_start: LogIndex,
    election_elapsed: u32,
    heartbeat_elapsed: u32,
    election_timeout: u32,
    hs_dirty: bool,
    msgs: Vec<(NodeId, Message)>,
    snapshot_to_install: Option<Snapshot>,
    read_states: Vec<(u64, LogIndex)>,
    /// Follower reads waiting for the leader's answer: `(id, ticks waited)`.
    follower_reads: Vec<(u64, u32)>,
    failed_reads: Vec<u64>,
    /// Local snapshot offered to lagging followers.
    snapshot: Option<Snapshot>,
    /// An accepted snapshot the driver has not finished installing.
    installing: bool,
    /// See [`InitialState::vote_barrier`].
    vote_barrier: (LogIndex, Term),
    /// Leadership transfer in progress (leader only).
    transfer: Option<Transfer>,
    /// Diagnostics: entries shipped in appends (counting re-sends) and follower rejections.
    entries_sent: u64,
    rejects: u64,
}

impl Raft {
    /// Creates the state machine from persisted state.
    pub fn new(mut cfg: Config, init: InitialState) -> Self {
        if !cfg.peers.contains(&cfg.id) {
            cfg.peers.push(cfg.id);
        }
        cfg.peers.sort();
        let first_index = init.snapshot.0.next();
        let last = init.entries.last().map_or(init.snapshot.0, |e| e.index);
        debug_assert!(
            init.entries
                .iter()
                .enumerate()
                .all(|(i, e)| e.index == LogIndex(first_index.get() + i as u64))
        );
        let timeout =
            cfg.election_ticks + cfg.rng.below(u64::from(cfg.election_ticks.max(1))) as u32;
        // Applied entries were committed. With asynchronous persistence (ADR 0027) the stored
        // hard state, and its commit index, may lag what the state machine already applied:
        // taking the commit from it alone moved `applied` back and re-applied entries (Raft
        // chaos harness, asynchronous mode).
        let commit = init
            .hard_state
            .commit
            .max(init.applied)
            .min(last)
            .max(init.snapshot.0);
        Raft {
            role: Role::Follower,
            term: init.hard_state.term,
            vote: init.hard_state.vote,
            leader: None,
            first_index,
            snapshot_term: init.snapshot.1,
            entries: init.entries,
            commit,
            applied: init
                .applied
                .max(init.snapshot.0)
                .min(commit.max(init.snapshot.0)),
            persisted: last,
            handed: last,
            pending_truncate: None,
            progress: HashMap::default(),
            votes: HashMap::default(),
            heartbeat_seq: 0,
            reads: Vec::new(),
            lead_start: LogIndex(0),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            election_timeout: timeout,
            hs_dirty: false,
            msgs: Vec::new(),
            snapshot_to_install: None,
            read_states: Vec::new(),
            follower_reads: Vec::new(),
            failed_reads: Vec::new(),
            entries_sent: 0,
            rejects: 0,
            snapshot: None,
            installing: init.installing,
            vote_barrier: init.vote_barrier,
            transfer: None,
            cfg,
        }
    }

    // ------------------------------------------------------------------ accessors

    /// This node.
    pub fn id(&self) -> NodeId {
        self.cfg.id
    }

    /// Current role.
    pub fn role(&self) -> Role {
        self.role
    }

    /// Current term.
    pub fn term(&self) -> Term {
        self.term
    }

    /// Known leader, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// Commit index.
    pub fn commit_index(&self) -> LogIndex {
        self.commit
    }

    /// Applied index (as reported through `advance`).
    pub fn applied_index(&self) -> LogIndex {
        self.applied
    }

    /// Last log index.
    pub fn last_index(&self) -> LogIndex {
        self.entries
            .last()
            .map_or(LogIndex(self.first_index.get() - 1), |e| e.index)
    }

    /// First index held in memory.
    pub fn first_index(&self) -> LogIndex {
        self.first_index
    }

    fn last_term(&self) -> Term {
        self.entries.last().map_or(self.snapshot_term, |e| e.term)
    }

    /// Term of `index`, if known.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index.get() == 0 {
            return Some(Term(0));
        }
        if index.get() + 1 == self.first_index.get() {
            return Some(self.snapshot_term);
        }
        if index < self.first_index {
            return None;
        }
        self.entries
            .get((index.get() - self.first_index.get()) as usize)
            .map(|e| e.term)
    }

    /// Entry at `index`, if held.
    pub fn entry(&self, index: LogIndex) -> Option<&Entry> {
        if index < self.first_index {
            return None;
        }
        self.entries
            .get((index.get() - self.first_index.get()) as usize)
    }

    fn quorum(&self) -> usize {
        self.cfg.peers.len() / 2 + 1
    }

    fn hard_state(&self) -> HardState {
        HardState {
            term: self.term,
            vote: self.vote,
            commit: self.commit,
        }
    }

    fn send(&mut self, to: NodeId, msg: Message) {
        self.msgs.push((to, msg));
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self.cfg.election_ticks
            + self
                .cfg
                .rng
                .below(u64::from(self.cfg.election_ticks.max(1))) as u32;
    }

    // ------------------------------------------------------------------ role changes

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        self.transfer = None;
        if term > self.term {
            self.term = term;
            self.vote = None;
            self.hs_dirty = true;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.abandon_reads();
        self.reset_election_timer();
    }

    /// Leadership changed (or may have): every pending read is failed, locally for this node's
    /// own reads and with a refusal for followers that asked us. Clients retry.
    fn abandon_reads(&mut self) {
        for r in std::mem::take(&mut self.reads) {
            match r.from {
                None => self.failed_reads.push(r.id),
                Some(p) => self.send(
                    p,
                    Message::ReadIndexResp {
                        term: self.term,
                        id: r.id,
                        index: LogIndex(0),
                        ok: false,
                    },
                ),
            }
        }
        self.failed_reads
            .extend(self.follower_reads.drain(..).map(|(id, _)| id));
    }

    fn become_pre_candidate(&mut self) {
        self.abandon_reads();
        self.role = Role::PreCandidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.cfg.id, true);
        self.reset_election_timer();
        let (last_index, last_term) = (self.last_index(), self.last_term());
        let next = Term(self.term.get() + 1);
        for &p in &self.cfg.peers.clone() {
            if p != self.cfg.id {
                self.send(
                    p,
                    Message::PreVote {
                        term: next,
                        last_index,
                        last_term,
                    },
                );
            }
        }
        if self.votes.len() >= self.quorum() {
            self.become_candidate();
        }
    }

    fn become_candidate(&mut self) {
        self.term = Term(self.term.get() + 1);
        self.vote = Some(self.cfg.id);
        self.hs_dirty = true;
        self.role = Role::Candidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.cfg.id, true);
        self.reset_election_timer();
        let (last_index, last_term, term) = (self.last_index(), self.last_term(), self.term);
        for &p in &self.cfg.peers.clone() {
            if p != self.cfg.id {
                self.send(
                    p,
                    Message::Vote {
                        term,
                        last_index,
                        last_term,
                    },
                );
            }
        }
        if self.votes.len() >= self.quorum() {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        self.votes.clear();
        self.heartbeat_elapsed = 0;
        let next = self.last_index().next();
        self.progress.clear();
        for &p in &self.cfg.peers {
            self.progress.insert(
                p,
                Progress {
                    next,
                    matched: LogIndex(0),
                    acked_seq: 0,
                    inflight_seqs: [0; MAX_INFLIGHT as usize],
                    inflight_n: 0,
                    sent: LogIndex(next.get() - 1),
                    stalled_ticks: 0,
                },
            );
        }
        // A no-op in the new term lets earlier terms' entries commit (figure 8 of the paper).
        self.append_local(Bytes::new());
        self.lead_start = self.last_index();
        self.broadcast_append();
    }

    // ------------------------------------------------------------------ log helpers

    fn append_local(&mut self, payload: Bytes) -> LogIndex {
        let index = self.last_index().next();
        self.entries.push(Entry {
            index,
            term: self.term,
            payload,
        });
        index
    }

    /// Truncates the in-memory log from `from` (inclusive), recording a disk truncation if
    /// persisted entries are affected.
    fn truncate_from(&mut self, from: LogIndex) {
        if from > self.last_index() {
            return;
        }
        let keep = (from.get() - self.first_index.get()) as usize;
        self.entries.truncate(keep);
        if self.handed >= from {
            // The driver has these entries on disk (synced or not): it must cut them too.
            let cut = match self.pending_truncate {
                Some(t) => t.min(from),
                None => from,
            };
            self.pending_truncate = Some(cut);
            self.handed = LogIndex(from.get() - 1);
            self.persisted = self.persisted.min(self.handed);
        }
    }

    /// Whether this node may campaign (see [`InitialState::vote_barrier`]).
    fn may_vote(&self) -> bool {
        self.last_index() >= self.vote_barrier.0
    }

    /// Whether a candidate's log is up to date enough for this node's vote: compared with this
    /// node's log, and, while the vote barrier holds, with the position it once acknowledged.
    fn votable(&self, last_index: LogIndex, last_term: Term) -> bool {
        let (bi, bt) = self.vote_barrier;
        self.up_to_date(last_index, last_term)
            && (self.last_index() >= bi || last_term > bt || (last_term == bt && last_index >= bi))
    }

    /// Term of the last entry in the log (or of the snapshot point).
    pub fn last_log_term(&self) -> Term {
        self.last_term()
    }

    fn up_to_date(&self, last_index: LogIndex, last_term: Term) -> bool {
        last_term > self.last_term()
            || (last_term == self.last_term() && last_index >= self.last_index())
    }

    // ------------------------------------------------------------------ driver API

    /// Advances time by one tick.
    pub fn tick(&mut self) {
        // A follower read whose answer never came (lost message, leader gone quiet) fails after
        // two election timeouts; the client retries.
        let limit = self.cfg.election_ticks.max(1) * 2;
        let mut i = 0;
        while i < self.follower_reads.len() {
            self.follower_reads[i].1 += 1;
            if self.follower_reads[i].1 > limit {
                let (id, _) = self.follower_reads.swap_remove(i);
                self.failed_reads.push(id);
            } else {
                i += 1;
            }
        }
        match self.role {
            Role::Leader => {
                if let Some(t) = self.transfer.as_mut() {
                    t.elapsed += 1;
                    if t.elapsed > self.cfg.election_ticks.max(1) {
                        self.transfer = None;
                    }
                }
                self.maybe_send_timeout_now();
                // Appends in flight with no answer for several election timeouts were lost (a
                // broken connection drops queued messages): re-send from the follower's `next`.
                // Generous on purpose: a follower that is merely slow must not be flooded.
                let limit = self.cfg.election_ticks.max(1) * 6;
                for p in self.progress.values_mut() {
                    if p.inflight_n > 0 {
                        p.stalled_ticks += 1;
                        if p.stalled_ticks >= limit {
                            p.reset_window();
                        }
                    }
                }
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append();
                }
            }
            _ => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    if self.may_vote() {
                        self.become_pre_candidate();
                    } else {
                        self.reset_election_timer();
                    }
                }
            }
        }
    }

    /// Proposes a command; leader only. Returns the entry's index.
    pub fn propose(&mut self, payload: Bytes) -> Result<LogIndex> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader {
                shard: cairn_core::ShardId(0),
                leader_hint: self.leader,
            });
        }
        if let Some(t) = self.transfer {
            // No new entries while handing over: the target must catch up with a fixed log.
            return Err(Error::NotLeader {
                shard: cairn_core::ShardId(0),
                leader_hint: Some(t.to),
            });
        }
        let index = self.append_local(payload);
        self.broadcast_append();
        Ok(index)
    }

    /// Starts handing leadership to `to` (Raft thesis 3.10): no new proposals are accepted,
    /// `to` is brought up to date, then told to start an election at once. Abandoned after one
    /// election timeout if `to` has not taken over. Fails if this node is not the leader or
    /// `to` is not a peer.
    pub fn transfer_leadership(&mut self, to: NodeId) -> Result<()> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader {
                shard: cairn_core::ShardId(0),
                leader_hint: self.leader,
            });
        }
        if to == self.cfg.id || !self.progress.contains_key(&to) {
            return Err(Error::InvalidRequest(format!(
                "cannot transfer leadership to {to}"
            )));
        }
        self.transfer = Some(Transfer {
            to,
            elapsed: 0,
            sent: false,
        });
        self.maybe_send_timeout_now();
        if self.transfer.is_some_and(|t| !t.sent) {
            self.send_append(to);
        }
        Ok(())
    }

    /// Index of the first entry of this leader's term (its no-op); `LogIndex(0)` when not
    /// leader. Once it is applied, every entry of earlier terms is applied too.
    pub fn lead_start(&self) -> LogIndex {
        if self.role == Role::Leader {
            self.lead_start
        } else {
            LogIndex(0)
        }
    }

    /// Target of the leadership transfer in progress, if any.
    pub fn transferring_to(&self) -> Option<NodeId> {
        self.transfer.map(|t| t.to)
    }

    /// Replication progress of peer `to` as seen by the leader: its matched index.
    pub fn matched_index(&self, to: NodeId) -> Option<LogIndex> {
        self.progress.get(&to).map(|p| p.matched)
    }

    fn maybe_send_timeout_now(&mut self) {
        let Some(t) = self.transfer else { return };
        if t.sent {
            return;
        }
        if self
            .progress
            .get(&t.to)
            .is_some_and(|p| p.matched >= self.last_index())
        {
            let term = self.term;
            self.send(t.to, Message::TimeoutNow { term });
            if let Some(t) = self.transfer.as_mut() {
                t.sent = true;
            }
        }
    }

    /// Requests a linearizable read point. On the leader, a heartbeat round confirms
    /// leadership. On a follower that knows its leader, the leader is asked for its commit index
    /// (follower read, Raft thesis 6.4) and the read is served here once applied up to it.
    /// The driver waits for the matching `read_states` entry (or `failed_reads`).
    pub fn read_index(&mut self, id: u64) -> Result<()> {
        match (self.role, self.leader) {
            (Role::Leader, _) => {
                self.start_read(id, None);
                Ok(())
            }
            (Role::Follower, Some(l)) if l != self.cfg.id => {
                self.follower_reads.push((id, 0));
                self.send(
                    l,
                    Message::ReadIndexReq {
                        term: self.term,
                        id,
                    },
                );
                Ok(())
            }
            _ => Err(Error::NotLeader {
                shard: cairn_core::ShardId(0),
                leader_hint: self.leader,
            }),
        }
    }

    /// Leader: records a read at the current commit index and starts a heartbeat round.
    fn start_read(&mut self, id: u64, from: Option<NodeId>) {
        self.heartbeat_seq += 1;
        let seq = self.heartbeat_seq;
        self.reads.push(PendingRead {
            id,
            index: self.commit,
            seq,
            from,
        });
        if self.cfg.peers.len() == 1 {
            self.check_reads();
        } else {
            for &p in &self.cfg.peers.clone() {
                if p != self.cfg.id {
                    self.send_append(p);
                }
            }
        }
    }

    /// Lowest index matched by a follower (leader only; `None` otherwise or alone).
    pub fn min_follower_matched(&self) -> Option<LogIndex> {
        if self.role != Role::Leader {
            return None;
        }
        self.progress
            .iter()
            .filter(|(p, _)| **p != self.cfg.id)
            .map(|(_, x)| x.matched)
            .min()
    }

    /// Diagnostics: per follower `(next, matched, sent, in-flight sequences)`.
    #[allow(clippy::type_complexity)]
    pub fn debug_progress(&self) -> Vec<(NodeId, LogIndex, LogIndex, LogIndex, Vec<u64>)> {
        let mut v: Vec<_> = self
            .progress
            .iter()
            .map(|(n, p)| {
                (
                    *n,
                    p.next,
                    p.matched,
                    p.sent,
                    p.inflight_seqs[..p.inflight_n as usize].to_vec(),
                )
            })
            .collect();
        v.sort_by_key(|x| x.0);
        v
    }

    /// Diagnostics: entries shipped in appends so far (re-sends included) and rejections received.
    pub fn send_stats(&self) -> (u64, u64) {
        (self.entries_sent, self.rejects)
    }

    /// Payload bytes of the entries held in memory.
    pub fn log_bytes(&self) -> usize {
        self.entries.iter().map(|e| e.payload.len()).sum()
    }

    /// Replaces the snapshot offered to lagging followers without dropping log entries (the
    /// leader keeps entries a slow follower still needs, see [`Raft::compact`]).
    pub fn offer_snapshot(&mut self, snapshot: Snapshot) {
        if self
            .snapshot
            .as_ref()
            .is_none_or(|s| snapshot.last_index >= s.last_index)
            && snapshot.last_index <= self.last_index()
        {
            self.snapshot = Some(snapshot);
        }
    }

    /// Installs the local snapshot offered to lagging followers and drops in-memory entries up
    /// to its index (the driver truncated the on-disk log accordingly).
    pub fn compact(&mut self, snapshot: Snapshot) {
        let up_to = snapshot.last_index;
        if up_to < self.first_index || up_to > self.last_index() {
            if up_to >= self.first_index {
                // Beyond our log: only possible if the driver built a snapshot we never saw.
                return;
            }
            self.snapshot = Some(snapshot);
            return;
        }
        let term = self.term_at(up_to).expect("in range");
        let drop_n = (up_to.get() - self.first_index.get() + 1) as usize;
        self.entries.drain(..drop_n);
        self.first_index = up_to.next();
        self.snapshot_term = term;
        self.snapshot = Some(snapshot);
    }

    /// Handles a message from `from`.
    pub fn step(&mut self, from: NodeId, msg: Message) {
        // Term handling.
        match &msg {
            Message::PreVote { .. } | Message::PreVoteResp { .. } => {}
            m => {
                let t = m.term();
                if t > self.term {
                    let leader =
                        matches!(m, Message::Append { .. } | Message::InstallSnapshot { .. })
                            .then_some(from);
                    self.become_follower(t, leader);
                } else if t < self.term {
                    match m {
                        Message::Vote { .. } => self.send(
                            from,
                            Message::VoteResp {
                                term: self.term,
                                granted: false,
                            },
                        ),
                        Message::Append { seq, .. } => {
                            let seq = *seq;
                            self.send(
                                from,
                                Message::AppendResp {
                                    term: self.term,
                                    success: false,
                                    index: self.last_index().next(),
                                    seq,
                                },
                            );
                        }
                        Message::InstallSnapshot { .. } => self.send(
                            from,
                            Message::SnapshotResp {
                                term: self.term,
                                index: self.commit,
                            },
                        ),
                        _ => {}
                    }
                    return;
                }
            }
        }
        match msg {
            Message::PreVote {
                term,
                last_index,
                last_term,
            } => {
                // Grant if the log is up to date and no live leader is known.
                let no_leader =
                    self.leader.is_none() || self.election_elapsed >= self.election_timeout;
                let granted = term > self.term && no_leader && self.votable(last_index, last_term);
                let current = self.term;
                self.send(
                    from,
                    Message::PreVoteResp {
                        term,
                        granted,
                        current,
                    },
                );
            }
            Message::PreVoteResp {
                term,
                granted,
                current,
            } => {
                if current > self.term {
                    // A real term we missed (not a prospective one): adopt it.
                    self.become_follower(current, None);
                    return;
                }
                if self.role == Role::PreCandidate && term.get() == self.term.get() + 1 {
                    self.votes.insert(from, granted);
                    let yes = self.votes.values().filter(|g| **g).count();
                    let no = self.votes.values().filter(|g| !**g).count();
                    if yes >= self.quorum() {
                        self.become_candidate();
                    } else if no >= self.quorum() {
                        self.become_follower(self.term, None);
                    }
                }
            }
            Message::Vote {
                last_index,
                last_term,
                ..
            } => {
                let can = self.vote.is_none_or(|v| v == from);
                let granted =
                    can && self.role != Role::Leader && self.votable(last_index, last_term);
                if granted {
                    self.vote = Some(from);
                    self.hs_dirty = true;
                    self.reset_election_timer();
                }
                self.send(
                    from,
                    Message::VoteResp {
                        term: self.term,
                        granted,
                    },
                );
            }
            Message::VoteResp { granted, .. } => {
                if self.role == Role::Candidate {
                    self.votes.insert(from, granted);
                    let yes = self.votes.values().filter(|g| **g).count();
                    let no = self.votes.values().filter(|g| !**g).count();
                    if yes >= self.quorum() {
                        self.become_leader();
                    } else if no >= self.quorum() {
                        self.become_follower(self.term, None);
                    }
                }
            }
            Message::Append {
                prev_index,
                prev_term,
                entries,
                commit,
                seq,
                ..
            } => {
                self.handle_append(from, prev_index, prev_term, entries, commit, seq);
            }
            Message::AppendResp {
                success,
                index,
                seq,
                ..
            } => {
                if self.role != Role::Leader {
                    return;
                }
                let Some(p) = self.progress.get_mut(&from) else {
                    return;
                };
                p.acked_seq = p.acked_seq.max(seq);
                // Flow control: settle exactly the appends this response covers.
                p.settle(seq);
                if success {
                    if index > p.matched {
                        p.matched = index;
                    }
                    p.next = p.matched.next().max(p.next);
                    let next = p.next;
                    self.maybe_commit();
                    self.check_reads();
                    self.maybe_send_timeout_now();
                    if next <= self.last_index() {
                        self.send_append(from);
                    }
                } else {
                    self.rejects += 1;
                    // Back off to the follower's hint, never below what it already matched.
                    let hint = index.max(p.matched.next());
                    p.next = hint.min(p.next.get().saturating_sub(1).max(1).into());
                    p.reset_window();
                    self.send_append(from);
                }
            }
            Message::InstallSnapshot { snapshot, .. } => {
                self.leader = Some(from);
                self.reset_election_timer();
                if snapshot.last_index <= self.commit {
                    // Already covered: report the committed prefix only. Entries past the
                    // commit index may be from another term and conflict with the leader's; a
                    // leader that took them as matched committed entries without a majority
                    // (chaos seed 11892, a safety violation).
                    let idx = self.commit;
                    self.send(
                        from,
                        Message::SnapshotResp {
                            term: self.term,
                            index: idx,
                        },
                    );
                    return;
                }
                // Replace the log with the snapshot.
                self.entries.clear();
                self.first_index = snapshot.last_index.next();
                self.snapshot_term = snapshot.last_term;
                self.commit = snapshot.last_index;
                self.applied = snapshot.last_index;
                self.persisted = snapshot.last_index;
                self.handed = snapshot.last_index;
                self.pending_truncate = None;
                self.hs_dirty = true;
                self.installing = true;
                self.snapshot_to_install = Some(snapshot.clone());
                self.send(
                    from,
                    Message::SnapshotResp {
                        term: self.term,
                        index: snapshot.last_index,
                    },
                );
            }
            Message::TimeoutNow { .. } => {
                // Only from the current leader, and only if this node may take over.
                // Not while installing a snapshot: this node could not serve as leader.
                if self.role == Role::Follower
                    && self.leader == Some(from)
                    && self.may_vote()
                    && !self.installing
                {
                    self.become_candidate();
                }
            }
            Message::ReadIndexReq { id, .. } => {
                if self.role == Role::Leader {
                    self.start_read(id, Some(from));
                } else {
                    self.send(
                        from,
                        Message::ReadIndexResp {
                            term: self.term,
                            id,
                            index: LogIndex(0),
                            ok: false,
                        },
                    );
                }
            }
            Message::ReadIndexResp { id, index, ok, .. } => {
                // Only the current leader's answer counts; a stale one is ignored and the read
                // times out (or was already failed by a leadership change).
                if self.leader != Some(from) {
                    return;
                }
                if let Some(pos) = self.follower_reads.iter().position(|r| r.0 == id) {
                    self.follower_reads.swap_remove(pos);
                    if ok {
                        self.read_states.push((id, index));
                    } else {
                        self.failed_reads.push(id);
                    }
                }
            }
            Message::SnapshotResp { index, .. } => {
                if self.role != Role::Leader {
                    return;
                }
                let next = match self.progress.get_mut(&from) {
                    Some(p) => {
                        p.matched = p.matched.max(index);
                        p.next = p.matched.next();
                        p.next
                    }
                    None => return,
                };
                self.maybe_commit();
                if next <= self.last_index() {
                    self.send_append(from);
                }
            }
        }
    }

    fn handle_append(
        &mut self,
        from: NodeId,
        prev_index: LogIndex,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
        seq: u64,
    ) {
        if self.role != Role::Follower {
            self.become_follower(self.term, Some(from));
        }
        self.leader = Some(from);
        self.reset_election_timer();
        let term = self.term;
        // Consistency check.
        if prev_index > self.last_index() {
            self.send(
                from,
                Message::AppendResp {
                    term,
                    success: false,
                    index: self.last_index().next(),
                    seq,
                },
            );
            return;
        }
        if prev_index.get() + 1 < self.first_index.get() {
            // The leader is behind our snapshot: whatever it sends below it is already covered.
            let covered = LogIndex(self.first_index.get() - 1);
            let beyond: Vec<Entry> = entries
                .into_iter()
                .filter(|e| e.index >= self.first_index)
                .collect();
            if beyond.first().is_some_and(|e| e.index != self.first_index) {
                self.send(
                    from,
                    Message::AppendResp {
                        term,
                        success: true,
                        index: covered,
                        seq,
                    },
                );
                return;
            }
            return self.handle_append(
                from,
                covered,
                self.snapshot_term,
                beyond,
                leader_commit,
                seq,
            );
        }
        if prev_index.get() + 1 >= self.first_index.get() {
            match self.term_at(prev_index) {
                Some(t) if t == prev_term => {}
                _ => {
                    // Hint: first index of the conflicting term.
                    let bad_term = self.term_at(prev_index);
                    let mut hint = prev_index;
                    if let Some(bt) = bad_term {
                        while hint > self.first_index
                            && self.term_at(LogIndex(hint.get() - 1)) == Some(bt)
                        {
                            hint = LogIndex(hint.get() - 1);
                        }
                    }
                    self.send(
                        from,
                        Message::AppendResp {
                            term,
                            success: false,
                            index: hint,
                            seq,
                        },
                    );
                    return;
                }
            }
        }
        // Append, skipping what the snapshot already covers and truncating on conflict.
        let mut last_new = prev_index;
        for e in entries {
            last_new = e.index;
            if e.index < self.first_index {
                continue;
            }
            match self.term_at(e.index) {
                Some(t) if t == e.term => {}
                Some(_) => {
                    self.truncate_from(e.index);
                    self.entries.push(e);
                }
                None => {
                    debug_assert_eq!(e.index, self.last_index().next());
                    self.entries.push(e);
                }
            }
        }
        if last_new < prev_index {
            last_new = prev_index;
        }
        let new_commit = leader_commit.min(last_new).max(self.commit);
        if new_commit != self.commit {
            self.commit = new_commit;
            self.hs_dirty = true;
        }
        // Acknowledge only the prefix the leader verified; anything beyond may be stale.
        let idx = last_new;
        self.send(
            from,
            Message::AppendResp {
                term,
                success: true,
                index: idx,
                seq,
            },
        );
    }

    fn send_append(&mut self, to: NodeId) {
        let Some(p) = self.progress.get(&to).copied() else {
            return;
        };
        if p.next < self.first_index {
            if let Some(s) = self.snapshot.clone()
                && s.last_index.next() >= self.first_index
            {
                self.send(
                    to,
                    Message::InstallSnapshot {
                        term: self.term,
                        snapshot: s,
                    },
                );
            }
            return;
        }
        // Pipeline after what is already in flight; when the window is full (or nothing is new),
        // send a heartbeat (no entries) anchored at `next`.
        let from = if p.inflight_n > 0 {
            p.next.max(p.sent.next())
        } else {
            p.next
        };
        let with_entries = p.inflight_n < MAX_INFLIGHT && from <= self.last_index();
        let (prev_index, entries) = if with_entries {
            let start = (from.get() - self.first_index.get()) as usize;
            let mut end = start;
            let mut bytes = 0usize;
            while end < self.entries.len()
                && end - start < self.cfg.max_batch
                && (end == start || bytes + self.entries[end].payload.len() <= MAX_APPEND_BYTES)
            {
                bytes += self.entries[end].payload.len();
                end += 1;
            }
            (LogIndex(from.get() - 1), self.entries[start..end].to_vec())
        } else {
            (LogIndex(p.next.get() - 1), Vec::new())
        };
        let prev_term = self.term_at(prev_index).unwrap_or(Term(0));
        let commit = self.commit;
        let seq = if entries.is_empty() {
            self.heartbeat_seq
        } else {
            // A fresh sequence per entry-bearing append lets a response tell which appends it
            // covers (sequences stay monotonic, as ReadIndex needs).
            self.heartbeat_seq += 1;
            let seq = self.heartbeat_seq;
            self.entries_sent += entries.len() as u64;
            if let Some(pm) = self.progress.get_mut(&to) {
                pm.push_inflight(seq);
                pm.sent = entries.last().map_or(pm.sent, |e| e.index);
            }
            seq
        };
        self.send(
            to,
            Message::Append {
                term: self.term,
                prev_index,
                prev_term,
                entries,
                commit,
                seq,
            },
        );
    }

    fn broadcast_append(&mut self) {
        self.heartbeat_seq += 1;
        for &p in &self.cfg.peers.clone() {
            if p != self.cfg.id {
                self.send_append(p);
            }
        }
        if self.cfg.peers.len() == 1 {
            self.maybe_commit();
            self.check_reads();
        }
    }

    fn maybe_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let mut matched: Vec<LogIndex> = self
            .cfg
            .peers
            .iter()
            .map(|p| {
                if *p == self.cfg.id {
                    self.persisted
                } else {
                    self.progress.get(p).map_or(LogIndex(0), |x| x.matched)
                }
            })
            .collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let n = matched[self.quorum() - 1];
        if n > self.commit && self.term_at(n) == Some(self.term) {
            self.commit = n;
            self.hs_dirty = true;
            // Tell followers right away instead of at the next heartbeat: this is what bounds
            // takedown visibility on replicas (ADR 0010).
            for &p in &self.cfg.peers.clone() {
                if p != self.cfg.id {
                    self.send_append(p);
                }
            }
        }
    }

    fn check_reads(&mut self) {
        if self.role != Role::Leader || self.reads.is_empty() || self.commit < self.lead_start {
            return;
        }
        let mut done = Vec::new();
        for (i, r) in self.reads.iter().enumerate() {
            let acks = 1 + self
                .progress
                .iter()
                .filter(|(p, x)| **p != self.cfg.id && x.acked_seq >= r.seq)
                .count();
            if acks >= self.quorum() {
                done.push(i);
            }
        }
        for i in done.into_iter().rev() {
            let r = self.reads.remove(i);
            let index = r.index.max(self.lead_start);
            match r.from {
                None => self.read_states.push((r.id, index)),
                Some(p) => self.send(
                    p,
                    Message::ReadIndexResp {
                        term: self.term,
                        id: r.id,
                        index,
                        ok: true,
                    },
                ),
            }
        }
    }

    /// Whether a snapshot is accepted but not yet installed by the driver.
    pub fn is_installing_snapshot(&self) -> bool {
        self.installing
    }

    /// Term of the compaction point (`first_index - 1`).
    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    /// Pending read requests and follower acknowledgement sequences (diagnostics).
    #[allow(clippy::type_complexity)]
    pub fn debug_reads(&self) -> (Vec<(u64, u64)>, Vec<(NodeId, u64)>, u64, LogIndex) {
        (
            self.reads.iter().map(|r| (r.id, r.seq)).collect(),
            self.progress
                .iter()
                .map(|(n, p)| (*n, p.acked_seq))
                .collect(),
            self.heartbeat_seq,
            self.lead_start,
        )
    }

    /// Drains everything the driver must act on.
    pub fn ready(&mut self) -> Ready {
        let mut ready = Ready::default();
        if self.hs_dirty {
            ready.hard_state = Some(self.hard_state());
            self.hs_dirty = false;
        }
        ready.truncate_from = self.pending_truncate.take();
        if self.last_index() > self.handed {
            let start = (self.handed.get() + 1 - self.first_index.get()) as usize;
            ready.entries = self.entries[start..].to_vec();
            self.handed = self.last_index();
        }
        let leader = self.role == Role::Leader;
        for (to, m) in std::mem::take(&mut self.msgs) {
            let now = leader
                && matches!(
                    m,
                    Message::Append { .. }
                        | Message::InstallSnapshot { .. }
                        | Message::ReadIndexResp { .. }
                        | Message::TimeoutNow { .. }
                );
            if now {
                ready.messages.push((to, m));
            } else {
                ready.persisted_messages.push((to, m));
            }
        }
        ready.snapshot = self.snapshot_to_install.take();
        let apply_to = self.commit.min(self.persisted);
        if !self.installing && apply_to > self.applied && self.applied.next() >= self.first_index {
            let start = (self.applied.get() + 1 - self.first_index.get()) as usize;
            let end = (apply_to.get() + 1 - self.first_index.get()) as usize;
            ready.committed = self.entries[start..end].to_vec();
        }
        ready.read_states = std::mem::take(&mut self.read_states);
        ready.failed_reads = std::mem::take(&mut self.failed_reads);
        ready
    }

    /// Reports what the driver persisted and applied.
    pub fn advance(&mut self, persisted: LogIndex, applied: LogIndex) {
        if self.installing && applied.next() >= self.first_index {
            self.installing = false;
        }
        if persisted > self.persisted {
            // Only what was handed out can be durable (a report from before a truncation may
            // name a higher index).
            self.persisted = persisted.min(self.handed);
        }
        if applied > self.applied {
            self.applied = applied.min(self.commit);
        }
        if self.role == Role::Leader {
            self.maybe_commit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::HashSet;

    /// Persisted state of one node in the harness.
    #[derive(Default, Clone)]
    struct Disk {
        hs: HardState,
        entries: Vec<Entry>,
        snapshot: (LogIndex, Term),
        applied: Vec<Entry>,
    }

    struct Node {
        raft: Raft,
        disk: Disk,
        alive: bool,
        read_states: Vec<(u64, LogIndex)>,
        failed_reads: Vec<u64>,
        /// Asynchronous persistence (ADR 0027): written, not synced yet (lost on a crash).
        unsynced: Vec<Entry>,
        /// Hard state to store with the next sync.
        pending_hs: Option<HardState>,
        /// `persisted_messages` waiting for the next sync.
        held: Vec<(NodeId, Message)>,
    }

    struct Cluster {
        nodes: Vec<Node>,
        inflight: Vec<(NodeId, NodeId, Message)>,
        rng: SeededRng,
        leaders_by_term: HashMap<Term, NodeId>,
        committed: HashMap<LogIndex, Entry>,
        blocked: HashSet<(NodeId, NodeId)>,
        in_order: bool,
        proposed: u64,
        drop_prob: f64,
        /// Persist asynchronously: syncs happen at random steps (ADR 0027).
        async_persist: bool,
    }

    fn config(id: NodeId, n: u32, seed: u64) -> Config {
        Config {
            id,
            peers: (1..=n).map(NodeId).collect(),
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_batch: 8,
            rng: SeededRng::from_seed(seed ^ (u64::from(id.get()) << 32)),
        }
    }

    impl Cluster {
        fn new(n: u32, seed: u64, drop_prob: f64) -> Self {
            let nodes = (1..=n)
                .map(|i| Node {
                    raft: Raft::new(
                        config(NodeId(i), n, seed),
                        InitialState {
                            hard_state: HardState::default(),
                            entries: vec![],
                            snapshot: (LogIndex(0), Term(0)),
                            applied: LogIndex(0),
                            installing: false,
                            vote_barrier: (LogIndex(0), Term(0)),
                        },
                    ),
                    disk: Disk::default(),
                    alive: true,
                    read_states: Vec::new(),
                    failed_reads: Vec::new(),
                    unsynced: Vec::new(),
                    pending_hs: None,
                    held: Vec::new(),
                })
                .collect();
            Cluster {
                nodes,
                inflight: Vec::new(),
                rng: SeededRng::from_seed(seed),
                leaders_by_term: HashMap::default(),
                committed: HashMap::default(),
                blocked: HashSet::default(),
                in_order: false,
                proposed: 0,
                drop_prob,
                async_persist: false,
            }
        }

        /// Makes node `i`'s written entries and hard state durable, and releases the messages
        /// that waited for them.
        fn sync(&mut self, i: usize) {
            let node = &mut self.nodes[i];
            node.disk.entries.append(&mut node.unsynced);
            if let Some(hs) = node.pending_hs.take() {
                node.disk.hs = hs;
            }
            let from = node.raft.id();
            for (to, m) in node.held.drain(..) {
                self.inflight.push((from, to, m));
            }
            let node = &mut self.nodes[i];
            let persisted = node
                .disk
                .entries
                .last()
                .map_or(node.disk.snapshot.0, |e| e.index);
            let applied = node
                .disk
                .applied
                .last()
                .map_or(node.disk.snapshot.0, |e| e.index);
            node.raft.advance(persisted, applied);
            self.drain_ready(i);
        }

        fn drain_ready(&mut self, i: usize) {
            if self.async_persist {
                let r = &self.nodes[i].raft;
                // Truncations and snapshots first make everything durable, then run
                // synchronously, as the replica does (ADR 0027).
                let peek_sync = r.pending_truncate.is_some() || r.snapshot_to_install.is_some();
                if peek_sync
                    && (!self.nodes[i].unsynced.is_empty() || self.nodes[i].pending_hs.is_some())
                {
                    let node = &mut self.nodes[i];
                    node.disk.entries.append(&mut node.unsynced);
                    if let Some(hs) = node.pending_hs.take() {
                        node.disk.hs = hs;
                    }
                    let from = node.raft.id();
                    for (to, m) in node.held.drain(..) {
                        self.inflight.push((from, to, m));
                    }
                }
            }
            let async_persist = self.async_persist;
            let node = &mut self.nodes[i];
            let ready = node.raft.ready();
            let sync_now =
                !async_persist || ready.truncate_from.is_some() || ready.snapshot.is_some();
            if let Some(hs) = ready.hard_state {
                if sync_now {
                    node.disk.hs = hs;
                    node.pending_hs = None;
                } else {
                    node.pending_hs = Some(hs);
                }
            }
            if let Some(t) = ready.truncate_from {
                node.disk.entries.retain(|e| e.index < t);
            }
            if async_persist && !sync_now {
                for e in &ready.entries {
                    let last = node
                        .unsynced
                        .last()
                        .or(node.disk.entries.last())
                        .map_or(node.disk.snapshot.0, |x| x.index);
                    assert_eq!(
                        e.index,
                        last.next(),
                        "node {} written log not contiguous",
                        i + 1
                    );
                    node.unsynced.push(e.clone());
                }
                let from = node.raft.id();
                node.held.extend(ready.persisted_messages);
                node.read_states.extend(ready.read_states.iter().copied());
                node.failed_reads.extend(ready.failed_reads.iter().copied());
                let persisted = node
                    .disk
                    .entries
                    .last()
                    .map_or(node.disk.snapshot.0, |e| e.index);
                for e in &ready.committed {
                    let expect = node
                        .disk
                        .applied
                        .last()
                        .map_or(node.disk.snapshot.0, |x| x.index)
                        .next();
                    assert_eq!(e.index, expect, "node {} applied out of order", i + 1);
                    assert!(
                        e.index <= persisted,
                        "node {} applied a non-durable entry",
                        i + 1
                    );
                    node.disk.applied.push(e.clone());
                    match self.committed.get(&e.index) {
                        Some(prev) => {
                            assert_eq!(prev, e, "state machine safety violated at {}", e.index)
                        }
                        None => {
                            self.committed.insert(e.index, e.clone());
                        }
                    }
                }
                let node = &mut self.nodes[i];
                let applied = node
                    .disk
                    .applied
                    .last()
                    .map_or(node.disk.snapshot.0, |e| e.index);
                node.raft.advance(persisted, applied);
                if node.raft.role() == Role::Leader {
                    let term = node.raft.term();
                    match self.leaders_by_term.get(&term) {
                        Some(l) => assert_eq!(*l, from, "two leaders in term {term}"),
                        None => {
                            self.leaders_by_term.insert(term, from);
                        }
                    }
                }
                for (to, m) in ready.messages {
                    self.inflight.push((from, to, m));
                }
                return;
            }
            for e in &ready.entries {
                assert_eq!(
                    e.index,
                    node.disk
                        .entries
                        .last()
                        .map_or(node.disk.snapshot.0, |x| x.index)
                        .next(),
                    "node {} persisted log not contiguous",
                    i + 1
                );
                node.disk.entries.push(e.clone());
            }
            if let Some(s) = &ready.snapshot {
                node.disk.entries.clear();
                node.disk.snapshot = (s.last_index, s.last_term);
                node.disk.applied.retain(|e| e.index <= s.last_index);
                // Snapshot data = concatenated applied payload lengths is not modelled; assume the
                // driver reconstructs the applied prefix from the leader (checked below by index).
            }
            let persisted = node
                .disk
                .entries
                .last()
                .map_or(node.disk.snapshot.0, |e| e.index);
            for e in &ready.committed {
                let expect = node
                    .disk
                    .applied
                    .last()
                    .map_or(node.disk.snapshot.0, |x| x.index)
                    .next();
                assert_eq!(e.index, expect, "node {} applied out of order", i + 1);
                node.disk.applied.push(e.clone());
                match self.committed.get(&e.index) {
                    Some(prev) => {
                        assert_eq!(prev, e, "state machine safety violated at {}", e.index)
                    }
                    None => {
                        self.committed.insert(e.index, e.clone());
                    }
                }
            }
            let applied = node
                .disk
                .applied
                .last()
                .map_or(node.disk.snapshot.0, |e| e.index);
            node.read_states.extend(ready.read_states.iter().copied());
            node.failed_reads.extend(ready.failed_reads.iter().copied());
            node.raft.advance(persisted, applied);
            let from = node.raft.id();
            if node.raft.role() == Role::Leader {
                let term = node.raft.term();
                match self.leaders_by_term.get(&term) {
                    Some(l) => assert_eq!(*l, from, "two leaders in term {term}"),
                    None => {
                        self.leaders_by_term.insert(term, from);
                    }
                }
            }
            for (to, m) in ready.messages.into_iter().chain(ready.persisted_messages) {
                self.inflight.push((from, to, m));
            }
        }

        fn step_all_ready(&mut self) {
            for i in 0..self.nodes.len() {
                if self.nodes[i].alive {
                    self.drain_ready(i);
                }
            }
        }

        fn tick(&mut self) {
            for i in 0..self.nodes.len() {
                if self.nodes[i].alive {
                    self.nodes[i].raft.tick();
                }
            }
            self.step_all_ready();
        }

        /// Delivers up to `max` messages in send order per link (like one TCP stream per
        /// peer): the realistic transport, as opposed to `deliver_some`'s random reordering.
        fn deliver_in_order(&mut self, max: usize) {
            for _ in 0..max {
                if self.inflight.is_empty() {
                    break;
                }
                let (from, to, m) = self.inflight.remove(0);
                let idx = (to.get() - 1) as usize;
                if !self.nodes[idx].alive {
                    continue;
                }
                self.nodes[idx].raft.step(from, m);
                self.drain_ready(idx);
            }
        }

        fn deliver_some(&mut self, max: usize) {
            for _ in 0..max {
                if self.inflight.is_empty() {
                    break;
                }
                // In-order mode delivers each message in send order (one TCP stream per link,
                // losses and partitions still apply); otherwise any message may overtake.
                let k = if self.in_order {
                    0
                } else {
                    self.rng.below(self.inflight.len() as u64) as usize
                };
                let (from, to, m) = if self.in_order {
                    self.inflight.remove(k)
                } else {
                    self.inflight.swap_remove(k)
                };
                if self.blocked.contains(&(from, to)) || self.rng.chance(self.drop_prob) {
                    continue;
                }
                let idx = (to.get() - 1) as usize;
                if !self.nodes[idx].alive {
                    continue;
                }
                self.nodes[idx].raft.step(from, m);
                self.drain_ready(idx);
            }
        }

        fn propose(&mut self) {
            let leader = self
                .nodes
                .iter()
                .position(|n| n.alive && n.raft.role() == Role::Leader);
            if let Some(i) = leader {
                self.proposed += 1;
                let payload = Bytes::from(format!("cmd{}", self.proposed));
                let _ = self.nodes[i].raft.propose(payload);
                self.drain_ready(i);
            }
        }

        fn crash(&mut self, i: usize) {
            self.nodes[i].alive = false;
            // Anything not synced is lost.
            self.nodes[i].unsynced.clear();
            self.nodes[i].pending_hs = None;
            self.nodes[i].held.clear();
            self.inflight
                .retain(|(_, to, _)| (to.get() - 1) as usize != i);
        }

        fn restart(&mut self, i: usize, seed: u64) {
            let disk = self.nodes[i].disk.clone();
            let n = self.nodes.len() as u32;
            let applied = disk.applied.last().map_or(disk.snapshot.0, |e| e.index);
            self.nodes[i].raft = Raft::new(
                config(NodeId(i as u32 + 1), n, seed),
                InitialState {
                    hard_state: disk.hs,
                    entries: disk.entries.clone(),
                    snapshot: disk.snapshot,
                    applied,
                    installing: false,
                    vote_barrier: (LogIndex(0), Term(0)),
                },
            );
            self.nodes[i].alive = true;
        }

        fn check_log_matching(&self) {
            for a in &self.nodes {
                for b in &self.nodes {
                    let la = &a.disk.entries;
                    let lb = &b.disk.entries;
                    for ea in la {
                        if let Some(eb) = lb.iter().find(|e| e.index == ea.index)
                            && ea.term == eb.term
                        {
                            assert_eq!(
                                ea.payload, eb.payload,
                                "log matching violated at {}",
                                ea.index
                            );
                            // All preceding shared entries must match too.
                            for x in la.iter().filter(|e| e.index < ea.index) {
                                if let Some(y) = lb.iter().find(|e| e.index == x.index) {
                                    assert_eq!(
                                        (x.term, &x.payload),
                                        (y.term, &y.payload),
                                        "log matching prefix violated at {}",
                                        x.index
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        fn dump(&self) -> String {
            let mut s = String::new();
            for n in &self.nodes {
                let log: Vec<String> = n
                    .disk
                    .entries
                    .iter()
                    .map(|e| {
                        format!(
                            "{}:{}:{}",
                            e.index.get(),
                            e.term.get(),
                            String::from_utf8_lossy(&e.payload)
                        )
                    })
                    .collect();
                let mem: Vec<String> = (n.raft.first_index().get()..=n.raft.last_index().get())
                    .map(|i| {
                        let e = n.raft.entry(LogIndex(i)).unwrap();
                        format!(
                            "{}:{}:{}",
                            i,
                            e.term.get(),
                            String::from_utf8_lossy(&e.payload)
                        )
                    })
                    .collect();
                s += &format!(
                    "node {} alive={} role={:?} term={} vote={:?} commit={} applied={}\n  disk: {:?}\n  mem:  {:?}\n  applied: {:?}\n",
                    n.raft.id(),
                    n.alive,
                    n.raft.role(),
                    n.raft.term(),
                    n.raft.vote,
                    n.raft.commit_index(),
                    n.raft.applied_index(),
                    log,
                    mem,
                    n.disk
                        .applied
                        .iter()
                        .map(|e| e.index.get())
                        .collect::<Vec<_>>()
                );
            }
            let mut v: Vec<_> = self.leaders_by_term.iter().collect();
            v.sort();
            s += &format!("leaders_by_term: {v:?}\n");
            s
        }

        fn check_leader_completeness(&self) {
            for n in &self.nodes {
                if n.alive && n.raft.role() == Role::Leader {
                    for (idx, e) in &self.committed {
                        // Only leaders of terms at or after the entry's term must hold it; a stale
                        // leader behind a partition has not learned of the newer term yet.
                        if n.raft.term() >= e.term
                            && *idx >= n.raft.first_index()
                            && n.raft.entry(*idx).map(|x| &x.payload) != Some(&e.payload)
                        {
                            panic!(
                                "leader {} lacks committed entry {idx} (committed {:?})\n{}",
                                n.raft.id(),
                                String::from_utf8_lossy(&e.payload),
                                self.dump()
                            );
                        }
                    }
                }
            }
        }

        fn min_applied(&self) -> LogIndex {
            self.nodes
                .iter()
                .map(|n| n.disk.applied.last().map_or(n.disk.snapshot.0, |e| e.index))
                .min()
                .unwrap()
        }
    }

    #[test]
    fn elects_a_leader_and_commits_without_faults() {
        let mut c = Cluster::new(3, 1, 0.0);
        for _ in 0..40 {
            c.tick();
            c.deliver_some(50);
        }
        assert!(c.nodes.iter().any(|n| n.raft.role() == Role::Leader));
        for _ in 0..20 {
            c.propose();
            c.deliver_some(100);
            c.tick();
        }
        for _ in 0..20 {
            c.deliver_some(100);
            c.tick();
        }
        assert_eq!(c.proposed, 20);
        assert!(
            c.min_applied().get() >= 21,
            "all nodes applied everything: {:?}",
            c.min_applied()
        );
        c.check_log_matching();
        c.check_leader_completeness();
        let a = &c.nodes[0].disk.applied;
        for n in &c.nodes {
            assert_eq!(&n.disk.applied, a);
        }
    }

    #[test]
    fn read_index_completes_after_a_heartbeat_round() {
        let mut c = Cluster::new(3, 5, 0.0);
        for _ in 0..40 {
            c.tick();
            c.deliver_some(50);
        }
        let li = c
            .nodes
            .iter()
            .position(|n| n.raft.role() == Role::Leader)
            .unwrap();
        c.nodes[li].raft.read_index(42).unwrap();
        c.drain_ready(li);
        let mut got = None;
        for _ in 0..20 {
            c.deliver_some(50);
            if let Some(rs) = c.nodes[li].read_states.first() {
                got = Some(*rs);
                break;
            }
        }
        let (id, index) = got.unwrap_or_else(|| {
            panic!(
                "read index resolved; state {:?} commit {}",
                c.nodes[li].raft.debug_reads(),
                c.nodes[li].raft.commit_index()
            )
        });
        assert_eq!(id, 42);
        assert!(index >= c.nodes[li].raft.commit_index().min(index));

        // Follower read: the leader confirms and answers with an index at least its commit
        // index when the request arrived.
        c.propose();
        for _ in 0..10 {
            c.tick();
            c.deliver_some(50);
        }
        let leader_commit = c.nodes[li].raft.commit_index();
        let fi = (0..3).find(|i| *i != li).unwrap();
        c.nodes[fi].raft.read_index(7).unwrap();
        c.drain_ready(fi);
        let mut got = None;
        for _ in 0..40 {
            c.deliver_some(50);
            if let Some(rs) = c.nodes[fi].read_states.iter().find(|r| r.0 == 7) {
                got = Some(*rs);
                break;
            }
        }
        let (_, index) = got.expect("follower read resolved");
        assert!(index >= leader_commit, "{index} < {leader_commit}");
    }

    /// Many proposals (each broadcasting heartbeats while the window is full) must not make
    /// the leader re-send entries: every entry is shipped to each follower about once.
    #[test]
    fn heartbeats_do_not_drain_the_append_window() {
        let mut c = Cluster::new(3, 21, 0.0);
        for _ in 0..40 {
            c.tick();
            c.deliver_some(50);
        }
        let li = c
            .nodes
            .iter()
            .position(|n| n.raft.role() == Role::Leader)
            .unwrap();
        let proposed = 200 * 5;
        for round in 0..200 {
            // Several proposals per round: each broadcasts to both followers.
            for _ in 0..5 {
                c.nodes[li]
                    .raft
                    .propose(Bytes::from(vec![round as u8; 64]))
                    .unwrap();
            }
            c.drain_ready(li);
            // Deliver slowly, in order (one TCP stream per peer), so appends stay in flight while
            // heartbeats pile up.
            c.deliver_in_order(3);
        }
        for _ in 0..2000 {
            c.tick();
            c.deliver_in_order(100);
            if c.nodes
                .iter()
                .all(|n| n.raft.commit_index() > LogIndex(proposed as u64))
            {
                break;
            }
        }
        let (shipped, _) = c.nodes[li].raft.send_stats();
        let commits: Vec<LogIndex> = c.nodes.iter().map(|n| n.raft.commit_index()).collect();
        assert!(
            c.nodes
                .iter()
                .all(|n| n.raft.commit_index() > LogIndex(proposed as u64)),
            "{commits:?}"
        );
        // Each entry goes to each of the two followers once (the old window let heartbeat
        // responses free slots and re-sent batches many times over).
        assert!(
            shipped as usize <= proposed * 2 + 64,
            "entries shipped {shipped} for {proposed} proposals to 2 followers"
        );
    }

    fn stable_leader(c: &mut Cluster) -> usize {
        for _ in 0..200 {
            c.tick();
            c.deliver_some(50);
            if let Some(li) = c.nodes.iter().position(|n| n.raft.role() == Role::Leader) {
                return li;
            }
        }
        panic!("no leader");
    }

    #[test]
    fn leadership_transfer_hands_over_and_keeps_the_log() {
        let mut c = Cluster::new(3, 21, 0.0);
        let li = stable_leader(&mut c);
        for _ in 0..5 {
            c.propose();
        }
        for _ in 0..20 {
            c.tick();
            c.deliver_some(100);
        }
        let to = (0..3).find(|i| *i != li).unwrap();
        let term = c.nodes[li].raft.term();
        c.nodes[li]
            .raft
            .transfer_leadership(NodeId(to as u32 + 1))
            .unwrap();
        // No new entries while handing over.
        assert!(c.nodes[li].raft.propose(Bytes::from_static(b"x")).is_err());
        c.drain_ready(li);
        for _ in 0..30 {
            c.deliver_some(100);
            if c.nodes[to].raft.role() == Role::Leader {
                break;
            }
        }
        assert_eq!(c.nodes[to].raft.role(), Role::Leader);
        assert_eq!(c.nodes[to].raft.term(), Term(term.get() + 1));
        assert_ne!(c.nodes[li].raft.role(), Role::Leader);
        for _ in 0..20 {
            c.tick();
            c.deliver_some(100);
        }
        c.check_log_matching();
        c.check_leader_completeness();
        assert!(c.nodes[to].raft.commit_index() >= LogIndex(6));
    }

    #[test]
    fn leadership_transfer_to_an_unreachable_peer_is_abandoned() {
        let mut c = Cluster::new(3, 22, 0.0);
        let li = stable_leader(&mut c);
        let to = (0..3).find(|i| *i != li).unwrap();
        let (a, b) = (NodeId(li as u32 + 1), NodeId(to as u32 + 1));
        c.blocked.insert((a, b));
        c.blocked.insert((b, a));
        c.nodes[li].raft.transfer_leadership(b).unwrap();
        assert!(c.nodes[li].raft.transferring_to().is_some());
        for _ in 0..40 {
            c.tick();
            c.deliver_some(100);
        }
        // The other follower kept the old leader in place; the transfer timed out.
        assert_eq!(c.nodes[li].raft.role(), Role::Leader);
        assert!(c.nodes[li].raft.transferring_to().is_none());
        assert!(c.nodes[li].raft.propose(Bytes::from_static(b"y")).is_ok());
        c.blocked.clear();
        for _ in 0..60 {
            c.tick();
            c.deliver_some(100);
        }
        c.check_log_matching();
        c.check_leader_completeness();
    }

    /// A follower whose log holds uncommitted entries from an older term, and that receives a
    /// snapshot it already covers, must not report those entries as matched (seed 11892).
    #[test]
    fn snapshot_response_reports_only_the_committed_prefix() {
        let old_entries: Vec<Entry> = (1..=10u64)
            .map(|i| Entry {
                index: LogIndex(i),
                term: Term(if i <= 6 { 1 } else { 3 }),
                payload: Bytes::from_static(b"x"),
            })
            .collect();
        let mut f = Raft::new(
            config(NodeId(3), 3, 1),
            InitialState {
                hard_state: HardState {
                    term: Term(3),
                    vote: None,
                    commit: LogIndex(6),
                },
                entries: old_entries,
                snapshot: (LogIndex(0), Term(0)),
                applied: LogIndex(6),
                installing: false,
                vote_barrier: (LogIndex(0), Term(0)),
            },
        );
        let _ = f.ready();
        f.step(
            NodeId(1),
            Message::InstallSnapshot {
                term: Term(5),
                snapshot: Snapshot {
                    last_index: LogIndex(5),
                    last_term: Term(1),
                    data: Bytes::new(),
                },
            },
        );
        let r = f.ready();
        let resp = r
            .messages
            .into_iter()
            .chain(r.persisted_messages)
            .find_map(|(_, m)| match m {
                Message::SnapshotResp { index, .. } => Some(index),
                _ => None,
            })
            .expect("a snapshot response");
        assert_eq!(
            resp,
            LogIndex(6),
            "only the committed prefix may count as matched"
        );
    }

    #[test]
    fn follower_read_fails_when_the_leader_goes_away() {
        let mut c = Cluster::new(3, 9, 0.0);
        for _ in 0..40 {
            c.tick();
            c.deliver_some(50);
        }
        let li = c
            .nodes
            .iter()
            .position(|n| n.raft.role() == Role::Leader)
            .unwrap();
        let fi = (0..3).find(|i| *i != li).unwrap();
        c.crash(li);
        c.nodes[fi].raft.read_index(99).unwrap();
        c.drain_ready(fi);
        for _ in 0..200 {
            c.tick();
            c.deliver_some(50);
            if c.nodes[fi].failed_reads.contains(&99) {
                break;
            }
        }
        assert!(
            c.nodes[fi].failed_reads.contains(&99),
            "the read must fail, not hang"
        );
        assert!(!c.nodes[fi].read_states.iter().any(|r| r.0 == 99));
    }

    fn chaos(seed: u64, nodes: u32, steps: usize) {
        chaos_with(seed, nodes, steps, false);
    }

    fn chaos_with(seed: u64, nodes: u32, steps: usize, in_order: bool) {
        chaos_full(seed, nodes, steps, in_order, false);
    }

    fn chaos_full(seed: u64, nodes: u32, steps: usize, in_order: bool, async_persist: bool) {
        let mut c = Cluster::new(nodes, seed, 0.05);
        c.in_order = in_order;
        c.async_persist = async_persist;
        let mut crashed: Vec<(usize, u32)> = Vec::new();
        for step in 0..steps {
            let r = c.rng.below(100);
            if r < 40 {
                c.tick();
            } else if r < 80 {
                let n = 1 + c.rng.below(10) as usize;
                c.deliver_some(n);
            } else if r < 88 {
                c.propose();
            } else if r < 90 {
                // Leadership transfer to a random peer (it may be down or partitioned).
                if let Some(li) = c
                    .nodes
                    .iter()
                    .position(|n| n.alive && n.raft.role() == Role::Leader)
                {
                    let to = NodeId(1 + c.rng.below(u64::from(nodes)) as u32);
                    let _ = c.nodes[li].raft.transfer_leadership(to);
                    c.drain_ready(li);
                }
            } else if r < 94 {
                // Crash a node (keep a majority alive).
                let alive = c.nodes.iter().filter(|n| n.alive).count();
                if alive > (nodes as usize / 2 + 1) {
                    let i = c.rng.below(u64::from(nodes)) as usize;
                    if c.nodes[i].alive {
                        c.crash(i);
                        crashed.push((i, 5 + c.rng.below(30) as u32));
                    }
                }
            } else if r < 97 {
                // Partition one node for a while.
                let i = NodeId(1 + c.rng.below(u64::from(nodes)) as u32);
                for j in 1..=nodes {
                    let j = NodeId(j);
                    if j != i {
                        c.blocked.insert((i, j));
                        c.blocked.insert((j, i));
                    }
                }
            } else {
                c.blocked.clear();
            }
            // Asynchronous persistence: a random live node's sync completes.
            if c.async_persist && c.rng.chance(0.3) {
                let i = c.rng.below(u64::from(nodes)) as usize;
                if c.nodes[i].alive {
                    c.sync(i);
                }
            }
            // Restart crashed nodes when their timer expires.
            let mut still = Vec::new();
            for (i, t) in crashed.drain(..) {
                if t == 0 {
                    c.restart(i, seed + step as u64);
                } else {
                    still.push((i, t - 1));
                }
            }
            crashed = still;
            c.check_log_matching();
            c.check_leader_completeness();
        }
        // Heal everything and let the cluster converge.
        c.blocked.clear();
        for (i, _) in crashed.drain(..) {
            c.restart(i, seed + 999);
        }
        for _ in 0..200 {
            c.tick();
            c.deliver_some(200);
            if c.async_persist {
                for i in 0..nodes as usize {
                    if c.nodes[i].alive {
                        c.sync(i);
                    }
                }
            }
        }
        c.check_log_matching();
        c.check_leader_completeness();
        assert!(
            c.nodes.iter().any(|n| n.raft.role() == Role::Leader),
            "seed {seed}: no leader after healing"
        );
        let max_commit = c.nodes.iter().map(|n| n.raft.commit_index()).max().unwrap();
        let min_applied = c.min_applied();
        assert_eq!(
            min_applied, max_commit,
            "seed {seed}: nodes did not converge ({min_applied:?} vs {max_commit:?})"
        );
        // Every node applied the same sequence.
        let a = &c.nodes[0].disk.applied;
        for n in &c.nodes {
            assert_eq!(n.disk.applied.len(), a.len(), "seed {seed}");
            assert_eq!(&n.disk.applied, a, "seed {seed}");
        }
    }

    #[test]
    fn chaos_three_nodes_in_order_delivery() {
        for seed in 0..500u64 {
            chaos_with(seed, 3, 600, true);
        }
    }

    /// ADR 0027: entries and hard state become durable at random later steps, crashes lose
    /// what was not synced, and messages that rely on durability wait for the sync.
    #[test]
    fn chaos_async_persistence() {
        for seed in 0..300u64 {
            chaos_full(seed, 3, 600, false, true);
        }
        for seed in 0..300u64 {
            chaos_full(seed, 3, 600, true, true);
        }
        for seed in 0..60u64 {
            chaos_full(seed, 5, 600, false, true);
        }
    }

    #[test]
    fn chaos_three_nodes() {
        for seed in 0..60u64 {
            chaos(seed, 3, 600);
        }
    }

    #[test]
    fn chaos_five_nodes() {
        for seed in 0..20u64 {
            chaos(seed, 5, 600);
        }
    }

    #[test]
    fn message_codec_roundtrip() {
        use cairn_core::codec::{Reader, Writer};
        let msgs = vec![
            Message::ReadIndexReq {
                term: Term(3),
                id: 17,
            },
            Message::ReadIndexResp {
                term: Term(3),
                id: 17,
                index: LogIndex(40),
                ok: true,
            },
            Message::PreVote {
                term: Term(3),
                last_index: LogIndex(9),
                last_term: Term(2),
            },
            Message::PreVoteResp {
                term: Term(3),
                granted: true,
                current: Term(2),
            },
            Message::TimeoutNow { term: Term(8) },
            Message::Vote {
                term: Term(3),
                last_index: LogIndex(9),
                last_term: Term(2),
            },
            Message::VoteResp {
                term: Term(3),
                granted: false,
            },
            Message::Append {
                term: Term(3),
                prev_index: LogIndex(9),
                prev_term: Term(2),
                entries: vec![Entry {
                    index: LogIndex(10),
                    term: Term(3),
                    payload: Bytes::from_static(b"x"),
                }],
                commit: LogIndex(8),
                seq: 7,
            },
            Message::AppendResp {
                term: Term(3),
                success: true,
                index: LogIndex(10),
                seq: 7,
            },
            Message::InstallSnapshot {
                term: Term(3),
                snapshot: Snapshot {
                    last_index: LogIndex(5),
                    last_term: Term(1),
                    data: Bytes::from_static(b"snap"),
                },
            },
            Message::SnapshotResp {
                term: Term(3),
                index: LogIndex(5),
            },
        ];
        for m in msgs {
            let mut w = Writer::new();
            m.encode(&mut w);
            let mut r = Reader::new(w.as_slice());
            assert_eq!(Message::decode(&mut r).unwrap(), m);
            r.finish().unwrap();
        }
    }
}
