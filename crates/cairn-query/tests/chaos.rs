//! The Phase 3 signature test: clients read and write a 3-node shard while the simulator
//! injects partitions, message drops, crashes and restarts. Afterwards every read is checked
//! against a per-key model with real-time bounds, read-your-writes tokens are honoured, no
//! read that promises read-your-takedown returns a removed document, and the replicas converge.
//! Deletions by filter (ADR 0031) take part: "every document whose version is at most X",
//! resolved by each replica when it applies the entry. Patches (ADR 0031) change a document's
//! `pad` field: they never bring a deleted document back nor change its version, and the
//! replicas end with the same whole documents.

use bytes::Bytes;
use cairn_core::Runtime;
use cairn_core::{
    DocId, Document, Duration, FieldDef, FieldKind, HashMap, Instant, LogIndex, NodeId, Predicate,
    Schema, SeededRng, ShardId, Value,
};
use cairn_index::{HnswParams, VectorIndexParams};
use cairn_query::{Consistency, EngineConfig, Replica, ReplicaConfig, ReplicaHandle, Token};
use cairn_sim::{SimConfig, SimRuntime, Simulation};
use cairn_storage::{Command, DeleteScope, LogConfig, PatchOp, PatchTarget, StoreConfig};
use std::cell::RefCell;
use std::rc::Rc;

const KEYS: u64 = 12;

fn schema() -> Schema {
    Schema::new(vec![
        FieldDef {
            name: "v".into(),
            kind: FieldKind::Vector {
                dims: 2,
                metric: cairn_core::Metric::L2,
            },
        },
        FieldDef {
            name: "ver".into(),
            kind: FieldKind::I64,
        },
        FieldDef {
            name: "pad".into(),
            kind: FieldKind::Blob,
        },
    ])
    .unwrap()
    .with_reserved()
    .unwrap()
}

/// Model keys above this one are written and read through a text id (ADR 0031): half of the
/// workload goes through the per-shard dictionary, checked by the same model.
const KEYED_OFFSET: u64 = 1000;

fn is_keyed(key: u64) -> bool {
    key > KEYED_OFFSET
}

fn text_id(key: u64) -> String {
    format!("k{}", key - KEYED_OFFSET)
}

fn doc(key: u64, version: i64) -> Document {
    let d = Document::new(DocId(if is_keyed(key) { 0 } else { key }), 5)
        .set(0, Value::Vector(vec![key as f32, version as f32]))
        .set(1, Value::I64(version))
        .set(2, Value::Blob(Bytes::from(vec![1u8; 120])));
    if is_keyed(key) {
        d.set(3, Value::Blob(Bytes::from(text_id(key).into_bytes())))
    } else {
        d
    }
}

fn upsert_cmd(key: u64, version: i64) -> Command {
    if is_keyed(key) {
        Command::UpsertKeyed(vec![doc(key, version)])
    } else {
        Command::Upsert(vec![doc(key, version)])
    }
}

fn delete_cmd(key: u64) -> Command {
    if is_keyed(key) {
        Command::DeleteKeys(vec![text_id(key)])
    } else {
        Command::Delete(vec![DocId(key)])
    }
}

/// Deletes every document whose version is at most `bound`.
fn delete_below_cmd(bound: i64) -> Command {
    Command::DeleteWhere {
        scope: DeleteScope::All,
        filter: Predicate::Range {
            field: 1,
            lo: None,
            hi: Some(Value::I64(bound)),
            lo_inclusive: false,
            hi_inclusive: true,
        },
    }
}

/// Sets `pad` of `key` to `byte`s, by id or text id.
fn patch_op(key: u64, byte: u8) -> PatchOp {
    PatchOp {
        target: if is_keyed(key) {
            PatchTarget::Key(text_id(key))
        } else {
            PatchTarget::Id(DocId(key))
        },
        set: vec![(2, Some(Value::Blob(Bytes::from(vec![byte; 40]))))],
    }
}

/// A patch through whichever replica leads, retried until acknowledged or stopped.
async fn patch_retrying(
    rt: &SimRuntime,
    handles: &[ReplicaHandle],
    op: PatchOp,
    start: usize,
    stop: &Rc<RefCell<bool>>,
) {
    let mut target = start;
    while !*stop.borrow() {
        match handles[target].patch(vec![op.clone()], None).await {
            Ok(_) => return,
            Err(cairn_core::Error::NotLeader {
                leader_hint: Some(l),
                ..
            }) => target = (l.get() - 1) as usize,
            Err(_) => target = (target + 1) % handles.len(),
        }
        rt.sleep(Duration::from_millis(15)).await;
    }
}

async fn get_any(
    h: &ReplicaHandle,
    key: u64,
    c: Consistency,
) -> cairn_core::Result<Option<Document>> {
    if is_keyed(key) {
        h.get_key(text_id(key), c).await
    } else {
        h.get(DocId(key), c).await
    }
}

fn version_of(d: &Document) -> i64 {
    match d.values[1] {
        Some(Value::I64(v)) => v,
        _ => panic!("no version"),
    }
}

fn config(node: NodeId) -> ReplicaConfig {
    ReplicaConfig {
        shard: ShardId(0),
        id: node,
        peers: vec![NodeId(1), NodeId(2), NodeId(3)],
        tick: Duration::from_millis(10),
        election_ticks: 10,
        heartbeat_ticks: 2,
        engine: EngineConfig {
            store: StoreConfig {
                segment_filter: None,
                // About four documents: every run flushes, ships, compacts and snapshots (at
                // 3000 bytes the 12 keys never filled a memtable, so no flush ever ran).
                memtable_max_bytes: 800,
                log: LogConfig {
                    max_file_bytes: 1 << 14,
                },
                max_segments: 3,
                max_deleted_fraction: 0.3,
                target_segment_rows: 0,
                min_merge: 4,
                shard: 0,
            },
            vector: VectorIndexParams {
                hnsw: HnswParams {
                    m: 4,
                    ef_construction: 8,
                },
                ..VectorIndexParams::default()
            },
        },
        dir: "shard0".into(),
        seed: 3,
        own_receiver: true,
        idle_flush_ticks: 0,
        compaction_slots: None,
        ship_segments: true,
        build_parallel: None,
        // Leader balancing on (ADR 0020): leadership transfers run under the faults too.
        preferred_leader: Some(NodeId(1)),
    }
}

#[derive(Debug, Clone)]
enum Op {
    /// Upsert with a version; `token` once acknowledged.
    Write {
        key: u64,
        version: i64,
        token: Option<Token>,
    },
    /// Delete; `token` once acknowledged.
    Delete { key: u64, token: Option<Token> },
    /// Deletion by filter of every document with a version at most `bound`.
    DeleteBelow { bound: i64, token: Option<Token> },
    /// Read: `Some(Some(v))` found version v, `Some(None)` absent, `None` failed.
    Read {
        key: u64,
        consistency: Consistency,
        result: Option<Option<i64>>,
    },
}

#[derive(Debug, Clone)]
struct Record {
    client: u32,
    call: Instant,
    ret: Instant,
    op: Op,
}

type History = Rc<RefCell<Vec<Record>>>;

async fn propose_retrying(
    rt: &SimRuntime,
    handles: &[ReplicaHandle],
    cmd: Command,
    start: usize,
) -> Option<Token> {
    let mut target = start;
    loop {
        match handles[target].propose(cmd.clone()).await {
            Ok(t) => return Some(t),
            Err(cairn_core::Error::NotLeader {
                leader_hint: Some(l),
                ..
            }) => target = (l.get() - 1) as usize,
            Err(_) => target = (target + 1) % handles.len(),
        }
        rt.sleep(Duration::from_millis(15)).await;
    }
}

async fn read_retrying(
    rt: &SimRuntime,
    handles: &[ReplicaHandle],
    key: u64,
    c: Consistency,
    start: usize,
) -> Option<Option<i64>> {
    let mut target = start;
    for _ in 0..40 {
        match get_any(&handles[target], key, c).await {
            Ok(d) => return Some(d.map(|d| version_of(&d))),
            Err(cairn_core::Error::NotLeader {
                leader_hint: Some(l),
                ..
            }) => target = (l.get() - 1) as usize,
            Err(_) => target = (target + 1) % handles.len(),
        }
        rt.sleep(Duration::from_millis(15)).await;
    }
    None
}

/// One client: loops over writes, deletes and reads; retries a write with the same version
/// until acknowledged, so every write eventually has a token (retries may commit twice, which
/// the checker tolerates as identical values).
fn spawn_client(
    rt: SimRuntime,
    handles: Vec<ReplicaHandle>,
    client: u32,
    history: History,
    versions: Rc<RefCell<i64>>,
    stop: Rc<RefCell<bool>>,
) {
    let mut rng = SeededRng::from_seed(1000 + u64::from(client));
    let r = rt.clone();
    rt.spawn(async move {
        let mut last_token: Option<Token> = None;
        while !*stop.borrow() {
            let key = 1 + rng.below(KEYS) + if rng.below(2) == 0 { KEYED_OFFSET } else { 0 };
            let start = rng.below(handles.len() as u64) as usize;
            let call = r.now();
            let roll = rng.below(20);
            if roll < 8 {
                let version = {
                    let mut v = versions.borrow_mut();
                    *v += 1;
                    *v
                };
                // Recorded at invocation: a write still in flight when the run ends may already
                // be visible, and the checker must know about it.
                let pos = {
                    let mut h = history.borrow_mut();
                    h.push(Record {
                        client,
                        call,
                        ret: Instant::from_nanos(u64::MAX),
                        op: Op::Write {
                            key,
                            version,
                            token: None,
                        },
                    });
                    h.len() - 1
                };
                let token = propose_retrying(&r, &handles, upsert_cmd(key, version), start).await;
                if let Some(t) = token {
                    last_token = Some(last_token.map_or(t, |p| p.max(t)));
                }
                let mut h = history.borrow_mut();
                h[pos].ret = r.now();
                h[pos].op = Op::Write {
                    key,
                    version,
                    token,
                };
            } else if roll < 10 {
                let pos = {
                    let mut h = history.borrow_mut();
                    h.push(Record {
                        client,
                        call,
                        ret: Instant::from_nanos(u64::MAX),
                        op: Op::Delete { key, token: None },
                    });
                    h.len() - 1
                };
                let token = propose_retrying(&r, &handles, delete_cmd(key), start).await;
                if let Some(t) = token {
                    last_token = Some(last_token.map_or(t, |p| p.max(t)));
                }
                let mut h = history.borrow_mut();
                h[pos].ret = r.now();
                h[pos].op = Op::Delete { key, token };
            } else if roll < 13 {
                // A patch of `pad`: not a change of version, never a resurrection (checked by
                // the model through every read), and the same everywhere (checked at the end).
                patch_retrying(
                    &r,
                    &handles,
                    patch_op(key, rng.below(250) as u8),
                    start,
                    &stop,
                )
                .await;
            } else if roll < 14 {
                // Old versions only: recent writes survive, so the model keeps live keys.
                let bound = *versions.borrow() - rng.below(12) as i64;
                let pos = {
                    let mut h = history.borrow_mut();
                    h.push(Record {
                        client,
                        call,
                        ret: Instant::from_nanos(u64::MAX),
                        op: Op::DeleteBelow { bound, token: None },
                    });
                    h.len() - 1
                };
                let token = propose_retrying(&r, &handles, delete_below_cmd(bound), start).await;
                if let Some(t) = token {
                    last_token = Some(last_token.map_or(t, |p| p.max(t)));
                }
                let mut h = history.borrow_mut();
                h[pos].ret = r.now();
                h[pos].op = Op::DeleteBelow { bound, token };
            } else {
                let consistency = match rng.below(3) {
                    0 => Consistency::Linearizable,
                    1 => last_token.map_or(Consistency::Stale, Consistency::ReadYourWrites),
                    _ => Consistency::Stale,
                };
                let result = read_retrying(&r, &handles, key, consistency, start).await;
                history.borrow_mut().push(Record {
                    client,
                    call,
                    ret: r.now(),
                    op: Op::Read {
                        key,
                        consistency,
                        result,
                    },
                });
            }
            r.sleep(Duration::from_millis(rng.below(30))).await;
        }
    });
}

/// Sequential per-key state derived from acknowledged writes in token order.
struct Model {
    /// Per key: `(index, value)` in index order; value `None` = deleted.
    writes: HashMap<u64, Vec<(LogIndex, Option<i64>)>>,
}

impl Model {
    fn build(history: &[Record]) -> Self {
        // Acknowledged operations in log order: a deletion by filter removes the keys whose
        // current version is within its bound, which depends on everything before it.
        let mut ops: Vec<(LogIndex, &Op)> = history
            .iter()
            .filter_map(|r| match &r.op {
                Op::Write { token: Some(t), .. }
                | Op::Delete { token: Some(t), .. }
                | Op::DeleteBelow { token: Some(t), .. } => Some((t.index, &r.op)),
                _ => None,
            })
            .collect();
        ops.sort_by_key(|(i, _)| *i);
        let mut writes: HashMap<u64, Vec<(LogIndex, Option<i64>)>> = HashMap::default();
        let mut current: HashMap<u64, i64> = HashMap::default();
        for (index, op) in ops {
            match op {
                Op::Write { key, version, .. } => {
                    writes
                        .entry(*key)
                        .or_default()
                        .push((index, Some(*version)));
                    current.insert(*key, *version);
                }
                Op::Delete { key, .. } => {
                    writes.entry(*key).or_default().push((index, None));
                    current.remove(key);
                }
                Op::DeleteBelow { bound, .. } => {
                    let gone: Vec<u64> = current
                        .iter()
                        .filter(|(_, v)| **v <= *bound)
                        .map(|(k, _)| *k)
                        .collect();
                    for k in gone {
                        writes.entry(k).or_default().push((index, None));
                        current.remove(&k);
                    }
                }
                Op::Read { .. } => {}
            }
        }
        for v in writes.values_mut() {
            v.sort();
        }
        Model { writes }
    }

    /// Value of `key` right after log index `i`.
    fn value_at(&self, key: u64, i: LogIndex) -> Option<i64> {
        self.writes
            .get(&key)
            .and_then(|w| w.iter().rev().find(|(idx, _)| *idx <= i))
            .and_then(|(_, v)| *v)
    }

    /// Whether `value` is the state of `key` at some index in `[lo, max_index]`.
    fn plausible(&self, key: u64, lo: LogIndex, value: Option<i64>) -> bool {
        if self.value_at(key, lo) == value {
            return true;
        }
        self.writes
            .get(&key)
            .is_some_and(|w| w.iter().any(|(idx, v)| *idx >= lo && *v == value))
    }
}

fn check(history: &[Record]) -> (usize, usize) {
    let model = Model::build(history);
    let mut checked = 0;
    let mut reads = 0;
    for r in history {
        let Op::Read {
            key,
            consistency,
            result: Some(result),
        } = &r.op
        else {
            continue;
        };
        reads += 1;
        // Lower bound on what the read must reflect.
        let lower = match consistency {
            Consistency::Linearizable => {
                // Every write to this key acknowledged before the read was called.
                history
                    .iter()
                    .filter_map(|w| match &w.op {
                        Op::Write {
                            key: k,
                            token: Some(t),
                            ..
                        }
                        | Op::Delete {
                            key: k,
                            token: Some(t),
                        } if k == key && w.ret <= r.call => Some(t.index),
                        Op::DeleteBelow { token: Some(t), .. } if w.ret <= r.call => Some(t.index),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(LogIndex(0))
            }
            Consistency::ReadYourWrites(t) => t.index,
            Consistency::Stale => LogIndex(0),
        };
        // In-flight writes to this key overlapping the read may or may not be visible.
        let in_flight: Vec<Option<i64>> = history
            .iter()
            .filter_map(|w| match &w.op {
                Op::Write {
                    key: k, version, ..
                } if k == key && w.call <= r.ret && w.ret >= r.call => Some(Some(*version)),
                Op::Delete { key: k, .. } if k == key && w.call <= r.ret && w.ret >= r.call => {
                    Some(None)
                }
                Op::DeleteBelow { .. } if w.call <= r.ret && w.ret >= r.call => Some(None),
                _ => None,
            })
            .collect();
        // A deletion by filter retried after an ambiguous failure may also have committed
        // earlier than its acknowledged entry, where it removed versions the acknowledged one
        // no longer sees: an absent result is then explained by any such deletion called
        // before the read returned whose bound covers the version the model expects.
        let filtered_away = result.is_none()
            && model.value_at(*key, lower).is_some_and(|v| {
                history.iter().any(|w| {
                    matches!(&w.op, Op::DeleteBelow { bound, .. } if *bound >= v) && w.call <= r.ret
                })
            });
        let ok =
            model.plausible(*key, lower, *result) || in_flight.contains(result) || filtered_away;
        assert!(
            ok,
            "client {} read key {key} at {:?}..{:?} with {consistency:?} got {result:?}; model lower bound {lower:?}: {:?}",
            r.client,
            r.call,
            r.ret,
            model.writes.get(key)
        );
        // Read-your-takedown: a promise-bearing read after an acknowledged delete never returns
        // a version written before that delete.
        if !matches!(consistency, Consistency::Stale)
            && let Some(v) = result
        {
            for w in history {
                if let Op::Delete {
                    key: k,
                    token: Some(t),
                } = &w.op
                    && k == key
                    && t.index >= lower.min(t.index)
                    && ((matches!(consistency, Consistency::Linearizable) && w.ret <= r.call)
                        || matches!(consistency, Consistency::ReadYourWrites(rt) if rt.index >= t.index))
                {
                    // The version must come from a write at or after the delete.
                    let after = model.writes.get(key).is_some_and(|ws| {
                        ws.iter()
                            .any(|(idx, val)| *idx > t.index && *val == Some(*v))
                    });
                    let inflight = in_flight.contains(&Some(*v));
                    assert!(
                        after || inflight,
                        "read-your-takedown violated: client {} read key {key} = {v} after delete at {:?}",
                        r.client,
                        t.index
                    );
                }
            }
        }
        // The same for deletions by filter: a promise-bearing read after one never returns a
        // version within its bound, unless that version was written again afterwards.
        if !matches!(consistency, Consistency::Stale)
            && let Some(v) = result
        {
            for w in history {
                if let Op::DeleteBelow {
                    bound,
                    token: Some(t),
                } = &w.op
                    && *v <= *bound
                    && ((matches!(consistency, Consistency::Linearizable) && w.ret <= r.call)
                        || matches!(consistency, Consistency::ReadYourWrites(rt) if rt.index >= t.index))
                {
                    let after = model.writes.get(key).is_some_and(|ws| {
                        ws.iter()
                            .any(|(idx, val)| *idx > t.index && *val == Some(*v))
                    });
                    let inflight = in_flight.contains(&Some(*v));
                    assert!(
                        after || inflight,
                        "read-your-takedown by filter violated: client {} read key {key} = {v} after deleting versions <= {bound} at {:?}",
                        r.client,
                        t.index
                    );
                }
            }
        }
        checked += 1;
    }
    (reads, checked)
}

fn run(seed: u64) -> (usize, usize, u64) {
    let mut cfg = SimConfig::default();
    cfg.net.drop_prob = 0.03;
    // Slow syncs on two thirds of the seeds (ADR 0027): up to 30 ms, or up to 150 ms (beyond
    // the 100 ms election timeout), so that asynchronous persistence interleaves with
    // elections, appends and crashes.
    let sync_extra = match seed % 3 {
        1 => Duration::from_millis(30),
        2 => Duration::from_millis(150),
        _ => Duration::ZERO,
    };
    cfg.disk.sync_extra_max = sync_extra;
    let (sim, mut ex) = Simulation::new(seed, cfg);
    let mut handles: Vec<ReplicaHandle> = Vec::new();
    for n in 1..=3u32 {
        let h = ex.block_on({
            let sim = sim.clone();
            let hh = ex.handle();
            async move {
                Replica::spawn(sim.runtime(NodeId(n), &hh), config(NodeId(n)), schema())
                    .await
                    .unwrap()
            }
        });
        handles.push(h);
    }
    let client_rt = sim.runtime(NodeId(9), &ex.handle());
    let history: History = Rc::new(RefCell::new(Vec::new()));
    let versions = Rc::new(RefCell::new(0i64));
    let stop = Rc::new(RefCell::new(false));
    for c in 0..3u32 {
        spawn_client(
            client_rt.clone(),
            handles.clone(),
            c,
            history.clone(),
            versions.clone(),
            stop.clone(),
        );
    }
    let mut rng = SeededRng::from_seed(seed ^ 0x5eed);
    let mut down: Option<(NodeId, u32)> = None;
    // Rounds of simulated time with a fault decision between rounds.
    for round in 0..40 {
        let d = Duration::from_millis(200 + rng.below(400));
        let h = ex.handle();
        ex.block_on(h.sleep(d));
        // Restart a crashed node after its downtime.
        if let Some((n, rounds_left)) = down {
            if rounds_left == 0 {
                let h = ex.block_on({
                    let sim = sim.clone();
                    let hh = ex.handle();
                    async move {
                        Replica::spawn(sim.runtime(n, &hh), config(n), schema())
                            .await
                            .unwrap()
                    }
                });
                handles[(n.get() - 1) as usize] = h;
                // Clients keep their old handle list; refresh by respawning them is unnecessary:
                // the old handle answers "stopped" and clients move on to another node.
                down = None;
            } else {
                down = Some((n, rounds_left - 1));
            }
        }
        let roll = rng.below(10);
        if roll < 2 && down.is_none() {
            let n = NodeId(1 + rng.below(3) as u32);
            sim.crash(n, &mut ex);
            down = Some((n, 1 + rng.below(3) as u32));
        } else if roll < 4 {
            // Partition one node both ways.
            let n = NodeId(1 + rng.below(3) as u32);
            for m in 1..=3u32 {
                if m != n.get() {
                    sim.block(n, NodeId(m));
                    sim.block(NodeId(m), n);
                }
            }
        } else {
            for a in 1..=3u32 {
                for b in 1..=3u32 {
                    sim.unblock(NodeId(a), NodeId(b));
                }
            }
        }
        let _ = round;
    }
    // Heal, restart, stop clients, let everything settle.
    for a in 1..=3u32 {
        for b in 1..=3u32 {
            sim.unblock(NodeId(a), NodeId(b));
        }
    }
    if let Some((n, _)) = down.take() {
        let h = ex.block_on({
            let sim = sim.clone();
            let hh = ex.handle();
            async move {
                Replica::spawn(sim.runtime(n, &hh), config(n), schema())
                    .await
                    .unwrap()
            }
        });
        handles[(n.get() - 1) as usize] = h;
    }
    *stop.borrow_mut() = true;
    let h = ex.handle();
    // 5 s. A replica that comes back late fetches the segments it missed (ADR 0021), several
    // at a time and with chunks pipelined; one at a time, seeds 16167, 20616, 40964 and 57668
    // needed up to 15 s.
    // Convergence takes longer when syncs are slow: every acknowledgement waits for one.
    ex.block_on(h.sleep(Duration::from_secs(5) + sync_extra * 100));
    // Convergence: same applied index and same documents everywhere.
    let statuses: Vec<_> = ex.block_on({
        let hs = handles.clone();
        async move {
            let mut v = Vec::new();
            for h in &hs {
                v.push(h.status().await.expect("alive"));
            }
            v
        }
    });
    let applied: Vec<LogIndex> = statuses.iter().map(|s| s.applied).collect();
    assert!(
        applied.iter().all(|a| *a == applied[0]),
        "seed {seed}: replicas did not converge: {statuses:#?}"
    );
    let docs: Vec<Vec<Option<Document>>> = ex.block_on({
        let hs = handles.clone();
        async move {
            let mut all = Vec::new();
            for h in &hs {
                let mut v = Vec::new();
                for k in (1..=KEYS).chain(KEYED_OFFSET + 1..=KEYED_OFFSET + KEYS) {
                    v.push(get_any(h, k, Consistency::Stale).await.unwrap());
                }
                all.push(v);
            }
            all
        }
    });
    assert!(
        docs.iter().all(|d| *d == docs[0]),
        "seed {seed}: replicas hold different documents: {docs:?}\nstatuses: {:?}",
        statuses
            .iter()
            .map(|s| (
                s.id,
                s.role,
                s.term,
                s.commit,
                s.applied,
                s.live_docs,
                s.segments.clone()
            ))
            .collect::<Vec<_>>()
    );
    // Flushes built and fetched since each replica's last restart (a lower bound for the run).
    FLUSHES.with(|f| {
        let mut t = f.get();
        for s in &statuses {
            t[0] += s.flushes[0];
            t[1] += s.flushes[1];
        }
        f.set(t);
    });
    // Segment lists (ADR 0021): the log decides flushes and merges, so replicas with nothing
    // left to install hold the same segments.
    if statuses.iter().all(|s| s.flushes[2] == 0) {
        assert!(
            statuses.iter().all(|s| s.segments == statuses[0].segments),
            "seed {seed}: replicas hold different segments: {:?}",
            statuses
                .iter()
                .map(|s| s.segments.clone())
                .collect::<Vec<_>>()
        );
    }
    let hist = history.borrow();
    let (reads, checked) = check(&hist);
    let writes = hist
        .iter()
        .filter(|r| {
            matches!(
                r.op,
                Op::Write { token: Some(_), .. }
                    | Op::Delete { token: Some(_), .. }
                    | Op::DeleteBelow { token: Some(_), .. }
            )
        })
        .count();
    // With syncs slower than an election timeout, acknowledged writes are scarce under the
    // injected faults: about 0.1% of these seeds acknowledge none (synchronous persistence:
    // about 1%). That starvation is an open liveness problem (ADR 0027); these seeds still
    // check every safety property on whatever they did.
    let least = if sync_extra > Duration::from_millis(100) {
        0
    } else {
        5
    };
    assert!(
        writes >= least && reads >= least,
        "seed {seed}: too little activity: {writes} writes, {reads} reads"
    );
    (reads, checked, sim.digest())
}

thread_local! {
    /// Campaign diagnostics: segments built locally and fetched from a leader (ADR 0016).
    static FLUSHES: std::cell::Cell<[u64; 2]> = const { std::cell::Cell::new([0, 0]) };
}

#[test]
fn signature_test_under_faults() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::ERROR)
        .try_init();
    let mut total_reads = 0;
    for seed in 0..12u64 {
        let (reads, checked, _) = run(seed);
        assert_eq!(reads, checked);
        total_reads += reads;
    }
    assert!(total_reads > 500);
}

/// Campaign runner: `CAIRN_SEEDS=a..b cargo test --release -p cairn-query --test chaos campaign -- --ignored --nocapture`.
#[test]
#[ignore]
fn campaign() {
    let range = std::env::var("CAIRN_SEEDS").expect("CAIRN_SEEDS=a..b");
    let (a, b) = range.split_once("..").expect("a..b");
    let (a, b): (u64, u64) = (a.parse().unwrap(), b.parse().unwrap());
    let mut reads = 0usize;
    let mut writes = 0usize;
    for seed in a..b {
        let r = std::panic::catch_unwind(|| run(seed));
        match r {
            Ok((rd, _, _)) => {
                reads += rd;
                writes += 1;
            }
            Err(_) => {
                eprintln!("CAMPAIGN FAIL seed={seed}");
                std::process::exit(2);
            }
        }
        if seed % 100 == 0 {
            eprintln!("campaign progress seed={seed}");
        }
    }
    let [built, fetched] = FLUSHES.with(|f| f.get());
    eprintln!(
        "CAMPAIGN OK seeds={}..{} runs={writes} reads_checked={reads} segments_built={built} segments_fetched={fetched}",
        a, b
    );
}

#[test]
fn debug_seed() {
    if let Ok(seed) = std::env::var("CAIRN_SEED") {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::INFO)
            .try_init();
        run(seed.parse().unwrap());
    }
}

#[test]
fn signature_test_is_deterministic() {
    let a = run(100);
    let b = run(100);
    assert_eq!(a.2, b.2, "same seed, different trace digest");
}
