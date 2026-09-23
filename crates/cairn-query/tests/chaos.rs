//! The Phase 3 signature test: clients read and write a 3-node shard while the simulator
//! injects partitions, message drops, crashes and restarts. Afterwards every read is checked
//! against a per-key model with real-time bounds, read-your-writes tokens are honoured, no
//! read that promises read-your-takedown returns a removed document, and the replicas converge.

use bytes::Bytes;
use cairn_core::Runtime;
use cairn_core::{
    DocId, Document, Duration, FieldDef, FieldKind, HashMap, Instant, LogIndex, NodeId, Schema,
    SeededRng, ShardId, Value,
};
use cairn_index::{HnswParams, VectorIndexParams};
use cairn_query::{Consistency, EngineConfig, Replica, ReplicaConfig, ReplicaHandle, Token};
use cairn_sim::{SimConfig, SimRuntime, Simulation};
use cairn_storage::{Command, LogConfig, StoreConfig};
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
}

fn doc(key: u64, version: i64) -> Document {
    Document::new(DocId(key), 3)
        .set(0, Value::Vector(vec![key as f32, version as f32]))
        .set(1, Value::I64(version))
        .set(2, Value::Blob(Bytes::from(vec![1u8; 120])))
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
                memtable_max_bytes: 3000,
                log: LogConfig {
                    max_file_bytes: 1 << 14,
                },
                max_segments: 3,
                max_deleted_fraction: 0.3,
                target_segment_rows: 0,
                min_merge: 4,
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
        match handles[target].get(DocId(key), c).await {
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
            let key = 1 + rng.below(KEYS);
            let start = rng.below(handles.len() as u64) as usize;
            let call = r.now();
            let roll = rng.below(10);
            if roll < 4 {
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
                let token = propose_retrying(
                    &r,
                    &handles,
                    Command::Upsert(vec![doc(key, version)]),
                    start,
                )
                .await;
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
            } else if roll < 5 {
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
                let token =
                    propose_retrying(&r, &handles, Command::Delete(vec![DocId(key)]), start).await;
                if let Some(t) = token {
                    last_token = Some(last_token.map_or(t, |p| p.max(t)));
                }
                let mut h = history.borrow_mut();
                h[pos].ret = r.now();
                h[pos].op = Op::Delete { key, token };
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
        let mut writes: HashMap<u64, Vec<(LogIndex, Option<i64>)>> = HashMap::default();
        for r in history {
            match &r.op {
                Op::Write {
                    key,
                    version,
                    token: Some(t),
                } => {
                    writes
                        .entry(*key)
                        .or_default()
                        .push((t.index, Some(*version)));
                }
                Op::Delete {
                    key,
                    token: Some(t),
                } => {
                    writes.entry(*key).or_default().push((t.index, None));
                }
                _ => {}
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
                _ => None,
            })
            .collect();
        let ok = model.plausible(*key, lower, *result) || in_flight.contains(result);
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
        checked += 1;
    }
    (reads, checked)
}

fn run(seed: u64) -> (usize, usize, u64) {
    let mut cfg = SimConfig::default();
    cfg.net.drop_prob = 0.03;
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
    ex.block_on(h.sleep(Duration::from_secs(5)));
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
    let docs: Vec<Vec<Option<i64>>> = ex.block_on({
        let hs = handles.clone();
        async move {
            let mut all = Vec::new();
            for h in &hs {
                let mut v = Vec::new();
                for k in 1..=KEYS {
                    v.push(
                        h.get(DocId(k), Consistency::Stale)
                            .await
                            .unwrap()
                            .map(|d| version_of(&d)),
                    );
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
    let hist = history.borrow();
    let (reads, checked) = check(&hist);
    let writes = hist
        .iter()
        .filter(|r| {
            matches!(
                r.op,
                Op::Write { token: Some(_), .. } | Op::Delete { token: Some(_), .. }
            )
        })
        .count();
    assert!(
        writes >= 5 && reads >= 5,
        "seed {seed}: too little activity: {writes} writes, {reads} reads"
    );
    (reads, checked, sim.digest())
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
    eprintln!(
        "CAMPAIGN OK seeds={}..{} runs={writes} reads_checked={reads}",
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
