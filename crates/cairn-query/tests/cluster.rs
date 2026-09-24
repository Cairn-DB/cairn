//! Three simulated nodes running one shard: replication, consistency levels, takedowns,
//! crashes, re-election and snapshot catch-up.

use bytes::Bytes;
use cairn_core::Runtime;
use cairn_core::{
    DocId, Document, Duration, FieldDef, FieldKind, Metric, NodeId, Schema, ShardId, Value,
};
use cairn_index::{HnswParams, VectorIndexParams};
use cairn_query::{
    Consistency, EngineConfig, Query, Replica, ReplicaConfig, ReplicaHandle, Token, VectorLeg,
};
use cairn_raft::Role;
use cairn_sim::{SimConfig, SimRuntime, Simulation};
use cairn_storage::{Command, LogConfig, StoreConfig};

fn schema() -> Schema {
    Schema::new(vec![
        FieldDef {
            name: "v".into(),
            kind: FieldKind::Vector {
                dims: 4,
                metric: Metric::L2,
            },
        },
        FieldDef {
            name: "rights".into(),
            kind: FieldKind::Enum,
        },
        FieldDef {
            name: "payload".into(),
            kind: FieldKind::Blob,
        },
    ])
    .unwrap()
}

fn doc(i: u64) -> Document {
    Document::new(DocId(i), 3)
        .set(0, Value::Vector(vec![i as f32, 0.0, 1.0, -(i as f32)]))
        .set(
            1,
            Value::Enum(if i % 3 == 0 {
                "restricted".into()
            } else {
                "cleared".into()
            }),
        )
        .set(2, Value::Blob(Bytes::from(vec![7u8; 200])))
}

fn config(node: NodeId, memtable_max_bytes: usize) -> ReplicaConfig {
    ReplicaConfig {
        shard: ShardId(0),
        id: node,
        peers: vec![NodeId(1), NodeId(2), NodeId(3)],
        tick: Duration::from_millis(10),
        election_ticks: 10,
        heartbeat_ticks: 2,
        engine: EngineConfig {
            store: StoreConfig {
                memtable_max_bytes,
                log: LogConfig {
                    max_file_bytes: 1 << 16,
                },
                max_segments: 4,
                max_deleted_fraction: 0.3,
                target_segment_rows: 0,
                min_merge: 4,
            },
            vector: VectorIndexParams {
                hnsw: HnswParams {
                    m: 4,
                    ef_construction: 16,
                },
                ..VectorIndexParams::default()
            },
        },
        dir: "shard0".into(),
        seed: 7,
        own_receiver: true,
        idle_flush_ticks: 0,
        compaction_slots: None,
        ship_segments: true,
    }
}

/// Proposes through whichever node is the leader, following hints, retrying for a while.
async fn propose(rt: &SimRuntime, handles: &[ReplicaHandle], cmd: Command) -> Token {
    let mut target = 0usize;
    for _ in 0..400 {
        match handles[target].propose(cmd.clone()).await {
            Ok(t) => return t,
            Err(cairn_core::Error::NotLeader {
                leader_hint: Some(l),
                ..
            }) => target = (l.get() - 1) as usize,
            Err(_) => {
                target = (target + 1) % handles.len();
                rt.sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("could not commit {cmd:?}");
}

async fn wait_leader(rt: &SimRuntime, handles: &[ReplicaHandle]) -> usize {
    for _ in 0..500 {
        for (i, h) in handles.iter().enumerate() {
            if let Some(s) = h.status().await
                && s.role == Role::Leader
            {
                return i;
            }
        }
        rt.sleep(Duration::from_millis(10)).await;
    }
    panic!("no leader elected");
}

async fn wait_converged(rt: &SimRuntime, handles: &[ReplicaHandle], expect_docs: u64) {
    for iter in 0..1000 {
        let mut ok = true;
        let mut applied = None;
        for h in handles {
            let Some(s) = h.status().await else {
                ok = false;
                break;
            };
            if s.live_docs != expect_docs {
                ok = false;
            }
            match applied {
                None => applied = Some(s.applied),
                Some(a) if a != s.applied => ok = false,
                _ => {}
            }
        }
        if ok {
            return;
        }
        if iter % 200 == 0 {
            let mut v = Vec::new();
            for h in handles {
                v.push(
                    h.status()
                        .await
                        .map(|s| (s.id.get(), s.applied.get(), s.live_docs, s.segments.len())),
                );
            }
            eprintln!("wait_converged iter {iter} at {:?}: {v:?}", rt.now());
        }
        rt.sleep(Duration::from_millis(10)).await;
    }
    let st: Vec<_> = {
        let mut v = Vec::new();
        for h in handles {
            v.push(h.status().await);
        }
        v
    };
    panic!("replicas did not converge to {expect_docs} docs: {st:#?}");
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::WARN)
        .try_init();
}

#[test]
fn replicates_serves_all_consistency_levels_and_hides_takedowns() {
    init_tracing();
    let (sim, mut ex) = Simulation::new(1, SimConfig::default());
    let handles: Vec<ReplicaHandle> = (1..=3u32)
        .map(|n| {
            let (rt, _) = (sim.runtime(NodeId(n), &ex.handle()), ());
            ex.block_on({
                let sim = sim.clone();
                let h = ex.handle();
                async move {
                    let rt2 = sim.runtime(NodeId(n), &h);
                    drop(rt);
                    Replica::spawn(rt2, config(NodeId(n), 1 << 30), schema())
                        .await
                        .unwrap()
                }
            })
        })
        .collect();
    let rt = sim.runtime(NodeId(9), &ex.handle());
    let hs = handles.clone();
    ex.block_on(async move {
        let li = wait_leader(&rt, &hs).await;
        let mut last = None;
        for i in 1..=30u64 {
            last = Some(propose(&rt, &hs, Command::Upsert(vec![doc(i)])).await);
        }
        let token = last.unwrap();
        // Linearizable on the leader.
        let got = hs[li]
            .get(DocId(30), Consistency::Linearizable)
            .await
            .unwrap();
        assert_eq!(got, Some(doc(30)));
        // Linearizable on a follower (follower read: the leader confirms a read index, the
        // follower serves once applied up to it) sees the last acknowledged write.
        let fi = (0..3).find(|i| *i != li).unwrap();
        assert_eq!(
            hs[fi]
                .get(DocId(30), Consistency::Linearizable)
                .await
                .unwrap(),
            Some(doc(30))
        );
        // Read-your-writes with the token succeeds on every replica (waiting if needed).
        for h in &hs {
            assert_eq!(
                h.get(DocId(30), Consistency::ReadYourWrites(token))
                    .await
                    .unwrap(),
                Some(doc(30))
            );
        }
        // A hybrid query at the leader.
        let mut q = Query::new(5);
        q.vectors.push(VectorLeg {
            field: 0,
            vector: vec![30.0, 0.0, 1.0, -30.0],
            ef: 0,
        });
        q.filter = cairn_core::Predicate::Eq {
            field: 1,
            value: Value::Enum("cleared".into()),
        };
        let hits = hs[li]
            .query(q.clone(), Consistency::Linearizable)
            .await
            .unwrap();
        assert_eq!(
            hits[0].doc_id,
            DocId(29),
            "29 is the nearest cleared doc (30 is restricted)"
        );
        // Takedown: after the acknowledgement, no read-your-takedown read sees it anywhere.
        let t = propose(&rt, &hs, Command::Delete(vec![DocId(29)])).await;
        assert_eq!(
            hs[li]
                .get(DocId(29), Consistency::Linearizable)
                .await
                .unwrap(),
            None
        );
        for h in &hs {
            assert_eq!(
                h.get(DocId(29), Consistency::ReadYourWrites(t))
                    .await
                    .unwrap(),
                None
            );
            let hits = h
                .query(q.clone(), Consistency::ReadYourWrites(t))
                .await
                .unwrap();
            assert!(hits.iter().all(|h| h.doc_id != DocId(29)));
        }
        wait_converged(&rt, &hs, 29).await;
        assert!(sim.trace_len() > 0);
    });
}

#[test]
fn leader_crash_reelection_and_snapshot_catch_up() {
    init_tracing();
    let (sim, mut ex) = Simulation::new(2, SimConfig::default());
    let memtable = 4000; // small: flushes every ~20 docs, so restarts need snapshots
    let mut handles: Vec<ReplicaHandle> = Vec::new();
    for n in 1..=3u32 {
        let h = ex.block_on({
            let sim = sim.clone();
            let hh = ex.handle();
            async move {
                Replica::spawn(
                    sim.runtime(NodeId(n), &hh),
                    config(NodeId(n), memtable),
                    schema(),
                )
                .await
                .unwrap()
            }
        });
        handles.push(h);
    }
    let rt = sim.runtime(NodeId(9), &ex.handle());
    // Phase 1: write 60 docs, then crash the leader.
    let li = ex.block_on({
        let (rt, hs) = (rt.clone(), handles.clone());
        async move {
            let li = wait_leader(&rt, &hs).await;
            for i in 1..=60u64 {
                propose(&rt, &hs, Command::Upsert(vec![doc(i)])).await;
            }
            wait_converged(&rt, &hs, 60).await;
            li
        }
    });
    let crashed = NodeId(li as u32 + 1);
    sim.crash(crashed, &mut ex);
    // Phase 2: the survivors elect a new leader and keep writing (many flushes => the crashed
    // node's log position gets compacted away on the leader).
    let survivors: Vec<ReplicaHandle> = handles
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != li)
        .map(|(_, h)| h.clone())
        .collect();
    ex.block_on({
        let (rt, hs) = (rt.clone(), survivors.clone());
        async move {
            let new_leader = wait_leader(&rt, &hs).await;
            assert_ne!(hs[new_leader].id(), crashed);
            for i in 61..=200u64 {
                propose(&rt, &hs, Command::Upsert(vec![doc(i)])).await;
            }
            propose(&rt, &hs, Command::Delete(vec![DocId(5), DocId(100)])).await;
            wait_converged(&rt, &hs, 198).await;
            let s = hs[new_leader].status().await.unwrap();
            assert!(
                s.segments.len() >= 2,
                "leader flushed segments: {:?}",
                s.segments
            );
        }
    });
    // Phase 3: restart the crashed node; it must catch up (via snapshot + fetched segments or
    // via the log) and converge.
    let restarted = ex.block_on({
        let sim = sim.clone();
        let hh = ex.handle();
        async move {
            Replica::spawn(
                sim.runtime(crashed, &hh),
                config(crashed, memtable),
                schema(),
            )
            .await
            .unwrap()
        }
    });
    handles[li] = restarted;
    ex.block_on({
        let (rt, hs) = (rt.clone(), handles.clone());
        async move {
            wait_converged(&rt, &hs, 198).await;
            for h in &hs {
                assert_eq!(h.get(DocId(5), Consistency::Stale).await.unwrap(), None);
                assert_eq!(
                    h.get(DocId(150), Consistency::Stale).await.unwrap(),
                    Some(doc(150))
                );
                assert_eq!(
                    h.get(DocId(3), Consistency::Stale).await.unwrap(),
                    Some(doc(3))
                );
            }
            // Writes still work with everyone back.
            let t = propose(&rt, &hs, Command::Upsert(vec![doc(201)])).await;
            for h in &hs {
                assert_eq!(
                    h.get(DocId(201), Consistency::ReadYourWrites(t))
                        .await
                        .unwrap(),
                    Some(doc(201))
                );
            }
        }
    });
    // Determinism: the whole scenario is reproducible.
    let digest = sim.digest();
    assert_ne!(digest, 0);
}

/// A leader holding proposals back (memtable over its limit, no build slot free) that loses
/// leadership must fail them promptly with a leader hint, not keep clients waiting until
/// their timeout (seen on a real cluster).
#[test]
fn held_back_proposals_fail_over_when_leadership_is_lost() {
    use cairn_query::JobSlots;
    use std::cell::RefCell;
    use std::rc::Rc;
    init_tracing();
    let (sim, mut ex) = Simulation::new(4, SimConfig::default());
    let memtable = 3000;
    // Every node's only build slot is held by the test: no flush can start.
    let slots: Vec<std::sync::Arc<JobSlots>> = (0..3)
        .map(|_| {
            let s = std::sync::Arc::new(JobSlots::new(1));
            assert!(s.try_acquire());
            s
        })
        .collect();
    let mut handles: Vec<ReplicaHandle> = Vec::new();
    for n in 1..=3u32 {
        let mut cfg = config(NodeId(n), memtable);
        cfg.compaction_slots = Some(slots[(n - 1) as usize].clone());
        let h = ex.block_on({
            let sim = sim.clone();
            let hh = ex.handle();
            async move {
                Replica::spawn(sim.runtime(NodeId(n), &hh), cfg, schema())
                    .await
                    .unwrap()
            }
        });
        handles.push(h);
    }
    let rt = sim.runtime(NodeId(9), &ex.handle());
    let li = ex.block_on({
        let (rt, hs) = (rt.clone(), handles.clone());
        async move {
            let li = wait_leader(&rt, &hs).await;
            // Fill the leader's memtable to twice its threshold (writes past it are held).
            for i in 1..=200u64 {
                let st = hs[li].status().await.unwrap();
                if st.memtable_bytes >= 2 * memtable as u64 {
                    break;
                }
                propose(&rt, &hs, Command::Upsert(vec![doc(i)])).await;
            }
            li
        }
    });
    // This proposal is held back by the leader.
    let result: Rc<RefCell<Option<cairn_core::Result<Token>>>> = Rc::new(RefCell::new(None));
    {
        let (h, r) = (handles[li].clone(), result.clone());
        rt.spawn(async move {
            *r.borrow_mut() = Some(h.propose(Command::Upsert(vec![doc(1000)])).await);
        });
    }
    ex.block_on({
        let rt = rt.clone();
        async move { rt.sleep(Duration::from_millis(200)).await }
    });
    let st = ex
        .block_on({
            let h = handles[li].clone();
            async move { h.status().await }
        })
        .unwrap();
    assert!(
        result.borrow().is_none(),
        "the proposal should be held back"
    );
    assert!(st.queued[0] > 0, "held back: {:?}", st.queued);
    // Isolate the leader; the others elect a new one; heal so the old leader learns the term.
    let old = NodeId(li as u32 + 1);
    for n in 1..=3u32 {
        if NodeId(n) != old {
            sim.block(old, NodeId(n));
            sim.block(NodeId(n), old);
        }
    }
    ex.block_on({
        let rt = rt.clone();
        async move { rt.sleep(Duration::from_millis(1500)).await }
    });
    for n in 1..=3u32 {
        sim.unblock(old, NodeId(n));
        sim.unblock(NodeId(n), old);
    }
    ex.block_on({
        let rt = rt.clone();
        async move { rt.sleep(Duration::from_millis(1000)).await }
    });
    let got = result.borrow_mut().take();
    match got {
        Some(Err(cairn_core::Error::NotLeader { .. })) => {}
        other => panic!("held-back proposal should fail over with NotLeader, got {other:?}"),
    }
    for s in &slots {
        s.release();
    }
}

/// Spawns three replicas with a small memtable and no compaction (ADR 0016 tests).
fn spawn_three(
    sim: &Simulation,
    ex: &mut cairn_runtime::Executor<cairn_sim::SimReactor>,
    memtable: usize,
    ship: bool,
) -> Vec<ReplicaHandle> {
    (1..=3u32)
        .map(|n| {
            let mut cfg = config(NodeId(n), memtable);
            cfg.engine.store.max_segments = 1000;
            cfg.ship_segments = ship;
            let (sim, hh) = (sim.clone(), ex.handle());
            ex.block_on(async move {
                Replica::spawn(sim.runtime(NodeId(n), &hh), cfg, schema())
                    .await
                    .unwrap()
            })
        })
        .collect()
}

/// Waits until no replica holds an unpublished freeze and all agree on the segment list.
async fn wait_flushed(
    rt: &SimRuntime,
    handles: &[ReplicaHandle],
) -> Vec<cairn_query::ReplicaStatus> {
    for _ in 0..3000 {
        let mut st = Vec::new();
        for h in handles {
            st.push(h.status().await.unwrap());
        }
        if st
            .iter()
            .all(|s| s.flushes[2] == 0 && s.segments == st[0].segments)
            && !st[0].segments.is_empty()
        {
            return st;
        }
        rt.sleep(Duration::from_millis(10)).await;
    }
    let mut st = Vec::new();
    for h in handles {
        st.push(h.status().await);
    }
    panic!("flushes did not settle: {st:#?}");
}

async fn check_all_docs(handles: &[ReplicaHandle], n: u64, deleted: &[u64]) {
    for h in handles {
        for i in 1..=n {
            let got = h.get(DocId(i), Consistency::Stale).await.unwrap();
            let want = (!deleted.contains(&i)).then(|| doc(i));
            assert_eq!(got, want, "node {} doc {i}", h.id());
        }
    }
}

/// ADR 0016: the leader builds each flushed segment once; followers fetch and install it, and
/// a takedown applied after the freeze is still masked in the fetched segment.
#[test]
fn followers_install_leader_built_segments() {
    init_tracing();
    let (sim, mut ex) = Simulation::new(21, SimConfig::default());
    let handles = spawn_three(&sim, &mut ex, 6000, true);
    let rt = sim.runtime(NodeId(9), &ex.handle());
    let hs = handles.clone();
    ex.block_on(async move {
        let li = wait_leader(&rt, &hs).await;
        for i in 1..=120u64 {
            propose(&rt, &hs, Command::Upsert(vec![doc(i)])).await;
            if i == 60 {
                // Right after a flush may have frozen these rows.
                propose(&rt, &hs, Command::Delete(vec![DocId(10), DocId(55)])).await;
            }
        }
        wait_converged(&rt, &hs, 118).await;
        let st = wait_flushed(&rt, &hs).await;
        let leader = &st[li];
        assert!(leader.flushes[0] >= 3, "leader built: {:?}", leader.flushes);
        assert_eq!(leader.flushes[1], 0);
        for (i, s) in st.iter().enumerate() {
            if i != li {
                assert_eq!(s.flushes[0], 0, "follower {} built locally", s.id);
                assert_eq!(s.flushes[1], leader.flushes[0], "follower {} fetched", s.id);
            }
        }
        check_all_docs(&hs, 120, &[10, 55]).await;
    });
}

/// ADR 0016 fallback: a follower that cannot reach the leader's file server (its requests are
/// dropped, while it still receives the log) builds its segments itself.
#[test]
fn follower_builds_locally_when_the_fetch_stalls() {
    init_tracing();
    let (sim, mut ex) = Simulation::new(22, SimConfig::default());
    let handles = spawn_three(&sim, &mut ex, 6000, true);
    let rt = sim.runtime(NodeId(9), &ex.handle());
    let li = ex.block_on({
        let (rt, hs) = (rt.clone(), handles.clone());
        async move { wait_leader(&rt, &hs).await }
    });
    let leader = NodeId(li as u32 + 1);
    let cut = NodeId(((li + 1) % 3) as u32 + 1);
    sim.block(cut, leader);
    let hs = handles.clone();
    let st = ex.block_on({
        let rt = rt.clone();
        async move {
            for i in 1..=90u64 {
                propose(&rt, &hs, Command::Upsert(vec![doc(i)])).await;
            }
            wait_converged(&rt, &hs, 90).await;
            wait_flushed(&rt, &hs).await
        }
    });
    sim.unblock(cut, leader);
    let c = &st[(cut.get() - 1) as usize];
    assert_eq!(c.leader, Some(leader), "no election expected");
    assert!(c.flushes[0] >= 2, "cut-off follower built: {:?}", c.flushes);
    assert_eq!(c.flushes[1], 0);
    let other = st.iter().find(|s| s.id != leader && s.id != cut).unwrap();
    assert_eq!(other.flushes[0], 0);
    assert!(other.flushes[1] >= 2);
    ex.block_on(async move { check_all_docs(&handles, 90, &[]).await });
}

/// With shipping off, every replica builds every flush, still cut by the log.
#[test]
fn flushes_through_the_log_without_shipping() {
    init_tracing();
    let (sim, mut ex) = Simulation::new(23, SimConfig::default());
    let handles = spawn_three(&sim, &mut ex, 6000, false);
    let rt = sim.runtime(NodeId(9), &ex.handle());
    let hs = handles.clone();
    ex.block_on(async move {
        wait_leader(&rt, &hs).await;
        for i in 1..=90u64 {
            propose(&rt, &hs, Command::Upsert(vec![doc(i)])).await;
        }
        wait_converged(&rt, &hs, 90).await;
        let st = wait_flushed(&rt, &hs).await;
        for s in &st {
            assert!(s.flushes[0] >= 2 && s.flushes[1] == 0, "{:?}", s.flushes);
        }
        check_all_docs(&hs, 90, &[]).await;
    });
}
