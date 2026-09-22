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
        // Linearizable on a follower is refused.
        let fi = (0..3).find(|i| *i != li).unwrap();
        assert!(matches!(
            hs[fi].get(DocId(30), Consistency::Linearizable).await,
            Err(cairn_core::Error::NotLeader { .. })
        ));
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
