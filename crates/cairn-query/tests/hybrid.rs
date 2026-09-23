//! End-to-end hybrid queries against a naive reference, in the simulator.

use cairn_core::{
    DocId, Document, FieldDef, FieldKind, HashSet, Metric, NodeId, Predicate, Schema, SeededRng,
    Value,
};
use cairn_index::{HnswParams, VectorIndexParams};
use cairn_query::{EngineConfig, Fusion, Query, ShardEngine, TextLeg, VectorLeg};
use cairn_sim::{SimConfig, Simulation};
use cairn_storage::{Command, LogConfig, StoreConfig};

const WORDS: [&str; 12] = [
    "minister", "nuclear", "energy", "weather", "football", "election", "archive", "clip",
    "speech", "river", "market", "school",
];

fn schema() -> Schema {
    Schema::new(vec![
        FieldDef {
            name: "img".into(),
            kind: FieldKind::Vector {
                dims: 8,
                metric: Metric::L2,
            },
        },
        FieldDef {
            name: "title".into(),
            kind: FieldKind::Text,
        },
        FieldDef {
            name: "channel".into(),
            kind: FieldKind::Enum,
        },
        FieldDef {
            name: "year".into(),
            kind: FieldKind::I64,
        },
        FieldDef {
            name: "rights".into(),
            kind: FieldKind::Enum,
        },
    ])
    .unwrap()
}

fn doc(r: &mut SeededRng, id: u64) -> Document {
    let words: Vec<&str> = (0..2 + r.below(5))
        .map(|_| WORDS[r.below(12) as usize])
        .collect();
    let mut d = Document::new(DocId(id), 5)
        .set(
            0,
            Value::Vector((0..8).map(|_| r.unit_f64() as f32 * 2.0 - 1.0).collect()),
        )
        .set(2, Value::Enum(format!("ch{}", r.below(4))))
        .set(3, Value::I64(1990 + r.below(30) as i64))
        .set(
            4,
            Value::Enum(if r.chance(0.4) {
                "cleared".into()
            } else {
                "restricted".into()
            }),
        );
    if !r.chance(0.1) {
        d = d.set(1, Value::Text(words.join(" ")));
    }
    d.validate(&schema()).unwrap();
    d
}

fn disk_cfg(memtable_max_bytes: usize) -> EngineConfig {
    let mut c = cfg(memtable_max_bytes);
    c.vector.disk = true;
    c.vector.vamana = cairn_index::diskann::VamanaParams {
        r: 16,
        l_build: 32,
        pq_sample: 2_000,
        ..Default::default()
    };
    c
}

fn cfg(memtable_max_bytes: usize) -> EngineConfig {
    EngineConfig {
        store: StoreConfig {
            memtable_max_bytes,
            log: LogConfig {
                max_file_bytes: 1 << 20,
            },
            max_segments: 4,
            max_deleted_fraction: 0.3,
            target_segment_rows: 0,
            min_merge: 4,
        },
        vector: VectorIndexParams {
            hnsw: HnswParams {
                m: 8,
                ef_construction: 50,
            },
            ..VectorIndexParams::default()
        },
    }
}

fn filter(r: &mut SeededRng) -> Predicate {
    match r.below(4) {
        0 => Predicate::True,
        1 => Predicate::Eq {
            field: 4,
            value: Value::Enum("cleared".into()),
        },
        2 => Predicate::And(vec![
            Predicate::Eq {
                field: 4,
                value: Value::Enum("cleared".into()),
            },
            Predicate::Range {
                field: 3,
                lo: Some(Value::I64(1995)),
                hi: Some(Value::I64(2005)),
                lo_inclusive: true,
                hi_inclusive: true,
            },
        ]),
        _ => Predicate::In {
            field: 2,
            values: vec![Value::Enum("ch1".into()), Value::Enum("ch3".into())],
        },
    }
}

fn queries(r: &mut SeededRng, n: usize) -> Vec<Query> {
    (0..n)
        .map(|i| {
            let mut q = Query::new(10);
            q.exact = true;
            q.filter = filter(r);
            let kind = i % 4;
            if kind == 0 || kind == 2 {
                q.vectors.push(VectorLeg {
                    field: 0,
                    vector: (0..8).map(|_| r.unit_f64() as f32 * 2.0 - 1.0).collect(),
                    ef: 0,
                });
            }
            if kind == 1 || kind == 2 {
                q.text = Some(TextLeg {
                    field: 1,
                    text: format!(
                        "{} {}",
                        WORDS[r.below(12) as usize],
                        WORDS[r.below(12) as usize]
                    ),
                    all_terms: r.chance(0.3),
                });
            }
            if r.chance(0.3) {
                q.fusion = Fusion::Weighted {
                    weights: vec![1.0, 2.0],
                };
            }
            q
        })
        .collect()
}

fn ids(hits: &[cairn_query::Hit]) -> Vec<u64> {
    hits.iter().map(|h| h.doc_id.get()).collect()
}

#[test]
fn single_segment_matches_reference_exactly() {
    let (sim, mut ex) = Simulation::new(1, SimConfig::default());
    let rt = sim.runtime(NodeId(1), &ex.handle());
    ex.block_on(async move {
        let mut r = SeededRng::from_seed(11);
        let docs: Vec<Document> = (1..=400).map(|i| doc(&mut r, i)).collect();
        let mut engine = ShardEngine::open(rt.clone(), "shard", schema(), cfg(1 << 30))
            .await
            .unwrap();
        engine.write(&Command::Upsert(docs.clone())).await.unwrap();
        engine.flush().await.unwrap();
        assert_eq!(engine.store().segments().count(), 1);
        for (qi, q) in queries(&mut r, 40).iter().enumerate() {
            let got = engine.query(q).await.unwrap();
            let want = ShardEngine::<cairn_sim::SimRuntime>::reference(&schema(), &docs, q);
            let got_ids = ids(&got);
            let want_ids: Vec<u64> = want.iter().map(|d| d.get()).collect();
            assert_eq!(got_ids, want_ids, "query {qi}: {q:?}");
            assert!(got.windows(2).all(|w| w[0].score >= w[1].score));
        }
        // Filter-only and document fetch.
        let mut q = Query::new(5);
        q.filter = Predicate::Eq {
            field: 2,
            value: Value::Enum("ch2".into()),
        };
        q.with_documents = true;
        let got = engine.query(&q).await.unwrap();
        assert_eq!(got.len(), 5);
        for h in &got {
            assert_eq!(
                h.document.as_ref().unwrap().values[2],
                Some(Value::Enum("ch2".into()))
            );
        }
        // Invalid queries are rejected.
        let mut bad = Query::new(5);
        bad.vectors.push(VectorLeg {
            field: 1,
            vector: vec![0.0; 8],
            ef: 0,
        });
        assert!(engine.query(&bad).await.is_err());
        let mut bad = Query::new(5);
        bad.filter = Predicate::Eq {
            field: 1,
            value: Value::Text("x".into()),
        };
        assert!(engine.query(&bad).await.is_err());
    });
}

async fn check(
    engine: &mut ShardEngine<cairn_sim::SimRuntime>,
    model: &[Document],
    deleted_set: &HashSet<u64>,
    r: &mut SeededRng,
) {
    check_with(engine, model, deleted_set, r, true).await;
}

/// `exact`: vector-only queries must match the reference ranking exactly; otherwise (PQ
/// scoring) at least 80% of the reference results must come back.
async fn check_with(
    engine: &mut ShardEngine<cairn_sim::SimRuntime>,
    model: &[Document],
    deleted_set: &HashSet<u64>,
    r: &mut SeededRng,
    exact: bool,
) {
    for (qi, q) in queries(r, 40).iter().enumerate() {
        let got = engine.query(q).await.unwrap();
        let got_ids = ids(&got);
        assert!(
            got_ids.iter().all(|i| !deleted_set.contains(i)),
            "query {qi}: deleted doc returned"
        );
        for h in &got {
            let d = model
                .iter()
                .find(|m| m.id == h.doc_id)
                .expect("hit is a live doc");
            assert!(q.filter.matches(d), "query {qi}: filter violated");
        }
        let want = ShardEngine::<cairn_sim::SimRuntime>::reference(&schema(), model, q);
        let want_ids: Vec<u64> = want.iter().map(|d| d.get()).collect();
        if q.text.is_none() && exact {
            // Vector legs merge exactly across segments; text legs use per-segment statistics,
            // so only the vector/filter-only cases are exact.
            assert_eq!(got_ids, want_ids, "query {qi}: {q:?}");
        } else if q.text.is_none() {
            let overlap = got_ids.iter().filter(|i| want_ids.contains(i)).count();
            assert!(
                overlap * 5 >= want_ids.len() * 4,
                "query {qi}: vector overlap {overlap}/{}",
                want_ids.len()
            );
        } else {
            let overlap = got_ids.iter().filter(|i| want_ids.contains(i)).count();
            assert!(
                overlap * 2 >= want_ids.len().min(got_ids.len()),
                "query {qi}: text overlap {overlap}/{}",
                want_ids.len()
            );
        }
    }
}

#[test]
fn multi_segment_with_memtable_and_takedowns() {
    let (sim, mut ex) = Simulation::new(2, SimConfig::default());
    let rt = sim.runtime(NodeId(1), &ex.handle());
    ex.block_on(async move {
        let mut r = SeededRng::from_seed(12);
        let mut model: Vec<Document> = Vec::new();
        // Small memtable: many flushes and compactions along the way.
        let mut engine = ShardEngine::open(rt.clone(), "shard", schema(), cfg(6000))
            .await
            .unwrap();
        for i in 1..=600u64 {
            let d = doc(&mut r, i);
            model.push(d.clone());
            engine.write(&Command::Upsert(vec![d])).await.unwrap();
        }
        assert!(engine.store().segments().count() >= 2);
        assert!(
            !engine.store().memtable().is_empty(),
            "memtable should hold a tail"
        );
        // Upsert some existing docs with new content, delete others.
        let updated: Vec<Document> = (1..=30).map(|i| doc(&mut r, i * 7)).collect();
        for d in &updated {
            model.retain(|m| m.id != d.id);
            model.push(d.clone());
        }
        engine.write(&Command::Upsert(updated)).await.unwrap();
        let deleted: Vec<DocId> = (1..=40).map(|i| DocId(i * 13)).collect();
        model.retain(|m| !deleted.contains(&m.id));
        engine
            .write(&Command::Delete(deleted.clone()))
            .await
            .unwrap();
        let deleted_set: HashSet<u64> = deleted.iter().map(|d| d.get()).collect();
        check(&mut engine, &model, &deleted_set, &mut r).await;
        engine.flush().await.unwrap();
        check(&mut engine, &model, &deleted_set, &mut r).await;
        drop(engine);
        let mut engine = ShardEngine::open(rt.clone(), "shard", schema(), cfg(6000))
            .await
            .unwrap();
        check(&mut engine, &model, &deleted_set, &mut r).await;
    });
}

#[test]
fn disk_resident_segments_with_memtable_and_takedowns() {
    let (sim, mut ex) = Simulation::new(3, SimConfig::default());
    let rt = sim.runtime(NodeId(1), &ex.handle());
    ex.block_on(async move {
        let mut r = SeededRng::from_seed(13);
        let mut model: Vec<Document> = Vec::new();
        let mut engine = ShardEngine::open(rt.clone(), "shard", schema(), disk_cfg(6000))
            .await
            .unwrap();
        for i in 1..=600u64 {
            let d = doc(&mut r, i);
            model.push(d.clone());
            engine.write(&Command::Upsert(vec![d])).await.unwrap();
        }
        assert!(engine.store().segments().count() >= 2);
        let updated: Vec<Document> = (1..=30).map(|i| doc(&mut r, i * 7)).collect();
        for d in &updated {
            model.retain(|m| m.id != d.id);
            model.push(d.clone());
        }
        engine.write(&Command::Upsert(updated)).await.unwrap();
        let deleted: Vec<DocId> = (1..=40).map(|i| DocId(i * 13)).collect();
        model.retain(|m| !deleted.contains(&m.id));
        engine
            .write(&Command::Delete(deleted.clone()))
            .await
            .unwrap();
        let deleted_set: HashSet<u64> = deleted.iter().map(|d| d.get()).collect();
        check_with(&mut engine, &model, &deleted_set, &mut r, false).await;
        engine.flush().await.unwrap();
        check_with(&mut engine, &model, &deleted_set, &mut r, false).await;
        drop(engine);
        // Reopen: segments load their Vamana sections through Disk::map.
        let mut engine = ShardEngine::open(rt.clone(), "shard", schema(), disk_cfg(6000))
            .await
            .unwrap();
        let ids: Vec<_> = engine.store().segments().map(|m| m.id).collect();
        assert!(!ids.is_empty());
        for id in ids {
            let view = engine.store().segment(id).unwrap();
            assert!(
                view.reader.has_section("vamana.0"),
                "segment {id:?} is disk-resident"
            );
            assert!(!view.reader.has_section("hnsw.0"));
        }
        check_with(&mut engine, &model, &deleted_set, &mut r, false).await;
    });
}
