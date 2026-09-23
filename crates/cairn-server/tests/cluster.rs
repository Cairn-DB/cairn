//! Real processes: writes, hybrid queries, takedowns, a killed and restarted node; and shard
//! placement with fewer replicas than nodes.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use bytes::Bytes;
use cairn_client::Client;
use cairn_core::{
    DocId, Document, FieldDef, FieldKind, HashMap, Metric, NodeId, Predicate, Schema, Value,
};
use cairn_query::{Consistency, Query, TextLeg, VectorLeg};
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

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
            name: "rights".into(),
            kind: FieldKind::Enum,
        },
        FieldDef {
            name: "blob".into(),
            kind: FieldKind::Blob,
        },
    ])
    .unwrap()
}

fn doc(i: u64) -> Document {
    let words = ["nuclear", "energy", "minister", "weather", "football"];
    Document::new(DocId(i), 4)
        .set(
            0,
            Value::Vector(
                (0..8)
                    .map(|d| ((i * 7 + d) % 13) as f32 / 13.0 + i as f32 / 1000.0)
                    .collect(),
            ),
        )
        .set(
            1,
            Value::Text(format!(
                "{} {} clip {i}",
                words[(i % 5) as usize],
                words[((i / 5) % 5) as usize]
            )),
        )
        .set(
            2,
            Value::Enum(if i % 4 == 0 {
                "restricted".into()
            } else {
                "cleared".into()
            }),
        )
        .set(3, Value::Blob(Bytes::from(vec![(i % 251) as u8; 64])))
}

struct Proc {
    child: Child,
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(id: u32, addrs: &[(u32, SocketAddr)], data: &Path, schema_path: &Path) -> Proc {
    start_with(id, addrs, data, schema_path, 4, 0)
}

fn start_with(
    id: u32,
    addrs: &[(u32, SocketAddr)],
    data: &Path,
    schema_path: &Path,
    shards: u32,
    replication: usize,
) -> Proc {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cairn-server"));
    cmd.arg("--node-id")
        .arg(id.to_string())
        .arg("--listen")
        .arg(addrs[(id - 1) as usize].1.to_string())
        .arg("--data")
        .arg(data.join(format!("node{id}")))
        .arg("--schema")
        .arg(schema_path)
        .arg("--shards")
        .arg(shards.to_string())
        .arg("--replication")
        .arg(replication.to_string())
        .arg("--cores")
        .arg("2")
        .arg("--memtable-bytes")
        .arg("20000")
        .arg("--tick-ms")
        .arg("20");
    for (i, a) in addrs {
        cmd.arg("--peer").arg(format!("{i}={a}"));
    }
    cmd.stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit());
    Proc {
        child: cmd.spawn().expect("spawn cairn-server"),
    }
}

fn wait_ready(client: &mut Client) {
    let t0 = Instant::now();
    loop {
        if let Ok(st) = client.status()
            && !st.is_empty()
        {
            return;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "node did not come up"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_applied_equal(clients: &mut [Client], shards: usize) {
    let t0 = Instant::now();
    loop {
        let mut all: Vec<Vec<u64>> = Vec::new();
        for c in clients.iter_mut() {
            if let Ok(st) = c.status()
                && st.len() == shards
            {
                let mut v: Vec<u64> = st.iter().map(|s| s.applied.get()).collect();
                v.sort_unstable();
                all.push(v);
            }
        }
        if all.len() == clients.len() && all.iter().all(|a| *a == all[0]) {
            return;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "replicas did not converge: {all:?}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn three_process_cluster() {
    let dir = std::env::temp_dir().join(format!("cairn-cluster-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let schema_path = dir.join("schema.json");
    std::fs::write(&schema_path, serde_json::to_vec(&schema()).unwrap()).unwrap();
    let addrs: Vec<(u32, SocketAddr)> = (1..=3).map(|i| (i, free_port())).collect();
    let mut procs: Vec<Option<Proc>> = addrs
        .iter()
        .map(|(i, _)| Some(start(*i, &addrs, &dir, &schema_path)))
        .collect();
    let map: HashMap<NodeId, SocketAddr> = addrs.iter().map(|(i, a)| (NodeId(*i), *a)).collect();
    let mut client = Client::new(map.clone());
    wait_ready(&mut client);
    std::thread::sleep(Duration::from_secs(2));
    for (i, a) in &addrs {
        let mut c = Client::new([(NodeId(*i), *a)].into_iter().collect());
        let st = c.status().unwrap();
        eprintln!(
            "node {i}: {:?}",
            st.iter()
                .map(|s| (
                    s.role,
                    s.term.get(),
                    s.leader.map(|l| l.get()),
                    s.applied.get()
                ))
                .collect::<Vec<_>>()
        );
    }

    // Writes through whichever node leads each shard.
    let docs: Vec<Document> = (1..=300).map(doc).collect();
    for chunk in docs.chunks(50) {
        client.upsert(chunk.to_vec()).unwrap();
    }
    // Read-your-writes point reads and a hybrid query.
    for i in [1u64, 150, 300] {
        assert_eq!(
            client.get(DocId(i), client.read_your_writes()).unwrap(),
            Some(doc(i))
        );
    }
    let mut q = Query::new(5);
    q.vectors.push(VectorLeg {
        field: 0,
        vector: doc(45).values[0]
            .clone()
            .map(|v| match v {
                Value::Vector(x) => x,
                _ => unreachable!(),
            })
            .unwrap(),
        ef: 0,
    });
    q.text = Some(TextLeg {
        field: 1,
        text: "nuclear".into(),
        all_terms: false,
    });
    q.filter = Predicate::Eq {
        field: 2,
        value: Value::Enum("cleared".into()),
    };
    q.with_documents = true;
    let hits = client.query(q.clone(), client.read_your_writes()).unwrap();
    assert_eq!(hits.len(), 5);
    assert!(
        hits.iter()
            .all(|h| h.document.as_ref().unwrap().values[2] == Some(Value::Enum("cleared".into())))
    );
    assert_eq!(
        hits[0].doc_id,
        DocId(45),
        "doc 45 scores in both legs: {:?}",
        hits.iter().map(|h| h.doc_id).collect::<Vec<_>>()
    );

    // Takedown: gone for read-your-writes reads immediately.
    client.delete(vec![DocId(45), DocId(150)]).unwrap();
    assert_eq!(
        client.get(DocId(45), client.read_your_writes()).unwrap(),
        None
    );
    let hits = client.query(q.clone(), client.read_your_writes()).unwrap();
    assert!(hits.iter().all(|h| h.doc_id != DocId(45)));

    // Kill node 2, keep writing, restart it, and check it converges and serves.
    let mut clients: Vec<Client> = (1..=3).map(|_| Client::new(map.clone())).collect();
    for (i, c) in clients.iter_mut().enumerate() {
        c.max_attempts = 3;
        let mut only = HashMap::default();
        only.insert(NodeId(i as u32 + 1), map[&NodeId(i as u32 + 1)]);
        *c = Client::new(only);
    }
    wait_applied_equal(&mut clients, 4);
    procs[1] = None;
    std::thread::sleep(Duration::from_millis(500));
    for i in 301..=400u64 {
        client.upsert(vec![doc(i)]).unwrap();
    }
    client.delete(vec![DocId(5)]).unwrap();
    procs[1] = Some(start(2, &addrs, &dir, &schema_path));
    let mut c2 = Client::new([(NodeId(2), map[&NodeId(2)])].into_iter().collect());
    wait_ready(&mut c2);
    wait_applied_equal(&mut clients, 4);
    assert_eq!(
        c2.get(DocId(400), Consistency::Stale).unwrap(),
        Some(doc(400))
    );
    assert_eq!(c2.get(DocId(5), Consistency::Stale).unwrap(), None);
    assert_eq!(c2.get(DocId(45), Consistency::Stale).unwrap(), None);
    drop(procs);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Four nodes, eight shards, three replicas per shard: every node hosts six shards, every
/// request works from every entry node (forwarded to a hosting node), and a killed node leaves
/// every shard with a majority.
#[test]
fn placement_with_fewer_replicas_than_nodes() {
    let dir = std::env::temp_dir().join(format!("cairn-placement-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let schema_path = dir.join("schema.json");
    std::fs::write(&schema_path, serde_json::to_vec(&schema()).unwrap()).unwrap();
    let addrs: Vec<(u32, SocketAddr)> = (1..=4).map(|i| (i, free_port())).collect();
    let mut procs: Vec<Option<Proc>> = addrs
        .iter()
        .map(|(i, _)| Some(start_with(*i, &addrs, &dir, &schema_path, 8, 3)))
        .collect();
    let map: HashMap<NodeId, SocketAddr> = addrs.iter().map(|(i, a)| (NodeId(*i), *a)).collect();
    let single = |i: u32| {
        let mut c = Client::new([(NodeId(i), map[&NodeId(i)])].into_iter().collect());
        c.max_attempts = 5;
        c
    };
    let mut client = Client::new(map.clone());
    wait_ready(&mut client);
    for i in 1..=4 {
        let mut c = single(i);
        wait_ready(&mut c);
        assert_eq!(c.status().unwrap().len(), 6, "node {i} hosts 6 of 8 shards");
    }
    std::thread::sleep(Duration::from_secs(2));

    let docs: Vec<Document> = (1..=300).map(doc).collect();
    for chunk in docs.chunks(50) {
        client.upsert(chunk.to_vec()).unwrap();
    }
    let tokens = client.tokens();
    let mut q = Query::new(5);
    q.vectors.push(VectorLeg {
        field: 0,
        vector: match doc(45).values[0].clone().unwrap() {
            Value::Vector(x) => x,
            _ => unreachable!(),
        },
        ef: 0,
    });
    // Every node answers point reads and queries for every shard, hosted or not.
    for i in 1..=4 {
        let mut c = single(i);
        for id in [1u64, 77, 150, 299] {
            assert_eq!(
                c.get(DocId(id), Consistency::Linearizable).unwrap(),
                Some(doc(id)),
                "node {i} doc {id}"
            );
        }
        let hits = c.query(q.clone(), Consistency::Linearizable).unwrap();
        assert_eq!(hits.len(), 5);
        assert_eq!(hits[0].doc_id, DocId(45), "node {i}");
    }
    // Read-your-writes with this client's tokens, entering through each node.
    for i in 1..=4 {
        let mut c = single(i);
        let hits = c
            .call(&cairn_proto::Request::Query {
                query: q.clone(),
                consistency: Consistency::ReadYourWrites(tokens[0]),
                tokens: tokens.clone(),
            })
            .unwrap();
        assert!(matches!(hits, cairn_proto::Response::Hits(h) if h.len() == 5));
    }

    // Takedown visible from every entry node.
    client.delete(vec![DocId(45)]).unwrap();
    for i in 1..=4 {
        let mut c = single(i);
        assert_eq!(c.get(DocId(45), Consistency::Linearizable).unwrap(), None);
        let hits = c.query(q.clone(), Consistency::Linearizable).unwrap();
        assert!(hits.iter().all(|h| h.doc_id != DocId(45)), "node {i}");
    }

    // Kill node 2: every shard keeps two of its three replicas, so writes and linearizable
    // reads continue through the surviving nodes.
    procs[1] = None;
    std::thread::sleep(Duration::from_secs(2));
    let mut survivors: HashMap<NodeId, SocketAddr> = map.clone();
    survivors.remove(&NodeId(2));
    let mut c = Client::new(survivors);
    for i in 301..=340u64 {
        c.upsert(vec![doc(i)]).unwrap();
    }
    for id in [1u64, 150, 320, 340] {
        assert_eq!(
            c.get(DocId(id), Consistency::Linearizable).unwrap(),
            Some(doc(id))
        );
    }
    assert_eq!(c.query(q, Consistency::Linearizable).unwrap().len(), 5);
    drop(procs);
    let _ = std::fs::remove_dir_all(&dir);
}
