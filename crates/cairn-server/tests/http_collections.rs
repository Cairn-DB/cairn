//! Collections (ADR 0031, section 4) against three real processes: create through one node and
//! use through the others, isolation from `default`, a node restarted alone gets its
//! collections back, drop deletes the files everywhere, and a recreated name starts empty.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

struct Proc(Child);

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

const ADMIN: &str = "test-admin-key-0123456789";

fn http(addr: SocketAddr, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
    let Ok(mut s) = TcpStream::connect(addr) else {
        return (0, Value::Null);
    };
    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let payload = body.map(|b| b.to_string()).unwrap_or_default();
    write!(
        s,
        "{method} {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\
         Authorization: Bearer {ADMIN}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    )
    .unwrap();
    let mut raw = String::new();
    if s.read_to_string(&mut raw).is_err() || raw.len() < 12 {
        return (0, Value::Null);
    }
    let status: u16 = raw[9..12].parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map(|x| x.1).unwrap_or("");
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

/// Retries a call until `ok` holds, for up to 30 s.
fn until(
    what: &str,
    mut f: impl FnMut() -> (u16, Value),
    ok: impl Fn(u16, &Value) -> bool,
) -> Value {
    let t0 = Instant::now();
    loop {
        let (st, v) = f();
        if ok(st, &v) {
            return v;
        }
        assert!(t0.elapsed() < Duration::from_secs(30), "{what}: {st} {v}");
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn default_schema() -> Value {
    json!({ "fields": [
        { "name": "embedding", "kind": { "Vector": { "dims": 4, "metric": "Cosine" } } },
        { "name": "text", "kind": "Text" },
    ] })
}

fn notes_schema() -> Value {
    json!({ "fields": [
        { "name": "embedding", "kind": { "Vector": { "dims": 2, "metric": "L2" } } },
        { "name": "body", "kind": "Text" },
        { "name": "parent", "kind": "Enum" },
    ] })
}

fn has_collection_files(dir: &Path, id: u32) -> Vec<PathBuf> {
    (1..=3)
        .map(|n| dir.join(format!("node{n}/c{id}")))
        .filter(|p| p.exists())
        .collect()
}

#[test]
fn collections_are_created_used_and_dropped_across_the_cluster() {
    let dir = std::env::temp_dir().join(format!("cairn-collections-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let schema_path = dir.join("schema.json");
    std::fs::write(&schema_path, default_schema().to_string()).unwrap();
    let raft: Vec<SocketAddr> = (0..3).map(|_| free_port()).collect();
    let web: Vec<SocketAddr> = (0..3).map(|_| free_port()).collect();
    let start = |i: usize| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_cairn-server"));
        cmd.args(["--node-id", &(i + 1).to_string()])
            .args(["--listen", &raft[i].to_string()])
            .args(["--http-listen", &web[i].to_string()])
            .arg("--data")
            .arg(dir.join(format!("node{}", i + 1)))
            .arg("--schema")
            .arg(&schema_path)
            .args(["--shards", "2", "--cores", "2", "--tick-ms", "20"])
            .args(["--memtable-bytes", "4000"])
            .env("CAIRN_HTTP_ADMIN_KEY", ADMIN)
            .env("RUST_LOG", "warn,cairn_server=info");
        for (j, a) in raft.iter().enumerate() {
            cmd.arg("--peer").arg(format!("{}={a}", j + 1));
        }
        let log = std::fs::File::create(dir.join(format!("node{}.log", i + 1))).unwrap();
        cmd.stdout(log.try_clone().unwrap()).stderr(log);
        Proc(cmd.spawn().expect("spawn"))
    };
    let mut procs: Vec<Option<Proc>> = (0..3).map(|i| Some(start(i))).collect();
    for w in &web {
        until(
            "up",
            || http(*w, "GET", "/v1/collections", None),
            |st, v| st == 200 && v["collections"][0]["name"] == "default",
        );
    }

    // Create through node 1 (answered once every shard has a leader).
    let body = json!({ "name": "notes", "schema": notes_schema(), "shards": 3 });
    let (st, created) = http(web[0], "POST", "/v1/collections", Some(&body));
    assert_eq!(st, 201, "{created}");
    assert_eq!(
        created,
        json!({ "name": "notes", "shards": 3, "schema": notes_schema() })
    );
    let (st, _) = http(web[1], "POST", "/v1/collections", Some(&body));
    assert_eq!(st, 409, "same name twice");
    for bad in [
        json!({ "name": "default", "schema": notes_schema() }),
        json!({ "name": "Bad Name", "schema": notes_schema() }),
        json!({ "name": "x", "schema": { "fields": [{ "name": "_tenant", "kind": "Enum" }] } }),
        json!({ "name": "x", "schema": notes_schema(), "shards": 0 }),
    ] {
        let (st, e) = http(web[2], "POST", "/v1/collections", Some(&bad));
        assert_eq!(st, 400, "{bad}: {e}");
    }
    // Every node lists it and serves it.
    for w in &web {
        let v = until(
            "listed",
            || http(*w, "GET", "/v1/collections", None),
            |st, v| st == 200 && v["collections"].as_array().is_some_and(|a| a.len() == 2),
        );
        assert_eq!(v["collections"][1]["name"], "notes");
        let (st, sch) = http(*w, "GET", "/v1/collections/notes/schema", None);
        assert_eq!((st, sch), (200, notes_schema()));
    }

    // The same ids in `default` and `notes`: two documents, with their own schemas.
    let notes: Vec<Value> = (0..12)
        .map(|i| json!({ "id": format!("n{i}"), "embedding": [i as f32, 1.0], "body": format!("otter note {i}"), "parent": format!("p{}", i % 3) }))
        .collect();
    let (st, ack) = http(
        web[1],
        "POST",
        "/v1/collections/notes/documents",
        Some(&json!({ "documents": notes })),
    );
    assert_eq!(st, 200, "{ack}");
    let t1 = ack["consistency_token"].as_str().unwrap().to_owned();
    let (st, ack) = http(
        web[1],
        "POST",
        "/v1/documents",
        Some(
            &json!({ "documents": [{ "id": "n1", "embedding": [1.0, 0.0, 0.0, 0.0], "text": "otter in default" }] }),
        ),
    );
    assert_eq!(st, 200, "{ack}");
    let t2 = ack["consistency_token"].as_str().unwrap().to_owned();
    let after = format!("{t1},{t2}");
    for w in &web {
        let (st, d) = http(
            *w,
            "GET",
            &format!("/v1/collections/notes/documents/n1?after={after}"),
            None,
        );
        assert_eq!((st, d["body"].as_str()), (200, Some("otter note 1")));
        let (st, d) = http(*w, "GET", &format!("/v1/documents/n1?after={after}"), None);
        assert_eq!((st, d["text"].as_str()), (200, Some("otter in default")));
        let (st, res) = http(
            *w,
            "POST",
            "/v1/collections/notes/search",
            Some(
                &json!({ "k": 50, "text": { "field": "body", "query": "otter" }, "after": after }),
            ),
        );
        assert_eq!(
            (st, res["hits"].as_array().map_or(0, Vec::len)),
            (200, 12),
            "{res}"
        );
        let (_, res) = http(
            *w,
            "POST",
            "/v1/search",
            Some(
                &json!({ "k": 50, "text": { "field": "text", "query": "otter" }, "after": after }),
            ),
        );
        assert_eq!(res["hits"].as_array().map_or(0, Vec::len), 1, "{res}");
    }
    // The collection's own schema is enforced; an unknown collection is a 404.
    let (st, _) = http(
        web[0],
        "POST",
        "/v1/collections/notes/documents",
        Some(&json!({ "documents": [{ "id": "z", "text": "wrong schema" }] })),
    );
    assert_eq!(st, 400);
    let (st, _) = http(web[0], "GET", "/v1/collections/nope/documents/1", None);
    assert_eq!(st, 404);
    // Deletion by parent inside the collection.
    let (st, ack) = http(
        web[2],
        "POST",
        "/v1/collections/notes/documents/delete",
        Some(&json!({ "filter": { "field": "parent", "eq": "p0" }, "after": after })),
    );
    assert_eq!((st, &ack["deleted"]), (200, &json!(4)), "{ack}");
    let after = ack["consistency_token"].as_str().unwrap().to_owned();

    // Node 3 restarts alone: it starts its replicas of `notes` again from its catalog replica.
    procs[2] = None;
    procs[2] = Some(start(2));
    let res = until(
        "restarted node serves notes",
        || {
            http(
                web[2],
                "POST",
                "/v1/collections/notes/search",
                Some(
                    &json!({ "k": 50, "consistency": "stale", "text": { "field": "body", "query": "otter" } }),
                ),
            )
        },
        |st, v| st == 200 && v["hits"].as_array().is_some_and(|a| a.len() == 8),
    );
    assert!(
        res["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|h| h["id"] != "n0")
    );
    let (st, d) = http(
        web[2],
        "GET",
        &format!("/v1/collections/notes/documents/n2?after={after}"),
        None,
    );
    assert_eq!((st, d["parent"].as_str()), (200, Some("p2")));
    assert!(!has_collection_files(&dir, 1).is_empty());

    // Drop: gone from every node, and its files deleted everywhere.
    let (st, _) = http(web[0], "DELETE", "/v1/collections/default", None);
    assert_eq!(st, 400);
    let (st, v) = http(web[1], "DELETE", "/v1/collections/notes", None);
    assert_eq!((st, v), (200, json!({ "dropped": "notes" })));
    for w in &web {
        until(
            "dropped",
            || http(*w, "GET", "/v1/collections", None),
            |st, v| st == 200 && v["collections"].as_array().is_some_and(|a| a.len() == 1),
        );
    }
    let t0 = Instant::now();
    while !has_collection_files(&dir, 1).is_empty() {
        assert!(
            t0.elapsed() < Duration::from_secs(20),
            "files left: {:?}",
            has_collection_files(&dir, 1)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let (st, _) = http(web[2], "GET", "/v1/collections/notes/documents/n1", None);
    assert_eq!(st, 404);
    // `default` is untouched.
    let (st, d) = http(web[0], "GET", &format!("/v1/documents/n1?after={t2}"), None);
    assert_eq!((st, d["text"].as_str()), (200, Some("otter in default")));

    // The name can be used again: a new collection, empty, with new files.
    let (st, created) = http(
        web[2],
        "POST",
        "/v1/collections",
        Some(&json!({ "name": "notes", "schema": notes_schema(), "shards": 1 })),
    );
    assert_eq!((st, &created["shards"]), (201, &json!(1)), "{created}");
    let (st, res) = http(
        web[0],
        "POST",
        "/v1/collections/notes/search",
        Some(&json!({ "k": 50 })),
    );
    assert_eq!((st, &res["hits"]), (200, &json!([])), "{res}");
    let t0 = Instant::now();
    while has_collection_files(&dir, 2).len() < 3 {
        assert!(
            t0.elapsed() < Duration::from_secs(20),
            "new collection's files"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    drop(procs);
    let _ = std::fs::remove_dir_all(&dir);
}
