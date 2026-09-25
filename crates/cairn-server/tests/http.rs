//! HTTP/JSON API (ADR 0023) against three real processes: writes, reads with the consistency
//! token, hybrid search with filters, takedowns never read back through any node, input
//! errors, and a killed node.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use cairn_core::{FieldDef, FieldKind, Metric, Schema};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
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

/// One HTTP/1.1 request with `Connection: close`; returns the status and the JSON body.
fn http(addr: SocketAddr, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let payload = body.map(|b| b.to_string()).unwrap_or_default();
    write!(
        s,
        "{method} {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    )
    .unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).unwrap();
    let status: u16 = raw[9..12].parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map(|x| x.1).unwrap_or("");
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

fn schema() -> Schema {
    Schema::new(vec![
        FieldDef {
            name: "embedding".into(),
            kind: FieldKind::Vector {
                dims: 4,
                metric: Metric::Cosine,
            },
        },
        FieldDef {
            name: "text".into(),
            kind: FieldKind::Text,
        },
        FieldDef {
            name: "source".into(),
            kind: FieldKind::Enum,
        },
        FieldDef {
            name: "created".into(),
            kind: FieldKind::Date,
        },
    ])
    .unwrap()
}

fn doc(i: u64) -> Value {
    let words = ["nuclear energy", "weather report", "football match"];
    json!({
        "id": i,
        "embedding": [1.0, (i % 7) as f32, (i % 3) as f32, 0.5],
        "text": format!("{} number {i}", words[(i % 3) as usize]),
        "source": if i % 2 == 0 { "tv" } else { "radio" },
        "created": 19_000 + i as i64,
    })
}

#[test]
fn http_api_end_to_end() {
    let dir = std::env::temp_dir().join(format!("cairn-http-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let schema_path = dir.join("schema.json");
    std::fs::write(&schema_path, serde_json::to_vec(&schema()).unwrap()).unwrap();
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
            .args(["--shards", "3", "--cores", "2", "--tick-ms", "20"])
            .args(["--memtable-bytes", "4000"]);
        for (j, a) in raft.iter().enumerate() {
            cmd.arg("--peer").arg(format!("{}={a}", j + 1));
        }
        cmd.stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        Proc(cmd.spawn().expect("spawn"))
    };
    let mut procs: Vec<Option<Proc>> = (0..3).map(|i| Some(start(i))).collect();
    // Up: every HTTP port answers and every node sees its 3 replicas.
    let t0 = Instant::now();
    for w in &web {
        loop {
            if let Ok(s) = std::panic::catch_unwind(|| http(*w, "GET", "/v1/status", None))
                && s.0 == 200
                && s.1["replicas"].as_array().map_or(0, Vec::len) == 3
            {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(30),
                "HTTP did not come up"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    assert_eq!(
        http(web[0], "GET", "/health", None),
        (200, json!({ "status": "ok" }))
    );
    let (st, sch) = http(web[1], "GET", "/v1/schema", None);
    assert_eq!(st, 200);
    assert_eq!(sch["fields"].as_array().unwrap().len(), 4);

    // Writes through node 1, read back through node 2 with the token.
    let docs: Vec<Value> = (1..=60).map(doc).collect();
    let (st, ack) = http(
        web[0],
        "POST",
        "/v1/documents",
        Some(&json!({ "documents": docs })),
    );
    assert_eq!(st, 200, "{ack}");
    assert_eq!(ack["count"].as_u64().unwrap(), 60);
    let token = ack["consistency_token"].as_str().unwrap().to_owned();
    let (st, d) = http(
        web[1],
        "GET",
        &format!("/v1/documents/7?after={token}"),
        None,
    );
    assert_eq!(st, 200);
    assert_eq!(d, doc(7));
    let (st, _) = http(web[2], "GET", "/v1/documents/7", None); // linearizable by default
    assert_eq!(st, 200);

    // Hybrid search through node 3: vector + text + filter.
    let search = |node: usize, after: &str| {
        http(
            web[node],
            "POST",
            "/v1/search",
            Some(&json!({
                "k": 10,
                "vector": { "field": "embedding", "values": [1.0, 0.0, 1.0, 0.5] },
                "text": { "field": "text", "query": "nuclear" },
                "filter": { "and": [
                    { "field": "source", "eq": "tv" },
                    { "field": "created", "gte": 19_010 }
                ] },
                "after": after,
            })),
        )
    };
    let (st, res) = search(2, &token);
    assert_eq!(st, 200, "{res}");
    let hits = res["hits"].as_array().unwrap();
    assert!(!hits.is_empty());
    for h in hits {
        let d = &h["document"];
        assert_eq!(d["source"], "tv");
        assert!(d["created"].as_i64().unwrap() >= 19_010);
        assert_eq!(h["legs"].as_array().unwrap().len(), 2);
    }
    let first = hits[0]["id"].as_u64().unwrap();

    // A pure filter (no legs) spans every shard and returns matches in id order.
    let (st, res) = http(
        web[1],
        "POST",
        "/v1/search",
        Some(
            &json!({ "k": 100, "filter": { "field": "source", "eq": "tv" },
                      "with_documents": false, "after": token }),
        ),
    );
    assert_eq!(st, 200, "{res}");
    let ids: Vec<u64> = res["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["id"].as_u64().unwrap())
        .collect();
    assert_eq!(ids, (1..=60).filter(|i| i % 2 == 0).collect::<Vec<u64>>());
    assert!(
        res["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|h| h["legs"] == json!([]))
    );
    let (st, res) = http(
        web[2],
        "POST",
        "/v1/search",
        Some(&json!({ "k": 5, "after": token })),
    );
    assert_eq!(st, 200);
    assert_eq!(res["hits"].as_array().unwrap().len(), 5);

    // Takedown through node 2: gone through every node for a client holding the token.
    let (st, ack) = http(
        web[1],
        "DELETE",
        &format!("/v1/documents/{first}?after={token}"),
        None,
    );
    assert_eq!(st, 200);
    let token2 = ack["consistency_token"].as_str().unwrap().to_owned();
    for w in &web {
        let (st, _) = http(
            *w,
            "GET",
            &format!("/v1/documents/{first}?after={token2}"),
            None,
        );
        assert_eq!(st, 404, "taken-down document read back");
    }
    for n in 0..3 {
        let (st, res) = search(n, &token2);
        assert_eq!(st, 200);
        assert!(
            res["hits"]
                .as_array()
                .unwrap()
                .iter()
                .all(|h| h["id"].as_u64() != Some(first)),
            "taken-down document returned by search on node {}",
            n + 1
        );
    }
    let (st, _) = http(
        web[0],
        "POST",
        "/v1/documents/delete",
        Some(&json!({ "ids": [1, 2, 3] })),
    );
    assert_eq!(st, 200);

    // Input errors are 400 with a message; a missing document is 404.
    for body in [
        json!({ "documents": [{ "id": 1, "nope": 1 }] }),
        json!({ "documents": [{ "id": 1, "embedding": [1.0] }] }),
        json!({ "documents": [{ "embedding": [1.0, 2.0, 3.0, 4.0] }] }),
        json!({ "documents": [] }),
    ] {
        let (st, e) = http(web[0], "POST", "/v1/documents", Some(&body));
        assert_eq!(st, 400, "{body}");
        assert!(e["error"].is_string());
    }
    let (st, _) = http(web[0], "GET", "/v1/documents/7?consistency=eventual", None);
    assert_eq!(st, 400);
    let (st, _) = http(web[0], "GET", "/v1/documents/999999", None);
    assert_eq!(st, 404);
    let (st, _) = http(
        web[0],
        "POST",
        "/v1/search",
        Some(&json!({ "vector": { "field": "text", "values": [1.0] } })),
    );
    assert_eq!(st, 400);

    // Node 1 dies: writes and reads through the others keep working.
    procs[0] = None;
    let (st, ack) = http(
        web[1],
        "POST",
        "/v1/documents",
        Some(&json!({ "documents": [doc(100)] })),
    );
    assert_eq!(st, 200, "{ack}");
    let t = ack["consistency_token"].as_str().unwrap().to_owned();
    let (st, d) = http(web[2], "GET", &format!("/v1/documents/100?after={t}"), None);
    assert_eq!((st, d), (200, doc(100)));
    drop(procs);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A CA and the certificate of node 1 (`node-1.cairn`), as PEM files in `dir`.
fn write_node_cert(dir: &std::path::Path) {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    p.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::DigitalSignature,
    ];
    let ca = p.self_signed(&ca_key).unwrap();
    std::fs::write(dir.join("ca.pem"), ca.pem()).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let mut p = rcgen::CertificateParams::new(vec!["node-1.cairn".to_owned()]).unwrap();
    p.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let cert = p.signed_by(&key, &ca, &ca_key).unwrap();
    std::fs::write(dir.join("node1.pem"), cert.pem()).unwrap();
    std::fs::write(dir.join("node1.key"), key.serialize_pem()).unwrap();
}

/// With mutual TLS, the plaintext HTTP port is refused unless explicitly allowed; once allowed,
/// the HTTP layer reaches the node over mTLS with the node's own certificate.
#[test]
fn http_next_to_mtls_needs_explicit_consent() {
    let dir = std::env::temp_dir().join(format!("cairn-http-tls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write_node_cert(&dir);
    let schema_path = dir.join("schema.json");
    std::fs::write(&schema_path, serde_json::to_vec(&schema()).unwrap()).unwrap();
    let (raft, web) = (free_port(), free_port());
    let cmd = |allow: bool| {
        let mut c = Command::new(env!("CARGO_BIN_EXE_cairn-server"));
        c.args(["--node-id", "1", "--listen", &raft.to_string()])
            .args(["--peer", &format!("1={raft}")])
            .args(["--http-listen", &web.to_string()])
            .arg("--data")
            .arg(dir.join("node1"))
            .arg("--schema")
            .arg(&schema_path)
            .arg("--tls-ca")
            .arg(dir.join("ca.pem"))
            .arg("--tls-cert")
            .arg(dir.join("node1.pem"))
            .arg("--tls-key")
            .arg(dir.join("node1.key"))
            .args(["--shards", "2", "--cores", "2", "--tick-ms", "20"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if allow {
            c.arg("--http-allow-plaintext");
        }
        c
    };
    let out = cmd(false).output().unwrap();
    assert!(
        !out.status.success(),
        "plaintext HTTP next to mTLS was accepted"
    );
    let _p = Proc(cmd(true).spawn().unwrap());
    let t0 = Instant::now();
    let ack = loop {
        if let Ok((200, ack)) = std::panic::catch_unwind(|| {
            http(
                web,
                "POST",
                "/v1/documents",
                Some(&json!({ "documents": [doc(5)] })),
            )
        }) {
            break ack;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "HTTP over mTLS did not work"
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    let t = ack["consistency_token"].as_str().unwrap().to_owned();
    assert_eq!(
        http(web, "GET", &format!("/v1/documents/5?after={t}"), None),
        (200, doc(5))
    );
    drop(_p);
    let _ = std::fs::remove_dir_all(&dir);
}
