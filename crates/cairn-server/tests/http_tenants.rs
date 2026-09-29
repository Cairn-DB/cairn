//! Tenants (ADR 0031) against a real process: a tenant-scoped key never reads, searches,
//! lists or deletes another tenant's document, even with the same ids, guessed ids or hostile
//! filters; an unscoped key acts for a tenant through the `Cairn-Tenant` header and erases one.
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
            name: "n".into(),
            kind: FieldKind::I64,
        },
    ])
    .unwrap()
}

/// Plain HTTP request with an optional key and tenant header.
fn http(
    addr: SocketAddr,
    key: &str,
    tenant: Option<&str>,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let payload = body.map(|b| b.to_string()).unwrap_or_default();
    let tenant = tenant.map_or(String::new(), |t| format!("Cairn-Tenant: {t}\r\n"));
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Authorization: Bearer {key}\r\n{tenant}Content-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{payload}",
        payload.len()
    );
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    s.write_all(req.as_bytes()).unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).unwrap();
    let status: u16 = raw[9..12].parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map(|x| x.1).unwrap_or("");
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

/// `cairn-server keygen`, with extra arguments.
fn keygen(args: &[&str]) -> (String, Value) {
    let out = Command::new(env!("CARGO_BIN_EXE_cairn-server"))
        .arg("keygen")
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    (v["key"].as_str().unwrap().to_owned(), v["entry"].clone())
}

fn doc(id: Value, text: &str, n: i64) -> Value {
    json!({ "id": id, "embedding": [1.0, n as f32, 0.5, 0.0], "text": text, "n": n })
}

fn sorted(mut v: Vec<Value>) -> Vec<Value> {
    v.sort_by_key(|x| x.to_string());
    v
}

fn ids(res: &Value) -> Vec<Value> {
    let mut v: Vec<Value> = res["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("no hits: {res}"))
        .iter()
        .map(|h| h["id"].clone())
        .collect();
    v.sort_by_key(|x| x.to_string());
    v
}

#[test]
fn tenants_are_isolated_by_their_keys() {
    let dir = std::env::temp_dir().join(format!("cairn-tenants-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    // A tenant-scoped key cannot hold the admin role.
    let refused = Command::new(env!("CARGO_BIN_EXE_cairn-server"))
        .args(["keygen", "x", "admin", "--tenant", "acme"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    let (acme, e1) = keygen(&["acme-app", "read,write,takedown", "--tenant", "acme"]);
    let (globex, e2) = keygen(&["globex-app", "read,write,takedown", "--tenant", "globex"]);
    let (backend, e3) = keygen(&["backend", "read,write,takedown"]);
    let keys_path = dir.join("keys.json");
    std::fs::write(&keys_path, json!({ "keys": [e1, e2, e3] }).to_string()).unwrap();
    let schema_path = dir.join("schema.json");
    std::fs::write(&schema_path, serde_json::to_vec(&schema()).unwrap()).unwrap();
    let (raft, web) = (free_port(), free_port());
    let log = std::fs::File::create(dir.join("stderr.log")).unwrap();
    let _proc = Proc(
        Command::new(env!("CARGO_BIN_EXE_cairn-server"))
            .args(["--node-id", "1", "--listen", &raft.to_string()])
            .args(["--peer", &format!("1={raft}")])
            .args(["--http-listen", &web.to_string()])
            .arg("--data")
            .arg(dir.join("node1"))
            .arg("--schema")
            .arg(&schema_path)
            .arg("--http-keys")
            .arg(&keys_path)
            .args(["--shards", "3", "--cores", "2", "--tick-ms", "20"])
            .env_remove("CAIRN_HTTP_ADMIN_KEY")
            .env_remove("RUST_LOG")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    );
    let call = |key: &str, tenant: Option<&str>, m: &str, p: &str, b: Option<&Value>| {
        http(web, key, tenant, m, p, b)
    };
    let t0 = Instant::now();
    while TcpStream::connect(web).is_err() {
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "HTTP did not come up"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // The same ids in two tenants and without a tenant: three different documents.
    let write = |key: &str, tenant: Option<&str>, who: &str| {
        let body = json!({ "documents": [
            doc(json!(1), &format!("{who} one shared"), 1),
            doc(json!("doc-a"), &format!("{who} alpha shared"), 2),
            doc(json!(format!("{who}-only")), &format!("{who} private shared"), 3),
        ] });
        let t0 = Instant::now();
        loop {
            let (st, ack) = call(key, tenant, "POST", "/v1/documents", Some(&body));
            if st == 200 {
                return ack["consistency_token"].as_str().unwrap().to_owned();
            }
            assert!(t0.elapsed() < Duration::from_secs(30), "write: {st} {ack}");
            std::thread::sleep(Duration::from_millis(200));
        }
    };
    let t1 = write(&acme, None, "acme");
    let t2 = write(&globex, None, "globex");
    let t3 = write(&backend, None, "plain");
    let after = [t1, t2, t3].join(",");

    // Reads: each key sees its own document under the shared ids.
    for (key, who) in [(&acme, "acme"), (&globex, "globex"), (&backend, "plain")] {
        for (path, text) in [
            ("1", format!("{who} one shared")),
            ("doc-a", format!("{who} alpha shared")),
        ] {
            let (st, d) = call(
                key,
                None,
                "GET",
                &format!("/v1/documents/{path}?after={after}"),
                None,
            );
            assert_eq!(
                (st, d["text"].as_str()),
                (200, Some(text.as_str())),
                "{who}"
            );
            assert!(d.get("_tenant").is_none(), "{d}");
        }
    }
    // Another tenant's own id is not reachable, even guessed.
    let (st, _) = call(
        &acme,
        None,
        "GET",
        &format!("/v1/documents/globex-only?after={after}"),
        None,
    );
    assert_eq!(st, 404);
    // A scoped key cannot switch tenants; an unscoped one can, through the header.
    let (st, _) = call(&acme, Some("globex"), "GET", "/v1/documents/1", None);
    assert_eq!(st, 403);
    let (st, _) = call(
        &acme,
        Some("acme"),
        "GET",
        &format!("/v1/documents/1?after={after}"),
        None,
    );
    assert_eq!(st, 200);
    let (st, d) = call(
        &backend,
        Some("globex"),
        "GET",
        &format!("/v1/documents/doc-a?after={after}"),
        None,
    );
    assert_eq!((st, d["text"].as_str()), (200, Some("globex alpha shared")));
    let (st, _) = call(
        &backend,
        Some("bad tenant!"),
        "GET",
        "/v1/documents/1",
        None,
    );
    assert_eq!(st, 400);

    // Searches: pure filter, text and vector, with hostile filters.
    let everything = json!({ "k": 100, "filter": { "field": "n", "gte": 0 }, "after": after });
    let (_, res) = call(&acme, None, "POST", "/v1/search", Some(&everything));
    assert_eq!(
        ids(&res),
        sorted(vec![json!(1), json!("acme-only"), json!("doc-a")])
    );
    let text = json!({ "k": 100, "text": { "field": "text", "query": "shared" }, "after": after });
    let (_, res) = call(&globex, None, "POST", "/v1/search", Some(&text));
    assert_eq!(
        ids(&res),
        sorted(vec![json!(1), json!("doc-a"), json!("globex-only")])
    );
    assert!(res["hits"].as_array().unwrap().iter().all(|h| {
        h["document"]["text"]
            .as_str()
            .unwrap()
            .starts_with("globex")
    }));
    let vector = json!({ "k": 100, "vector": { "field": "embedding", "values": [1.0, 1.0, 0.5, 0.0] },
                         "filter": { "or": [{ "field": "n", "is_null": true }, { "not": { "field": "n", "is_null": true } }] },
                         "after": after });
    let (_, res) = call(&acme, None, "POST", "/v1/search", Some(&vector));
    assert_eq!(ids(&res).len(), 3);
    for hostile in [
        json!({ "field": "_tenant", "eq": "globex" }),
        json!({ "or": [{ "field": "_tenant", "is_null": false }] }),
        json!({ "field": "_key", "is_null": false }),
    ] {
        let (st, _) = call(
            &acme,
            None,
            "POST",
            "/v1/search",
            Some(&json!({ "k": 10, "filter": hostile })),
        );
        assert_eq!(st, 400, "{hostile}");
    }
    // Unscoped, without a header: every document, a tenant's ones marked with their tenant.
    let (_, res) = call(&backend, None, "POST", "/v1/search", Some(&everything));
    let hits = res["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 9);
    assert_eq!(hits.iter().filter(|h| h["_tenant"] == "acme").count(), 3);
    assert_eq!(
        hits.iter().filter(|h| h.get("_tenant").is_none()).count(),
        3
    );

    // Deletions: by id and by filter, each within its own tenant.
    let (st, ack) = call(&acme, None, "DELETE", "/v1/documents/1", None);
    assert_eq!(st, 200, "{ack}");
    let t = ack["consistency_token"].as_str().unwrap().to_owned();
    let (st, _) = call(
        &acme,
        None,
        "GET",
        &format!("/v1/documents/1?after={t}"),
        None,
    );
    assert_eq!(st, 404);
    for key in [&globex, &backend] {
        let (st, _) = call(
            key,
            None,
            "GET",
            &format!("/v1/documents/1?after={t}"),
            None,
        );
        assert_eq!(st, 200, "another tenant's document 1 was deleted");
    }
    let (st, ack) = call(
        &acme,
        None,
        "POST",
        "/v1/documents/delete",
        Some(&json!({ "ids": ["globex-only", "doc-a"], "filter": { "field": "n", "gte": 0 } })),
    );
    assert_eq!((st, &ack["deleted"]), (200, &json!(1)), "{ack}");
    let (st, ack) = call(
        &globex,
        None,
        "POST",
        "/v1/documents/delete",
        Some(&json!({ "filter": { "field": "n", "lte": 2 }, "after": after })),
    );
    assert_eq!((st, &ack["deleted"]), (200, &json!(2)), "{ack}");
    let t = ack["consistency_token"].as_str().unwrap().to_owned();
    let (_, res) = call(
        &backend,
        None,
        "POST",
        "/v1/search",
        Some(&json!({ "k": 100, "filter": { "field": "n", "gte": 0 }, "after": t })),
    );
    // Left: acme-only, globex-only, and the three plain documents.
    assert_eq!(res["hits"].as_array().unwrap().len(), 5, "{res}");

    // Erasing a tenant: unscoped keys only.
    let (st, _) = call(&acme, None, "DELETE", "/v1/tenants/globex", None);
    assert_eq!(st, 403);
    let (st, _) = call(&acme, None, "DELETE", "/v1/tenants/acme", None);
    assert_eq!(st, 403);
    let (st, ack) = call(
        &backend,
        None,
        "DELETE",
        &format!("/v1/tenants/acme?after={t}"),
        None,
    );
    assert_eq!((st, &ack["deleted"]), (200, &json!(1)), "{ack}");
    let t = ack["consistency_token"].as_str().unwrap().to_owned();
    let (_, res) = call(
        &acme,
        None,
        "POST",
        "/v1/search",
        Some(&json!({ "k": 100, "after": t })),
    );
    assert_eq!(res["hits"], json!([]));
    let (_, res) = call(
        &globex,
        None,
        "POST",
        "/v1/search",
        Some(&json!({ "k": 100, "after": t })),
    );
    assert_eq!(ids(&res), sorted(vec![json!("globex-only")]));

    // The audit trail names the tenant.
    let t0 = Instant::now();
    loop {
        let log = std::fs::read_to_string(dir.join("stderr.log")).unwrap();
        if let Some(line) = log.lines().find(|l| l.contains("tenant erased")) {
            assert!(
                line.contains("\"acme\"") && line.contains("\"backend\""),
                "{line}"
            );
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "no audit line: {log}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = std::fs::remove_dir_all(&dir);
}
