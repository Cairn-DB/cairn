//! Retention (ADR 0031) against a real process: expired documents are hidden from reads at
//! once, then deleted by the shard leaders, which the audit trail records; for `default`
//! (`--expires-field`) and for a collection created with `expires_field`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn ids(res: &Value) -> Vec<String> {
    let mut v: Vec<String> = res["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("{res}"))
        .iter()
        .map(|h| h["id"].as_str().unwrap().to_owned())
        .collect();
    v.sort();
    v
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::new();
    let mut esc = false;
    for c in s.chars() {
        match (esc, c) {
            (false, '\u{1b}') => esc = true,
            (true, 'm') => esc = false,
            (true, _) => {}
            (false, c) => out.push(c),
        }
    }
    out
}

/// Waits for an audit line naming `collection` with `count=<n>`.
fn wait_audit(log: &std::path::Path, collection: &str, n: u64) {
    let t0 = Instant::now();
    loop {
        let text = std::fs::read_to_string(log).unwrap_or_default();
        let found = text.lines().any(|l| {
            let plain = strip_ansi(l);
            plain.contains("expired documents deleted")
                && plain.contains(&format!("collection={collection}"))
                && plain.contains(&format!("count={n}"))
        });
        if found {
            return;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(20),
            "no deletion of {n} expired documents in {collection}:\n{text}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn expired_documents_are_hidden_then_deleted() {
    let dir = std::env::temp_dir().join(format!("cairn-retention-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let schema = json!({ "fields": [
        { "name": "text", "kind": "Text" },
        { "name": "expires", "kind": "I64" },
    ] });
    std::fs::write(dir.join("schema.json"), schema.to_string()).unwrap();
    let (raft, web) = (free_port(), free_port());
    let log = dir.join("node.log");
    let f = std::fs::File::create(&log).unwrap();
    let _proc = Proc(
        Command::new(env!("CARGO_BIN_EXE_cairn-server"))
            .args(["--node-id", "1", "--listen", &raft.to_string()])
            .args(["--peer", &format!("1={raft}")])
            .args(["--http-listen", &web.to_string()])
            .arg("--data")
            .arg(dir.join("node1"))
            .arg("--schema")
            .arg(dir.join("schema.json"))
            .args(["--shards", "3", "--cores", "2", "--tick-ms", "20"])
            .args([
                "--expires-field",
                "expires",
                "--retention-interval-ms",
                "300",
            ])
            .env("CAIRN_HTTP_ADMIN_KEY", ADMIN)
            .env("RUST_LOG", "warn,cairn_server::audit=info")
            .stdout(f.try_clone().unwrap())
            .stderr(f)
            .spawn()
            .unwrap(),
    );
    let t0 = Instant::now();
    while http(web, "GET", "/v1/collections", None).0 != 200 {
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "HTTP did not come up"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // `default`: one document expires in 1.5 s, one in 10 minutes, one never.
    let soon = now_ms() + 1_500;
    let docs = json!({ "documents": [
        { "id": "soon", "text": "lynx soon", "expires": soon },
        { "id": "later", "text": "lynx later", "expires": now_ms() + 600_000 },
        { "id": "never", "text": "lynx never" },
    ] });
    let (st, ack) = http(web, "POST", "/v1/documents", Some(&docs));
    assert_eq!(st, 200, "{ack}");
    let t = ack["consistency_token"].as_str().unwrap().to_owned();
    let search = json!({ "k": 10, "text": { "field": "text", "query": "lynx" }, "after": t });
    let (_, res) = http(web, "POST", "/v1/search", Some(&search));
    assert_eq!(ids(&res), ["later", "never", "soon"]);
    let (st, _) = http(web, "GET", &format!("/v1/documents/soon?after={t}"), None);
    assert_eq!(st, 200);
    // Past its expiry: hidden at once, whatever the sweep.
    while now_ms() <= soon {
        std::thread::sleep(Duration::from_millis(50));
    }
    let (st, _) = http(web, "GET", &format!("/v1/documents/soon?after={t}"), None);
    assert_eq!(st, 404, "an expired document was read");
    let (_, res) = http(web, "POST", "/v1/search", Some(&search));
    assert_eq!(ids(&res), ["later", "never"]);
    // A patch leaves an expired document alone (it would otherwise come back).
    let (st, ack) = http(
        web,
        "PATCH",
        "/v1/documents/soon",
        Some(&json!({ "set": { "expires": now_ms() + 600_000 } })),
    );
    assert_eq!((st, &ack["patched"]), (200, &json!(0)), "{ack}");
    // And deleted by its shard's leader.
    wait_audit(&log, "default", 1);

    // A collection with retention, on a Date field; an invalid retention field is refused.
    let bad = json!({ "name": "bad", "schema": schema, "expires_field": "text" });
    let (st, _) = http(web, "POST", "/v1/collections", Some(&bad));
    assert_eq!(st, 400);
    let body = json!({ "name": "sessions", "expires_field": "until", "schema": { "fields": [
        { "name": "text", "kind": "Text" }, { "name": "until", "kind": "Date" },
    ] } });
    let (st, created) = http(web, "POST", "/v1/collections", Some(&body));
    assert_eq!(
        (st, &created["expires_field"]),
        (201, &json!("until")),
        "{created}"
    );
    let docs = json!({ "documents": [
        { "id": "old-1", "text": "ocelot", "until": now_ms() - 1 },
        { "id": "old-2", "text": "ocelot", "until": now_ms() - 60_000 },
        { "id": "fresh", "text": "ocelot", "until": now_ms() + 600_000 },
    ] });
    let (st, ack) = http(
        web,
        "POST",
        "/v1/collections/sessions/documents",
        Some(&docs),
    );
    assert_eq!(st, 200, "{ack}");
    let t = ack["consistency_token"].as_str().unwrap().to_owned();
    let (_, res) = http(
        web,
        "POST",
        "/v1/collections/sessions/search",
        Some(&json!({ "k": 10, "after": t })),
    );
    assert_eq!(
        ids(&res),
        ["fresh"],
        "already expired when written: never visible"
    );
    let t0 = Instant::now();
    loop {
        let text = std::fs::read_to_string(&log).unwrap_or_default();
        let deleted: u64 = text
            .lines()
            .map(strip_ansi)
            .filter(|l| {
                l.contains("expired documents deleted") && l.contains("collection=sessions")
            })
            .filter_map(|l| {
                let rest = &l[l.find("count=")? + 6..];
                let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                digits.parse::<u64>().ok()
            })
            .sum();
        if deleted == 2 {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(20),
            "expected 2 deletions in sessions, got {deleted}:\n{text}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = std::fs::remove_dir_all(&dir);
}
