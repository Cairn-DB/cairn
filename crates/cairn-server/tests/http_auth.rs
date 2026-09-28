//! HTTP API authentication (ADR 0030) against a real process: API keys and roles, the
//! takedown audit trail, HTTPS, the refusal to serve without keys, and the admin key generated
//! on a first start.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use cairn_core::{FieldDef, FieldKind, Metric, Schema};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
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
    ])
    .unwrap()
}

fn request(method: &str, path: &str, key: Option<&str>, body: Option<&Value>) -> String {
    let payload = body.map(|b| b.to_string()).unwrap_or_default();
    let auth = key.map_or(String::new(), |k| format!("Authorization: Bearer {k}\r\n"));
    format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{auth}\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    )
}

fn parse(raw: &str) -> (u16, Value) {
    let status: u16 = raw[9..12].parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map(|x| x.1).unwrap_or("");
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

/// Plain HTTP request.
fn http(
    addr: SocketAddr,
    key: Option<&str>,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    s.write_all(request(method, path, key, body).as_bytes())
        .unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).unwrap();
    parse(&raw)
}

/// HTTPS request, trusting only `ca`.
fn https(
    addr: SocketAddr,
    ca: &rustls::pki_types::CertificateDer<'static>,
    key: Option<&str>,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> std::io::Result<(u16, Value)> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca.clone()).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let conn =
        rustls::ClientConnection::new(Arc::new(config), "localhost".try_into().unwrap()).unwrap();
    let tcp = TcpStream::connect(addr)?;
    tcp.set_read_timeout(Some(Duration::from_secs(30)))?;
    let mut tls = rustls::StreamOwned::new(conn, tcp);
    tls.write_all(request(method, path, key, body).as_bytes())?;
    let mut raw = Vec::new();
    // The server closes without close_notify on some paths; what was read is complete.
    let _ = tls.read_to_end(&mut raw);
    Ok(parse(&String::from_utf8_lossy(&raw)))
}

fn server(dir: &Path, raft: SocketAddr, web: SocketAddr) -> Command {
    let schema_path = dir.join("schema.json");
    std::fs::write(&schema_path, serde_json::to_vec(&schema()).unwrap()).unwrap();
    let mut c = Command::new(env!("CARGO_BIN_EXE_cairn-server"));
    c.args(["--node-id", "1", "--listen", &raft.to_string()])
        .args(["--peer", &format!("1={raft}")])
        .args(["--http-listen", &web.to_string()])
        .arg("--data")
        .arg(dir.join("node1"))
        .arg("--schema")
        .arg(&schema_path)
        .args(["--shards", "2", "--cores", "2", "--tick-ms", "20"])
        .env_remove("CAIRN_HTTP_ADMIN_KEY")
        .env_remove("RUST_LOG")
        .stdout(Stdio::null());
    c
}

/// `cairn-server keygen`: the secret and the keys-file entry.
fn keygen(id: &str, roles: &str) -> (String, Value) {
    let out = Command::new(env!("CARGO_BIN_EXE_cairn-server"))
        .args(["keygen", id, roles])
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

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("cairn-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn keys_roles_audit_and_https() {
    let dir = tempdir("http-auth");
    // Keys: one per role, plus an admin, written the way an operator would.
    let (reader, e1) = keygen("reader", "read");
    let (writer, e2) = keygen("ingest", "write,read");
    let (remover, e3) = keygen("compliance", "takedown");
    let (admin, e4) = keygen("ops", "admin");
    let keys_path = dir.join("keys.json");
    std::fs::write(&keys_path, json!({ "keys": [e1, e2, e3, e4] }).to_string()).unwrap();
    assert!(
        !std::fs::read_to_string(&keys_path)
            .unwrap()
            .contains(&reader)
    );
    // A self-signed certificate for the HTTP port.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    std::fs::write(dir.join("http.pem"), cert.cert.pem()).unwrap();
    std::fs::write(dir.join("http.key"), cert.key_pair.serialize_pem()).unwrap();
    let ca = cert.cert.der().clone();

    let (raft, web) = (free_port(), free_port());
    let log = std::fs::File::create(dir.join("stderr.log")).unwrap();
    let mut cmd = server(&dir, raft, web);
    cmd.arg("--http-keys")
        .arg(&keys_path)
        .arg("--http-tls-cert")
        .arg(dir.join("http.pem"))
        .arg("--http-tls-key")
        .arg(dir.join("http.key"))
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    let proc = Proc(cmd.spawn().unwrap());

    let t0 = Instant::now();
    while !matches!(https(web, &ca, None, "GET", "/health", None), Ok((200, _))) {
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "HTTPS did not come up"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let call = |key: Option<&str>, m: &str, p: &str, b: Option<&Value>| {
        https(web, &ca, key, m, p, b).expect("https")
    };
    // Writes need `write`: no key 401, a wrong key 401, the reader 403, the writer 200.
    let body = json!({ "documents": [{ "id": 1, "embedding": [1.0, 0.0, 0.0, 0.0], "text": "nuclear energy" }] });
    assert_eq!(call(None, "POST", "/v1/documents", Some(&body)).0, 401);
    assert_eq!(
        call(
            Some("cairn_wrong_key_value"),
            "POST",
            "/v1/documents",
            Some(&body)
        )
        .0,
        401
    );
    assert_eq!(
        call(Some(&reader), "POST", "/v1/documents", Some(&body)).0,
        403
    );
    let t0 = Instant::now();
    let ack = loop {
        let (st, ack) = call(Some(&writer), "POST", "/v1/documents", Some(&body));
        if st == 200 {
            break ack;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "write failed: {st} {ack}"
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    let token = ack["consistency_token"].as_str().unwrap().to_owned();
    // Reads need `read`: the reader and the writer may, the takedown-only key may not.
    let path = format!("/v1/documents/1?after={token}");
    assert_eq!(call(Some(&reader), "GET", &path, None).0, 200);
    assert_eq!(call(Some(&writer), "GET", &path, None).0, 200);
    assert_eq!(call(Some(&remover), "GET", &path, None).0, 403);
    let search = json!({ "text": { "field": "text", "query": "nuclear" }, "after": token });
    let (st, res) = call(Some(&reader), "POST", "/v1/search", Some(&search));
    assert_eq!((st, res["hits"].as_array().map_or(0, Vec::len)), (200, 1));
    // Takedowns need `takedown`: the writer may not, the compliance key may.
    assert_eq!(
        call(Some(&writer), "DELETE", "/v1/documents/1", None).0,
        403
    );
    let (st, del) = call(Some(&remover), "DELETE", "/v1/documents/1", None);
    assert_eq!(st, 200, "{del}");
    let t2 = del["consistency_token"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            Some(&reader),
            "GET",
            &format!("/v1/documents/1?after={t2}"),
            None
        )
        .0,
        404
    );
    // Administration needs `admin`; /health is open.
    assert_eq!(call(Some(&reader), "GET", "/v1/status", None).0, 403);
    assert_eq!(call(Some(&admin), "GET", "/v1/status", None).0, 200);
    assert_eq!(call(Some(&admin), "GET", "/v1/admin/merges", None).0, 200);
    assert_eq!(call(None, "GET", "/health", None).0, 200);
    // Plain HTTP on the HTTPS port gets nothing usable.
    let plain = std::panic::catch_unwind(|| http(web, Some(&admin), "GET", "/health", None));
    assert!(
        !matches!(plain, Ok((200, _))),
        "plain HTTP answered on the HTTPS port"
    );

    drop(proc);
    // The audit trail (tracing, on stdout) names the key and the document, and carries the token.
    let log = std::fs::read_to_string(dir.join("stderr.log")).unwrap();
    let line = log
        .lines()
        .find(|l| l.contains("takedown") && l.contains("cairn_server::audit"))
        .unwrap_or_else(|| panic!("no audit line in:\n{log}"));
    assert!(
        line.contains("compliance") && line.contains("[1]") && line.contains(&t2),
        "{line}"
    );
    assert!(!log.contains(&remover), "a secret reached the log");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn http_without_keys_is_refused_and_a_first_start_can_generate_one() {
    let dir = tempdir("http-nokeys");
    let (raft, web) = (free_port(), free_port());
    // No key at all: refused, with a message saying how to fix it.
    let out = server(&dir, raft, web)
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("API keys"));
    // A first start that generates an admin key: printed once, only its digest stored.
    let keys_path = dir.join("keys.json");
    let log = std::fs::File::create(dir.join("stderr.log")).unwrap();
    let mut cmd = server(&dir, raft, web);
    cmd.arg("--http-keys")
        .arg(&keys_path)
        .arg("--http-generate-admin-key")
        .stderr(log);
    let _p = Proc(cmd.spawn().unwrap());
    let t0 = Instant::now();
    let key = loop {
        let log = std::fs::read_to_string(dir.join("stderr.log")).unwrap();
        if let Some(k) = log.lines().map(str::trim).find(|l| l.starts_with("cairn_")) {
            break k.to_owned();
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "no generated key in:\n{log}"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(!std::fs::read_to_string(&keys_path).unwrap().contains(&key));
    let t0 = Instant::now();
    loop {
        if let Ok((200, _)) =
            std::panic::catch_unwind(|| http(web, Some(&key), "GET", "/v1/status", None))
        {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "generated key refused"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(http(web, None, "GET", "/v1/status", None).0, 401);
    let _ = std::fs::remove_dir_all(&dir);
}
