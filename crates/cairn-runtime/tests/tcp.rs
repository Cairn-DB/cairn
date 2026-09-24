//! Nodes over real TCP: node messages both ways and client round trips, in plaintext and with
//! mutual TLS (ADR 0018): identity checks, foreign CAs, anonymous clients, protocol versions,
//! and segment format negotiation.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use bytes::Bytes;
use cairn_core::{Network, NodeId};
use cairn_runtime::tcp::{ClientConn, Hello, PROTOCOL_VERSION, read_frame, write_frame};
use cairn_runtime::tls::{ClientTls, NodeTls, node_name};
use cairn_runtime::{Executor, TcpNetwork, TcpNetworkConfig, ThreadReactor};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

/// A test CA that signs certificates.
struct Ca {
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
}

type Identity = (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>);

impl Ca {
    fn new(name: &str) -> Self {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.distinguished_name
            .push(rcgen::DnType::CommonName, name.to_owned());
        p.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        Ca {
            cert: p.self_signed(&key).unwrap(),
            key,
        }
    }

    fn der(&self) -> CertificateDer<'static> {
        self.cert.der().clone()
    }

    fn issue(&self, dns: &str) -> Identity {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut p = rcgen::CertificateParams::new(vec![dns.to_owned()]).unwrap();
        p.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = p.signed_by(&key, &self.cert, &self.key).unwrap();
        (
            vec![cert.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
    }

    fn node_tls(&self, cert_name: &str, anonymous_clients: bool) -> NodeTls {
        let (chain, key) = self.issue(cert_name);
        NodeTls::from_der(self.der(), chain, key, anonymous_clients).unwrap()
    }
}

fn start(
    id: u32,
    listen: SocketAddr,
    peers: &HashMap<NodeId, SocketAddr>,
    tls: Option<NodeTls>,
    segment_version_max: u32,
) -> (Executor<ThreadReactor>, TcpNetwork) {
    let mut ex = Executor::new(ThreadReactor::new());
    let net = TcpNetwork::start(
        TcpNetworkConfig {
            id: NodeId(id),
            listen,
            peers: peers.clone(),
            drop_prob: 0.0,
            tls,
            segment_version_max,
        },
        ex.reactor().completer(),
    )
    .unwrap();
    (ex, net)
}

/// Waits until `net` has queued node messages (inbound count), or the deadline passes.
fn inbound_within(net: &TcpNetwork, d: Duration) -> u64 {
    let t = Instant::now();
    while t.elapsed() < d {
        let (_, inbound) = net.queue_stats();
        if inbound > 0 {
            return inbound;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    net.queue_stats().1
}

/// Node 2 echoes one node message and answers one client request; node 1 checks both.
fn echo_round_trip(tls1: Option<NodeTls>, tls2: Option<NodeTls>, client: Option<ClientTls>) {
    let (a1, a2) = (free_port(), free_port());
    let peers: HashMap<NodeId, SocketAddr> =
        [(NodeId(1), a1), (NodeId(2), a2)].into_iter().collect();
    let peers2 = peers.clone();
    let t2 = std::thread::spawn(move || {
        let (mut ex, net) = start(2, a2, &peers2, tls2, 1);
        let n = net.clone();
        ex.block_on(async move {
            let (from, msg) = n.recv().await.unwrap();
            assert_eq!(from, NodeId(1));
            n.send(from, Bytes::from([b"echo:".as_slice(), &msg].concat()))
                .await
                .unwrap();
            let req = n.client_recv().await;
            n.client_reply(
                req.conn,
                req.req,
                Bytes::from([b"resp:".as_slice(), &req.payload].concat()),
            );
        });
        net.stop();
    });
    let (mut ex, net) = start(1, a1, &peers, tls1, 1);
    let n = net.clone();
    let echoed = ex.block_on(async move {
        n.send(NodeId(2), Bytes::from_static(b"hello"))
            .await
            .unwrap();
        n.recv().await.unwrap()
    });
    assert_eq!(echoed, (NodeId(2), Bytes::from_static(b"echo:hello")));
    let c = ClientConn::connect_with(a2, client.as_ref().map(|t| (t, NodeId(2)))).unwrap();
    let r = c.call(b"ping", Duration::from_secs(5)).unwrap();
    assert_eq!(&r[..], b"resp:ping");
    t2.join().unwrap();
    net.stop();
}

#[test]
fn nodes_exchange_frames_and_clients_get_replies() {
    echo_round_trip(None, None, None);
}

#[test]
fn mutual_tls_between_nodes_and_with_clients() {
    let ca = Ca::new("cairn test CA");
    let client = ClientTls::from_der(ca.der(), Some(ca.issue("client.cairn"))).unwrap();
    echo_round_trip(
        Some(ca.node_tls(&node_name(NodeId(1)), false)),
        Some(ca.node_tls(&node_name(NodeId(2)), false)),
        Some(client),
    );
}

#[test]
fn large_frames_over_tls() {
    // Several TLS records per frame, both directions, with a reply written from a helper
    // thread while the connection's reader waits.
    let ca = Ca::new("cairn test CA");
    let a = free_port();
    let peers: HashMap<NodeId, SocketAddr> = [(NodeId(1), a)].into_iter().collect();
    let (mut ex, net) = start(1, a, &peers, Some(ca.node_tls("node-1.cairn", true)), 1);
    let client = ClientTls::from_der(ca.der(), None).unwrap();
    let c = std::sync::Arc::new(ClientConn::connect_with(a, Some((&client, NodeId(1)))).unwrap());
    let big: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
    let (c2, big2) = (c.clone(), big.clone());
    let caller = std::thread::spawn(move || c2.call(&big2, Duration::from_secs(20)).unwrap());
    let n = net.clone();
    ex.block_on(async move {
        let req = n.client_recv().await;
        assert_eq!(req.payload.len(), 3_000_000);
        let mut resp = req.payload.to_vec();
        resp.reverse();
        n.client_reply(req.conn, req.req, Bytes::from(resp));
    });
    let r = caller.join().unwrap();
    let mut want = big;
    want.reverse();
    assert_eq!(&r[..], &want[..]);
    net.stop();
}

#[test]
fn a_node_cannot_claim_another_nodes_identity() {
    let ca = Ca::new("cairn test CA");
    let (a1, a2) = (free_port(), free_port());
    let peers: HashMap<NodeId, SocketAddr> =
        [(NodeId(1), a1), (NodeId(2), a2)].into_iter().collect();
    let (_ex2, net2) = start(2, a2, &peers, Some(ca.node_tls("node-2.cairn", false)), 1);
    // A process holding node 3's certificate (same CA) pretends to be node 1.
    let (_ex1, impostor) = start(1, a1, &peers, Some(ca.node_tls("node-3.cairn", false)), 1);
    let _ = futures_lite_block(impostor.send(NodeId(2), Bytes::from_static(b"forged")));
    assert_eq!(
        inbound_within(&net2, Duration::from_millis(800)),
        0,
        "a message from the impostor was accepted"
    );
    impostor.stop();
    // Control: the real node 1 (its own certificate) gets through at once.
    let (_ex1b, real) = start(
        1,
        free_port(),
        &peers,
        Some(ca.node_tls("node-1.cairn", false)),
        1,
    );
    let _ = futures_lite_block(real.send(NodeId(2), Bytes::from_static(b"genuine")));
    assert_eq!(inbound_within(&net2, Duration::from_secs(3)), 1);
    real.stop();
    net2.stop();
}

#[test]
fn a_certificate_from_another_ca_is_refused() {
    let ca = Ca::new("cairn test CA");
    let other = Ca::new("someone else's CA");
    let (a1, a2) = (free_port(), free_port());
    let peers: HashMap<NodeId, SocketAddr> =
        [(NodeId(1), a1), (NodeId(2), a2)].into_iter().collect();
    let (_ex2, net2) = start(2, a2, &peers, Some(ca.node_tls("node-2.cairn", true)), 1);
    let (_ex1, stranger) = start(
        1,
        a1,
        &peers,
        Some(other.node_tls("node-1.cairn", false)),
        1,
    );
    let _ = futures_lite_block(stranger.send(NodeId(2), Bytes::from_static(b"x")));
    assert_eq!(inbound_within(&net2, Duration::from_millis(800)), 0);
    // Clients: one trusting another CA cannot connect; plaintext cannot either.
    let foreign_client = ClientTls::from_der(other.der(), None).unwrap();
    assert!(ClientConn::connect_with(a2, Some((&foreign_client, NodeId(2)))).is_err());
    let plain = ClientConn::connect(a2);
    if let Ok(c) = plain {
        assert!(c.call(b"ping", Duration::from_millis(500)).is_err());
    }
    stranger.stop();
    net2.stop();
}

#[test]
fn clients_need_a_certificate_unless_anonymous_clients_are_allowed() {
    let ca = Ca::new("cairn test CA");
    let anonymous = ClientTls::from_der(ca.der(), None).unwrap();
    for (allow, expect_ok) in [(false, false), (true, true)] {
        let a = free_port();
        let peers: HashMap<NodeId, SocketAddr> = [(NodeId(1), a)].into_iter().collect();
        let (mut ex, net) = start(1, a, &peers, Some(ca.node_tls("node-1.cairn", allow)), 1);
        let conn = ClientConn::connect_with(a, Some((&anonymous, NodeId(1))));
        let ok = match conn {
            Ok(c) => {
                let caller = std::thread::spawn(move || {
                    c.call(b"ping", Duration::from_millis(1500)).is_ok()
                });
                if expect_ok {
                    let n = net.clone();
                    ex.block_on(async move {
                        let req = n.client_recv().await;
                        n.client_reply(req.conn, req.req, Bytes::from_static(b"pong"));
                    });
                }
                caller.join().unwrap()
            }
            Err(_) => false,
        };
        assert_eq!(ok, expect_ok, "anonymous clients allowed: {allow}");
        net.stop();
    }
}

#[test]
fn hello_versions() {
    let node = Hello::Node {
        id: NodeId(4),
        protocol: PROTOCOL_VERSION,
        segment_max: 3,
    };
    assert_eq!(Hello::decode(&node.encode()), Some(node));
    assert!(node.compatible());
    let client = Hello::Client {
        protocol: PROTOCOL_VERSION,
    };
    assert_eq!(Hello::decode(&client.encode()), Some(client));
    // The 4-byte hello of protocol 1 and versions outside the supported range.
    assert_eq!(Hello::decode(&4u32.to_le_bytes()), None);
    assert!(
        !Hello::Client {
            protocol: PROTOCOL_VERSION + 1
        }
        .compatible()
    );
    assert!(!Hello::Client { protocol: 1 }.compatible());
}

#[test]
fn an_incompatible_protocol_version_is_disconnected() {
    let a = free_port();
    let peers: HashMap<NodeId, SocketAddr> = [(NodeId(1), a)].into_iter().collect();
    let (_ex, net) = start(1, a, &peers, None, 1);
    for hello in [
        4u32.to_le_bytes().to_vec(), // protocol 1
        Hello::Client {
            protocol: PROTOCOL_VERSION + 1,
        }
        .encode(),
    ] {
        let mut s = TcpStream::connect(a).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        write_frame(&mut s, 0, 0, &hello).unwrap();
        let _ = write_frame(&mut s, 2, 1, b"ping");
        // The node closes the connection instead of answering.
        assert!(read_frame(&mut s).is_err());
    }
    net.stop();
}

#[test]
fn segment_format_is_negotiated_from_peer_hellos() {
    let (a1, a2, a3) = (free_port(), free_port(), free_port());
    let peers: HashMap<NodeId, SocketAddr> = [(NodeId(1), a1), (NodeId(2), a2), (NodeId(3), a3)]
        .into_iter()
        .collect();
    let (_e1, n1) = start(1, a1, &peers, None, 5);
    assert_eq!(
        n1.peer_segment_version(),
        None,
        "no peer has said hello yet"
    );
    let (_e2, n2) = start(2, a2, &peers, None, 7);
    let _ = futures_lite_block(n2.send(NodeId(1), Bytes::from_static(b"hi")));
    inbound_within(&n1, Duration::from_secs(2));
    assert_eq!(n1.peer_segment_version(), None, "node 3 is still unknown");
    let (_e3, n3) = start(3, a3, &peers, None, 3);
    let _ = futures_lite_block(n3.send(NodeId(1), Bytes::from_static(b"hi")));
    let t = Instant::now();
    while n1.peer_segment_version().is_none() && t.elapsed() < Duration::from_secs(2) {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(n1.peer_segment_version(), Some(3));
    for n in [n1, n2, n3] {
        n.stop();
    }
}

/// `TcpNetwork::send` completes immediately (it only queues); poll it once.
fn futures_lite_block<F: std::future::Future>(f: F) -> F::Output {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut f = std::pin::pin!(f);
    match f.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        std::task::Poll::Pending => panic!("send should be ready"),
    }
}
