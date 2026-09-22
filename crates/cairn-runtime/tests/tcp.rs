//! Two nodes over real TCP: node messages both ways and a client round trip.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use bytes::Bytes;
use cairn_core::{Network, NodeId};
use cairn_runtime::tcp::ClientConn;
use cairn_runtime::{Executor, TcpNetwork, TcpNetworkConfig, ThreadReactor};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

#[test]
fn nodes_exchange_frames_and_clients_get_replies() {
    let (a1, a2) = (free_port(), free_port());
    let peers: HashMap<NodeId, SocketAddr> =
        [(NodeId(1), a1), (NodeId(2), a2)].into_iter().collect();
    // Node 2: echo server thread with its own executor.
    let peers2 = peers.clone();
    let t2 = std::thread::spawn(move || {
        let mut ex = Executor::new(ThreadReactor::new());
        let net = TcpNetwork::start(
            TcpNetworkConfig {
                id: NodeId(2),
                listen: a2,
                peers: peers2,
                drop_prob: 0.0,
            },
            ex.reactor().completer(),
        )
        .unwrap();
        let n = net.clone();
        ex.block_on(async move {
            // One node message and one client request.
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
    // Node 1: sends to node 2 and waits for the echo.
    let mut ex = Executor::new(ThreadReactor::new());
    let net = TcpNetwork::start(
        TcpNetworkConfig {
            id: NodeId(1),
            listen: a1,
            peers,
            drop_prob: 0.0,
        },
        ex.reactor().completer(),
    )
    .unwrap();
    let n = net.clone();
    let echoed = ex.block_on(async move {
        n.send(NodeId(2), Bytes::from_static(b"hello"))
            .await
            .unwrap();
        n.recv().await.unwrap()
    });
    assert_eq!(echoed, (NodeId(2), Bytes::from_static(b"echo:hello")));
    // Client round trip to node 2.
    let c = ClientConn::connect(a2).unwrap();
    let r = c.call(b"ping", Duration::from_secs(5)).unwrap();
    assert_eq!(&r[..], b"resp:ping");
    t2.join().unwrap();
    net.stop();
}
