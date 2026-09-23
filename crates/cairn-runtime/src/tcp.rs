//! TCP transport: one reader thread per accepted connection, one writer thread per peer,
//! length-prefixed frames. Node-to-node frames implement the `Network` trait; client frames are
//! surfaced separately through [`TcpNetwork::client_recv`] and answered with
//! [`TcpNetwork::client_reply`].
//!
//! Frame: `u32 length | u8 kind | u64 request id | payload`. Kinds: 0 hello (payload: `u32` node
//! id, or `u32::MAX` for a client), 1 node message, 2 client request, 3 client response.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::pool::Completer;
use bytes::Bytes;
use cairn_core::error::IoErrorKind;
use cairn_core::{Error, Network, NodeId, Result};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::Duration as StdDuration;

const KIND_HELLO: u8 = 0;
const KIND_NODE: u8 = 1;
const KIND_CLIENT_REQ: u8 = 2;
const KIND_CLIENT_RESP: u8 = 3;
const MAX_FRAME: usize = 64 << 20;

/// Writes one frame.
pub fn write_frame(w: &mut impl Write, kind: u8, req: u64, payload: &[u8]) -> std::io::Result<()> {
    let len = (1 + 8 + payload.len()) as u32;
    let mut head = [0u8; 13];
    head[..4].copy_from_slice(&len.to_le_bytes());
    head[4] = kind;
    head[5..13].copy_from_slice(&req.to_le_bytes());
    w.write_all(&head)?;
    w.write_all(payload)?;
    w.flush()
}

/// Reads one frame: `(kind, request id, payload)`.
pub fn read_frame(r: &mut impl Read) -> std::io::Result<(u8, u64, Vec<u8>)> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    if !(9..=MAX_FRAME).contains(&len) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad frame length",
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    let kind = body[0];
    let req = u64::from_le_bytes(body[1..9].try_into().expect("8 bytes"));
    body.drain(..9);
    Ok((kind, req, body))
}

/// A request from a client connection.
#[derive(Debug)]
pub struct ClientRequest {
    /// Connection id (to reply on).
    pub conn: u64,
    /// Request id (echoed in the reply).
    pub req: u64,
    /// Payload.
    pub payload: Bytes,
}

struct Inbox<T> {
    items: VecDeque<T>,
    waker: Option<Waker>,
}

impl<T> Inbox<T> {
    fn new() -> Self {
        Inbox {
            items: VecDeque::new(),
            waker: None,
        }
    }
}

/// Network configuration.
#[derive(Debug, Clone)]
pub struct TcpNetworkConfig {
    /// This node.
    pub id: NodeId,
    /// Address to listen on.
    pub listen: SocketAddr,
    /// Peer addresses.
    pub peers: HashMap<NodeId, SocketAddr>,
    /// Fraction of node messages to drop (tests only).
    pub drop_prob: f64,
}

/// Bytes a peer's send queue may hold. Beyond it messages are dropped: node traffic is Raft
/// and file shipping, which both retry, and an unbounded queue turned a sender-side bug into
/// an out-of-memory kill (65 GB on a real network).
const PEER_QUEUE_MAX_BYTES: usize = 256 << 20;

/// A peer's pending messages and their total size.
#[derive(Default)]
struct PendingOut {
    items: VecDeque<Bytes>,
    bytes: usize,
}

type PeerQueue = Arc<(Mutex<PendingOut>, std::sync::Condvar)>;

struct Shared {
    node_inbox: Mutex<Inbox<(NodeId, Bytes)>>,
    client_inbox: Mutex<Inbox<ClientRequest>>,
    client_conns: Mutex<HashMap<u64, Arc<Mutex<TcpStream>>>>,
    peer_queues: Mutex<HashMap<NodeId, PeerQueue>>,
    completer: Completer,
    cfg: TcpNetworkConfig,
    next_conn: AtomicU64,
    stopped: AtomicBool,
    drop_seq: AtomicU64,
}

/// The TCP network.
#[derive(Clone)]
pub struct TcpNetwork {
    shared: Arc<Shared>,
}

impl TcpNetwork {
    /// Binds the listener and starts the acceptor thread.
    pub fn start(cfg: TcpNetworkConfig, completer: Completer) -> Result<Self> {
        let listener =
            TcpListener::bind(cfg.listen).map_err(|e| Error::io(IoErrorKind::Other, e))?;
        let shared = Arc::new(Shared {
            node_inbox: Mutex::new(Inbox::new()),
            client_inbox: Mutex::new(Inbox::new()),
            client_conns: Mutex::new(HashMap::new()),
            peer_queues: Mutex::new(HashMap::new()),
            completer,
            cfg,
            next_conn: AtomicU64::new(1),
            stopped: AtomicBool::new(false),
            drop_seq: AtomicU64::new(1),
        });
        // A live network can deliver at any time: keep the reactor from declaring a stall.
        shared.completer.begin();
        let s = shared.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if s.stopped.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(stream) = stream else { continue };
                let s2 = s.clone();
                std::thread::spawn(move || Self::serve_connection(s2, stream));
            }
        });
        Ok(TcpNetwork { shared })
    }

    /// The bound address.
    pub fn local_addr(&self) -> SocketAddr {
        self.shared.cfg.listen
    }

    /// Stops accepting (existing threads exit as their sockets close).
    pub fn stop(&self) {
        self.shared.stopped.store(true, Ordering::SeqCst);
    }

    fn wake_inbox<T>(shared: &Shared, inbox: &Mutex<Inbox<T>>, item: T) {
        let waker = {
            let mut g = inbox.lock().expect("inbox poisoned");
            g.items.push_back(item);
            g.waker.take()
        };
        // Interrupt the reactor's park and wake the waiting task on the executor thread.
        shared.completer.notify(Box::new(move || {
            if let Some(w) = waker {
                w.wake();
            }
        }));
    }

    fn serve_connection(shared: Arc<Shared>, mut stream: TcpStream) {
        let _ = stream.set_nodelay(true);
        let Ok((kind, _, hello)) = read_frame(&mut stream) else {
            return;
        };
        if kind != KIND_HELLO || hello.len() != 4 {
            return;
        }
        let who = u32::from_le_bytes(hello[..4].try_into().expect("4 bytes"));
        if who == u32::MAX {
            // Client connection.
            let conn = shared.next_conn.fetch_add(1, Ordering::SeqCst);
            let writer = Arc::new(Mutex::new(stream.try_clone().expect("clone stream")));
            shared
                .client_conns
                .lock()
                .expect("conns")
                .insert(conn, writer);
            while let Ok((kind, req, payload)) = read_frame(&mut stream) {
                if kind == KIND_CLIENT_REQ {
                    Self::wake_inbox(
                        &shared,
                        &shared.client_inbox,
                        ClientRequest {
                            conn,
                            req,
                            payload: Bytes::from(payload),
                        },
                    );
                }
            }
            shared.client_conns.lock().expect("conns").remove(&conn);
        } else {
            let from = NodeId(who);
            while let Ok((kind, _, payload)) = read_frame(&mut stream) {
                if kind == KIND_NODE {
                    Self::wake_inbox(&shared, &shared.node_inbox, (from, Bytes::from(payload)));
                }
            }
        }
    }

    fn peer_queue(&self, to: NodeId) -> PeerQueue {
        let mut qs = self.shared.peer_queues.lock().expect("queues");
        if let Some(q) = qs.get(&to) {
            return q.clone();
        }
        let q = Arc::new((Mutex::new(PendingOut::default()), std::sync::Condvar::new()));
        qs.insert(to, q.clone());
        let shared = self.shared.clone();
        let q2 = q.clone();
        std::thread::spawn(move || Self::writer_loop(shared, to, q2));
        q
    }

    /// Connects (retrying) and drains the peer's queue.
    fn writer_loop(shared: Arc<Shared>, to: NodeId, q: PeerQueue) {
        let Some(addr) = shared.cfg.peers.get(&to).copied() else {
            return;
        };
        let mut stream: Option<TcpStream> = None;
        loop {
            if shared.stopped.load(Ordering::SeqCst) {
                return;
            }
            let msg = {
                let (m, cv) = &*q;
                let mut g = m.lock().expect("peer queue");
                while g.items.is_empty() {
                    let (g2, _) = cv
                        .wait_timeout(g, StdDuration::from_millis(200))
                        .expect("peer queue");
                    g = g2;
                    if shared.stopped.load(Ordering::SeqCst) {
                        return;
                    }
                }
                let msg = g.items.pop_front().expect("non-empty");
                g.bytes -= msg.len();
                msg
            };
            for _attempt in 0..2 {
                if stream.is_none() {
                    match TcpStream::connect_timeout(&addr, StdDuration::from_millis(500)) {
                        Ok(mut s) => {
                            let _ = s.set_nodelay(true);
                            if write_frame(
                                &mut s,
                                KIND_HELLO,
                                0,
                                &shared.cfg.id.get().to_le_bytes(),
                            )
                            .is_ok()
                            {
                                stream = Some(s);
                            }
                        }
                        Err(_) => {
                            std::thread::sleep(StdDuration::from_millis(50));
                            continue;
                        }
                    }
                }
                if let Some(s) = stream.as_mut() {
                    if write_frame(s, KIND_NODE, 0, &msg).is_ok() {
                        break;
                    }
                    stream = None;
                }
            }
            // Best effort: a message that could not be sent is dropped (Raft retries).
        }
    }

    /// Next client request (server side).
    pub fn client_recv(&self) -> impl Future<Output = ClientRequest> + '_ {
        std::future::poll_fn(move |cx| {
            let mut g = self.shared.client_inbox.lock().expect("inbox poisoned");
            match g.items.pop_front() {
                Some(r) => Poll::Ready(r),
                None => {
                    g.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }

    /// Replies to a client request (written on a helper thread).
    pub fn client_reply(&self, conn: u64, req: u64, payload: Bytes) {
        let writer = self
            .shared
            .client_conns
            .lock()
            .expect("conns")
            .get(&conn)
            .cloned();
        if let Some(w) = writer {
            std::thread::spawn(move || {
                let mut s = w.lock().expect("stream");
                let _ = write_frame(&mut *s, KIND_CLIENT_RESP, req, &payload);
            });
        }
    }
}

impl Network for TcpNetwork {
    fn queue_stats(&self) -> (u64, u64) {
        let out: u64 = self
            .shared
            .peer_queues
            .lock()
            .expect("queues")
            .values()
            .map(|q| q.0.lock().expect("peer queue").bytes as u64)
            .sum();
        let inbound = self.shared.node_inbox.lock().expect("inbox").items.len()
            + self.shared.client_inbox.lock().expect("inbox").items.len();
        (out, inbound as u64)
    }

    fn send(&self, to: NodeId, message: Bytes) -> impl Future<Output = Result<()>> {
        let result = if to == self.shared.cfg.id {
            Self::wake_inbox(&self.shared, &self.shared.node_inbox, (to, message));
            Ok(())
        } else if !self.shared.cfg.peers.contains_key(&to) {
            Err(Error::io(
                IoErrorKind::Unreachable,
                format!("unknown node {to}"),
            ))
        } else {
            let drop = self.shared.cfg.drop_prob > 0.0 && {
                let n = self.shared.drop_seq.fetch_add(1, Ordering::Relaxed);
                (cairn_core::hash::xxh3_64(&n.to_le_bytes()) % 10_000) as f64 / 10_000.0
                    < self.shared.cfg.drop_prob
            };
            if !drop {
                let q = self.peer_queue(to);
                let (m, cv) = &*q;
                let mut g = m.lock().expect("peer queue");
                if g.bytes + message.len() <= PEER_QUEUE_MAX_BYTES {
                    g.bytes += message.len();
                    g.items.push_back(message);
                    std::mem::drop(g);
                    cv.notify_one();
                }
            }
            Ok(())
        };
        async move { result }
    }

    fn recv(&self) -> impl Future<Output = Result<(NodeId, Bytes)>> {
        std::future::poll_fn(move |cx| {
            let mut g = self.shared.node_inbox.lock().expect("inbox poisoned");
            match g.items.pop_front() {
                Some(m) => Poll::Ready(Ok(m)),
                None => {
                    g.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }

    fn local_id(&self) -> NodeId {
        self.shared.cfg.id
    }
}

/// A client-side connection: sends requests and reads responses on a reader thread.
pub struct ClientConn {
    writer: Mutex<TcpStream>,
    pending: Arc<Mutex<HashMap<u64, std::sync::mpsc::Sender<Bytes>>>>,
    next_req: AtomicU64,
}

impl ClientConn {
    /// Connects and sends the client hello.
    pub fn connect(addr: SocketAddr) -> Result<Self> {
        let mut s = TcpStream::connect_timeout(&addr, StdDuration::from_secs(2))
            .map_err(|e| Error::io(IoErrorKind::Unreachable, e))?;
        let _ = s.set_nodelay(true);
        write_frame(&mut s, KIND_HELLO, 0, &u32::MAX.to_le_bytes())
            .map_err(|e| Error::io(IoErrorKind::Other, e))?;
        let pending: Arc<Mutex<HashMap<u64, std::sync::mpsc::Sender<Bytes>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut reader = s
            .try_clone()
            .map_err(|e| Error::io(IoErrorKind::Other, e))?;
        let p2 = pending.clone();
        std::thread::spawn(move || {
            while let Ok((kind, req, payload)) = read_frame(&mut reader) {
                if kind == KIND_CLIENT_RESP
                    && let Some(tx) = p2.lock().expect("pending").remove(&req)
                {
                    let _ = tx.send(Bytes::from(payload));
                }
            }
            p2.lock().expect("pending").clear();
        });
        Ok(ClientConn {
            writer: Mutex::new(s),
            pending,
            next_req: AtomicU64::new(1),
        })
    }

    /// Sends a request and waits (blocking) for its response.
    pub fn call(&self, payload: &[u8], timeout: StdDuration) -> Result<Bytes> {
        let req = self.next_req.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = std::sync::mpsc::channel();
        self.pending.lock().expect("pending").insert(req, tx);
        {
            let mut w = self.writer.lock().expect("writer");
            write_frame(&mut *w, KIND_CLIENT_REQ, req, payload)
                .map_err(|e| Error::io(IoErrorKind::Other, e))?;
        }
        rx.recv_timeout(timeout).map_err(|_| {
            self.pending.lock().expect("pending").remove(&req);
            Error::io(IoErrorKind::Other, "request timed out")
        })
    }
}
