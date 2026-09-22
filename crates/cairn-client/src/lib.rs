//! Blocking client. Sends requests to one node and follows `NotLeader` hints to another; keeps
//! the highest token seen per shard so `read_your_writes()` reads reflect this client's writes.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use cairn_core::error::IoErrorKind;
use cairn_core::{DocId, Document, Error, HashMap, NodeId, Result};
use cairn_proto::{Request, Response};
use cairn_query::{Consistency, Hit, Query, ReplicaStatus, Token};
use cairn_runtime::tcp::ClientConn;
use std::net::SocketAddr;
use std::time::Duration;

/// A connected client.
pub struct Client {
    addrs: HashMap<NodeId, SocketAddr>,
    conns: HashMap<NodeId, ClientConn>,
    current: NodeId,
    tokens: HashMap<u32, Token>,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Attempts across redirects and reconnects.
    pub max_attempts: usize,
}

impl Client {
    /// Creates a client for the given nodes; connections are opened lazily.
    pub fn new(addrs: HashMap<NodeId, SocketAddr>) -> Self {
        let current = addrs.keys().min().copied().unwrap_or(NodeId(1));
        Client {
            addrs,
            conns: HashMap::default(),
            current,
            tokens: HashMap::default(),
            timeout: Duration::from_secs(10),
            max_attempts: 40,
        }
    }

    /// Node the client currently talks to.
    pub fn current_node(&self) -> NodeId {
        self.current
    }

    /// Highest token seen per shard (for read-your-writes).
    pub fn tokens(&self) -> Vec<Token> {
        let mut v: Vec<Token> = self.tokens.values().copied().collect();
        v.sort();
        v
    }

    fn conn(&mut self, node: NodeId) -> Result<&ClientConn> {
        if !self.conns.contains_key(&node) {
            let addr = *self.addrs.get(&node).ok_or_else(|| {
                Error::io(IoErrorKind::Unreachable, format!("unknown node {node}"))
            })?;
            let c = ClientConn::connect(addr)?;
            self.conns.insert(node, c);
        }
        Ok(self.conns.get(&node).expect("inserted"))
    }

    fn next_node(&self, after: NodeId) -> NodeId {
        let mut ids: Vec<NodeId> = self.addrs.keys().copied().collect();
        ids.sort();
        let i = ids.iter().position(|n| *n == after).unwrap_or(0);
        ids[(i + 1) % ids.len()]
    }

    /// Sends a request, following leader hints and rotating on connection failures.
    pub fn call(&mut self, req: &Request) -> Result<Response> {
        let bytes = req.to_bytes();
        let mut last_err = None;
        for _ in 0..self.max_attempts {
            let node = self.current;
            let timeout = self.timeout;
            let result = match self.conn(node) {
                Ok(c) => c.call(&bytes, timeout),
                Err(e) => Err(e),
            };
            match result {
                Ok(resp) => match Response::from_bytes(&resp)? {
                    Response::Error {
                        message,
                        leader_hint: Some(l),
                    } if l != node => {
                        last_err = Some(message);
                        self.current = l;
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Response::Error {
                        message,
                        leader_hint,
                    } if message.contains("not the leader") || message.contains("stopped") => {
                        last_err = Some(message);
                        self.current = leader_hint.unwrap_or_else(|| self.next_node(node));
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    other => return Ok(other),
                },
                Err(e) => {
                    last_err = Some(e.to_string());
                    self.conns.remove(&node);
                    self.current = self.next_node(node);
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
        Err(Error::io(
            IoErrorKind::Unreachable,
            format!(
                "gave up after {} attempts: {}",
                self.max_attempts,
                last_err.unwrap_or_default()
            ),
        ))
    }

    fn expect_ack(&mut self, resp: Response) -> Result<Vec<Token>> {
        match resp {
            Response::Ack(tokens) => {
                for t in &tokens {
                    let e = self.tokens.entry(t.shard.get()).or_insert(*t);
                    if t.index > e.index {
                        *e = *t;
                    }
                }
                Ok(tokens)
            }
            Response::Error { message, .. } => Err(Error::Internal(message)),
            other => Err(Error::Internal(format!("unexpected response {other:?}"))),
        }
    }

    /// Inserts or replaces documents.
    pub fn upsert(&mut self, docs: Vec<Document>) -> Result<Vec<Token>> {
        let r = self.call(&Request::Upsert(docs))?;
        self.expect_ack(r)
    }

    /// Deletes documents (takedown).
    pub fn delete(&mut self, ids: Vec<DocId>) -> Result<Vec<Token>> {
        let r = self.call(&Request::Delete(ids))?;
        self.expect_ack(r)
    }

    /// Point read. `read_your_writes()` expands to every token this client has seen; an explicit
    /// `ReadYourWrites(token)` is sent as is.
    pub fn get(&mut self, id: DocId, consistency: Consistency) -> Result<Option<Document>> {
        let tokens = if consistency == self.read_your_writes() {
            self.tokens()
        } else {
            Vec::new()
        };
        match self.call(&Request::Get {
            id,
            consistency,
            tokens,
        })? {
            Response::Doc(d) => Ok(d),
            Response::Error { message, .. } => Err(Error::Internal(message)),
            other => Err(Error::Internal(format!("unexpected response {other:?}"))),
        }
    }

    /// Hybrid query across all shards.
    pub fn query(&mut self, query: Query, consistency: Consistency) -> Result<Vec<Hit>> {
        let tokens = if matches!(consistency, Consistency::ReadYourWrites(_)) {
            self.tokens()
        } else {
            Vec::new()
        };
        match self.call(&Request::Query {
            query,
            consistency,
            tokens,
        })? {
            Response::Hits(h) => Ok(h),
            Response::Error { message, .. } => Err(Error::Internal(message)),
            other => Err(Error::Internal(format!("unexpected response {other:?}"))),
        }
    }

    /// Status of the node the client currently talks to.
    pub fn status(&mut self) -> Result<Vec<ReplicaStatus>> {
        match self.call(&Request::Status)? {
            Response::Status(s) => Ok(s),
            Response::Error { message, .. } => Err(Error::Internal(message)),
            other => Err(Error::Internal(format!("unexpected response {other:?}"))),
        }
    }

    /// Sentinel meaning "read-your-writes with every token this client has seen"; expanded by
    /// `get` and `query` into the per-shard token list.
    pub fn read_your_writes(&self) -> Consistency {
        Consistency::ReadYourWrites(Token {
            shard: cairn_core::ShardId(0),
            index: cairn_core::LogIndex(0),
        })
    }
}
