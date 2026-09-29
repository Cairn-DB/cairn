//! Blocking client. Sends requests to one node and follows `NotLeader` hints to another; keeps
//! the highest token seen per shard so `read_your_writes()` reads reflect this client's writes.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use cairn_core::error::IoErrorKind;
use cairn_core::{DocId, Document, Error, HashMap, NodeId, Predicate, Result};
use cairn_proto::{DeleteScope, Request, Response};
use cairn_query::{Consistency, Hit, Query, ReplicaStatus, Token};
use cairn_runtime::tcp::ClientConn;
use cairn_runtime::tls::ClientTls;
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

fn default_tls_slot() -> &'static Mutex<Option<ClientTls>> {
    static SLOT: OnceLock<Mutex<Option<ClientTls>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

/// Sets the TLS settings every [`Client::new`] in this process uses from now on (`None`:
/// plaintext). Tools that create many clients set it once from their command line.
pub fn set_default_tls(tls: Option<ClientTls>) {
    *default_tls_slot().lock().expect("tls slot") = tls;
}

/// A connected client.
pub struct Client {
    addrs: HashMap<NodeId, SocketAddr>,
    conns: HashMap<NodeId, ClientConn>,
    current: NodeId,
    tokens: HashMap<u32, Token>,
    tls: Option<ClientTls>,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Attempts across redirects and reconnects.
    pub max_attempts: usize,
    /// The collection requests go to (`None`: `default`).
    collection: Option<String>,
}

impl Client {
    /// Creates a client for the given nodes; connections are opened lazily. Successive clients
    /// in a process start on successive nodes, so reads served locally (stale, read-your-writes)
    /// and query coordination spread over the cluster instead of all landing on one node.
    pub fn new(addrs: HashMap<NodeId, SocketAddr>) -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let mut ids: Vec<NodeId> = addrs.keys().copied().collect();
        ids.sort();
        let current = if ids.is_empty() {
            NodeId(1)
        } else {
            ids[NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % ids.len()]
        };
        Client {
            addrs,
            conns: HashMap::default(),
            current,
            tokens: HashMap::default(),
            tls: default_tls_slot().lock().expect("tls slot").clone(),
            timeout: Duration::from_secs(10),
            max_attempts: 40,
            collection: None,
        }
    }

    /// Uses TLS (or plaintext with `None`) for connections opened from now on.
    pub fn with_tls(mut self, tls: Option<ClientTls>) -> Self {
        self.tls = tls;
        self.conns.clear();
        self
    }

    /// Sends document requests (writes, reads, searches, deletions) to `collection` from now on
    /// (ADR 0031); `None` for `default`.
    pub fn set_collection(&mut self, collection: Option<String>) {
        self.collection = collection.filter(|c| c != "default");
    }

    /// The collection document requests go to.
    pub fn collection(&self) -> Option<&str> {
        self.collection.as_deref()
    }

    /// Creates a collection, with an optional retention field (ADR 0031); returns its
    /// definition (JSON).
    pub fn create_collection(
        &mut self,
        name: &str,
        schema_json: &str,
        shards: u32,
        expires_field: Option<&str>,
    ) -> Result<String> {
        self.one_collection(Request::CreateCollection {
            name: name.into(),
            schema: schema_json.into(),
            shards,
            expires_field: expires_field.unwrap_or_default().into(),
        })
    }

    /// Drops a collection and deletes its data on every node; returns its definition (JSON).
    pub fn drop_collection(&mut self, name: &str) -> Result<String> {
        self.one_collection(Request::DropCollection { name: name.into() })
    }

    /// Definitions (JSON) of the live collections, `default` first.
    pub fn list_collections(&mut self) -> Result<Vec<String>> {
        match self.call(&Request::ListCollections)? {
            Response::Collections(v) => Ok(v),
            Response::Error { message, .. } => Err(Error::Internal(message)),
            other => Err(Error::Internal(format!("unexpected response {other:?}"))),
        }
    }

    fn one_collection(&mut self, req: Request) -> Result<String> {
        match self.call(&req)? {
            Response::Collections(mut v) if v.len() == 1 => Ok(v.remove(0)),
            Response::Error { message, .. } => Err(Error::Internal(message)),
            other => Err(Error::Internal(format!("unexpected response {other:?}"))),
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
        if self.conns.get(&node).is_none_or(ClientConn::is_closed) {
            let addr = *self.addrs.get(&node).ok_or_else(|| {
                Error::io(IoErrorKind::Unreachable, format!("unknown node {node}"))
            })?;
            let c = ClientConn::connect_with(addr, self.tls.as_ref().map(|t| (t, node)))?;
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
        let bytes = match (&self.collection, req) {
            (
                Some(c),
                Request::Upsert(_)
                | Request::Delete(_)
                | Request::Get { .. }
                | Request::Query { .. }
                | Request::UpsertKeyed(_)
                | Request::DeleteKeys(_)
                | Request::GetKey { .. }
                | Request::DeleteWhere { .. },
            ) => Request::In {
                collection: c.clone(),
                req: Box::new(req.clone()),
            }
            .to_bytes(),
            _ => req.to_bytes(),
        };
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

    /// Inserts or replaces documents identified by text ids (ADR 0031): each document holds
    /// its text id in the reserved `_key` field.
    pub fn upsert_keyed(&mut self, docs: Vec<Document>) -> Result<Vec<Token>> {
        let r = self.call(&Request::UpsertKeyed(docs))?;
        self.expect_ack(r)
    }

    /// Deletes documents by text id (takedown).
    pub fn delete_keys(&mut self, keys: Vec<String>) -> Result<Vec<Token>> {
        let r = self.call(&Request::DeleteKeys(keys))?;
        self.expect_ack(r)
    }

    /// Deletes the documents of `scope` that match `filter`, on every shard the scope reaches
    /// (ADR 0031): the documents live when each shard applies the command, not a standing
    /// rule. Returns how many were removed, and the tokens.
    pub fn delete_where(
        &mut self,
        scope: DeleteScope,
        filter: Predicate,
    ) -> Result<(u64, Vec<Token>)> {
        let r = self.call(&Request::DeleteWhere {
            shard: None,
            scope,
            filter,
        })?;
        match r {
            Response::Deleted { count, tokens } => {
                let tokens = self.expect_ack(Response::Ack(tokens))?;
                Ok((count, tokens))
            }
            other => self.expect_ack(other).map(|t| (0, t)),
        }
    }

    /// Point read by text id, with the same consistency rules as [`Client::get`].
    pub fn get_key(&mut self, key: String, consistency: Consistency) -> Result<Option<Document>> {
        let tokens = if consistency == self.read_your_writes() {
            self.tokens()
        } else {
            Vec::new()
        };
        match self.call(&Request::GetKey {
            key,
            consistency,
            tokens,
        })? {
            Response::Doc(d) => Ok(d),
            Response::Error { message, .. } => Err(Error::Internal(message)),
            other => Err(Error::Internal(format!("unexpected response {other:?}"))),
        }
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

    /// Pauses or resumes merges on the node this client talks to (not the whole cluster).
    pub fn set_merges_paused(&mut self, paused: bool) -> Result<()> {
        match self.call(&Request::SetMergesPaused(paused))? {
            Response::Ack(_) => Ok(()),
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
