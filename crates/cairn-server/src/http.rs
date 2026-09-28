//! HTTP/JSON API (ADR 0023). A thin layer over the binary client protocol: every request goes
//! through a pooled [`cairn_client::Client`], which follows leader hints like any other
//! client, so the HTTP path adds no replication logic of its own.
//!
//! Documents are flat JSON objects keyed by field name, plus `id`. Writes return a
//! `consistency_token`; passing it back as `after` gives read-your-writes, which is how an
//! HTTP client that took a document down makes sure it never reads it again.

use axum::extract::{DefaultBodyLimit, Path, Query as UrlQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use cairn_client::Client;
use cairn_core::{
    DocId, Document, FieldKind, HashMap, LogIndex, NodeId, Predicate, Schema, ShardId, Value,
};
use cairn_proto::{Request, Response as Wire};
use cairn_query::{Consistency, Fusion, Hit, Query, ReplicaStatus, TextLeg, Token, VectorLeg};
use cairn_runtime::tls::ClientTls;
use serde::Deserialize;
use serde_json::{Map, Value as Json_, json};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

/// HTTP front-end settings.
#[derive(Clone)]
pub struct HttpConfig {
    /// Address to listen on.
    pub listen: SocketAddr,
    /// Every node of the cluster, for the internal client.
    pub nodes: HashMap<NodeId, SocketAddr>,
    /// The collection schema.
    pub schema: Schema,
    /// TLS for the internal client (the node's own certificate), when the cluster uses it.
    pub tls: Option<ClientTls>,
    /// Largest request body accepted.
    pub max_body_bytes: usize,
    /// This node's build slots, which carry its merge pause flag (`/v1/admin/merges`).
    pub job_slots: Arc<cairn_query::JobSlots>,
    /// Accepted API keys (ADR 0030); `None` serves without authentication (development only).
    pub auth: Option<Arc<crate::auth::ApiKeys>>,
    /// TLS for the HTTP port itself; `None` serves plain HTTP.
    pub https: Option<Arc<tokio_rustls::rustls::ServerConfig>>,
}

struct Shared {
    cfg: HttpConfig,
    pool: Mutex<Vec<Client>>,
}

impl Shared {
    fn client(&self) -> Client {
        self.pool
            .lock()
            .expect("pool")
            .pop()
            .unwrap_or_else(|| Client::new(self.cfg.nodes.clone()).with_tls(self.cfg.tls.clone()))
    }

    fn give_back(&self, c: Client) {
        let mut p = self.pool.lock().expect("pool");
        if p.len() < 64 {
            p.push(c);
        }
    }
}

/// An error answered as `{"error": message}`.
#[derive(Debug)]
pub struct ApiError(StatusCode, String);

impl ApiError {
    fn bad(msg: impl Into<String>) -> Self {
        ApiError(StatusCode::BAD_REQUEST, msg.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<cairn_core::Error> for ApiError {
    fn from(e: cairn_core::Error) -> Self {
        let msg = match &e {
            cairn_core::Error::Internal(m) => m.clone(),
            other => other.to_string(),
        };
        let status = if msg.starts_with("schema error") || msg.starts_with("invalid request") {
            StatusCode::BAD_REQUEST
        } else if matches!(e, cairn_core::Error::Io { .. }) {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        ApiError(status, msg)
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Starts the HTTP server on its own threads and returns the bound address.
pub fn start(cfg: HttpConfig) -> anyhow::Result<SocketAddr> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(64)
        .thread_name("cairn-http")
        .enable_all()
        .build()?;
    let listener = rt.block_on(tokio::net::TcpListener::bind(cfg.listen))?;
    let addr = listener.local_addr()?;
    let https = cfg.https.clone();
    let app = router(cfg);
    std::thread::Builder::new()
        .name("cairn-http".into())
        .spawn(move || {
            rt.block_on(async move {
                match https {
                    None => {
                        if let Err(e) = axum::serve(listener, app).await {
                            tracing::error!("http server stopped: {e}");
                        }
                    }
                    Some(tls) => serve_tls(listener, app, tls).await,
                }
            })
        })?;
    Ok(addr)
}

/// HTTPS: each connection does its TLS handshake in a task of its own (a slow client never
/// holds up the accept loop), then is served as HTTP/1.1.
async fn serve_tls(
    listener: tokio::net::TcpListener,
    app: Router,
    tls: Arc<tokio_rustls::rustls::ServerConfig>,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("https accept: {e}");
                continue;
            }
        };
        let (acceptor, app) = (acceptor.clone(), app.clone());
        tokio::spawn(async move {
            let stream = match tokio::time::timeout(
                std::time::Duration::from_secs(10),
                acceptor.accept(tcp),
            )
            .await
            {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    tracing::debug!(%peer, "tls handshake failed: {e}");
                    return;
                }
                Err(_) => return,
            };
            let service = hyper_util::service::TowerToHyperService::new(app);
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(%peer, "https connection: {e}");
            }
        });
    }
}

/// The API key that authenticated a request (its id), for the takedown audit trail.
#[derive(Clone)]
struct KeyId(String);

/// Checks the `Authorization` header against the node's keys and the role the route needs
/// (ADR 0030): 401 without a valid key, 403 without the role.
async fn authorize(
    State(s): State<Arc<Shared>>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let Some(keys) = &s.cfg.auth else {
        return next.run(req).await;
    };
    let Some(role) = crate::auth::required_role(req.method().as_str(), req.uri().path()) else {
        return next.run(req).await;
    };
    let header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let Some(key) = keys.authenticate(header) else {
        return (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, "Bearer")],
            Json(json!({ "error": "missing or invalid API key" })),
        )
            .into_response();
    };
    if !key.allows(role) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": format!("key {:?} lacks the {role:?} role", key.id) })),
        )
            .into_response();
    }
    req.extensions_mut().insert(KeyId(key.id.clone()));
    next.run(req).await
}

/// Records a takedown: which key asked, for which documents, and the token that proves it
/// (target `cairn_server::audit`, on by default).
fn audit_takedown(key: Option<&KeyId>, ids: &[u64], resp: &Json<Json_>) {
    let shown: Vec<u64> = ids.iter().copied().take(100).collect();
    tracing::info!(
        target: "cairn_server::audit",
        key = key.map_or("-", |k| k.0.as_str()),
        count = ids.len(),
        ids = ?shown,
        token = resp.0["consistency_token"].as_str().unwrap_or(""),
        "takedown"
    );
}

/// The routes, for tests and embedding.
pub fn router(cfg: HttpConfig) -> Router {
    let limit = cfg.max_body_bytes;
    let shared = Arc::new(Shared {
        cfg,
        pool: Mutex::new(Vec::new()),
    });
    Router::new()
        .route("/health", get(|| async { Json(json!({ "status": "ok" })) }))
        .route("/v1/schema", get(schema))
        .route("/v1/status", get(status))
        .route("/v1/documents", post(upsert))
        .route("/v1/documents/delete", post(delete_many))
        .route("/v1/documents/{id}", get(get_doc).delete(delete_one))
        .route("/v1/search", post(search))
        .route("/v1/admin/merges", get(merges).post(set_merges))
        .layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            authorize,
        ))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(shared)
}

/// Runs a blocking client call off the async threads; the client goes back to the pool only
/// after a successful call (a failed one may hold a broken connection).
async fn with_client<T: Send + 'static>(
    s: &Arc<Shared>,
    f: impl FnOnce(&mut Client) -> cairn_core::Result<T> + Send + 'static,
) -> ApiResult<T> {
    let s2 = s.clone();
    tokio::task::spawn_blocking(move || {
        let mut c = s2.client();
        let r = f(&mut c);
        if r.is_ok() {
            s2.give_back(c);
        }
        r
    })
    .await
    .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(ApiError::from)
}

async fn schema(State(s): State<Arc<Shared>>) -> Json<Json_> {
    Json(serde_json::to_value(&s.cfg.schema).unwrap_or(Json_::Null))
}

async fn status(State(s): State<Arc<Shared>>) -> ApiResult<Json<Json_>> {
    let st = with_client(&s, |c| c.status()).await?;
    Ok(Json(
        json!({ "replicas": st.iter().map(status_json).collect::<Vec<_>>() }),
    ))
}

fn status_json(s: &ReplicaStatus) -> Json_ {
    json!({
        "node": s.id.get(),
        "role": format!("{:?}", s.role).to_lowercase(),
        "term": s.term.get(),
        "leader": s.leader.map(|l| l.get()),
        "commit": s.commit.get(),
        "applied": s.applied.get(),
        "live_docs": s.live_docs,
        "memtable_bytes": s.memtable_bytes,
        "raft_log_bytes": s.raft_log_bytes,
        "segments": s.segments,
        "segments_built": s.flushes[0],
        "segments_fetched": s.flushes[1],
        "pending": s.flushes[2],
        "merges_running": s.merges[0],
        "merges_pending": s.merges[1],
        "merges_paused": s.merges_paused,
    })
}

#[derive(Deserialize)]
struct MergesBody {
    paused: bool,
}

/// Merge pause state of this node (the one serving the HTTP request).
async fn merges(State(s): State<Arc<Shared>>) -> Json<Json_> {
    Json(json!({ "paused": s.cfg.job_slots.merges_paused() }))
}

/// Pauses or resumes merges on this node only: call it on every node to pause the cluster.
/// Merges already running or committed complete; `/v1/status` shows when none is left.
async fn set_merges(State(s): State<Arc<Shared>>, Json(body): Json<MergesBody>) -> Json<Json_> {
    s.cfg.job_slots.set_merges_paused(body.paused);
    Json(json!({ "paused": body.paused }))
}

// ---------------------------------------------------------------- consistency tokens

/// Parses `shard.index,shard.index,...`.
fn parse_tokens(s: &str) -> ApiResult<Vec<Token>> {
    s.split(',')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (a, b) = p
                .split_once('.')
                .ok_or_else(|| ApiError::bad(format!("bad consistency token {p:?}")))?;
            Ok(Token {
                shard: ShardId(a.parse().map_err(|_| ApiError::bad("bad token shard"))?),
                index: LogIndex(b.parse().map_err(|_| ApiError::bad("bad token index"))?),
            })
        })
        .collect()
}

/// Per-shard maximum of both lists, formatted.
fn merge_tokens(a: &[Token], b: &[Token]) -> String {
    let mut m: std::collections::BTreeMap<u32, u64> = Default::default();
    for t in a.iter().chain(b) {
        let e = m.entry(t.shard.get()).or_default();
        *e = (*e).max(t.index.get());
    }
    m.iter()
        .map(|(s, i)| format!("{s}.{i}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Consistency from its name and an optional token. Default: read-your-writes when a token is
/// given, linearizable otherwise (correct by default; `stale` is opt-in).
fn consistency(name: Option<&str>, after: Option<&str>) -> ApiResult<(Consistency, Vec<Token>)> {
    let tokens = after.map(parse_tokens).transpose()?.unwrap_or_default();
    let ryw = Consistency::ReadYourWrites(Token {
        shard: ShardId(0),
        index: LogIndex(0),
    });
    let c = match (name, tokens.is_empty()) {
        (None, false) | (Some("read_your_writes"), _) => ryw,
        (None, true) | (Some("linearizable"), _) => Consistency::Linearizable,
        (Some("stale"), _) => Consistency::Stale,
        (Some(other), _) => {
            return Err(ApiError::bad(format!(
                "consistency must be stale, read_your_writes or linearizable, not {other:?}"
            )));
        }
    };
    Ok((c, tokens))
}

// ---------------------------------------------------------------- documents <-> JSON

fn field_value(kind: &FieldKind, name: &str, v: &Json_) -> ApiResult<Option<Value>> {
    let bad = || ApiError::bad(format!("field {name:?}: expected {}", kind_name(kind)));
    if v.is_null() {
        return Ok(None);
    }
    Ok(Some(match kind {
        FieldKind::Vector { dims, .. } => {
            let a = v.as_array().ok_or_else(bad)?;
            if a.len() != *dims as usize {
                return Err(ApiError::bad(format!(
                    "field {name:?}: vector has {} dimensions, schema says {dims}",
                    a.len()
                )));
            }
            Value::Vector(
                a.iter()
                    .map(|x| x.as_f64().map(|f| f as f32).ok_or_else(bad))
                    .collect::<ApiResult<_>>()?,
            )
        }
        FieldKind::Text => Value::Text(v.as_str().ok_or_else(bad)?.to_owned()),
        FieldKind::I64 => Value::I64(v.as_i64().ok_or_else(bad)?),
        FieldKind::F64 => Value::F64(v.as_f64().ok_or_else(bad)?),
        FieldKind::Bool => Value::Bool(v.as_bool().ok_or_else(bad)?),
        FieldKind::Date => Value::Date(v.as_i64().ok_or_else(bad)?),
        FieldKind::Enum => Value::Enum(v.as_str().ok_or_else(bad)?.to_owned()),
        FieldKind::Set => Value::Set(
            v.as_array()
                .ok_or_else(bad)?
                .iter()
                .map(|x| x.as_str().map(str::to_owned).ok_or_else(bad))
                .collect::<ApiResult<_>>()?,
        ),
        FieldKind::Blob => Value::Blob(
            base64::engine::general_purpose::STANDARD
                .decode(v.as_str().ok_or_else(bad)?)
                .map_err(|_| bad())?
                .into(),
        ),
    }))
}

fn kind_name(k: &FieldKind) -> &'static str {
    match k {
        FieldKind::Vector { .. } => "an array of numbers",
        FieldKind::Text | FieldKind::Enum => "a string",
        FieldKind::I64 | FieldKind::Date => "an integer",
        FieldKind::F64 => "a number",
        FieldKind::Bool => "a boolean",
        FieldKind::Set => "an array of strings",
        FieldKind::Blob => "a base64 string",
    }
}

fn value_json(v: &Value) -> Json_ {
    match v {
        Value::Vector(x) => json!(x),
        Value::Text(s) | Value::Enum(s) => json!(s),
        Value::I64(i) | Value::Date(i) => json!(i),
        Value::F64(f) => json!(f),
        Value::Bool(b) => json!(b),
        Value::Set(s) => json!(s),
        Value::Blob(b) => json!(base64::engine::general_purpose::STANDARD.encode(b)),
    }
}

fn doc_from_json(schema: &Schema, v: &Json_) -> ApiResult<Document> {
    let obj = v
        .as_object()
        .ok_or_else(|| ApiError::bad("a document is a JSON object"))?;
    let id = obj
        .get("id")
        .and_then(Json_::as_u64)
        .ok_or_else(|| ApiError::bad("a document needs an unsigned integer \"id\""))?;
    let mut d = Document::new(DocId(id), schema.fields.len());
    for (k, v) in obj {
        if k == "id" {
            continue;
        }
        let i = schema
            .index_of(k)
            .ok_or_else(|| ApiError::bad(format!("unknown field {k:?}")))?;
        if let Some(val) = field_value(&schema.fields[i].kind, k, v)? {
            d = d.set(i, val);
        }
    }
    Ok(d)
}

fn doc_json(schema: &Schema, d: &Document) -> Json_ {
    let mut m = Map::new();
    m.insert("id".into(), json!(d.id.get()));
    for (f, v) in schema.fields.iter().zip(&d.values) {
        if let Some(v) = v {
            m.insert(f.name.clone(), value_json(v));
        }
    }
    Json_::Object(m)
}

// ---------------------------------------------------------------- filters

/// `{"and": [..]}`, `{"or": [..]}`, `{"not": {..}}`, or `{"field": name, op: value, ...}` with
/// ops `eq`, `in`, `gt`, `gte`, `lt`, `lte`, `is_null`.
fn filter(schema: &Schema, v: &Json_) -> ApiResult<Predicate> {
    if v.is_null() {
        return Ok(Predicate::True);
    }
    let obj = v
        .as_object()
        .ok_or_else(|| ApiError::bad("a filter is a JSON object"))?;
    let list = |x: &Json_| -> ApiResult<Vec<Predicate>> {
        x.as_array()
            .ok_or_else(|| ApiError::bad("\"and\" and \"or\" take an array"))?
            .iter()
            .map(|p| filter(schema, p))
            .collect()
    };
    if let Some(x) = obj.get("and") {
        return Ok(Predicate::And(list(x)?));
    }
    if let Some(x) = obj.get("or") {
        return Ok(Predicate::Or(list(x)?));
    }
    if let Some(x) = obj.get("not") {
        return Ok(Predicate::Not(Box::new(filter(schema, x)?)));
    }
    let name = obj
        .get("field")
        .and_then(Json_::as_str)
        .ok_or_else(|| ApiError::bad("a filter needs \"and\", \"or\", \"not\" or \"field\""))?;
    let field = schema
        .index_of(name)
        .ok_or_else(|| ApiError::bad(format!("unknown field {name:?}")))?;
    let kind = &schema.fields[field].kind;
    // A `Set` field compares against single values.
    let scalar = match kind {
        FieldKind::Set => &FieldKind::Enum,
        k => k,
    };
    let one = |x: &Json_| -> ApiResult<Value> {
        field_value(scalar, name, x)?.ok_or_else(|| ApiError::bad("null is not a filter value"))
    };
    let mut parts = Vec::new();
    let (mut lo, mut hi) = (None, None);
    for (op, x) in obj {
        match op.as_str() {
            "field" => {}
            "eq" => parts.push(Predicate::Eq {
                field,
                value: one(x)?,
            }),
            "in" => parts.push(Predicate::In {
                field,
                values: x
                    .as_array()
                    .ok_or_else(|| ApiError::bad("\"in\" takes an array"))?
                    .iter()
                    .map(&one)
                    .collect::<ApiResult<_>>()?,
            }),
            "gt" => lo = Some((one(x)?, false)),
            "gte" => lo = Some((one(x)?, true)),
            "lt" => hi = Some((one(x)?, false)),
            "lte" => hi = Some((one(x)?, true)),
            "is_null" => {
                let p = Predicate::IsNull { field };
                parts.push(if x.as_bool() == Some(false) {
                    Predicate::Not(Box::new(p))
                } else {
                    p
                });
            }
            other => return Err(ApiError::bad(format!("unknown filter operator {other:?}"))),
        }
    }
    if lo.is_some() || hi.is_some() {
        parts.push(Predicate::Range {
            field,
            lo_inclusive: lo.as_ref().is_some_and(|l| l.1),
            hi_inclusive: hi.as_ref().is_some_and(|h| h.1),
            lo: lo.map(|l| l.0),
            hi: hi.map(|h| h.0),
        });
    }
    match parts.len() {
        0 => Err(ApiError::bad(format!("filter on {name:?} has no operator"))),
        1 => Ok(parts.pop().expect("one")),
        _ => Ok(Predicate::And(parts)),
    }
}

// ---------------------------------------------------------------- handlers

#[derive(Deserialize)]
struct UpsertBody {
    documents: Vec<Json_>,
    #[serde(default)]
    after: Option<String>,
}

/// `count` documents written; the token covers them and every token passed as `after`.
fn ack(count: usize, written: &[Token], after: Option<&str>) -> ApiResult<Json<Json_>> {
    let prior = after.map(parse_tokens).transpose()?.unwrap_or_default();
    Ok(Json(json!({
        "count": count,
        "consistency_token": merge_tokens(&prior, written),
    })))
}

async fn upsert(
    State(s): State<Arc<Shared>>,
    Json(body): Json<UpsertBody>,
) -> ApiResult<Json<Json_>> {
    let docs = body
        .documents
        .iter()
        .map(|d| doc_from_json(&s.cfg.schema, d))
        .collect::<ApiResult<Vec<_>>>()?;
    if docs.is_empty() {
        return Err(ApiError::bad("no documents"));
    }
    let n = docs.len();
    let tokens = with_client(&s, move |c| c.upsert(docs)).await?;
    ack(n, &tokens, body.after.as_deref())
}

#[derive(Deserialize)]
struct DeleteBody {
    ids: Vec<u64>,
    #[serde(default)]
    after: Option<String>,
}

async fn delete_many(
    State(s): State<Arc<Shared>>,
    key: Option<axum::Extension<KeyId>>,
    Json(body): Json<DeleteBody>,
) -> ApiResult<Json<Json_>> {
    if body.ids.is_empty() {
        return Err(ApiError::bad("no ids"));
    }
    let ids: Vec<DocId> = body.ids.iter().copied().map(DocId).collect();
    let n = ids.len();
    let tokens = with_client(&s, move |c| c.delete(ids)).await?;
    let resp = ack(n, &tokens, body.after.as_deref())?;
    audit_takedown(key.as_deref(), &body.ids, &resp);
    Ok(resp)
}

#[derive(Deserialize)]
struct ReadParams {
    consistency: Option<String>,
    after: Option<String>,
}

async fn delete_one(
    State(s): State<Arc<Shared>>,
    key: Option<axum::Extension<KeyId>>,
    Path(id): Path<u64>,
    UrlQuery(p): UrlQuery<ReadParams>,
) -> ApiResult<Json<Json_>> {
    let tokens = with_client(&s, move |c| c.delete(vec![DocId(id)])).await?;
    let resp = ack(1, &tokens, p.after.as_deref())?;
    audit_takedown(key.as_deref(), &[id], &resp);
    Ok(resp)
}

async fn get_doc(
    State(s): State<Arc<Shared>>,
    Path(id): Path<u64>,
    UrlQuery(p): UrlQuery<ReadParams>,
) -> ApiResult<Response> {
    let (consistency, tokens) = consistency(p.consistency.as_deref(), p.after.as_deref())?;
    let doc = with_client(&s, move |c| {
        match c.call(&Request::Get {
            id: DocId(id),
            consistency,
            tokens,
        })? {
            Wire::Doc(d) => Ok(d),
            Wire::Error { message, .. } => Err(cairn_core::Error::Internal(message)),
            other => Err(cairn_core::Error::Internal(format!(
                "unexpected response {other:?}"
            ))),
        }
    })
    .await?;
    Ok(match doc {
        Some(d) => Json(doc_json(&s.cfg.schema, &d)).into_response(),
        None => ApiError(StatusCode::NOT_FOUND, format!("no document {id}")).into_response(),
    })
}

#[derive(Deserialize)]
struct VectorBody {
    field: String,
    values: Vec<f32>,
    #[serde(default)]
    ef: u32,
}

#[derive(Deserialize)]
struct TextBody {
    field: String,
    query: String,
    #[serde(default)]
    all_terms: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FusionBody {
    Rrf { k: f32 },
    Weighted(Vec<f32>),
}

#[derive(Deserialize)]
struct SearchBody {
    #[serde(default = "default_k")]
    k: usize,
    #[serde(default)]
    vector: Option<VectorBody>,
    #[serde(default)]
    vectors: Vec<VectorBody>,
    #[serde(default)]
    text: Option<TextBody>,
    #[serde(default)]
    filter: Json_,
    #[serde(default)]
    fusion: Option<FusionBody>,
    #[serde(default)]
    oversample: Option<usize>,
    #[serde(default = "yes")]
    with_documents: bool,
    #[serde(default)]
    consistency: Option<String>,
    #[serde(default)]
    after: Option<String>,
}

fn default_k() -> usize {
    10
}

fn yes() -> bool {
    true
}

fn hit_json(schema: &Schema, h: &Hit) -> Json_ {
    let mut m = Map::new();
    m.insert("id".into(), json!(h.doc_id.get()));
    m.insert("score".into(), json!(h.score));
    m.insert(
        "legs".into(),
        json!(
            h.legs
                .iter()
                .map(|l| l.map(|l| json!({ "rank": l.rank, "score": l.score })))
                .collect::<Vec<_>>()
        ),
    );
    if let Some(d) = &h.document {
        m.insert("document".into(), doc_json(schema, d));
    }
    Json_::Object(m)
}

async fn search(
    State(s): State<Arc<Shared>>,
    Json(body): Json<SearchBody>,
) -> ApiResult<Json<Json_>> {
    let schema = &s.cfg.schema;
    if body.k == 0 || body.k > 10_000 {
        return Err(ApiError::bad("k must be between 1 and 10000"));
    }
    let mut q = Query::new(body.k);
    for v in body.vector.iter().chain(&body.vectors) {
        let field = schema
            .index_of(&v.field)
            .ok_or_else(|| ApiError::bad(format!("unknown field {:?}", v.field)))?;
        match schema.fields[field].kind {
            FieldKind::Vector { dims, .. } if dims as usize == v.values.len() => {}
            FieldKind::Vector { dims, .. } => {
                return Err(ApiError::bad(format!(
                    "query vector has {} dimensions, {:?} has {dims}",
                    v.values.len(),
                    v.field
                )));
            }
            _ => {
                return Err(ApiError::bad(format!(
                    "{:?} is not a vector field",
                    v.field
                )));
            }
        }
        q.vectors.push(VectorLeg {
            field,
            vector: v.values.clone(),
            ef: v.ef,
        });
    }
    if let Some(t) = &body.text {
        let field = schema
            .index_of(&t.field)
            .ok_or_else(|| ApiError::bad(format!("unknown field {:?}", t.field)))?;
        if !matches!(schema.fields[field].kind, FieldKind::Text) {
            return Err(ApiError::bad(format!("{:?} is not a text field", t.field)));
        }
        q.text = Some(TextLeg {
            field,
            text: t.query.clone(),
            all_terms: t.all_terms,
        });
    }
    q.filter = filter(schema, &body.filter)?;
    if let Some(f) = body.fusion {
        q.fusion = match f {
            FusionBody::Rrf { k } => Fusion::Rrf { k },
            FusionBody::Weighted(w) => {
                if w.len() != q.leg_count() {
                    return Err(ApiError::bad(
                        "one fusion weight per leg (vectors, then text)",
                    ));
                }
                Fusion::Weighted { weights: w }
            }
        };
    }
    if let Some(o) = body.oversample {
        q.oversample = o.clamp(1, 64);
    }
    q.with_documents = body.with_documents;
    let (consistency, tokens) = consistency(body.consistency.as_deref(), body.after.as_deref())?;
    let hits = with_client(&s, move |c| {
        match c.call(&Request::Query {
            query: q,
            consistency,
            tokens,
        })? {
            Wire::Hits(h) => Ok(h),
            Wire::Error { message, .. } => Err(cairn_core::Error::Internal(message)),
            other => Err(cairn_core::Error::Internal(format!(
                "unexpected response {other:?}"
            ))),
        }
    })
    .await?;
    Ok(Json(json!({
        "hits": hits.iter().map(|h| hit_json(schema, h)).collect::<Vec<_>>()
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::{FieldDef, Metric};

    fn schema() -> Schema {
        Schema::new(vec![
            FieldDef {
                name: "v".into(),
                kind: FieldKind::Vector {
                    dims: 2,
                    metric: Metric::L2,
                },
            },
            FieldDef {
                name: "tags".into(),
                kind: FieldKind::Set,
            },
            FieldDef {
                name: "n".into(),
                kind: FieldKind::I64,
            },
            FieldDef {
                name: "b".into(),
                kind: FieldKind::Blob,
            },
        ])
        .unwrap()
    }

    #[test]
    fn documents_round_trip_through_json() {
        let s = schema();
        let j = json!({ "id": 7, "v": [1.0, 2.5], "tags": ["a", "b"], "n": -3, "b": "AAEC" });
        let d = doc_from_json(&s, &j).unwrap();
        assert_eq!(d.values[3], Some(Value::Blob(vec![0u8, 1, 2].into())));
        assert_eq!(doc_json(&s, &d), j);
        assert!(doc_from_json(&s, &json!({ "id": 1, "v": [1.0] })).is_err());
        assert!(doc_from_json(&s, &json!({ "id": 1, "nope": 1 })).is_err());
        assert!(doc_from_json(&s, &json!({ "v": [1.0, 2.0] })).is_err());
    }

    #[test]
    fn filters_parse() {
        let s = schema();
        let p = filter(
            &s,
            &json!({ "and": [ { "field": "tags", "in": ["a"] }, { "field": "n", "gte": 1, "lt": 5 } ] }),
        )
        .unwrap();
        assert_eq!(
            p,
            Predicate::And(vec![
                Predicate::In {
                    field: 1,
                    values: vec![Value::Enum("a".into())]
                },
                Predicate::Range {
                    field: 2,
                    lo: Some(Value::I64(1)),
                    hi: Some(Value::I64(5)),
                    lo_inclusive: true,
                    hi_inclusive: false
                },
            ])
        );
        assert!(filter(&s, &json!({ "field": "n", "eq": "x" })).is_err());
        assert!(filter(&s, &json!({ "field": "n", "near": 1 })).is_err());
        assert_eq!(filter(&s, &Json_::Null).unwrap(), Predicate::True);
    }

    #[test]
    fn tokens_merge_per_shard() {
        let a = parse_tokens("0.5,2.9").unwrap();
        let b = parse_tokens("0.7,1.1").unwrap();
        assert_eq!(merge_tokens(&a, &b), "0.7,1.1,2.9");
        assert!(parse_tokens("x").is_err());
        let (c, t) = consistency(None, Some("0.3")).unwrap();
        assert!(matches!(c, Consistency::ReadYourWrites(_)) && t.len() == 1);
        assert_eq!(
            consistency(None, None).unwrap().0,
            Consistency::Linearizable
        );
        assert!(consistency(Some("eventual"), None).is_err());
    }
}
