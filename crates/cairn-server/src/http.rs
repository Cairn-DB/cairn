//! HTTP/JSON API (ADR 0023). A thin layer over the binary client protocol: every request goes
//! through a pooled [`cairn_client::Client`], which follows leader hints like any other
//! client, so the HTTP path adds no replication logic of its own.
//!
//! Documents are flat JSON objects keyed by field name, plus `id`. Writes return a
//! `consistency_token`; passing it back as `after` gives read-your-writes, which is how an
//! HTTP client that took a document down makes sure it never reads it again.
//!
//! Tenants (ADR 0031): a request acts for one tenant when its key is scoped to it, or when an
//! unscoped key names one in the `Cairn-Tenant` header. Its documents are then stored under
//! text ids prefixed with the tenant, carry the tenant in `_tenant`, and every read, search
//! and deletion is restricted to that tenant.

use crate::catalog::{self, CollectionDef};
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
use cairn_proto::{DeleteScope, PatchOp, PatchTarget, Request, Response as Wire};
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
    /// Key signing proofs of deletion (ADR 0031); `None`: the proof route is off.
    pub proof_key: Option<Arc<ring::signature::Ed25519KeyPair>>,
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

/// The tenant a request acts for (ADR 0031), and whether its key imposes it.
#[derive(Clone, Default)]
struct Scope {
    tenant: Option<String>,
    from_key: bool,
}

/// Header through which an unscoped key acts for one tenant.
const TENANT_HEADER: &str = "cairn-tenant";

fn forbidden(msg: String) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({ "error": msg }))).into_response()
}

/// Checks the `Authorization` header against the node's keys and the role the route needs
/// (ADR 0030): 401 without a valid key, 403 without the role. Sets the request's tenant scope.
async fn authorize(
    State(s): State<Arc<Shared>>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let named = match req.headers().get(TENANT_HEADER).map(|v| v.to_str()) {
        None => None,
        Some(Ok(t)) if crate::auth::valid_tenant(t) => Some(t.to_owned()),
        Some(_) => {
            return ApiError::bad("Cairn-Tenant: 1 to 128 letters, digits, '_', '.' or '-'")
                .into_response();
        }
    };
    let Some(keys) = &s.cfg.auth else {
        req.extensions_mut().insert(Scope {
            tenant: named,
            from_key: false,
        });
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
        return forbidden(format!("key {:?} lacks the {role:?} role", key.id));
    }
    let scope = match (&key.tenant, named) {
        (Some(own), Some(other)) if *own != other => {
            return forbidden(format!("key {:?} cannot act for tenant {other:?}", key.id));
        }
        (Some(own), _) => Scope {
            tenant: Some(own.clone()),
            from_key: true,
        },
        (None, named) => Scope {
            tenant: named,
            from_key: false,
        },
    };
    req.extensions_mut().insert(KeyId(key.id.clone()));
    req.extensions_mut().insert(scope);
    next.run(req).await
}

/// Path parameters of a route (`c`, `id`, `tenant`), absent on routes without any.
type Params = Option<Path<std::collections::HashMap<String, String>>>;

fn param<'a>(p: &'a Params, name: &str) -> Option<&'a str> {
    p.as_ref().and_then(|p| p.0.get(name)).map(String::as_str)
}

/// The collection a request addresses: `/v1/collections/{c}/...`, or `default`.
async fn collection(s: &Arc<Shared>, p: &Params) -> ApiResult<Arc<CollectionDef>> {
    let name = param(p, "c").unwrap_or(catalog::DEFAULT).to_owned();
    if let Some(c) = catalog::current().get(&name) {
        return Ok(c);
    }
    // Created through another node: a linearizable listing brings it here.
    refresh_catalog(s).await?;
    catalog::current()
        .get(&name)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no collection {name:?}")))
}

/// Records a takedown: which key asked, for which documents, and the token that proves it
/// (target `cairn_server::audit`, on by default).
fn audit_takedown(key: Option<&KeyId>, scope: &Scope, ids: &[ApiId], resp: &Json<Json_>) {
    let shown: Vec<String> = ids.iter().take(100).map(ApiId::to_string).collect();
    tracing::info!(
        target: "cairn_server::audit",
        key = key.map_or("-", |k| k.0.as_str()),
        tenant = scope.tenant.as_deref().unwrap_or("-"),
        count = ids.len(),
        ids = %format!("[{}]", shown.join(", ")),
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
        .route("/v1/documents/patch", post(patch_many))
        .route(
            "/v1/documents/{id}",
            get(get_doc).delete(delete_one).patch(patch_one),
        )
        .route("/v1/search", post(search))
        .route("/v1/tenants/{tenant}", axum::routing::delete(erase_tenant))
        .route("/v1/deletions/proof", post(deletion_proof))
        .route("/v1/deletions/key", get(proof_key))
        .route("/v1/collections/{c}/deletions/proof", post(deletion_proof))
        .route(
            "/v1/collections",
            get(list_collections).post(create_collection),
        )
        .route(
            "/v1/collections/{c}",
            get(get_collection).delete(drop_collection),
        )
        .route("/v1/collections/{c}/schema", get(schema))
        .route("/v1/collections/{c}/documents", post(upsert))
        .route("/v1/collections/{c}/documents/delete", post(delete_many))
        .route("/v1/collections/{c}/documents/patch", post(patch_many))
        .route(
            "/v1/collections/{c}/documents/{id}",
            get(get_doc).delete(delete_one).patch(patch_one),
        )
        .route("/v1/collections/{c}/search", post(search))
        .route(
            "/v1/collections/{c}/tenants/{tenant}",
            axum::routing::delete(erase_tenant),
        )
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
    with_client_in(s, catalog::DEFAULT, f).await
}

/// [`with_client`], with document requests sent to `collection`.
async fn with_client_in<T: Send + 'static>(
    s: &Arc<Shared>,
    collection: &str,
    f: impl FnOnce(&mut Client) -> cairn_core::Result<T> + Send + 'static,
) -> ApiResult<T> {
    let s2 = s.clone();
    let collection = collection.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut c = s2.client();
        c.set_collection(Some(collection));
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

/// The schema as the client wrote it: reserved fields (ADR 0031) are internal.
/// A schema as the client wrote it: reserved fields (ADR 0031) are internal.
fn visible_schema(schema: &Schema) -> Json_ {
    let visible = Schema {
        fields: schema
            .fields
            .iter()
            .filter(|f| !cairn_core::schema::is_reserved(&f.name))
            .cloned()
            .collect(),
    };
    serde_json::to_value(&visible).unwrap_or(Json_::Null)
}

async fn schema(State(s): State<Arc<Shared>>, params: Params) -> ApiResult<Json<Json_>> {
    let coll = collection(&s, &params).await?;
    Ok(Json(visible_schema(&coll.schema)))
}

fn collection_json(c: &CollectionDef) -> Json_ {
    let mut j = json!({ "name": c.name, "shards": c.shards, "schema": visible_schema(&c.schema) });
    if let Some(f) = &c.expires_field {
        j["expires_field"] = json!(f);
    }
    j
}

/// Wall-clock time for retention (ADR 0031), in Unix milliseconds.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// `p`, restricted to documents not expired now: expired documents are hidden at once,
/// before the shard leaders delete them.
fn unexpired(coll: &CollectionDef, p: Predicate) -> Predicate {
    match coll.expired(now_ms()) {
        None => p,
        Some(expired) => {
            let live = Predicate::Not(Box::new(expired));
            if matches!(p, Predicate::True) {
                live
            } else {
                Predicate::And(vec![p, live])
            }
        }
    }
}

/// Whether a document has expired.
fn is_expired(coll: &CollectionDef, d: &Document) -> bool {
    coll.expired(now_ms()).is_some_and(|p| p.matches(d))
}

#[derive(Deserialize)]
struct CreateCollectionBody {
    name: String,
    schema: Json_,
    #[serde(default)]
    shards: Option<u32>,
    /// Retention: a field holding each document's expiry, in Unix milliseconds.
    #[serde(default)]
    expires_field: Option<String>,
}

/// Creates a collection (ADR 0031): answers once every shard is ready.
async fn create_collection(
    State(s): State<Arc<Shared>>,
    Json(body): Json<CreateCollectionBody>,
) -> ApiResult<Response> {
    let shards = match body.shards {
        Some(n) => n,
        None => catalog::current()
            .get(catalog::DEFAULT)
            .map_or(1, |d| d.shards),
    };
    let schema = body.schema.to_string();
    let name = body.name.clone();
    let expires = body.expires_field.clone();
    let def = with_client(&s, move |c| {
        c.create_collection(&name, &schema, shards, expires.as_deref())
    })
    .await
    .map_err(|e| {
        if e.1.contains("already exists") {
            ApiError(StatusCode::CONFLICT, e.1)
        } else if e.1.contains("collection name")
            || e.1.contains("shards must")
            || e.1.contains("schema")
            || e.1.contains("expires_field")
        {
            ApiError(StatusCode::BAD_REQUEST, e.1)
        } else {
            e
        }
    })?;
    let def: CollectionDef = serde_json::from_str(&def)
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    tracing::info!(target: "cairn_server::audit", collection = %def.name, shards = def.shards, "collection created");
    Ok((StatusCode::CREATED, Json(collection_json(&def))).into_response())
}

/// Lists the live collections (a linearizable read, on whichever node answers) and merges the
/// listing into this node's view, so that a drop is seen here at once.
async fn refresh_catalog(s: &Arc<Shared>) -> ApiResult<Vec<Arc<CollectionDef>>> {
    let defs = with_client(s, |c| c.list_collections()).await?;
    let defs: Vec<Arc<CollectionDef>> = defs
        .iter()
        .filter_map(|d| serde_json::from_str::<CollectionDef>(d).ok())
        .map(Arc::new)
        .collect();
    catalog::observe(&defs, &[], true);
    Ok(defs)
}

async fn list_collections(State(s): State<Arc<Shared>>) -> ApiResult<Json<Json_>> {
    let defs = refresh_catalog(&s).await?;
    Ok(Json(json!({
        "collections": defs.iter().map(|d| collection_json(d)).collect::<Vec<_>>()
    })))
}

async fn get_collection(State(s): State<Arc<Shared>>, params: Params) -> ApiResult<Json<Json_>> {
    let coll = collection(&s, &params).await?;
    Ok(Json(collection_json(&coll)))
}

/// Drops a collection and deletes its data on every node (admin). Audited.
async fn drop_collection(
    State(s): State<Arc<Shared>>,
    key: Option<axum::Extension<KeyId>>,
    params: Params,
) -> ApiResult<Json<Json_>> {
    let coll = collection(&s, &params).await?;
    let name = coll.name.clone();
    let def = with_client(&s, move |c| c.drop_collection(&name))
        .await
        .map_err(|e| {
            if e.1.contains("cannot be dropped") {
                ApiError(StatusCode::BAD_REQUEST, e.1)
            } else if e.1.contains("no collection") {
                ApiError(StatusCode::NOT_FOUND, e.1)
            } else {
                e
            }
        })?;
    let def: CollectionDef = serde_json::from_str(&def)
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    tracing::info!(
        target: "cairn_server::audit",
        key = key.as_deref().map_or("-", |k| k.0.as_str()),
        collection = %def.name,
        "collection dropped"
    );
    Ok(Json(json!({ "dropped": def.name })))
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

/// A document id as the client writes it (ADR 0031): an unsigned integer below 2^63, or a text
/// id (a non-empty string of at most 1024 bytes).
#[derive(Debug, Clone, PartialEq)]
enum ApiId {
    Num(u64),
    Key(String),
}

impl std::fmt::Display for ApiId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiId::Num(n) => write!(f, "{n}"),
            ApiId::Key(k) => write!(f, "{k:?}"),
        }
    }
}

const MAX_KEY_BYTES: usize = 1024;

fn api_key(k: &str) -> ApiResult<ApiId> {
    if k.is_empty() || k.len() > MAX_KEY_BYTES {
        return Err(ApiError::bad(format!(
            "a text id has 1 to {MAX_KEY_BYTES} bytes"
        )));
    }
    if k.contains(TENANT_SEP) {
        return Err(ApiError::bad("a text id cannot contain U+001F"));
    }
    Ok(ApiId::Key(k.to_owned()))
}

/// An id in a JSON body: a number, or a string (a text id).
fn api_id(v: &Json_) -> ApiResult<ApiId> {
    match v {
        Json_::String(k) => api_key(k),
        _ => match v.as_u64() {
            Some(n) if n < DocId::KEYED_BIT => Ok(ApiId::Num(n)),
            _ => Err(ApiError::bad(
                "an id is an unsigned integer below 2^63 or a non-empty string",
            )),
        },
    }
}

/// An id in a URL path: digits are an integer id (compatibility with 0.1), anything else a text
/// id. A text id made only of digits is reached with `?id_type=text`.
fn path_id(p: &str, id_type: Option<&str>) -> ApiResult<ApiId> {
    match id_type {
        Some("text") => api_key(p),
        Some("int") | None => match p.parse::<u64>() {
            Ok(n) if n < DocId::KEYED_BIT => Ok(ApiId::Num(n)),
            Ok(_) => Err(ApiError::bad("integer ids are below 2^63")),
            Err(_) if id_type.is_none() => api_key(p),
            Err(_) => Err(ApiError::bad(format!("{p:?} is not an integer id"))),
        },
        Some(other) => Err(ApiError::bad(format!(
            "id_type is text or int, not {other:?}"
        ))),
    }
}

/// Separates a tenant from the id in a stored text id (ADR 0031). Client text ids cannot hold
/// it, so a tenant's ids never meet ids written without a tenant.
const TENANT_SEP: char = '\u{1f}';

/// The id stored for `id` under `tenant`: a text id `<tenant>U+001F#<digits>` for an integer,
/// `<tenant>U+001F$<text>` for a text id. Without a tenant, the id itself.
fn stored_id(tenant: Option<&str>, id: &ApiId) -> ApiId {
    match (tenant, id) {
        (None, id) => id.clone(),
        (Some(t), ApiId::Num(n)) => ApiId::Key(format!("{t}{TENANT_SEP}#{n}")),
        (Some(t), ApiId::Key(k)) => ApiId::Key(format!("{t}{TENANT_SEP}${k}")),
    }
}

/// The id a client wrote, and its tenant, from a stored text id.
fn split_stored(k: &str) -> (Json_, Option<&str>) {
    match k.split_once(TENANT_SEP) {
        None => (json!(k), None),
        Some((t, rest)) => {
            let id = match (rest.strip_prefix('#'), rest.strip_prefix('$')) {
                (Some(d), _) => d.parse::<u64>().map_or_else(|_| json!(d), |n| json!(n)),
                (None, Some(text)) => json!(text),
                (None, None) => json!(rest),
            };
            (id, Some(t))
        }
    }
}

/// The filter restricting a request to its tenant, if any.
fn tenant_filter(schema: &Schema, tenant: Option<&str>) -> ApiResult<Option<Predicate>> {
    let Some(t) = tenant else {
        return Ok(None);
    };
    let field = schema
        .index_of(cairn_core::schema::TENANT_FIELD)
        .ok_or_else(|| ApiError::bad("this collection has no tenants"))?;
    Ok(Some(Predicate::Eq {
        field,
        value: Value::Enum(t.to_owned()),
    }))
}

/// `p`, restricted to the request's tenant.
fn scoped(schema: &Schema, tenant: Option<&str>, p: Predicate) -> ApiResult<Predicate> {
    Ok(match tenant_filter(schema, tenant)? {
        None => p,
        Some(t) if matches!(p, Predicate::True) => t,
        Some(t) => Predicate::And(vec![p, t]),
    })
}

/// A document from JSON, with the id it is stored under (see [`stored_id`]).
fn doc_from_json(schema: &Schema, tenant: Option<&str>, v: &Json_) -> ApiResult<(ApiId, Document)> {
    let obj = v
        .as_object()
        .ok_or_else(|| ApiError::bad("a document is a JSON object"))?;
    let id = stored_id(
        tenant,
        &api_id(
            obj.get("id")
                .ok_or_else(|| ApiError::bad("a document needs an \"id\""))?,
        )?,
    );
    let mut d = Document::new(
        match id {
            ApiId::Num(n) => DocId(n),
            ApiId::Key(_) => DocId(0),
        },
        schema.fields.len(),
    );
    for (k, v) in obj {
        if k == "id" {
            continue;
        }
        if cairn_core::schema::is_reserved(k) {
            return Err(ApiError::bad(format!(
                "field {k:?}: names starting with '_' are reserved"
            )));
        }
        let i = schema
            .index_of(k)
            .ok_or_else(|| ApiError::bad(format!("unknown field {k:?}")))?;
        if let Some(val) = field_value(&schema.fields[i].kind, k, v)? {
            d = d.set(i, val);
        }
    }
    if let ApiId::Key(k) = &id {
        let i = schema
            .index_of(cairn_core::schema::KEY_FIELD)
            .ok_or_else(|| ApiError::bad("this collection has no text ids: use integer ids"))?;
        d = d.set(i, Value::Blob(bytes::Bytes::from(k.clone().into_bytes())));
    }
    if let Some(t) = tenant {
        let i = schema
            .index_of(cairn_core::schema::TENANT_FIELD)
            .ok_or_else(|| ApiError::bad("this collection has no tenants"))?;
        d = d.set(i, Value::Enum(t.to_owned()));
    }
    Ok((id, d))
}

/// The id a client sees: the id it wrote (without its tenant), else the integer.
fn id_json(schema: &Schema, d: &Document) -> Json_ {
    if let Some(i) = schema.index_of(cairn_core::schema::KEY_FIELD)
        && let Some(Some(Value::Blob(b))) = d.values.get(i)
        && let Ok(k) = std::str::from_utf8(b)
    {
        return split_stored(k).0;
    }
    json!(d.id.get())
}

/// The tenant of a document, if it has one.
fn tenant_of<'a>(schema: &Schema, d: &'a Document) -> Option<&'a str> {
    match d
        .values
        .get(schema.index_of(cairn_core::schema::TENANT_FIELD)?)
    {
        Some(Some(Value::Enum(t))) => Some(t),
        _ => None,
    }
}

/// A document as JSON. Outside a tenant scope, a tenant's document shows its tenant as
/// `_tenant`; inside one, the tenant is implied.
fn doc_json(schema: &Schema, scope: &Scope, d: &Document) -> Json_ {
    let mut m = Map::new();
    m.insert("id".into(), id_json(schema, d));
    if scope.tenant.is_none()
        && let Some(t) = tenant_of(schema, d)
    {
        m.insert(cairn_core::schema::TENANT_FIELD.into(), json!(t));
    }
    for (f, v) in schema.fields.iter().zip(&d.values) {
        if cairn_core::schema::is_reserved(&f.name) {
            continue;
        }
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
        .filter(|_| !cairn_core::schema::is_reserved(name))
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

type ScopeExt = Option<axum::Extension<Scope>>;

fn scope_of(e: ScopeExt) -> Scope {
    e.map(|e| e.0).unwrap_or_default()
}

async fn upsert(
    State(s): State<Arc<Shared>>,
    scope: ScopeExt,
    params: Params,
    Json(body): Json<UpsertBody>,
) -> ApiResult<Json<Json_>> {
    let scope = scope_of(scope);
    let coll = collection(&s, &params).await?;
    let docs = body
        .documents
        .iter()
        .map(|d| doc_from_json(&coll.schema, scope.tenant.as_deref(), d))
        .collect::<ApiResult<Vec<_>>>()?;
    if docs.is_empty() {
        return Err(ApiError::bad("no documents"));
    }
    let n = docs.len();
    let (keyed, plain): (Vec<_>, Vec<_>) = docs
        .into_iter()
        .partition(|(id, _)| matches!(id, ApiId::Key(_)));
    let keyed: Vec<Document> = keyed.into_iter().map(|x| x.1).collect();
    let plain: Vec<Document> = plain.into_iter().map(|x| x.1).collect();
    let tokens = with_client_in(&s, &coll.name, move |c| {
        let mut t = Vec::new();
        if !plain.is_empty() {
            t.extend(c.upsert(plain)?);
        }
        if !keyed.is_empty() {
            t.extend(c.upsert_keyed(keyed)?);
        }
        Ok(t)
    })
    .await?;
    ack(n, &tokens, body.after.as_deref())
}

#[derive(Deserialize)]
struct DeleteBody {
    #[serde(default)]
    ids: Option<Vec<Json_>>,
    /// Deletion by filter (ADR 0031); with `ids`, only those ids that match it.
    #[serde(default)]
    filter: Option<Json_>,
    #[serde(default)]
    after: Option<String>,
}

/// Splits stored ids into integer and text candidates of a deletion by filter.
fn delete_scopes(ids: &[ApiId]) -> Vec<DeleteScope> {
    let (mut nums, mut keys) = (Vec::new(), Vec::new());
    for id in ids {
        match id {
            ApiId::Num(n) => nums.push(DocId(*n)),
            ApiId::Key(k) => keys.push(k.clone()),
        }
    }
    let mut v = Vec::new();
    if !nums.is_empty() {
        v.push(DeleteScope::Ids(nums));
    }
    if !keys.is_empty() {
        v.push(DeleteScope::Keys(keys));
    }
    v
}

/// Runs deletions by filter over each scope: documents removed, and tokens.
async fn delete_where(
    s: &Arc<Shared>,
    coll: &CollectionDef,
    scopes: Vec<DeleteScope>,
    filter: Predicate,
) -> ApiResult<(u64, Vec<Token>)> {
    with_client_in(s, &coll.name, move |c| {
        let (mut count, mut tokens) = (0, Vec::new());
        for scope in scopes {
            let (n, t) = c.delete_where(scope, filter.clone())?;
            count += n;
            tokens.extend(t);
        }
        Ok((count, tokens))
    })
    .await
}

async fn delete_many(
    State(s): State<Arc<Shared>>,
    key: Option<axum::Extension<KeyId>>,
    scope: ScopeExt,
    params: Params,
    Json(body): Json<DeleteBody>,
) -> ApiResult<Json<Json_>> {
    let scope = scope_of(scope);
    let coll = collection(&s, &params).await?;
    let tenant = scope.tenant.as_deref();
    let ids = body
        .ids
        .as_ref()
        .map(|v| v.iter().map(api_id).collect::<ApiResult<Vec<_>>>())
        .transpose()?;
    let Some(f) = &body.filter else {
        let ids = ids.ok_or_else(|| ApiError::bad("give \"ids\", \"filter\", or both"))?;
        if ids.is_empty() {
            return Err(ApiError::bad("no ids"));
        }
        let n = ids.len();
        let tokens = delete_ids(&s, &coll, &scope, &ids).await?;
        let resp = ack(n, &tokens, body.after.as_deref())?;
        audit_takedown(key.as_deref(), &scope, &ids, &resp);
        return Ok(resp);
    };
    let pred = filter(&coll.schema, f)?;
    if matches!(&pred, Predicate::True) || matches!(&pred, Predicate::And(v) if v.is_empty()) {
        return Err(ApiError::bad(
            "a filter that matches every document is refused: give a condition",
        ));
    }
    pred.validate(&coll.schema)
        .map_err(|e| ApiError::bad(e.to_string()))?;
    let scopes = match &ids {
        None => vec![DeleteScope::All],
        Some(ids) if ids.is_empty() => return Err(ApiError::bad("no ids")),
        Some(ids) => delete_scopes(
            &ids.iter()
                .map(|id| stored_id(tenant, id))
                .collect::<Vec<_>>(),
        ),
    };
    let (count, tokens) =
        delete_where(&s, &coll, scopes, scoped(&coll.schema, tenant, pred)?).await?;
    let prior = body
        .after
        .as_deref()
        .map(parse_tokens)
        .transpose()?
        .unwrap_or_default();
    let resp = Json(json!({
        "deleted": count,
        "consistency_token": merge_tokens(&prior, &tokens),
    }));
    tracing::info!(
        target: "cairn_server::audit",
        key = key.as_deref().map_or("-", |k| k.0.as_str()),
        collection = %coll.name,
        tenant = tenant.unwrap_or("-"),
        filter = %f,
        ids = ids.as_ref().map_or(0, Vec::len),
        count,
        token = resp.0["consistency_token"].as_str().unwrap_or(""),
        "takedown by filter"
    );
    Ok(resp)
}

/// Takes down integer and text ids. Under a tenant, only those of the tenant's documents: the
/// ids are the tenant's own, and the deletion is also restricted by `_tenant`.
async fn delete_ids(
    s: &Arc<Shared>,
    coll: &CollectionDef,
    scope: &Scope,
    ids: &[ApiId],
) -> ApiResult<Vec<Token>> {
    let tenant = scope.tenant.as_deref();
    if let Some(only) = tenant_filter(&coll.schema, tenant)? {
        let stored: Vec<ApiId> = ids.iter().map(|id| stored_id(tenant, id)).collect();
        return Ok(delete_where(s, coll, delete_scopes(&stored), only).await?.1);
    }
    let mut nums = Vec::new();
    let mut keys = Vec::new();
    for id in ids {
        match id {
            ApiId::Num(n) => nums.push(DocId(*n)),
            ApiId::Key(k) => keys.push(k.clone()),
        }
    }
    with_client_in(s, &coll.name, move |c| {
        let mut t = Vec::new();
        if !nums.is_empty() {
            t.extend(c.delete(nums)?);
        }
        if !keys.is_empty() {
            t.extend(c.delete_keys(keys)?);
        }
        Ok(t)
    })
    .await
}

/// The changes of one document: `{"field": value}` sets, `{"field": null}` clears.
fn patch_op(schema: &Schema, tenant: Option<&str>, id: &ApiId, set: &Json_) -> ApiResult<PatchOp> {
    let obj = set
        .as_object()
        .ok_or_else(|| ApiError::bad("\"set\" is an object of fields"))?;
    let mut changes = Vec::new();
    for (k, v) in obj {
        if k == "id" || cairn_core::schema::is_reserved(k) {
            return Err(ApiError::bad(format!("field {k:?} cannot be patched")));
        }
        let i = schema
            .index_of(k)
            .ok_or_else(|| ApiError::bad(format!("unknown field {k:?}")))?;
        changes.push((i as u32, field_value(&schema.fields[i].kind, k, v)?));
    }
    Ok(PatchOp {
        target: match stored_id(tenant, id) {
            ApiId::Num(n) => PatchTarget::Id(DocId(n)),
            ApiId::Key(k) => PatchTarget::Key(k),
        },
        set: changes,
    })
}

async fn run_patch(
    s: &Arc<Shared>,
    coll: &CollectionDef,
    ops: Vec<PatchOp>,
    after: Option<&str>,
) -> ApiResult<Json<Json_>> {
    let (count, tokens) = with_client_in(s, &coll.name, move |c| c.patch(ops)).await?;
    let prior = after.map(parse_tokens).transpose()?.unwrap_or_default();
    Ok(Json(json!({
        "patched": count,
        "consistency_token": merge_tokens(&prior, &tokens),
    })))
}

#[derive(Deserialize)]
struct ProofBody {
    ids: Vec<Json_>,
    /// The takedown's consistency token: each replica must have applied it.
    #[serde(default)]
    after: Option<String>,
}

/// The node's public key for proofs of deletion.
async fn proof_key(State(s): State<Arc<Shared>>) -> ApiResult<Json<Json_>> {
    let key = s
        .cfg
        .proof_key
        .as_ref()
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "no proof key".into()))?;
    Ok(Json(json!({
        "public_key": crate::proof::public_key(key),
        "algorithm": "Ed25519",
    })))
}

/// Proof of deletion (ADR 0031): asks every replica of the documents' shards whether it has
/// applied the takedown (`after`) and still holds them, and returns the report signed by this
/// node. `verdict` is `deleted everywhere` only if every document is proven deleted.
async fn deletion_proof(
    State(s): State<Arc<Shared>>,
    key: Option<axum::Extension<KeyId>>,
    scope: ScopeExt,
    params: Params,
    Json(body): Json<ProofBody>,
) -> ApiResult<Json<Json_>> {
    let scope = scope_of(scope);
    let coll = collection(&s, &params).await?;
    let signer = s
        .cfg
        .proof_key
        .clone()
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "no proof key".into()))?;
    if body.ids.is_empty() || body.ids.len() > 10_000 {
        return Err(ApiError::bad("between 1 and 10000 ids"));
    }
    let asked = body.ids.iter().map(api_id).collect::<ApiResult<Vec<_>>>()?;
    let tokens = body
        .after
        .as_deref()
        .map(parse_tokens)
        .transpose()?
        .unwrap_or_default();
    // The node reports ids first, then text ids: remember where each asked id went.
    let (mut nums, mut keys, mut slots) = (Vec::new(), Vec::new(), Vec::new());
    for id in &asked {
        match stored_id(scope.tenant.as_deref(), id) {
            ApiId::Num(n) => {
                slots.push((false, nums.len()));
                nums.push(DocId(n));
            }
            ApiId::Key(k) => {
                slots.push((true, keys.len()));
                keys.push(k);
            }
        }
    }
    let n_nums = nums.len();
    let raw = with_client_in(&s, &coll.name, move |c| {
        c.deletion_check(nums, keys, tokens)
    })
    .await?;
    let raw: Json_ = serde_json::from_str(&raw)
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let checked = raw["documents"].as_array().cloned().unwrap_or_default();
    let mut documents = Vec::new();
    let mut all = true;
    for (id, (is_key, i)) in asked.iter().zip(slots) {
        let mut d = checked
            .get(if is_key { n_nums + i } else { i })
            .cloned()
            .unwrap_or(Json_::Null);
        all &= d["verdict"] == "deleted";
        d["id"] = match id {
            ApiId::Num(n) => json!(n),
            ApiId::Key(k) => json!(k),
        };
        documents.push(d);
    }
    let report = json!({
        "kind": "cairn-deletion-proof/1",
        "collection": coll.name,
        "tenant": scope.tenant,
        "token": body.after,
        "generated_at_ms": now_ms(),
        "checked_by_node": raw["checked_by"],
        "documents": documents,
        "shards": raw["shards"],
        "verdict": if all { "deleted everywhere" } else { "not proven" },
    });
    tracing::info!(
        target: "cairn_server::audit",
        key = key.as_deref().map_or("-", |k| k.0.as_str()),
        collection = %coll.name,
        ids = asked.len(),
        verdict = report["verdict"].as_str().unwrap_or(""),
        "deletion proof"
    );
    Ok(Json(crate::proof::sign(&signer, report)))
}

#[derive(Deserialize)]
struct PatchOneBody {
    set: Json_,
    #[serde(default)]
    after: Option<String>,
}

/// Changes some fields of one document (ADR 0031); a missing document is not created.
async fn patch_one(
    State(s): State<Arc<Shared>>,
    scope: ScopeExt,
    params: Params,
    UrlQuery(p): UrlQuery<ReadParams>,
    Json(body): Json<PatchOneBody>,
) -> ApiResult<Json<Json_>> {
    let scope = scope_of(scope);
    let coll = collection(&s, &params).await?;
    let id = path_id(
        param(&params, "id").unwrap_or_default(),
        p.id_type.as_deref(),
    )?;
    let op = patch_op(&coll.schema, scope.tenant.as_deref(), &id, &body.set)?;
    run_patch(&s, &coll, vec![op], body.after.as_deref()).await
}

#[derive(Deserialize)]
struct PatchManyBody {
    patches: Vec<Json_>,
    #[serde(default)]
    after: Option<String>,
}

/// Changes fields of several documents: `{"patches": [{"id": ..., "set": {...}}]}`.
async fn patch_many(
    State(s): State<Arc<Shared>>,
    scope: ScopeExt,
    params: Params,
    Json(body): Json<PatchManyBody>,
) -> ApiResult<Json<Json_>> {
    let scope = scope_of(scope);
    let coll = collection(&s, &params).await?;
    if body.patches.is_empty() {
        return Err(ApiError::bad("no patches"));
    }
    let ops = body
        .patches
        .iter()
        .map(|p| {
            let id = api_id(
                p.get("id")
                    .ok_or_else(|| ApiError::bad("a patch needs an \"id\""))?,
            )?;
            let set = p
                .get("set")
                .ok_or_else(|| ApiError::bad("a patch needs \"set\""))?;
            patch_op(&coll.schema, scope.tenant.as_deref(), &id, set)
        })
        .collect::<ApiResult<Vec<_>>>()?;
    run_patch(&s, &coll, ops, body.after.as_deref()).await
}

#[derive(Deserialize)]
struct ReadParams {
    consistency: Option<String>,
    after: Option<String>,
    /// `text` or `int`: how to read the id in the path (default: digits are an integer).
    id_type: Option<String>,
}

async fn delete_one(
    State(s): State<Arc<Shared>>,
    key: Option<axum::Extension<KeyId>>,
    scope: ScopeExt,
    params: Params,
    UrlQuery(p): UrlQuery<ReadParams>,
) -> ApiResult<Json<Json_>> {
    let scope = scope_of(scope);
    let coll = collection(&s, &params).await?;
    let id = path_id(
        param(&params, "id").unwrap_or_default(),
        p.id_type.as_deref(),
    )?;
    let tokens = delete_ids(&s, &coll, &scope, std::slice::from_ref(&id)).await?;
    let resp = ack(1, &tokens, p.after.as_deref())?;
    audit_takedown(key.as_deref(), &scope, &[id], &resp);
    Ok(resp)
}

/// Erases a tenant: every document it holds (ADR 0031). Unscoped keys only.
async fn erase_tenant(
    State(s): State<Arc<Shared>>,
    key: Option<axum::Extension<KeyId>>,
    scope: ScopeExt,
    params: Params,
    UrlQuery(p): UrlQuery<ReadParams>,
) -> ApiResult<Json<Json_>> {
    let scope = scope_of(scope);
    let coll = collection(&s, &params).await?;
    let tenant = param(&params, "tenant").unwrap_or_default().to_owned();
    if scope.from_key {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "a tenant-scoped key cannot erase a tenant".into(),
        ));
    }
    if !crate::auth::valid_tenant(&tenant) {
        return Err(ApiError::bad(
            "a tenant is 1 to 128 letters, digits, '_', '.' or '-'",
        ));
    }
    let only = tenant_filter(&coll.schema, Some(&tenant))?.expect("a tenant");
    let (count, tokens) = delete_where(&s, &coll, vec![DeleteScope::All], only).await?;
    let prior = p
        .after
        .as_deref()
        .map(parse_tokens)
        .transpose()?
        .unwrap_or_default();
    let resp = Json(json!({
        "deleted": count,
        "consistency_token": merge_tokens(&prior, &tokens),
    }));
    tracing::info!(
        target: "cairn_server::audit",
        key = key.as_deref().map_or("-", |k| k.0.as_str()),
        collection = %coll.name,
        tenant,
        count,
        token = resp.0["consistency_token"].as_str().unwrap_or(""),
        "tenant erased"
    );
    Ok(resp)
}

async fn get_doc(
    State(s): State<Arc<Shared>>,
    scope: ScopeExt,
    params: Params,
    UrlQuery(p): UrlQuery<ReadParams>,
) -> ApiResult<Response> {
    let scope = scope_of(scope);
    let coll = collection(&s, &params).await?;
    let id = path_id(
        param(&params, "id").unwrap_or_default(),
        p.id_type.as_deref(),
    )?;
    let (consistency, tokens) = consistency(p.consistency.as_deref(), p.after.as_deref())?;
    let req_id = stored_id(scope.tenant.as_deref(), &id);
    let doc = with_client_in(&s, &coll.name, move |c| {
        let req = match req_id {
            ApiId::Num(n) => Request::Get {
                id: DocId(n),
                consistency,
                tokens,
            },
            ApiId::Key(key) => Request::GetKey {
                key,
                consistency,
                tokens,
            },
        };
        match c.call(&req)? {
            Wire::Doc(d) => Ok(d),
            Wire::Error { message, .. } => Err(cairn_core::Error::Internal(message)),
            other => Err(cairn_core::Error::Internal(format!(
                "unexpected response {other:?}"
            ))),
        }
    })
    .await?;
    // A tenant's ids cannot name another tenant's documents; checked again all the same.
    let doc = doc.filter(|d| {
        scope
            .tenant
            .as_deref()
            .is_none_or(|t| tenant_of(&coll.schema, d) == Some(t))
            && !is_expired(&coll, d)
    });
    Ok(match doc {
        Some(d) => Json(doc_json(&coll.schema, &scope, &d)).into_response(),
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
    /// One hit per value of this field (ADR 0031): a document's best chunk, for instance.
    #[serde(default)]
    group_by: Option<String>,
}

fn default_k() -> usize {
    10
}

fn yes() -> bool {
    true
}

fn hit_json(schema: &Schema, scope: &Scope, h: &Hit) -> Json_ {
    let mut m = Map::new();
    let (id, tenant) = match (&h.key, &h.document) {
        (Some(k), _) => split_stored(k),
        (None, Some(d)) => (id_json(schema, d), tenant_of(schema, d)),
        (None, None) => (json!(h.doc_id.get()), None),
    };
    m.insert("id".into(), id);
    if scope.tenant.is_none()
        && let Some(t) = tenant
    {
        m.insert(cairn_core::schema::TENANT_FIELD.into(), json!(t));
    }
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
        m.insert("document".into(), doc_json(schema, scope, d));
    }
    Json_::Object(m)
}

async fn search(
    State(s): State<Arc<Shared>>,
    scope: ScopeExt,
    params: Params,
    Json(body): Json<SearchBody>,
) -> ApiResult<Json<Json_>> {
    let scope = scope_of(scope);
    let coll = collection(&s, &params).await?;
    let schema = &coll.schema;
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
    let user_filter = filter(schema, &body.filter)?;
    user_filter
        .validate(schema)
        .map_err(|e| ApiError::bad(e.to_string()))?;
    q.filter = unexpired(&coll, scoped(schema, scope.tenant.as_deref(), user_filter)?);
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
    let run = |q: Query| {
        let tokens = tokens.clone();
        with_client_in(&s, &coll.name, move |c| {
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
    };
    let Some(group) = &body.group_by else {
        let hits = run(q).await?;
        return Ok(Json(json!({
            "hits": hits.iter().map(|h| hit_json(schema, &scope, h)).collect::<Vec<_>>()
        })));
    };
    // One hit per group: fetch more candidates than `k`, keep each group's best in rank order,
    // and widen (up to 10,000 candidates) while fewer than `k` groups came back and more exist.
    let field = schema
        .index_of(group)
        .filter(|_| !cairn_core::schema::is_reserved(group))
        .ok_or_else(|| ApiError::bad(format!("group_by: unknown field {group:?}")))?;
    if matches!(
        schema.fields[field].kind,
        FieldKind::Vector { .. } | FieldKind::Blob | FieldKind::Set
    ) {
        return Err(ApiError::bad(format!(
            "group_by: {group:?} is not a scalar field"
        )));
    }
    let k = body.k;
    let mut fetch = (k * 4).clamp(k, 10_000);
    let (hits, keys) = loop {
        let mut qk = q.clone();
        qk.k = fetch;
        qk.with_documents = true;
        let hits = run(qk).await?;
        let mut seen: std::collections::HashSet<String> = Default::default();
        let mut kept = Vec::new();
        let mut keys = Vec::new();
        for h in &hits {
            let value = h
                .document
                .as_ref()
                .and_then(|d| d.values.get(field).cloned().flatten());
            // A hit without a value is a group of its own.
            let key = match &value {
                Some(v) => format!("{v:?}"),
                None => format!("\u{0}{}", h.doc_id.get()),
            };
            if seen.insert(key) {
                kept.push(h.clone());
                keys.push(value);
            }
            if kept.len() == k {
                break;
            }
        }
        if kept.len() >= k || hits.len() < fetch || fetch >= 10_000 {
            break (kept, keys);
        }
        fetch = (fetch * 2).min(10_000);
    };
    let out: Vec<Json_> = hits
        .iter()
        .zip(keys)
        .map(|(h, value)| {
            let mut j = hit_json(schema, &scope, h);
            j["group"] = value.as_ref().map_or(Json_::Null, value_json);
            if !body.with_documents
                && let Some(m) = j.as_object_mut()
            {
                m.remove("document");
            }
            j
        })
        .collect();
    Ok(Json(json!({ "hits": out })))
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
    fn tenant_documents_are_namespaced() {
        let sk = schema().with_reserved().unwrap();
        let acme = Scope {
            tenant: Some("acme".into()),
            from_key: true,
        };
        for j in [
            json!({ "id": 7, "n": 1 }),
            json!({ "id": "doc-7", "n": 1 }),
            json!({ "id": "12", "n": 1 }),
        ] {
            let (id, d) = doc_from_json(&sk, Some("acme"), &j).unwrap();
            let ApiId::Key(stored) = &id else {
                panic!("a tenant's ids are stored as text ids");
            };
            assert!(stored.starts_with("acme\u{1f}"));
            assert_eq!(split_stored(stored), (j["id"].clone(), Some("acme")));
            assert_eq!(tenant_of(&sk, &d), Some("acme"));
            // Inside the tenant the document reads back as written; outside, with its tenant.
            assert_eq!(doc_json(&sk, &acme, &d), j);
            let mut open = j.clone();
            open["_tenant"] = json!("acme");
            assert_eq!(doc_json(&sk, &Scope::default(), &d), open);
            // The same id in another tenant, or without one, is another document.
            let (other, _) = doc_from_json(&sk, Some("globex"), &j).unwrap();
            let (plain, _) = doc_from_json(&sk, None, &j).unwrap();
            assert!(other != id && plain != id);
        }
        let only = tenant_filter(&sk, Some("acme")).unwrap().unwrap();
        assert_eq!(scoped(&sk, Some("acme"), Predicate::True).unwrap(), only);
        assert_eq!(scoped(&sk, None, Predicate::True).unwrap(), Predicate::True);
        let p = Predicate::IsNull { field: 2 };
        assert_eq!(
            scoped(&sk, Some("acme"), p.clone()).unwrap(),
            Predicate::And(vec![p, only])
        );
    }

    #[test]
    fn documents_round_trip_through_json() {
        let s = schema();
        let j = json!({ "id": 7, "v": [1.0, 2.5], "tags": ["a", "b"], "n": -3, "b": "AAEC" });
        let (id, d) = doc_from_json(&s, None, &j).unwrap();
        assert_eq!(id, ApiId::Num(7));
        assert_eq!(d.values[3], Some(Value::Blob(vec![0u8, 1, 2].into())));
        assert_eq!(doc_json(&s, &Scope::default(), &d), j);
        // Text ids (ADR 0031): held in `_key`, returned as the id, reserved names refused.
        let sk = s.with_reserved().unwrap();
        let j = json!({ "id": "doc-7", "n": 2 });
        let (id, d) = doc_from_json(&sk, None, &j).unwrap();
        assert_eq!(id, ApiId::Key("doc-7".into()));
        assert_eq!(doc_json(&sk, &Scope::default(), &d), j);
        assert!(
            doc_from_json(&s, None, &j).is_err(),
            "no text ids without the reserved fields"
        );
        assert!(doc_from_json(&sk, None, &json!({ "id": "a", "_tenant": "x" })).is_err());
        assert!(doc_from_json(&sk, None, &json!({ "id": "" })).is_err());
        assert!(doc_from_json(&sk, None, &json!({ "id": "a\u{1f}b" })).is_err());
        assert!(doc_from_json(&sk, None, &json!({ "id": 1u64 << 63 })).is_err());
        assert_eq!(path_id("42", None).unwrap(), ApiId::Num(42));
        assert_eq!(
            path_id("42", Some("text")).unwrap(),
            ApiId::Key("42".into())
        );
        assert_eq!(path_id("doc-1", None).unwrap(), ApiId::Key("doc-1".into()));
        assert!(path_id("doc-1", Some("int")).is_err());
        assert!(doc_from_json(&s, None, &json!({ "id": 1, "v": [1.0] })).is_err());
        assert!(doc_from_json(&s, None, &json!({ "id": 1, "nope": 1 })).is_err());
        assert!(doc_from_json(&s, None, &json!({ "v": [1.0, 2.0] })).is_err());
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
