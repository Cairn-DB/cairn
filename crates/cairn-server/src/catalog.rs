//! Collections (ADR 0031, section 4): their definitions, how requests route to their shards,
//! and the replicated catalog that holds them.
//!
//! The catalog is an ordinary shard group ([`CATALOG_SHARD`]) hosted by every node. Each
//! collection is one document, keyed by its name. A dropped collection leaves a tombstone keyed
//! `dropped/<id>`, so that a node that was down still removes its files. Creations and drops
//! are serialized by the catalog leader, which reads the catalog linearizably before choosing
//! a collection's id and shard range. Ids and shard ranges are never reused. Every node
//! reconciles its replicas with the catalog (see `node.rs`).
//!
//! The collection given by `--schema` at startup is `default`: shards `0..shards`, the data
//! directories of 0.2, and not stored in the catalog.

use bytes::Bytes;
use cairn_core::{DocId, Document, Error, FieldDef, FieldKind, Result, Schema, ShardId, Value};
use cairn_proto::{shard_of, shard_of_key};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock, RwLock};

/// The catalog's shard group. Internal ids of text ids embed their shard in 23 bits, so every
/// shard id stays below 2^23; the catalog takes the last one.
pub const CATALOG_SHARD: ShardId = ShardId((1 << 23) - 1);

/// Largest shard id a collection may use.
pub const MAX_SHARD: u32 = CATALOG_SHARD.0 - 1;

/// Name of the collection defined at startup.
pub const DEFAULT: &str = "default";

/// Shards a collection may have.
pub const MAX_SHARDS: u32 = 1024;

/// One collection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionDef {
    /// Name (`[a-z0-9_-]{1,64}`).
    pub name: String,
    /// Id, never reused: its data lives under `c<id>/`.
    pub id: u32,
    /// First shard id; the collection owns `base..base + shards`.
    pub base: u32,
    /// Shard count.
    pub shards: u32,
    /// Schema, with the reserved fields.
    pub schema: Schema,
}

impl CollectionDef {
    /// The shard of a document id.
    pub fn route(&self, id: DocId) -> Result<ShardId> {
        match id.keyed_shard() {
            // The internal id of a text id carries its (global) shard.
            Some(s) if (self.base..self.base + self.shards).contains(&s) => Ok(ShardId(s)),
            Some(_) => Err(Error::InvalidRequest(format!(
                "id {id} is not an id of collection {:?}",
                self.name
            ))),
            None => Ok(ShardId(self.base + shard_of(id, self.shards).get())),
        }
    }

    /// The shard of a text id.
    pub fn route_key(&self, key: &str) -> ShardId {
        ShardId(self.base + shard_of_key(key, self.shards).get())
    }

    /// Every shard of the collection.
    pub fn shard_ids(&self) -> impl Iterator<Item = ShardId> {
        (self.base..self.base + self.shards).map(ShardId)
    }

    /// Whether `shard` belongs to the collection.
    pub fn owns(&self, shard: ShardId) -> bool {
        (self.base..self.base + self.shards).contains(&shard.get())
    }

    /// Directory of one of its shards, under the data directory.
    pub fn shard_dir(&self, shard: ShardId) -> String {
        if self.id == 0 {
            format!("shard{}", shard.get())
        } else {
            format!("c{}/shard{}", self.id, shard.get())
        }
    }
}

/// Directory of a collection's data (not of `default`, which lives at the top level).
pub fn collection_dir(id: u32) -> String {
    format!("c{id}")
}

/// Checks a collection name.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

/// The catalog's own schema: a kind (`collection` or `dropped`), the definition as JSON.
pub fn catalog_schema() -> Schema {
    Schema::new(vec![
        FieldDef {
            name: "kind".into(),
            kind: FieldKind::Enum,
        },
        FieldDef {
            name: "def".into(),
            kind: FieldKind::Blob,
        },
    ])
    .and_then(|s| s.with_reserved())
    .expect("the catalog schema is valid")
}

/// A catalog entry as a document of the catalog shard.
pub fn entry_doc(key: &str, kind: &str, def: &CollectionDef) -> Document {
    let schema = catalog_schema();
    let mut d = Document::new(DocId(0), schema.fields.len())
        .set(0, Value::Enum(kind.into()))
        .set(
            1,
            Value::Blob(Bytes::from(serde_json::to_vec(def).expect("serializable"))),
        );
    let k = schema
        .index_of(cairn_core::schema::KEY_FIELD)
        .expect("reserved key field");
    d = d.set(k, Value::Blob(Bytes::from(key.as_bytes().to_vec())));
    d
}

/// Reads a catalog document back: its kind and definition.
pub fn parse_entry(d: &Document) -> Option<(String, CollectionDef)> {
    let kind = match d.values.first() {
        Some(Some(Value::Enum(k))) => k.clone(),
        _ => return None,
    };
    let def = match d.values.get(1) {
        Some(Some(Value::Blob(b))) => serde_json::from_slice(b).ok()?,
        _ => return None,
    };
    Some((kind, def))
}

/// Key of a dropped collection's tombstone.
pub fn tombstone_key(id: u32) -> String {
    format!("dropped/{id}")
}

/// What this node knows of the catalog.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    /// Live collections, `default` first.
    pub live: Vec<Arc<CollectionDef>>,
    /// Dropped collections (their tombstones).
    pub dropped: Vec<CollectionDef>,
}

impl Catalog {
    /// A live collection by name.
    pub fn get(&self, name: &str) -> Option<Arc<CollectionDef>> {
        self.live.iter().find(|c| c.name == name).cloned()
    }

    /// The live collection owning `shard`.
    pub fn owner(&self, shard: ShardId) -> Option<Arc<CollectionDef>> {
        self.live.iter().find(|c| c.owns(shard)).cloned()
    }

    /// The next free collection id and shard base, past everything ever allocated.
    pub fn next_ids(&self) -> (u32, u32) {
        let all = self.live.iter().map(|c| c.as_ref()).chain(&self.dropped);
        let (mut id, mut base) = (1, 0);
        for c in all {
            id = id.max(c.id + 1);
            base = base.max(c.base + c.shards);
        }
        (id, base)
    }
}

static CATALOG: OnceLock<RwLock<Catalog>> = OnceLock::new();

fn cell() -> &'static RwLock<Catalog> {
    CATALOG.get_or_init(|| RwLock::new(Catalog::default()))
}

/// This process's view of the catalog (one node per process).
pub fn current() -> Catalog {
    cell().read().expect("catalog").clone()
}

/// Replaces this process's view of the catalog (startup).
pub fn install(c: Catalog) {
    *cell().write().expect("catalog") = c;
}

/// Merges what a catalog read showed into this process's view. Collection ids are never
/// reused, so the view only grows: a collection seen live is added unless known dropped, and a
/// dropped one stays dropped. A `complete` listing (a linearizable read of every live
/// collection) also marks as dropped every known collection it no longer contains. A stale
/// read can therefore never bring a dropped collection back.
pub fn observe(live: &[Arc<CollectionDef>], dropped: &[CollectionDef], complete: bool) {
    let mut v = cell().write().expect("catalog");
    for d in dropped {
        if !v.dropped.iter().any(|x| x.id == d.id) {
            v.dropped.push(d.clone());
        }
    }
    if complete {
        let gone: Vec<CollectionDef> = v
            .live
            .iter()
            .filter(|l| l.id != 0 && !live.iter().any(|x| x.id == l.id))
            .map(|l| l.as_ref().clone())
            .collect();
        for g in gone {
            if !v.dropped.iter().any(|x| x.id == g.id) {
                v.dropped.push(g);
            }
        }
    }
    for l in live {
        if !v.live.iter().any(|x| x.id == l.id) && !v.dropped.iter().any(|x| x.id == l.id) {
            v.live.push(l.clone());
        }
    }
    let dropped_ids: Vec<u32> = v.dropped.iter().map(|d| d.id).collect();
    v.live.retain(|l| !dropped_ids.contains(&l.id));
}

/// Builds a catalog from the `default` collection and the catalog shard's documents.
pub fn from_entries(default: CollectionDef, docs: &[Document]) -> Catalog {
    let mut c = Catalog {
        live: vec![Arc::new(default)],
        dropped: Vec::new(),
    };
    let mut entries: Vec<(String, CollectionDef)> = docs.iter().filter_map(parse_entry).collect();
    entries.sort_by_key(|(_, d)| d.id);
    for (kind, def) in entries {
        match kind.as_str() {
            "collection" => c.live.push(Arc::new(def)),
            "dropped" => c.dropped.push(def),
            _ => {}
        }
    }
    // A collection dropped and recreated under the same name keeps only its live entry live.
    let dropped: Vec<u32> = c.dropped.iter().map(|d| d.id).collect();
    c.live.retain(|l| !dropped.contains(&l.id));
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(name: &str, id: u32, base: u32, shards: u32) -> CollectionDef {
        CollectionDef {
            name: name.into(),
            id,
            base,
            shards,
            schema: catalog_schema(),
        }
    }

    #[test]
    fn the_view_never_brings_a_dropped_collection_back() {
        install(from_entries(def(DEFAULT, 0, 0, 2), &[]));
        let a = Arc::new(def("a", 1, 2, 1));
        let b = Arc::new(def("b", 2, 3, 1));
        observe(std::slice::from_ref(&a), &[], false);
        observe(std::slice::from_ref(&b), &[], false);
        assert!(current().get("a").is_some() && current().get("b").is_some());
        // A complete listing without `a`: dropped. A stale read still showing it changes nothing.
        observe(&[current().get(DEFAULT).unwrap(), b.clone()], &[], true);
        assert!(current().get("a").is_none());
        observe(std::slice::from_ref(&a), &[], false);
        assert!(current().get("a").is_none());
        // Recreated under the same name: a new id, live.
        observe(&[Arc::new(def("a", 3, 4, 1))], &[], false);
        assert_eq!(current().get("a").unwrap().id, 3);
        observe(&[], &[b.as_ref().clone()], false);
        assert!(current().get("b").is_none());
        assert!(current().get(DEFAULT).is_some(), "default is never dropped");
    }

    #[test]
    fn routing_stays_within_the_collection() {
        let c = def("docs", 3, 16, 4);
        for i in 0..1000 {
            assert!(c.owns(c.route(DocId(i)).unwrap()));
            assert!(c.owns(c.route_key(&format!("k{i}"))));
        }
        assert_eq!(c.route(DocId::keyed(17, 5).unwrap()).unwrap(), ShardId(17));
        assert!(c.route(DocId::keyed(3, 5).unwrap()).is_err());
        assert_eq!(c.shard_dir(ShardId(17)), "c3/shard17");
        let d = def(DEFAULT, 0, 0, 4);
        assert_eq!(d.shard_dir(ShardId(2)), "shard2");
        // The default collection routes exactly as 0.2 did.
        for i in 0..1000 {
            assert_eq!(d.route(DocId(i)).unwrap(), shard_of(DocId(i), 4));
        }
    }

    #[test]
    fn entries_round_trip_and_ids_are_never_reused() {
        let a = def("a", 1, 4, 2);
        let b = def("b", 2, 6, 3);
        let docs = vec![
            entry_doc("a", "collection", &a),
            entry_doc("b", "dropped", &b),
            entry_doc("b", "collection", &def("b", 3, 9, 1)),
        ];
        let c = from_entries(def(DEFAULT, 0, 0, 4), &docs);
        assert_eq!(
            c.live
                .iter()
                .map(|l| (l.name.as_str(), l.id))
                .collect::<Vec<_>>(),
            vec![(DEFAULT, 0), ("a", 1), ("b", 3)]
        );
        assert_eq!(c.dropped, vec![b]);
        assert_eq!(c.next_ids(), (4, 10));
        assert_eq!(c.owner(ShardId(5)).unwrap().name, "a");
        assert!(
            c.owner(ShardId(7)).is_none(),
            "dropped shards have no owner"
        );
        for (n, ok) in [
            ("docs", true),
            ("a-b_1", true),
            ("", false),
            ("Docs", false),
            ("a/b", false),
        ] {
            assert_eq!(valid_name(n), ok, "{n}");
        }
    }
}
