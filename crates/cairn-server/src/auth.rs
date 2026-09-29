//! API keys and roles for the HTTP API (ADR 0030).
//!
//! A key is a random secret shown once. The server keeps only its SHA-256 digest, in a JSON
//! file: `{"keys": [{"id": "ingest", "sha256": "<hex>", "roles": ["write", "read"]}]}`.
//! Requests carry `Authorization: Bearer <key>`. Each route needs one role; `admin` holds them
//! all, and `takedown` is kept apart from `write` so that deletions can be granted to a
//! compliance service alone.

use ring::digest::{SHA256, digest};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// A role an API key may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Reads, searches and the schema.
    Read,
    /// Inserts and replacements.
    Write,
    /// Deletions (takedowns).
    Takedown,
    /// Cluster status and administration; implies every other role.
    Admin,
}

impl Role {
    fn bit(self) -> u8 {
        match self {
            Role::Read => 1,
            Role::Write => 2,
            Role::Takedown => 4,
            Role::Admin => 8,
        }
    }

    /// Parses `read`, `write`, `takedown` or `admin`.
    pub fn parse(s: &str) -> anyhow::Result<Role> {
        Ok(match s {
            "read" => Role::Read,
            "write" => Role::Write,
            "takedown" => Role::Takedown,
            "admin" => Role::Admin,
            other => anyhow::bail!("unknown role {other:?} (read, write, takedown, admin)"),
        })
    }

    fn name(self) -> &'static str {
        match self {
            Role::Read => "read",
            Role::Write => "write",
            Role::Takedown => "takedown",
            Role::Admin => "admin",
        }
    }
}

/// One accepted key: its id (logged, never secret), the digest of the secret, its roles.
#[derive(Debug, Clone)]
pub struct ApiKey {
    /// Name used in logs and in the takedown audit trail.
    pub id: String,
    hash: [u8; 32],
    roles: u8,
    /// The only tenant this key reaches (ADR 0031), `None` for every tenant.
    pub tenant: Option<String>,
}

/// Checks a tenant name: 1 to 128 bytes of ASCII letters, digits, `_`, `.` and `-`.
pub fn valid_tenant(t: &str) -> bool {
    !t.is_empty()
        && t.len() <= 128
        && t.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

fn check_tenant(id: &str, tenant: Option<&str>, roles: u8) -> anyhow::Result<()> {
    if let Some(t) = tenant {
        if !valid_tenant(t) {
            anyhow::bail!(
                "key {id:?}: tenant {t:?} must be 1 to 128 letters, digits, '_', '.' or '-'"
            );
        }
        if roles & Role::Admin.bit() != 0 {
            anyhow::bail!("key {id:?}: a tenant-scoped key cannot hold the admin role");
        }
    }
    Ok(())
}

impl ApiKey {
    /// Whether this key may use a route that needs `role`.
    pub fn allows(&self, role: Role) -> bool {
        self.roles & (role.bit() | Role::Admin.bit()) != 0
    }

    /// The entry to store in a keys file.
    pub fn entry(&self) -> KeyEntry {
        KeyEntry {
            id: self.id.clone(),
            sha256: hex(&self.hash),
            roles: [Role::Read, Role::Write, Role::Takedown, Role::Admin]
                .into_iter()
                .filter(|r| self.roles & r.bit() != 0)
                .map(|r| r.name().to_owned())
                .collect(),
            tenant: self.tenant.clone(),
        }
    }
}

/// A key as stored in the keys file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyEntry {
    /// Key name.
    pub id: String,
    /// Hex SHA-256 of the secret.
    pub sha256: String,
    /// Role names.
    pub roles: Vec<String>,
    /// The only tenant the key reaches (ADR 0031); absent for every tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct KeysFile {
    keys: Vec<KeyEntry>,
}

/// The keys a node accepts.
#[derive(Debug, Clone, Default)]
pub struct ApiKeys {
    keys: Vec<ApiKey>,
}

fn sha256(secret: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(digest(&SHA256, secret.as_bytes()).as_ref());
    out
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// Equality that takes the same time whatever the inputs (digests of fixed length).
fn same(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn roles_mask(roles: &[Role]) -> u8 {
    roles.iter().fold(0, |m, r| m | r.bit())
}

impl ApiKeys {
    /// Parses a keys file.
    pub fn from_json(bytes: &[u8]) -> anyhow::Result<ApiKeys> {
        let f: KeysFile = serde_json::from_slice(bytes)?;
        let mut keys = ApiKeys::default();
        for e in f.keys {
            let hash = unhex(&e.sha256)
                .ok_or_else(|| anyhow::anyhow!("key {:?}: sha256 must be 64 hex digits", e.id))?;
            let roles: Vec<Role> = e
                .roles
                .iter()
                .map(|r| Role::parse(r))
                .collect::<anyhow::Result<_>>()?;
            if roles.is_empty() {
                anyhow::bail!("key {:?} has no role", e.id);
            }
            check_tenant(&e.id, e.tenant.as_deref(), roles_mask(&roles))?;
            keys.push(ApiKey {
                id: e.id,
                hash,
                roles: roles_mask(&roles),
                tenant: e.tenant,
            })?;
        }
        Ok(keys)
    }

    /// Reads a keys file.
    pub fn load(path: &Path) -> anyhow::Result<ApiKeys> {
        let bytes = std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("reading keys file {}: {e}", path.display()))?;
        Self::from_json(&bytes).map_err(|e| anyhow::anyhow!("keys file {}: {e}", path.display()))
    }

    /// Serializes as a keys file (digests only).
    pub fn to_json(&self) -> String {
        let f = KeysFile {
            keys: self.keys.iter().map(ApiKey::entry).collect(),
        };
        serde_json::to_string_pretty(&f).expect("serializable")
    }

    /// Adds a key given in clear (for instance from an environment variable).
    pub fn add_plain(&mut self, id: &str, secret: &str, roles: &[Role]) -> anyhow::Result<()> {
        if secret.len() < 16 {
            anyhow::bail!("key {id:?} is too short: at least 16 characters");
        }
        self.push(ApiKey {
            id: id.to_owned(),
            hash: sha256(secret),
            roles: roles_mask(roles),
            tenant: None,
        })
    }

    fn push(&mut self, k: ApiKey) -> anyhow::Result<()> {
        if self.keys.iter().any(|x| x.id == k.id) {
            anyhow::bail!("duplicate key id {:?}", k.id);
        }
        self.keys.push(k);
        Ok(())
    }

    /// Number of keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether there is no key.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// The key matching an `Authorization` header value (`Bearer <secret>`), if any. Every
    /// key is compared, in constant time, whatever matches.
    pub fn authenticate(&self, header: Option<&str>) -> Option<&ApiKey> {
        let secret = header?.strip_prefix("Bearer ")?.trim();
        let h = sha256(secret);
        let mut found = None;
        for k in &self.keys {
            if same(&k.hash, &h) && found.is_none() {
                found = Some(k);
            }
        }
        found
    }
}

/// A new random secret (`cairn_` then 32 random bytes, URL-safe base64) and its key.
pub fn generate(
    id: &str,
    roles: &[Role],
    tenant: Option<&str>,
) -> anyhow::Result<(String, ApiKey)> {
    use base64::Engine as _;
    check_tenant(id, tenant, roles_mask(roles))?;
    let mut raw = [0u8; 32];
    SystemRandom::new()
        .fill(&mut raw)
        .map_err(|_| anyhow::anyhow!("no secure random source"))?;
    let secret = format!(
        "cairn_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
    );
    let key = ApiKey {
        id: id.to_owned(),
        hash: sha256(&secret),
        roles: roles_mask(roles),
        tenant: tenant.map(str::to_owned),
    };
    Ok((secret, key))
}

/// The role a request needs, or `None` for open routes (`/health`). Unknown routes need
/// `admin`, so a route added later is closed until it is mapped here.
pub fn required_role(method: &str, path: &str) -> Option<Role> {
    // Routes of a named collection need what the same route of `default` needs.
    if let Some(rest) = path.strip_prefix("/v1/collections/") {
        return match rest.split_once('/') {
            Some((_, "schema")) if method == "GET" => Some(Role::Read),
            Some((_, sub)) => required_role(method, &format!("/v1/{sub}")),
            None if method == "GET" => Some(Role::Read),
            // Dropping a collection, and anything unknown.
            None => Some(Role::Admin),
        };
    }
    match (method, path) {
        ("GET", "/v1/collections") => Some(Role::Read),
        (_, "/health") => None,
        ("GET", "/v1/schema") => Some(Role::Read),
        ("POST", "/v1/search") => Some(Role::Read),
        ("POST", "/v1/documents") => Some(Role::Write),
        ("POST", "/v1/documents/delete") => Some(Role::Takedown),
        ("GET", p) if p.starts_with("/v1/documents/") => Some(Role::Read),
        ("DELETE", p) if p.starts_with("/v1/documents/") => Some(Role::Takedown),
        ("DELETE", p) if p.starts_with("/v1/tenants/") => Some(Role::Takedown),
        _ => Some(Role::Admin),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_authenticate_by_secret_and_check_roles() {
        let (secret, key) = generate("ingest", &[Role::Write, Role::Read], None).unwrap();
        assert!(secret.starts_with("cairn_") && secret.len() > 40);
        let mut keys = ApiKeys::from_json(
            format!(
                r#"{{"keys":[{}]}}"#,
                serde_json::to_string(&key.entry()).unwrap()
            )
            .as_bytes(),
        )
        .unwrap();
        keys.add_plain("ops", "an-admin-secret-long-enough", &[Role::Admin])
            .unwrap();
        let k = keys
            .authenticate(Some(&format!("Bearer {secret}")))
            .unwrap();
        assert_eq!(k.id, "ingest");
        assert!(k.allows(Role::Read) && k.allows(Role::Write));
        assert!(!k.allows(Role::Takedown) && !k.allows(Role::Admin));
        let a = keys
            .authenticate(Some("Bearer an-admin-secret-long-enough"))
            .unwrap();
        assert!(a.allows(Role::Takedown) && a.allows(Role::Read));
        assert!(keys.authenticate(Some("Bearer wrong")).is_none());
        assert!(
            keys.authenticate(Some(&secret)).is_none(),
            "no Bearer prefix"
        );
        assert!(keys.authenticate(None).is_none());
        // The file keeps digests only, and reads back the same.
        let json = keys.to_json();
        assert!(!json.contains(&secret) && !json.contains("an-admin-secret"));
        let again = ApiKeys::from_json(json.as_bytes()).unwrap();
        assert_eq!(again.len(), 2);
        assert!(
            again
                .authenticate(Some(&format!("Bearer {secret}")))
                .is_some()
        );
    }

    #[test]
    fn bad_key_files_are_refused() {
        for bad in [
            r#"{"keys":[{"id":"a","sha256":"00","roles":["read"]}]}"#,
            r#"{"keys":[{"id":"a","sha256":"0000000000000000000000000000000000000000000000000000000000000000","roles":[]}]}"#,
            r#"{"keys":[{"id":"a","sha256":"0000000000000000000000000000000000000000000000000000000000000000","roles":["root"]}]}"#,
            r#"{"keys":[{"id":"a","sha256":"0000000000000000000000000000000000000000000000000000000000000000","roles":["read"]},{"id":"a","sha256":"0000000000000000000000000000000000000000000000000000000000000000","roles":["read"]}]}"#,
        ] {
            assert!(ApiKeys::from_json(bad.as_bytes()).is_err(), "{bad}");
        }
        assert!(
            ApiKeys::default()
                .add_plain("x", "short", &[Role::Read])
                .is_err()
        );
        let zeros = "0".repeat(64);
        for bad in [
            format!(r#"{{"keys":[{{"id":"a","sha256":"{zeros}","roles":["read"],"tenant":""}}]}}"#),
            format!(
                r#"{{"keys":[{{"id":"a","sha256":"{zeros}","roles":["read"],"tenant":"a b"}}]}}"#
            ),
            format!(
                r#"{{"keys":[{{"id":"a","sha256":"{zeros}","roles":["admin"],"tenant":"acme"}}]}}"#
            ),
        ] {
            assert!(ApiKeys::from_json(bad.as_bytes()).is_err(), "{bad}");
        }
        assert!(generate("t", &[Role::Admin], Some("acme")).is_err());
        assert!(generate("t", &[Role::Read], Some("acme/x")).is_err());
    }

    #[test]
    fn tenant_scoped_keys_round_trip() {
        let (secret, key) = generate("acme-app", &[Role::Read, Role::Write], Some("acme")).unwrap();
        let entry = serde_json::to_string(&key.entry()).unwrap();
        assert!(entry.contains(r#""tenant":"acme""#));
        let keys = ApiKeys::from_json(format!(r#"{{"keys":[{entry}]}}"#).as_bytes()).unwrap();
        let k = keys
            .authenticate(Some(&format!("Bearer {secret}")))
            .unwrap();
        assert_eq!(k.tenant.as_deref(), Some("acme"));
        // Unscoped entries carry no tenant field at all.
        let (_, open) = generate("ops", &[Role::Read], None).unwrap();
        assert!(
            !serde_json::to_string(&open.entry())
                .unwrap()
                .contains("tenant")
        );
    }

    #[test]
    fn every_route_needs_its_role() {
        assert_eq!(required_role("GET", "/health"), None);
        assert_eq!(required_role("GET", "/v1/schema"), Some(Role::Read));
        assert_eq!(required_role("POST", "/v1/search"), Some(Role::Read));
        assert_eq!(required_role("GET", "/v1/documents/7"), Some(Role::Read));
        assert_eq!(required_role("POST", "/v1/documents"), Some(Role::Write));
        assert_eq!(
            required_role("DELETE", "/v1/documents/7"),
            Some(Role::Takedown)
        );
        assert_eq!(
            required_role("POST", "/v1/documents/delete"),
            Some(Role::Takedown)
        );
        assert_eq!(
            required_role("DELETE", "/v1/tenants/acme"),
            Some(Role::Takedown)
        );
        assert_eq!(required_role("GET", "/v1/status"), Some(Role::Admin));
        assert_eq!(required_role("GET", "/v1/collections"), Some(Role::Read));
        assert_eq!(required_role("POST", "/v1/collections"), Some(Role::Admin));
        assert_eq!(
            required_role("GET", "/v1/collections/docs"),
            Some(Role::Read)
        );
        assert_eq!(
            required_role("DELETE", "/v1/collections/docs"),
            Some(Role::Admin)
        );
        assert_eq!(
            required_role("GET", "/v1/collections/docs/schema"),
            Some(Role::Read)
        );
        assert_eq!(
            required_role("POST", "/v1/collections/docs/documents"),
            Some(Role::Write)
        );
        assert_eq!(
            required_role("POST", "/v1/collections/docs/documents/delete"),
            Some(Role::Takedown)
        );
        assert_eq!(
            required_role("GET", "/v1/collections/docs/documents/7"),
            Some(Role::Read)
        );
        assert_eq!(
            required_role("DELETE", "/v1/collections/docs/tenants/acme"),
            Some(Role::Takedown)
        );
        assert_eq!(
            required_role("POST", "/v1/collections/docs/search"),
            Some(Role::Read)
        );
        assert_eq!(
            required_role("POST", "/v1/collections/docs/admin/merges"),
            Some(Role::Admin)
        );
        assert_eq!(required_role("POST", "/v1/admin/merges"), Some(Role::Admin));
        assert_eq!(required_role("PUT", "/v1/anything"), Some(Role::Admin));
    }
}
