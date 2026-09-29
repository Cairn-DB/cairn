//! Proofs of deletion (ADR 0031): a report of what every replica said about some documents,
//! signed with the node's Ed25519 key, and its verification.
//!
//! The signature covers the report serialized as compact JSON with sorted keys, which is how
//! `serde_json` writes it: anyone can check it with the public key in the proof, or better, a
//! public key they obtained from the node beforehand (`GET /v1/deletions/key`).

use anyhow::{Context, bail};
use base64::Engine as _;
use ring::rand::SystemRandom;
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use serde_json::{Value, json};
use std::path::Path;

/// Loads the node's proof key, or creates it on first start (PKCS#8, only readable by the
/// owner).
pub fn load_or_create_key(path: &Path) -> anyhow::Result<Ed25519KeyPair> {
    if !path.exists() {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .map_err(|_| anyhow::anyhow!("no secure random source"))?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, pkcs8.as_ref())
            .with_context(|| format!("writing {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
    }
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    Ed25519KeyPair::from_pkcs8(&bytes)
        .map_err(|_| anyhow::anyhow!("{}: not an Ed25519 PKCS#8 key", path.display()))
}

fn b64(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// The node's public key, base64.
pub fn public_key(key: &Ed25519KeyPair) -> String {
    b64(key.public_key().as_ref())
}

/// A signed proof: `{report, signature, public_key, algorithm}`.
pub fn sign(key: &Ed25519KeyPair, report: Value) -> Value {
    let bytes = serde_json::to_vec(&report).expect("serializable");
    json!({
        "report": report,
        "signature": b64(key.sign(&bytes).as_ref()),
        "public_key": public_key(key),
        "algorithm": "Ed25519",
    })
}

/// Checks a proof's signature, with `trusted` (base64) when given, else the key in the proof
/// (which only shows the proof was not altered since it was signed by that key). Returns the
/// report.
pub fn verify(proof: &Value, trusted: Option<&str>) -> anyhow::Result<Value> {
    let report = proof.get("report").context("no report")?;
    let sig = proof
        .get("signature")
        .and_then(Value::as_str)
        .context("no signature")?;
    let key = match trusted {
        Some(k) => k,
        None => proof
            .get("public_key")
            .and_then(Value::as_str)
            .context("no public key")?,
    };
    let dec = |s: &str| base64::engine::general_purpose::STANDARD.decode(s);
    let bytes = serde_json::to_vec(report)?;
    if UnparsedPublicKey::new(&ED25519, dec(key)?)
        .verify(&bytes, &dec(sig)?)
        .is_err()
    {
        bail!("the signature does not match the report");
    }
    Ok(report.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proofs_verify_and_alterations_do_not() {
        let dir = std::env::temp_dir().join(format!("cairn-proof-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let key = load_or_create_key(&dir.join("k.pk8")).unwrap();
        // The same key comes back on the next start.
        let again = load_or_create_key(&dir.join("k.pk8")).unwrap();
        assert_eq!(public_key(&key), public_key(&again));
        let proof = sign(
            &key,
            json!({ "verdict": "deleted everywhere", "documents": [{ "id": "a" }] }),
        );
        assert_eq!(
            verify(&proof, None).unwrap()["verdict"],
            "deleted everywhere"
        );
        assert!(verify(&proof, Some(&public_key(&key))).is_ok());
        // Any change to the report breaks it, and so does another key.
        let mut altered = proof.clone();
        altered["report"]["verdict"] = json!("not proven");
        assert!(verify(&altered, None).is_err());
        let other = Ed25519KeyPair::from_pkcs8(
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        assert!(verify(&proof, Some(&public_key(&other))).is_err());
        // Re-serializing a proof read from text gives the same bytes (sorted keys).
        let text = serde_json::to_string_pretty(&proof).unwrap();
        let back: Value = serde_json::from_str(&text).unwrap();
        assert!(verify(&back, None).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
