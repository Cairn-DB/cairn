//! The manifest: the one mutable file of a shard, replaced atomically.
//!
//! Layout: magic, version, payload length, payload, crc32 of everything before it. Written to
//! `<name>.tmp`, synced, then renamed over `<name>` (the `Disk` contract makes the rename
//! durable). Readers verify magic, version and checksum.

use crate::codec::{Reader, Writer, crc32};
use bytes::Bytes;
use cairn_core::error::IoErrorKind;
use cairn_core::{Disk, Error, OpenMode, Result, Runtime};

const MAGIC: &[u8; 8] = b"CRNMANI1";
/// Manifest container version. Bump on any layout change (ADR 0004).
pub const MANIFEST_VERSION: u32 = 1;

/// Something that can be stored in a manifest file.
pub trait Manifest: Sized {
    /// Encodes the payload.
    fn encode(&self, w: &mut Writer);
    /// Decodes the payload; must consume everything.
    fn decode(r: &mut Reader<'_>) -> Result<Self>;
}

/// Reads and atomically replaces a manifest file at a fixed path.
pub struct ManifestStore<R: Runtime> {
    rt: R,
    path: String,
    tmp_path: String,
}

impl<R: Runtime> ManifestStore<R> {
    /// Store for the manifest at `path` (relative to the data directory).
    pub fn new(rt: R, path: impl Into<String>) -> Self {
        let path = path.into();
        let tmp_path = format!("{path}.tmp");
        ManifestStore { rt, path, tmp_path }
    }

    /// Path of the manifest file.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Encodes `m` into the on-disk container.
    pub fn encode<M: Manifest>(m: &M) -> Bytes {
        let mut payload = Writer::new();
        m.encode(&mut payload);
        let mut w = Writer::with_capacity(payload.len() + 24);
        w.raw(MAGIC).u32(MANIFEST_VERSION).bytes(payload.as_slice());
        let crc = crc32(w.as_slice());
        w.u32(crc);
        w.into_bytes()
    }

    /// Decodes an on-disk container.
    pub fn decode<M: Manifest>(bytes: &[u8]) -> Result<M> {
        if bytes.len() < 16 {
            return Err(Error::corruption("manifest too short"));
        }
        let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let expected = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        if crc32(body) != expected {
            return Err(Error::corruption("manifest checksum mismatch"));
        }
        let mut r = Reader::new(body);
        if r.raw(8)? != MAGIC {
            return Err(Error::corruption("manifest magic mismatch"));
        }
        let version = r.u32()?;
        if version != MANIFEST_VERSION {
            return Err(Error::UnsupportedVersion {
                found: version,
                supported: MANIFEST_VERSION,
            });
        }
        let payload = r.bytes()?;
        r.finish()?;
        let mut pr = Reader::new(payload);
        let m = M::decode(&mut pr)?;
        pr.finish()?;
        Ok(m)
    }

    /// Loads the manifest, or `None` if no manifest exists yet. A leftover `.tmp` is removed.
    pub async fn load<M: Manifest>(&self) -> Result<Option<M>> {
        let disk = self.rt.disk();
        if disk.exists(&self.tmp_path).await? {
            disk.remove(&self.tmp_path).await?;
        }
        if !disk.exists(&self.path).await? {
            return Ok(None);
        }
        let f = disk.open(&self.path, OpenMode::Read).await?;
        let len = disk.len(&f).await?;
        let bytes = disk.read_at(&f, 0, len as usize).await?;
        Self::decode(&bytes).map(Some)
    }

    /// Atomically replaces the manifest with `m`.
    pub async fn store<M: Manifest>(&self, m: &M) -> Result<()> {
        let disk = self.rt.disk();
        let bytes = Self::encode(m);
        let f = disk.open(&self.tmp_path, OpenMode::CreateTruncate).await?;
        disk.write_at(&f, 0, bytes).await?;
        disk.sync(&f).await?;
        disk.rename(&self.tmp_path, &self.path).await?;
        Ok(())
    }
}

impl<R: Runtime> ManifestStore<R> {
    /// Whether an error means "no manifest yet" rather than damage.
    pub fn is_missing(e: &Error) -> bool {
        e.io_kind() == Some(IoErrorKind::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::NodeId;
    use cairn_sim::{SimConfig, Simulation};

    #[derive(Debug, PartialEq, Clone)]
    struct Demo {
        generation: u64,
        names: Vec<String>,
    }

    impl Manifest for Demo {
        fn encode(&self, w: &mut Writer) {
            w.u64(self.generation).u32(self.names.len() as u32);
            for n in &self.names {
                w.str(n);
            }
        }
        fn decode(r: &mut Reader<'_>) -> Result<Self> {
            let generation = r.u64()?;
            let n = r.u32()?;
            let mut names = Vec::new();
            for _ in 0..n {
                names.push(r.str()?.to_owned());
            }
            Ok(Demo { generation, names })
        }
    }

    #[test]
    fn container_roundtrip_and_corruption_detection() {
        let m = Demo {
            generation: 7,
            names: vec!["a".into(), "bb".into()],
        };
        let bytes = ManifestStore::<cairn_sim::SimRuntime>::encode(&m);
        assert_eq!(
            ManifestStore::<cairn_sim::SimRuntime>::decode::<Demo>(&bytes).unwrap(),
            m
        );
        for i in 0..bytes.len() {
            let mut bad = bytes.to_vec();
            bad[i] ^= 0x01;
            assert!(
                ManifestStore::<cairn_sim::SimRuntime>::decode::<Demo>(&bad).is_err(),
                "flip at {i} undetected"
            );
        }
        for cut in 0..bytes.len() {
            assert!(ManifestStore::<cairn_sim::SimRuntime>::decode::<Demo>(&bytes[..cut]).is_err());
        }
    }

    #[test]
    fn store_survives_crash_at_any_point_with_old_or_new_manifest() {
        for seed in 0..40u64 {
            let (sim, mut ex) = Simulation::new(seed, SimConfig::default());
            let rt = sim.runtime(NodeId(1), &ex.handle());
            let store = ManifestStore::new(rt.clone(), "MANIFEST");
            let v1 = Demo {
                generation: 1,
                names: vec!["one".into()],
            };
            let v2 = Demo {
                generation: 2,
                names: vec!["one".into(), "two".into()],
            };
            let (s1, v1c) = (ManifestStore::new(rt.clone(), "MANIFEST"), v1.clone());
            ex.block_on(async move { s1.store(&v1c).await.unwrap() });
            // Start storing v2 and crash after a seed-dependent delay.
            let (s2, v2c) = (ManifestStore::new(rt.clone(), "MANIFEST"), v2.clone());
            rt.spawn(async move {
                let _ = s2.store(&v2c).await;
            });
            let delay = cairn_core::Duration::from_micros(seed * 37);
            let h = ex.handle();
            ex.block_on(h.sleep(delay));
            sim.crash(NodeId(1), &mut ex);
            let rt2 = sim.runtime(NodeId(1), &ex.handle());
            let store2 = ManifestStore::new(rt2, "MANIFEST");
            let loaded = ex.block_on(async move { store2.load::<Demo>().await.unwrap() });
            assert!(
                loaded == Some(v1.clone()) || loaded == Some(v2.clone()),
                "seed {seed}: {loaded:?}"
            );
            drop(store);
        }
    }
}
