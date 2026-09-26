//! Segment container: one immutable file holding named, page-aligned, hashed sections.
//!
//! ```text
//! [header 16 B: magic, version, reserved][pad to 4 KiB]
//! [section 0][pad][section 1][pad]...
//! [toc: u32 count, per section: name, offset, len, xxh3][u64 xxh3 of all bytes before toc]
//! [u32 toc_len][u32 crc32(toc + file hash)][magic tail 8 B]
//! ```
//!
//! The file hash lets a follower verify a shipped segment before installing it. Sections are
//! verified against their xxh3 when read whole; ranged reads skip verification and are meant for
//! sections that carry their own checksums.

use bytes::Bytes;
use cairn_core::codec::{Reader, Writer, crc32, xxh3};
use cairn_core::{Disk, Error, OpenMode, Result, Runtime};
use xxhash_rust::xxh3::Xxh3;

const MAGIC: &[u8; 8] = b"CRNSEG01";
const MAGIC_TAIL: &[u8; 8] = b"CRNSEGEN";
/// Newest segment format version this build writes and reads. The version covers the container
/// and the encoding of every section (vector, text, structured, disk index): any change that an
/// older reader cannot decode bumps it (ADR 0004, ADR 0018).
pub const SEGMENT_VERSION: u32 = 1;
/// Oldest segment format version this build reads.
pub const SEGMENT_VERSION_MIN_READ: u32 = 1;
/// Oldest segment format version this build can still write, so that a cluster in the middle
/// of a rolling upgrade keeps writing what its oldest member reads.
pub const SEGMENT_VERSION_MIN_WRITE: u32 = 1;

/// The segment format a node writes, given the lowest format its peers read (`None` until
/// every peer has said hello): the newest this build writes that every peer reads, never older
/// than this build can write. A peer that reads only older formats than
/// `SEGMENT_VERSION_MIN_WRITE` cannot be served and must be upgraded first.
pub fn negotiated_segment_version(peers_max: Option<u32>) -> u32 {
    match peers_max {
        Some(p) => p.clamp(SEGMENT_VERSION_MIN_WRITE, SEGMENT_VERSION),
        None => SEGMENT_VERSION_MIN_WRITE,
    }
}
/// Section alignment.
pub const PAGE: u64 = 4096;
const TAIL_LEN: u64 = 4 + 4 + 8;
const HEADER_LEN: usize = 16;

/// Location of a section inside the container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionMeta {
    /// Section name (for example `vec.img.f32`).
    pub name: String,
    /// Byte offset in the file.
    pub offset: u64,
    /// Length in bytes.
    pub len: u64,
    /// xxh3-64 of the section bytes.
    pub hash: u64,
}

fn pad_to(len: u64) -> u64 {
    len.div_ceil(PAGE) * PAGE
}

/// Writes a segment file section by section.
pub struct SegmentWriter<R: Runtime> {
    rt: R,
    file: <R::Disk as Disk>::File,
    path: String,
    tmp_path: String,
    offset: u64,
    sections: Vec<SectionMeta>,
    hasher: Xxh3,
}

impl<R: Runtime> SegmentWriter<R> {
    /// Starts writing `path` in the newest format (written as `path.tmp` and renamed on finish).
    pub async fn create(rt: R, path: &str) -> Result<Self> {
        Self::create_version(rt, path, SEGMENT_VERSION).await
    }

    /// Starts writing `path` in format `version` (between `SEGMENT_VERSION_MIN_WRITE` and
    /// `SEGMENT_VERSION`).
    pub async fn create_version(rt: R, path: &str, version: u32) -> Result<Self> {
        if !(SEGMENT_VERSION_MIN_WRITE..=SEGMENT_VERSION).contains(&version) {
            return Err(Error::UnsupportedVersion {
                found: version,
                supported: SEGMENT_VERSION,
            });
        }
        let tmp_path = format!("{path}.tmp");
        let file = rt.disk().open(&tmp_path, OpenMode::CreateTruncate).await?;
        let mut header = Writer::with_capacity(PAGE as usize);
        header.raw(MAGIC).u32(version).u32(0);
        debug_assert_eq!(header.len(), HEADER_LEN);
        header.raw(&vec![0u8; PAGE as usize - HEADER_LEN]);
        let mut hasher = Xxh3::new();
        hasher.update(header.as_slice());
        rt.disk().write_at(&file, 0, header.into_bytes()).await?;
        Ok(SegmentWriter {
            rt,
            file,
            path: path.to_owned(),
            tmp_path,
            offset: PAGE,
            sections: Vec::new(),
            hasher,
        })
    }

    /// Appends a section. Names must be unique.
    pub async fn add_section(&mut self, name: &str, data: &[u8]) -> Result<()> {
        if self.sections.iter().any(|s| s.name == name) {
            return Err(Error::Internal(format!("duplicate section {name:?}")));
        }
        let meta = SectionMeta {
            name: name.to_owned(),
            offset: self.offset,
            len: data.len() as u64,
            hash: xxh3(data),
        };
        let padded = pad_to(data.len() as u64);
        let mut buf = Vec::with_capacity(padded as usize);
        buf.extend_from_slice(data);
        buf.resize(padded as usize, 0);
        self.hasher.update(&buf);
        self.rt
            .disk()
            .write_at(&self.file, self.offset, Bytes::from(buf))
            .await?;
        self.offset += padded;
        self.sections.push(meta);
        Ok(())
    }

    /// Writes the table of contents, syncs, renames into place. Returns `(file length, file hash)`.
    pub async fn finish(self) -> Result<(u64, u64)> {
        let mut toc = Writer::new();
        toc.u32(self.sections.len() as u32);
        for s in &self.sections {
            toc.str(&s.name).u64(s.offset).u64(s.len).u64(s.hash);
        }
        let file_hash = self.hasher.digest();
        toc.u64(file_hash);
        let toc_len = toc.len() as u32;
        let crc = crc32(toc.as_slice());
        let mut tail = Writer::with_capacity(toc.len() + TAIL_LEN as usize);
        tail.raw(toc.as_slice())
            .u32(toc_len)
            .u32(crc)
            .raw(MAGIC_TAIL);
        let total = self.offset + tail.len() as u64;
        let disk = self.rt.disk();
        disk.write_at(&self.file, self.offset, tail.into_bytes())
            .await?;
        disk.sync(&self.file).await?;
        disk.rename(&self.tmp_path, &self.path).await?;
        Ok((total, file_hash))
    }
}

/// Reads sections of a segment file.
pub struct SegmentReader<R: Runtime> {
    rt: R,
    file: <R::Disk as Disk>::File,
    len: u64,
    sections: Vec<SectionMeta>,
    file_hash: u64,
}

impl<R: Runtime> SegmentReader<R> {
    /// Opens `path`, validating header and table of contents.
    pub async fn open(rt: R, path: &str) -> Result<Self> {
        let disk = rt.disk();
        let file = disk.open(path, OpenMode::Read).await?;
        let len = disk.len(&file).await?;
        if len < PAGE + TAIL_LEN {
            return Err(Error::corruption(format!(
                "segment {path} too short ({len} bytes)"
            )));
        }
        let header = disk.read_at(&file, 0, HEADER_LEN).await?;
        let mut r = Reader::new(&header);
        if r.raw(8)? != MAGIC {
            return Err(Error::corruption(format!("segment {path}: bad magic")));
        }
        let version = r.u32()?;
        if !(SEGMENT_VERSION_MIN_READ..=SEGMENT_VERSION).contains(&version) {
            return Err(Error::UnsupportedVersion {
                found: version,
                supported: SEGMENT_VERSION,
            });
        }
        let tail = disk
            .read_at(&file, len - TAIL_LEN, TAIL_LEN as usize)
            .await?;
        let mut r = Reader::new(&tail);
        let toc_len = r.u32()? as u64;
        let crc = r.u32()?;
        if r.raw(8)? != MAGIC_TAIL {
            return Err(Error::corruption(format!("segment {path}: bad tail magic")));
        }
        if toc_len + TAIL_LEN + PAGE > len {
            return Err(Error::corruption(format!(
                "segment {path}: toc length {toc_len} out of range"
            )));
        }
        let toc = disk
            .read_at(&file, len - TAIL_LEN - toc_len, toc_len as usize)
            .await?;
        if crc32(&toc) != crc {
            return Err(Error::corruption(format!(
                "segment {path}: toc checksum mismatch"
            )));
        }
        let mut r = Reader::new(&toc);
        let n = r.u32()? as usize;
        let mut sections = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            let name = r.str()?.to_owned();
            let offset = r.u64()?;
            let slen = r.u64()?;
            let hash = r.u64()?;
            if offset < PAGE
                || offset
                    .checked_add(slen)
                    .is_none_or(|end| end > len - TAIL_LEN - toc_len)
            {
                return Err(Error::corruption(format!(
                    "segment {path}: section {name:?} out of range"
                )));
            }
            sections.push(SectionMeta {
                name,
                offset,
                len: slen,
                hash,
            });
        }
        let file_hash = r.u64()?;
        r.finish()?;
        Ok(SegmentReader {
            rt,
            file,
            len,
            sections,
            file_hash,
        })
    }

    /// Section directory.
    pub fn sections(&self) -> &[SectionMeta] {
        &self.sections
    }

    /// Metadata of the named section.
    pub fn section(&self, name: &str) -> Option<&SectionMeta> {
        self.sections.iter().find(|s| s.name == name)
    }

    /// Whether the segment has a section named `name`.
    pub fn has_section(&self, name: &str) -> bool {
        self.section(name).is_some()
    }

    /// File length in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the container has no sections.
    pub fn is_empty(&self) -> bool {
        self.sections.is_empty()
    }

    /// Hash of the file body as recorded in the footer.
    pub fn file_hash(&self) -> u64 {
        self.file_hash
    }

    /// Reads and verifies a whole section.
    pub async fn read_section(&self, name: &str) -> Result<Bytes> {
        let meta = self
            .section(name)
            .ok_or_else(|| Error::corruption(format!("missing section {name:?}")))?;
        let data = self
            .rt
            .disk()
            .read_at(&self.file, meta.offset, meta.len as usize)
            .await?;
        if xxh3(&data) != meta.hash {
            return Err(Error::corruption(format!(
                "section {name:?} checksum mismatch"
            )));
        }
        Ok(data)
    }

    /// The runtime (for offloading work on this segment's data).
    pub fn runtime(&self) -> &R {
        &self.rt
    }

    /// The runtime's readahead hint for mapped sections (see `Disk::prefetcher`).
    pub fn prefetcher(&self) -> Option<fn(&[u8])> {
        self.rt.disk().prefetcher()
    }

    /// Maps a whole section read-only (see `Disk::map`) after verifying its checksum; the
    /// verification reads the section once, sequentially. Sections start on a page boundary,
    /// so page-aligned offsets inside the section stay page-aligned in the file.
    pub async fn map_section(&self, name: &str) -> Result<Bytes> {
        let meta = self
            .section(name)
            .ok_or_else(|| Error::corruption(format!("missing section {name:?}")))?;
        let file = self.rt.disk().map(&self.file).await?;
        let end = meta
            .offset
            .checked_add(meta.len)
            .filter(|e| *e <= file.len() as u64)
            .ok_or_else(|| Error::corruption(format!("section {name:?} beyond mapped file")))?;
        let data = file.slice(meta.offset as usize..end as usize);
        // Hashing a large mapped section reads it all: off the core, so a loading replica keeps
        // answering Raft (a stalled follower made its leader re-send appends).
        let (d, hash) = (data.clone(), meta.hash);
        let ok = self.rt.offload(move || xxh3(&d) == hash).await;
        if !ok {
            return Err(Error::corruption(format!(
                "section {name:?} checksum mismatch"
            )));
        }
        Ok(data)
    }

    /// The whole file mapped, with its section table: readable from any thread (ADR 0026).
    pub async fn mapped(&self) -> Result<MappedSegment> {
        let data = self.rt.disk().map(&self.file).await?;
        Ok(MappedSegment {
            data,
            sections: std::sync::Arc::new(self.sections.clone()),
        })
    }

    /// Reads `len` bytes at `offset` within a section, without verification.
    pub async fn read_range(&self, name: &str, offset: u64, len: usize) -> Result<Bytes> {
        let meta = self
            .section(name)
            .ok_or_else(|| Error::corruption(format!("missing section {name:?}")))?;
        if offset + len as u64 > meta.len {
            return Err(Error::corruption(format!(
                "range {offset}+{len} outside section {name:?}"
            )));
        }
        self.rt
            .disk()
            .read_at(&self.file, meta.offset + offset, len)
            .await
    }

    /// As [`SegmentReader::verify_file_hash`], hashing a mapping of the file on a helper
    /// thread: nothing runs on the calling core but the mapping (ADR 0027).
    pub async fn verify_file_hash_off(&self) -> Result<bool> {
        let body_len = self
            .sections
            .iter()
            .map(|s| s.offset + pad_to(s.len))
            .max()
            .unwrap_or(PAGE) as usize;
        let data = self.rt.disk().map(&self.file).await?;
        if data.len() < body_len {
            return Ok(false);
        }
        let expected = self.file_hash;
        Ok(self
            .rt
            .offload(move || {
                let mut h = Xxh3::new();
                h.update(&data[..body_len]);
                h.digest() == expected
            })
            .await)
    }

    /// Recomputes the body hash from disk and compares it with the footer (shipping check).
    pub async fn verify_file_hash(&self) -> Result<bool> {
        let body_len = self
            .sections
            .iter()
            .map(|s| s.offset + pad_to(s.len))
            .max()
            .unwrap_or(PAGE);
        let mut hasher = Xxh3::new();
        let mut off = 0u64;
        while off < body_len {
            let chunk = (body_len - off).min(1 << 20) as usize;
            let data = self.rt.disk().read_at(&self.file, off, chunk).await?;
            hasher.update(&data);
            off += chunk as u64;
        }
        Ok(hasher.digest() == self.file_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::NodeId;
    use cairn_sim::{SimConfig, Simulation};

    #[test]
    fn roundtrip_sections_and_ranges() {
        let (sim, mut ex) = Simulation::new(1, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            rt.disk().create_dir_all("segs").await.unwrap();
            let mut w = SegmentWriter::create(rt.clone(), "segs/1.seg")
                .await
                .unwrap();
            let big: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
            w.add_section("docids", b"abc").await.unwrap();
            w.add_section("vec.img.f32", &big).await.unwrap();
            w.add_section("empty", b"").await.unwrap();
            assert!(w.add_section("docids", b"x").await.is_err());
            let (len, hash) = w.finish().await.unwrap();
            assert!(!rt.disk().exists("segs/1.seg.tmp").await.unwrap());
            let r = SegmentReader::open(rt.clone(), "segs/1.seg").await.unwrap();
            assert_eq!(r.len(), len);
            assert_eq!(r.file_hash(), hash);
            assert_eq!(r.sections().len(), 3);
            assert_eq!(&r.read_section("docids").await.unwrap()[..], b"abc");
            assert_eq!(&r.read_section("vec.img.f32").await.unwrap()[..], &big[..]);
            assert_eq!(r.read_section("empty").await.unwrap().len(), 0);
            assert_eq!(
                &r.read_range("vec.img.f32", 5000, 3).await.unwrap()[..],
                &big[5000..5003]
            );
            assert!(r.read_range("vec.img.f32", 9999, 2).await.is_err());
            assert!(r.read_section("nope").await.is_err());
            assert!(r.verify_file_hash().await.unwrap());
            assert_eq!(r.section("vec.img.f32").unwrap().offset % PAGE, 0);
        });
    }

    #[test]
    fn corruption_anywhere_is_detected() {
        let (sim, mut ex) = Simulation::new(2, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        let rt2 = rt.clone();
        ex.block_on(async move {
            let mut w = SegmentWriter::create(rt2, "s.seg").await.unwrap();
            w.add_section("a", &[1u8; 100]).await.unwrap();
            w.add_section("b", &[2u8; 5000]).await.unwrap();
            w.finish().await.unwrap();
        });
        let image = sim
            .disk(NodeId(1))
            .inspect(|d| d.durable_content("s.seg").unwrap().to_vec());
        let toc_start = image.len() - TAIL_LEN as usize - 60;
        // Flip one byte in: header, section a, section b, toc, tail. Each must be caught.
        let probes = [
            0usize,
            9,
            PAGE as usize + 3,
            2 * PAGE as usize + 4000,
            toc_start + 5,
            image.len() - 6,
        ];
        for pos in probes {
            let mut bad = image.clone();
            bad[pos] ^= 0x10;
            let (sim, mut ex) = Simulation::new(3, SimConfig::default());
            sim.disk(NodeId(1))
                .modify(|d| d.set_durable_file("s.seg", &bad));
            let rt = sim.runtime(NodeId(1), &ex.handle());
            let detected = ex.block_on(async move {
                match SegmentReader::open(rt.clone(), "s.seg").await {
                    Err(_) => true,
                    Ok(r) => {
                        r.read_section("a").await.is_err()
                            || r.read_section("b").await.is_err()
                            || !r.verify_file_hash().await.unwrap()
                    }
                }
            });
            assert!(detected, "flip at {pos} undetected");
        }
        // Truncation anywhere is detected too.
        for cut in [
            10usize,
            PAGE as usize + 50,
            image.len() - 30,
            image.len() - 1,
        ] {
            let (sim, mut ex) = Simulation::new(3, SimConfig::default());
            sim.disk(NodeId(1))
                .modify(|d| d.set_durable_file("s.seg", &image[..cut]));
            let rt = sim.runtime(NodeId(1), &ex.handle());
            assert!(
                ex.block_on(async move { SegmentReader::open(rt.clone(), "s.seg").await.is_err() }),
                "cut at {cut}"
            );
        }
    }

    /// ADR 0018: readers accept exactly the supported range, writers write only versions this
    /// build can write, and the negotiated version never exceeds what every peer reads.
    #[test]
    fn format_versions_are_a_contract() {
        assert_eq!(negotiated_segment_version(None), SEGMENT_VERSION_MIN_WRITE);
        assert_eq!(negotiated_segment_version(Some(u32::MAX)), SEGMENT_VERSION);
        assert_eq!(
            negotiated_segment_version(Some(0)),
            SEGMENT_VERSION_MIN_WRITE
        );
        assert!(negotiated_segment_version(Some(SEGMENT_VERSION)) <= SEGMENT_VERSION);
        let (sim, mut ex) = Simulation::new(3, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            assert!(
                SegmentWriter::create_version(rt.clone(), "x.seg", SEGMENT_VERSION + 1)
                    .await
                    .is_err()
            );
            assert!(
                SegmentWriter::create_version(rt.clone(), "x.seg", SEGMENT_VERSION_MIN_WRITE - 1)
                    .await
                    .is_err()
            );
            let mut w = SegmentWriter::create_version(rt.clone(), "v.seg", SEGMENT_VERSION)
                .await
                .unwrap();
            w.add_section("docids", b"abc").await.unwrap();
            w.finish().await.unwrap();
            assert!(SegmentReader::open(rt.clone(), "v.seg").await.is_ok());
            // The version field sits after the 8-byte magic.
            let f = rt.disk().open("v.seg", OpenMode::ReadWrite).await.unwrap();
            for v in [SEGMENT_VERSION + 1, SEGMENT_VERSION_MIN_READ - 1] {
                rt.disk()
                    .write_at(&f, 8, bytes::Bytes::copy_from_slice(&v.to_le_bytes()))
                    .await
                    .unwrap();
                match SegmentReader::open(rt.clone(), "v.seg").await {
                    Err(Error::UnsupportedVersion { found, .. }) => assert_eq!(found, v),
                    Err(e) => panic!("version {v}: unexpected error {e}"),
                    Ok(_) => panic!("version {v} must be refused"),
                }
            }
        });
    }
}

/// A segment file mapped with its section table (from [`SegmentReader::mapped`]). It needs no
/// runtime, so a helper thread can decode the segment's indexes from it (ADR 0026). Every
/// section read is checked against its hash.
#[derive(Clone)]
pub struct MappedSegment {
    data: Bytes,
    sections: std::sync::Arc<Vec<SectionMeta>>,
}

impl MappedSegment {
    /// Whether the segment has a section named `name`.
    pub fn has_section(&self, name: &str) -> bool {
        self.sections.iter().any(|s| s.name == name)
    }

    /// The named section, after checking its hash.
    pub fn read_section(&self, name: &str) -> Result<Bytes> {
        let meta = self
            .sections
            .iter()
            .find(|s| s.name == name)
            .ok_or_else(|| Error::corruption(format!("missing section {name:?}")))?;
        let end = meta
            .offset
            .checked_add(meta.len)
            .filter(|e| *e <= self.data.len() as u64)
            .ok_or_else(|| Error::corruption(format!("section {name:?} beyond mapped file")))?;
        let data = self.data.slice(meta.offset as usize..end as usize);
        if xxh3(&data) != meta.hash {
            return Err(Error::corruption(format!(
                "section {name:?} checksum mismatch"
            )));
        }
        Ok(data)
    }
}
