//! Raft messages and persisted state.

use bytes::Bytes;
use cairn_core::codec::{Reader, Writer};
use cairn_core::{Error, LogIndex, NodeId, Result, Term};

/// A log entry as seen by Raft.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Index.
    pub index: LogIndex,
    /// Term.
    pub term: Term,
    /// Opaque command (empty for the leader's no-op).
    pub payload: Bytes,
}

/// State that must be durable before certain messages are sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HardState {
    /// Current term.
    pub term: Term,
    /// Who this node voted for in `term`.
    pub vote: Option<NodeId>,
    /// Highest known committed index (advisory; recomputed after restart).
    pub commit: LogIndex,
}

/// A snapshot: the state machine's content up to `last_index`, as opaque bytes (the shard
/// manifest; segment files are shipped out of band by the driver).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Last index covered.
    pub last_index: LogIndex,
    /// Term of `last_index`.
    pub last_term: Term,
    /// Opaque data.
    pub data: Bytes,
}

/// Raft protocol messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Pre-vote request for `term` (the term the sender would start).
    PreVote {
        /// Prospective term.
        term: Term,
        /// Sender's last log index.
        last_index: LogIndex,
        /// Sender's last log term.
        last_term: Term,
    },
    /// Pre-vote response.
    PreVoteResp {
        /// Prospective term echoed.
        term: Term,
        /// Whether the vote would be granted.
        granted: bool,
    },
    /// Vote request.
    Vote {
        /// Candidate's term.
        term: Term,
        /// Candidate's last log index.
        last_index: LogIndex,
        /// Candidate's last log term.
        last_term: Term,
    },
    /// Vote response.
    VoteResp {
        /// Responder's term.
        term: Term,
        /// Whether granted.
        granted: bool,
    },
    /// Append entries (a heartbeat when `entries` is empty).
    Append {
        /// Leader's term.
        term: Term,
        /// Index preceding `entries`.
        prev_index: LogIndex,
        /// Term of `prev_index`.
        prev_term: Term,
        /// Entries to append.
        entries: Vec<Entry>,
        /// Leader's commit index.
        commit: LogIndex,
        /// Heartbeat sequence (for ReadIndex).
        seq: u64,
    },
    /// Append response.
    AppendResp {
        /// Responder's term.
        term: Term,
        /// Whether the entries matched and were appended.
        success: bool,
        /// On success: last index now in the follower's log. On failure: hint for `next_index`.
        index: LogIndex,
        /// Heartbeat sequence echoed.
        seq: u64,
    },
    /// Install a snapshot.
    InstallSnapshot {
        /// Leader's term.
        term: Term,
        /// The snapshot.
        snapshot: Snapshot,
    },
    /// Snapshot response.
    SnapshotResp {
        /// Responder's term.
        term: Term,
        /// Last index the follower now holds.
        index: LogIndex,
    },
}

impl Message {
    /// The sender's term as carried by the message.
    pub fn term(&self) -> Term {
        match self {
            Message::PreVote { term, .. }
            | Message::PreVoteResp { term, .. }
            | Message::Vote { term, .. }
            | Message::VoteResp { term, .. }
            | Message::Append { term, .. }
            | Message::AppendResp { term, .. }
            | Message::InstallSnapshot { term, .. }
            | Message::SnapshotResp { term, .. } => *term,
        }
    }

    /// Encodes the message.
    pub fn encode(&self, w: &mut Writer) {
        match self {
            Message::PreVote {
                term,
                last_index,
                last_term,
            } => {
                w.u8(1)
                    .u64(term.get())
                    .u64(last_index.get())
                    .u64(last_term.get());
            }
            Message::PreVoteResp { term, granted } => {
                w.u8(2).u64(term.get()).u8(u8::from(*granted));
            }
            Message::Vote {
                term,
                last_index,
                last_term,
            } => {
                w.u8(3)
                    .u64(term.get())
                    .u64(last_index.get())
                    .u64(last_term.get());
            }
            Message::VoteResp { term, granted } => {
                w.u8(4).u64(term.get()).u8(u8::from(*granted));
            }
            Message::Append {
                term,
                prev_index,
                prev_term,
                entries,
                commit,
                seq,
            } => {
                w.u8(5)
                    .u64(term.get())
                    .u64(prev_index.get())
                    .u64(prev_term.get())
                    .u64(commit.get())
                    .u64(*seq)
                    .u32(entries.len() as u32);
                for e in entries {
                    w.u64(e.index.get()).u64(e.term.get()).bytes(&e.payload);
                }
            }
            Message::AppendResp {
                term,
                success,
                index,
                seq,
            } => {
                w.u8(6)
                    .u64(term.get())
                    .u8(u8::from(*success))
                    .u64(index.get())
                    .u64(*seq);
            }
            Message::InstallSnapshot { term, snapshot } => {
                w.u8(7)
                    .u64(term.get())
                    .u64(snapshot.last_index.get())
                    .u64(snapshot.last_term.get())
                    .bytes(&snapshot.data);
            }
            Message::SnapshotResp { term, index } => {
                w.u8(8).u64(term.get()).u64(index.get());
            }
        }
    }

    /// Decodes a message.
    pub fn decode(r: &mut Reader<'_>) -> Result<Message> {
        Ok(match r.u8()? {
            1 => Message::PreVote {
                term: Term(r.u64()?),
                last_index: LogIndex(r.u64()?),
                last_term: Term(r.u64()?),
            },
            2 => Message::PreVoteResp {
                term: Term(r.u64()?),
                granted: r.u8()? != 0,
            },
            3 => Message::Vote {
                term: Term(r.u64()?),
                last_index: LogIndex(r.u64()?),
                last_term: Term(r.u64()?),
            },
            4 => Message::VoteResp {
                term: Term(r.u64()?),
                granted: r.u8()? != 0,
            },
            5 => {
                let term = Term(r.u64()?);
                let prev_index = LogIndex(r.u64()?);
                let prev_term = Term(r.u64()?);
                let commit = LogIndex(r.u64()?);
                let seq = r.u64()?;
                let n = r.u32()? as usize;
                if n > 1 << 20 {
                    return Err(Error::corruption("append batch too large"));
                }
                let mut entries = Vec::with_capacity(n);
                for _ in 0..n {
                    entries.push(Entry {
                        index: LogIndex(r.u64()?),
                        term: Term(r.u64()?),
                        payload: Bytes::copy_from_slice(r.bytes()?),
                    });
                }
                Message::Append {
                    term,
                    prev_index,
                    prev_term,
                    entries,
                    commit,
                    seq,
                }
            }
            6 => Message::AppendResp {
                term: Term(r.u64()?),
                success: r.u8()? != 0,
                index: LogIndex(r.u64()?),
                seq: r.u64()?,
            },
            7 => Message::InstallSnapshot {
                term: Term(r.u64()?),
                snapshot: Snapshot {
                    last_index: LogIndex(r.u64()?),
                    last_term: Term(r.u64()?),
                    data: Bytes::copy_from_slice(r.bytes()?),
                },
            },
            8 => Message::SnapshotResp {
                term: Term(r.u64()?),
                index: LogIndex(r.u64()?),
            },
            t => return Err(Error::corruption(format!("unknown raft message tag {t}"))),
        })
    }
}
