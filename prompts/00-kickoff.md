# Kickoff prompt (Phase 0): paste this as the first message in Claude Code

You are starting work on **Cairn**, a distributed multimodal database written in Rust. The
repository you are in contains a workspace scaffold and the full project brief.

## Read first, in this order
1. `SPEC.md`: goals, decisions made, open questions, roadmap, success criteria.
2. `CLAUDE.md`: working rules, commands, definition of done.
3. `docs/adr/`: existing architecture decision records.
4. `Cargo.toml`: workspace and dependency list.

## Why this project exists
A media archive (illustrative case, not a real client) runs Elasticsearch + a vector DB +
application-side fusion. Queries mixing image similarity, speech/text and rights filters take
seconds, filtered ANN wastes most candidates, and a rights takedown stays visible in the vector
index long after the metadata changes. Cairn replaces that with one engine and one query plan
where a takedown is a single atomic, Raft-replicated write.

## Your job in this session: Phase 0 only. Write NO feature code.

1. **Validate the scaffold.** The `Cargo.toml` was generated without a Rust toolchain and is not
   compile-checked. Run `cargo check --workspace`, fix whatever breaks (versions, features, lints),
   and report each fix. Pay particular attention to `raft` 0.7 (old crate: codec features,
   `rand` 0.8 coexisting with `rand` 0.10, maintenance status).
2. **Challenge the brief.** Read the DECIDED items (D1–D7) critically. If any looks wrong,
   incomplete, or contradicts another, say so with evidence before proposing anything else.
   I would rather hear "this is a mistake" now than after 10k lines of code.
3. **Resolve the OPEN questions (O1–O8).** For each: 2–3 options, trade-offs, your recommendation,
   and what experiment or benchmark would confirm it cheaply. Record accepted choices as ADRs
   (`docs/adr/0002-…`, and so on) with status `proposed`. I will accept them.
4. **Propose the architecture**: refine the crate layout in `SPEC.md` section 6, define the
   `Clock` / `Disk` / `Network` trait boundaries in prose (no implementation), and describe how a
   write travels from client to Raft log to segment to index, and how a hybrid query travels through
   the planner.
5. **Design the verification strategy**: how the deterministic simulator works, what faults it
   injects, what the linearizability checker verifies, how the takedown signature test is expressed,
   and what goes into fuzzing / loom / Miri.
6. **List the top risks**, ranked by likelihood × impact, each with a cheap early experiment that
   would retire or confirm it (for example: "filtered ANN at 0.1% selectivity may need a different
   algorithm; test with X on SIFT1M in Phase 2 week 1").
7. **Refine the roadmap**: split each phase into concrete milestones with measurable exit criteria.

## Output
- Updated or new files in `docs/` (ADRs, `docs/architecture.md`, `docs/verification.md`, `docs/risks.md`).
- A short summary in the chat: what you changed, what you want me to decide, what worries you most.
- A green `cargo check --workspace` and `cargo fmt --check`.

## Constraints
- Stay in Phase 0. Do not implement engine code, even "just a quick prototype".
- Do not add heavy dependencies without asking. Evidence over enthusiasm: no performance
  claim without a source or a planned benchmark.
- If a fact about a crate, dataset or algorithm is something you cannot verify, mark it
  "unverified" instead of stating it as fact.
- Stop and ask if two goals in the spec conflict.
