# Releasing

Images and releases come from `.github/workflows/release.yml`, triggered by a version tag.

## What a release publishes

- **Images** `ghcr.io/cairn-db/cairn:<version>` and `:<major>.<minor>`:
  - for linux/amd64 and linux/arm64, cross-compiled natively (no emulated rustc);
  - with an SBOM and build provenance attached;
  - signed keylessly with cosign (Sigstore, through the workflow's GitHub identity).
- **A GitHub release** whose notes are the version's section of `CHANGELOG.md`. Versions below
  1.0 are marked as pre-releases.
- `ghcr.io/cairn-db/cairn:edge` is published separately, on every push to `main`
  (`image.yml`, amd64 only). It is not a release.

## Gates

The `verify` job must pass before anything is published:
- the tag matches `version` in the workspace `Cargo.toml`;
- `CHANGELOG.md` has a `## [x.y.z]` section;
- fmt, clippy and every test pass;
- a 3,000-seed simulation campaign finds no violation.

## Steps

1. Rehearse: run the `release` workflow by hand (Actions → release → Run workflow). It verifies
   and builds both architectures without pushing or releasing.
2. Set `version = "x.y.z"` in the workspace `Cargo.toml` and run `cargo check`, which updates
   `Cargo.lock`.
3. In `CHANGELOG.md`, rename `## [Unreleased]` to `## [x.y.z] - YYYY-MM-DD` and open a new,
   empty `## [Unreleased]`.
4. Commit ("Release x.y.z"), then tag and push:
   ```bash
   git tag -a vx.y.z -m "Cairn x.y.z"
   git push origin main vx.y.z
   ```
5. Once the workflow is green, check the image:
   ```bash
   docker pull ghcr.io/cairn-db/cairn:x.y.z
   cosign verify ghcr.io/cairn-db/cairn:x.y.z \
     --certificate-identity-regexp '(?i)https://github.com/cairn-db/cairn/.github/workflows/release.yml@.*' \
     --certificate-oidc-issuer https://token.actions.githubusercontent.com
   ```
6. The first time only: make the GHCR package public (package settings → visibility), once the
   repository is public.

## Packages (clients and integrations)

`.github/workflows/publish-packages.yml` publishes, from a release tag, by hand:
- to PyPI: `cairn-db-client`, `langchain-cairn` and `llama-index-vector-stores-cairn`;
- to npm: `@cairn-db/client` and `@cairn-db/langchain`.

Their versions must equal the tag's (the workflow checks).

One-time setup:
- **PyPI**: for each of the three projects, add a trusted publisher (pypi.org, Publishing →
  Add a pending publisher): owner `Cairn-DB`, repository `cairn`, workflow
  `publish-packages.yml`, environment `pypi`. Create the environment `pypi` in the
  repository settings, ideally with a required reviewer.
- **npm**: create the organization `cairn-db`, then a granular access token that can publish
  `@cairn-db/*`, stored as the repository secret `NPM_TOKEN`.

Then: Actions → publish-packages → Run workflow, with the tag (`v0.3.0`). A version cannot be
published twice on either registry: fix forward with a new version.
