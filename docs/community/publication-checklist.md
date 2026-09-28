# Publication checklist (for the owner's review)

Prepared locally on 2026-09-28. **Nothing has been published.** Each step below needs the
owner's explicit go.

## Audit done

- **Secrets.** Private keys, AWS/GitHub/Google/Slack/OpenAI-style tokens and password
  assignments were searched in the tree and in the whole history (`git log --all -p`). None
  found. Test certificates are generated at run time (`tools/scripts/gen-certs.sh`, `rcgen`);
  none are committed.
- **Names.** No personal e-mail, no cloud project id, no name of unrelated projects or
  services, in the tree or the history. The history was rewritten on 2026-09-25 to the GitHub
  noreply address.
- **Public IP addresses.** None, only private ranges (10.77.0.0/24, 127.0.0.1).
- **Size.** The `.git` pack is 623 KiB and the largest tracked file is 132 KiB.
  `/data` (datasets, logs, local scripts) and `/target` are gitignored.
- **License.** Apache-2.0 (`LICENSE`). Dependencies are permissive; mimalloc is MIT.

## Decisions for the owner

1. **Internal files.** Three files are not secret but do reflect how the project was built.
   Keep, trim or remove:
   - `CLAUDE.md`: the agent's working rules, including the "autonomous delivery mode" section;
   - `prompts/`: the kickoff prompt;
   - `docs/progress.md`: the full journal, with cloud costs.

   Decided (2026-09-28): keep all three, with CLAUDE.md reduced to what explains the context,
   and personal details removed.
2. **Visibility.** Publish the repository as private first, to review it on GitHub, then
   switch it to public.
3. **Repository name.** `cairn-db/cairn`. The issue templates and the organization README link
   to it.

## Steps, once approved

```bash
# 1. Organization profile
gh api -X PATCH orgs/cairn-db -f description="Hybrid search database where a deletion is final" \
  -f blog="https://cairn-db.com"          # only if the site exists
gh repo create cairn-db/.github --public --description "Organization profile"
#    then push docs/community/org-profile/README.md as profile/README.md in that repository

# 2. Main repository (private first)
gh repo create cairn-db/cairn --private --description "Distributed hybrid search (vector + text + filters) with a deletion guarantee. Rust, Raft."
git remote add origin https://github.com/cairn-db/cairn.git
git push -u origin main
gh repo edit cairn-db/cairn --enable-discussions --add-topic rust,database,vector-search,hybrid-search,raft,rag,full-text-search,data-governance
#    Settings: enable private vulnerability reporting (Security tab), protect main (PR + CI)

# 3. Labels (docs/community/labels.md), e.g.
gh label create deletion-guarantee --color b60205 --description "A takedown not honoured" -R cairn-db/cairn

# 4. First issues (docs/community/first-issues.md), one gh issue create per section

# 5. When ready: gh repo edit cairn-db/cairn --visibility public --accept-visibility-change-consequences
```

## After publication

- CI (`.github/workflows/ci.yml`) and the image workflow (`image.yml`, to GHCR) run on the
  first push. Check them and that the image appears at `ghcr.io/cairn-db/cairn`.
- The issue templates point to Discussions and to private vulnerability reporting: both must
  be enabled.
