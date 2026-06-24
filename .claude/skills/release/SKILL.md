---
name: release
description: Drive Teranode's release lifecycle — survey state, triage hotfix candidates, cherry-pick PRs onto release branches, cut numbered beta tags, and promote tested betas to stable patches. Use when the user types "/release", says "cut a release", "tag a beta", "hotfix triage", "what's the release status", "prepare patch release", or works on `release/v*` branches.
---

# Teranode Release Manager

Drives the release lifecycle: hotfix triage, cherry-picks, beta tags, prod-gated stable patches.

## Branch and tag model

Teranode follows trunk-based development. `main` is the trunk; all changes — internal and external — land there first via pull request. External contributors work from forks per the [Fork and Pull Request Guidelines](https://bsv-blockchain.github.io/teranode/howto/forkAndPullRequestGuidelines/).

- `main` — development tip. Every PR lands here first.
- `release/vX.Y` — long-lived release branch, cut from `main` at minor bumps. Hotfixes arrive as squash-merge cherry-picks.

Tag scheme:
- Beta: `vX.Y.Z-beta-N` (e.g. `v0.15.0-beta-1`). Tagged on `release/vX.Y`. Immutable. The `release/vX.Y` branch is cut from `main` at the start of the minor's beta phase so all betas of that minor live on the same branch.
- Stable: `vX.Y.Z`. Targets the SHA of the validated beta (never the branch tip — a hotfix landing after soak would otherwise get silently promoted).

Canonical state source: **GitHub Releases page**. Don't maintain a separate doc — the `status` mode below renders it live.

## Modes

Invoke via `/release <mode> [args]`. Modes:

### `status` — survey the release landscape

Run in parallel:

```bash
gh release list --limit 15
git branch -r | grep -E 'origin/release/v[0-9]'
git tag -l 'v*' | sort -V | tail -20
```

For each active `release/v*` branch, report:
- Tip SHA and headline
- Latest stable tag on that branch
- Latest beta tag (if any) and how many commits behind tip
- Count of commits on tip not yet tagged

Also:
- Top of `main` — when was the last release branch cut from it? `git log --oneline <release/vX.Y-base>..origin/main | wc -l` for "ahead of branch" volume.
- Surface open `fix(...)` PRs on `main` as potential candidates (non-exhaustive — full classification in `triage` mode).

### `triage [release-branch]` — classify backport candidates

Walk `git log <release-branch>..origin/main --first-parent`. Pull each commit's PR (via `(#NNN)` in title or `gh api`). Classify:

**Pick** (small, defensive, surgical, real-world bug):
- Defensive guards / bounds checks
- Silent-failure error surfacing (errors previously swallowed)
- Consensus-correctness fixes (audit-flagged)
- Targeted operational fixes with explicit prod evidence in the PR body

**Hold** (high blast radius, no urgent payload):
- Performance rewrites
- Concurrency model changes (worker pools, channel restructures)
- Test-only PRs (skip unless paired with a fix)

**Caution** (judgment call — ask user):
- State-machine fixes (FSM transitions, catchup logic, reorg handling)
- Network-protocol behavior changes

For each candidate report: PR #, title, author, risk tier, and whether prerequisites likely exist (other main commits touching the same files). Recommend a pick set; don't act without confirmation.

### `hotfix <PR>` — cherry-pick a single PR

Steps:

1. Verify the active branch is the intended `release/vX.Y`.
2. `gh pr view <PR> --json number,state,baseRefName,mergeCommit,commits` — capture title, merge SHA, commit count.
3. Confirm squash-merge shape: `git log --format="%H %P" -1 <merge-sha>` must show a **single** parent. If multiple parents, it's a true merge commit — ask user how to proceed.
4. **Apply**: `git cherry-pick <merge-sha>`. Plain form commits automatically; the squash-merge title (with `(#NNN)` suffix) is preserved.
5. **No conflicts** → `git push origin release/vX.Y`.
6. **Conflicts** → never auto-resolve (repo rule). Either:
   - **Anchor-drift type** (HEAD-side empty in `<<<<<<<` blocks, new code wrapped in markers): identify prerequisite PRs via `git log <release-branch>..origin/main -- <conflicting-file>`. Each listed commit is a candidate prerequisite. Show them, recommend layering the standalone-backport-worthy ones first.
   - **Real content overlap**: show conflict hunks, propose strategy, wait for user OK.
   - Abort cleanly: `git cherry-pick --abort`.
7. **Probe-only variant**: when uncertain, `git cherry-pick -n <merge-sha>` dry-runs without committing.
   - **Precondition: working tree must be clean.** Verify with `git status --porcelain` returning empty. If dirty, stash user changes first (`git stash push -m user-state-before-probe`) and pop them back **after** the probe is done. Never run a blanket discard while user state is mixed in.
   - Cleanup after a clean-precondition probe: `git reset HEAD && git stash push --include-untracked -m probe-discard && git stash drop` (the `-n` variant doesn't leave a cherry-pick session for `--abort`).
   - Avoid `git reset --hard`, `git checkout HEAD -- <path>`, and `git clean -f` (per `.claude/rules/git-workflow.md`). Developer-local safety tooling (e.g. dcg, pre-commit guards) may also block these — fall back to stash-based recovery.

### `beta <X.Y.Z>` — cut a numbered beta tag

1. Determine next beta number: `git tag -l "v<X.Y.Z>-beta-*" | sort -V | tail -1`, then bump. First beta of a new minor uses `-beta-1`.
2. Confirm `release/vX.Y` tip is what should be tagged (`git log --oneline -3`).
3. Build release notes from `git log --pretty='* %s by @<author>' <prev-tag>..HEAD` — manual format since cherry-picks lose PR auto-association.
4. `gh release create vX.Y.Z-beta-N --target release/vX.Y --title vX.Y.Z-beta-N --notes-file <path> --prerelease`.
5. Return tag URL.

### `stable <X.Y.Z>` — promote to stable patch (PROD GATE)

**Mandatory production gate.** Ask the user, one question at a time if needed:

1. **Beta candidate** — which `vX.Y.Z-beta-N` is the tested baseline? Get the exact tag.
2. **Mainnet beta peers** — which peers are running this beta? For ≥ 24 hours? Following chaintip (not stalled, not bouncing FSM states)?
3. **Testnet** — at least one node running this beta for ≥ 24 hours, following chaintip?
4. **TeraTestNet** — at least one node running this beta for ≥ 24 hours, following chaintip?
5. **Sync-from-zero** — verified on at least one node with this beta?
6. **Upgrade/restart during sync** — verified that nodes mid-IBD survive an upgrade or restart on this beta?
7. **Open issues** — any open critical bug tickets / Slack threads against this candidate?

**If ANY answer is "no", "not yet", "not sure", or "skip": refuse to tag.** Tell the user exactly which gate failed and what evidence is needed. Suggest waiting, running the missing test, or downgrading the request to another beta cut.

If all pass:
1. **Resolve the beta SHA**: `git rev-parse vX.Y.Z-beta-N`. This is what gets tagged — never the branch tip.
2. **Verify nothing has landed on top of the beta**: `git rev-parse release/vX.Y` must equal the beta SHA. If they differ, **refuse to tag**: a hotfix landed after the beta soak and that commit hasn't been validated. Tell the user; require a fresh beta cut.
3. Build release notes from the range `<prev-stable-tag>..<beta-tag>` (never `..HEAD`).
4. `gh release create vX.Y.Z --target <beta-sha> --title vX.Y.Z --notes-file <path>`. Explicit SHA target — bypasses branch-tip resolution entirely.
5. Return tag URL.
6. Offer to draft an Announcements discussion post (see "Discussion announcement" section below). Don't post automatically — show the draft, let user edit, confirm before publishing.

### `minor` — propose a minor version bump

Trigger a conversation, don't act unilaterally:
- "main has accumulated N commits since `release/vX.Y` was cut on <date>. Notable changes: <top 3-5 features/breaking>."
- "Cut a new `release/vX.(Y+1)`?"

If yes, discover which remote tracks the canonical main:
```bash
# Most clones of this repo only have `origin` pointing at bsv-blockchain/teranode.
# Forked workflows have `upstream` for the canonical and `origin` for the fork.
# Pick whichever is configured; verify with `git remote -v` first.
REMOTE=$(git remote | grep -E '^upstream$' || echo origin)
git checkout main && git fetch "$REMOTE" main && git rebase "$REMOTE/main"
git checkout -b release/vX.(Y+1)
git push -u origin release/vX.(Y+1)
```
Then suggest `release beta vX.(Y+1).0` for the first beta cut.

## Release notes format

GitHub's "Generate release notes" emits an empty body for release branches built via cherry-picks (the cherry-pick commits have no PR association on this branch). Always write notes manually:

```markdown
## What's Changed

* <commit headline> by @<author> in https://github.com/bsv-blockchain/teranode/pull/<PR>
* ...

**Full Changelog**: https://github.com/bsv-blockchain/teranode/compare/<prev-tag>...<new-tag>
```

- Pull commit headlines from `git log --pretty='%s' <prev-tag>..<new-tag>`.
- PR numbers come from the `(#NNN)` suffix already present in cherry-pick titles.
- Author: `gh pr view <NNN> --json author -q .author.login`.

## Discussion announcement

After publishing a stable tag, offer to draft an Announcements discussion post at https://github.com/bsv-blockchain/teranode/discussions/categories/announcements. **Never post automatically.** Show the draft, let user edit, confirm before publishing.

Use `gh api graphql` to create the discussion if the user approves. The category ID for Announcements can be discovered via `gh api graphql -f query='{ repository(owner: "bsv-blockchain", name: "teranode") { discussionCategories(first: 10) { nodes { id name } } } }'`.

### Template

```markdown
# vX.Y.Z — <one-line theme>

**Released:** <YYYY-MM-DD>
**GitHub:** https://github.com/bsv-blockchain/teranode/releases/tag/vX.Y.Z
**Container:** ghcr.io/bsv-blockchain/teranode:vX.Y.Z

## Upgrade priority
<Critical | Recommended | Optional> — <one-sentence reason>.

## Highlights
- <Top change> (#PR)
- <Top change> (#PR)
- <Top change> (#PR)

## Known issues
<None | bulleted list with workarounds>

[Full changelog](https://github.com/bsv-blockchain/teranode/releases/tag/vX.Y.Z)
```

### Drafting rules

- **Patch release (small set)**: list each PR individually under Highlights.
- **Minor / major release (many PRs)**: group by theme (Performance, Consensus correctness, Stability, Features, etc.) — never a flat 50-bullet list. Cap at ~5-7 PRs per theme; pick the user-visible ones, drop test-only / docs / refactors.
- **Upgrade priority**:
  - *Critical* — fixes a known prod incident class or consensus correctness issue
  - *Recommended* — meaningful operational fixes, no known prod impact
  - *Optional* — perf or features only
- **No node naming.** "mainnet/testnet/teratestnet" is fine; specific hostnames leak fleet topology.
- **Theme line in title** is a tight summary — "operational fixes", "UTXO conflict-state hardening", "new minor: performance + consensus", etc. Avoid generic "bug fixes".

## Mechanical constraints

These apply to every git operation in this skill:

- **NEVER** `git reset --hard`, `git checkout HEAD -- <path>`, `git restore .`, or `git clean -f` — per `.claude/rules/git-workflow.md`. Developer-local safety tooling (e.g. dcg) may also block them. Alternatives:
  - Rewind a branch: switch off it first, then `git branch -f <branch> <ref>`, then switch back.
  - Discard dirty state: `git stash push --include-untracked -m discard && git stash drop` (only when working tree changes are known-safe to lose).
  - Restore one file: `git stash` first, then `git checkout <ref> -- <file>` (safe when WT is clean for that path).
- **NEVER** auto-resolve merge conflicts (per repo rule). Show hunks, propose strategy, wait.
- `gh pr edit --body` / `--title` silently fails on bsv-blockchain repos (GraphQL projects-classic deprecation). Use REST: `gh api -X PATCH repos/bsv-blockchain/teranode/pulls/<n> -f body=...`. Verify with `gh pr view <n> --json body -q .body | head -3` after every edit.
- Force-push to release branches: prefer `--force-with-lease=<branch>:<expected-sha>` (explicit anchor) over bare `--force`. Bare `--force-with-lease` weakens if you fetched right before pushing.
- Cherry-pick a squash-merge: plain `git cherry-pick <sha>` works (single parent → no `-m`).
- Cherry-pick a true merge commit: `git cherry-pick -m 1 <sha>` to pick the mainline-side change. Usually means the PR wasn't squash-merged — ask user before applying.

## Risk classification reference

| Class | Examples | Backport? |
|-------|----------|-----------|
| Defensive guard | bounds-check, nil-check, panic-recover | Yes — small, surgical |
| Silent-failure surface | error returned instead of swallowed | Yes — operational bug |
| Consensus correctness | duplicate-tx detection, nBits validation | Yes — audit-flagged |
| Targeted ops fix | resource leak, queue race, FSM-RUN gate | Yes — if reproduced in prod |
| State machine fix | catchup correction, reorg recovery | Caution — review test coverage + prod evidence |
| Concurrency rewrite | unbounded goroutine → worker pool | Hold — soak on main first |
| Performance rework | cache refactor, query optimization | Hold — no fix payload |
| Test-only | regression test, property test | Skip — CI weight, no field value |

## Common operations cookbook

### Find prerequisite PRs when an anchor-drift conflict hits

```bash
git log <release-branch>..origin/main -- <conflicting-file>
```

Each listed commit is a candidate prerequisite. Check whether it's:
- Standalone-backport-worthy (small fix, clear bug) → cherry-pick first
- A drive-by refactor → user judgment

After applying prereqs, re-run the target cherry-pick. Usually applies clean.

### Clean up after a cherry-pick probe

```bash
git reset HEAD
git stash push --include-untracked -m discard-probe
git stash drop
```

### Map a commit to its PR

The squash-merge title usually carries `(#NNN)`. If not:

```bash
gh api repos/bsv-blockchain/teranode/commits/<sha>/pulls \
  --jq '.[] | "#\(.number) \(.title)"'
```

### Inspect what's between two tags

```bash
git log --oneline <prev-tag>..<new-tag>
git diff --stat <prev-tag>..<new-tag>
```

## End-of-cycle

After tagging a stable patch:
1. Verify release URL (`gh release view <tag>`).
2. Ask user if any deployment / monitoring follow-up is needed (e.g. update fleet, watch Coralogix for the next 30 min).
3. Offer to run `triage` for the next round on the same release branch.

Never proactively close issues, send Slack notifications, or mutate external systems without explicit user authorization.
