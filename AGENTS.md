# AGENTS.md

Guardrails and project context for AI-assisted development, read by all AI tools
(Claude Code via `CLAUDE.md` → `@AGENTS.md`, others directly). Keep it lean and
command-first — it loads on every request.

## Project

Teranode: horizontally scalable BSV Blockchain node. Microservices in `services/`,
pluggable stores in `stores/`, Svelte dashboard in `ui/dashboard/`. Config:
`settings.conf` (defaults, committed), `settings_local.conf` (local, not committed).

## Commands

```bash
make build          # Binary with dashboard
make test           # Unit tests (no integration)
make smoketest      # E2E smoke tests
make sequentialtest # Order-dependent tests
make testall        # Everything
make fuzz           # Fuzz the parsers locally (FUZZTIME=30s default)
make lint           # Changed files vs main
make dev            # Dev mode with dashboard
make gen            # Regenerate protobuf Go code
gci write --skip-generated -s standard -s default <file>  # Fix import ordering lint
```

## Architecture

- **Core pipeline**: Propagation → Validator → Block Assembly → Block Validation →
  Block Persister → Blockchain (state/FSM).
- **Communication**: gRPC (sync), Kafka (async), HTTP/WebSocket (external), UDP
  multicast (high-perf tx propagation).
- **Key patterns**: horizontal scaling, event-driven, UTXO model, Merkle trees,
  two-phase commit.
- **Port config**: `settings.conf` lines 88-140.

Detailed docs (read on demand): [`teranodeIntro.md`](docs/topics/teranodeIntro.md),
[`teranode-microservices-overview.md`](docs/topics/architecture/teranode-microservices-overview.md),
[`teranode-overall-system-design.md`](docs/topics/architecture/teranode-overall-system-design.md).

## Git Workflow

All developers work in forks with `upstream` pointing to the original repo.

```bash
# Sync with upstream
git fetch upstream && git rebase upstream/main

# New branch (always from synced main)
git checkout main && git fetch upstream && git rebase upstream/main && git checkout -b <branch>

# Push (if conflicts: STOP and ask)
git fetch upstream && git rebase upstream/main && git push origin <branch>
```

- **NEVER `git reset --hard`** — it destroys uncommitted work. Use `git stash`.
- **NEVER auto-resolve merge conflicts** — show the conflicting files and wait for
  approval on the resolution strategy.

## Reviewing a Team PR

Full process: [`docs/references/reviewProcess.md`](docs/references/reviewProcess.md). Agent steps: the `review-fix-forward` skill in `.claude/skills/`.

- P0: request changes. The assignee fixes it. Never fix a P0 yourself.
- P1–P3: finish the review first. Then fix them in separate `review-fix:` commits.
- Before fixing, read the PR comments for an open `Fixing review findings:` claim. If one is open, do not push fixes.
- If no claim is open, post a claim comment before the first fix.
- Push to the PR branch only when the user asks in this session. Fast-forward only.
- Fix only what the review found. Never rebase, force-push, or resolve conflicts.
- Post one hand-back comment: findings, fixing commits, open items, questions.
- In that comment, advise the assignee whether each existing approver should re-review. Criteria are in the process file. Do not ask the approver yourself.
- The assignee still owns the PR. The other reviewer approves the final head commit.

## Go Conventions

- No `Get` prefix on getters: `Name()`, not `GetName()`.
- Log messages: always a single line.
- Full reference: [`docs/references/codingConventions.md`](docs/references/codingConventions.md).

## Testing

- Don't mock the blockchain client/store — use the `sqlitememory` store.
- Don't mock Kafka — use `in_memory_kafka.go`.
- Use `require` from testify, not `assert`.
- Avoid `t.Parallel()` unless the test is specifically exercising concurrency.
- Use TestContainers for integration tests needing external services (Aerospike,
  PostgreSQL).

Test file layout:

- A test for code in `foo.go` goes in `foo_test.go`, including a test for a new
  behaviour or a bug fix. Create a new `_test.go` file only alongside a new
  production file.
- There are two exceptions. A test that needs its own build tag gets its own file,
  because Go applies build tags per file; name it for the tag
  (`foo_integration_test.go`). A package's shared test code gets its own file too:
  `main_test.go` for `TestMain`, `helpers_test.go` for the package fixture,
  `export_test.go`. Benchmarks also go in `foo_test.go`.
- Never name a test file by how it was written: no `_additional`, `_coverage`,
  `_extra`, `_more`, `_new` or numeric suffixes.
- Existing split files stay as they are. A PR that adds tests to code with several
  test files adds them to `foo_test.go`, or to the existing file that holds the
  closest test.
- Keep unit tests in the same directory as the code; don't move them to a
  separate tree. The top-level `test/` tree holds the e2e, multi-node,
  consensus-vector, long-running, sequential, chaos and soak suites.
- Search production code first and open tests only when needed. Most hits for a
  common identifier are in tests: `git grep <name> -- ':!*_test.go'`.
- New code still meets the 80% coverage gate. The rules below change only how
  coverage is reached: an assertion or a table row in an existing test comes first.
- Add to an existing test, not only an existing file. When a test already reaches
  the code, add an assertion there. When only the inputs or the entry point differ
  (a sync and an async version, two handlers with the same failure), add a row to
  that test's table. Write a new Test function only for a genuinely new setup.
- When a PR adds a second test whose setup repeats an existing one, or a third case
  that differs only in inputs, turn them into a table or move the shared setup into
  the package's fixture in the same PR. A package with no table or fixture gets one
  the first time a PR adds tests there.
- Where a shared fixture uses a mock, it registers every expectation a test relies
  on as required, never `.Maybe()`, so a call the code stops making still fails.
- A store test that must pass on SQLite and PostgreSQL is one test body over a
  backend loop, with a per-case timeout so a hang fails by name. No `_SQLite` and
  `_Postgres` copies. Start PostgreSQL with `test/utils/postgres`, not
  testcontainers' `postgres.Run`, except in a test of server configuration that
  needs its own container settings.
- A comment above a test is at most three lines: the behaviour it pins and why it
  matters. Incident history, mutation proofs and review discussion go in the PR
  body. Never put an issue number from a private tracker in a code comment.

Test tags: `testtxmetacache` (small cache for testing), `largetxmetacache`
(production cache size), `aerospike` (gates one native-ops integration test; the
Aerospike store itself is always compiled and its container-backed tests run untagged,
skipping when Docker is unavailable — reconciliation notes section 72).

```bash
make smoketest TEST_RETRY_COUNT=3                       # retry (smoke/sequential only)
make sequentialtest TEST_RETRY_COUNT=5 TEST_RETRY_DELAY=3
go test -v -race -tags "testtxmetacache" -run TestName ./path/to/package  # single test
```

## Service Interfaces

- `Interface.go` uses native Go types only — no protobuf types in signatures. Return
  simple types: `error`, `bool`, `[]string`, domain structs.
- `Client.go` keeps protobuf/gRPC imports internal; public methods match the
  interface, converting via internal helpers.
- Reference implementation: `services/p2p/`.

## GitHub `#` References

GitHub auto-links `#<number>` to issues/PRs wherever it renders text: PR/issue
titles, descriptions, comments, reviews, and commit messages. Writing `#1`, `#2` as
list markers or rankings creates bogus cross-references that spam unrelated issues'
timelines.

- Write `#123` only when you intend to reference issue/PR 123.
- For lists and rankings use Markdown ordered lists (`1.`, `2.`) — never `#1`, `#2`.
- To show a literal `#123` without linking, insert a word joiner between `#` and the
  number: `#&NoBreak;123` — renders as "#123", no link, no timeline spam.
- Commit messages don't render HTML entities, so the `&NoBreak;` trick fails there —
  just avoid bare `#<number>` unless you mean the reference.

## Verification

Run the relevant set before claiming success and loop until green — never claim
"should work" without re-running.

```bash
# Go
go test ./... && go test -race ./... && go vet ./...
golangci-lint run && staticcheck ./... && govulncheck ./... && gosec ./...

# Frontend (ui/dashboard)
npm test && npm run lint && npm run check && npm run build
```

## Engineering Rules

- Correctness, safety, maintainability, clarity, then speed — in that order. Treat AI
  output as untrusted until verified.
- Prefer minimal diffs. No large rewrites or unrelated refactoring bundled into a
  change.
- Write or adjust tests with the change; where practical, write the failing test
  first.
- Don't assume — if a fact is unverified, check it or say so. Surface tradeoffs
  instead of burying them.
- Self-review before reporting (logic, edge cases, races, security, side effects),
  and report concretely: what changed, what was run and its result, residual risk.
- A breaking change gets a `## Breaking` section in the PR description, and the `breaking`
  label if it exists. The release skill collects these sections into the release notes.
  Breaking means an operator or integrator must act before or during
  the upgrade, or something that worked stops working without their action: a removed,
  renamed or newly required setting, a change to what an existing setting does, or a
  startup failure on previously valid config; access a deployment didn't need before
  (credentials, Kafka ACLs); a wire, proto, gRPC or HTTP API incompatibility for
  external clients, error and status codes included; a consensus change, or any other
  upgrade-order or no-downgrade constraint, between services that can be upgraded
  separately too; a removed, renamed or redefined metric or label value; a store or
  interface change that breaks out-of-tree implementations. A bug fix that does any of
  these still counts. No section for other bug fixes (even observable ones), new
  limits or defaults that don't break sane existing use, internal behaviour changes, log
  changes, error-message wording or performance changes. Section: 1–3 lines saying what
  changed, who is affected and what to do. Detail goes in the rest of the PR. Do not
  edit a shared file for this: the repo has no `CHANGELOG.md`.

## Security Rules

- Never commit secrets, or log them.
- No unsafe execution (`eval`, shell injection, unsanitised exec) or injection risks
  (SQL, command, template, XSS).
- No insecure file handling (path traversal, unsafe permissions, untrusted
  deserialisation).

## Specialized Agents

Domain-specific agents live in `.claude/agents/`, one Markdown file per agent, each
with its own description. Claude Code loads them automatically — read the directory
for the current roster rather than a list duplicated here. Some are vendored from
[VoltAgent/awesome-claude-code-subagents](https://github.com/VoltAgent/awesome-claude-code-subagents).
