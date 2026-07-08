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

Test tags: `testtxmetacache` (small cache for testing), `largetxmetacache`
(production cache size), `aerospike` (tests requiring Aerospike).

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

## Security Rules

- Never commit secrets, or log them.
- No unsafe execution (`eval`, shell injection, unsanitised exec) or injection risks
  (SQL, command, template, XSS).
- No insecure file handling (path traversal, unsafe permissions, untrusted
  deserialisation).

## Specialized Agents

Domain-specific agents in `.claude/agents/`, invoked via the Agent tool:

- **bitcoin-expert** — BSV protocol, consensus, cryptography, BSV-specific features
- **test-writer-fixer** — writes/fixes tests after code changes
- **api-tester** — API load testing and contract validation
- **backend-architect** — system design and architecture decisions
- **document-reviewer** — documentation quality and accuracy

Vendored from [VoltAgent/awesome-claude-code-subagents](https://github.com/VoltAgent/awesome-claude-code-subagents):
**golang-pro** (concurrency, perf, idiomatic Go), **code-reviewer** (language-agnostic
review), **security-auditor** (consensus/UTXO/crypto), **qa-expert** (coverage,
race/fuzz, CI gates), **performance-engineer** (hot-path, locks, throughput),
**typescript-pro**, **frontend-developer** (`ui/dashboard` Svelte), **security-engineer**
(infra/CI/supply-chain), **penetration-tester** (DoS vectors, panic chains).
