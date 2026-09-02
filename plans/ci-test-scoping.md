# CI Test Scoping — "run only what's needed"

Status: **implemented locally, pending live-CI validation** · Branch: `ci-test-optimize` · Date: 2026-06-15

Implemented: P1 tool (+tests, verified against real graph), P2 composite action,
P3 Makefile SEQ_PKGS, P4 sequential `--packages` filter, P5 pr_tests wiring,
P6 pr_smoketests wiring, P7 sequential safety-net on merge-to-main. Not yet run
in live CI (needs a PR push to exercise the `scope` job end to end).

## Goal

Stop running the full test surface on every PR. Detect what a PR actually changes
and run only the tests that change can affect — across the unit, smoketest, and
sequentialtest suites — without ever letting a real break reach `main` undetected.

## Decisions (locked)

| Decision | Choice |
|---|---|
| Change→test mapping | **Reverse-dependency closure** from the Go import graph |
| Suites in scope | Unit (`make test`) + smoketest + sequentialtest (longtest untouched) |
| Safety net | **Full suite on merge to `main`** is the real gate; PR runs are an optimization |
| Manual override | **`ci-full` PR label** forces the complete suite |

## Concrete Rules

**Definitions**
- *Changed files* = `git diff --name-only <merge-base(main,HEAD)>...HEAD` (PR) / push diff (merge).
- *Changed package* = any Go package dir containing a changed file (`.go` or embedded asset/testdata).
- *Affected set* = reverse-dependency closure: every package that *is* a changed
  package or *transitively imports* one, from `go list -deps` over the whole module
  (test/ packages are graph nodes too).

**Tier 0 — Global inputs → run FULL suite (unit + smoke + sequential)**
Import graph can't capture the blast radius of these:
`go.mod`, `go.sum`, `Makefile`, `.github/workflows/**`, `.github/actions/**`,
`settings/` + `settings*.conf`, `.golangci.{yml,yaml,json,toml}`, any `Dockerfile*`
(root image + `test/utils` helper images), the `test/docker-compose*` and
`compose/docker-compose*` stacks, the scoping tool/scripts themselves. Also forced
by the `ci-full` label and by push to `main`/`staging`/`release`.

**Tier 1 — No Go and no test-relevant files changed → run NOTHING.**
Docs, markdown, unrelated assets.

**Tier 2 — Otherwise → compute affected set, scope each suite to its slice**
1. Unit (`make test`): affected ∩ non-`test/` packages, no coverage instrumentation.
2. smoketest: run iff affected closure includes `test/e2e/daemon/ready` (per-package granularity; that suite is effectively one package).
3. sequentialtest: affected ∩ `test/sequentialtest/...` → pass that subset to the runner; if empty, skip.

**Build-tag rule (correctness):** compute the closure with the **union of all CI
build tags** (`testtxmetacache`, `aerospike`, `native`, `memory`, `postgres`,
`sqlite`, + smoketest tags). Over-including tags enlarges the closure (safe,
never scopes out); a missing tag hides import edges (unsafe). Union = safe.

## Worked examples

- **Change `services/blockchain/*.go`** → closure = blockchain + importers
  (validator, blockassembly, blockvalidation, …) + any e2e/seq package that boots a
  node importing blockchain. `p2p`, `asset`, unrelated stores do not run.
- **Change only `test/sequentialtest/double_spend/foo_test.go`** → nothing imports a
  test package, so closure = just that package → run only that one sequential test;
  unit + smoke run nothing.

## Implementation Plan

### P1 — Affected-packages tool  `test/scripts/affected/main.go` (+ `_test.go`)
- Inputs: changed-file list (stdin), `-tags` (union), repo root.
- Runs `go list -deps -json -tags <union> ./...`; builds {ImportPath, Dir, Deps}.
- Maps changed files → changed packages (longest Dir prefix; non-Go files count).
- Computes reverse-dep closure.
- Emits JSON: `{full, reason, run_unit, unit_pkgs[], run_smoke, run_seq, seq_pkgs[]}`.
- Tier 0 / Tier 1 classification from the raw file list before/around the graph.
- Unit-tested with fixture file lists + a fixture graph (test-first per AGENTS.md).

### P2 — Shared composite action  `.github/actions/test-scope/action.yml`
- Steps: checkout, setup Go, restore Go cache, compute changed files vs base,
  run the tool, expose outputs. Honors `ci-full` label and push-to-main → full.
- Reused by a `scope` job in each PR workflow (maps step outputs → job outputs).

### P3 — Makefile
- Unit: `TEST_PKGS` path already exists (keep; source from tool).
- sequentialtest: thread `SEQ_PKGS` into `run_tests_sequentially.sh`.

### P4 — `test/scripts/run_tests_sequentially.sh`
- Add `--packages "dir1 dir2"` (or `SEQ_PKGS` env) to restrict discovery to the
  affected sequential package dirs; sharding/`--db` keep working on the filtered list.

### P5 — Wire `teranode_pr_tests.yaml`
- Replace the current dorny-based `changes` job with a `scope` job (P2 action).
- `golangci-lint` and `test` are **not** scope-gated: the downstream Sonar
  pipeline (`sonar-inputs` -> `sonar-pr-analyze.yaml`) hard-requires
  `golangci-lint-report.xml` and `coverage.out`, and a missing input fails the
  required "SonarQube Quality Gate" check. Both jobs always run and always
  upload; `scope` only decides WHAT `test` runs (`unit_pkgs` via `TEST_PKGS`).
- Coverage is therefore produced on every run, including scoped ones, with
  `-coverpkg` narrowed to the scoped set (the closure contains every changed
  production package, so Sonar's new-code coverage is unaffected). Instrumenting
  `./...` is what drives the ~110GB full-run peak, not writing a profile.
- `any_go` is retained as a diagnostic in the scope-decision log only.

### P6 — Wire `teranode_pr_smoketests.yaml`
- Add `scope` job; gate `smoketest` on `run_smoke`, `sequential` on `run_seq`
  (pass `seq_pkgs`). prunertest / legacy-sync / chainintegrity: **run iff the
  closure touches their package**, else skip on PRs; always run on merge-to-main
  and `ci-full`. (Resolved.)
- Every gated job's `if:` carries a status function and a
  `needs.scope.result != 'success'` clause. This is load-bearing: without it a
  `scope` job that dies at the runner/step level - checkout, setup-go, go-cache,
  i.e. before the action can fail open - leaves `$GITHUB_OUTPUT` unwritten, every
  gate evaluates `'' == 'true'` false, and the jobs are **skipped rather than
  failed**. A skipped required check satisfies branch protection, so the failure
  mode is a green PR with zero tests run.

### P7 — Safety net
- Confirm push→`main` runs full unit+smoke (it does via `teranode_main_tests.yaml`).
- **Add `make sequentialtest` to `teranode_main_tests.yaml`** to close the gap, so
  the merge gate truly covers all three scoped suites. (Done: sharded across 7,
  no `SEQ_PKGS`, and **not** `continue-on-error` - an advisory job leaves the
  workflow green and so catches nothing.)
- **Add `legacy-sync` and `chainintegrity` to `teranode_main_tests.yaml`.** These
  were the two PR-gated suites with no merge-to-main gate at all: `make smoketest`
  `-skip`s every `TestLegacySync*`, `teranode_main_build.yaml` has chainintegrity
  commented out, and nightly runs the separate binary-based `chainintegrity.run`
  rather than the `test/e2e/chainintegrity` go-test suite. Without them a closure
  mis-scope for either suite would never be caught anywhere. (Done.)

### P8 — Migration / rollout
- Land P1–P4 (tool + scripts) with unit tests first; verify the tool locally against
  several real diffs (`git diff` of recent PRs) before wiring workflows.
- Wire workflows behind the safety net so a mis-scope is caught on `main`.

## Risks
- **Build-tag gaps** → mitigated by union tags + full-on-merge safety net.
- **`go list` needs module cache** in the scope job → cache restore; ~1.3s graph build once warm.
- **e2e per-package granularity** is coarse (run-or-skip), not per-test — acceptable.
- **Generated/embedded inputs** (proto, testdata) → treated as changing their
  package. `//go:embed` assets are mapped via `go list`'s `EmbedFiles` /
  `TestEmbedFiles` / `XTestEmbedFiles` rather than by directory prefix, so a
  root-package embed (which owns no subtree to prefix-match) still maps correctly.
- **Runtime inputs no import edge can express** are Tier-0 global inputs: all of
  `test/scripts/` (the smoke runner shells out to `gotestsum_with_retry.sh` and
  `list_test_shard.sh`), and all non-Go files under `compose/` (aerospike configs
  mounted as `/etc/aerospike.conf`, `compose/scripts/` helpers, and the stack
  definitions). Go sources under `compose/` stay scoped - those the graph does see.
- prunertest/legacy-sync/chainintegrity gating policy resolved (P6/P7): PR-gated
  on the closure, with a real merge-to-main gate for each.
