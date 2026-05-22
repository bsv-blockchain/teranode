# Per-package spec generation prompt

This is the canonical brief for the Teranode per-directory spec project. A
session that opens this file and follows it should be able to pick up the
work without needing the original conversation. Calibrated against
`services/blockassembly/spec.md`, which is the reference spec.

---

## Goal

Produce a reviewable, **normative** `spec.md` for every meaningful package
directory in this repository, in the spirit of openspec.dev. The spec
defines the contracts the implementation MUST conform to. Each requirement
is independently testable and carries a stable ID.

The end state is one `spec.md` per package directory, all merged on a
feature branch, ready for review and conformance testing.

---

## Hard rules

### 1. Specify intent, not implementation

Do not paraphrase code. Define behavior the implementation must conform
to, in terms a competent developer could re-implement from. If your only
evidence for a requirement is "the code does this", that is a red flag —
find a stronger intent signal or move the doubt to Open questions.

Implementation mechanics (how something is achieved internally — locks,
channels, atomic pointers, line-numbered code references, struct shapes)
do **not** belong in normative sections. They go in **Non-Normative
Notes** at the end, as background reading for maintainers.

### 2. Anchor on intent signals first, code last

Priority order when researching a package:

1. `README.md`, `doc.go`, top-of-file package comments
2. Files named `ARCHITECTURE.md`, `DESIGN.md`, `RFC*`, `ADR*`, anything
   under `docs/` that mentions the package by name. Teranode has rich
   topic and reference docs under `docs/topics/services/` and
   `docs/references/services/` — read those.
3. Exported type and method doc comments
4. RPC `.proto` file comments (these are part of the public contract)
5. Test names and table-test descriptions (often encode intent better
   than the code under test)
6. Commit messages and PR descriptions for files added when the package
   was created (`git log --diff-filter=A -- <dir>`)
7. Names of packages, types, and key functions
8. Implementation details (last resort)

### 3. Be assertive, not hedging

The spec is the reference the code must adhere to. Make strong,
declarative statements. **Do not pile up `[INFERRED]` tags on every
claim.** The human review pass is what corrects mistakes — that is the
review's job, not the writer's job.

- Reserve `[INFERRED]` only for items with no real intent signal at
  all, and even then prefer to just state the requirement and surface
  the doubt as an Open Question.
- Do not write inline `> Reviewer: confirm or correct` notes; move that
  doubt to the Open Questions section.

### 4. Requirement IDs are mandatory

Every normative requirement MUST carry a stable ID of the form
`<PKG>-<CATEGORY>-<NNN>`, where:

- `<PKG>` is a short package abbreviation (e.g. `BA` for blockassembly,
  `VAL` for validator, `BC` for blockchain, `SS` for subtreestore).
  Pick once per package and use consistently.
- `<CATEGORY>` is a contract section name (`INGEST`, `SUBTREE`,
  `MINING`, `REORG`, `STARTUP`, `DEPENDENCY`, `CONFIG`,
  `OBSERVABILITY`, `STATE`, etc.).
- `<NNN>` is a zero-padded sequence within the category.

Example: `BA-INGEST-001`, `BA-REORG-014`, `VAL-CONFIG-003`.

**Rules for IDs:**

- IDs are **stable across edits**. Once assigned, never renumber, never
  reuse — even if the requirement is rewritten or its meaning changes
  substantially. Conformance tests reference these IDs.
- When a requirement is removed, leave the ID retired (do not reuse).
- New requirements take the next free number in their category.
- Cross-references between requirements use the IDs (e.g. "see
  BA-CANDIDATE-007").

### 5. Conformance language

Use RFC 2119 keywords consistently:

- **MUST / SHALL** — absolute requirement; non-conformance is a defect.
- **MUST NOT / SHALL NOT** — absolute prohibition.
- **SHOULD** — strong recommendation; deviation requires justification.
- **SHOULD NOT** — strong prohibition with justified-deviation possible.
- **MAY** — permitted but not required.

Each requirement should encode one obligation. Multi-clause requirements
get split into separate IDs.

### 6. Make every threshold measurable

Vague phrases like "approximately", "promptly", "fresh", "high
throughput", "persistent failure" are not testable. Replace with
explicit values, configured settings, or quantified bounds:

- Bad: "rejects with a recognizable error"
- Good: "MUST return gRPC status `UNAVAILABLE` with message
  `service not ready - unmined transactions are still being loaded`"

- Bad: "on persistent failure"
- Good: "after N consecutive failures within window W (configured via
  `<setting>`)"

When a threshold is intended to be configurable, name the setting in
the Configuration Contract and reference the setting from the
requirement; do not hardcode the value into the requirement.

### 7. Separate normative content from notes and gaps

Each spec is divided into three logical parts (in one file, separated
by section headers):

1. **Normative sections** — the contract. Numbered requirements with
   IDs. This is what the implementation must conform to.
2. **Non-Normative Notes** — algorithms, internal architecture,
   diagrams, mechanism descriptions. Useful background; not binding.
3. **Conformance Gaps** — known divergences between this spec and the
   current implementation. Each gap names the IDs it affects and
   describes the work needed.

Open Questions live alongside the Conformance Gaps section but are
distinct: gaps are "spec is right, code is wrong"; questions are "spec
is unsure, needs decision."

### 8. Mermaid diagrams where prose is dense

Diagrams are non-normative; they help the reader navigate the
normative content. Place diagrams in the relevant contract section or
in the Non-Normative Notes section.

Common diagrams:

1. **Context / collaborators** — `graph LR` showing callers and
   dependencies.
2. **Service lifecycle** — `stateDiagram-v2` for distinct operational
   phases.
3. **End-to-end happy-path flow** — `sequenceDiagram` for the primary
   request/response pattern.
4. **Decision-tree / classification flowchart** — `flowchart TD` for
   branching on input classification.

Use **Mermaid**, not PlantUML/SVG.

**Mermaid gotchas to avoid:**

- Do **not** use `;` inside message text or node labels — Mermaid treats
  `;` as a statement separator, which breaks the surrounding block.
  Use `,` or `—` instead.
- Do **not** use `(` `)` inside flowchart node labels (`[...]` or
  `{...}` shapes) — Mermaid's flowchart parser uses parens to delimit
  shape variants. Substitute em dash or comma.
- Multi-line text inside node labels and notes uses `<br/>`, not `\n`.
- Validate diagrams via <https://mermaid.live> or IDE preview before
  committing.

### 9. Do not name internal types, fields, or function signatures

Specs talk about behavior visible to callers and observable state.

**Exception:** Public API operations (gRPC RPC method names, settings
key names, exported metric names, configured environment variables)
are part of the public contract and may be named.

**Implementation identifiers** (internal Go types, channel names,
goroutine roles, struct fields) belong only in Non-Normative Notes.

### 10. No length cap

Be thorough. Length follows necessity. Some specs will be short (small
utility packages); some will run thousands of lines (complex services).
Do not pad and do not trim for length's sake.

### 11. One spec per directory that is a meaningful unit

Skip:

- `vendor/`, `.github/`, `node_modules/`, `.worktrees/`,
  `.claude/worktrees/`, `.idea/`
- `data/`, `daemon/data/`, `bins/`, `build/`, `ui/dashboard/build/`,
  `ui/dashboard/.svelte-kit/`
- Generated code directories — but if the directory has a `.proto`
  source, write a spec describing the API contract.
- Directories with no source files

For directories whose only role is to group subpackages (no own code),
still write a `spec.md`, but make it a short index that states the
grouping's purpose and lists child packages with one-line summaries.

### 12. Sub-packages do NOT get pointer sections in their parent

A parent spec does not need a "Child packages" section. Each
subdirectory has its own `spec.md`; let the reader navigate.

---

## Spec template

Use this section structure exactly. Sections may be omitted only when
they would be genuinely empty for the package in question (e.g. a pure
library package has no State Machine).

```markdown
# Spec: <package import path>

**Status:** Draft — generated, awaiting human review
**Last generated:** <YYYY-MM-DD>
**Sources consulted:** <files actually read>
**Requirement-ID prefix:** `<PKG>` (chosen abbreviation for this package)

> Optional: note any intentionally-skipped sources.

## Status and Scope

One paragraph framing what this spec covers. State that the spec
defines normative behavior visible to callers and observable state,
NOT implementation; that implementation mechanics live in
Non-Normative Notes; that known divergences live in Conformance Gaps.

## Terminology

Glossary of domain terms with package-specific meaning. One or two
sentences each.

## Normative Requirements

The contract, organized into sections by topic. Each section has its
own ID category. Requirements use RFC 2119 language and are
independently testable. Suggested categories for a service package:

- ### Transaction Ingest Contract (`<PKG>-INGEST-NNN`)
- ### Subtree Contract (`<PKG>-SUBTREE-NNN`)
- ### Mining Candidate Contract (`<PKG>-CANDIDATE-NNN`)
- ### Mining Solution Contract (`<PKG>-SOLUTION-NNN`)
- ### Chain-Tip and Reorg Contract (`<PKG>-REORG-NNN`)
- ### Startup and Recovery Contract (`<PKG>-STARTUP-NNN`)
- ### Dependency Failure Contract (`<PKG>-DEPENDENCY-NNN`)
- ### Configuration Contract (`<PKG>-CONFIG-NNN`)
- ### Observability Contract (`<PKG>-OBSERVABILITY-NNN`)

For non-service packages, choose categories that fit the package's
shape. Do not invent contract sections that don't apply.

Requirement format:

> **<PKG>-CATEGORY-001** The package SHALL <testable behavior>.
> [Optional: rationale or note in italics.]

## Public API Contract

For each public-facing operation (gRPC method, HTTP endpoint, exported
function), a contract table:

| Field | Value |
|---|---|
| **Operation** | `OperationName` |
| **Valid states** | States in which the operation is accepted |
| **Request validation** | Required input shape and bounds |
| **Success effect** | Observable change after success |
| **Error responses** | Each error condition with status code and message |
| **Idempotency** | Whether repeat invocations have additional effect |
| **Concurrency** | Behavior under concurrent calls |
| **Persistence** | What is durably written, when |
| **Requirement IDs** | List of normative IDs governing this operation |

## State Machine

If the package has distinct operational states, include:

1. A state diagram (mermaid `stateDiagram-v2`).
2. A **per-state × per-operation matrix** as a table, showing for each
   (state, operation) pair: accept / reject / queue / specific error.

## Non-Functional Requirements

Numbered requirements (`<PKG>-NFR-NNN`) for performance, concurrency,
durability, security, observability targets. Each must be measurable.

## Conformance Test Matrix

For each major requirement (or requirement cluster), one or more
acceptance scenarios in given/when/then form, referencing the IDs:

> **AC-<PKG>-INGEST-003-1.** Given the service is in `Running`,
> when an `AddTxBatchColumnar` request arrives with a txid array of
> length N and a parent-tx-offsets array of length M ≠ N+1, then the
> service MUST reject the entire batch and no transaction is enqueued.
> Tests requirements: BA-INGEST-003.

## Non-Normative Notes

Architecture overview, internal mechanics, algorithms, performance
characteristics, sub-package boundaries, lock-free data structures,
observed implementation patterns. Mark this section explicitly as
non-normative; it does not bind the implementation.

## Conformance Gaps

Known divergences between this spec and the current implementation.
Format:

> **GAP-<PKG>-001.** <Short title>
> *Affects:* `<PKG>-CATEGORY-NNN`, `<PKG>-CATEGORY-MMM`.
> <Description of the divergence.>
> *Work required:* <concrete change>.

## Open Questions

Numbered list of unresolved design questions (not gaps). The reviewer
must decide before the spec can be considered final.
```

---

## Process

### Setting up

1. Build the worklist. From the repo root:

   ```bash
   find . -type d \
     -not -path './.git*' \
     -not -path './vendor/*' \
     -not -path '*/node_modules*' \
     -not -path './.worktrees/*' \
     -not -path './.claude*' \
     -not -path './.idea*' \
     -not -path './data*' \
     -not -path './daemon/data*' \
     -not -path './bins*' \
     -not -path './build/*' \
     -not -path './ui/dashboard/build*' \
     -not -path './ui/dashboard/.svelte-kit*' \
     | sort > _spec_worklist.tmp
   ```

   Prune obvious non-package entries. Save to `_spec_worklist.md` at
   the repo root with a checkbox per directory.

2. Pick the package's ID prefix and record it in
   `_spec_worklist.md` (e.g. `services/validator → VAL`,
   `services/blockchain → BC`). Reuse across the package's spec.

3. Confirm the branch strategy. Per project convention (CLAUDE.md +
   `.claude/rules/git-workflow.md`), never commit to `main` — work on
   a feature branch.

### Per spec

1. Process top-level directories first, then descend.

2. Before writing a spec for `pkg/foo`, read in this order:
   1. `pkg/foo/README.md` if any
   2. `pkg/foo/doc.go`
   3. The package comment block at the top of every source file
   4. Any `docs/topics/.../<package>.md` and
      `docs/references/.../<package>*.md` files
   5. Any `.proto` files — read RPC and message comments thoroughly;
      every RPC needs an entry in the Public API Contract section
   6. Test function names for behavior signals
   7. Code bodies — only if needed

3. Sketch the requirement ID assignments before writing prose. Group
   by category, allocate `001..NNN` in each category. This avoids
   renumbering during drafting.

4. Write the spec to `pkg/foo/spec.md`. Tick the worklist box.

5. Commit each spec individually with a message like
   `docs(spec): services/foo`.

### Quality bar before saving

- Does every normative bullet have an ID, RFC 2119 keyword, and a
  measurable outcome?
- Does every public-facing operation have a contract table entry?
- For state-bearing packages: is there a per-state × per-operation
  matrix?
- For each requirement, is there at least one acceptance scenario in
  the Conformance Test Matrix?
- Are implementation details confined to Non-Normative Notes?
- Are known divergences in Conformance Gaps (not in Open Questions)?
- Are remaining unresolved design questions in Open Questions?
- Could a reviewer disagree with anything? (If no, the spec is too
  vague.)

---

## Working style

- Process directories one at a time and commit after each, so
  progress is visible and reversible.
- Update `_spec_worklist.md` as you go.
- Keep specs as long as they need to be, no longer.
- If two sibling packages overlap or conflict, note it in both specs'
  Open Questions.

---

## Reference spec

`services/blockassembly/spec.md` is the reference. It was used to
calibrate this template through a 30-question Q&A pass plus a
structural review. When in doubt about format or tone, look there.

The reference spec illustrates:

- Package ID prefix `BA` with categories INGEST, SUBTREE, CANDIDATE,
  SOLUTION, REORG, STARTUP, DEPENDENCY, CONFIG, OBSERVABILITY, STATE.
- Per-RPC contract tables for the 18-method gRPC surface.
- Per-state × per-operation matrix anchoring the State Machine section.
- Acceptance criteria for each major requirement.
- Three documented Conformance Gaps as work items.

---

## What is explicitly NOT in scope

- Writing or fixing implementation code (other than narrowly-scoped
  comment / proto-comment cleanup that the spec exposes).
- Resolving Open Questions on the writer's own initiative.
- Restructuring the existing `docs/` tree.
- Adding spec content to `CLAUDE.md` files.
- Generating diagrams as SVG/PNG (use Mermaid only).
