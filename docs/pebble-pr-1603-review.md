# Pebble UTXO store review

Reviewed PR: https://github.com/bsv-blockchain/teranode/pull/1603

Original reviewed commit: `f1c641b575fd40bdaee7b706fadff9440cd35988`.

## Findings

1. **High: explicit unlock is ignored by Unspend.** In the original
   `stores/utxo/pebble/spend.go:383-399`, an omitted lock argument and explicit
   `false` both mean "do nothing". Conflict reversal and rollback pass `false`
   to unlock parents; leaving them locked prevents subsequent normal spends.
   Preserve all three states: omitted leaves the lock unchanged, true sets it,
   and false clears it.

2. **High: revalidation fails after a previously validated child's parent is
   pruned.** In the original `stores/utxo/pebble/spend.go:164-171`, a missing
   parent unconditionally fails the spend. SQL and Aerospike check whether the
   spending transaction already exists and absorb the missing-parent error
   for an already validated transaction. Pebble must retain this behavior
   while continuing to reject unknown children and other spend errors.

## Reproduction evidence

The original Pebble suite passed with the race detector under Go 1.26.
Two regression tests from the local follow-up commit were run against the
original PR through a temporary Go overlay:

- `TestSpendAbsorbsBlessedNotFound` failed with `UTXO_ERROR -> TX_NOT_FOUND`
  after deleting the parent and revalidating its already stored child.
- `TestUnspendClearsLockWhenAsked` failed because the lock flag remained `8`
  after `Unspend(..., false)`.

## Fixes

The existing local second-pass fix commit implements both corrections and
includes these regression tests in `stores/utxo/pebble/review2_test.go`.
The fixes accompany that commit's other Pebble corrections.

The branch was rebased onto current upstream main without conflicts. Main now
requires Go 1.27. The previous Swiss dependency excluded Go 1.27 in its runtime
build constraint and failed compilation with undefined runtime helper symbols.
Swiss is updated to `v0.0.0-20260820225851-333444432258`, whose runtime constraint
supports Go 1.27.

Compatibility with current main also requires:

- `ParentOutputsForValidation`, reading each parent's own outputs and lowest
  recorded block height from a consistent snapshot. Missing payloads produce
  per-outpoint errors rather than false absence verdicts.
- `SpendAndCreateMulti`, delegating to the shared implementation.
- Honoring `RequireParentOutputs` so bulk import's second wave cannot treat a
  just-created child record as proof that its inputs were previously validated.
- Using a supplied transaction ID without first hashing a seeded transaction
  containing nil output gaps. The new shared parent-output test reproduced
  this panic before the correction.

The shared parent-output and bulk-write contract suites are registered in
`stores/utxo/pebble/current_contract_test.go`, with additional checks for
storage faults, cancellation, and closed stores.

## Verification of the updated branch

Checks target the Pebble store and factory with `pebble,testtxmetacache`
build tags:

- `go test`: passed.
- `go test -race`: passed, including both reviewed regressions and the new
  parent-output and bulk-write contracts.
- `go vet`: passed.
- `golangci-lint run`: passed, zero issues.
- `staticcheck`: passed.
- Standalone `gosec` on the Pebble package: 33 findings in existing code,
  primarily integer conversions in the binary codec, plus directory permissions.
  This invocation does not honor the existing `nolint:gosec` annotations.
- `govulncheck` on the Pebble package: reported 10 reachable vulnerabilities
  across the Go 1.27.1 standard library and shared dependencies. Its fixes include
  Go 1.27.2, OpenTelemetry 1.45.0, and gRPC 1.83.1. These broader upgrades are
  outside the two review corrections; this is not a clean security-scan result.

Repository-wide tests, container integration tests, and mainnet validation were
not run.

This review does not establish mainnet readiness or crash recovery under
process termination; the PR remains experimental.
