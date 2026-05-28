// Package blockassembly conformance tests.
//
// Each test in this file asserts exactly one Acceptance Criterion (AC) from the
// Conformance Test Matrix in spec.md. The test name encodes the AC ID so the
// mapping spec → test → code is mechanical:
//
//	AC-BA-INGEST-006.1  ⇄  TestACBAIngest006_1_AddTxIsIdempotent
//
// Conventions:
//   - Exactly one AC per test. Resist asserting anything else.
//   - The docblock copies the AC verbatim; failure messages cite the AC ID.
//   - Stubs that are not yet implemented call t.Skip with a reason — either
//     "not yet implemented" or "blocked by GAP-BA-NNN: …" so unimplemented
//     contract surface is visible in `go test -v` output and a closed GAP
//     becomes mechanically "delete the Skip and write the body."
//
// Coverage report:
//
//	specd=$(grep -c '^\*\*AC-BA-' services/blockassembly/spec.md)
//	impl=$(grep -c '^func TestACBA' services/blockassembly/conformance_test.go)
//	skip=$(go test -run '^TestACBA' -v -tags testtxmetacache ./services/blockassembly/ 2>&1 | grep -c '^--- SKIP')
//	echo "Conformance: $((impl-skip))/$specd implemented, $skip skipped"
package blockassembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Transaction Ingest (BA-INGEST-NNN)
// ============================================================================

// AC-BA-INGEST-004.1 — Inconsistent columnar batch is rejected wholesale.
//
// Given the service is in Running, when an AddTxBatchColumnar request arrives
// with a parent-tx-offsets array whose last value exceeds the
// parent-hashes-pool length, then the service MUST reject the batch and zero
// transactions from the batch are enqueued.
func TestACBAIngest004_1_ColumnarInconsistentBatchRejected(t *testing.T) {
	// --- Given: a Running service with no prior transactions ---
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	preCount := server.TxCount()

	// One transaction; parent-tx-offsets claims 5 parents but the parent-hashes
	// pool is empty — the precise inconsistency BA-INGEST-004 calls out.
	txid := chainhash.HashH([]byte("AC-BA-INGEST-004.1"))
	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txid[:],
		Fees:                 []uint64{100},
		Sizes:                []uint64{250},
		ParentTxHashesPacked: []byte{}, // pool is empty …
		ParentTxOffsets:      []uint32{0, 5}, // … but the last offset claims 5
		VoutIdxsPacked:       []uint32{},
		VoutIdxsTxOffsets:    []uint32{0, 0},
	}

	// --- When ---
	_, err := server.AddTxBatchColumnar(t.Context(), req)

	// --- Then: rejected, and zero of the batch's txs are enqueued ---
	require.Error(t, err, "AC-BA-INGEST-004.1: inconsistent batch must be rejected")
	require.Contains(t, err.Error(), "parent_tx_offsets",
		"rejection must name the offending field")

	// Give any (incorrect) async enqueue a window to land before we assert absence.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, preCount, server.TxCount(),
		"AC-BA-INGEST-004.1: rejected batch must not enqueue any tx")
}

// AC-BA-INGEST-006.1 — idempotent AddTx (BA-INGEST-006).
//
// Given the service is in Running and transaction T has been accepted via
// AddTx, when T is submitted again via AddTx, then the assembly state MUST
// contain T exactly once (no duplicate inclusion in subtrees or candidates).
func TestACBAIngest006_1_AddTxIsIdempotent(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-INGEST-006.1) — needs the right observable; see outline")
	// Observation from a first attempt: server.TxCount() increments on every
	// AddTx call regardless of whether the txid is a duplicate — so it counts
	// *add operations*, not unique txids in the pipeline. The contract is
	// about subtree/candidate inclusion, not TxCount. Use one of:
	//   (a) GetBlockAssemblyTxs and count occurrences of the txid.
	//   (b) Drive a subtree to completion (force or wait), retrieve it from
	//       the Subtree Store, and count occurrences of the txid in its nodes.
	//   (c) Call GetMiningCandidate, walk its subtree hashes, and count.
	// Option (a) is cheapest; option (b) is the most direct read of the contract.
	//
	// Skeleton (option a):
	//   server, _ := setupServer(t)
	//   require.NoError(t, server.blockAssembler.Start(t.Context()))
	//   ... build req with a fixed txid; call AddTx twice ...
	//   resp, err := server.GetBlockAssemblyTxs(t.Context(), &emptypb.Empty{})
	//   require.NoError(t, err)
	//   count := 0
	//   for _, h := range resp.GetTxHashes() {
	//       if bytes.Equal(h, txHash[:]) { count++ }
	//   }
	//   require.Equal(t, 1, count, "AC-BA-INGEST-006.1 violated")
}

// AC-BA-INGEST-008.1 — concurrent AddTx/RemoveTx safety (BA-INGEST-008).
//
// Given the service is in Running and AddTx(T) and RemoveTx(T) are submitted
// concurrently, then for any candidate generated after both have completed,
// T MUST NOT appear in that candidate.
func TestACBAIngest008_1_ConcurrentAddRemove_TxAbsentFromLaterCandidate(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-INGEST-008.1) — should be run under -race")
	// Outline:
	//   1. setupServer(t); start the assembler.
	//   2. Launch AddTx(T) and RemoveTx(T) in two goroutines via a sync.WaitGroup.
	//   3. After both return, call GetMiningCandidate.
	//   4. Inspect the candidate's subtrees (via GetBlockAssemblyTxs or by
	//      decoding the returned subtree hashes) and assert T not present.
}

// ============================================================================
// Subtree Contract (BA-SUBTREE-NNN)
// ============================================================================

// AC-BA-SUBTREE-007.1 — every subtree referenced by an announced block is
// retrievable from the Subtree Store at announcement time (BA-SUBTREE-007).
func TestACBASubtree007_1_AnnouncedBlockSubtreesPresentInStore(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-SUBTREE-007.1)")
	// Outline:
	//   1. setupServer(t) with a memory blob store you can inspect.
	//   2. Drive AddTx → GetMiningCandidate → SubmitMiningSolution to finalize a block.
	//   3. Hook the Blockchain client's AddBlock to capture the announcement moment.
	//   4. At that moment, assert every subtree hash in the block exists in the store.
}

// AC-BA-SUBTREE-010.1 — coinbase placeholder anywhere other than (first
// subtree, first node) MUST error and no candidate is issued (BA-SUBTREE-010).
func TestACBASubtree010_1_MisplacedCoinbasePlaceholderErrors(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-SUBTREE-010.1)")
	// Outline:
	//   1. Construct a SubtreeProcessor state with a coinbase placeholder at index 1.
	//   2. Invoke the candidate-construction path.
	//   3. Assert an error is raised and no candidate is returned.
	// Note: TestBlockAssembly_ShouldNotAllowMoreThanOneCoinbaseTx already covers
	// adjacent ground; consider extracting and renaming or wrapping.
}

// ============================================================================
// Mining Candidate (BA-CANDIDATE-NNN)
// ============================================================================

// AC-BA-CANDIDATE-004.1 — candidate TTL of 600s (BA-CANDIDATE-004).
//
// Given a candidate was issued at time T0, when more than 600 seconds have
// elapsed since T0 with no successful solution submission, then
// SubmitMiningSolution against that candidate MUST return NotFound.
func TestACBACandidate004_1_ExpiredCandidateReturnsNotFound(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-CANDIDATE-004.1) — needs an injectable clock to avoid time.Sleep(600s)")
	// Outline:
	//   1. Refactor JobStore TTL plumbing to accept an injectable clock.
	//   2. setupServer(t) with that clock; issue a candidate.
	//   3. Advance the clock past jobTTL (10m); call SubmitMiningSolution.
	//   4. Assert the returned gRPC error maps to NotFound.
}

// AC-BA-CANDIDATE-005.1 — successful solution wipes ALL candidates
// (BA-CANDIDATE-005).
//
// Given two candidates C1 and C2 are both currently issued, when a successful
// SubmitMiningSolution for C1 completes, then SubmitMiningSolution for C2 MUST
// return NotFound.
func TestACBACandidate005_1_SuccessfulSolutionWipesAllCandidates(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-CANDIDATE-005.1)")
	// Outline:
	//   1. setupServer(t); start; add a few txs so candidates are non-empty.
	//   2. Issue C1 via GetMiningCandidate; issue C2 via GetMiningCandidate.
	//   3. Solve C1 and SubmitMiningSolution(C1) — expect success.
	//   4. SubmitMiningSolution(C2) — assert NotFound.
}

// AC-BA-CANDIDATE-006.1 — linear-extension stale candidate returns the
// stale-chain error (BA-CANDIDATE-006 / BA-SOLUTION-005).
//
// Given a candidate was issued before an external block extended the local
// chain, when the chain has advanced (linear extension) but no JobStore
// eviction has occurred, then SubmitMiningSolution for the now-stale candidate
// MUST return the stale-chain error per BA-SOLUTION-005.
func TestACBACandidate006_1_StaleCandidateOnLinearExtension(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-CANDIDATE-006.1)")
	// Outline:
	//   1. setupServer(t); issue C1 at tip H.
	//   2. Inject an external block extending H → H+1 (via blockchain client's AddBlock).
	//   3. Solve C1 against H; call SubmitMiningSolution.
	//   4. Assert the error message contains "candidate is stale: chain has
	//      already advanced past its parent".
}

// ============================================================================
// Mining Solution (BA-SOLUTION-NNN)
// ============================================================================

// AC-BA-SOLUTION-008.1 — invalid block ⇒ chain unchanged + candidate removed
// + full Reset (BA-SOLUTION-008).
//
// Given a candidate is solved with a nonce that produces a block failing block
// validation, when SubmitMiningSolution is invoked, then the chain MUST NOT be
// modified, the candidate MUST be removed from the JobStore, and the assembler
// MUST be in Resetting (transitioning to Running) immediately afterward.
func TestACBASolution008_1_InvalidBlockTriggersResetWithoutChainChange(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-SOLUTION-008.1)")
	// Outline:
	//   1. Issue a candidate; tamper the header (e.g. zero the difficulty target).
	//   2. Snapshot best block hash; call SubmitMiningSolution.
	//   3. Assert error; best block hash unchanged.
	//   4. Eventually-poll GetBlockAssemblyState; assert State transitions
	//      through "resetting" back to "running".
	//   5. Assert the candidate ID is no longer accepted (NotFound).
}

// ============================================================================
// Chain-Tip and Reorg (BA-REORG-NNN)
// ============================================================================

// AC-BA-REORG-006.1 — deep reorg triggers full reset (BA-REORG-006).
//
// Given a competing chain arrives with a depth from common ancestor of 150
// blocks at current chain height of 5000 (coinbase maturity threshold = 100),
// then the service MUST perform a full reset and MUST NOT execute
// move-back / move-forward in place.
func TestACBAReorg006_1_DeepReorgPerformsFullReset(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-REORG-006.1) — needs synthetic 5000-block chain harness")
	// Outline:
	//   1. Build a synthetic chain harness reaching height 5000.
	//   2. Inject a competing chain whose fork point is 150 blocks back.
	//   3. Trigger reconciliation; spy on the assembler to confirm Reset path
	//      (not move-back / move-forward) was taken.
}

// ============================================================================
// Startup and Recovery (BA-STARTUP-NNN)
// ============================================================================

// AC-BA-STARTUP-003.1 — loading-guard rejection message (BA-STARTUP-003).
//
// Given startup recovery is in progress (unminedTransactionsLoading == true),
// when SubmitMiningSolution is invoked, then the response MUST be a gRPC error
// containing the literal text
// `service not ready - unmined transactions are still being loaded`.
func TestACBAStartup003_1_LoadingGuardRejectionMessage(t *testing.T) {
	// --- Given: a server whose loading guard is forced on ---
	//
	// Note: we deliberately do NOT call Start() — its completion would clear
	// the flag. Setting the atomic directly is what reorg_race_test.go does
	// (services/blockassembly/reorg_race_test.go:23). The guard at
	// Server.go:1286 is the first statement of SubmitMiningSolution, so it
	// fires before any field of the request is inspected; an empty request
	// is therefore sufficient.
	server, _ := setupServer(t)
	server.blockAssembler.unminedTransactionsLoading.Store(true)

	// --- When ---
	_, err := server.SubmitMiningSolution(t.Context(), &blockassembly_api.SubmitMiningSolutionRequest{})

	// --- Then: error contains the exact literal contract string ---
	require.Error(t, err, "AC-BA-STARTUP-003.1: must reject while loading")
	require.Contains(t, err.Error(),
		"service not ready - unmined transactions are still being loaded",
		"AC-BA-STARTUP-003.1: rejection string is part of the public contract — must match verbatim")
}

// AC-BA-STARTUP-004.1 — UTXO-iterator error on startup ⇒ refuse to start
// (BA-STARTUP-004).
//
// Given the UTXO store returns an error when the service requests its
// unmined-tx iterator on startup, then the service MUST NOT transition to
// Running and MUST report a startup failure.
func TestACBAStartup004_1_UtxoIteratorErrorPreventsRunning(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-STARTUP-004.1)")
	// Outline:
	//   1. setupServer(t) with a UTXO store whose unmined-iterator constructor returns error.
	//   2. Start the assembler; expect a non-nil error from Start().
	//   3. Assert State never reaches "running" (poll GetBlockAssemblyState).
}

// AC-BA-STARTUP-007.1 — ValidateInputs marks + cascades conflicting txs
// (BA-STARTUP-007).
//
// Given an unmined transaction T whose input is recorded in the UTXO store as
// spent by a different transaction T', when ResetBlockAssemblyValidateInputs
// is invoked, then T MUST be marked conflicting, T's descendants in the
// assembly pipeline MUST be evicted, and the operation MUST complete
// successfully.
func TestACBAStartup007_1_ValidateInputsMarksAndCascadesConflicting(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-STARTUP-007.1)")
	// Outline:
	//   1. Seed UTXO store such that T's input is spent by T' (not T).
	//   2. Insert T plus a descendant chain T→C1→C2 into the assembler.
	//   3. Call ResetBlockAssemblyValidateInputs; expect success.
	//   4. Assert T marked conflicting in UTXO store; C1 and C2 evicted from pipeline.
}

// AC-BA-STARTUP-010.1 — ingest continues during Reset (BA-STARTUP-010).
//
// Given the service is in Resetting due to an in-flight ResetBlockAssembly,
// when AddTxBatch is invoked, then the request MUST be accepted and the
// transactions MUST be present in the queue when the reset completes.
func TestACBAStartup010_1_IngestContinuesDuringReset(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-STARTUP-010.1)")
	// Outline:
	//   1. setupServer(t); drive into Resetting (hook the reset to block on a chan).
	//   2. While Resetting, call AddTxBatch with N txs; expect success.
	//   3. Unblock the reset; eventually-poll TxCount == N.
}

// ============================================================================
// Dependency Failure (BA-DEPENDENCY-NNN) — blocked by GAP-BA-001
// ============================================================================

// AC-BA-DEPENDENCY-002.1 — Idle transition on persistent Blockchain failure
// (BA-DEPENDENCY-002).
//
// Given the Blockchain service has returned three consecutive errors within a
// 30-second window with no successful interleaved call, when the service
// evaluates dependency health, then it MUST transition to Idle and
// GetBlockAssemblyState MUST report BlockAssemblyState == "idle" within one
// probe interval.
func TestACBADependency002_1_TransitionsToIdleOnPersistentFailure(t *testing.T) {
	t.Skip("blocked by GAP-BA-001: Idle state and BlockchainFailureThreshold not yet implemented")
	// Outline (post-GAP):
	//   1. setupServer with a Blockchain mock primed to fail 3× within 30s.
	//   2. Drive any operation that calls the Blockchain (e.g. SendNotification).
	//   3. Eventually-poll GetBlockAssemblyState; assert State == "idle".
}

// AC-BA-DEPENDENCY-005.1 — Idle ⇒ Running on probe success
// (BA-DEPENDENCY-005).
//
// Given the service is in Idle, when a Blockchain probe succeeds and the
// service has reconciled to the current authoritative tip, then it MUST
// transition to Running.
func TestACBADependency005_1_RecoversFromIdleOnProbeSuccess(t *testing.T) {
	t.Skip("blocked by GAP-BA-001: Idle state and BlockchainProbeInterval not yet implemented")
	// Outline (post-GAP):
	//   1. Drive into Idle as in AC-BA-DEPENDENCY-002.1.
	//   2. Restore the Blockchain mock to success.
	//   3. Eventually-poll GetBlockAssemblyState; assert State == "running"
	//      within one probe interval.
}

// ============================================================================
// Configuration (BA-CONFIG-NNN)
// ============================================================================

// AC-BA-CONFIG-002.1 — non-power-of-2 Minimum refuses to start
// (BA-CONFIG-002).
//
// Given MinimumMerkleItemsPerSubtree is set to 1000 (not a power of two), when
// the service is started, then it MUST refuse to start and emit a
// configuration-validation error naming the offending setting.
func TestACBAConfig002_1_NonPowerOfTwoMinimumRefusesToStart(t *testing.T) {
	t.Skip("blocked by GAP-BA-002: config-load power-of-two validation not yet implemented")
	// Outline (post-GAP):
	//   1. Build settings with MinimumMerkleItemsPerSubtree = 1000.
	//   2. Call settings.Validate() (or whatever the new entry point is).
	//   3. Assert error returned; error message names "MinimumMerkleItemsPerSubtree".
}

// AC-BA-CONFIG-008.1 — GenerateBlocks gated by GenerateSupported
// (BA-CONFIG-008).
//
// Given ChainCfgParams.GenerateSupported == false, when GenerateBlocks is
// invoked, then it MUST return an error with message
// `generate is not supported`.
func TestACBAConfig008_1_GenerateBlocksDisabledOnUnsupportedNetwork(t *testing.T) {
	// --- Given: a Running service on a network where GenerateSupported is false ---
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	// Force the mainnet-equivalent constraint: regtest defaults to true in test
	// setups, so set explicitly per BA-CONFIG-007/008.
	server.blockAssembler.settings.ChainCfgParams.GenerateSupported = false

	// --- When ---
	_, err := server.GenerateBlocks(t.Context(), &blockassembly_api.GenerateBlocksRequest{Count: 1})

	// --- Then ---
	require.Error(t, err, "AC-BA-CONFIG-008.1: GenerateBlocks must reject when GenerateSupported == false")
	require.Contains(t, err.Error(), "generate is not supported",
		"AC-BA-CONFIG-008.1: error message must be the literal `generate is not supported`")
}

// ============================================================================
// Observability (BA-OBSERVABILITY-NNN)
// ============================================================================

// AC-BA-OBSERVABILITY-003.1 — mid-reorg state reporting
// (BA-OBSERVABILITY-003).
//
// Given the service is mid-reorg, when GetBlockAssemblyState is invoked, then
// BlockAssemblyState MUST be one of "reorging" or related transient values,
// never "running".
func TestACBAObservability003_1_MidReorgStateNeverRunning(t *testing.T) {
	t.Skip("not yet implemented (AC-BA-OBSERVABILITY-003.1)")
	// Outline:
	//   1. setupServer(t); hook the reorg path to block on a chan after
	//      setCurrentRunningState(StateReorging).
	//   2. Trigger a reorg; while blocked, call GetBlockAssemblyState repeatedly.
	//   3. Assert every observation is one of the transient state strings,
	//      never "running": {"reorging","resetting","movingUp",
	//      "reconciling","blockchainSubscription"}.
	//   4. Unblock; assert eventual return to "running".
}
