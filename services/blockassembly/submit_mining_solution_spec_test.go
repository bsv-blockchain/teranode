package blockassembly

// Negative-path conformance tests for the Mining Solution Contract
// (BA-SOLUTION-NNN) defined in services/blockassembly/spec.md.
//
// These tests deliberately exercise the *rejection* and *failure* paths of
// SubmitMiningSolution / submitMiningSolution, driving the service against the
// letter of the spec to surface divergences. Where a test pins behaviour that
// currently diverges from the spec, the divergence is called out inline with a
// `SPEC DIVERGENCE` comment so it is discoverable.
//
// Contract references:
//   BA-SOLUTION-003  ordered persist(coinbase)->submit(block), all-or-invalidate
//   BA-SOLUTION-004  unknown/expired candidate -> NotFound
//   BA-SOLUTION-005  linear-extension stale -> exact stale message
//   BA-SOLUTION-007  post-submission step failure -> invalidate block
//   BA-SOLUTION-008  invalid block -> job removed + full Reset, chain unchanged
//   BA-CANDIDATE-005 successful submission clears ALL candidates

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/mining"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// craftJob registers a synthetic job with the given previous-block hash into the
// jobStore and returns its 32-byte candidate ID. It bypasses GetMiningCandidate
// so the negative paths downstream of the jobStore lookup can be driven
// deterministically without a completed subtree pipeline.
func craftJob(t *testing.T, s *BlockAssembly, prevHash chainhash.Hash) []byte {
	t.Helper()

	id := chainhash.HashH(fmt.Appendf(nil, "crafted-job-%s", t.Name()))

	nbits := model.NBit{0xff, 0xff, 0x7f, 0x20} // regtest max target
	s.jobStore.Set(id, &subtreeprocessor.Job{
		ID:       &id,
		Subtrees: nil, // empty: coinbase-only candidate, enough for the rejection paths
		MiningCandidate: &model.MiningCandidate{
			Id:           id[:],
			PreviousHash: prevHash[:],
			Version:      1,
			NBits:        nbits[:],
			Height:       124,
		},
	}, jobTTL)

	return id[:]
}

// coinbaseWithInputs builds a parseable transaction with the requested number of
// inputs, each carrying an unlocking script of scriptSigLen bytes. It is used to
// exercise the coinbase request-validation branches of submitMiningSolution.
func coinbaseWithInputs(t *testing.T, numInputs, scriptSigLen int) []byte {
	t.Helper()

	tx := bt.NewTx()

	for range numInputs {
		script := make([]byte, scriptSigLen)
		input := &bt.Input{
			PreviousTxOutIndex: 0xffffffff,
			SequenceNumber:     0xffffffff,
			UnlockingScript:    bscript.NewFromBytes(script),
		}
		require.NoError(t, input.PreviousTxIDAdd(&chainhash.Hash{}))
		tx.Inputs = append(tx.Inputs, input)
	}

	require.NoError(t, tx.AddP2PKHOutputFromAddress("mfrbdGs7RS9ynLoAkeaEmwsQqi8LNVzY2E", 5000000000))

	return tx.Bytes()
}

// nonCoinbaseTx builds a structurally-valid 1-input transaction whose input
// references a NON-zero previous txid. It therefore passes submitMiningSolution's
// coinbase request-validation (exactly one input, scriptSig length within bounds)
// but deterministically fails model.Block.Valid's IsCoinbase() check — giving a
// reliable "invalid block" independent of the (trivial) regtest difficulty.
func nonCoinbaseTx(t *testing.T) []byte {
	t.Helper()

	tx := bt.NewTx()
	prev := chainhash.HashH([]byte("not-a-coinbase-prev-txid"))
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes(make([]byte, 8)),
	}
	require.NoError(t, input.PreviousTxIDAdd(&prev))
	tx.Inputs = append(tx.Inputs, input)
	require.NoError(t, tx.AddP2PKHOutputFromAddress("mfrbdGs7RS9ynLoAkeaEmwsQqi8LNVzY2E", 5000000000))

	return tx.Bytes()
}

// failingSetStore wraps a blob.Store and forces Set to fail, letting a test drive
// the BA-SOLUTION-003 coinbase-persistence failure branch.
type failingSetStore struct {
	blob.Store
}

func (f *failingSetStore) Set(_ context.Context, _ []byte, _ fileformat.FileType, _ []byte, _ ...options.FileOption) error {
	return errors.NewStorageError("injected tx-store Set failure")
}

// newRunningServer spins up a BlockAssembly wired to real in-memory stores with
// the blockchain FSM in RUNNING, the assembler started and reconciled, ready to
// mine and submit. txStore may be nil.
func newRunningServer(t *testing.T, txStore blob.Store) (*BlockAssembly, context.Context) {
	t.Helper()

	initPrometheusMetrics()

	common := testutil.NewCommonTestSetup(t)

	const subtreeSize = 4
	common.Settings.BlockAssembly.InitialMerkleItemsPerSubtree = subtreeSize
	common.Settings.BlockAssembly.MinimumMerkleItemsPerSubtree = subtreeSize
	common.Settings.BlockAssembly.SubmitMiningSolutionWaitForResponse = true

	subtreeStore := memory.New()

	ctx, cancel := context.WithCancel(common.Ctx)

	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, common.Logger, common.Settings, t)
	_ = utxoStore.SetBlockHeight(123)

	s := New(common.Logger, common.Settings, txStore, utxoStore, subtreeStore, blockchainClient)
	s.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, s.Init(ctx))

	require.NoError(t, blockchainClient.Run(ctx, "blockassembly-spec-test"))

	t.Cleanup(func() {
		cancel()
		_ = s.Stop(context.Background())
		if s.blockAssembler != nil {
			s.blockAssembler.Wait()
		}
	})

	require.NoError(t, s.blockAssembler.Start(ctx))
	require.Eventually(t, func() bool {
		return s.blockAssembler.GetCurrentRunningState() == StateRunning
	}, 5*time.Second, 50*time.Millisecond, "block assembler did not reach Running state")

	return s, ctx
}

// mineRealCandidate fills a subtree, obtains a candidate, mines a valid solution
// and returns both. It is the shared "arrange" step for the end-to-end paths.
func mineRealCandidate(t *testing.T, s *BlockAssembly, ctx context.Context, salt string) (*model.MiningCandidate, *model.MiningSolution) {
	t.Helper()

	const subtreeSize = 4
	subtreeStore := s.subtreeStore

	parentHash := chainhash.HashH([]byte("spec-parent-" + salt))
	for i := range subtreeSize - 1 {
		txHash := chainhash.HashH(fmt.Appendf(nil, "spec-tx-%s-%d", salt, i))
		s.blockAssembler.AddTxBatch(
			[]subtreepkg.Node{{Hash: txHash, Fee: uint64(1000 + i), SizeInBytes: 250}},
			[]*subtreepkg.TxInpoints{singleParentInpointsPtr(parentHash, uint32(i))},
		)
	}

	var candidate *model.MiningCandidate

	require.Eventually(t, func() bool {
		c, err := s.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{IncludeSubtrees: true})
		if err != nil || c == nil || len(c.SubtreeHashes) == 0 {
			return false
		}
		for _, h := range c.SubtreeHashes {
			if _, gErr := subtreeStore.Get(ctx, h, fileformat.FileTypeSubtree); gErr != nil {
				return false
			}
		}
		candidate = c
		return true
	}, 10*time.Second, 100*time.Millisecond, "completed subtree was not persisted")

	solution, err := mining.Mine(ctx, s.settings, candidate, nil)
	require.NoError(t, err)
	require.NotNil(t, solution)

	return candidate, solution
}

// ---------------------------------------------------------------------------
// BA-SOLUTION-004 — unknown / malformed candidate
// ---------------------------------------------------------------------------

// TestBASolution004_UnknownCandidate_ReturnsNotFound pins BA-SOLUTION-004 and
// AC-BA-CANDIDATE-005.1's NotFound requirement: a solution against a candidate
// the service does not recognise MUST return NotFound.
func TestBASolution004_UnknownCandidate_ReturnsNotFound(t *testing.T) {
	s, _ := setupServer(t)
	require.NoError(t, s.blockAssembler.Start(t.Context()))

	unknown := chainhash.HashH([]byte("never-registered"))
	req := &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:    unknown[:],
			Nonce: 1,
		},
	}

	resp, err := s.submitMiningSolution(t.Context(), req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.True(t, errors.Is(err, errors.ErrNotFound), "expected NotFound, got %v", err)
}

// TestBASolution004_MalformedCandidateID rejects an ID that is not 32 bytes
// (the request-validation row for SubmitMiningSolution).
func TestBASolution004_MalformedCandidateID(t *testing.T) {
	s, _ := setupServer(t)
	require.NoError(t, s.blockAssembler.Start(t.Context()))

	req := &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:    []byte{0x01, 0x02, 0x03}, // not 32 bytes
			Nonce: 1,
		},
	}

	resp, err := s.submitMiningSolution(t.Context(), req)
	require.Error(t, err)
	require.Nil(t, resp)
}

// ---------------------------------------------------------------------------
// BA-SOLUTION-005 — linear-extension stale candidate
// ---------------------------------------------------------------------------

// TestBASolution005_LinearExtensionStale pins BA-SOLUTION-005 /
// AC-BA-CANDIDATE-006.1: when the candidate's previous-block hash equals the
// current best block's previous-block hash, the service MUST reject with the
// exact stale message.
func TestBASolution005_LinearExtensionStale(t *testing.T) {
	s, _ := setupServer(t)
	require.NoError(t, s.blockAssembler.Start(t.Context()))

	best, _ := s.blockAssembler.CurrentBlock()
	require.NotNil(t, best, "best block header must be loaded after Start")

	// Craft a candidate whose parent is the SAME as the current tip's parent —
	// i.e. the chain has already advanced past the candidate's intended parent.
	id := craftJob(t, s, *best.HashPrevBlock)

	req := &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{Id: id, Nonce: 1},
	}

	resp, err := s.submitMiningSolution(t.Context(), req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "candidate is stale: chain has already advanced past its parent")
}

// ---------------------------------------------------------------------------
// Coinbase request validation (SubmitMiningSolution request-validation row)
// ---------------------------------------------------------------------------

// TestBASolution_CoinbaseMustHaveExactlyOneInput rejects a supplied coinbase
// carrying more than one input before it can reach block construction.
func TestBASolution_CoinbaseMustHaveExactlyOneInput(t *testing.T) {
	s, _ := setupServer(t)
	require.NoError(t, s.blockAssembler.Start(t.Context()))

	// Non-matching parent so we pass the stale gate and reach coinbase validation.
	id := craftJob(t, s, chainhash.HashH([]byte("distinct-parent")))

	req := &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:         id,
			Nonce:      1,
			CoinbaseTx: coinbaseWithInputs(t, 2, 8), // two inputs
		},
	}

	resp, err := s.submitMiningSolution(t.Context(), req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "coinbase transaction must have exactly one input")
}

// TestBASolution_CoinbaseScriptSigTooShort rejects a coinbase whose unlocking
// script is shorter than the 2-byte minimum.
func TestBASolution_CoinbaseScriptSigTooShort(t *testing.T) {
	s, _ := setupServer(t)
	require.NoError(t, s.blockAssembler.Start(t.Context()))

	id := craftJob(t, s, chainhash.HashH([]byte("distinct-parent-2")))

	req := &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:         id,
			Nonce:      1,
			CoinbaseTx: coinbaseWithInputs(t, 1, 1), // scriptSig length 1 (< 2)
		},
	}

	resp, err := s.submitMiningSolution(t.Context(), req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "bad coinbase length")
}

// TestBASolution_CoinbaseUnparseable rejects raw bytes that do not deserialize
// into a transaction.
func TestBASolution_CoinbaseUnparseable(t *testing.T) {
	s, _ := setupServer(t)
	require.NoError(t, s.blockAssembler.Start(t.Context()))

	id := craftJob(t, s, chainhash.HashH([]byte("distinct-parent-3")))

	req := &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:         id,
			Nonce:      1,
			CoinbaseTx: []byte{0xde, 0xad, 0xbe, 0xef},
		},
	}

	resp, err := s.submitMiningSolution(t.Context(), req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "failed to convert coinbaseTx")
}

// ---------------------------------------------------------------------------
// BA-CANDIDATE-005 — successful submission clears ALL candidates
// ---------------------------------------------------------------------------

// TestBACandidate005_SuccessClearsAllCandidates pins AC-BA-CANDIDATE-005.1: after
// a successful submission for C1, a second outstanding candidate C2 MUST become
// unrecognisable (NotFound).
func TestBACandidate005_SuccessClearsAllCandidates(t *testing.T) {
	s, ctx := newRunningServer(t, nil)

	// C1 fully mineable.
	c1, sol1 := mineRealCandidate(t, s, ctx, "c1")

	// Mutate assembly state (complete another subtree) so the next candidate has a
	// distinct, content-derived ID. Without a state change GetMiningCandidate
	// returns the same job — the candidate ID is derived from assembly content.
	const subtreeSize = 4
	parent := chainhash.HashH([]byte("c2-parent"))
	for i := range subtreeSize - 1 {
		txHash := chainhash.HashH(fmt.Appendf(nil, "c2-tx-%d", i))
		s.blockAssembler.AddTxBatch(
			[]subtreepkg.Node{{Hash: txHash, Fee: uint64(2000 + i), SizeInBytes: 250}},
			[]*subtreepkg.TxInpoints{singleParentInpointsPtr(parent, uint32(i))},
		)
	}

	// C2: a second, distinct candidate now outstanding alongside C1.
	var c2 *model.MiningCandidate
	require.Eventually(t, func() bool {
		c, err := s.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{})
		if err != nil || c == nil {
			return false
		}
		c2 = c
		return string(c2.Id) != string(c1.Id)
	}, 10*time.Second, 100*time.Millisecond, "second distinct candidate was not issued")
	require.NotEqual(t, c1.Id, c2.Id, "second candidate must have a distinct ID")

	// Submit C1 successfully.
	resp, err := s.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         c1.Id,
		Nonce:      sol1.Nonce,
		Time:       sol1.Time,
		Version:    sol1.Version,
		CoinbaseTx: sol1.Coinbase,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	// C2 must now be NotFound (whole JobStore cleared).
	_, err = s.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:    c2.Id,
		Nonce: 1,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound), "C2 must be NotFound after C1 succeeds, got %v", err)
}

// ---------------------------------------------------------------------------
// BA-SOLUTION-003 / BA-SOLUTION-007 — coinbase-persistence failure handling
// ---------------------------------------------------------------------------

// TestBASolution003_CoinbaseStoreFailure_AllOrInvalidate drives BA-SOLUTION-003's
// "Steps (a) through (c) MUST all complete successfully or the block MUST be
// invalidated per BA-SOLUTION-007" against a TX store whose Set always fails.
//
// SPEC DIVERGENCE (BA-SOLUTION-003 / BA-SOLUTION-007): the current implementation
// only *logs* the coinbase TX-store Set failure (Server.go, submitMiningSolution)
// and proceeds to submit the block to the Blockchain service, returning Ok. It
// neither aborts the submission nor invalidates the block. There is no
// InvalidateBlock call on the post-submission failure path at all.
//
// This test asserts the SPEC-CORRECT behaviour: when persisting the coinbase
// (step a) fails, the submission MUST fail. It currently FAILS against the
// implementation, exposing the BA-SOLUTION-003 / BA-SOLUTION-007 defect — a block
// is announced to the chain even though its coinbase transaction was never
// persisted to the TX store.
func TestBASolution003_CoinbaseStoreFailure_AllOrInvalidate(t *testing.T) {
	failing := &failingSetStore{Store: memory.New()}
	s, ctx := newRunningServer(t, failing)

	c1, sol1 := mineRealCandidate(t, s, ctx, "cbfail")

	resp, err := s.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         c1.Id,
		Nonce:      sol1.Nonce,
		Time:       sol1.Time,
		Version:    sol1.Version,
		CoinbaseTx: sol1.Coinbase,
	})

	// BA-SOLUTION-003: steps (a) persist coinbase .. (c) submit block MUST all
	// complete successfully or the block MUST be invalidated (BA-SOLUTION-007).
	// The coinbase persistence (step a) was forced to fail, so the submission must
	// not report success.
	require.Error(t, err,
		"BA-SOLUTION-003/007: coinbase TX-store persistence failure must fail the submission (or invalidate the block)")
	require.False(t, resp != nil && resp.Ok,
		"BA-SOLUTION-003/007: submission must not return Ok when the coinbase was not persisted")
}

// ---------------------------------------------------------------------------
// BA-SOLUTION-008 — invalid block resets assembler, chain unchanged
// ---------------------------------------------------------------------------

// TestBASolution008_InvalidBlock_ResetsAndLeavesChainUnchanged pins
// AC-BA-SOLUTION-008.1: a solution producing an invalid block MUST remove the
// candidate from the JobStore, MUST NOT change the chain, and MUST drive the
// assembler through Reset back to Running.
func TestBASolution008_InvalidBlock_ResetsAndLeavesChainUnchanged(t *testing.T) {
	s, ctx := newRunningServer(t, nil)

	c1, sol1 := mineRealCandidate(t, s, ctx, "invalid")

	_, heightBefore := s.blockAssembler.CurrentBlock()

	// Supply a structurally-valid-but-not-a-coinbase transaction. It clears the
	// handler's coinbase request-validation yet deterministically fails
	// block.Valid (IsCoinbase()), producing a genuine invalid block that must
	// trigger the BA-SOLUTION-008 reset path.
	_, err := s.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         c1.Id,
		Nonce:      sol1.Nonce,
		Time:       sol1.Time,
		Version:    sol1.Version,
		CoinbaseTx: nonCoinbaseTx(t),
	})
	require.Error(t, err, "a solution producing an invalid block must be rejected")

	// Chain height must be unchanged.
	require.Eventually(t, func() bool {
		return s.blockAssembler.GetCurrentRunningState() == StateRunning
	}, 5*time.Second, 50*time.Millisecond, "assembler must return to Running after invalid-block reset")

	_, heightAfter := s.blockAssembler.CurrentBlock()
	require.Equal(t, heightBefore, heightAfter, "chain height MUST NOT change on an invalid solution")

	// The offending candidate must be gone from the JobStore -> NotFound.
	_, err = s.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:    c1.Id,
		Nonce: 1,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound), "offending candidate must be removed from JobStore, got %v", err)
}

// ===========================================================================
// Mining Candidate Contract (BA-CANDIDATE-NNN)
// ===========================================================================

// TestBAState001_GetMiningCandidateDuringLoading_MustReject drives the state
// matrix (BA-STATE-001): in LoadingUnmined, GetMiningCandidate MUST "reject (not
// ready)".
//
// SPEC DIVERGENCE (BA-STATE-001, BA-CANDIDATE-001): GetMiningCandidate only gates
// on the *blockchain service's* FSM being RUNNING (Server.go:1286). It never
// consults the block-assembly-side `unminedTransactionsLoading` flag the way
// AddTx / SubmitMiningSolution / reset variants do. During startup recovery the
// blockchain service is typically already RUNNING while block assembly is still
// replaying unmined transactions, so this call hands out a candidate built from a
// PARTIALLY-RECOVERED pipeline — a candidate that can omit not-yet-recovered
// unmined transactions. A miner could then mine a block on an incomplete set.
//
// This test asserts the SPEC-CORRECT behaviour (rejection) and therefore FAILS
// against the current implementation, exposing the gap.
func TestBAState001_GetMiningCandidateDuringLoading_MustReject(t *testing.T) {
	s, ctx := newRunningServer(t, nil)

	// Simulate the LoadingUnmined phase: the blockchain FSM is RUNNING (set up by
	// newRunningServer) but block assembly is still replaying unmined txs.
	s.blockAssembler.unminedTransactionsLoading.Store(true)
	t.Cleanup(func() { s.blockAssembler.unminedTransactionsLoading.Store(false) })

	_, err := s.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{})

	// BA-STATE-001 / matrix row "GetMiningCandidate | LoadingUnmined | reject (not ready)".
	require.Error(t, err,
		"BA-STATE-001: GetMiningCandidate MUST reject while unmined transactions are still loading")
	require.Contains(t, err.Error(), "not ready",
		"BA-STARTUP-003 style guard expected: 'service not ready - unmined transactions are still being loaded'")
}

// TestBACandidate011_GetCandidateBlock_UnknownAndMalformed pins BA-CANDIDATE-011:
// GetCandidateBlock MUST return NotFound for an unknown/expired candidate ID, and
// reject a malformed (non-32-byte) ID.
func TestBACandidate011_GetCandidateBlock_UnknownAndMalformed(t *testing.T) {
	s, _ := setupServer(t)
	require.NoError(t, s.blockAssembler.Start(t.Context()))

	t.Run("unknown ID -> NotFound", func(t *testing.T) {
		unknown := chainhash.HashH([]byte("no-such-candidate"))
		_, err := s.GetCandidateBlock(t.Context(), &blockassembly_api.GetCandidateBlockRequest{Id: unknown[:]})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound), "expected NotFound, got %v", err)
	})

	t.Run("malformed ID -> rejected", func(t *testing.T) {
		_, err := s.GetCandidateBlock(t.Context(), &blockassembly_api.GetCandidateBlockRequest{Id: []byte{0x01, 0x02}})
		require.Error(t, err)
	})
}

// TestBACandidate006_ChainTipChangeDoesNotWipeCandidates pins BA-CANDIDATE-006:
// the candidate-tracking store MUST NOT be invalidated on chain-tip change; only
// a successful submission clears it (BA-CANDIDATE-005). A candidate that has gone
// stale via linear extension must therefore still be *recognized* (returning the
// stale error), never NotFound.
//
// This is verified structurally: the only jobStore-clearing paths in the service
// are DeleteAll on successful submission and Delete of the single offending job on
// an invalid block (Server.go:1587, 1622). No chain-notification / move-forward
// path touches the jobStore. The behavioural consequence — a stale candidate
// yields the stale error rather than NotFound — is covered by
// TestBASolution005_LinearExtensionStale.
func TestBACandidate006_ChainTipChangeDoesNotWipeCandidates(t *testing.T) {
	s, _ := setupServer(t)
	require.NoError(t, s.blockAssembler.Start(t.Context()))

	// A recognized-but-stale candidate returns the stale error (recognized), not
	// NotFound (wiped). craftJob installs a candidate whose parent matches the
	// current tip's parent, i.e. the chain has advanced past it.
	best, _ := s.blockAssembler.CurrentBlock()
	require.NotNil(t, best)
	id := craftJob(t, s, *best.HashPrevBlock)

	_, err := s.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{Id: id, Nonce: 1},
	})
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrNotFound),
		"BA-CANDIDATE-006: a stale candidate must remain recognized, not be wiped to NotFound")
	require.Contains(t, err.Error(), "candidate is stale")
}
