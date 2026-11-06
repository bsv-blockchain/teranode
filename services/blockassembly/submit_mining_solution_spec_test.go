package blockassembly

// Negative-path conformance tests for the Mining Solution Contract
// (BA-SOLUTION-NNN) and Mining Candidate Contract (BA-CANDIDATE-NNN) defined in
// services/blockassembly/spec.md.
//
// These tests exercise the rejection paths of SubmitMiningSolution /
// submitMiningSolution against real in-memory stores. Two spec divergences that
// still fail today (BA-SOLUTION-003/007 and BA-STATE-001) live in
// submit_mining_solution_known_defects_test.go behind the bagaps build tag.
//
// Contract references:
//   BA-SOLUTION-004  unknown/expired candidate -> NotFound; malformed ID -> InvalidArgument
//   BA-SOLUTION-005  linear-extension stale -> exact stale message
//   BA-CANDIDATE-005 successful submission clears ALL candidates
//   BA-CANDIDATE-006 chain-tip change does not wipe candidates

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

// fillOneSubtree adds enough transactions to complete one subtree, so the next
// candidate carries new content and therefore a new content-derived ID.
func fillOneSubtree(t *testing.T, s *BlockAssembly, salt string) {
	t.Helper()

	const subtreeSize = 4

	parentHash := chainhash.HashH([]byte("spec-parent-" + salt))
	for i := range subtreeSize - 1 {
		txHash := chainhash.HashH(fmt.Appendf(nil, "spec-tx-%s-%d", salt, i))
		require.True(t, s.blockAssembler.AddTxBatchIfRoom(
			[]subtreepkg.Node{{Hash: txHash, Fee: uint64(1000 + i), SizeInBytes: 250}},
			[]*subtreepkg.TxInpoints{singleParentInpointsPtr(parentHash, uint32(i))},
		), "block assembly queue refused the transaction")
	}
}

// mineRealCandidate fills a subtree, obtains a candidate, mines a valid solution
// and returns both. It is the shared "arrange" step for the end-to-end paths.
func mineRealCandidate(t *testing.T, s *BlockAssembly, ctx context.Context, salt string) (*model.MiningCandidate, *model.MiningSolution) {
	t.Helper()

	fillOneSubtree(t, s, salt)

	var candidate *model.MiningCandidate

	require.Eventually(t, func() bool {
		c, err := s.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{IncludeSubtrees: true})
		if err != nil || c == nil || len(c.SubtreeHashes) == 0 {
			return false
		}
		for _, h := range c.SubtreeHashes {
			if _, gErr := s.subtreeStore.Get(ctx, h, fileformat.FileTypeSubtree); gErr != nil {
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
// Unknown / malformed candidate
// ---------------------------------------------------------------------------

// TestSubmitMiningSolution_UnknownCandidateIsNotFound: a solution against a
// candidate the service does not recognise MUST return NotFound.
//
// Spec: BA-SOLUTION-004, AC-BA-CANDIDATE-005.1.
func TestSubmitMiningSolution_UnknownCandidateIsNotFound(t *testing.T) {
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

// TestSubmitMiningSolution_MalformedCandidateIDIsInvalidArgument: a candidate ID
// that is not 32 bytes is a bad request, rejected as InvalidArgument (as
// GetCandidateBlock does) rather than an unclassified error.
//
// Spec: BA-SOLUTION-004 (SubmitMiningSolution request-validation row).
func TestSubmitMiningSolution_MalformedCandidateIDIsInvalidArgument(t *testing.T) {
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
	require.True(t, errors.Is(err, errors.ErrInvalidArgument), "expected InvalidArgument, got %v", err)
}

// ---------------------------------------------------------------------------
// Stale candidate
// ---------------------------------------------------------------------------

// TestSubmitMiningSolution_StaleCandidateIsRejectedButStillRecognized: when the
// candidate's previous-block hash equals the current tip's previous-block hash,
// the chain has already advanced past the candidate's parent and the service
// MUST reject with the exact stale message.
//
// The same request also shows the candidate was not wiped by the chain advancing:
// it is still recognized (stale error), never NotFound. The only paths that clear
// the job store are DeleteAll on a successful submission and Delete of the one
// offending job on an invalid block; no chain-notification path touches it.
//
// Spec: BA-SOLUTION-005, AC-BA-CANDIDATE-006.1, BA-CANDIDATE-006.
func TestSubmitMiningSolution_StaleCandidateIsRejectedButStillRecognized(t *testing.T) {
	s, _ := setupServer(t)
	require.NoError(t, s.blockAssembler.Start(t.Context()))

	best, _ := s.blockAssembler.CurrentBlock()
	require.NotNil(t, best, "best block header must be loaded after Start")

	// Given a candidate whose parent is the SAME as the current tip's parent
	staleCandidateID := craftJob(t, s, *best.HashPrevBlock)

	// When a solution for it is submitted
	resp, err := s.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{Id: staleCandidateID, Nonce: 1},
	})

	// Then it is rejected as stale, and is still a known candidate
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "candidate is stale: chain has already advanced past its parent")
	require.False(t, errors.Is(err, errors.ErrNotFound),
		"a stale candidate must remain recognized, not be wiped to NotFound")
}

// ---------------------------------------------------------------------------
// Coinbase request validation (SubmitMiningSolution request-validation row)
// ---------------------------------------------------------------------------

// TestSubmitMiningSolution_RejectsCoinbaseWithMoreThanOneInput rejects a supplied
// coinbase carrying more than one input before it can reach block construction.
func TestSubmitMiningSolution_RejectsCoinbaseWithMoreThanOneInput(t *testing.T) {
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

// TestSubmitMiningSolution_RejectsCoinbaseScriptSigTooShort rejects a coinbase
// whose unlocking script is shorter than the 2-byte minimum.
func TestSubmitMiningSolution_RejectsCoinbaseScriptSigTooShort(t *testing.T) {
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

// TestSubmitMiningSolution_RejectsUnparseableCoinbase rejects raw bytes that do
// not deserialize into a transaction.
func TestSubmitMiningSolution_RejectsUnparseableCoinbase(t *testing.T) {
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
// Successful submission clears ALL candidates
// ---------------------------------------------------------------------------

// TestSubmitMiningSolution_SuccessClearsEveryOutstandingCandidate: after a
// successful submission for candidate C1, a second outstanding candidate C2 MUST
// become unrecognisable (NotFound).
//
// Spec: BA-CANDIDATE-005, AC-BA-CANDIDATE-005.1.
func TestSubmitMiningSolution_SuccessClearsEveryOutstandingCandidate(t *testing.T) {
	s, ctx := newRunningServer(t, nil)

	// Given a fully mineable candidate C1
	c1, sol1 := mineRealCandidate(t, s, ctx, "c1")

	// And a second, distinct candidate C2 outstanding alongside it. The candidate ID
	// is derived from assembly content, so another subtree must complete first.
	fillOneSubtree(t, s, "c2")

	var c2 *model.MiningCandidate
	require.Eventually(t, func() bool {
		c, err := s.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{})
		if err != nil || c == nil {
			return false
		}
		c2 = c
		return string(c2.Id) != string(c1.Id)
	}, 10*time.Second, 100*time.Millisecond, "second distinct candidate was not issued")

	// When C1 is submitted successfully
	resp, err := s.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         c1.Id,
		Nonce:      sol1.Nonce,
		Time:       sol1.Time,
		Version:    sol1.Version,
		CoinbaseTx: sol1.Coinbase,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	// Then C2 is NotFound (the whole job store was cleared)
	_, err = s.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:    c2.Id,
		Nonce: 1,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound), "C2 must be NotFound after C1 succeeds, got %v", err)
}
