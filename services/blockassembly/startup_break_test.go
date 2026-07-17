// Package blockassembly adversarial startup tests.
//
// These tests are written with the explicit intention of BREAKING the startup /
// recovery path — they drive the failure modes the BA-STARTUP contract names but
// that the existing suite only exercised on the happy path (the pre-existing
// TestLoadUnminedTransactionsCoverage subtests discard the returned error with
// `_ =` and assert `true`, so they prove execution, not refusal semantics).
//
// Contract focus:
//
//   - BA-STARTUP-004 / AC-BA-STARTUP-004.1: "If unmined-transaction recovery
//     fails for any reason — UTXO store unavailable, iterator construction error,
//     mid-iteration failure, or post-load reconciliation error — the service MUST
//     refuse to start. The service MUST NOT enter Running with partial recovery
//     state." Each named failure mode gets its own probe below.
//   - BA-STARTUP-005: recovery MUST be idempotent across restarts. A crash during
//     recovery must leave the unmined-since index unmodified so the next start
//     re-attempts from a clean slate. We assert that a failed load performs NO
//     store mutation (no MarkTransactionsOnLongestChain, no SetConflicting) and
//     that the in-flight loading flag is cleared so a retry starts clean.
//   - initState + issue #980 genesis guard: a corrupt/truncated persisted
//     checkpoint on a live (non-genesis) chain MUST refuse to start rather than
//     silently adopt the tip (which would skip coinbase-UTXO creation). This
//     exercises the DecodeState-error arm of initState, distinct from the
//     already-covered ErrNoRows arm.
package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newRecoveryTestAssembler builds a minimal BlockAssembler wired only with the
// collaborators loadUnminedTransactions touches, anchored at genesis (height 0).
// It deliberately omits the subtree processor: every test here must fail before
// any transaction reaches it, and a nil processor turns an accidental reach into
// a loud panic rather than a silent pass.
func newRecoveryTestAssembler(t *testing.T, store utxostore.Store, bc *blockchain.Mock) *BlockAssembler {
	t.Helper()

	ba := &BlockAssembler{
		logger:           ulogger.TestLogger{},
		settings:         createTestSettings(t),
		stats:            gocore.NewStat("startup-break-test"),
		utxoStore:        store,
		blockchainClient: bc,
	}
	ba.setBestBlockHeader(model.GenesisBlockHeader, 0)

	return ba
}

// assertNoRecoveryMutation asserts the two UTXO-store writes that a load can
// perform were never issued — the BA-STARTUP-005 idempotency guarantee: a failed
// recovery must leave the unmined-since index untouched.
func assertNoRecoveryMutation(t *testing.T, store *utxostore.MockUtxostore) {
	t.Helper()
	store.AssertNotCalled(t, "MarkTransactionsOnLongestChain", mock.Anything, mock.Anything, mock.Anything)
	store.AssertNotCalled(t, "SetConflicting", mock.Anything, mock.Anything, mock.Anything)
}

// BA-STARTUP-004 / AC-BA-STARTUP-004.1 — iterator construction error.
// GetUnminedTxIterator failing on startup MUST abort recovery. The service can
// never reach Running because Start propagates this error (Server.go: "failed to
// load un-mined transactions"). Also asserts BA-STARTUP-005: no mutation, flag
// cleared for a clean retry.
func TestStartup_RefusesWhenIteratorConstructionFails(t *testing.T) {
	initPrometheusMetrics()

	store := new(utxostore.MockUtxostore)
	bc := &blockchain.Mock{}
	bc.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{0}, nil)

	// A non-nil iterator is returned alongside the error so the mock's unchecked
	// type-assertion doesn't panic; loadUnminedTransactions must ignore it because
	// err != nil.
	iter := new(utxostore.MockUnminedTxIterator)
	store.On("GetUnminedTxIterator").
		Return(iter, errors.NewProcessingError("utxo store unavailable"))

	ba := newRecoveryTestAssembler(t, store, bc)

	err := ba.loadUnminedTransactions(context.Background())
	require.Error(t, err, "BA-STARTUP-004: iterator construction failure must abort recovery")
	require.ErrorContains(t, err, "error getting unmined tx iterator")

	require.False(t, ba.unminedTransactionsLoading.Load(),
		"loading flag must be cleared after a failed load so the next start retries clean (BA-STARTUP-005)")
	assertNoRecoveryMutation(t, store)
}

// BA-STARTUP-004 — mid-iteration failure. The iterator constructs cleanly but
// fails partway through scanning; recovery MUST abort rather than proceed with a
// partial transaction set.
func TestStartup_RefusesOnMidIterationFailure(t *testing.T) {
	initPrometheusMetrics()

	store := new(utxostore.MockUtxostore)
	bc := &blockchain.Mock{}
	bc.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{0}, nil)

	iter := new(utxostore.MockUnminedTxIterator)
	// First page fails — a store error surfacing after the iterator was created.
	iter.On("Next", mock.Anything).
		Return([]*utxostore.UnminedTransaction(nil), errors.NewProcessingError("aerospike scan interrupted"))
	iter.On("Close").Return(nil).Maybe()
	store.On("GetUnminedTxIterator").Return(iter, nil)

	ba := newRecoveryTestAssembler(t, store, bc)

	err := ba.loadUnminedTransactions(context.Background())
	require.Error(t, err, "BA-STARTUP-004: a mid-iteration store failure must abort recovery")
	require.ErrorContains(t, err, "error getting unmined transaction")

	require.False(t, ba.unminedTransactionsLoading.Load(), "loading flag must be cleared after a failed load")
	assertNoRecoveryMutation(t, store)
}

// BA-STARTUP-004 — chain-reconciliation prerequisite failure. Recovery needs the
// current best-chain header-ID set to classify already-mined transactions; if
// that read fails, recovery MUST abort BEFORE requesting the iterator (so the
// UTXO store is never even touched).
func TestStartup_RefusesWhenBestChainHeaderIDsUnavailable(t *testing.T) {
	initPrometheusMetrics()

	store := new(utxostore.MockUtxostore)
	bc := &blockchain.Mock{}
	bc.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32(nil), errors.NewProcessingError("blockchain store unreachable"))

	ba := newRecoveryTestAssembler(t, store, bc)

	err := ba.loadUnminedTransactions(context.Background())
	require.Error(t, err, "BA-STARTUP-004: inability to read the best-chain header IDs must abort recovery")
	require.ErrorContains(t, err, "error getting best block headers")

	require.False(t, ba.unminedTransactionsLoading.Load(), "loading flag must be cleared after a failed load")
	// The iterator must never be requested when the reconciliation prerequisite failed.
	store.AssertNotCalled(t, "GetUnminedTxIterator")
	assertNoRecoveryMutation(t, store)
}

// BA-STARTUP-004 — post-load reconciliation failure. The scan completes, but the
// data-integrity fix-up (marking transactions that are on the best chain yet
// still flagged unmined) fails. Recovery MUST surface that error and refuse to
// start rather than resume with half-reconciled UTXO state.
func TestStartup_RefusesWhenPostLoadReconciliationFails(t *testing.T) {
	initPrometheusMetrics()

	const bestBlockID = uint32(7)

	store := new(utxostore.MockUtxostore)
	bc := &blockchain.Mock{}
	bc.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{bestBlockID}, nil)

	// One transaction that is already on the best chain (BlockIDs intersects the
	// best-chain set) yet still carries unmined_since>0 — exactly the
	// inconsistency loadUnminedTransactions repairs via MarkTransactionsOnLongestChain.
	inconsistentHash := chainhash.HashH([]byte("startup-004-inconsistent"))
	page := []*utxostore.UnminedTransaction{{
		Node:         &subtreepkg.Node{Hash: inconsistentHash, Fee: 10, SizeInBytes: 100},
		BlockIDs:     []uint32{bestBlockID},
		UnminedSince: 123,
	}}

	iter := new(utxostore.MockUnminedTxIterator)
	iter.On("Next", mock.Anything).Return(page, nil).Once()
	iter.On("Next", mock.Anything).Return([]*utxostore.UnminedTransaction(nil), nil).Once()
	iter.On("Close").Return(nil).Maybe()
	store.On("GetUnminedTxIterator").Return(iter, nil)

	// The reconciliation write fails.
	store.On("MarkTransactionsOnLongestChain", mock.Anything, mock.Anything, true).
		Return(errors.NewProcessingError("utxo store write rejected"))

	ba := newRecoveryTestAssembler(t, store, bc)

	err := ba.loadUnminedTransactions(context.Background())
	require.Error(t, err, "BA-STARTUP-004: a post-load reconciliation write failure must abort recovery")
	require.ErrorContains(t, err, "error marking transactions as mined on longest chain")

	require.False(t, ba.unminedTransactionsLoading.Load(), "loading flag must be cleared after a failed load")
}

// BA-STARTUP-004 + issue #980 genesis guard — corrupt persisted checkpoint on a
// live chain. When a BlockAssembler checkpoint exists but is unreadable
// (truncated/corrupt), initState treats it as "no usable state" and falls back
// to the chain tip. On a chain past genesis this fallback would skip
// per-block coinbase-UTXO creation, so the node MUST refuse to start. This drives
// the DecodeState-error arm of initState — distinct from the ErrNoRows arm
// already covered by TestBlockAssembly_Start's "no state but non-genesis" case.
func TestStartup_RefusesOnCorruptCheckpointAtLiveHeight(t *testing.T) {
	initPrometheusMetrics()

	// Valid 4-byte height prefix followed by a truncated (non-80-byte) header —
	// DecodeState parses the height then fails on the header, so GetState returns
	// a non-NotFound error and initState cannot adopt this checkpoint.
	corruptState := make([]byte, 4+40)
	corruptState[0] = 0x05 // arbitrary non-zero height in the prefix

	tipHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  model.GenesisBlockHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1_700_000_000,
		Bits:           model.NBit{0xff, 0xff, 0x7f, 0x20},
		Nonce:          1,
	}

	bc := &blockchain.Mock{}
	bc.On("GetState", mock.Anything, mock.Anything).Return(corruptState, nil)
	// Chain is well past genesis — adopting the tip here would corrupt the UTXO set.
	bc.On("GetBestBlockHeader", mock.Anything).
		Return(tipHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	// SetState should never be reached on the refuse path; leave it unmocked so a
	// stray call panics the test.

	ba := newRecoveryTestAssembler(t, new(utxostore.MockUtxostore), bc)
	// Clear the genesis anchor the helper set, so initState must derive state from
	// the (corrupt) checkpoint / tip rather than a pre-seeded best block.
	ba.bestBlock.Store(&BestBlockInfo{})

	err := ba.initState(context.Background())
	require.Error(t, err, "BA-STARTUP-004/#980: a corrupt checkpoint on a live chain must refuse to start")
	require.ErrorContains(t, err, "refusing to start")
}
