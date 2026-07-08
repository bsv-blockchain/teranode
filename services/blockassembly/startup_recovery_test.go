// Package blockassembly startup & recovery tests.
//
// These tests close coverage gaps for the BA-STARTUP contract:
//
//   - BA-STARTUP-008: CheckBlockAssemblyValidateInputs read-only check —
//     including the guarantee that detecting an invalid tx does NOT mutate
//     state, and that already-mined and skipped txs are excluded from the count.
//   - BA-STARTUP-003 / BA-STARTUP-011: the unmined-loading guard on the
//     ResetBlockAssemblyValidateInputs and CheckBlockAssemblyValidateInputs
//     endpoints (the two guarded operations not exercised in server_test.go).
//   - BA-STARTUP-007: validateUnminedTxInputs marks a transaction conflicting
//     and cascades eviction of its descendants. Both corrupted-state
//     conditions ((a) input spent by a different tx, (b) input spent by self
//     but a counter-conflicting tx is confirmed on chain) are covered.
package blockassembly

import (
	"context"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// hashPtrMatcher matches a *chainhash.Hash argument equal to h.
func hashPtrMatcher(h chainhash.Hash) interface{} {
	want := h
	return mock.MatchedBy(func(p *chainhash.Hash) bool {
		return p != nil && p.IsEqual(&want)
	})
}

// fieldsContain matches a []fields.FieldName argument that contains want.
func fieldsContain(want fields.FieldName) interface{} {
	return mock.MatchedBy(func(fs []fields.FieldName) bool {
		for _, f := range fs {
			if f == want {
				return true
			}
		}
		return false
	})
}

// txSpending builds a minimal transaction with a single input spending parent:0.
func txSpending(parent chainhash.Hash) *bt.Tx {
	tx := bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: 0}
	_ = in.PreviousTxIDAdd(&parent)
	tx.Inputs = []*bt.Input{in}
	return tx
}

// ----------------------------------------------------------------------------
// BA-STARTUP-008 — CheckBlockAssemblyValidateInputs / CheckInputValidation
// ----------------------------------------------------------------------------

// CheckInputValidation is the read-only scan behind the
// CheckBlockAssemblyValidateInputs gRPC endpoint. It must report the number of
// unmined transactions whose inputs no longer validate, without mutating state.
func TestBlockAssembly_CheckInputValidation(t *testing.T) {
	initPrometheusMetrics()

	t.Run("no unmined transactions yields zero invalid", func(t *testing.T) {
		mockStore := new(utxostore.MockUtxostore)
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{0}, nil)

		iter := new(utxostore.MockUnminedTxIterator)
		iter.On("Next", mock.Anything).Return(nil, nil).Once()
		iter.On("Close").Return(nil)
		mockStore.On("GetUnminedTxIterator").Return(iter, nil)

		ba := &BlockAssembler{
			logger:           ulogger.TestLogger{},
			settings:         createTestSettings(t),
			utxoStore:        mockStore,
			blockchainClient: mockBC,
		}
		ba.setBestBlockHeader(model.GenesisBlockHeader, 0)

		invalid, err := ba.CheckInputValidation(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 0, invalid, "a clean unmined set must report zero invalid transactions")

		mockStore.AssertExpectations(t)
		iter.AssertExpectations(t)
	})

	t.Run("transaction with unreadable inputs counts as invalid", func(t *testing.T) {
		mockStore := new(utxostore.MockUtxostore)
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{0}, nil)

		txHash := chainhash.HashH([]byte("ba-startup-008-invalid"))
		unmined := []*utxostore.UnminedTransaction{{
			Node: &subtreepkg.Node{Hash: txHash, Fee: 100, SizeInBytes: 250},
		}}

		iter := new(utxostore.MockUnminedTxIterator)
		iter.On("Next", mock.Anything).Return(unmined, nil).Once()
		iter.On("Next", mock.Anything).Return(nil, nil).Once()
		iter.On("Close").Return(nil)
		mockStore.On("GetUnminedTxIterator").Return(iter, nil)

		// validateUnminedTxInputs(dryRun=true) bails out (invalid) when the tx
		// metadata cannot be read.
		mockStore.On("Get", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.NewProcessingError("simulated metadata read failure"))

		ba := &BlockAssembler{
			logger:           ulogger.TestLogger{},
			settings:         createTestSettings(t),
			utxoStore:        mockStore,
			blockchainClient: mockBC,
		}
		ba.setBestBlockHeader(model.GenesisBlockHeader, 0)

		invalid, err := ba.CheckInputValidation(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 1, invalid, "an unmined tx with unverifiable inputs must be counted invalid")
	})

	t.Run("genuinely conflicting tx is counted invalid without mutating state", func(t *testing.T) {
		// BA-STARTUP-008: the read-only check runs the SAME corrupted-state
		// detection as BA-STARTUP-007(a) — input spent by a different tx — but
		// MUST NOT mark the tx conflicting or evict anything. This is the
		// guarantee the "unreadable inputs" case above cannot prove, because
		// that path bails out before reaching any mutating branch.
		parentHash := chainhash.HashH([]byte("ba-startup-008-parent"))
		txHash := chainhash.HashH([]byte("ba-startup-008-loser"))
		winnerHash := chainhash.HashH([]byte("ba-startup-008-winner"))

		mockStore := new(utxostore.MockUtxostore)
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{0}, nil)

		unmined := []*utxostore.UnminedTransaction{{
			Node: &subtreepkg.Node{Hash: txHash, Fee: 100, SizeInBytes: 250},
		}}
		iter := new(utxostore.MockUnminedTxIterator)
		iter.On("Next", mock.Anything).Return(unmined, nil).Once()
		iter.On("Next", mock.Anything).Return(nil, nil).Once()
		iter.On("Close").Return(nil)
		mockStore.On("GetUnminedTxIterator").Return(iter, nil)

		// T spends parent:0, not yet conflicting.
		mockStore.On("Get", mock.Anything, hashPtrMatcher(txHash), mock.Anything).
			Return(&meta.Data{Tx: txSpending(parentHash), Conflicting: false}, nil)
		// parent:0 is recorded as spent by a different ("winner") tx.
		mockStore.On("Get", mock.Anything, hashPtrMatcher(parentHash), mock.Anything).
			Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&winnerHash, 0)}}, nil)

		// No SetConflicting expectation is registered: testify will fail the
		// test if the read-only check attempts to mutate. The subtree processor
		// is likewise asserted untouched below.
		mockStp := &subtreeprocessor.MockSubtreeProcessor{}

		ba := &BlockAssembler{
			logger:           ulogger.TestLogger{},
			settings:         createTestSettings(t),
			utxoStore:        mockStore,
			blockchainClient: mockBC,
			subtreeProcessor: mockStp,
		}
		ba.setBestBlockHeader(model.GenesisBlockHeader, 0)

		invalid, err := ba.CheckInputValidation(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 1, invalid, "a genuinely conflicting unmined tx must be counted invalid")

		mockStore.AssertExpectations(t)
		mockStore.AssertNotCalled(t, "SetConflicting", mock.Anything, mock.Anything, mock.Anything)
		mockStp.AssertNotCalled(t, "Remove", mock.Anything, mock.Anything)
	})

	t.Run("skipped and already-mined transactions are excluded from the count", func(t *testing.T) {
		// BA-STARTUP-008: only genuinely-unmined transactions are validated.
		// A tx flagged Skip, or one whose BlockIDs intersect the current chain,
		// MUST be excluded before validation runs. No per-tx Get expectations are
		// registered, so any attempt to validate either tx would panic the mock.
		const bestBlockID = uint32(7)

		skippedHash := chainhash.HashH([]byte("ba-startup-008-skipped"))
		minedHash := chainhash.HashH([]byte("ba-startup-008-mined"))

		mockStore := new(utxostore.MockUtxostore)
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{bestBlockID}, nil)

		unmined := []*utxostore.UnminedTransaction{
			{Node: &subtreepkg.Node{Hash: skippedHash}, Skip: true},
			{Node: &subtreepkg.Node{Hash: minedHash}, BlockIDs: []uint32{bestBlockID}},
		}
		iter := new(utxostore.MockUnminedTxIterator)
		iter.On("Next", mock.Anything).Return(unmined, nil).Once()
		iter.On("Next", mock.Anything).Return(nil, nil).Once()
		iter.On("Close").Return(nil)
		mockStore.On("GetUnminedTxIterator").Return(iter, nil)

		ba := &BlockAssembler{
			logger:           ulogger.TestLogger{},
			settings:         createTestSettings(t),
			utxoStore:        mockStore,
			blockchainClient: mockBC,
		}
		ba.setBestBlockHeader(model.GenesisBlockHeader, 0)

		invalid, err := ba.CheckInputValidation(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 0, invalid, "skipped and already-mined txs must not be validated or counted")

		mockStore.AssertExpectations(t)
		mockStore.AssertNotCalled(t, "Get", mock.Anything, mock.Anything, mock.Anything)
	})
}

// CheckBlockAssemblyValidateInputs surfaces the invalid count as a gRPC error
// and is gated by the unmined-loading guard (BA-STARTUP-003 / BA-STARTUP-011).
func TestBlockAssembly_CheckBlockAssemblyValidateInputs_GRPC(t *testing.T) {
	t.Run("clean state returns ok", func(t *testing.T) {
		server, _ := setupServer(t)
		require.NoError(t, server.blockAssembler.Start(t.Context()))

		_, err := server.CheckBlockAssemblyValidateInputs(t.Context(), &blockassembly_api.EmptyMessage{})
		require.NoError(t, err, "BA-STARTUP-008: a clean unmined set must validate without error")
	})

	t.Run("rejected while unmined transactions are loading", func(t *testing.T) {
		server, _ := setupServer(t)
		server.blockAssembler.unminedTransactionsLoading.Store(true)

		_, err := server.CheckBlockAssemblyValidateInputs(t.Context(), &blockassembly_api.EmptyMessage{})
		require.Error(t, err, "BA-STARTUP-011: CheckBlockAssemblyValidateInputs must reject while loading")
		require.Contains(t, err.Error(),
			"service not ready - unmined transactions are still being loaded")
	})
}

// ResetBlockAssemblyValidateInputs is the third reset variant and is likewise
// gated by the unmined-loading guard (BA-STARTUP-003 / BA-STARTUP-011).
func TestBlockAssembly_ResetBlockAssemblyValidateInputs_LoadingGuard(t *testing.T) {
	server, _ := setupServer(t)
	server.blockAssembler.unminedTransactionsLoading.Store(true)

	_, err := server.ResetBlockAssemblyValidateInputs(t.Context(), &blockassembly_api.EmptyMessage{})
	require.Error(t, err, "BA-STARTUP-011: ResetBlockAssemblyValidateInputs must reject while loading")
	require.Contains(t, err.Error(),
		"service not ready - unmined transactions are still being loaded")
}

// ----------------------------------------------------------------------------
// BA-STARTUP-007 — validateUnminedTxInputs marks conflicting + cascades
// ----------------------------------------------------------------------------

// Case (a): an unmined transaction whose input is recorded in the UTXO store as
// spent by a DIFFERENT transaction must be marked conflicting, and the marking
// must cascade to its descendants, evicting each cascaded tx from the subtree
// processor.
func TestBlockAssembly_ValidateUnminedTxInputs_CaseA_SpentByDifferentTxCascades(t *testing.T) {
	ctx := context.Background()

	parentHash := chainhash.HashH([]byte("case-a-parent"))
	winnerHash := chainhash.HashH([]byte("case-a-winner"))
	txHash := chainhash.HashH([]byte("case-a-loser"))
	childHash := chainhash.HashH([]byte("case-a-child"))
	grandchildHash := chainhash.HashH([]byte("case-a-grandchild"))

	mockStore := new(utxostore.MockUtxostore)
	// T metadata: one input spending parentHash:0, not yet conflicting.
	mockStore.On("Get", mock.Anything, hashPtrMatcher(txHash), mock.Anything).
		Return(&meta.Data{Tx: txSpending(parentHash), Conflicting: false}, nil)
	// parent:0 is spent by a different ("winner") tx.
	mockStore.On("Get", mock.Anything, hashPtrMatcher(parentHash), mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&winnerHash, 0)}}, nil)
	// MarkConflictingRecursively BFS: T -> child -> grandchild -> (none).
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{txHash}, true).
		Return([]*utxostore.Spend{{TxID: &txHash, Vout: 0}}, []chainhash.Hash{childHash}, nil)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{childHash}, true).
		Return([]*utxostore.Spend{{TxID: &childHash, Vout: 0}}, []chainhash.Hash{grandchildHash}, nil)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{grandchildHash}, true).
		Return([]*utxostore.Spend{{TxID: &grandchildHash, Vout: 0}}, []chainhash.Hash{}, nil)

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("Remove", mock.Anything, txHash).Return(nil).Once()
	mockStp.On("Remove", mock.Anything, childHash).Return(nil).Once()
	mockStp.On("Remove", mock.Anything, grandchildHash).Return(nil).Once()

	ba := &BlockAssembler{
		logger:           ulogger.TestLogger{},
		settings:         createTestSettings(t),
		utxoStore:        mockStore,
		subtreeProcessor: mockStp,
	}

	ok := ba.validateUnminedTxInputs(ctx, txHash, map[uint32]bool{0: true}, false)
	assert.False(t, ok, "BA-STARTUP-007(a): tx whose input is spent by another tx must be invalid")

	mockStore.AssertExpectations(t)
	mockStp.AssertExpectations(t)
	mockStp.AssertNumberOfCalls(t, "Remove", 3)
}

// Case (b): an unmined transaction whose input is spent by ITSELF, but whose
// parent has a counter-conflicting child confirmed on the current chain, must
// also be marked conflicting.
func TestBlockAssembly_ValidateUnminedTxInputs_CaseB_CounterConflictingOnChain(t *testing.T) {
	ctx := context.Background()

	parentHash := chainhash.HashH([]byte("case-b-parent"))
	txHash := chainhash.HashH([]byte("case-b-tx"))
	counterHash := chainhash.HashH([]byte("case-b-counter"))
	const confirmedBlockID = uint32(42)

	mockStore := new(utxostore.MockUtxostore)
	// T metadata.
	mockStore.On("Get", mock.Anything, hashPtrMatcher(txHash), mock.Anything).
		Return(&meta.Data{Tx: txSpending(parentHash), Conflicting: false}, nil)
	// parent:0 is spent by T itself (matches) -> fall through to case 2.
	mockStore.On("Get", mock.Anything, hashPtrMatcher(parentHash), fieldsContain(fields.Utxos)).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&txHash, 0)}}, nil)
	// parent has a counter-conflicting child (not T).
	mockStore.On("Get", mock.Anything, hashPtrMatcher(parentHash), fieldsContain(fields.ConflictingChildren)).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{counterHash}}, nil)
	// the counter tx is confirmed on the current chain.
	mockStore.On("Get", mock.Anything, hashPtrMatcher(counterHash), mock.Anything).
		Return(&meta.Data{BlockIDs: []uint32{confirmedBlockID}}, nil)
	// markAsConflicting(T): no descendants.
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{txHash}, true).
		Return([]*utxostore.Spend{{TxID: &txHash, Vout: 0}}, []chainhash.Hash{}, nil)

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("Remove", mock.Anything, txHash).Return(nil).Once()

	ba := &BlockAssembler{
		logger:           ulogger.TestLogger{},
		settings:         createTestSettings(t),
		utxoStore:        mockStore,
		subtreeProcessor: mockStp,
	}

	ok := ba.validateUnminedTxInputs(ctx, txHash, map[uint32]bool{confirmedBlockID: true}, false)
	assert.False(t, ok, "BA-STARTUP-007(b): tx must be invalid when a counter-conflicting tx is confirmed on chain")

	mockStore.AssertExpectations(t)
	mockStp.AssertExpectations(t)
}
