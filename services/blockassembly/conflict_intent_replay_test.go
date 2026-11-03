package blockassembly

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofields "github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// blockchainMembershipMock returns a blockchain client mock that reports every
// block as on/off the longest chain per onChain. notFound makes GetBlockHeader
// report the block as unknown (treated as off-chain).
func blockchainMembershipMock(onChain, notFound bool) *blockchain.Mock {
	m := &blockchain.Mock{}
	if notFound {
		m.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), errors.NewBlockNotFoundError("unknown block"))
		return m
	}

	m.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{ID: 7}, nil)
	m.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(onChain, nil)

	return m
}

// replaySpyStore embeds the utxo mock and records WAL lifecycle calls so the
// BlockAssembler replay path can be asserted without a full store backend.
type replaySpyStore struct {
	*utxo.MockUtxostore

	pending    []utxo.ConflictIntent
	pendingErr error
	completed  []chainhash.Hash
	// drainOnReRead makes the post-replay re-read of the WAL (the call that
	// updates the pending gauge) return empty, modelling a store that actually
	// removed the intents that were completed during the drain loop.
	drainOnReRead bool
	pendingCalls  int
}

func (s *replaySpyStore) PendingConflictIntents(_ context.Context) ([]utxo.ConflictIntent, error) {
	s.pendingCalls++
	if s.drainOnReRead && s.pendingCalls > 1 {
		return nil, s.pendingErr
	}

	return s.pending, s.pendingErr
}

func (s *replaySpyStore) BeginConflictIntent(_ context.Context, _ utxo.ConflictIntent) error {
	return nil
}

func (s *replaySpyStore) CompleteConflictIntent(_ context.Context, intentID chainhash.Hash) error {
	s.completed = append(s.completed, intentID)
	return nil
}

// newReplayTestAssembler builds the minimal BlockAssembler needed to drive
// replayPendingConflictIntents — it touches utxoStore, blockchainClient (for
// the chain-membership gate) and logger.
func newReplayTestAssembler(store utxo.Store, bc blockchain.ClientI) *BlockAssembler {
	// Production initialises metrics in Server.New; do the same so the replay
	// path's gauge/counter are non-nil.
	initPrometheusMetrics()

	return &BlockAssembler{
		logger:           ulogger.TestLogger{},
		utxoStore:        store,
		blockchainClient: bc,
	}
}

// TestReplayPendingConflictIntents_NoPending is a no-op when the WAL is empty.
func TestReplayPendingConflictIntents_NoPending(t *testing.T) {
	spy := &replaySpyStore{MockUtxostore: &utxo.MockUtxostore{}}
	b := newReplayTestAssembler(spy, blockchainMembershipMock(false, true))

	b.replayPendingConflictIntents(context.Background())

	require.Empty(t, spy.completed, "nothing to complete when no intents are pending")
}

// TestReplayPendingConflictIntents_PendingGaugeReflectsPostReplay verifies the
// conflict_intents_pending gauge is updated to the post-replay WAL count (so an
// alert clears once recovery succeeds) rather than latching at the startup count.
func TestReplayPendingConflictIntents_PendingGaugeReflectsPostReplay(t *testing.T) {
	demoted := chainhash.HashH([]byte("gauge-demoted"))

	intent := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentReverse,
		BlockHeight: 500,
		TxHashes:    []chainhash.Hash{demoted},
		StartedAt:   1,
	}

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		for _, item := range args.Get(1).([]*utxo.UnresolvedMetaData) {
			item.Data = &meta.Data{}
		}
	}).Return(nil).Maybe()
	// Demoted tx resolves with a nil body → ReverseProcessConflicting is a no-op
	// success → intent completed.
	mockStore.On("Get", mock.Anything, &demoted, mock.Anything).Return(&meta.Data{}, nil)

	// First read returns the pending intent; the post-replay re-read returns empty
	// (the store removed the completed intent).
	spy := &replaySpyStore{MockUtxostore: mockStore, pending: []utxo.ConflictIntent{intent}, drainOnReRead: true}
	b := newReplayTestAssembler(spy, blockchainMembershipMock(false, true))

	// Sentinel so we can prove the gauge was actively updated.
	prometheusBlockAssemblerConflictIntentsPending.Set(99)

	b.replayPendingConflictIntents(context.Background())

	require.Equal(t, float64(0), testutil.ToFloat64(prometheusBlockAssemblerConflictIntentsPending),
		"pending gauge must reflect the drained WAL after a successful replay, not the startup count")
}

// TestReplayPendingConflictIntents_LoadErrorIsNotFatal verifies a failure to
// read the WAL is logged/counted but does not panic or block startup.
func TestReplayPendingConflictIntents_LoadErrorIsNotFatal(t *testing.T) {
	spy := &replaySpyStore{
		MockUtxostore: &utxo.MockUtxostore{},
		pendingErr:    errors.NewStorageError("wal read failed"),
	}
	b := newReplayTestAssembler(spy, blockchainMembershipMock(false, true))

	require.NotPanics(t, func() {
		b.replayPendingConflictIntents(context.Background())
	})
	require.Empty(t, spy.completed)
}

// TestReplayPendingConflictIntents_ReverseReplayCompletes drives a reverse
// intent through replay. The demoted tx resolves to a record with a nil body,
// so ReverseProcessConflicting treats it as already-resolved (a no-op success)
// and the intent is cleared from the WAL.
func TestReplayPendingConflictIntents_ReverseReplayCompletes(t *testing.T) {
	demoted := chainhash.HashH([]byte("replay-demoted"))

	intent := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentReverse,
		BlockHeight: 500,
		TxHashes:    []chainhash.Hash{demoted},
		StartedAt:   1,
	}

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		for _, item := range args.Get(1).([]*utxo.UnresolvedMetaData) {
			item.Data = &meta.Data{}
		}
	}).Return(nil).Maybe()
	// Demoted tx resolves with a nil body → ReverseProcessConflicting skips it
	// (nothing to restore) and returns success.
	mockStore.On("Get", mock.Anything, &demoted, mock.Anything).
		Return(&meta.Data{}, nil)

	spy := &replaySpyStore{MockUtxostore: mockStore, pending: []utxo.ConflictIntent{intent}}
	// Block is unknown/off-chain → reverse intent is NOT stale → it replays.
	b := newReplayTestAssembler(spy, blockchainMembershipMock(false, true))

	b.replayPendingConflictIntents(context.Background())

	require.Contains(t, spy.completed, intent.IntentID(), "successful replay must clear the intent from the WAL")
}

// Legacy WALs do not identify lock owners. Recovery must preserve the lock
// and the WAL so an operator can inspect the original state.
func TestReplayPendingConflictIntents_StaleLegacyForwardRequiresRepair(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	parent := bt.NewTx()
	parentIn := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 200000, SequenceNumber: 0xFFFFFFFF, UnlockingScript: bscript.NewFromBytes([]byte{})}
	_ = parentIn.PreviousTxIDAdd(&chainhash.Hash{7, 7, 7})
	parent.Inputs = []*bt.Input{parentIn}
	parent.Outputs = []*bt.Output{{Satoshis: 100000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	parentHash := parent.TxIDChainHash()
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentHash}, true))

	// L: the counter, already the canonical (non-conflicting) spender of parent[0].
	txL := bt.NewTx()
	require.NoError(t, txL.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	txL.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txL.Outputs = []*bt.Output{{Satoshis: 90000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err = store.SpendAndCreate(ctx, txL, 10, utxo.WithCreateOnly())
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(ctx, txL, store.GetBlockHeight()+1, utxo.WithSpendOnly())
	require.NoError(t, err)

	// W: the would-be forward winner, left Conflicting=true.
	txW := bt.NewTx()
	require.NoError(t, txW.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	txW.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txW.Outputs = []*bt.Output{{Satoshis: 80000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err = store.SpendAndCreate(ctx, txW, 10, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)
	txWHash := txW.TxIDChainHash()

	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*parentHash}, true))
	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 10, BlockHash: chainhash.HashH([]byte("reorged-out-block")), TxHashes: []chainhash.Hash{*txWHash}, StartedAt: 1}
	require.NoError(t, store.BeginConflictIntent(ctx, intent))
	// The real blockchain store has no such block, so the forward is stale.
	replayFailed := items.blockAssembler.replayPendingConflictIntents(ctx)
	m, err := store.Get(ctx, parentHash, utxofields.Locked)
	require.NoError(t, err)
	require.True(t, m.Locked)
	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, intent.IntentID(), pending[0].IntentID())
	require.ErrorContains(t, items.blockAssembler.checkConflictRecoveryReady(ctx, replayFailed), "failed to replay")
}

// A stale reverse uses the real forward write path. Its new forward WAL must
// remain until BlockAssembly acknowledges the subtree and queue updates.
func TestHealStaleConflictIntent_ReverseRealStore(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	parent := bt.NewTx()
	parentIn := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 200000, SequenceNumber: 0xFFFFFFFF, UnlockingScript: bscript.NewFromBytes([]byte{})}
	_ = parentIn.PreviousTxIDAdd(&chainhash.Hash{7, 7, 7})
	parent.Inputs = []*bt.Input{parentIn}
	parent.Outputs = []*bt.Output{{Satoshis: 100000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	parentHash := parent.TxIDChainHash()
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentHash}, true))

	// L: the counter, already the canonical (non-conflicting) spender of parent[0].
	txL := bt.NewTx()
	require.NoError(t, txL.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	txL.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txL.Outputs = []*bt.Output{{Satoshis: 90000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err = store.SpendAndCreate(ctx, txL, 10, utxo.WithCreateOnly())
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(ctx, txL, store.GetBlockHeight()+1, utxo.WithSpendOnly())
	require.NoError(t, err)

	// W: the would-be forward winner, left Conflicting=true.
	txW := bt.NewTx()
	require.NoError(t, txW.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	txW.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txW.Outputs = []*bt.Output{{Satoshis: 80000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err = store.SpendAndCreate(ctx, txW, 10, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)
	txWHash := txW.TxIDChainHash()

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentReverse, BlockHeight: 10, BlockHash: chainhash.HashH([]byte("re-applied-block")), TxHashes: []chainhash.Hash{*txWHash}, StartedAt: 1}
	require.NoError(t, items.blockAssembler.healStaleConflictIntent(ctx, intent))
	m, err := store.Get(ctx, txWHash, utxofields.Conflicting)
	require.NoError(t, err)
	require.False(t, m.Conflicting)
	spends, err := utxo.GetSpends(txW)
	require.NoError(t, err)
	actual, err := store.GetSpend(ctx, spends[0])
	require.NoError(t, err)
	require.Equal(t, txWHash, actual.SpendingData.TxID)
	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, utxo.ConflictIntentForward, pending[0].Kind)
	require.Nil(t, pending[0].Recovery, "the pending scan must not load checklists")

	recovery, err := store.LoadConflictRecovery(ctx, pending[0].IntentID())
	require.NoError(t, err)
	require.Equal(t, utxo.ConflictRecoveryFinished, recovery.Phase)
}

// createStaleReverseParent stores a mined parent with the given number of outputs.
func createStaleReverseParent(ctx context.Context, t *testing.T, store utxo.Store, outputs int) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	parentIn := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 200000, SequenceNumber: 0xFFFFFFFF, UnlockingScript: bscript.NewFromBytes([]byte{})}
	_ = parentIn.PreviousTxIDAdd(&chainhash.Hash{7, 7, 7})
	parent.Inputs = []*bt.Input{parentIn}

	for i := 0; i < outputs; i++ {
		parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 100000, LockingScript: bscript.NewFromBytes([]byte{0x52})})
	}

	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))

	return parent
}

// createStaleReverseSpender stores a transaction that spends parent[vout]. An owner also takes the spend.
func createStaleReverseSpender(ctx context.Context, t *testing.T, store utxo.Store, parent *bt.Tx, vout uint32, satoshis uint64, owner bool) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From(parent.TxIDChainHash().String(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	tx.Outputs = []*bt.Output{{Satoshis: satoshis, LockingScript: bscript.NewFromBytes([]byte{0x52})}}

	_, _, err := store.SpendAndCreate(ctx, tx, 10, utxo.WithConflicting(!owner), utxo.WithCreateOnly())
	require.NoError(t, err)

	if owner {
		_, _, err = store.SpendAndCreate(ctx, tx, store.GetBlockHeight()+1, utxo.WithSpendOnly())
		require.NoError(t, err)
	}

	return tx
}

func requireConflictOwner(ctx context.Context, t *testing.T, store utxo.Store, owner *bt.Tx, conflicting ...*bt.Tx) {
	t.Helper()

	m, err := store.Get(ctx, owner.TxIDChainHash(), utxofields.Conflicting)
	require.NoError(t, err)
	require.False(t, m.Conflicting)

	spends, err := utxo.GetSpends(owner)
	require.NoError(t, err)

	actual, err := store.GetSpend(ctx, spends[0])
	require.NoError(t, err)
	require.Equal(t, owner.TxIDChainHash(), actual.SpendingData.TxID)

	for _, tx := range conflicting {
		m, err = store.Get(ctx, tx.TxIDChainHash(), utxofields.Conflicting)
		require.NoError(t, err)
		require.True(t, m.Conflicting)
	}
}

// A reverse can crash after its WAL record and before its first step. If the block is back on the
// longest chain at restart, the winner still owns its input and is not conflicting, and no forward
// checklist exists. The heal must accept that applied state, clear the WAL and let block assembly start.
func TestReplayPendingConflictIntents_StaleReverseNothingApplied(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	parent := createStaleReverseParent(ctx, t, store, 1)
	txL := createStaleReverseSpender(ctx, t, store, parent, 0, 90000, false)
	txW := createStaleReverseSpender(ctx, t, store, parent, 0, 80000, true)

	addBlockWithMinedSet(ctx, t, items, blockHeader1)

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentReverse, BlockHeight: 1, BlockHash: *blockHeader1.Hash(), TxHashes: []chainhash.Hash{*txW.TxIDChainHash()}, StartedAt: 1}
	require.NoError(t, store.BeginConflictIntent(ctx, intent))

	replayFailed := items.blockAssembler.replayPendingConflictIntents(ctx)
	require.Empty(t, replayFailed)

	require.NoError(t, items.blockAssembler.checkConflictRecoveryReady(ctx, replayFailed))

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending)

	requireConflictOwner(ctx, t, store, txW, txL)
}

// A partial reverse flipped one winner back and left the other applied. The heal must re-apply the
// flipped winner and leave the applied winner as it is.
func TestHealStaleConflictIntent_ReversePartiallyApplied(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	parent := createStaleReverseParent(ctx, t, store, 2)

	// Output 0: the reverse did not reach W1. W1 still owns the spend and L1 is conflicting.
	txL1 := createStaleReverseSpender(ctx, t, store, parent, 0, 90000, false)
	txW1 := createStaleReverseSpender(ctx, t, store, parent, 0, 80000, true)

	// Output 1: the reverse finished for W2. L2 owns the spend again and W2 is conflicting.
	txL2 := createStaleReverseSpender(ctx, t, store, parent, 1, 90000, true)
	txW2 := createStaleReverseSpender(ctx, t, store, parent, 1, 80000, false)

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentReverse, BlockHeight: 10, BlockHash: chainhash.HashH([]byte("partially-reversed-block")),
		TxHashes: []chainhash.Hash{*txW1.TxIDChainHash(), *txW2.TxIDChainHash()}, StartedAt: 1}
	require.NoError(t, items.blockAssembler.healStaleConflictIntent(ctx, intent))

	requireConflictOwner(ctx, t, store, txW1, txL1)
	requireConflictOwner(ctx, t, store, txW2, txL2)
}

// TestReplayPendingConflictIntents_RealStoreReverseConverges is the crash-
// recovery integration test (#861) against a REAL sqlitememory store and a real
// BlockAssembler. It reproduces the most common crash window — a SIGKILL AFTER
// ReverseProcessConflicting's terminal step but BEFORE the WAL completion delete
// — by leaving a reverse intent behind for an operation whose effects are
// already fully applied. Startup replay must re-run the reverse, observe it is
// already applied (idempotent no-op via isReverseFullyApplied), clear the WAL,
// and leave the UTXO state untouched.
func TestReplayPendingConflictIntents_RealStoreReverseConverges(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	require.NotNil(t, items)

	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	// Parent tx with one spendable output (input references a nonexistent tx;
	// Create does not validate inputs, and PreviousTxSatoshis covers the fee).
	parent := bt.NewTx()
	parentIn := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 200000, SequenceNumber: 0xFFFFFFFF, UnlockingScript: bscript.NewFromBytes([]byte{})}
	_ = parentIn.PreviousTxIDAdd(&chainhash.Hash{9, 9, 9})
	parent.Inputs = []*bt.Input{parentIn}
	parent.Outputs = []*bt.Output{{Satoshis: 100000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	parentHash := parent.TxIDChainHash()
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentHash}, true))

	// txD: the demoted loser, spends parent[0], created Conflicting=true.
	txD := bt.NewTx()
	require.NoError(t, txD.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	txD.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txD.Outputs = []*bt.Output{{Satoshis: 90000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err = store.SpendAndCreate(ctx, txD, 10, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)
	txDHash := txD.TxIDChainHash()

	// txC: the counter/winner, spends parent[0] for real so parent[0]'s spending
	// data points at C — the fully-applied post-reverse state.
	txC := bt.NewTx()
	require.NoError(t, txC.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	txC.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txC.Outputs = []*bt.Output{{Satoshis: 80000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err = store.SpendAndCreate(ctx, txC, 10, utxo.WithCreateOnly())
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(ctx, txC, store.GetBlockHeight()+1, utxo.WithSpendOnly())
	require.NoError(t, err)

	// The reverse for txD is already fully applied; only the WAL completion was
	// lost to the crash. Re-record the intent to simulate that.
	intent := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentReverse,
		BlockHeight: 10,
		TxHashes:    []chainhash.Hash{*txDHash},
		StartedAt:   1,
	}
	require.NoError(t, store.BeginConflictIntent(ctx, intent))

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "intent should be pending before replay")

	// The intent's (zero) block hash is unknown to the chain → off-chain → a
	// reverse intent is not stale, so replay proceeds. Use a deterministic
	// membership mock rather than relying on the real client's not-found behaviour.
	items.blockAssembler.blockchainClient = blockchainMembershipMock(false, true)

	// Restart path.
	items.blockAssembler.replayPendingConflictIntents(ctx)

	// Converged: the WAL is clear and the UTXO state is unchanged.
	pending, err = store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending, "replay must clear the WAL once the operation has converged")

	dMeta, err := store.Get(ctx, txDHash, utxofields.Conflicting)
	require.NoError(t, err)
	require.True(t, dMeta.Conflicting, "demoted tx must remain conflicting after idempotent reverse replay")
}

// TestReplayConflictIntent_UnknownKind surfaces an error for an unrecognised
// intent kind rather than silently skipping it.
func TestReplayConflictIntent_UnknownKind(t *testing.T) {
	b := newReplayTestAssembler(&replaySpyStore{MockUtxostore: &utxo.MockUtxostore{}}, blockchainMembershipMock(false, true))

	err := b.replayConflictIntent(context.Background(), utxo.ConflictIntent{
		Kind:     utxo.ConflictIntentKind("bogus"),
		TxHashes: []chainhash.Hash{chainhash.HashH([]byte("x"))},
	})

	require.Error(t, err)
}

// The stale block and the current chain both contain the same transaction.
// An old intent must not authorize undo of its currently confirmed spend.
func TestStaleConflictRecoveryPreservesReminedWinner(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	parent := bt.NewTx()
	parentIn := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 200000, SequenceNumber: 0xFFFFFFFF, UnlockingScript: bscript.NewFromBytes([]byte{})}
	_ = parentIn.PreviousTxIDAdd(&chainhash.Hash{7, 7, 7})
	parent.Inputs = []*bt.Input{parentIn}
	parent.Outputs = []*bt.Output{{Satoshis: 100000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	parentHash := parent.TxIDChainHash()
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentHash}, true))

	// L: the counter, already the canonical (non-conflicting) spender of parent[0].
	txL := bt.NewTx()
	require.NoError(t, txL.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	txL.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txL.Outputs = []*bt.Output{{Satoshis: 90000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err = store.SpendAndCreate(ctx, txL, 10, utxo.WithCreateOnly())
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(ctx, txL, store.GetBlockHeight()+1, utxo.WithSpendOnly())
	require.NoError(t, err)

	// W: the would-be forward winner, left Conflicting=true.
	txW := bt.NewTx()
	require.NoError(t, txW.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	txW.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txW.Outputs = []*bt.Output{{Satoshis: 80000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, _, err = store.SpendAndCreate(ctx, txW, 10, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)
	txWHash := txW.TxIDChainHash()

	blockHash := chainhash.HashH([]byte("stale-side-block"))
	winners := []chainhash.Hash{*txWHash}
	_, _, err = utxo.ProcessConflicting(ctx, store, 10, blockHash, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	addBlockWithMinedSet(ctx, t, items, blockHeader1)
	side := *blockHeader1
	side.Nonce++
	addBlockWithMinedSet(ctx, t, items, &side)
	addBlockWithMinedSet(ctx, t, items, blockHeader2)
	_, sideMeta, err := items.blockchainClient.GetBlockHeader(ctx, side.Hash())
	require.NoError(t, err)
	_, mainMeta, err := items.blockchainClient.GetBlockHeader(ctx, blockHeader2.Hash())
	require.NoError(t, err)
	onChain, err := items.blockchainClient.CheckBlockIsInCurrentChain(ctx, []uint32{sideMeta.ID})
	require.NoError(t, err)
	require.False(t, onChain)
	onChain, err = items.blockchainClient.CheckBlockIsInCurrentChain(ctx, []uint32{mainMeta.ID})
	require.NoError(t, err)
	require.True(t, onChain)
	for _, id := range []uint32{sideMeta.ID, mainMeta.ID} {
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txWHash}, utxo.MinedBlockInfo{BlockID: id, BlockHeight: 2})
		require.NoError(t, err)
	}
	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 10, BlockHash: blockHash, TxHashes: winners}
	// The current chain confirms the same winner, so the forward outcome stands and the heal needs
	// no rollback. Refusing here would keep the intent pending and block every restart.
	require.NoError(t, items.blockAssembler.healStaleConflictIntent(ctx, intent))
	m, err := store.Get(ctx, txWHash, utxofields.Conflicting)
	require.NoError(t, err)
	require.False(t, m.Conflicting)
	inputs, err := utxo.GetSpends(txW)
	require.NoError(t, err)
	actual, err := store.GetSpend(ctx, inputs[0])
	require.NoError(t, err)
	require.Equal(t, txWHash, actual.SpendingData.TxID)
	r, err := store.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Nil(t, r, "the heal retires the intent")
	parentMeta, err := store.Get(ctx, parentHash, utxofields.Locked)
	require.NoError(t, err)
	require.False(t, parentMeta.Locked, "the heal must not leave the parent locked")

	// On restart nothing is left to heal, and block assembly may start.
	replayFailed := items.blockAssembler.replayPendingConflictIntents(ctx)
	require.NoError(t, items.blockAssembler.checkConflictRecoveryReady(ctx, replayFailed))
	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending)

	// The reverse path must enforce the same rule, including restart replay.
	reverse := intent
	reverse.Kind = utxo.ConflictIntentReverse
	require.ErrorContains(t, items.blockAssembler.replayConflictIntent(ctx, reverse), "confirmed on the current chain")
	m, err = store.Get(ctx, txWHash, utxofields.Conflicting)
	require.NoError(t, err)
	require.False(t, m.Conflicting)
	actual, err = store.GetSpend(ctx, inputs[0])
	require.NoError(t, err)
	require.Equal(t, txWHash, actual.SpendingData.TxID)
}

// A finished forward intent whose replay fails must stop startup. Its checklist still reads finished
// and its block is on the longest chain, which the gate alone would accept, but the store now
// contradicts the checklist.
func TestCheckConflictRecoveryReady_RefusesFailedReplay(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	parent := createStaleReverseParent(ctx, t, store, 1)
	createStaleReverseSpender(ctx, t, store, parent, 0, 90000, true)
	txW := createStaleReverseSpender(ctx, t, store, parent, 0, 80000, false)

	addBlockWithMinedSet(ctx, t, items, blockHeader1)

	winners := []chainhash.Hash{*txW.TxIDChainHash()}
	_, _, err := utxo.ProcessConflicting(ctx, store, 1, *blockHeader1.Hash(), winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	// Something took the winner's input before block assembly acknowledged the operation, and left the
	// winner not Conflicting. No reverse sets that state, so the replay cannot plan the winner again.
	// (A Conflicting winner is what a reverse that stopped part way leaves; the replay heals that.)
	winningSpends, err := utxo.GetSpends(txW)
	require.NoError(t, err)
	require.NoError(t, store.Unspend(ctx, winningSpends))

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 1, BlockHash: *blockHeader1.Hash(), TxHashes: winners}

	replayFailed := items.blockAssembler.replayPendingConflictIntents(ctx)
	require.Contains(t, replayFailed, intent.IntentID())

	require.NoError(t, items.blockAssembler.checkConflictRecoveryReady(ctx, nil), "the phase and chain checks alone accept this intent")
	require.ErrorContains(t, items.blockAssembler.checkConflictRecoveryReady(ctx, replayFailed), "failed to replay")
}

// A crash right after a reverse began leaves its intent and the finished forward intent of the same
// block. Replay must run the reverse first, which retires the forward intent, and clear both.
func TestReplayPendingConflictIntents_ReverseRetiresForward(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	parent := createStaleReverseParent(ctx, t, store, 1)
	txL := createStaleReverseSpender(ctx, t, store, parent, 0, 90000, true)
	txW := createStaleReverseSpender(ctx, t, store, parent, 0, 80000, false)

	block := chainhash.HashH([]byte("reverse-retires-forward-block"))
	winners := []chainhash.Hash{*txW.TxIDChainHash()}
	_, _, err := utxo.ProcessConflicting(ctx, store, 10, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	reverse := utxo.ConflictIntent{Kind: utxo.ConflictIntentReverse, BlockHeight: 10, BlockHash: block, TxHashes: winners, StartedAt: 1}
	require.NoError(t, store.BeginConflictIntent(ctx, reverse))

	// The block is unknown to the chain, so it is off the longest chain: the reverse is current
	// and the forward is stale.
	items.blockAssembler.blockchainClient = blockchainMembershipMock(false, true)

	replayFailed := items.blockAssembler.replayPendingConflictIntents(ctx)
	require.Empty(t, replayFailed)

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending)

	requireConflictOwner(ctx, t, store, txL, txW)
}

// A reverse that fails leaves the forward intent of the same block untouched. Rolling the forward
// back over a partial reverse could corrupt the store, so both stay pending for repair.
func TestReplayPendingConflictIntents_FailedReverseKeepsForward(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	parent := createStaleReverseParent(ctx, t, store, 1)
	createStaleReverseSpender(ctx, t, store, parent, 0, 90000, true)
	txW := createStaleReverseSpender(ctx, t, store, parent, 0, 80000, false)

	block := chainhash.HashH([]byte("failed-reverse-keeps-forward-block"))
	winners := []chainhash.Hash{*txW.TxIDChainHash()}
	_, _, err := utxo.ProcessConflicting(ctx, store, 10, block, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	child := createStaleReverseSpender(ctx, t, store, txW, 0, 70000, true)

	reverse := utxo.ConflictIntent{Kind: utxo.ConflictIntentReverse, BlockHeight: 10, BlockHash: block, TxHashes: winners, StartedAt: 1}
	require.NoError(t, store.BeginConflictIntent(ctx, reverse))

	childHash := *child.TxIDChainHash()
	items.blockAssembler.utxoStore = &evictionFaultStore{Store: store, failMarkOf: &childHash}
	items.blockAssembler.blockchainClient = blockchainMembershipMock(false, true)

	forward := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 10, BlockHash: block, TxHashes: winners}

	replayFailed := items.blockAssembler.replayPendingConflictIntents(ctx)
	require.Contains(t, replayFailed, reverse.IntentID())
	require.Contains(t, replayFailed, forward.IntentID())

	r, err := store.LoadConflictRecovery(ctx, forward.IntentID())
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, utxo.ConflictRecoveryFinished, r.Phase, "the forward checklist must not be rolled back over a failed reverse")
}

// After a crash, block assembly can resume behind the block of a finished forward intent. Startup
// must leave that intent to the catch-up and acknowledge only the intents of blocks it has reached.
// The catch-up then accepts the applied winner from the finished checklist.
func TestPrepareRecoveredConflictUpdates_WaitsForAssemblyTip(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	addBlockWithMinedSet(ctx, t, items, blockHeader1)
	addBlockWithMinedSet(ctx, t, items, blockHeader2)
	items.blockAssembler.setBestBlockHeader(blockHeader1, 1)

	parent := createStaleReverseParent(ctx, t, store, 2)
	txL1 := createStaleReverseSpender(ctx, t, store, parent, 0, 90000, true)
	txW1 := createStaleReverseSpender(ctx, t, store, parent, 0, 80000, false)
	txL2 := createStaleReverseSpender(ctx, t, store, parent, 1, 90000, true)
	txW2 := createStaleReverseSpender(ctx, t, store, parent, 1, 80000, false)

	reached := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 1, BlockHash: *blockHeader1.Hash(), TxHashes: []chainhash.Hash{*txW1.TxIDChainHash()}}
	ahead := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 2, BlockHash: *blockHeader2.Hash(), TxHashes: []chainhash.Hash{*txW2.TxIDChainHash()}}

	for _, intent := range []utxo.ConflictIntent{reached, ahead} {
		_, _, err := utxo.ProcessConflicting(ctx, store, intent.BlockHeight, intent.BlockHash, intent.TxHashes, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
		require.NoError(t, err)
	}

	items.blockAssembler.subtreeProcessor.Stop(ctx)

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("MarkRecoveredConflicts", mock.Anything, mock.Anything).Return(nil)
	mockStp.On("Stop", mock.Anything).Return()
	items.blockAssembler.subtreeProcessor = mockStp

	ids, err := items.blockAssembler.prepareRecoveredConflictUpdates(ctx)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{reached.IntentID()}, ids, "only the intent of a block that assembly reached may be acknowledged")
	mockStp.AssertCalled(t, "MarkRecoveredConflicts", mock.Anything, []chainhash.Hash{*txL1.TxIDChainHash()})
	mockStp.AssertNumberOfCalls(t, "MarkRecoveredConflicts", 1)
	require.NotContains(t, items.blockAssembler.unminedDropHashes, *txL2.TxIDChainHash())

	// The catch-up applies block 2: ProcessConflicting accepts the applied winner from the
	// checklist, and the block's acknowledgement completes the intent.
	losers, _, err := utxo.ProcessConflicting(ctx, store, ahead.BlockHeight, ahead.BlockHash, ahead.TxHashes, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	require.True(t, losers.Exists(*txL2.TxIDChainHash()))
	require.NoError(t, utxo.CompleteConflictIntentsForBlock(ctx, store, ahead.BlockHash))

	r, err := store.LoadConflictRecovery(ctx, ahead.IntentID())
	require.NoError(t, err)
	require.Nil(t, r)
	requireConflictOwner(ctx, t, store, txW2, txL2)
}

// A stale forward intent of block X whose winner W1 the current chain confirms again, while W2 is
// not confirmed. When block assembly reached X, its catch-up moves X back and reverses W2, so the heal
// only retires the intent. When it did not reach X, no moveBack follows, so the heal reverses W2 itself.
func TestHealStaleConflictIntent_PartialRemineReversesWhenAssemblyDidNotReachBlock(t *testing.T) {
	for _, reached := range []bool{true, false} {
		t.Run(fmt.Sprintf("reached=%t", reached), func(t *testing.T) {
			ctx := t.Context()
			items := setupBlockAssemblyTest(t)
			store := items.utxoStore
			require.NoError(t, store.SetBlockHeight(10))

			parent := createStaleReverseParent(ctx, t, store, 2)
			txL1 := createStaleReverseSpender(ctx, t, store, parent, 0, 90000, true)
			txW1 := createStaleReverseSpender(ctx, t, store, parent, 0, 80000, false)
			txL2 := createStaleReverseSpender(ctx, t, store, parent, 1, 90000, true)
			txW2 := createStaleReverseSpender(ctx, t, store, parent, 1, 80000, false)

			addBlockWithMinedSet(ctx, t, items, blockHeader1)
			side := *blockHeader1
			side.Nonce++
			addBlockWithMinedSet(ctx, t, items, &side)
			addBlockWithMinedSet(ctx, t, items, blockHeader2)

			winners := []chainhash.Hash{*txW1.TxIDChainHash(), *txW2.TxIDChainHash()}
			_, _, err := utxo.ProcessConflicting(ctx, store, 1, *side.Hash(), winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			require.NoError(t, err)

			_, mainMeta, err := items.blockchainClient.GetBlockHeader(ctx, blockHeader2.Hash())
			require.NoError(t, err)
			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txW1.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: mainMeta.ID, BlockHeight: 2})
			require.NoError(t, err)

			if reached {
				items.blockAssembler.setBestBlockHeader(&side, 1)
			} else {
				items.blockAssembler.setBestBlockHeader(blockHeader1, 1)
			}

			intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 1, BlockHash: *side.Hash(), TxHashes: winners}
			require.NoError(t, items.blockAssembler.healStaleConflictIntent(ctx, intent))

			r, err := store.LoadConflictRecovery(ctx, intent.IntentID())
			require.NoError(t, err)
			require.Nil(t, r, "the heal retires the intent")

			requireConflictOwner(ctx, t, store, txW1, txL1)

			if reached {
				requireConflictOwner(ctx, t, store, txW2, txL2)
				return
			}

			requireConflictOwner(ctx, t, store, txL2, txW2)

			pending, err := store.PendingConflictIntents(ctx)
			require.NoError(t, err)
			require.Empty(t, pending, "the reverse completes its own intent")
		})
	}
}

// Startup may encounter an acknowledged-in-memory block whose durable WAL delete failed,
// after the independent pruner removed a fully spent parent.
func TestReplayPendingConflictIntents_ConfirmedPrunedParent(t *testing.T) {
	for _, missing := range []bool{true, false} {
		t.Run(fmt.Sprintf("missing=%t", missing), func(t *testing.T) {
			ctx := t.Context()
			items := setupBlockAssemblyTest(t)
			store := items.utxoStore
			require.NoError(t, store.SetBlockHeight(10))
			parent := createStaleReverseParent(ctx, t, store, 1)
			createStaleReverseSpender(ctx, t, store, parent, 0, 90000, true)
			winner := createStaleReverseSpender(ctx, t, store, parent, 0, 80000, false)
			addBlockWithMinedSet(ctx, t, items, blockHeader1)
			winners := []chainhash.Hash{*winner.TxIDChainHash()}
			_, _, err := utxo.ProcessConflicting(ctx, store, 1, *blockHeader1.Hash(), winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
			require.NoError(t, err)
			_, blockMeta, err := items.blockchainClient.GetBlockHeader(ctx, blockHeader1.Hash())
			require.NoError(t, err)
			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{winner.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: blockMeta.ID, BlockHeight: 1})
			require.NoError(t, err)
			if missing {
				require.NoError(t, store.Delete(ctx, parent.TxIDChainHash()))
			} else {
				spends, e := utxo.GetSpends(winner)
				require.NoError(t, e)
				require.NoError(t, store.Unspend(ctx, spends))
			}
			failed := items.blockAssembler.replayPendingConflictIntents(ctx)
			if missing {
				require.Empty(t, failed)
				require.NoError(t, items.blockAssembler.checkConflictRecoveryReady(ctx, failed))
			} else {
				require.NotEmpty(t, failed, "confirmation must not hide a present parent's ownership mismatch")
				require.Error(t, items.blockAssembler.checkConflictRecoveryReady(ctx, failed))
			}
		})
	}
}

// A reverse that succeeded but did not retire the finished forward checklist leaves it pending. By
// the next startup the restored loser is confirmed and the pruner has removed the shared parent. The
// replay must still see the reversed winner, retire the checklist and let block assembly start.
func TestReplayPendingConflictIntents_ReversedPrunedParent(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))
	parent := createStaleReverseParent(ctx, t, store, 1)
	loser := createStaleReverseSpender(ctx, t, store, parent, 0, 90000, true)
	winner := createStaleReverseSpender(ctx, t, store, parent, 0, 80000, false)
	addBlockWithMinedSet(ctx, t, items, blockHeader1)

	staleBlock := chainhash.HashH([]byte("reversed-pruned-parent"))
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	_, _, err := utxo.ProcessConflicting(ctx, store, 1, staleBlock, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)
	_, _, err = utxo.ReverseProcessConflicting(ctx, store, 1, staleBlock, winners)
	require.NoError(t, err)

	forwardID := utxo.ForwardConflictIntentID(staleBlock)
	r, err := store.LoadConflictRecovery(ctx, forwardID)
	require.NoError(t, err)
	require.Equal(t, utxo.ConflictRecoveryFinished, r.Phase, "the reverse leaves the forward checklist for retirement")

	_, blockMeta, err := items.blockchainClient.GetBlockHeader(ctx, blockHeader1.Hash())
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{loser.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: blockMeta.ID, BlockHeight: 1})
	require.NoError(t, err)
	require.NoError(t, store.Delete(ctx, parent.TxIDChainHash()))

	failed := items.blockAssembler.replayPendingConflictIntents(ctx)
	require.Empty(t, failed)
	require.NoError(t, items.blockAssembler.checkConflictRecoveryReady(ctx, failed))

	r, err = store.LoadConflictRecovery(ctx, forwardID)
	require.NoError(t, err)
	require.Nil(t, r, "the replay retires the reversed checklist")

	m, err := store.Get(ctx, winner.TxIDChainHash(), utxofields.Conflicting)
	require.NoError(t, err)
	require.True(t, m.Conflicting, "the reversed winner stays reversed")
}

// An assembler without a UTXO store has no WAL. The startup checks must skip it, as the replay does.
func TestConflictRecoveryStartupChecks_NilStore(t *testing.T) {
	b := &BlockAssembler{}

	require.NoError(t, b.checkConflictRecoveryReady(t.Context(), nil))

	ids, err := b.prepareRecoveredConflictUpdates(t.Context())
	require.NoError(t, err)
	require.Empty(t, ids)
}

// failingWALStore wraps a real store and fails one WAL read, so the startup checks report it.
type failingWALStore struct {
	utxo.Store
	failPending   bool
	failLoad      bool
	pendingSeen   int
	failPendingAt int
}

func (s *failingWALStore) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	s.pendingSeen++

	if s.failPending || (s.failPendingAt > 0 && s.pendingSeen == s.failPendingAt) {
		return nil, errors.NewStorageError("injected WAL read failure")
	}

	return s.Store.PendingConflictIntents(ctx)
}

func (s *failingWALStore) LoadConflictRecovery(ctx context.Context, id chainhash.Hash) (*utxo.ConflictRecovery, error) {
	if s.failLoad {
		return nil, errors.NewStorageError("injected checklist read failure")
	}

	return s.Store.LoadConflictRecovery(ctx, id)
}

// The startup checks refuse to start on every unresolved shape of the WAL, and report a store that
// cannot be read instead of starting on a guess.
func TestConflictRecoveryStartupChecks_Branches(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	b := items.blockAssembler
	require.NoError(t, store.SetBlockHeight(10))

	wrapped := &failingWALStore{Store: store}
	b.utxoStore = wrapped

	wrapped.failPending = true
	require.ErrorContains(t, b.checkConflictRecoveryReady(ctx, nil), "injected WAL read failure")
	_, err := b.prepareRecoveredConflictUpdates(ctx)
	require.ErrorContains(t, err, "injected WAL read failure")
	require.Empty(t, b.replayPendingConflictIntents(ctx), "a replay that cannot read the WAL reports no failed intent")
	wrapped.failPending = false

	// A reverse intent of a block that is not on the chain is unresolved, and the preparation
	// skips it.
	reverse := utxo.ConflictIntent{Kind: utxo.ConflictIntentReverse, BlockHeight: 1, BlockHash: chainhash.HashH([]byte("off-chain")), StartedAt: 1}
	require.NoError(t, store.BeginConflictIntent(ctx, reverse))
	require.ErrorContains(t, b.checkConflictRecoveryReady(ctx, nil), "is unresolved")
	ids, err := b.prepareRecoveredConflictUpdates(ctx)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.NoError(t, store.CompleteConflictIntent(ctx, reverse.IntentID()))

	// A finished forward intent whose block is not on the chain is stale, and one whose checklist
	// cannot be read is an error.
	parent := createStaleReverseParent(ctx, t, store, 1)
	createStaleReverseSpender(ctx, t, store, parent, 0, 90000, true)
	winner := createStaleReverseSpender(ctx, t, store, parent, 0, 80000, false)
	staleBlock := chainhash.HashH([]byte("stale-block"))
	winners := []chainhash.Hash{*winner.TxIDChainHash()}
	_, _, err = utxo.ProcessConflicting(ctx, store, 1, staleBlock, winners, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	require.ErrorContains(t, b.checkConflictRecoveryReady(ctx, nil), "is stale")
	ids, err = b.prepareRecoveredConflictUpdates(ctx)
	require.NoError(t, err)
	require.Empty(t, ids, "a stale intent is left to the replay")

	wrapped.failLoad = true
	require.ErrorContains(t, b.checkConflictRecoveryReady(ctx, nil), "injected checklist read failure")
	_, err = b.prepareRecoveredConflictUpdates(ctx)
	require.ErrorContains(t, err, "injected checklist read failure")
	require.ErrorContains(t, b.replayConflictIntent(ctx, utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHash: staleBlock}), "injected checklist read failure")
	wrapped.failLoad = false

	require.ErrorContains(t, b.replayConflictIntent(ctx, utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHash: chainhash.HashH([]byte("unknown"))}), "legacy forward conflict intent")
	require.ErrorContains(t, b.replayConflictIntent(ctx, utxo.ConflictIntent{Kind: "bogus"}), "unknown conflict intent kind")
	require.ErrorContains(t, b.healStaleConflictIntent(ctx, utxo.ConflictIntent{Kind: "bogus"}), "unknown conflict intent kind")

	reached, err := b.assemblyReachedBlock(ctx, chainhash.HashH([]byte("unknown")))
	require.NoError(t, err)
	require.False(t, reached, "an unknown block was not reached")
}

// A WAL that cannot be re-read after the reverse intents marks every forward intent as failed.
func TestReplayPendingConflictIntents_ReReadFailure(t *testing.T) {
	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	b := items.blockAssembler
	require.NoError(t, store.SetBlockHeight(10))

	addBlockWithMinedSet(ctx, t, items, blockHeader1)

	// An empty stale reverse heals at once, and a forward intent of an unknown block stays.
	reverse := utxo.ConflictIntent{Kind: utxo.ConflictIntentReverse, BlockHeight: 1, BlockHash: *blockHeader1.Hash(), StartedAt: 1}
	require.NoError(t, store.BeginConflictIntent(ctx, reverse))
	forward := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 1, BlockHash: chainhash.HashH([]byte("forward-block")), StartedAt: 1}
	require.NoError(t, store.BeginConflictIntent(ctx, forward))

	b.utxoStore = &failingWALStore{Store: store, failPendingAt: 2}

	failed := b.replayPendingConflictIntents(ctx)
	require.Contains(t, failed, forward.IntentID())
	require.NotContains(t, failed, reverse.IntentID())
}
