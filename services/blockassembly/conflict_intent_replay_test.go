package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
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

// TestReplayPendingConflictIntents_StaleForwardHealed covers the exact gap the
// reviewer flagged: a forward intent whose block has reorged OFF the longest
// chain must not be re-applied (that would undo the valid reorg) — but a bare
// discard would leave a partially-applied forward torn, in particular with the
// double-spend parents stuck Locked because step 5's unlock never ran on the
// crash. The fix heals by undoing the forward AND unlocking those parents.
//
// This drives a real sqlitememory store through the BlockAssembler gate. The
// pre-state is the residue of such a crash: the counter L is already the spender
// (so the inverse Reverse short-circuits — no full execution, no sqlitememory
// connection deadlock) while the parent is still wrongly Locked.
func TestReplayPendingConflictIntents_StaleForwardHealed(t *testing.T) {
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

	// The crash residue: the forward locked parent at step 2 and never unlocked it.
	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*parentHash}, true))
	m, err := store.Get(ctx, parentHash, utxofields.Locked)
	require.NoError(t, err)
	require.True(t, m.Locked, "precondition: parent is locked")

	intent := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentForward,
		BlockHeight: 10,
		BlockHash:   chainhash.HashH([]byte("reorged-out-block")),
		TxHashes:    []chainhash.Hash{*txWHash},
		StartedAt:   1,
	}
	require.NoError(t, store.BeginConflictIntent(ctx, intent))

	// Block is off the longest chain → forward intent is stale → heal.
	items.blockAssembler.blockchainClient = blockchainMembershipMock(false, false)

	items.blockAssembler.replayPendingConflictIntents(ctx)

	// Healed: the parent the forward locked is unlocked again, and the WAL is clear.
	m, err = store.Get(ctx, parentHash, utxofields.Locked)
	require.NoError(t, err)
	require.False(t, m.Locked, "stale forward heal must unlock the parent the forward left locked")

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending, "healed stale intent must be cleared from the WAL")
}

// TestReplayPendingConflictIntents_StaleReverseHealed is the mirror: a reverse
// intent whose block is back ON the longest chain must be healed by RE-APPLYING
// the forward (ProcessConflicting), not discarded. Uses a mock store to assert
// the gate routes a stale reverse into ProcessConflicting (which records its own
// store calls) and completes the intent — convergence of the op itself is
// covered by the real-backend crash harness.
func TestReplayPendingConflictIntents_StaleReverseHealed(t *testing.T) {
	demoted := chainhash.HashH([]byte("stale-demoted"))

	demotedTx := bt.NewTx()
	require.NoError(t, demotedTx.From(chainhash.HashH([]byte("p")).String(), 0, "76a914000000000000000000000000000000000000000088ac", 1000))

	intent := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentReverse,
		BlockHeight: 300,
		BlockHash:   chainhash.HashH([]byte("re-applied-block")),
		TxHashes:    []chainhash.Hash{demoted},
		StartedAt:   1,
	}

	mockStore := &utxo.MockUtxostore{}
	// Minimal ProcessConflicting flow with no losers (GetCounterConflicting → []).
	mockStore.On("Get", mock.Anything, &demoted, mock.Anything).Return(&meta.Data{Tx: demotedTx, Conflicting: true}, nil)
	mockStore.On("GetCounterConflicting", mock.Anything, demoted).Return([]chainhash.Hash{}, nil)
	mockStore.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockStore.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, nil)
	mockStore.On("SetConflicting", mock.Anything, mock.Anything, mock.Anything).Return([]*utxo.Spend{}, []chainhash.Hash{}, nil)
	mockStore.On("SetLocked", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	spy := &replaySpyStore{MockUtxostore: mockStore, pending: []utxo.ConflictIntent{intent}}
	// Block IS on the longest chain → reverse intent is stale → heal via forward.
	b := newReplayTestAssembler(spy, blockchainMembershipMock(true, false))

	b.replayPendingConflictIntents(context.Background())

	require.Contains(t, spy.completed, intent.IntentID(), "a stale reverse intent must be healed (forward re-applied) and cleared from the WAL")
	mockStore.AssertCalled(t, "SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
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
