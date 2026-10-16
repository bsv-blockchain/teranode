package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// cancelMidCallStore is the real sqlitememory store with one hook: the store call for holdTx
// signals that it has started, waits until the test has cancelled the caller's context, and
// only then reaches the store, with whatever context it was handed. That is a cancel landing
// mid-call, between the moment a combined SpendAndCreate starts and the moment it finishes.
type cancelMidCallStore struct {
	utxo.Store

	holdTx    chainhash.Hash
	started   chan struct{}
	cancelled chan struct{}
	heldCtx   chan error
}

func (s *cancelMidCallStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	if *tx.TxIDChainHash() == s.holdTx {
		close(s.started)
		<-s.cancelled

		s.heldCtx <- ctx.Err()
	}

	return s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
}

// A cancel from above must not land inside a one-wave store call. The one-wave apply runs under
// the caller's errgroups: a failed chained create cancels it from the sibling wave, and a failed
// subtree file write or prefetch cancels it from batchCtx. A combined SpendAndCreate spends and
// then creates, and its rollback runs under the same context, so a cancel mid-call can leave a
// transaction's inputs spent with no row of its own (review 5493551911). The call that has
// started must finish, and the one that has not must not start.
//
// The end state is asserted on the real store: the transaction whose call was in flight is
// created with its input spent by it, and the one queued behind it is absent with its input
// unspent. The wave fails with a context error, so a caller that classifies the failure reads
// a retry, not a verdict on the block.
func TestApplyOneWave_ACancelFromAboveLetsTheInFlightCallFinishAndStartsNoOther(t *testing.T) {
	bv, recorder, cleanup := newOneWaveHarness(t, "one_wave_cancel_mid_call", false)
	defer cleanup()

	root, key := seedRoot(t, recorder.Store, 2, "ONE_WAVE_CANCEL_MID_CALL_KEY")

	inFlight := spendOf(t, key, root, 0, 90_000)
	queued := spendOf(t, key, root, 1, 90_000)

	store := &cancelMidCallStore{
		Store:     recorder.Store,
		holdTx:    *inFlight.TxIDChainHash(),
		started:   make(chan struct{}),
		cancelled: make(chan struct{}),
		heldCtx:   make(chan error, 1),
	}
	bv.utxoStore = store

	block := &model.Block{Height: 100, ID: 42}
	items := []txApply{{tx: inFlight}, {tx: queued}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-store.started
		cancel()
		close(store.cancelled)
	}()

	// limit 1: the queued transaction's goroutine cannot start until the in-flight call is done,
	// by which time the cancel has landed.
	err := bv.applyOneWave(ctx, block, items, false, false, 1, func(*bt.Tx) {})

	require.NoError(t, <-store.heldCtx, "the in-flight store call must not see the caller's cancel")

	require.Error(t, err, "a cancelled wave fails")
	require.True(t, errors.IsContextError(err), "a cancel must surface as a context error, got: %v", err)

	_, getErr := recorder.Store.Get(context.Background(), inFlight.TxIDChainHash())
	require.NoError(t, getErr, "the in-flight transaction must be created")
	requireSpentBy(t, recorder.Store, root, 0, inFlight)

	_, getErr = recorder.Store.Get(context.Background(), queued.TxIDChainHash())
	require.True(t, errors.Is(getErr, errors.ErrTxNotFound), "the queued transaction must not be applied, got: %v", getErr)

	resp, spendErr := recorder.Store.GetSpend(context.Background(), &utxo.Spend{TxID: root.TxIDChainHash(), Vout: 1})
	require.NoError(t, spendErr)
	require.Nil(t, resp.SpendingData, "the queued transaction's input must stay unspent")
}
