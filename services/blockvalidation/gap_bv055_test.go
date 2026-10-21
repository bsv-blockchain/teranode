//go:build bvgaps

package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// gap055Store is the real sqlitememory store with two hooks on create-only SpendAndCreate calls:
// the call for holdTx signals it started, waits until the sibling (failTx) has failed, gives the
// errgroup a bounded window to cancel the context it was handed, records ctx.Err() and then
// reaches the real store; the call for failTx waits for holdTx to start and fails immediately.
type gap055Store struct {
	utxo.Store

	holdTx, failTx chainhash.Hash

	started    chan struct{}
	siblingErr chan struct{}
	heldCtxErr chan error
}

func (s *gap055Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	if parseCreateOptions(opts).CreateOnly {
		switch *tx.TxIDChainHash() {
		case s.holdTx:
			close(s.started)
			<-s.siblingErr

			// A cancel from the errgroup lands right after the sibling's goroutine returns; when it
			// does, Done fires at once. When the call is correctly detached it never fires, and the
			// bounded wait simply elapses.
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}

			s.heldCtxErr <- ctx.Err()
		case s.failTx:
			<-s.started
			defer close(s.siblingErr)

			return nil, nil, errors.NewProcessingError("gap055: sibling create fails")
		}
	}

	return s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
}

// TestGapBV055_ChainedCreateWaveDoesNotCancelInFlightCreate covers GAP-BV-055 (BV-QUICK-018).
//
// Spec-correct behaviour: like the one-wave and spend waves, the chained create-only wave must
// not let a sibling failure (or shutdown) cancel a create that is already running; the in-flight
// store call must be detached from the errgroup context. Expected to fail today: createWave
// (quick_validate.go) passes the errgroup's createCtx straight into SpendAndCreate, so the failure
// of one create cancels the other's context mid-call. Mirrors
// TestApplyOneWave_ACancelFromAboveLetsTheInFlightCallFinishAndStartsNoOther.
func TestGapBV055_ChainedCreateWaveDoesNotCancelInFlightCreate(t *testing.T) {
	bv, recorder, cleanup := newOneWaveHarness(t, "gap055_create_wave", false)
	defer cleanup()

	root, key := seedRoot(t, recorder.Store, 2, "GAP055_KEY")

	inFlight := spendOf(t, key, root, 0, 90_000)
	failing := spendOf(t, key, root, 1, 90_000)

	store := &gap055Store{
		Store:      recorder.Store,
		holdTx:     *inFlight.TxIDChainHash(),
		failTx:     *failing.TxIDChainHash(),
		started:    make(chan struct{}),
		siblingErr: make(chan struct{}),
		heldCtxErr: make(chan error, 1),
	}
	bv.utxoStore = store

	block := &model.Block{Height: 100, ID: 42}
	items := []txApply{{tx: inFlight}, {tx: failing}}

	// limit 2 so both creates are in flight together.
	err := bv.createWave(context.Background(), block, items, false, false, 2, func(*bt.Tx) {})

	// Precondition: the sibling's failure is what the wave reports.
	require.Error(t, err, "precondition: the wave must fail because the sibling create failed")

	select {
	case ctxErr := <-store.heldCtxErr:
		require.NoError(t, ctxErr, "the in-flight create's context was cancelled by the sibling failure (createWave runs SpendAndCreate under the errgroup context)")
	default:
		t.Fatal("precondition: the in-flight create never reported its context state")
	}
}
