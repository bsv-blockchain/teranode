package validator

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// leaseLike is a lane position source with the allocator's Start and Ready: it holds a lease once
// leased is set, and gives no positions without one.
type leaseLike struct {
	started atomic.Bool
	leased  atomic.Bool
	n       atomic.Uint64
}

func (s *leaseLike) Start(context.Context) error {
	s.started.Store(true)
	return nil
}

func (s *leaseLike) Ready() error {
	if !s.leased.Load() {
		return errors.NewServiceUnavailableError("no lane lease yet")
	}

	return nil
}

func (s *leaseLike) Next(context.Context) (uint64, error) {
	if !s.leased.Load() {
		return lane.None, errors.NewServiceUnavailableError("no lane lease")
	}

	return lane.Pack(5, s.n.Add(1)), nil
}

func newLaneLeaseValidator(t *testing.T, source *leaseLike) (*Validator, utxo.Store) {
	t.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BatcherDrainMode = true

	storeURL, err := url.Parse("sqlitememory:///lanelease")
	require.NoError(t, err)

	store, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	taker, ok := utxo.As[utxo.LanePositionTaker](store)
	require.True(t, ok)
	taker.SetLanePositionSource(source)

	v, err := New(ctx, ulogger.TestLogger{}, tSettings, store, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	return v.(*Validator), store
}

func TestValidatorIsNotReadyWithoutALease(t *testing.T) {
	source := &leaseLike{}
	v, _ := newLaneLeaseValidator(t, source)

	require.True(t, source.started.Load(), "a validator starts its lease when it is built")

	_, msg, _ := v.Health(context.Background(), false)
	require.Contains(t, msg, "LaneLease: BAD")
}

func TestValidatorBecomesReadyOnceItsLeaseOpens(t *testing.T) {
	source := &leaseLike{}
	v, _ := newLaneLeaseValidator(t, source)

	source.leased.Store(true)

	_, msg, _ := v.Health(context.Background(), false)
	require.Contains(t, msg, "LaneLease: GOOD")
}

// A transaction validated with no lease fails with service unavailable, all the way out of
// Validate, and is neither stored nor judged invalid: nothing about the transaction is wrong.
func TestValidateFailsWithServiceUnavailableWithoutALanePosition(t *testing.T) {
	const height = 1_000

	source := &leaseLike{}
	v, store := newLaneLeaseValidator(t, source)

	w := tests.BuildMultiWorkload(t, 0xc9, 1, 1)

	source.leased.Store(true)
	w.StoreRoots(t, store, height-1)
	source.leased.Store(false)

	_, err := v.Validate(context.Background(), w.Txs[0], height,
		WithSkipPolicyChecks(true),
		WithSkipScriptValidation(true),
		WithAddTXToBlockAssembly(false),
		WithSkipTxMetaPublishing(true),
		WithCandidateParentMedianTime(1),
		WithUnconfirmedParentsAtCandidateHeight(true))
	require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	require.False(t, errors.Is(err, errors.ErrTxInvalid), "a missing lane position says nothing about the transaction")

	_, err = store.Get(context.Background(), w.Txs[0].TxIDChainHash(), fields.Fee)
	require.ErrorIs(t, err, errors.ErrTxNotFound)
}
