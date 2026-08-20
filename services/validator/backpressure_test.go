package validator

// Tests for the block-assembly back-pressure gate.
//
// The gate sheds MEMPOOL ingest in validateInternal — after the terminal
// in-memory checks (coinbase, option guards, finality), so a structurally
// invalid submission still gets its terminal verdict, and before ANY UTXO work
// — while block assembly's queue is over blockassembly_max_queued_transactions.
// Placement is the whole point: a reject must leave the transaction completely
// untouched (no spends, no created record, nothing Locked) so the submitter can
// simply retry. Block-context traffic (InBlock), callers that skip block
// assembly, and already-accepted transactions (SkipBackpressure) are exempt.
//
// The poller behind the verdict is tested with the monitor itself, in
// services/blockassembly/queue_monitor_test.go.

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// staticIngestGate drives the back-pressure verdict directly, so the gate can
// be exercised without standing up a queue poller.
type staticIngestGate bool

func (g staticIngestGate) Overloaded() bool { return bool(g) }

func TestBackpressureGate(t *testing.T) {
	t.Run("mempool tx rejected untouched while overloaded", func(t *testing.T) {
		ctx := context.Background()
		mockStore := &utxo.MockUtxostore{}
		tSettings := test.CreateBaseTestSettings(t)

		validator, err := New(ctx, ulogger.TestLogger{}, tSettings, mockStore, nil, nil, nil, nil, nil)
		require.NoError(t, err)

		v := validator.(*Validator)
		v.blockAssemblyQueueMonitor = staticIngestGate(true)

		// GetBlockState is a cached-state read used by the in-memory finality
		// check that deliberately runs BEFORE the gate (terminal verdicts beat
		// a retryable "busy"). It touches no UTXO records.
		mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000}).Maybe()

		tx, _ := makeExtendGraceTxAndParent(t)

		_, err = v.validateInternal(ctx, tx, 100, &Options{AddTXToBlockAssembly: true})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrThresholdExceeded), "expected THRESHOLD_EXCEEDED, got: %v", err)

		// The reject must happen before ANY UTXO record interaction: no
		// Get/Spend/SpendAndCreate expectation is registered, so any such call
		// fails the test here.
		mockStore.AssertExpectations(t)
	})

	t.Run("terminal verdicts beat the busy signal while overloaded", func(t *testing.T) {
		ctx := context.Background()
		mockStore := &utxo.MockUtxostore{}
		tSettings := test.CreateBaseTestSettings(t)

		validator, err := New(ctx, ulogger.TestLogger{}, tSettings, mockStore, nil, nil, nil, nil, nil)
		require.NoError(t, err)

		v := validator.(*Validator)
		v.blockAssemblyQueueMonitor = staticIngestGate(true)

		mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000}).Maybe()

		// A coinbase submission is structurally unacceptable: it must get its
		// terminal reject, not a retryable THRESHOLD_EXCEEDED a broadcaster
		// would replay forever.
		_, coinbaseTx := makeExtendGraceTxAndParent(t)

		_, err = v.validateInternal(ctx, coinbaseTx, 100, &Options{AddTXToBlockAssembly: true})
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrThresholdExceeded), "coinbase must get a terminal verdict, got: %v", err)
	})

	t.Run("block-context traffic exempt while overloaded", func(t *testing.T) {
		ctx := context.Background()
		mockStore := &utxo.MockUtxostore{}
		tSettings := test.CreateBaseTestSettings(t)

		validator, err := New(ctx, ulogger.TestLogger{}, tSettings, mockStore, nil, nil, nil, nil, nil)
		require.NoError(t, err)

		v := validator.(*Validator)
		v.blockAssemblyQueueMonitor = staticIngestGate(true)

		// Distinctive downstream failure: parent lookup fails, proving the tx
		// got PAST the gate and into real validation work.
		mockStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("parent gone"))
		mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(errors.NewTxNotFoundError("not found"))
		mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000}).Maybe()

		tx, _ := makeExtendGraceTxAndParent(t)

		_, err = v.validateInternal(ctx, tx, 100, &Options{AddTXToBlockAssembly: true, InBlock: true})
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrThresholdExceeded), "block-context tx must not be shed, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "expected the downstream missing-parent failure, got: %v", err)
	})

	t.Run("non-block-assembly callers exempt while overloaded", func(t *testing.T) {
		ctx := context.Background()
		mockStore := &utxo.MockUtxostore{}
		tSettings := test.CreateBaseTestSettings(t)

		validator, err := New(ctx, ulogger.TestLogger{}, tSettings, mockStore, nil, nil, nil, nil, nil)
		require.NoError(t, err)

		v := validator.(*Validator)
		v.blockAssemblyQueueMonitor = staticIngestGate(true)

		mockStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("parent gone"))
		mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(errors.NewTxNotFoundError("not found"))
		mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000}).Maybe()

		tx, _ := makeExtendGraceTxAndParent(t)

		_, err = v.validateInternal(ctx, tx, 100, &Options{AddTXToBlockAssembly: false})
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrThresholdExceeded), "caller skipping block assembly must not be shed, got: %v", err)
	})

	t.Run("not overloaded passes the gate", func(t *testing.T) {
		ctx := context.Background()
		mockStore := &utxo.MockUtxostore{}
		tSettings := test.CreateBaseTestSettings(t)

		validator, err := New(ctx, ulogger.TestLogger{}, tSettings, mockStore, nil, nil, nil, nil, nil)
		require.NoError(t, err)

		v := validator.(*Validator)

		mockStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("parent gone"))
		mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(errors.NewTxNotFoundError("not found"))
		mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000}).Maybe()

		tx, _ := makeExtendGraceTxAndParent(t)

		_, err = v.validateInternal(ctx, tx, 100, &Options{AddTXToBlockAssembly: true})
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrThresholdExceeded))
	})
}
