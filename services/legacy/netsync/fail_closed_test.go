package netsync

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// createConflictingSpyValidator records the CreateConflicting validator option
// observed on each Validate call and, optionally, returns a fixed error to model
// a conflicting/double-spend outcome. It lets the fail-closed tests assert both
// which option the netsync path passed AND how PreValidateTransactions treats the
// resulting error, without standing up a real validator + UTXO store.
type createConflictingSpyValidator struct {
	validator.MockValidator

	mu                  sync.Mutex
	sawCreateConflict   int // number of Validate calls that observed CreateConflicting=true
	sawNoCreateConflict int // number of Validate calls that observed CreateConflicting=false
	returnErr           error
	callCount           atomic.Int64
}

func (v *createConflictingSpyValidator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...validator.Option) (*meta.Data, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	v.callCount.Add(1)

	processed := validator.ProcessOptions(opts...)

	v.mu.Lock()
	if processed.CreateConflicting {
		v.sawCreateConflict++
	} else {
		v.sawNoCreateConflict++
	}
	returnErr := v.returnErr
	v.mu.Unlock()

	if returnErr != nil {
		return nil, returnErr
	}

	return &meta.Data{}, nil
}

func (v *createConflictingSpyValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *validator.Options) (*meta.Data, error) {
	return v.Validate(ctx, tx, blockHeight)
}

func (v *createConflictingSpyValidator) TriggerBatcher() {}

func (v *createConflictingSpyValidator) createConflictCounts() (with, without int) {
	v.mu.Lock()
	defer v.mu.Unlock()

	return v.sawCreateConflict, v.sawNoCreateConflict
}

// failClosedCheckpointHeight is the single hard-coded checkpoint used by the
// fail-closed harness; block height 500 is below it so the outpoint-only gate can
// engage.
const failClosedCheckpointHeight = int32(1000)

// newFailClosedSyncManager wires a SyncManager whose legacyFailClosed gate engages
// (or not) exactly as requested: the outpoint-only prerequisite on, a supporting
// store, a checkpoint above the test height, the unified route off, and the
// fail-closed flag set per the argument. It returns the manager ready to drive
// PreValidateTransactions.
func newFailClosedSyncManager(t *testing.T, failClosed bool, cv validator.Interface) *SyncManager {
	t.Helper()

	tSettings, params := newOutpointOnlySettings(t, true, true, failClosedCheckpointHeight)
	tSettings.BlockValidation.LegacyBelowCheckpointFailClosed = failClosed
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = false
	tSettings.Legacy.SpendBatcherSize = 2
	tSettings.Legacy.SpendBatcherConcurrency = 2

	return &SyncManager{
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		validationClient: cv,
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
	}
}

// makeSameBlockParentChainTxMap builds n regular txs where tx[k] spends tx[k-1]'s
// output (a same-block parent chain). All spends succeed at the validator (no
// conflict), so this exercises the happy path where the WithCreateConflicting drop
// must NOT introduce any spurious failure.
func makeSameBlockParentChainTxMap(t *testing.T, n int) *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper] {
	t.Helper()

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](n)

	var prevHash *chainhash.Hash

	for i := 0; i < n; i++ {
		tx := bt.NewTx()
		tx.Version = 1

		if prevHash == nil {
			require.NoError(t, tx.From(chainhash.HashH([]byte(fmt.Sprintf("coinbase-%d", i))).String(), 0, "76a914", uint64(1_000_000)))
		} else {
			require.NoError(t, tx.From(prevHash.String(), 0, "76a914", uint64(1_000_000)))
		}

		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(900_000)))

		h := *tx.TxIDChainHash()
		txMap.Set(h, &TxMapWrapper{Tx: tx})
		prevHash = &h
	}

	return txMap
}

// TestLegacyFailClosed_SameBlockParentChain_NoConflictingNodes proves that with the
// fail-closed flag ON, a below-checkpoint block whose txs form a same-block parent
// chain still validates: the validator sees WithCreateConflicting=false on every tx
// (the drop happened) and PreValidateTransactions returns no error. Because no tx is
// ever validated with CreateConflicting=true, the store can never mark a txMeta
// Conflicting, so no conflicting subtree node can be written downstream.
func TestLegacyFailClosed_SameBlockParentChain_NoConflictingNodes(t *testing.T) {
	initPrometheusMetrics()

	cv := &createConflictingSpyValidator{} // all spends succeed

	sm := newFailClosedSyncManager(t, true, cv)

	txMap := makeSameBlockParentChainTxMap(t, 5)

	// failClosed is true on this path.
	require.True(t, sm.legacyFailClosed(500))

	err := sm.PreValidateTransactions(context.Background(), txMap, chainhash.Hash{}, 500, 0, 0, true, true)
	require.NoError(t, err, "same-block parent chain must validate under fail-closed with no spurious ErrTxNotFound")

	with, without := cv.createConflictCounts()
	require.Zero(t, with, "fail-closed path must NOT pass WithCreateConflicting(true) to the validator")
	require.Equal(t, 5, without, "every tx must be validated with CreateConflicting=false so no conflicting node can be produced")
}

// TestLegacyFailClosed_GenuineConflict_HardFails proves that with the flag ON a
// genuine conflict (validator returns ErrTxConflicting) is NOT swallowed: it surfaces
// as a non-retryable ProcessingError from PreValidateTransactions, so the block is
// never committed. The error is non-retryable (ERR_TX_CONFLICTING is not in the
// retryable set), so it lands in hardFail rather than looping.
func TestLegacyFailClosed_GenuineConflict_HardFails(t *testing.T) {
	initPrometheusMetrics()

	cv := &createConflictingSpyValidator{
		returnErr: errors.ErrTxConflicting,
	}

	sm := newFailClosedSyncManager(t, true, cv)

	txMap := makeTxMap(t, 3)

	require.True(t, sm.legacyFailClosed(500))

	err := sm.PreValidateTransactions(context.Background(), txMap, chainhash.Hash{}, 500, 0, 0, true, true)
	require.Error(t, err, "genuine conflict must hard-fail under fail-closed")
	require.Contains(t, err.Error(), "non-retryable", "conflict must surface via the non-retryable hardFail branch")
	require.False(t, errors.IsRetryableError(err), "the surfaced error must be non-retryable")

	with, _ := cv.createConflictCounts()
	require.Zero(t, with, "fail-closed path must NOT pass WithCreateConflicting(true) even for a conflicting tx")
}

// TestLegacyFailClosed_FlagOff_ByteIdentical proves that with the flag OFF the
// behaviour is unchanged: WithCreateConflicting(true) is still appended (the validator
// observes it on every tx) and an ErrTxConflicting result is still swallowed, so
// PreValidateTransactions returns no error and the block proceeds exactly as today.
func TestLegacyFailClosed_FlagOff_ByteIdentical(t *testing.T) {
	initPrometheusMetrics()

	cv := &createConflictingSpyValidator{
		returnErr: errors.ErrTxConflicting,
	}

	sm := newFailClosedSyncManager(t, false, cv)

	txMap := makeTxMap(t, 3)

	require.False(t, sm.legacyFailClosed(500), "flag off must keep fail-closed OFF")

	// failClosed=false is what ValidateTransactionsLegacyMode threads through when
	// legacyFailClosed is false.
	err := sm.PreValidateTransactions(context.Background(), txMap, chainhash.Hash{}, 500, 0, 0, true, false)
	require.NoError(t, err, "flag-off path must still swallow ErrTxConflicting and proceed")

	with, without := cv.createConflictCounts()
	require.Equal(t, 3, with, "flag-off path must still append WithCreateConflicting(true) on every tx")
	require.Zero(t, without, "no tx should be validated without CreateConflicting on the flag-off path")
}

// TestSyncManager_legacyFailClosed is the truth table for the gate helper: the flag
// must be ON, the outpoint-only prerequisite must hold, and the unified route must be
// OFF for the inline fail-closed path to engage.
func TestSyncManager_legacyFailClosed(t *testing.T) {
	const checkpointHeight = int32(1000)
	const below = uint32(500)
	const above = uint32(1500)

	tests := []struct {
		name       string
		failClosed bool
		outpoint   bool
		unified    bool
		height     uint32
		want       bool
	}{
		{name: "all off", failClosed: false, outpoint: false, unified: false, height: below, want: false},
		{name: "failClosed on, outpoint off => no-op", failClosed: true, outpoint: false, unified: false, height: below, want: false},
		{name: "failClosed on, outpoint on, below => engages", failClosed: true, outpoint: true, unified: false, height: below, want: true},
		{name: "failClosed on, outpoint on, above checkpoint => off", failClosed: true, outpoint: true, unified: false, height: above, want: false},
		{name: "failClosed on, outpoint on, unified on => off (unified wins)", failClosed: true, outpoint: true, unified: true, height: below, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings, params := newOutpointOnlySettings(t, tt.outpoint, true, checkpointHeight)
			tSettings.BlockValidation.LegacyBelowCheckpointFailClosed = tt.failClosed
			tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = tt.unified

			sm := &SyncManager{
				settings:    tSettings,
				chainParams: params,
				logger:      ulogger.TestLogger{},
				utxoStore:   &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
			}

			require.Equal(t, tt.want, sm.legacyFailClosed(tt.height),
				"legacyFailClosed(%d) failClosed=%v outpoint=%v unified=%v", tt.height, tt.failClosed, tt.outpoint, tt.unified)
		})
	}
}
