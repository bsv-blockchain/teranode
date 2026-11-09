package validator

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// creatingExistsShim wraps a real store to script the exists-in-creating-state scenario: the combined
// create-first SpendAndCreate fails the first attempt with TX_LOCKED (so the retry loop
// re-enters) and returns ErrTxExists on the retry, while GetMeta reports the record as
// still Creating. The store rolls a creating duplicate forward inside SpendAndCreate, so
// the retry returns ErrTxExists marked utxo.MarkRolledForward with the spends when
// rollForwardComplete is set, and an unmarked ErrTxExists (the store did not complete the
// record) otherwise.
type creatingExistsShim struct {
	utxostore.Store

	childHash           chainhash.Hash
	rollForwardComplete bool
	locked              bool
	notCreating         bool
	events              *[]string

	rollForwardFlags   utxostore.IgnoreFlags
	rollForwardSkipExt bool
	combinedCalls      int
	rollForwardCalls   int
	finalizeCalls      int
	setLockedCalls     int
	setLockedValue     bool
	deleteCalls        int
	unspendCalls       int
}

func (s *creatingExistsShim) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	o, _ := utxostore.ParseCreateOptions(opts...)

	// Combined create-first path: attempt 0 is TX_LOCKED (drives the retry loop), attempt 1
	// finds the tentative record already present.
	s.combinedCalls++
	if s.combinedCalls == 1 {
		return nil, nil, errors.NewTxLockedError("tx locked on first attempt")
	}

	if s.notCreating {
		return nil, nil, errors.NewTxExistsError("tx exists")
	}

	if !s.rollForwardComplete {
		return nil, nil, errors.NewTxExistsError("tx exists in creating state")
	}

	// The store's roll-forward ran inside this call, under the caller's flags.
	s.rollForwardCalls++
	s.rollForwardFlags = o.IgnoreFlags
	s.rollForwardSkipExt = o.SkipExtendedInputs

	spent := make([]*utxostore.Spend, len(tx.Inputs))
	for i := range spent {
		spent[i] = &utxostore.Spend{}
	}

	return nil, spent, utxostore.MarkRolledForward(errors.NewTxExistsError("tx exists in creating state"))
}

func (s *creatingExistsShim) DeleteComplete(ctx context.Context, hash *chainhash.Hash) error {
	s.deleteCalls++
	return s.Store.DeleteComplete(ctx, hash)
}

func (s *creatingExistsShim) Unspend(ctx context.Context, spends []*utxostore.Spend, flagAsLocked ...bool) error {
	s.unspendCalls++
	return s.Store.Unspend(ctx, spends, flagAsLocked...)
}

func (s *creatingExistsShim) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	if hash != nil && *hash == s.childHash {
		// The record exists but is still mid create-first flight.
		data.Creating = !s.notCreating
		data.Locked = s.locked

		return nil
	}

	return s.Store.GetMeta(ctx, hash, data)
}

func (s *creatingExistsShim) FinalizeTransaction(ctx context.Context, tx *bt.Tx, attempt []byte) error {
	s.finalizeCalls++
	return s.Store.FinalizeTransaction(ctx, tx, attempt)
}

// SetLocked is a no-op here: the roll-forward unlocks the record, but the shim intercepts
// SpendAndCreate so the child was never actually created in the embedded store.
func (s *creatingExistsShim) SetLocked(_ context.Context, _ []chainhash.Hash, value bool) error {
	s.setLockedCalls++
	s.setLockedValue = value

	if s.events != nil {
		*s.events = append(*s.events, fmt.Sprintf("setLocked(%t)", value))
	}

	return nil
}

// newCreatingExistsValidator seeds a real sqlitememory store with a coinbase-style parent,
// wraps it in the exists-in-creating shim, and returns a Validator plus a child tx.
// It mirrors the below-checkpoint outpoint-only harness (proven to reach SpendAndCreate).
func newCreatingExistsValidator(t *testing.T, dbName string, rollForwardComplete bool) (*Validator, *bt.Tx, *creatingExistsShim) {
	t.Helper()
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
	tSettings.BlockAssembly.Disabled = true // addToBlockAssembly=false → plain combined path

	utxoStoreURL, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)

	realStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, realStore.SetBlockHeight(500))
	require.NoError(t, realStore.SetMedianBlockTime(1700000000))

	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	parentTx := bt.NewTx()
	coinbaseInput := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	require.NoError(t, coinbaseInput.PreviousTxIDAdd(new(chainhash.Hash)))
	parentTx.Inputs = append(parentTx.Inputs, coinbaseInput)
	parentTx.Outputs = append(parentTx.Outputs, &bt.Output{Satoshis: 500, LockingScript: coinbaseScript})

	_, err = realStore.Create(ctx, parentTx, 499, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	childTx := bt.NewTx()
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	require.NoError(t, childInput.PreviousTxIDAdd(parentTx.TxIDChainHash()))
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{Satoshis: 400, LockingScript: coinbaseScript})

	shim := &creatingExistsShim{
		Store:               realStore,
		childHash:           *childTx.TxIDChainHash(),
		rollForwardComplete: rollForwardComplete,
	}

	v := &Validator{
		logger:      logger,
		utxoStore:   shim,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	return v, childTx, shim
}

func creatingExistsOpts() *Options {
	return &Options{
		SkipScriptValidation: true,
		SkipPolicyChecks:     true,
		OutpointOnlySpend:    true,
		IgnoreLocked:         true,
		AddTXToBlockAssembly: true,
	}
}

// TestValidate_CreateFirst_ErrTxExistsCreating: when a create-first record already exists
// in the tentative "creating" state (reached here via the TX_LOCKED retry loop), the
// validator must NOT report it as validated. It must roll the record forward first, and
// fail closed if the roll-forward re-spend cannot cover every input.
func TestValidate_CreateFirst_ErrTxExistsCreating(t *testing.T) {
	ctx := context.Background()

	t.Run("creating record the store did not roll forward is not treated as validated", func(t *testing.T) {
		v, childTx, shim := newCreatingExistsValidator(t, "creating_exists_incomplete", false)

		_, err := v.ValidateWithOptions(ctx, childTx, 500, creatingExistsOpts())

		require.Error(t, err, "a creating record whose roll-forward re-spend is incomplete must not validate")
		require.Equal(t, 2, shim.combinedCalls, "attempt 0 (TX_LOCKED) then attempt 1 (ErrTxExists) must both run")
		require.Equal(t, 0, shim.rollForwardCalls, "the store reported no roll-forward")
		require.Equal(t, 0, shim.finalizeCalls, "an unrolled creating record must not be finalized")
		require.Equal(t, 0, shim.deleteCalls, "a record this attempt did not create is not deleted")
	})

	t.Run("complete roll-forward finalizes and validates", func(t *testing.T) {
		v, childTx, shim := newCreatingExistsValidator(t, "creating_exists_complete", true)

		md, err := v.ValidateWithOptions(ctx, childTx, 500, creatingExistsOpts())

		require.NoError(t, err, "a creating record whose inputs are fully re-spent must roll forward and validate")
		require.NotNil(t, md)
		require.False(t, md.Creating, "the returned meta must be finalized once rolled forward")
		require.False(t, md.Locked, "the returned meta must be unlocked once rolled forward")
		require.Equal(t, 1, shim.rollForwardCalls)
		require.Equal(t, 0, shim.setLockedCalls, "the roll-forward leaves the lock to the block-assembly hand-off")
	})
}

type eventBlockAssembly struct {
	events *[]string
	stored []chainhash.Hash
	err    error
}

func (b *eventBlockAssembly) Store(_ context.Context, hash *chainhash.Hash, _, _ uint64, _ subtree.TxInpoints) (bool, error) {
	if b.err != nil {
		return false, b.err
	}

	b.stored = append(b.stored, *hash)
	*b.events = append(*b.events, "store")

	return true, nil
}

func (b *eventBlockAssembly) RemoveTx(_ context.Context, _ *chainhash.Hash) error {
	return nil
}

func withEventBlockAssembly(v *Validator, shim *creatingExistsShim) *eventBlockAssembly {
	events := make([]string, 0, 2)
	shim.events = &events
	ba := &eventBlockAssembly{events: &events}
	v.settings.BlockAssembly.Disabled = false
	v.blockAssembler = ba

	return ba
}

func TestValidate_CreateFirst_RollForwardIgnoreFlags(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name         string
		ignoreLocked bool
		outpointOnly bool
	}{
		{"ignore locked false", false, true},
		{"ignore locked true", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, childTx, shim := newCreatingExistsValidator(t, "creating_exists_flags_"+tc.name, true)

			opts := creatingExistsOpts()
			opts.IgnoreLocked = tc.ignoreLocked
			opts.OutpointOnlySpend = tc.outpointOnly
			opts.SkipScriptValidation = true

			_, err := v.ValidateWithOptions(ctx, childTx, 500, opts)
			require.NoError(t, err)
			require.Equal(t, 1, shim.rollForwardCalls)
			require.Equal(t, tc.ignoreLocked, shim.rollForwardFlags.IgnoreLocked, "the store's SpendAndCreate options must carry the caller's IgnoreLocked")
			require.Equal(t, tc.outpointOnly, shim.rollForwardFlags.SkipUTXOHashCheck, "the roll-forward must skip the hash check only on the outpoint-only path")
			require.Equal(t, tc.outpointOnly, shim.rollForwardSkipExt, "the roll-forward must skip extended inputs only on the outpoint-only path")
		})
	}
}

func TestIgnoreSpendOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   *Options
	}{
		{"plain", &Options{}},
		{"ignore locked", &Options{IgnoreLocked: true}},
		{"outpoint only", &Options{OutpointOnlySpend: true}},
		{"both", &Options{IgnoreLocked: true, OutpointOnlySpend: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, err := utxostore.ParseCreateOptions(spendIgnoreOptions(tc.in)...)
			require.NoError(t, err)
			require.Equal(t, tc.in.IgnoreLocked, o.IgnoreFlags.IgnoreLocked)
			require.Equal(t, tc.in.OutpointOnlySpend, o.IgnoreFlags.SkipUTXOHashCheck)
			require.Equal(t, tc.in.OutpointOnlySpend, o.SkipExtendedInputs)
		})
	}
}

func TestValidate_CreateFirst_RollForwardHandoff(t *testing.T) {
	ctx := context.Background()

	t.Run("rolled forward tx reaches block assembly then unlocks", func(t *testing.T) {
		v, childTx, shim := newCreatingExistsValidator(t, "creating_exists_handoff", true)
		shim.locked = true
		ba := withEventBlockAssembly(v, shim)

		md, err := v.ValidateWithOptions(ctx, childTx, 500, creatingExistsOpts())

		require.NoError(t, err)
		require.Equal(t, []chainhash.Hash{*childTx.TxIDChainHash()}, ba.stored)
		require.Equal(t, []string{"store", "setLocked(false)"}, *shim.events, "block assembly must receive the tx before the record is unlocked")
		require.False(t, md.Creating)
		require.False(t, md.Locked, "the deferred unlock clears Locked after the hand-off")
	})

	t.Run("without block assembly there is no store and no unlock", func(t *testing.T) {
		v, childTx, shim := newCreatingExistsValidator(t, "creating_exists_no_ba", true)
		shim.locked = true
		ba := withEventBlockAssembly(v, shim)

		opts := creatingExistsOpts()
		opts.AddTXToBlockAssembly = false

		md, err := v.ValidateWithOptions(ctx, childTx, 500, opts)

		require.NoError(t, err)
		require.Empty(t, ba.stored)
		require.Empty(t, *shim.events, "a caller that does not own the hand-off must leave the record locked")
		require.False(t, md.Creating)
		require.True(t, md.Locked)
	})

	t.Run("failed roll-forward neither stores nor unlocks", func(t *testing.T) {
		v, childTx, shim := newCreatingExistsValidator(t, "creating_exists_rf_fail", false)
		shim.locked = true
		ba := withEventBlockAssembly(v, shim)

		_, err := v.ValidateWithOptions(ctx, childTx, 500, creatingExistsOpts())

		require.Error(t, err)
		require.Empty(t, ba.stored, "a creating record the store did not complete must not reach block assembly")
		require.Empty(t, *shim.events)
	})

	t.Run("non creating resubmit returns early", func(t *testing.T) {
		v, childTx, shim := newCreatingExistsValidator(t, "creating_exists_resubmit", true)
		shim.locked = true
		shim.notCreating = true
		ba := withEventBlockAssembly(v, shim)

		md, err := v.ValidateWithOptions(ctx, childTx, 500, creatingExistsOpts())

		require.NoError(t, err)
		require.Empty(t, ba.stored)
		require.Empty(t, *shim.events)
		require.Equal(t, 0, shim.rollForwardCalls)
		require.True(t, md.Locked)
	})
}

// TestValidate_CreateFirst_RolledForwardShedIsNotUnwound: a record this attempt only rolled
// forward was created by another attempt, so a queue-full shed of its hand-off must leave it
// as it is (finalized, locked) for the unmined reload. Deleting it or unspending its inputs
// would undo another attempt's work.
func TestValidate_CreateFirst_RolledForwardShedIsNotUnwound(t *testing.T) {
	ctx := context.Background()

	v, childTx, shim := newCreatingExistsValidator(t, "creating_exists_shed", true)
	shim.locked = true
	ba := withEventBlockAssembly(v, shim)
	ba.err = errors.NewThresholdExceededError("block assembly queue full")

	keptBefore := testutil.ToFloat64(prometheusValidatorShedRolledForwardKept)
	unwindsBefore := testutil.ToFloat64(prometheusValidatorShedUnwindTotal)

	_, err := v.ValidateWithOptions(ctx, childTx, 500, creatingExistsOpts())

	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the caller still sees the shed")
	require.Equal(t, 0, shim.deleteCalls, "a rolled-forward record must not be deleted by the shed")
	require.Equal(t, 0, shim.unspendCalls, "a rolled-forward record's inputs must not be unspent by the shed")
	require.Equal(t, 0, shim.setLockedCalls, "the record stays locked for the unmined reload")
	require.Equal(t, keptBefore+1, testutil.ToFloat64(prometheusValidatorShedRolledForwardKept))
	require.Equal(t, unwindsBefore, testutil.ToFloat64(prometheusValidatorShedUnwindTotal), "no unwind is attempted")
}
