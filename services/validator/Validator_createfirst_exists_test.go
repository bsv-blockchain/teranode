package validator

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// creatingExistsShim wraps a real store to script the exists-in-creating-state scenario: the combined
// create-first SpendAndCreate fails the first attempt with TX_LOCKED (so the retry loop
// re-enters) and returns ErrTxExists on the retry, while GetMeta reports the record as
// still Creating. The roll-forward's spend-only SpendAndCreate returns either a complete
// or an incomplete spend set depending on rollForwardComplete.
type creatingExistsShim struct {
	utxostore.Store

	childHash           chainhash.Hash
	rollForwardComplete bool

	combinedCalls    int
	rollForwardCalls int
	finalizeCalls    int
	setLockedCalls   int
	setLockedValue   bool
}

func (s *creatingExistsShim) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	o, _ := utxostore.ParseCreateOptions(opts...)

	if o.SpendOnly {
		// The ErrTxExists roll-forward re-spend.
		s.rollForwardCalls++

		if s.rollForwardComplete {
			spent := make([]*utxostore.Spend, len(tx.Inputs))
			for i := range spent {
				spent[i] = &utxostore.Spend{}
			}

			return nil, spent, nil
		}

		return nil, nil, nil // incomplete: no per-input spends → RollForwardCreating fails closed
	}

	// Combined create-first path: attempt 0 is TX_LOCKED (drives the retry loop), attempt 1
	// finds the tentative record already present.
	s.combinedCalls++
	if s.combinedCalls == 1 {
		return nil, nil, errors.NewTxLockedError("tx locked on first attempt")
	}

	return nil, nil, errors.NewTxExistsError("tx exists in creating state")
}

func (s *creatingExistsShim) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	if hash != nil && *hash == s.childHash {
		// The record exists but is still mid create-first flight.
		data.Creating = true
		return nil
	}

	return s.Store.GetMeta(ctx, hash, data)
}

func (s *creatingExistsShim) FinalizeTransaction(ctx context.Context, tx *bt.Tx) error {
	s.finalizeCalls++
	return s.Store.FinalizeTransaction(ctx, tx)
}

// SetLocked is a no-op here: the roll-forward unlocks the record, but the shim intercepts
// SpendAndCreate so the child was never actually created in the embedded store.
func (s *creatingExistsShim) SetLocked(_ context.Context, _ []chainhash.Hash, value bool) error {
	s.setLockedCalls++
	s.setLockedValue = value

	return nil
}

// newCreatingExistsValidator seeds a real sqlitememory store with a coinbase-style parent,
// wraps it in the exists-in-creating shim, and returns a Validator plus a child tx.
// It mirrors the below-checkpoint outpoint-only harness (proven to reach SpendAndCreate).
func newCreatingExistsValidator(t *testing.T, dbName string, rollForwardComplete bool) (*Validator, *bt.Tx, *creatingExistsShim) {
	t.Helper()
	tracing.SetupMockTracer()

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

	t.Run("incomplete roll-forward is not treated as validated", func(t *testing.T) {
		v, childTx, shim := newCreatingExistsValidator(t, "creating_exists_incomplete", false)

		_, err := v.ValidateWithOptions(ctx, childTx, 500, creatingExistsOpts())

		require.Error(t, err, "a creating record whose roll-forward re-spend is incomplete must not validate")
		require.Equal(t, 2, shim.combinedCalls, "attempt 0 (TX_LOCKED) then attempt 1 (ErrTxExists) must both run")
		require.Equal(t, 1, shim.rollForwardCalls, "the ErrTxExists branch must attempt a roll-forward re-spend")
		require.Equal(t, 0, shim.finalizeCalls, "an incomplete roll-forward must not finalize")
	})

	t.Run("complete roll-forward finalizes and validates", func(t *testing.T) {
		v, childTx, shim := newCreatingExistsValidator(t, "creating_exists_complete", true)

		md, err := v.ValidateWithOptions(ctx, childTx, 500, creatingExistsOpts())

		require.NoError(t, err, "a creating record whose inputs are fully re-spent must roll forward and validate")
		require.NotNil(t, md)
		require.False(t, md.Creating, "the returned meta must be finalized once rolled forward")
		require.False(t, md.Locked, "the returned meta must be unlocked once rolled forward")
		require.Equal(t, 1, shim.rollForwardCalls)
		require.Equal(t, 1, shim.finalizeCalls, "a complete roll-forward must finalize")
		require.Equal(t, 1, shim.setLockedCalls, "a complete roll-forward must unlock the record")
		require.False(t, shim.setLockedValue, "unlock means SetLocked(false)")
	})
}
