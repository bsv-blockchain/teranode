package validator

import (
	"context"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestValidateDoesNotBlessCallerCreatedSpender pins the legacy block path's
// half of the "already blessed" fallback. When a spend fails because the parent
// record is gone, the validator reads the child's own metadata and blesses it
// if it is mined, unlocked and not conflicting. The legacy path creates every
// transaction of a block, mined and unlocked, before it validates, so that
// record satisfies all three and proves nothing: a replay of a transaction
// whose parent was pruned too would be blessed by the copy the replay just
// wrote. WithSpenderCreatedByCaller switches the bless off, at the validator
// and at the store beneath it.
//
// What this test proves is the store's half reached through the validator:
// both stores answer a failed spend with an aggregate ErrUtxoError, which
// validateInternal handles before it ever reaches its own ErrTxNotFound
// branch, so the validator-level gate is defensive and cannot be observed
// with these stores. Breaking the store gate fails this test; breaking only
// the validator gate does not.
func TestValidateDoesNotBlessCallerCreatedSpender(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}

	utxoStoreURL, err := url.Parse("sqlitememory:///spender_created_by_caller")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(500))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	parentTx := bt.NewTx()
	coinbaseInput := &bt.Input{PreviousTxOutIndex: 0xffffffff, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, coinbaseInput.PreviousTxIDAdd(new(chainhash.Hash)))
	parentTx.Inputs = append(parentTx.Inputs, coinbaseInput)
	parentTx.Outputs = append(parentTx.Outputs, &bt.Output{Satoshis: 500, LockingScript: script})
	_, err = store.Create(ctx, parentTx, 100, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	childTx := bt.NewTx()
	childInput := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xfffffffe, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, childInput.PreviousTxIDAdd(parentTx.TxIDChainHash()))
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{Satoshis: 400, LockingScript: script})

	// The child is created the way the legacy create phase creates it: mined
	// info supplied, no lock.
	_, _, err = store.SpendAndCreate(ctx, childTx, 101, utxostore.WithCreateOnly(),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 101, BlockHeight: 101}),
		utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// The parent is gone.
	require.NoError(t, store.DeleteComplete(ctx, parentTx.TxIDChainHash()))

	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	base := Options{
		SkipUtxoCreation:     true,
		SkipScriptValidation: true,
		SkipPolicyChecks:     true,
		OutpointOnlySpend:    true,
		IgnoreLocked:         true,
	}

	control := base
	_, err = v.ValidateWithOptions(ctx, childTx, 500, &control)
	require.NoError(t, err, "control: a pre-existing mined, unlocked child with a pruned parent is blessed")

	created := base
	created.SpenderCreatedByCaller = true
	_, err = v.ValidateWithOptions(ctx, childTx, 500, &created)
	require.ErrorIs(t, err, errors.ErrTxNotFound, "a record the caller wrote itself is not proof of prior validation")
}
