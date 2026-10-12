package validator

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// decorateSpyStore wraps a real utxo.Store and counts every call to
// PreviousOutputsDecorate and BatchPreviousOutputsDecorate. This lets a test
// assert that the below-checkpoint OutpointOnlySpend fast path issues zero
// parent-read calls through extendTransaction.
type decorateSpyStore struct {
	utxostore.Store
	decorateCallCount atomic.Int64
}

func (s *decorateSpyStore) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	s.decorateCallCount.Add(1)
	return s.Store.PreviousOutputsDecorate(ctx, tx)
}

func (s *decorateSpyStore) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	s.decorateCallCount.Add(1)
	return s.Store.BatchPreviousOutputsDecorate(ctx, txs)
}

// TestValidate_OutpointOnlySpend verifies that ValidateWithOptions with
// OutpointOnlySpend=true succeeds on an un-extended child transaction, and
// that the parent output is recorded as spent (a competing spender is rejected).
//
// Exercises:
//   - Site 1: parent Get (block-heights + extend) is skipped entirely
//   - Site 2: Spend is issued with SkipUTXOHashCheck=true
//   - Site 4: TxMetaDataFromTxNoFee is called (SkipUtxoCreation=true path)
//
// The parent is stored without extension (WithSkipExtendedInputs), so any path
// that attempted to extend the child would fail with a nil-satoshis error.
func TestValidate_OutpointOnlySpend(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// OutpointOnlySpend is only legitimate at or below a checkpoint; model a checkpoint
	// above the test height so the validator's below-checkpoint guard permits the fast path.
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}

	utxoStoreURL, err := url.Parse("sqlitememory:///outpointonly_spend")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(500))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	// parentTx: coinbase-style with one P2PKH output (500 sat).
	// Stored without extended inputs so it carries no satoshi metadata —
	// any path that tries to read parent satoshis would fail.
	parentTx := bt.NewTx()
	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	coinbaseInput := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	zeroHash := new(chainhash.Hash)
	err = coinbaseInput.PreviousTxIDAdd(zeroHash)
	require.NoError(t, err)
	parentTx.Inputs = append(parentTx.Inputs, coinbaseInput)
	parentTx.Outputs = append(parentTx.Outputs, &bt.Output{
		Satoshis:      500,
		LockingScript: coinbaseScript,
	})

	// Store parent with WithSkipExtendedInputs so no fee/extension is required.
	_, err = store.Create(ctx, parentTx, 499, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// childTx: spends parentTx output:0.
	// Inputs are NOT extended (no PreviousTxScript, no PreviousTxSatoshis).
	childTx := bt.NewTx()
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	err = childInput.PreviousTxIDAdd(parentTx.TxIDChainHash())
	require.NoError(t, err)
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{
		Satoshis:      400,
		LockingScript: coinbaseScript,
	})

	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	opts := &Options{
		SkipUtxoCreation:     true,
		SkipScriptValidation: true,
		SkipPolicyChecks:     true,
		OutpointOnlySpend:    true,
		IgnoreLocked:         true,
	}

	_, err = v.ValidateWithOptions(ctx, childTx, 500, opts)
	require.NoError(t, err, "outpoint-only spend on un-extended input must succeed")

	// Verify the parent output is now spent by attempting to spend it with a
	// different competing transaction. The store records childTx's spending_data
	// on the output; any other spender must be rejected with ErrSpent.
	competingTx := bt.NewTx()
	competingInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x01}), // different unlocking script → different txid
	}
	err = competingInput.PreviousTxIDAdd(parentTx.TxIDChainHash())
	require.NoError(t, err)
	competingTx.Inputs = append(competingTx.Inputs, competingInput)
	competingTx.Outputs = append(competingTx.Outputs, &bt.Output{
		Satoshis:      300,
		LockingScript: coinbaseScript,
	})

	_, err = store.Spend(ctx, competingTx, 500, utxostore.IgnoreFlags{
		IgnoreLocked:      true,
		SkipUTXOHashCheck: true,
	})
	require.Error(t, err, "competing spender must be rejected after outpoint-only spend committed the first spender")
}

// TestValidate_OutpointOnlySpend_ConflictingFallbackMinimalCreate is the regression
// guard for the conflicting-spend fallback on the outpoint-only fast path. When a
// below-checkpoint tx hits already-spent state with CreateConflicting set (the legacy
// PreValidateTransactions option set), the validator saves it as conflicting via
// CreateInUtxoStore. That fallback create must use the SAME minimal-create option as the
// primary create — otherwise it calls GetFees on the deliberately un-decorated tx and
// returns a processing error, which the legacy path treats as a hard failure and stalls
// catchup. With the fix the fallback returns ErrTxConflicting (benign for legacy catchup).
func TestValidate_OutpointOnlySpend_ConflictingFallbackMinimalCreate(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}

	utxoStoreURL, err := url.Parse("sqlitememory:///outpointonly_conflicting")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = utxoStoreURL
	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(500))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	// Parent stored minimal (un-decorated), one output.
	parentTx := bt.NewTx()
	cbIn := &bt.Input{PreviousTxOutIndex: 0xffffffff, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, cbIn.PreviousTxIDAdd(new(chainhash.Hash)))
	parentTx.Inputs = append(parentTx.Inputs, cbIn)
	parentTx.Outputs = append(parentTx.Outputs, &bt.Output{Satoshis: 500, LockingScript: coinbaseScript})
	_, err = store.Create(ctx, parentTx, 499, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// First spender consumes parent:0 (outpoint-only), marking it spent.
	firstTx := bt.NewTx()
	firstIn := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xfffffffe, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, firstIn.PreviousTxIDAdd(parentTx.TxIDChainHash()))
	firstTx.Inputs = append(firstTx.Inputs, firstIn)
	firstTx.Outputs = append(firstTx.Outputs, &bt.Output{Satoshis: 400, LockingScript: coinbaseScript})
	_, err = store.Create(ctx, firstTx, 500, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)
	_, err = store.Spend(ctx, firstTx, 500, utxostore.IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true})
	require.NoError(t, err)

	// Competing tx: also spends parent:0, un-decorated, distinct txid.
	competingTx := bt.NewTx()
	compIn := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xfffffffe, UnlockingScript: bscript.NewFromBytes([]byte{0x01})}
	require.NoError(t, compIn.PreviousTxIDAdd(parentTx.TxIDChainHash()))
	competingTx.Inputs = append(competingTx.Inputs, compIn)
	competingTx.Outputs = append(competingTx.Outputs, &bt.Output{Satoshis: 300, LockingScript: coinbaseScript})

	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	// The legacy below-checkpoint fast-path option set.
	opts := &Options{
		SkipUtxoCreation:     true,
		SkipScriptValidation: true,
		SkipPolicyChecks:     true,
		CreateConflicting:    true,
		OutpointOnlySpend:    true,
		IgnoreLocked:         true,
	}

	_, err = v.ValidateWithOptions(ctx, competingTx, 500, opts)
	require.Error(t, err, "competing spender must be rejected")
	require.True(t, errors.Is(err, errors.ErrTxConflicting),
		"conflicting fallback on the outpoint-only path must return ErrTxConflicting (via minimal create), not a GetFees processing error; got: %v", err)
}

// TestValidate_OutpointOnlySpend_RequiresSkipScriptValidation verifies that
// setting OutpointOnlySpend=true without SkipScriptValidation=true returns a
// clear misconfiguration error before any extend/parent-read work is attempted.
func TestValidate_OutpointOnlySpend_RequiresSkipScriptValidation(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// OutpointOnlySpend is only legitimate at or below a checkpoint; model a checkpoint
	// above the test height so the validator's below-checkpoint guard permits the fast path.
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}

	utxoStoreURL, err := url.Parse("sqlitememory:///outpointonly_misconfig")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(500))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	// Minimal child tx — contents don't matter; the guard fires before any store access.
	childTx := bt.NewTx()
	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	zeroHash := new(chainhash.Hash)
	err = childInput.PreviousTxIDAdd(zeroHash)
	require.NoError(t, err)
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{
		Satoshis:      400,
		LockingScript: coinbaseScript,
	})

	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	// OutpointOnlySpend=true but SkipScriptValidation is left false — misconfiguration.
	opts := &Options{
		SkipUtxoCreation:  true,
		OutpointOnlySpend: true,
		// SkipScriptValidation intentionally omitted (false)
	}

	_, err = v.ValidateWithOptions(ctx, childTx, 500, opts)
	require.Error(t, err, "OutpointOnlySpend without SkipScriptValidation must return an error")
	require.Contains(t, err.Error(), "OutpointOnlySpend requires SkipScriptValidation")
}

// TestValidate_OutpointOnlySpend_RejectedAboveCheckpoint verifies the validator's
// defence-in-depth height guard: even a correctly-paired OutpointOnlySpend +
// SkipScriptValidation request must be rejected when the block height is above the
// highest hardcoded checkpoint, independently of the caller. This closes the gap where
// only blockvalidation (not the validator) enforced the checkpoint bound.
func TestValidate_OutpointOnlySpend_RejectedAboveCheckpoint(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// Checkpoint is BELOW the validation height — the fast path must not engage here.
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 100}}

	utxoStoreURL, err := url.Parse("sqlitememory:///outpointonly_abovecp")
	require.NoError(t, err)
	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(500))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	// Minimal child tx — the guard fires before any store access.
	childTx := bt.NewTx()
	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	require.NoError(t, childInput.PreviousTxIDAdd(new(chainhash.Hash)))
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{Satoshis: 400, LockingScript: coinbaseScript})

	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	// Correctly paired, but height 500 > checkpoint 100 → must be rejected by the guard.
	opts := &Options{
		SkipUtxoCreation:     true,
		SkipScriptValidation: true,
		OutpointOnlySpend:    true,
		IgnoreLocked:         true,
	}

	_, err = v.ValidateWithOptions(ctx, childTx, 500, opts)
	require.Error(t, err, "OutpointOnlySpend above the highest checkpoint must be rejected")
	require.Contains(t, err.Error(), "must not be used above the highest checkpoint")
}

// TestValidate_OutpointOnlySpend_RejectedOnUnsupportingStore verifies the validator's
// fail-closed store-capability guard: a correctly-paired, below-checkpoint OutpointOnlySpend
// request is still rejected when the UTXO store reports it does not support the fast path.
// Such a store ignores SkipUTXOHashCheck/SkipExtendedInputs and would hard-error on the
// un-decorated inputs; the guard rejects before any store access so a misconfigured caller
// cannot stall IBD. Uses a NullStore (SupportsOutpointOnlySpend() == false).
func TestValidate_OutpointOnlySpend_RejectedOnUnsupportingStore(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// Checkpoint well above the height so the height guard passes; the STORE guard fires.
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}

	// A store that reports no fast-path support — the guard must reject before touching it.
	store, err := nullstore.NewNullStore()
	require.NoError(t, err)
	require.False(t, store.SupportsOutpointOnlySpend(), "precondition: store does not support the fast path")

	childTx := bt.NewTx()
	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	require.NoError(t, childInput.PreviousTxIDAdd(new(chainhash.Hash)))
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{Satoshis: 400, LockingScript: coinbaseScript})

	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	opts := &Options{
		SkipUtxoCreation:     true,
		SkipScriptValidation: true,
		OutpointOnlySpend:    true,
		IgnoreLocked:         true,
	}

	_, err = v.ValidateWithOptions(ctx, childTx, 500, opts)
	require.Error(t, err, "OutpointOnlySpend on a store that does not support it must be rejected")
	require.Contains(t, err.Error(), "requires a UTXO store that supports it")
}

// TestValidate_OutpointOnlySpend_NoParentRead is the TDD regression guard for the bug
// where validateTransaction (~line 1677) contained an ungated re-extend block:
//
//	if !tx.IsExtended() {
//	    if err := v.extendTransaction(ctx, tx); err != nil { ... }
//	}
//
// extendTransaction calls PreviousOutputsDecorate, issuing one SELECT raw_tx per
// parent. With OutpointOnlySpend=true the fast path in validateInternal (~line 735)
// deliberately leaves the tx un-extended, but the ungated re-extend in
// validateTransaction immediately undid that — re-issuing exactly the per-parent
// parent reads the fast path exists to eliminate (142,733 per-tx observed on mainnet).
//
// The fix gates the re-extend on !validationOptions.OutpointOnlySpend:
//
//	if !validationOptions.OutpointOnlySpend && !tx.IsExtended() { ... }
//
// This test proves correctness by wrapping the real store in a decorateSpyStore
// and asserting that zero PreviousOutputsDecorate calls are issued when
// OutpointOnlySpend=true. It will FAIL on pre-fix code (count>0) and PASS after.
func TestValidate_OutpointOnlySpend_NoParentRead(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// OutpointOnlySpend is only legitimate at or below a checkpoint; model a checkpoint
	// above the test height so the validator's below-checkpoint guard permits the fast path.
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}

	utxoStoreURL, err := url.Parse("sqlitememory:///outpointonly_noparentread")
	require.NoError(t, err)

	realStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, realStore.SetBlockHeight(500))
	require.NoError(t, realStore.SetMedianBlockTime(1700000000))

	// Wrap the real store in a spy so we can count PreviousOutputsDecorate calls.
	spy := &decorateSpyStore{Store: realStore}

	// parentTx: coinbase-style with one P2PKH output.
	// Stored WITHOUT extended inputs (WithSkipExtendedInputs) so it carries no
	// satoshi metadata. Any path that attempts to extend the child via
	// PreviousOutputsDecorate would fail (or at minimum increment the spy counter).
	parentTx := bt.NewTx()
	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	coinbaseInput := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	zeroHash := new(chainhash.Hash)
	err = coinbaseInput.PreviousTxIDAdd(zeroHash)
	require.NoError(t, err)
	parentTx.Inputs = append(parentTx.Inputs, coinbaseInput)
	parentTx.Outputs = append(parentTx.Outputs, &bt.Output{
		Satoshis:      500,
		LockingScript: coinbaseScript,
	})

	_, err = realStore.Create(ctx, parentTx, 499, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// childTx: spends parentTx output:0. Inputs are NOT extended — no
	// PreviousTxScript, no PreviousTxSatoshis — so IsExtended() returns false
	// and any re-extend attempt via PreviousOutputsDecorate would fire.
	childTx := bt.NewTx()
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	err = childInput.PreviousTxIDAdd(parentTx.TxIDChainHash())
	require.NoError(t, err)
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{
		Satoshis:      400,
		LockingScript: coinbaseScript,
	})

	v := &Validator{
		logger:      logger,
		utxoStore:   spy, // spy wraps the real store — intercepts decorate calls
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	opts := &Options{
		SkipUtxoCreation:     true,
		SkipScriptValidation: true,
		SkipPolicyChecks:     true,
		OutpointOnlySpend:    true,
		IgnoreLocked:         true,
	}

	_, err = v.ValidateWithOptions(ctx, childTx, 500, opts)
	require.NoError(t, err, "outpoint-only spend on un-extended input must succeed")

	// THE REGRESSION GUARD: PreviousOutputsDecorate must NEVER be called when
	// OutpointOnlySpend=true. Before the fix, validateTransaction re-extended the tx
	// unconditionally, firing PreviousOutputsDecorate once per transaction. After the
	// fix the guard prevents the re-extend and this counter stays at zero.
	require.Equal(t, int64(0), spy.decorateCallCount.Load(),
		"OutpointOnlySpend must issue zero PreviousOutputsDecorate calls; "+
			"a non-zero count means validateTransaction is re-issuing the per-parent "+
			"reads that the fast path exists to eliminate")
}

// TestValidate_OutpointOnlySpend_BIP68HeightSkipped is the TDD regression guard for the bug
// where BIP68 sequence-lock validation ran on the OutpointOnlySpend fast path even though
// validateInternal intentionally left utxoHeights nil — causing "MTP store not loaded" errors
// on V2 txs with height-based relative locks at/above CSVHeight, stalling block validation.
//
// The fix adds `validationOptions.OutpointOnlySpend ||` as the first disjunct in the BIP68
// entry-guard at validateTransaction (~line 1753), causing it to return nil immediately
// without touching utxoHeights. Below-checkpoint BIP68 compliance is already certified by
// the pinned hardcoded checkpoint — same basis as skipping script validation.
//
// RED (before fix): blockHeight=1000 >= CSVHeight=1, blockchainClient != nil, SkipPolicyChecks=true
// → BIP68 guard passes → readMTPsLocked called with empty mtpStore → error returned.
// GREEN (after fix): OutpointOnlySpend=true short-circuits the BIP68 guard → returns nil.
func TestValidate_OutpointOnlySpend_BIP68HeightSkipped(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// OutpointOnlySpend is only legitimate at or below a checkpoint; model a checkpoint
	// above the test height so the validator's below-checkpoint guard permits the fast path.
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}

	// CSVHeight=1 ensures BIP68 is active at any practical blockHeight.
	tSettings.ChainCfgParams.CSVHeight = 1

	utxoStoreURL, err := url.Parse("sqlitememory:///outpointonly_bip68height")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(1000))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	// parentTx: coinbase-style with one P2PKH output.
	// Stored without extended inputs so no satoshi metadata is present — any path
	// that tries to extend the child would fail.
	parentTx := bt.NewTx()
	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	coinbaseInput := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	zeroHash := new(chainhash.Hash)
	err = coinbaseInput.PreviousTxIDAdd(zeroHash)
	require.NoError(t, err)
	parentTx.Inputs = append(parentTx.Inputs, coinbaseInput)
	parentTx.Outputs = append(parentTx.Outputs, &bt.Output{
		Satoshis:      500,
		LockingScript: coinbaseScript,
	})

	_, err = store.Create(ctx, parentTx, 990, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// childTx: V2 tx with a height-based BIP68 relative lock (SequenceNumber=10).
	// Disable bit (0x80000000) is CLEAR and type bit (0x00400000) is CLEAR → height-based,
	// 10-block relative lock. Inputs are NOT extended (no PreviousTxScript/Satoshis).
	childTx := bt.NewTx()
	childTx.Version = 2
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     10, // height-based relative lock: 10 blocks
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	err = childInput.PreviousTxIDAdd(parentTx.TxIDChainHash())
	require.NoError(t, err)
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{
		Satoshis:      400,
		LockingScript: coinbaseScript,
	})

	// blockchainClient is non-nil so the `v.blockchainClient == nil` guard in
	// validateTransaction does NOT short-circuit BIP68. The mock has no expectations
	// set: BIP68 only does a nil-check on blockchainClient; it does not call any
	// client methods — it reads from the pre-loaded mtpStore instead.
	mockClient := &blockchain.Mock{}

	v := &Validator{
		logger:           logger,
		utxoStore:        store,
		settings:         tSettings,
		txValidator:      NewTxValidator(logger, tSettings),
		stats:            gocore.NewStat("validator"),
		blockchainClient: mockClient,
		// mtpStore intentionally left nil/empty: before the fix, BIP68 runs and
		// readMTPsLocked returns "MTP store not loaded up to height 1000". After the
		// fix, OutpointOnlySpend=true short-circuits before readMTPsLocked is reached.
	}

	opts := &Options{
		SkipUtxoCreation:          true,
		SkipScriptValidation:      true,
		SkipPolicyChecks:          true,
		OutpointOnlySpend:         true,
		IgnoreLocked:              true,
		CandidateParentMedianTime: 1700000000, // required when blockHeight >= CSVHeight && SkipPolicyChecks=true
	}

	// RED trigger: blockHeight=1000 >= CSVHeight=1, blockchainClient != nil,
	// SkipPolicyChecks=true → BIP68 runs → readMTPsLocked fails on empty mtpStore.
	// AFTER fix: OutpointOnlySpend=true short-circuits BIP68 → returns nil.
	_, err = v.ValidateWithOptions(ctx, childTx, 1000, opts)
	require.NoError(t, err, "BIP68 sequence-lock must be skipped on OutpointOnlySpend fast path")
}

// outpointOnlyFixture is one isolated outpoint-only scenario: its own SQL-memory
// store (keyed by the URL path, so a distinct path per case), its own funded
// coinbase parent and its own un-extended child spending that parent's output:0.
//
// Per-case isolation is mandatory, not tidiness: an admitted outpoint-only
// validation CONSUMES the parent output, so a shared fixture would make a later
// case's outcome depend on whether an earlier case had already spent the UTXO
// rather than on the bound under test.
type outpointOnlyFixture struct {
	store *sql.Store
	child *bt.Tx
}

// newOutpointOnlyFixture builds the fixture. tipHeight is the store's own chain
// tip (what the tip bound reads); parentHeight is the height the coinbase parent
// is created at and must be at or below tipHeight, or the parent reads back
// IMMATURE and the spend fails for a reason unrelated to the bound.
func newOutpointOnlyFixture(t *testing.T, ctx context.Context, logger ulogger.Logger,
	tSettings *settings.Settings, storePath string, tipHeight, parentHeight uint32) *outpointOnlyFixture {
	t.Helper()

	utxoStoreURL, err := url.Parse("sqlitememory:///" + storePath)
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(tipHeight))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

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

	_, err = store.Create(ctx, parentTx, parentHeight, utxostore.WithSkipExtendedInputs(true))
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

	return &outpointOnlyFixture{store: store, child: childTx}
}

func (f *outpointOnlyFixture) validator(logger ulogger.Logger, tSettings *settings.Settings) *Validator {
	return &Validator{
		logger:      logger,
		utxoStore:   f.store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}
}

// TestValidate_OutpointOnlySpend_TipBound is the regression guard for the second
// checkpoint bound: the pre-existing guard bounds the height the CALLER asserts,
// which an attacker simply sets low, so it is satisfied by construction. This one
// bounds the height the NODE'S OWN CHAIN has reached, which the caller cannot set.
//
// The caller height stays at 500 in every row — comfortably below the checkpoint,
// so the caller-height guard passes and the only thing varying is the store's own
// tip. The 1,000,000 row is the boundary the `>` comparison claims to admit and is
// the initial-block-download regression guard: validating the block AT checkpoint
// height must still work. The 1,000,001 row is what used to pass.
func TestValidate_OutpointOnlySpend_TipBound(t *testing.T) {
	tracing.SetupMockTracer()

	const checkpointHeight = 1_000_000

	cases := []struct {
		name      string
		tipHeight uint32
		rejected  bool
	}{
		{name: "tip below checkpoint", tipHeight: checkpointHeight - 1},
		{name: "tip at checkpoint", tipHeight: checkpointHeight},
		{name: "tip past checkpoint", tipHeight: checkpointHeight + 1, rejected: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}

			f := newOutpointOnlyFixture(t, ctx, logger, tSettings,
				fmt.Sprintf("outpointonly_tipbound_%d", tc.tipHeight), tc.tipHeight, 499)

			opts := &Options{
				SkipUtxoCreation:     true,
				SkipScriptValidation: true,
				SkipPolicyChecks:     true,
				OutpointOnlySpend:    true,
				IgnoreLocked:         true,
			}

			_, err := f.validator(logger, tSettings).ValidateWithOptions(ctx, f.child, 500, opts)

			if !tc.rejected {
				require.NoError(t, err,
					"a node whose own tip is at or below the checkpoint must still admit the fast path")

				return
			}

			require.Error(t, err, "a node whose own tip is past the checkpoint must reject the fast path")
			require.Contains(t, err.Error(), "chain tip is past the highest checkpoint")
			require.Contains(t, err.Error(), fmt.Sprintf("%d", tc.tipHeight),
				"the error must name the tip height the operator can act on")
		})
	}
}

// TestValidate_OutpointOnlySpend_LegacyCandidateBlockShape pins the tip bound
// against the shape the legitimate producer actually sends: legacy netsync's
// below-checkpoint fast path, validating candidate block C while the tip is still
// C-1. That ordering is not incidental — netsync validates block H's transactions
// inside prepareSubtrees BEFORE H is handed to block validation, and block
// dispatch is serial (a single goroutine consumes the block queue), so the tip
// cannot reach H while H is being validated.
//
// Sub-case 1 is that boundary and must succeed. Sub-case 2 is the same request on
// a node whose tip has genuinely moved past the checkpoint and must be rejected.
func TestValidate_OutpointOnlySpend_LegacyCandidateBlockShape(t *testing.T) {
	tracing.SetupMockTracer()

	const checkpointHeight = 1_000_000

	// The option set handle_block.go's below-checkpoint fast path builds, not a
	// minimal subset: a guard that only bit on a reduced option set would not
	// prove anything about the real caller.
	legacyOpts := func() *Options {
		return &Options{
			SkipUtxoCreation:          true,
			AddTXToBlockAssembly:      false,
			SkipPolicyChecks:          true,
			InBlock:                   true,
			SkipTxMetaPublishing:      true,
			SkipScriptValidation:      true,
			CandidateBlockTime:        1700000000,
			CandidateParentMedianTime: 1700000000,
			CreateConflicting:         true,
			OutpointOnlySpend:         true,
			IgnoreLocked:              true,
		}
	}

	cases := []struct {
		name      string
		tipHeight uint32
		rejected  bool
	}{
		{name: "candidate block at checkpoint, tip one behind", tipHeight: checkpointHeight - 1},
		{name: "same request once the tip is past the checkpoint", tipHeight: checkpointHeight + 1, rejected: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}

			f := newOutpointOnlyFixture(t, ctx, logger, tSettings,
				fmt.Sprintf("outpointonly_candidateshape_%d", tc.tipHeight), tc.tipHeight, tc.tipHeight-1)

			// Caller height is the candidate block C itself, which the pre-existing
			// caller-height guard admits (`>` , not `>=`).
			_, err := f.validator(logger, tSettings).ValidateWithOptions(ctx, f.child, checkpointHeight, legacyOpts())

			if !tc.rejected {
				require.NoError(t, err,
					"the checkpoint-proven legacy caller must still work at the boundary")

				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), "chain tip is past the highest checkpoint")
			require.Contains(t, err.Error(), fmt.Sprintf("%d", tc.tipHeight),
				"the error must name the tip height the operator can act on")
		})
	}
}
