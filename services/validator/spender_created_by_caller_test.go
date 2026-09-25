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
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
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

// TestValidateReportsPrunedReplayRatherThanConflict: replay C spends P:0, whose
// parent carries C's marker, and Q:0, which after a reorg a different
// transaction X now holds. The store answers the marker for P:0 and ErrSpent for
// Q:0. With CreateConflicting set, as the legacy block path sets it, the ErrSpent
// alone used to send the validator down the conflicting-create branch, which
// returned ErrTxConflicting with the marker rejection dropped from the chain.
// netsync swallows ErrTxConflicting below the checkpoint, so the block committed
// with C back in the store. A pruned replay must reach the caller as one.
// Reproduced by review.
func TestValidateReportsPrunedReplayRatherThanConflict(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
	tSettings.BatcherDrainMode = true
	tSettings.Pruner.UTXODefensiveEnabled = false

	utxoStoreURL, err := url.Parse("sqlitememory:///pruned_replay_not_conflict")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	sql.ResetPrunerServiceForTests()
	t.Cleanup(sql.ResetPrunerServiceForTests)

	require.NoError(t, store.SetBlockHeight(1000))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	newParent := func(seed byte) *bt.Tx {
		tx := bt.NewTx()
		// An ordinary, non-coinbase input, so the outputs carry no maturity rule.
		input := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
		funding := chainhash.HashH([]byte{seed})
		require.NoError(t, input.PreviousTxIDAdd(&funding))
		tx.Inputs = append(tx.Inputs, input)
		// Output 1 stays unspent so the parent survives the prune.
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 500, LockingScript: script}, &bt.Output{Satoshis: 500, LockingScript: script})
		_, createErr := store.Create(ctx, tx, 1000, utxostore.WithSkipExtendedInputs(true))
		require.NoError(t, createErr)

		return tx
	}

	spending := func(outputs []*bt.Tx, satoshis uint64) *bt.Tx {
		tx := bt.NewTx()
		for _, prev := range outputs {
			input := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: prev.Outputs[0].Satoshis, PreviousTxScript: prev.Outputs[0].LockingScript,
				SequenceNumber: 0xfffffffe, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
			require.NoError(t, input.PreviousTxIDAdd(prev.TxIDChainHash()))
			tx.Inputs = append(tx.Inputs, input)
		}

		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: satoshis, LockingScript: script})

		return tx
	}

	mined := func(height uint32, txs ...*bt.Tx) {
		hashes := make([]*chainhash.Hash, 0, len(txs))
		for _, tx := range txs {
			hashes = append(hashes, tx.TxIDChainHash())
		}

		_, minedErr := store.SetMinedMulti(ctx, hashes, utxostore.MinedBlockInfo{BlockID: height, BlockHeight: height, OnLongestChain: true})
		require.NoError(t, minedErr)
	}

	p := newParent(0x01)
	q := newParent(0x02)

	c := spending([]*bt.Tx{p, q}, 900)
	_, _, err = store.SpendAndCreate(ctx, c, 1000)
	require.NoError(t, err)
	mined(1000, p, q, c)

	g := spending([]*bt.Tx{c}, 800)
	_, _, err = store.SpendAndCreate(ctx, g, 1001)
	require.NoError(t, err)
	mined(1001, g)

	// Q:0 is released and X takes it before C is pruned, so the prune marks only
	// P (the marker is written only where the output still names C) and C's
	// input on Q answers ErrSpent.
	q0Hash, err := util.UTXOHashFromOutput(q.TxIDChainHash(), q.Outputs[0], 0)
	require.NoError(t, err)
	require.NoError(t, store.Unspend(ctx, []*utxostore.Spend{{
		TxID: q.TxIDChainHash(), Vout: 0, UTXOHash: q0Hash, SpendingData: spendpkg.NewSpendingData(c.TxIDChainHash(), 1),
	}}))

	x := spending([]*bt.Tx{q}, 450)
	_, _, err = store.SpendAndCreate(ctx, x, 1400)
	require.NoError(t, err, "fixture: X takes Q:0")

	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	_, err = svc.Prune(ctx, 1001+tSettings.GetUtxoStoreBlockHeightRetention()+10, "prune-c")
	require.NoError(t, err)

	_, err = store.Get(ctx, c.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "fixture: C is pruned")

	// The legacy create phase writes C again, locked and mined, before validating.
	_, _, err = store.SpendAndCreate(ctx, c, 1400, utxostore.WithCreateOnly(), utxostore.WithLocked(true),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1400, BlockHeight: 1400}), utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	opts := Options{
		SkipUtxoCreation:       true,
		SkipScriptValidation:   true,
		SkipPolicyChecks:       true,
		OutpointOnlySpend:      true,
		IgnoreLocked:           true,
		CreateConflicting:      true,
		SpenderCreatedByCaller: true,
		// Post-CSV height, so the validator needs the candidate parent MTP.
		CandidateParentMedianTime: 1700000000,
	}

	_, err = v.ValidateWithOptions(ctx, c, 1400, &opts)
	require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "the replay must be reported as a pruned replay")
	require.NotErrorIs(t, err, errors.ErrTxConflicting, "a pruned replay is not a conflict to reconcile")

	meta, err := store.Get(ctx, c.TxIDChainHash())
	require.NoError(t, err)
	require.False(t, meta.Conflicting, "the recreated record must not be flagged conflicting, which is what the conflicting branch did")
}
