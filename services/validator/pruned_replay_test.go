package validator

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

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
		SkipUtxoCreation:     true,
		SkipScriptValidation: true,
		SkipPolicyChecks:     true,
		OutpointOnlySpend:    true,
		IgnoreLocked:         true,
		CreateConflicting:    true,
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

// TestValidatePrunedReplayInABlock: replay C spends n parents' output 0. The
// first n-1 of those outputs were released and taken by X before C was pruned,
// so they answer ErrSpent and only the last parent carries C's marker.
//
// With 11 inputs the marker answer is the eleventh failed spend, past the
// validator's cap of 10 aggregated errors, and it must still reach the caller.
// On this store it would without utxo.ReplayRejectionsFirst too: the SQL spend
// joins its per-input errors into one chain, so the tenth kept error still
// links to the marker answer. TestBatchedSpendKeepsReplayPastTheErrorCap pins
// the Aerospike batched spend, where the errors stand alone.
//
// In a block above the checkpoint the replay spends outputs C already spent on
// this chain, so the tx is invalid and the block with it. Below the checkpoint
// (script validation skipped, outpoint-only spends) the block is valid by
// definition and the answer stays a non-verdict error.
func TestValidatePrunedReplayInABlock(t *testing.T) {
	tracing.SetupMockTracer()

	for _, n := range []int{2, 11} {
		t.Run(fmt.Sprintf("%d inputs", n), func(t *testing.T) {
			ctx := context.Background()
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.Disabled = true
			tSettings.BatcherDrainMode = true
			tSettings.Pruner.UTXODefensiveEnabled = false

			utxoStoreURL, err := url.Parse(fmt.Sprintf("sqlitememory:///pruned_replay_in_block_%d", n))
			require.NoError(t, err)

			store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
			require.NoError(t, err)

			sql.ResetPrunerServiceForTests()
			t.Cleanup(sql.ResetPrunerServiceForTests)

			require.NoError(t, store.SetBlockHeight(1000))
			require.NoError(t, store.SetMedianBlockTime(1700000000))

			privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("PRUNED_REPLAY_IN_BLOCK_TEST_KEY!"))

			mined := func(height uint32, txs ...*bt.Tx) {
				hashes := make([]*chainhash.Hash, 0, len(txs))
				for _, tx := range txs {
					hashes = append(hashes, tx.TxIDChainHash())
				}

				_, minedErr := store.SetMinedMulti(ctx, hashes, utxostore.MinedBlockInfo{BlockID: height, BlockHeight: height, OnLongestChain: true})
				require.NoError(t, minedErr)
			}

			// Coinbase parents mined at height 1, mature long before 1400. Output
			// 1 stays unspent so each parent survives the prune.
			parents := make([]*bt.Tx, n)
			cInputs := make([]transactions.TxOption, 0, n)
			xInputs := make([]transactions.TxOption, 0, n-1)

			for i := range parents {
				parents[i] = transactions.Create(t,
					transactions.WithCoinbaseData(1, fmt.Sprintf("/pruned replay parent %d/", i)),
					transactions.WithP2PKHOutputs(2, 10_000, publicKey),
				)
				_, err = store.Create(ctx, parents[i], 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1, BlockHeight: 1, OnLongestChain: true}))
				require.NoError(t, err)

				cInputs = append(cInputs, transactions.WithInput(parents[i], 0, privateKey))
				if i < n-1 {
					xInputs = append(xInputs, transactions.WithInput(parents[i], 0, privateKey))
				}
			}

			c := transactions.Create(t, append(cInputs, transactions.WithPrivateKey(privateKey), transactions.WithP2PKHOutputs(1, uint64(n)*9_000, publicKey))...) //nolint:gosec // small test count
			_, _, err = store.SpendAndCreate(ctx, c, 1000)
			require.NoError(t, err)
			mined(1000, c)

			g := transactions.Create(t, transactions.WithPrivateKey(privateKey), transactions.WithInput(c, 0, privateKey), transactions.WithP2PKHOutputs(1, uint64(n)*8_000, publicKey)) //nolint:gosec // small test count
			_, _, err = store.SpendAndCreate(ctx, g, 1001)
			require.NoError(t, err)
			mined(1001, g)

			// Release the first n-1 outputs and let X take them, so the prune
			// marks only the last parent, the one output that still names C.
			for i := 0; i < n-1; i++ {
				hash, hashErr := util.UTXOHashFromOutput(parents[i].TxIDChainHash(), parents[i].Outputs[0], 0)
				require.NoError(t, hashErr)
				require.NoError(t, store.Unspend(ctx, []*utxostore.Spend{{
					TxID: parents[i].TxIDChainHash(), Vout: 0, UTXOHash: hash, SpendingData: spendpkg.NewSpendingData(c.TxIDChainHash(), i),
				}}))
			}

			x := transactions.Create(t, append(xInputs, transactions.WithPrivateKey(privateKey), transactions.WithP2PKHOutputs(1, uint64(n-1)*9_000, publicKey))...) //nolint:gosec // small test count
			_, _, err = store.SpendAndCreate(ctx, x, 1400)
			require.NoError(t, err, "fixture: X takes the released outputs")

			svc, err := store.GetPrunerService()
			require.NoError(t, err)
			_, err = svc.Prune(ctx, 1001+tSettings.GetUtxoStoreBlockHeightRetention()+10, "prune-c")
			require.NoError(t, err)

			_, err = store.Get(ctx, c.TxIDChainHash())
			require.ErrorIs(t, err, errors.ErrTxNotFound, "fixture: C is pruned")

			v := &Validator{
				logger:      logger,
				utxoStore:   store,
				settings:    tSettings,
				txValidator: NewTxValidator(logger, tSettings),
				stats:       gocore.NewStat("validator"),
			}

			t.Run("above the checkpoint", func(t *testing.T) {
				opts := NewDefaultOptions()
				opts.CreateConflicting = true
				opts.SkipPolicyChecks = true
				opts.CandidateParentMedianTime = 1700000000

				_, err := v.ValidateWithOptions(ctx, c, 1400, opts)
				require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "the replay is reported as one, past the error cap")
				require.ErrorIs(t, err, errors.ErrTxInvalid, "a block re-including a pruned transaction is invalid")
				require.NotErrorIs(t, err, errors.ErrTxConflicting)
			})

			t.Run("below the checkpoint", func(t *testing.T) {
				tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}

				opts := NewDefaultOptions()
				opts.CreateConflicting = true
				opts.SkipPolicyChecks = true
				opts.SkipScriptValidation = true
				opts.OutpointOnlySpend = true
				opts.IgnoreLocked = true
				opts.CandidateParentMedianTime = 1700000000

				_, err := v.ValidateWithOptions(ctx, c, 1400, opts)
				require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "the replay is reported as one, past the error cap")
				require.NotErrorIs(t, err, errors.ErrTxInvalid, "below the checkpoint a marker answer never condemns the block")
			})
		})
	}
}
