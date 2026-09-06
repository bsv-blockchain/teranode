package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// These tests pin the one-wave apply: a transaction with no parent in this block has its
// inputs spent and its outputs created by ONE store call, while a transaction that spends a
// sibling keeps the two waves it has always needed. They run against the sqlitememory UTXO
// store, so "applied" means the rows are really there, and wrap it in a recorder so the shape
// of every call — combined, create-only, spend-only — is observable.

// recordedApply is the shape of one SpendAndCreate the batch made.
type recordedApply struct {
	txID       chainhash.Hash
	createOnly bool
	spendOnly  bool
	err        error
}

// combined reports whether the call was the one-wave form: neither half suppressed.
func (r recordedApply) combined() bool { return !r.createOnly && !r.spendOnly }

// applyRecorder is a real utxo.Store that also remembers every SpendAndCreate made through it.
//
// failCombined, when set, makes every combined (one-wave) call fail without reaching the store,
// which is how the barrier test forces the one-wave apply to lose.
type applyRecorder struct {
	utxo.Store

	failCombined      error
	failCombinedDelay time.Duration

	mu    sync.Mutex
	calls []recordedApply
}

func (a *applyRecorder) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	options := parseCreateOptions(opts)

	if a.failCombined != nil && !options.CreateOnly && !options.SpendOnly {
		// The delay is what makes the barrier test a real discriminator. It lets the chained
		// create wave finish and park on the barrier BEFORE the one-wave apply reports its
		// failure, so a barrier released on failure has a waiter to release.
		if a.failCombinedDelay > 0 {
			time.Sleep(a.failCombinedDelay)
		}

		a.mu.Lock()
		a.calls = append(a.calls, recordedApply{txID: *tx.TxIDChainHash(), err: a.failCombined})
		a.mu.Unlock()

		return nil, nil, a.failCombined
	}

	md, spends, err := a.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)

	a.mu.Lock()
	a.calls = append(a.calls, recordedApply{
		txID:       *tx.TxIDChainHash(),
		createOnly: options.CreateOnly,
		spendOnly:  options.SpendOnly,
		err:        err,
	})
	a.mu.Unlock()

	return md, spends, err
}

// callsFor returns every recorded call for one transaction.
func (a *applyRecorder) callsFor(h *chainhash.Hash) []recordedApply {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make([]recordedApply, 0, 2)

	for _, c := range a.calls {
		if c.txID == *h {
			out = append(out, c)
		}
	}

	return out
}

// spendOnlyCalls counts the calls that carried WithSpendOnly, whatever their transaction.
func (a *applyRecorder) spendOnlyCalls() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	n := 0

	for _, c := range a.calls {
		if c.spendOnly {
			n++
		}
	}

	return n
}

// spendCallsFor counts the spend-only calls recorded for one transaction. A block that was
// stopped before its spend wave ran has none.
func (a *applyRecorder) spendCallsFor(tx *bt.Tx) int {
	a.mu.Lock()
	defer a.mu.Unlock()

	n := 0

	for _, c := range a.calls {
		if c.spendOnly && c.txID == *tx.TxIDChainHash() {
			n++
		}
	}

	return n
}

func (a *applyRecorder) reset() {
	a.mu.Lock()
	a.calls = nil
	a.mu.Unlock()
}

// newOneWaveHarness builds a BlockValidation over a fresh sqlitememory UTXO store wrapped in
// the recorder. dbName must be unique per test so two tests never share a database.
func newOneWaveHarness(t *testing.T, dbName string) (*BlockValidation, *applyRecorder, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	logger := ulogger.TestLogger{}
	tSettings := testutil.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)

	realStore, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	recorder := &applyRecorder{Store: realStore}

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		utxoStore:                     recorder,
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		spendRetryBackoff:             time.Millisecond,
	}

	cleanup := func() {
		bv.blockExistsCache.Stop()
		bv.lastValidatedBlocks.Stop()
		cancel()
	}

	return bv, recorder, cleanup
}

// oneWaveBatchFor builds a batch out of txs, one subtree per transaction, and runs the real
// extend stage over it so the in-block-parent partition is derived by production code rather
// than asserted into place.
func oneWaveBatchFor(t *testing.T, bv *BlockValidation, block *model.Block, txs []*bt.Tx) *SubtreeProcessingBatch {
	t.Helper()

	batch := &SubtreeProcessingBatch{
		subtreeData:  make([]*subtreepkg.Data, len(txs)),
		txRanges:     make([][2]int, len(txs)),
		batchTxs:     make([]*bt.Tx, 0, len(txs)),
		batchStart:   0,
		batchEnd:     len(txs),
		outpointOnly: false,
	}

	for i, tx := range txs {
		batch.subtreeData[i] = &subtreepkg.Data{Txs: []*bt.Tx{tx}}
	}

	require.NoError(t, bv.extendBatch(context.Background(), block, batch, map[chainhash.Hash]*bt.Tx{}))
	require.Len(t, batch.hasInBlockParent, len(txs), "the extend stage must answer for every tx")

	return batch
}

// seedRoot writes a coinbase-shaped transaction with nOutputs spendable outputs into the store
// the way an already-mined ancestor sits there, and returns it with the key that unlocks it.
func seedRoot(t *testing.T, store utxo.Store, nOutputs int, seed string) (*bt.Tx, *bec.PrivateKey) {
	t.Helper()

	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte(seed))

	root := transactions.Create(t,
		transactions.WithCoinbaseData(1, "/one-wave/"),
		transactions.WithP2PKHOutputs(nOutputs, 100_000, publicKey),
	)

	_, _, err := store.SpendAndCreate(context.Background(), root, 0,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1}), utxo.WithCreateOnly())
	require.NoError(t, err)

	return root, privateKey
}

// spendOf builds a transaction spending one output of parent.
func spendOf(t *testing.T, key *bec.PrivateKey, parent *bt.Tx, vout uint32, sats uint64) *bt.Tx {
	t.Helper()

	_, publicKey := bec.PrivateKeyFromBytes([]byte("one-wave-out"))

	return transactions.Create(t,
		transactions.WithPrivateKey(key),
		transactions.WithInput(parent, vout),
		transactions.WithP2PKHOutputs(1, sats, publicKey),
	)
}

// requireSpentBy asserts one output of tx was taken by spender.
//
// The assertion is on the spending data rather than on the status enum: quick validation
// creates a block's coins locked, and the SQL store reports a locked transaction's outputs as
// LOCKED whatever their spending data says. The spending data is the fact under test.
func requireSpentBy(t *testing.T, store utxo.Store, tx *bt.Tx, vout uint32, spender *bt.Tx) {
	t.Helper()

	resp, err := store.GetSpend(context.Background(), &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: vout})
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData,
		"output %d of %s must be spent", vout, tx.TxIDChainHash().String())
	require.Equal(t, spender.TxIDChainHash().String(), resp.SpendingData.TxID.String(),
		"output %d of %s must be spent by %s", vout, tx.TxIDChainHash().String(), spender.TxIDChainHash().String())
}

// TestOneWave_NoInBlockParentsTakesOneCall is case (a): every transaction of the batch spends
// only coins already in the store, so every one of them is applied by a single combined call
// and neither WithCreateOnly nor WithSpendOnly is ever passed.
func TestOneWave_NoInBlockParentsTakesOneCall(t *testing.T) {
	bv, recorder, cleanup := newOneWaveHarness(t, "one_wave_independent")
	defer cleanup()

	ctx := context.Background()

	root, key := seedRoot(t, recorder.Store, 3, "ONE_WAVE_INDEPENDENT_KEY")

	txs := []*bt.Tx{
		spendOf(t, key, root, 0, 90_000),
		spendOf(t, key, root, 1, 90_000),
		spendOf(t, key, root, 2, 90_000),
	}

	block := &model.Block{Height: 100, ID: 42}
	batch := oneWaveBatchFor(t, bv, block, txs)

	for i := range txs {
		require.False(t, batch.hasInBlockParent[i], "tx %d spends nothing of this block", i)
	}

	recorder.reset()
	require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, batch))

	for i, tx := range txs {
		calls := recorder.callsFor(tx.TxIDChainHash())
		require.Len(t, calls, 1, "tx %d must be applied by exactly one call", i)
		require.True(t, calls[0].combined(),
			"tx %d must take the combined call, got createOnly=%v spendOnly=%v", i, calls[0].createOnly, calls[0].spendOnly)
		require.NoError(t, calls[0].err)

		// Created.
		_, err := recorder.Store.Get(ctx, tx.TxIDChainHash())
		require.NoError(t, err, "tx %d must be in the store", i)

		// And its input spent.
		requireSpentBy(t, recorder.Store, root, uint32(i), tx)
	}
}

// TestOneWave_ChainedAndIndependentMix is case (b): a three-deep chain alongside transactions
// with no in-block parent. The chained ones keep the two waves, the independent ones take one
// call, and everything ends applied. Run five times because the two waves now run at the same
// time and the result must not depend on which finishes first.
func TestOneWave_ChainedAndIndependentMix(t *testing.T) {
	for run := 0; run < 5; run++ {
		t.Run(fmt.Sprintf("run %d", run), func(t *testing.T) {
			bv, recorder, cleanup := newOneWaveHarness(t, fmt.Sprintf("one_wave_mixed_%d", run))
			defer cleanup()

			ctx := context.Background()

			root, key := seedRoot(t, recorder.Store, 3, "ONE_WAVE_MIXED_KEY")

			// c1 -> c2 -> c3 is the chain; c1's parent is the store's, c2 and c3 spend a sibling.
			c1 := spendOf(t, key, root, 0, 90_000)
			c2 := spendOf(t, key, c1, 0, 80_000)
			c3 := spendOf(t, key, c2, 0, 70_000)

			// i1 and i2 spend the store's coins only.
			i1 := spendOf(t, key, root, 1, 90_000)
			i2 := spendOf(t, key, root, 2, 90_000)

			txs := []*bt.Tx{c1, c2, c3, i1, i2}

			block := &model.Block{Height: 100, ID: 42}
			batch := oneWaveBatchFor(t, bv, block, txs)

			require.Equal(t, []bool{false, true, true, false, false}, batch.hasInBlockParent,
				"only c2 and c3 spend a sibling")

			recorder.reset()
			require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, batch))

			for _, tx := range []*bt.Tx{c1, i1, i2} {
				calls := recorder.callsFor(tx.TxIDChainHash())
				require.Len(t, calls, 1, "an independent tx takes one call")
				require.True(t, calls[0].combined(), "and that call is the combined one")
			}

			for _, tx := range []*bt.Tx{c2, c3} {
				calls := recorder.callsFor(tx.TxIDChainHash())
				require.Len(t, calls, 2, "a chained tx takes two calls")

				var sawCreateOnly, sawSpendOnly bool

				for _, c := range calls {
					sawCreateOnly = sawCreateOnly || c.createOnly
					sawSpendOnly = sawSpendOnly || c.spendOnly
				}

				require.True(t, sawCreateOnly, "a chained tx keeps its create wave")
				require.True(t, sawSpendOnly, "a chained tx keeps its spend wave")
			}

			for _, tx := range txs {
				_, err := recorder.Store.Get(ctx, tx.TxIDChainHash())
				require.NoError(t, err, "%s must be in the store", tx.TxIDChainHash().String())
			}

			requireSpentBy(t, recorder.Store, root, 0, c1)
			requireSpentBy(t, recorder.Store, root, 1, i1)
			requireSpentBy(t, recorder.Store, root, 2, i2)
			requireSpentBy(t, recorder.Store, c1, 0, c2)
			requireSpentBy(t, recorder.Store, c2, 0, c3)
		})
	}
}

// TestOneWave_ReplayStampsAndCreatesNothingTwice is case (c): applying the same batch a second
// time succeeds, every transaction comes back as already existing rather than being created
// again, and the block facts are restamped.
func TestOneWave_ReplayStampsAndCreatesNothingTwice(t *testing.T) {
	bv, recorder, cleanup := newOneWaveHarness(t, "one_wave_replay")
	defer cleanup()

	ctx := context.Background()

	root, key := seedRoot(t, recorder.Store, 2, "ONE_WAVE_REPLAY_KEY")

	independent := spendOf(t, key, root, 0, 90_000)
	chained := spendOf(t, key, independent, 0, 80_000)
	other := spendOf(t, key, root, 1, 90_000)

	txs := []*bt.Tx{independent, chained, other}

	block := &model.Block{Height: 100, ID: 42}

	first := oneWaveBatchFor(t, bv, block, txs)
	require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, first))

	// The same block offered again, exactly as a dirty restart offers it.
	replayBlock := &model.Block{Height: 100, ID: 43}
	second := oneWaveBatchFor(t, bv, replayBlock, txs)

	recorder.reset()
	require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, replayBlock, second),
		"a replayed batch must apply cleanly")

	for _, tx := range txs {
		calls := recorder.callsFor(tx.TxIDChainHash())
		require.NotEmpty(t, calls)

		sawExists := false

		for _, c := range calls {
			if c.spendOnly {
				continue // the spend half of a replay reports nothing
			}

			sawExists = sawExists || errors.Is(c.err, errors.ErrTxExists)
		}

		require.True(t, sawExists,
			"%s must be reported as already existing, not created a second time", tx.TxIDChainHash().String())
	}

	// And the restamp landed: the mined-info update ran for the whole union.
	for _, tx := range txs {
		md, err := recorder.Store.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
		require.NoError(t, err)
		require.Contains(t, md.BlockIDs, uint32(43), "%s must carry the replayed block id", tx.TxIDChainHash().String())
	}
}

// TestOneWave_MissingParentFailsTheBlock is case (d): a transaction whose parent is in neither
// the block nor the store has no coin to spend, and the one-wave apply must fail the block
// rather than let it through.
func TestOneWave_MissingParentFailsTheBlock(t *testing.T) {
	bv, recorder, cleanup := newOneWaveHarness(t, "one_wave_missing_parent")
	defer cleanup()

	ctx := context.Background()

	// A parent that is never written anywhere.
	_, publicKey := bec.PrivateKeyFromBytes([]byte("ONE_WAVE_MISSING_PARENT_KEY"))
	privateKey, _ := bec.PrivateKeyFromBytes([]byte("ONE_WAVE_MISSING_PARENT_KEY"))

	ghost := transactions.Create(t,
		transactions.WithCoinbaseData(1, "/ghost/"),
		transactions.WithP2PKHOutputs(1, 100_000, publicKey),
	)

	orphan := spendOf(t, privateKey, ghost, 0, 90_000)

	block := &model.Block{Height: 100, ID: 42}
	batch := oneWaveBatchFor(t, bv, block, []*bt.Tx{orphan})

	require.False(t, batch.hasInBlockParent[0], "the missing parent is not in this block either")

	recorder.reset()

	err := bv.createAndSpendUTXOsForBatch(ctx, block, batch)
	require.Error(t, err, "a transaction with no parent anywhere must fail the block")
	require.True(t, errors.Is(err, errors.ErrTxNotFound),
		"the failure must be the not-found class, got: %v", err)
	require.Contains(t, err.Error(), fmt.Sprintf("%s:0", ghost.TxIDChainHash().String()),
		"the failure must name the missing outpoint, got: %v", err)
}

// TestOneWave_FailedApplyReleasesNoChainedSpends pins the barrier's failure side.
//
// The chained spend wave must not start when the one-wave apply has failed: the one-wave set
// is then only partly applied, and a chained transaction may spend the output of an
// independent sibling whose create never landed. The two-phase code it replaces guaranteed
// zero spends after a create-phase failure, and that guarantee has to survive the two waves
// running at the same time.
//
// What this test can and cannot prove. It catches any DETERMINISTIC release of the barrier on
// a failed apply — a barrier closed before the apply runs, or closed unconditionally with the
// gCtx guards gone. It did NOT go red against the shape it replaced, a plain
// `defer close(oneWaveDone)`, in 100 attempts: applyTxsWithRetry checks ctx.Err() before it
// issues anything, so the exposure is only the nanoseconds between the barrier closing and
// the errgroup cancelling its context, and the spend wave loses that race every time. The fix
// is therefore hardening of a reasoning hazard rather than of a reproduced failure, and this
// test is its regression guard, not its proof.
//
// failCombinedDelay makes the chained create wave finish and park on the barrier before the
// one-wave apply reports its failure, which is the widest window the black box allows.
func TestOneWave_FailedApplyReleasesNoChainedSpends(t *testing.T) {
	for run := 0; run < 5; run++ {
		t.Run(fmt.Sprintf("run %d", run), func(t *testing.T) {
			bv, recorder, cleanup := newOneWaveHarness(t, fmt.Sprintf("one_wave_barrier_%d", run))
			defer cleanup()

			ctx := context.Background()

			root, key := seedRoot(t, recorder.Store, 3, "ONE_WAVE_BARRIER_KEY")

			// c1 is independent and c2 spends it, so c2's spend depends on c1's create — which
			// is exactly the create the failing one-wave apply never lands.
			c1 := spendOf(t, key, root, 0, 90_000)
			c2 := spendOf(t, key, c1, 0, 80_000)
			i1 := spendOf(t, key, root, 1, 90_000)

			block := &model.Block{Height: 100, ID: 42}
			batch := oneWaveBatchFor(t, bv, block, []*bt.Tx{c1, c2, i1})

			require.Equal(t, []bool{false, true, false}, batch.hasInBlockParent)

			recorder.reset()
			recorder.failCombined = errors.NewProcessingError("forced one-wave failure")
			recorder.failCombinedDelay = 50 * time.Millisecond

			err := bv.createAndSpendUTXOsForBatch(ctx, block, batch)
			require.Error(t, err, "a failed one-wave apply must fail the block")

			require.Zero(t, recorder.spendOnlyCalls(),
				"no chained spend may be issued after the one-wave apply failed")
			require.Contains(t, err.Error(), "forced one-wave failure",
				"the apply's own failure must surface, not a context error masking it")
		})
	}
}
