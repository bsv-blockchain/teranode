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
	"github.com/bsv-blockchain/go-chaincfg"
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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// These tests pin the list apply: a batch goes to the store as ONE SpendAndCreateMulti list,
// in block order, each transaction carrying its own subtree index. They run against the
// sqlitememory UTXO store, so "applied" means the rows are really there, and wrap it in a
// recorder so every list, and every per-transaction call, is observable.

// recordedApply is the shape of one SpendAndCreate the batch made.
type recordedApply struct {
	txID       chainhash.Hash
	createOnly bool
	spendOnly  bool
	err        error
}

// combined reports whether the call both spent and created: neither half suppressed.
func (r recordedApply) combined() bool { return !r.createOnly && !r.spendOnly }

// applyRecorder is a real utxo.Store that also remembers every SpendAndCreate made through it.
type applyRecorder struct {
	utxo.Store

	// failMulti, when set, fails the next failMultiTimes SpendAndCreateMulti calls before they
	// reach the store.
	failMulti      error
	failMultiTimes int

	// failApply, when set, fails every SpendAndCreate before it reaches the store.
	failApply error

	mu    sync.Mutex
	calls []recordedApply
	lists []recordedList
}

// recordedList is one SpendAndCreateMulti the batch made.
type recordedList struct {
	txids       []chainhash.Hash
	subtreeIdxs []int
	results     []utxo.SpendAndCreateMultiResult
	err         error
}

func (a *applyRecorder) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	options := parseCreateOptions(opts)

	rec := recordedList{subtreeIdxs: options.SubtreeIdxs}
	for _, tx := range txs {
		rec.txids = append(rec.txids, *tx.TxIDChainHash())
	}

	a.mu.Lock()
	injected := a.failMulti
	if injected != nil {
		a.failMultiTimes--
		if a.failMultiTimes <= 0 {
			a.failMulti = nil
		}
	}
	a.mu.Unlock()

	if injected != nil {
		rec.err = injected

		a.mu.Lock()
		a.lists = append(a.lists, rec)
		a.mu.Unlock()

		return nil, injected
	}

	results, err := a.Store.SpendAndCreateMulti(ctx, txs, blockHeight, opts...)
	rec.results, rec.err = results, err

	a.mu.Lock()
	a.lists = append(a.lists, rec)
	a.mu.Unlock()

	return results, err
}

// recordedLists returns every SpendAndCreateMulti recorded so far.
func (a *applyRecorder) recordedLists() []recordedList {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]recordedList(nil), a.lists...)
}

func (a *applyRecorder) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	options := parseCreateOptions(opts)

	if a.failApply != nil {
		return nil, nil, a.failApply
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
	a.lists = nil
	a.mu.Unlock()
}

// newListHarness builds a BlockValidation over a fresh sqlitememory UTXO store wrapped in
// the recorder. dbName must be unique per test so two tests never share a database.
//
// outpointOnly runs the harness in the below-checkpoint mode that ships on the Hetzner nodes:
// the operator flag on and a checkpoint above the fixture height, because regtest ships no
// checkpoints and createAndSpendUTXOsForBatch's invariant I4 fails any outpoint-only batch
// above the highest one before it applies anything. CreateBaseTestSettings copies the chain
// params per call, so the checkpoint leaks into no other test.
func newListHarness(t *testing.T, dbName string, outpointOnly bool) (*BlockValidation, *applyRecorder, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	logger := ulogger.TestLogger{}
	tSettings := testutil.CreateBaseTestSettings(t)

	if outpointOnly {
		tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
		tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1000}}
	}

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

// listBatchForErr is listBatchFor without the require dependency. outpointOnly is the
// per-block mode the batch carries, the same value that drives create and spend below and
// that the I4 guard keys on.
func listBatchForErr(bv *BlockValidation, block *model.Block, txs []*bt.Tx, outpointOnly bool) (*SubtreeProcessingBatch, error) {
	batch := &SubtreeProcessingBatch{
		subtreeData:  make([]*subtreepkg.Data, len(txs)),
		txRanges:     make([][2]int, len(txs)),
		batchTxs:     make([]*bt.Tx, 0, len(txs)),
		batchStart:   0,
		batchEnd:     len(txs),
		outpointOnly: outpointOnly,
	}

	for i, tx := range txs {
		batch.subtreeData[i] = &subtreepkg.Data{Txs: []*bt.Tx{tx}}
	}

	if err := bv.extendBatch(context.Background(), block, batch, map[chainhash.Hash]*bt.Tx{}); err != nil {
		return nil, err
	}

	return batch, nil
}

// listBatchFor builds a batch out of txs, one subtree per transaction, and runs the real
// extend stage over it so the in-block-parent partition is derived by production code rather
// than asserted into place.

func listBatchFor(t *testing.T, bv *BlockValidation, block *model.Block, txs []*bt.Tx, outpointOnly bool) *SubtreeProcessingBatch {
	t.Helper()

	batch, err := listBatchForErr(bv, block, txs, outpointOnly)
	require.NoError(t, err)

	return batch
}

// seedRoot writes a coinbase-shaped transaction with nOutputs spendable outputs into the store
// the way an already-mined ancestor sits there, and returns it with the key that unlocks it.
func seedRoot(t *testing.T, store utxo.Store, nOutputs int, seed string) (*bt.Tx, *bec.PrivateKey) {
	t.Helper()

	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte(seed))

	root := transactions.Create(t,
		transactions.WithCoinbaseData(1, "/netted/"),
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

	_, publicKey := bec.PrivateKeyFromBytes([]byte("netted-out"))

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

// TestBatchList_BatchIsOneListInBlockOrder: the whole batch goes to the store as one list, in
// block order, each transaction with the subtree index it sits in, and every transaction ends
// created with its input spent.
func TestBatchList_BatchIsOneListInBlockOrder(t *testing.T) {
	bv, recorder, cleanup := newListHarness(t, "netted_one_list", false)
	defer cleanup()

	ctx := context.Background()

	root, key := seedRoot(t, recorder.Store, 3, "NETTED_ONE_LIST_KEY")

	txs := []*bt.Tx{
		spendOf(t, key, root, 0, 90_000),
		spendOf(t, key, root, 1, 90_000),
		spendOf(t, key, root, 2, 90_000),
	}

	block := &model.Block{Height: 100, ID: 42}
	batch := listBatchFor(t, bv, block, txs, false)

	recorder.reset()
	require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, batch))

	lists := recorder.recordedLists()
	require.Len(t, lists, 1, "one list for the batch")
	require.Equal(t, []int{0, 1, 2}, lists[0].subtreeIdxs, "each transaction's own subtree index")

	for i, tx := range txs {
		require.Equal(t, *tx.TxIDChainHash(), lists[0].txids[i], "block order")

		md, err := recorder.Store.Get(ctx, tx.TxIDChainHash(), fields.SubtreeIdxs, fields.BlockIDs)
		require.NoError(t, err, "tx %d must be in the store", i)
		require.Equal(t, []int{i}, md.SubtreeIdxs, "tx %d recorded in its own subtree", i)
		require.Equal(t, []uint32{42}, md.BlockIDs)

		requireSpentBy(t, recorder.Store, root, uint32(i), tx) //nolint:gosec // test index
	}
}

// TestBatchList_ChainedAndIndependentMix: a three-deep chain beside transactions with no parent in
// the block all end applied from the one list, every spend naming its child.
func TestBatchList_ChainedAndIndependentMix(t *testing.T) {
	listModes(t, "netted_mixed", func(t *testing.T, dbName string, outpointOnly bool) {
		bv, recorder, cleanup := newListHarness(t, dbName, outpointOnly)
		defer cleanup()

		ctx := context.Background()

		root, key := seedRoot(t, recorder.Store, 3, "NETTED_MIXED_KEY")

		c1 := spendOf(t, key, root, 0, 90_000)
		c2 := spendOf(t, key, c1, 0, 80_000)
		c3 := spendOf(t, key, c2, 0, 70_000)
		i1 := spendOf(t, key, root, 1, 90_000)
		i2 := spendOf(t, key, root, 2, 90_000)

		txs := []*bt.Tx{c1, c2, c3, i1, i2}

		block := &model.Block{Height: 100, ID: 42}
		batch := listBatchFor(t, bv, block, txs, outpointOnly)

		recorder.reset()
		require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, batch))
		require.Len(t, recorder.recordedLists(), 1)

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

// listModes runs a test once in decorate mode and once in the outpoint-only mode that ships,
// each over its own database.
func listModes(t *testing.T, dbName string, run func(t *testing.T, dbName string, outpointOnly bool)) {
	t.Helper()

	for _, outpointOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("outpointOnly=%v", outpointOnly), func(t *testing.T) {
			run(t, fmt.Sprintf("%s_outpoint_only_%v", dbName, outpointOnly), outpointOnly)
		})
	}
}

// TestBatchList_ReplayStampsAndCreatesNothingTwice: applying the same batch again, as a dirty
// restart does, succeeds, every transaction comes back as already existing, and the block facts
// are restamped.
func TestBatchList_ReplayStampsAndCreatesNothingTwice(t *testing.T) {
	listModes(t, "netted_replay", func(t *testing.T, dbName string, outpointOnly bool) {
		bv, recorder, cleanup := newListHarness(t, dbName, outpointOnly)
		defer cleanup()

		ctx := context.Background()

		root, key := seedRoot(t, recorder.Store, 2, "NETTED_REPLAY_KEY")

		first := spendOf(t, key, root, 0, 90_000)
		chained := spendOf(t, key, first, 0, 80_000)
		other := spendOf(t, key, root, 1, 90_000)

		txs := []*bt.Tx{first, chained, other}

		block := &model.Block{Height: 100, ID: 42}
		require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, listBatchFor(t, bv, block, txs, outpointOnly)))

		replayBlock := &model.Block{Height: 100, ID: 43}
		second := listBatchFor(t, bv, replayBlock, txs, outpointOnly)

		recorder.reset()
		require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, replayBlock, second), "a replayed batch must apply cleanly")

		lists := recorder.recordedLists()
		require.Len(t, lists, 1)

		for i, r := range lists[0].results {
			require.Equal(t, utxo.MultiTxExisted, r.Status, "tx %d must be reported as already existing, not created again", i)
		}

		for _, tx := range txs {
			md, err := recorder.Store.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
			require.NoError(t, err)
			require.Contains(t, md.BlockIDs, uint32(43), "%s must carry the replayed block id", tx.TxIDChainHash().String())
		}
	})
}

// TestBatchList_MissingParentFailsTheBlock: a transaction whose parent is in neither the block nor
// the store fails the block with the not-found class naming the outpoint, and is not created.
//
// Outpoint-only mode only: in the normal mode the extension step reads every parent first, so a
// missing parent fails there, before the list reaches the store.
func TestBatchList_MissingParentFailsTheBlock(t *testing.T) {
	const outpointOnly = true

	{
		bv, recorder, cleanup := newListHarness(t, "netted_missing_parent", outpointOnly)
		defer cleanup()

		ctx := context.Background()

		privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("NETTED_MISSING_PARENT_KEY"))

		ghost := transactions.Create(t,
			transactions.WithCoinbaseData(1, "/ghost/"),
			transactions.WithP2PKHOutputs(1, 100_000, publicKey),
		)

		orphan := spendOf(t, privateKey, ghost, 0, 90_000)

		block := &model.Block{Height: 100, ID: 42}
		batch := listBatchFor(t, bv, block, []*bt.Tx{orphan}, outpointOnly)

		err := bv.createAndSpendUTXOsForBatch(ctx, block, batch)
		require.Error(t, err, "a transaction with no parent anywhere must fail the block")
		require.True(t, errors.Is(err, errors.ErrTxNotFound), "the failure must be the not-found class, got: %v", err)
		require.Contains(t, err.Error(), fmt.Sprintf("%s:0", ghost.TxIDChainHash().String()),
			"the failure must name the missing outpoint, got: %v", err)

		_, err = recorder.Store.Get(ctx, orphan.TxIDChainHash())
		require.True(t, errors.Is(err, errors.ErrTxNotFound), "the orphan must not have been created, got: %v", err)
	}
}

// TestBatchList_StoreFaultRetriesTheList: a store fault on the list is retried, and the whole list
// is repeated, which is safe because existing records are recognised; a refusal is not retried.
func TestBatchList_StoreFaultRetriesTheList(t *testing.T) {
	bv, recorder, cleanup := newListHarness(t, "netted_retry", true)
	defer cleanup()

	ctx := context.Background()

	root, key := seedRoot(t, recorder.Store, 2, "NETTED_RETRY_KEY")

	txs := []*bt.Tx{spendOf(t, key, root, 0, 90_000), spendOf(t, key, root, 1, 90_000)}

	block := &model.Block{Height: 100, ID: 42}
	batch := listBatchFor(t, bv, block, txs, true)

	recorder.reset()
	recorder.failMulti = errors.NewStorageError("forced store fault")
	recorder.failMultiTimes = 2

	require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, batch))
	require.Len(t, recorder.recordedLists(), 3, "two faults, then the list goes through")

	for i, tx := range txs {
		requireSpentBy(t, recorder.Store, root, uint32(i), tx) //nolint:gosec // test index
	}
}

// TestBatchList_CancelStopsTheBackoff: a block cancelled while the list, or the one-at-a-time
// fallback for a refused list, waits out a retry backoff stops at once rather than sleeping the
// backoff out, fails the batch, and leaves none of its transactions in the store.
func TestBatchList_CancelStopsTheBackoff(t *testing.T) {
	const backoff = 30 * time.Second

	for _, tc := range []struct {
		name    string
		refused bool
	}{
		{name: "list", refused: false},
		{name: "one at a time", refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bv, recorder, cleanup := newListHarness(t, fmt.Sprintf("list_cancel_refused_%v", tc.refused), true)
			defer cleanup()

			bv.spendRetryBackoff = backoff

			root, key := seedRoot(t, recorder.Store, 1, "LIST_CANCEL_KEY")
			tx := spendOf(t, key, root, 0, 90_000)

			// The same transaction twice is a list the store refuses, which sends the batch
			// one transaction at a time.
			txs := []*bt.Tx{tx}
			if tc.refused {
				txs = append(txs, tx)
			}

			recorder.reset()

			fault := errors.NewStorageError("forced store fault")
			if tc.refused {
				recorder.failApply = fault
			} else {
				recorder.failMulti = fault
				recorder.failMultiTimes = 1_000
			}

			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(100*time.Millisecond, cancel)

			block := &model.Block{Height: 100, ID: 42}
			txids := make([]chainhash.Hash, len(txs))

			for i, x := range txs {
				txids[i] = *x.TxIDChainHash()
			}

			start := time.Now()
			err := bv.applyList(ctx, block, txs, txids, make([]int, len(txs)), true, true, func(*bt.Tx) {})
			elapsed := time.Since(start)

			require.Error(t, err)
			require.Less(t, elapsed, backoff/2, "cancellation must cut the backoff short")

			_, getErr := recorder.Store.Get(context.Background(), tx.TxIDChainHash())
			require.True(t, errors.Is(getErr, errors.ErrTxNotFound), "a cancelled batch writes nothing: %v", getErr)
		})
	}
}

// TestBatchList_RetryableParentFailureRetriesTheList: a parent that fails with a retryable store
// error marks its child ParentFailed, which is not itself retryable. The list must be retried
// rather than the block failed, as the per-transaction waves retried it.
func TestBatchList_RetryableParentFailureRetriesTheList(t *testing.T) {
	store := &utxo.MockUtxostore{}

	bv := &BlockValidation{
		logger:            ulogger.TestLogger{},
		settings:          testutil.CreateBaseTestSettings(t),
		utxoStore:         store,
		spendRetryBackoff: time.Millisecond,
	}

	_, publicKey := bec.PrivateKeyFromBytes([]byte("NETTED_RETRYABLE_KEY"))
	parent := transactions.Create(t, transactions.WithCoinbaseData(1, "/p/"), transactions.WithP2PKHOutputs(1, 1_000, publicKey))
	child := transactions.Create(t, transactions.WithCoinbaseData(2, "/c/"), transactions.WithP2PKHOutputs(1, 1_000, publicKey))
	txs := []*bt.Tx{parent, child}
	txids := []chainhash.Hash{*parent.TxIDChainHash(), *child.TxIDChainHash()}

	store.On("SpendAndCreateMulti", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]utxo.SpendAndCreateMultiResult{
		{Status: utxo.MultiTxFailed, Err: errors.NewStorageError("transient")},
		{Status: utxo.MultiTxParentFailed, Err: errors.NewProcessingError("a parent earlier in the list failed")},
	}, nil).Once()
	store.On("SpendAndCreateMulti", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]utxo.SpendAndCreateMultiResult{
		{Status: utxo.MultiTxCreated}, {Status: utxo.MultiTxCreated},
	}, nil).Once()

	block := &model.Block{Height: 100, ID: 42}
	require.NoError(t, bv.applyList(context.Background(), block, txs, txids, []int{0, 0}, false, false, func(*bt.Tx) {}))
	store.AssertNumberOfCalls(t, "SpendAndCreateMulti", 2)
}
