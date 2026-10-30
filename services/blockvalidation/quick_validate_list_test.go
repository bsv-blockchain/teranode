package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
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
	mined []chainhash.Hash
}

// recordedList is one SpendAndCreateMulti the batch made.
type recordedList struct {
	txids           []chainhash.Hash
	subtreeIdxs     []int
	belowCheckpoint bool
	results         []utxo.SpendAndCreateMultiResult
	err             error
}

func (a *applyRecorder) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	options := parseCreateOptions(opts)

	rec := recordedList{subtreeIdxs: options.SubtreeIdxs, belowCheckpoint: options.BelowCheckpoint}
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

// SetMinedMulti remembers every hash stamped with mined info, then forwards to the store.
func (a *applyRecorder) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash,
	minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	a.mu.Lock()
	for _, h := range hashes {
		a.mined = append(a.mined, *h)
	}
	a.mu.Unlock()

	return a.Store.SetMinedMulti(ctx, hashes, minedBlockInfo)
}

// recordedMined returns every hash stamped through SetMinedMulti so far.
func (a *applyRecorder) recordedMined() []chainhash.Hash {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]chainhash.Hash(nil), a.mined...)
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
	a.mined = nil
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
// extend stage over it, so each transaction is extended, or marked as missing a parent, by
// production code rather than by hand.
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

// TestBatchList_DeepChainIsBelowCheckpoint: a chain 50 deep goes to the store as one list marked
// below-checkpoint, which lets the SQL store write it in two waves rather than one round per link
// (the store contract test SpendAndCreateMultiBelowCheckpointTwoWaves pins the waves themselves).
// Every link ends spent by its child.
func TestBatchList_DeepChainIsBelowCheckpoint(t *testing.T) {
	listModes(t, "netted_deep_chain", func(t *testing.T, dbName string, outpointOnly bool) {
		bv, recorder, cleanup := newListHarness(t, dbName, outpointOnly)
		defer cleanup()

		ctx := context.Background()

		const depth = 50

		root, key := seedRoot(t, recorder.Store, 1, "NETTED_DEEP_CHAIN_KEY")

		txs := make([]*bt.Tx, 0, depth)
		parent := root

		for i := 0; i < depth; i++ {
			tx := spendOf(t, key, parent, 0, uint64(100_000-1_000*(i+1))) //nolint:gosec // test amounts
			txs = append(txs, tx)
			parent = tx
		}

		block := &model.Block{Height: 100, ID: 42}
		batch := listBatchFor(t, bv, block, txs, outpointOnly)

		recorder.reset()

		start := time.Now()
		require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, batch))
		t.Logf("a chain %d deep applied in %v", depth, time.Since(start))

		// One list for the batch. The SQLite store can fail the first transaction's combined
		// call with a lock conflict against the creates running beside it, which is retryable, so
		// the same list may be sent again; it is never a different list.
		lists := recorder.recordedLists()
		require.NotEmpty(t, lists)

		for _, l := range lists {
			require.Equal(t, lists[0].txids, l.txids, "a repeat sends the same list")
			require.True(t, l.belowCheckpoint, "quick validation marks its list below-checkpoint")
		}

		requireSpentBy(t, recorder.Store, root, 0, txs[0])

		for i := 1; i < depth; i++ {
			requireSpentBy(t, recorder.Store, txs[i-1], 0, txs[i])
		}
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

		// The store made the existing transactions' spends inside the list, so block validation
		// spends nothing again: on a replayed batch that would be a whole extra round of calls.
		require.Zero(t, recorder.spendOnlyCalls(), "block validation must not spend an existing transaction again")
	})
}

// TestBatchList_ReplayAfterCrashMakesTheMissingSpends: a crash can leave a batch's records
// written with none of their inputs spent, which is what main's create phase left behind when it
// stopped before its spend phase. Replaying the batch must make every spend, and block
// validation must not have to: the store spends an existing transaction's inputs itself.
func TestBatchList_ReplayAfterCrashMakesTheMissingSpends(t *testing.T) {
	listModes(t, "netted_replay_after_crash", func(t *testing.T, dbName string, outpointOnly bool) {
		bv, recorder, cleanup := newListHarness(t, dbName, outpointOnly)
		defer cleanup()

		ctx := context.Background()

		root, key := seedRoot(t, recorder.Store, 2, "NETTED_REPLAY_CRASH_KEY")

		first := spendOf(t, key, root, 0, 90_000)
		chained := spendOf(t, key, first, 0, 80_000)
		other := spendOf(t, key, root, 1, 90_000)

		txs := []*bt.Tx{first, chained, other}

		// The crash: every record written, no input spent.
		for _, tx := range txs {
			_, _, err := recorder.Store.SpendAndCreate(ctx, tx, 100, utxo.WithCreateOnly(),
				utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 100}))
			require.NoError(t, err)
		}

		block := &model.Block{Height: 100, ID: 42}

		recorder.reset()
		require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, listBatchFor(t, bv, block, txs, outpointOnly)))

		requireSpentBy(t, recorder.Store, root, 0, first)
		requireSpentBy(t, recorder.Store, first, 0, chained)
		requireSpentBy(t, recorder.Store, root, 1, other)

		require.Zero(t, recorder.spendOnlyCalls(), "the store makes the missing spends, not block validation")
	})
}

// TestBatchList_MissingParentFailsTheBlock: a transaction whose parent is in neither the block nor
// the store fails the block with the not-found class naming the outpoint.
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

		// The orphan has no parent in the list, so it takes one combined call, which spends before
		// it creates: the failed spend leaves no record behind. The spend carries
		// RequireParentOutputs, so the store does not take a record as proof the missing parent
		// was spent long ago.
		_, err = recorder.Store.Get(ctx, orphan.TxIDChainHash())
		require.True(t, errors.Is(err, errors.ErrTxNotFound), "a failed combined call writes no record, got: %v", err)
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
			err := bv.applyList(ctx, block, txs, txids, make([]int, len(txs)), true, true, func(*bt.Tx) {}, nil)
			elapsed := time.Since(start)

			require.Error(t, err)
			// A context error, not a processing one: the legacy park's commit classifier reads a
			// processing error as a verdict on the block and drops it.
			require.True(t, errors.IsContextError(err), "a cancel must surface as a context error, got: %v", err)
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
	require.NoError(t, bv.applyList(context.Background(), block, txs, txids, []int{0, 0}, false, false, func(*bt.Tx) {}, nil))
	store.AssertNumberOfCalls(t, "SpendAndCreateMulti", 2)
}

// callBoundStore is a real utxo.Store whose list write runs through the shared
// DefaultSpendAndCreateMulti with a short CallBound, and whose create of one transaction does
// not return until its call's context ends, as a create still waiting on its store's batch
// would. The hold applies once, so a repeat of the list goes through.
type callBoundStore struct {
	utxo.Store

	widths utxo.SpendAndCreateMultiWidths
	hold   chainhash.Hash
	fail   func(ctxErr error) error
	held   atomic.Bool
}

func (s *callBoundStore) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	return utxo.DefaultSpendAndCreateMulti(ctx, s, s.widths, txs, blockHeight, opts...)
}

func (s *callBoundStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	return utxo.SequentialSpendAndCreate(ctx, ulogger.TestLogger{}, s, tx, blockHeight, opts...)
}

// Spend and the pass-through half of Create reach the real store's own methods, which utxo.Store
// does not expose but the SQL store implements as a utxo.SequentialStore.
func (s *callBoundStore) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return s.Store.(utxo.SequentialStore).Spend(ctx, tx, blockHeight, ignoreFlags...)
}

func (s *callBoundStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, error) {
	if *tx.TxIDChainHash() == s.hold && s.held.CompareAndSwap(false, true) {
		<-ctx.Done()

		return nil, s.fail(ctx.Err())
	}

	return s.Store.(utxo.SequentialStore).Create(ctx, tx, blockHeight, opts...)
}

// TestBatchList_CallBoundExpiryIsARetryNotAVerdict: a store call in a below-checkpoint list that
// runs past its CallBound comes back MultiTxFailed with the context's error, and applyList fails
// the batch without retrying it. The legacy park's parkCommitFailure reads that failure after the
// gRPC hop from block validation, and must see a context error, which keeps the parked block for
// a retry, rather than a judgement, which deletes it and writes the hash off. The create's
// outcome is unknown, so the spend stays, and a repeat of the batch finishes the transaction.
// Both shapes a held create can return are covered: the bare context error a batcher wait
// returns, and teranode's ContextCanceledError around it.
func TestBatchList_CallBoundExpiryIsARetryNotAVerdict(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(ctxErr error) error
	}{
		{name: "bare context error", fail: func(ctxErr error) error { return ctxErr }},
		{name: "context canceled error", fail: func(ctxErr error) error {
			return errors.NewContextCanceledError("create did not finish", ctxErr)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bv, recorder, cleanup := newListHarness(t, "list_call_bound_"+strings.ReplaceAll(tc.name, " ", "_"), true)
			defer cleanup()

			ctx := context.Background()

			root, key := seedRoot(t, recorder.Store, 1, "LIST_CALL_BOUND_KEY")
			child := spendOf(t, key, root, 0, 90_000)

			widths := utxo.UniformSpendAndCreateMultiWidths(1)
			widths.CallBound = 200 * time.Millisecond

			store := &callBoundStore{Store: recorder.Store, widths: widths, hold: *child.TxIDChainHash(), fail: tc.fail}
			bv.utxoStore = store

			block := &model.Block{Height: 100, ID: 42}
			batch := listBatchFor(t, bv, block, []*bt.Tx{child}, true)

			err := bv.createAndSpendUTXOsForBatch(ctx, block, batch)
			require.Error(t, err)
			require.True(t, store.held.Load(), "the create must have been held past the call bound")

			// End state first: the spend stays, because the create may still commit, and no
			// record was written.
			requireSpentBy(t, recorder.Store, root, 0, child)

			_, getErr := recorder.Store.Get(ctx, child.TxIDChainHash())
			require.True(t, errors.Is(getErr, errors.ErrTxNotFound), "the held create wrote no record: %v", getErr)

			// What the legacy park sees, across the gRPC hop.
			parked := errors.UnwrapGRPC(errors.WrapGRPC(err))
			require.True(t, errors.IsContextError(parked), "a call-bound expiry must reach the park as a context error, got: %v", parked)
			require.False(t, errors.Is(parked, errors.ErrBlockInvalid), "a call-bound expiry is not a verdict on the block: %v", parked)
			require.False(t, errors.IsBlockCorrupt(parked), "a call-bound expiry is not a corrupt block: %v", parked)

			// A repeat of the batch finishes the transaction.
			require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, batch))
			requireSpentBy(t, recorder.Store, root, 0, child)

			_, getErr = recorder.Store.Get(ctx, child.TxIDChainHash())
			require.NoError(t, getErr, "the repeat must write the record")
		})
	}
}

// flakyTxStore is a real utxo.Store whose list write runs through the shared
// DefaultSpendAndCreateMulti, and whose SpendAndCreate of one transaction fails with a retryable
// store error, before reaching the store, for the first failTimes calls. With spendOnly, only
// that transaction's second-wave spend calls fail, so its create-only record is written first.
type flakyTxStore struct {
	utxo.Store

	fail      chainhash.Hash
	spendOnly bool
	failTimes atomic.Int32
}

func (s *flakyTxStore) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	return utxo.DefaultSpendAndCreateMulti(ctx, s, utxo.UniformSpendAndCreateMultiWidths(4), txs, blockHeight, opts...)
}

func (s *flakyTxStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	if *tx.TxIDChainHash() == s.fail && (!s.spendOnly || parseCreateOptions(opts).SpendOnly) && s.failTimes.Add(-1) >= 0 {
		return nil, nil, errors.NewStorageError("forced transient fault on %s", tx.TxIDChainHash())
	}

	return utxo.SequentialSpendAndCreate(ctx, ulogger.TestLogger{}, s, tx, blockHeight, opts...)
}

func (s *flakyTxStore) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return s.Store.(utxo.SequentialStore).Spend(ctx, tx, blockHeight, ignoreFlags...)
}

func (s *flakyTxStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, error) {
	return s.Store.(utxo.SequentialStore).Create(ctx, tx, blockHeight, opts...)
}

// flakyListBatch builds a batch of four transactions, a, b, c and d, where d spends b, over a
// store that fails b's write with a retryable error failTimes times.
func flakyListBatch(t *testing.T, dbName string, failTimes int32) (bv *BlockValidation, recorder *applyRecorder,
	root *bt.Tx, txs []*bt.Tx, cleanup func()) {
	t.Helper()

	bv, recorder, cleanup = newListHarness(t, dbName, true)

	root, key := seedRoot(t, recorder.Store, 3, "LIST_SUBSET_RETRY_KEY")
	a := spendOf(t, key, root, 0, 90_000)
	b := spendOf(t, key, root, 1, 90_000)
	c := spendOf(t, key, root, 2, 90_000)

	// spendOf pays every output to this key, so it is the one that spends b's output.
	outKey, _ := bec.PrivateKeyFromBytes([]byte("netted-out"))
	d := spendOf(t, outKey, b, 0, 80_000)

	flaky := &flakyTxStore{Store: recorder.Store, fail: *b.TxIDChainHash()}
	flaky.failTimes.Store(failTimes)
	recorder.Store = flaky

	return bv, recorder, root, []*bt.Tx{a, b, c, d}, cleanup
}

// TestBatchList_RetryableFailureRetriesOnlyWhatFailed: a transaction that fails with a retryable
// store error is retried with the child the store failed as ParentFailed beside it, and nothing
// that went through on the first pass is sent again, as the per-transaction waves retried only
// what failed. Every transaction ends written with its input spent.
func TestBatchList_RetryableFailureRetriesOnlyWhatFailed(t *testing.T) {
	bv, recorder, root, txs, cleanup := flakyListBatch(t, "list_subset_retry", 1)
	defer cleanup()

	a, b, c, d := txs[0], txs[1], txs[2], txs[3]

	block := &model.Block{Height: 100, ID: 42}
	batch := listBatchFor(t, bv, block, txs, true)

	recorder.reset()
	require.NoError(t, bv.createAndSpendUTXOsForBatch(context.Background(), block, batch))

	lists := recorder.recordedLists()
	require.Len(t, lists, 2, "one failed pass, then one retry")
	require.Equal(t, []chainhash.Hash{*a.TxIDChainHash(), *b.TxIDChainHash(), *c.TxIDChainHash(), *d.TxIDChainHash()}, lists[0].txids)
	require.Equal(t, []chainhash.Hash{*b.TxIDChainHash(), *d.TxIDChainHash()}, lists[1].txids,
		"the retry carries only the failed transaction and its child, in block order")
	require.Len(t, lists[1].subtreeIdxs, 2, "the retry's subtree indexes are cut to its transactions")

	requireSpentBy(t, recorder.Store, root, 0, a)
	requireSpentBy(t, recorder.Store, root, 1, b)
	requireSpentBy(t, recorder.Store, root, 2, c)
	requireSpentBy(t, recorder.Store, b, 0, d)

	for _, tx := range txs {
		_, err := recorder.Store.Get(context.Background(), tx.TxIDChainHash())
		require.NoError(t, err, "%s must be written", tx.TxIDChainHash())
	}
}

// TestBatchList_RetryWithNoProgressGivesUp: a retry that fails as many transactions as the pass
// before it makes no progress, and the batch fails then rather than after every retry.
func TestBatchList_RetryWithNoProgressGivesUp(t *testing.T) {
	bv, recorder, _, txs, cleanup := flakyListBatch(t, "list_no_progress", 1_000)
	defer cleanup()

	block := &model.Block{Height: 100, ID: 42}
	batch := listBatchFor(t, bv, block, txs, true)

	recorder.reset()
	err := bv.createAndSpendUTXOsForBatch(context.Background(), block, batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no progress")
	require.Len(t, recorder.recordedLists(), 2, "the first pass, then one retry that fails the same two")
}

// TestBatchList_GiveUpKeepsTheHardFailure: while one transaction keeps failing with a retryable
// error, another that fails for good does not fail the batch at once, because the batch repeats.
// When the batch gives up, its error must still carry that failure's reason, here a spend of an
// output that exists nowhere, and not only the retryable one.
func TestBatchList_GiveUpKeepsTheHardFailure(t *testing.T) {
	bv, recorder, cleanup := newListHarness(t, "list_give_up_hard", true)
	defer cleanup()

	root, key := seedRoot(t, recorder.Store, 2, "LIST_GIVE_UP_HARD_KEY")
	flakyTx := spendOf(t, key, root, 0, 90_000)

	// The ghost pays to the root's key, so one key signs both inputs of its spender.
	_, outPub := bec.PrivateKeyFromBytes([]byte("netted-out"))
	ghost := transactions.Create(t, transactions.WithCoinbaseData(9, "/ghost/"), transactions.WithP2PKHOutputs(1, 100_000, key.PubKey()))
	ghostSpender := transactions.Create(t,
		transactions.WithPrivateKey(key),
		transactions.WithInput(root, 1),
		transactions.WithInput(ghost, 0),
		transactions.WithP2PKHOutputs(1, 80_000, outPub),
	)

	flaky := &flakyTxStore{Store: recorder.Store, fail: *flakyTx.TxIDChainHash()}
	flaky.failTimes.Store(1_000)
	recorder.Store = flaky

	block := &model.Block{Height: 100, ID: 42}
	batch := listBatchFor(t, bv, block, []*bt.Tx{flakyTx, ghostSpender}, true)

	recorder.reset()
	err := bv.createAndSpendUTXOsForBatch(context.Background(), block, batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no progress")
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "the exit error must carry the hard failure's reason: %v", err)
	require.Contains(t, err.Error(), "forced transient fault", "the retryable failure is still named")
}

// TestBatchList_RetryKeepsAFailedChildsParent: a child whose second-wave spend fails with a
// retryable error after its parent in the list went through is retried with that parent, so it
// meets the store as it did the first time: its create-only record is found as existing and its
// spends are made with RequireParentOutputs. Retried alone it would get a combined call, which
// reads its own record as proof the spends were made, so a spend of an output that exists
// nowhere would pass. The child here also spends such an output, and the block must fail.
func TestBatchList_RetryKeepsAFailedChildsParent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ghost bool
	}{
		{name: "the retry finishes the child's spends", ghost: false},
		{name: "a spend of an output that exists nowhere still fails", ghost: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bv, recorder, cleanup := newListHarness(t, fmt.Sprintf("list_retry_parent_%v", tc.ghost), true)
			defer cleanup()

			root, key := seedRoot(t, recorder.Store, 1, "LIST_RETRY_PARENT_KEY")
			parent := spendOf(t, key, root, 0, 90_000)

			// spendOf pays every output to this key, so it signs the child's inputs.
			outKey, outPub := bec.PrivateKeyFromBytes([]byte("netted-out"))

			opts := []transactions.TxOption{
				transactions.WithPrivateKey(outKey),
				transactions.WithInput(parent, 0),
				transactions.WithP2PKHOutputs(1, 80_000, outPub),
			}

			if tc.ghost {
				ghost := transactions.Create(t, transactions.WithCoinbaseData(9, "/ghost/"), transactions.WithP2PKHOutputs(1, 100_000, outPub))
				opts = append(opts, transactions.WithInput(ghost, 0))
			}

			child := transactions.Create(t, opts...)

			flaky := &flakyTxStore{Store: recorder.Store, fail: *child.TxIDChainHash(), spendOnly: true}
			flaky.failTimes.Store(1)
			recorder.Store = flaky

			block := &model.Block{Height: 100, ID: 42}
			batch := listBatchFor(t, bv, block, []*bt.Tx{parent, child}, true)

			recorder.reset()
			err := bv.createAndSpendUTXOsForBatch(context.Background(), block, batch)

			lists := recorder.recordedLists()
			require.Len(t, lists, 2, "one failed pass, then one retry")
			require.Equal(t, []chainhash.Hash{*parent.TxIDChainHash(), *child.TxIDChainHash()}, lists[1].txids,
				"the retry carries the failed child with its parent in the list")

			if tc.ghost {
				require.Error(t, err, "a spend of an output that exists nowhere must fail the batch")
				require.True(t, errors.Is(err, errors.ErrTxNotFound), "the missing output must fail as not found: %v", err)

				return
			}

			require.NoError(t, err)
			requireSpentBy(t, recorder.Store, root, 0, parent)
			requireSpentBy(t, recorder.Store, parent, 0, child)

			// The parent was created on the first pass and comes back Existed only because the
			// retry carried it. It must not be collected again: only the child, which the
			// retry found existing, is stamped, and only once.
			require.Equal(t, []chainhash.Hash{*child.TxIDChainHash()}, recorder.recordedMined(),
				"a carried parent is not stamped with mined info again")
		})
	}
}
