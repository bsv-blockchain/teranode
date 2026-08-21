package pebble

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	pebbledb "github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// newExtendedTx builds a spendable transaction funded from the shared test fixture.
// satoshisSeed makes the txid unique per call; later outputs are small so the fee stays
// positive however many outputs are requested.
func newExtendedTx(t testing.TB, outputs int, satoshisSeed uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tests.Tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tests.Tx.Outputs[0].LockingScript,
		Satoshis:      tests.Tx.Outputs[0].Satoshis,
	}))

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})

	for i := 0; i < outputs; i++ {
		amount := uint64(1000 + i)
		if i == 0 {
			amount = satoshisSeed
		}

		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", amount))
	}

	return tx
}

func newSpendingTx(t testing.TB, parent *bt.Tx, vouts ...uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	for _, vout := range vouts {
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parent.TxIDChainHash(),
			Vout:          vout,
			LockingScript: parent.Outputs[vout].LockingScript,
			Satoshis:      parent.Outputs[vout].Satoshis,
		}))
	}

	for i := range tx.Inputs {
		tx.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})
	}

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	return tx
}

func mustCreate(t testing.TB, store *Store, tx *bt.Tx, height uint32, opts ...utxo.CreateOption) {
	t.Helper()

	_, err := store.Create(context.Background(), tx, height, opts...)
	require.NoError(t, err)
}

func TestHealthAndCapabilities(t *testing.T) {
	store := newTestStore(t)

	status, msg, err := store.Health(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, msg)

	require.True(t, store.SupportsOutpointOnlySpend())

	require.NoError(t, store.Close(context.Background()))

	status, _, err = store.Health(context.Background(), true)
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, status)

	// Close is idempotent: a second call must not double-close the database.
	require.NoError(t, store.Close(context.Background()))
}

func TestNewRejectsMissingPath(t *testing.T) {
	tSettings := settings.NewSettings()

	_, err := New(context.Background(), ulogger.TestLogger{}, tSettings, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrInvalidArgument))

	_, err = New(context.Background(), ulogger.TestLogger{}, tSettings, &url.URL{Scheme: "pebble"})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrInvalidArgument))
}

// TestPageSizeIsImmutable pins the guard that stops a store being reopened under a page
// size other than the one its records were written with — slot addressing depends on it,
// so a silent mismatch would misread every spend slot.
func TestPageSizeIsImmutable(t *testing.T) {
	dir := t.TempDir()
	storeURL := &url.URL{Scheme: "pebble", Path: dir}
	tSettings := settings.NewSettings()

	store, err := New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)
	require.NoError(t, store.Close(context.Background()))

	reopened, err := New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	reopened.pageSize = spikePageSizeSlots * 2

	err = reopened.validateMeta()
	require.Error(t, err)
	require.Contains(t, err.Error(), "immutable")

	require.NoError(t, reopened.Close(context.Background()))
}

func TestCreateDirectAndDuplicate(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 2, 10_000)

	md, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Equal(t, tx.Size(), int(md.SizeInBytes))

	_, err = store.Create(context.Background(), tx, 100)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxExists))

	got, err := store.Get(context.Background(), tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, tx.TxID(), got.Tx.TxID())
	require.Equal(t, uint32(100), got.UnminedSince)
}

// TestMultiPageLifecycle exercises the overflow-page paths end to end: create writes page
// records, a spend on a page slot patches and completes that page, Get(Utxos) reads spend
// state back across pages, and Unspend reopens the page.
func TestMultiPageLifecycle(t *testing.T) {
	store := newTestStore(t)

	outputs := int(store.pageSize) + 2
	parent := newExtendedTx(t, outputs, 20_000)
	mustCreate(t, store, parent, 100)

	pageVout := store.pageSize + 1

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 0, pageVout), 101)
	require.NoError(t, err)
	require.Len(t, spends, 2)

	for _, sp := range spends {
		require.NoError(t, sp.Err)
	}

	md, err := store.Get(context.Background(), parent.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)
	require.Len(t, md.SpendingDatas, outputs)
	require.NotNil(t, md.SpendingDatas[0])
	require.NotNil(t, md.SpendingDatas[pageVout])
	require.Nil(t, md.SpendingDatas[1])

	require.NoError(t, store.Unspend(context.Background(), spends))

	md, err = store.Get(context.Background(), parent.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)
	require.Nil(t, md.SpendingDatas[0])
	require.Nil(t, md.SpendingDatas[pageVout])

	// The reopened slots must be spendable again.
	spends, err = store.Spend(context.Background(), newSpendingTx(t, parent, 0, pageVout), 101)
	require.NoError(t, err)

	for _, sp := range spends {
		require.NoError(t, sp.Err)
	}
}

func TestUnminedIterator(t *testing.T) {
	store := newTestStore(t)

	unmined := make(map[chainhash.Hash]bool)

	for i := 0; i < 3; i++ {
		tx := newExtendedTx(t, 1, 30_000+uint64(i)*1000)
		mustCreate(t, store, tx, 100+uint32(i)) //nolint:gosec

		unmined[*tx.TxIDChainHash()] = true
	}

	mined := newExtendedTx(t, 1, 35_000)
	mustCreate(t, store, mined, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))

	it, err := store.GetUnminedTxIterator()
	require.NoError(t, err)

	all := drainUnmined(t, it)
	require.Len(t, all, 3)

	for _, u := range all {
		require.False(t, u.Skip)
		require.True(t, unmined[u.Node.Hash])
		require.NotZero(t, u.Node.Fee)
		require.NotZero(t, u.Node.SizeInBytes)
		require.NotNil(t, u.TxInpoints)
		require.Len(t, u.TxInpoints.ParentTxHashes, 1)
		require.NotZero(t, u.UnminedSince)
		require.NotZero(t, u.CreatedAt)
	}
}

func drainUnmined(t *testing.T, it utxo.UnminedTxIterator) []*utxo.UnminedTransaction {
	t.Helper()

	var all []*utxo.UnminedTransaction

	for {
		batch, err := it.Next(context.Background())
		require.NoError(t, err)

		if batch == nil {
			break
		}

		all = append(all, batch...)
	}

	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	return all
}

func TestPrunableIteratorRespectsCutoff(t *testing.T) {
	store := newTestStore(t)

	old := newExtendedTx(t, 1, 40_000)
	mustCreate(t, store, old, 50)

	recent := newExtendedTx(t, 1, 41_000)
	mustCreate(t, store, recent, 200)

	it, err := store.GetPrunableUnminedTxIterator(100)
	require.NoError(t, err)

	all := drainUnmined(t, it)
	require.Len(t, all, 1)
	require.Equal(t, *old.TxIDChainHash(), all[0].Node.Hash)
}

func TestConflictingIteratorAndSkipInUnmined(t *testing.T) {
	store := newTestStore(t)

	conflicting := newExtendedTx(t, 1, 50_000)
	mustCreate(t, store, conflicting, 100, utxo.WithConflicting(true))

	normal := newExtendedTx(t, 1, 51_000)
	mustCreate(t, store, normal, 100)

	it, err := store.GetConflictingTxIterator()
	require.NoError(t, err)

	all := drainUnmined(t, it)
	require.Len(t, all, 1)
	require.Equal(t, *conflicting.TxIDChainHash(), all[0].Node.Hash)

	// The unmined iterator must exclude the conflicting transaction.
	it, err = store.GetUnminedTxIterator()
	require.NoError(t, err)

	all = drainUnmined(t, it)
	require.Len(t, all, 1)
	require.Equal(t, *normal.TxIDChainHash(), all[0].Node.Hash)
}

func TestConsistencyScanFindsMinedButUnmined(t *testing.T) {
	store := newTestStore(t)

	inconsistent := newExtendedTx(t, 1, 60_000)
	mustCreate(t, store, inconsistent, 100)

	// Force the inconsistent state the scan exists to detect: block refs present while
	// unmined_since is still set.
	m, err := store.getMaster(inconsistent.TxIDChainHash())
	require.NoError(t, err)

	m.blockRefs = packBlockRefs([]utxo.MinedBlockInfo{{BlockID: 9, BlockHeight: 100}})
	require.NoError(t, store.db.Set(masterKey(inconsistent.TxIDChainHash()[:]), encodeMaster(m), store.sync))

	clean := newExtendedTx(t, 1, 61_000)
	mustCreate(t, store, clean, 100)

	it, err := store.ScanInconsistentUnminedTxs()
	require.NoError(t, err)

	var found []*utxo.InconsistentTxRecord

	for {
		batch, err := it.Next(context.Background())
		require.NoError(t, err)

		if batch == nil {
			break
		}

		found = append(found, batch...)
	}

	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.GreaterOrEqual(t, it.TotalScanned(), int64(2))
	require.Len(t, found, 1)
	require.Equal(t, *inconsistent.TxIDChainHash(), found[0].Hash)
	require.Equal(t, []uint32{9}, found[0].BlockIDs)
}

func TestQueryOldUnminedTransactions(t *testing.T) {
	store := newTestStore(t)

	old := newExtendedTx(t, 1, 70_000)
	mustCreate(t, store, old, 50)

	recent := newExtendedTx(t, 1, 71_000)
	mustCreate(t, store, recent, 200)

	hashes, err := store.QueryOldUnminedTransactions(context.Background(), 100)
	require.NoError(t, err)
	require.Len(t, hashes, 1)
	require.Equal(t, *old.TxIDChainHash(), hashes[0])
}

func TestRemoveBlockIDs(t *testing.T) {
	store := newTestStore(t)

	tx := newExtendedTx(t, 1, 80_000)
	mustCreate(t, store, tx, 100,
		utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true},
			utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 101, OnLongestChain: true}))

	require.NoError(t, store.RemoveBlockIDs(context.Background(),
		[]utxo.BlockIDsRemoval{{TxHash: tx.TxIDChainHash(), BlockIDs: []uint32{1}}}))

	md, err := store.Get(context.Background(), tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{2}, md.BlockIDs)

	// RemoveBlockIDs only trims membership; it must not resurrect unmined_since.
	m, err := store.getMaster(tx.TxIDChainHash())
	require.NoError(t, err)
	require.Zero(t, m.unminedSince)

	// Missing transactions are tolerated, and a nil hash is rejected.
	missing := newExtendedTx(t, 1, 81_000)
	require.NoError(t, store.RemoveBlockIDs(context.Background(),
		[]utxo.BlockIDsRemoval{{TxHash: missing.TxIDChainHash(), BlockIDs: []uint32{1}}}))

	require.NoError(t, store.RemoveBlockIDs(context.Background(), nil))

	err = store.RemoveBlockIDs(context.Background(), []utxo.BlockIDsRemoval{{BlockIDs: []uint32{1}}})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrInvalidArgument))
}

func TestRemoveFromConflictingChildren(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 1, 90_000)
	mustCreate(t, store, parent, 100)

	child := newSpendingTx(t, parent, 0)
	mustCreate(t, store, child, 101, utxo.WithConflicting(true))

	children, err := store.GetConflictingChildren(context.Background(), *parent.TxIDChainHash())
	require.NoError(t, err)
	require.Contains(t, children, *child.TxIDChainHash())

	require.NoError(t, store.RemoveFromConflictingChildren(context.Background(),
		[]utxo.ConflictingChildRemoval{{ParentHash: parent.TxIDChainHash(), ChildHash: child.TxIDChainHash()}}))

	children, err = store.GetConflictingChildren(context.Background(), *parent.TxIDChainHash())
	require.NoError(t, err)
	require.Empty(t, children)

	// Idempotent, empty input is a no-op, and nil hashes are rejected.
	require.NoError(t, store.RemoveFromConflictingChildren(context.Background(),
		[]utxo.ConflictingChildRemoval{{ParentHash: parent.TxIDChainHash(), ChildHash: child.TxIDChainHash()}}))
	require.NoError(t, store.RemoveFromConflictingChildren(context.Background(), nil))

	err = store.RemoveFromConflictingChildren(context.Background(),
		[]utxo.ConflictingChildRemoval{{ParentHash: parent.TxIDChainHash()}})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrInvalidArgument))
}

func TestPreserveAndExpire(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 1, 100_000)
	mustCreate(t, store, parent, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 0), 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	m, err := store.getMaster(parent.TxIDChainHash())
	require.NoError(t, err)
	require.NotZero(t, m.deleteAtHeight, "fully spent mined tx should carry a delete-at-height")

	require.NoError(t, store.PreserveTransactions(context.Background(),
		[]chainhash.Hash{*parent.TxIDChainHash()}, 500))

	m, err = store.getMaster(parent.TxIDChainHash())
	require.NoError(t, err)
	require.Zero(t, m.deleteAtHeight, "preservation must clear the delete stamp")
	require.Equal(t, int64(500), m.preserveUntil)

	// A transaction with neither a stamp nor an existing preservation is not at risk, so
	// it must be skipped rather than preserved.
	notEligible := newExtendedTx(t, 1, 101_000)
	mustCreate(t, store, notEligible, 100)

	require.NoError(t, store.PreserveTransactions(context.Background(),
		[]chainhash.Hash{*notEligible.TxIDChainHash()}, 500))

	m, err = store.getMaster(notEligible.TxIDChainHash())
	require.NoError(t, err)
	require.Zero(t, m.preserveUntil)

	require.NoError(t, store.ProcessExpiredPreservations(context.Background(), 501))

	m, err = store.getMaster(parent.TxIDChainHash())
	require.NoError(t, err)
	require.Zero(t, m.preserveUntil)
	require.Equal(t, int64(501)+store.retention(), m.deleteAtHeight,
		"an expired preservation on a fully spent mined tx must be re-stamped")
}

func TestDeleteRemovesEveryKey(t *testing.T) {
	store := newTestStore(t)

	tx := newExtendedTx(t, int(store.pageSize)+1, 110_000)
	mustCreate(t, store, tx, 100)

	utxoHash0, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	require.NoError(t, store.FreezeUTXOs(context.Background(),
		[]*utxo.Spend{{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0}}, store.settings))

	require.NoError(t, store.Delete(context.Background(), tx.TxIDChainHash()))

	_, err = store.Get(context.Background(), tx.TxIDChainHash())
	require.True(t, errors.Is(err, errors.ErrTxNotFound))

	for _, prefix := range []byte{prefixMaster, prefixPage, prefixHashes, prefixPayload, prefixOverride} {
		require.Zero(t, countPrefix(t, store, append([]byte{prefix}, tx.TxIDChainHash()[:]...)),
			"prefix %q should have no keys left", string(prefix))
	}

	// Deleting an absent transaction is a no-op.
	require.NoError(t, store.Delete(context.Background(), tx.TxIDChainHash()))
}

func countPrefix(t *testing.T, store *Store, prefix []byte) int {
	t.Helper()

	lower, upper := prefixBounds(prefix)

	iter, err := store.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	require.NoError(t, iter.Error())

	return count
}

func TestPrunerDeletesStampedTransactions(t *testing.T) {
	store := newTestStore(t)

	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, svc)

	again, err := store.GetPrunerService()
	require.NoError(t, err)
	require.Same(t, svc, again, "pruner service should be a singleton")

	svc.Start(context.Background())

	observer := &recordingObserver{}
	svc.AddObserver(observer)

	// Two transactions stamped for deletion below the prune height, one above it.
	for i := 0; i < 3; i++ {
		tx := newExtendedTx(t, 1, 120_000+uint64(i)*1000)
		mustCreate(t, store, tx, 100)

		m, err := store.getMaster(tx.TxIDChainHash())
		require.NoError(t, err)

		old := *m
		m.deleteAtHeight = 150

		if i == 2 {
			m.deleteAtHeight = 900
		}

		batch := store.db.NewBatch()
		require.NoError(t, store.stageMaster(batch, tx.TxIDChainHash()[:], &old, m))
		require.NoError(t, batch.Commit(store.sync))
		require.NoError(t, batch.Close())
	}

	deleted, err := svc.Prune(context.Background(), 200, "testblock")
	require.NoError(t, err)
	require.Equal(t, int64(2), deleted)
	require.Equal(t, uint32(200), observer.height)
	require.Equal(t, int64(2), observer.records)

	// The transaction stamped above the prune height must survive.
	require.Equal(t, 1, countPrefix(t, store, []byte{prefixDAHIdx}))
}

type recordingObserver struct {
	height  uint32
	records int64
}

func (o *recordingObserver) OnPruneComplete(height uint32, recordsProcessed int64) {
	o.height = height
	o.records = recordsProcessed
}

func TestBatchDecorate(t *testing.T) {
	store := newTestStore(t)

	tx1 := newExtendedTx(t, 1, 130_000)
	tx2 := newExtendedTx(t, 2, 131_000)
	missing := newExtendedTx(t, 1, 132_000)

	mustCreate(t, store, tx1, 100)
	mustCreate(t, store, tx2, 100)

	items := []*utxo.UnresolvedMetaData{
		{Hash: *tx1.TxIDChainHash(), Idx: 0},
		{Hash: *missing.TxIDChainHash(), Idx: 1},
		{Hash: *tx2.TxIDChainHash(), Idx: 2, Fields: []fields.FieldName{fields.Fee}},
	}

	require.NoError(t, store.BatchDecorate(context.Background(), items, fields.Fee, fields.SizeInBytes, fields.Tx))

	require.NoError(t, items[0].Err)
	require.Equal(t, tx1.Size(), int(items[0].Data.SizeInBytes))

	require.Error(t, items[1].Err)
	require.True(t, errors.Is(items[1].Err, errors.ErrTxNotFound))

	require.NoError(t, items[2].Err)
	require.Nil(t, items[2].Data.Tx, "per-item field selection must win over the batch default")
	require.NotZero(t, items[2].Data.Fee)
}

func TestPreviousOutputsDecorate(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 2, 140_000)
	mustCreate(t, store, parent, 100)

	child := bt.NewTx()
	child.Inputs = append(child.Inputs, &bt.Input{PreviousTxOutIndex: 1})
	require.NoError(t, child.Inputs[0].PreviousTxIDAdd(parent.TxIDChainHash()))

	require.NoError(t, store.PreviousOutputsDecorate(context.Background(), child))
	require.Equal(t, *parent.Outputs[1].LockingScript, *child.Inputs[0].PreviousTxScript)
	require.Equal(t, parent.Outputs[1].Satoshis, child.Inputs[0].PreviousTxSatoshis)

	// Already-decorated inputs are left untouched.
	existing := bscript.NewFromBytes([]byte{0x51})

	decorated := bt.NewTx()
	decorated.Inputs = append(decorated.Inputs, &bt.Input{PreviousTxOutIndex: 0, PreviousTxScript: existing})
	require.NoError(t, decorated.Inputs[0].PreviousTxIDAdd(parent.TxIDChainHash()))

	require.NoError(t, store.BatchPreviousOutputsDecorate(context.Background(), []*bt.Tx{decorated}))
	require.Same(t, existing, decorated.Inputs[0].PreviousTxScript)

	// An unknown parent surfaces as not-found rather than a silent nil script.
	orphan := bt.NewTx()
	orphan.Inputs = append(orphan.Inputs, &bt.Input{PreviousTxOutIndex: 0})
	require.NoError(t, orphan.Inputs[0].PreviousTxIDAdd(newExtendedTx(t, 1, 141_000).TxIDChainHash()))

	err := store.PreviousOutputsDecorate(context.Background(), orphan)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}
