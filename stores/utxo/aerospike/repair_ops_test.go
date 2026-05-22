package aerospike_test

import (
	"context"
	"sort"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newChildSpendingOutput builds a simple child tx that spends a single
// specified output of `parent`. Each call produces a unique child (unique
// locking script / satoshis) so callers can generate several distinct
// children from the same parent.
func newChildSpendingOutput(t *testing.T, parent *bt.Tx, vout uint32, tag uint64) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	err := child.From(
		parent.TxID(),
		vout,
		parent.Outputs[vout].LockingScript.String(),
		parent.Outputs[vout].Satoshis,
	)
	require.NoError(t, err)

	// Small unique output so different children produce different txids.
	err = child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 100+tag)
	require.NoError(t, err)

	return child
}

// newChildSpendingTwoParents builds a child tx that consumes one output each
// from two distinct parents. Used to verify that SetConflicting-style creation
// populates conflictingCs on multiple parents.
func newChildSpendingTwoParents(t *testing.T, parentA, parentB *bt.Tx, voutA, voutB uint32, tag uint64) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	err := child.From(parentA.TxID(), voutA, parentA.Outputs[voutA].LockingScript.String(), parentA.Outputs[voutA].Satoshis)
	require.NoError(t, err)

	err = child.From(parentB.TxID(), voutB, parentB.Outputs[voutB].LockingScript.String(), parentB.Outputs[voutB].Satoshis)
	require.NoError(t, err)

	err = child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 200+tag)
	require.NoError(t, err)

	return child
}

// seedConflictingChild creates `child` as a conflicting transaction which
// causes the aerospike Create implementation to append the child's hash to
// every parent's conflictingCs list.
func seedConflictingChild(t *testing.T, ctx context.Context, store interface {
	Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error)
}, child *bt.Tx) {
	t.Helper()
	_, err := store.Create(ctx, child, 1, utxo.WithConflicting(true))
	require.NoError(t, err)
}

// readConflictingChildren reads the conflictingCs bin from the tx's main
// record and returns the hashes it contains. If the bin is absent it returns
// an empty slice. The list comes back from Aerospike as []interface{} where
// each element is []byte (see get.go:processConflictingChildren).
func readConflictingChildren(t *testing.T, store interface {
	GetNamespace() string
	GetName() string
}, client interface {
	Get(policy *aerospike.BasePolicy, key *aerospike.Key, binNames ...string) (*aerospike.Record, aerospike.Error)
}, txHash *chainhash.Hash) []chainhash.Hash {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txHash.CloneBytes())
	require.NoError(t, err)

	rec, aeroErr := client.Get(util.GetAerospikeReadPolicy(tSettings), key, fields.ConflictingChildren.String())
	require.Nil(t, aeroErr)
	require.NotNil(t, rec)

	raw, ok := rec.Bins[fields.ConflictingChildren.String()].([]interface{})
	if !ok {
		return []chainhash.Hash{}
	}

	out := make([]chainhash.Hash, 0, len(raw))
	for _, v := range raw {
		b, ok := v.([]byte)
		require.True(t, ok, "conflictingCs entry is not []byte: %T", v)
		h, err := chainhash.NewHash(b)
		require.NoError(t, err)
		out = append(out, *h)
	}

	return out
}

// readBlockIDs reads and decodes the blockIDs bin from the tx's main record.
// Missing/empty bins return an empty slice.
func readBlockIDs(t *testing.T, store interface {
	GetNamespace() string
	GetName() string
}, client interface {
	Get(policy *aerospike.BasePolicy, key *aerospike.Key, binNames ...string) (*aerospike.Record, aerospike.Error)
}, txHash *chainhash.Hash) []uint32 {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txHash.CloneBytes())
	require.NoError(t, err)

	rec, aeroErr := client.Get(util.GetAerospikeReadPolicy(tSettings), key, fields.BlockIDs.String())
	require.Nil(t, aeroErr)
	require.NotNil(t, rec)

	raw, ok := rec.Bins[fields.BlockIDs.String()].([]interface{})
	if !ok {
		return []uint32{}
	}

	out := make([]uint32, 0, len(raw))
	for _, v := range raw {
		i, ok := v.(int)
		require.True(t, ok, "blockIDs entry is not int: %T", v)
		out = append(out, uint32(i)) // nolint:gosec // test data, known positive
	}

	return out
}

func sortedHashStrings(hs []chainhash.Hash) []string {
	out := make([]string, len(hs))
	for i := range hs {
		out[i] = hs[i].String()
	}
	sort.Strings(out)
	return out
}

func sortedUint32(xs []uint32) []uint32 {
	out := make([]uint32, len(xs))
	copy(out, xs)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestAerospike_RemoveFromConflictingChildren exercises the BatchOperate
// removal of children from parents' conflictingCs list.
func TestAerospike_RemoveFromConflictingChildren(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(func() { deferFn() })

	t.Run("empty slice is a no-op", func(t *testing.T) {
		cleanDB(t, client)

		require.NoError(t, store.RemoveFromConflictingChildren(ctx, nil))
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{}))
	})

	t.Run("nil hashes error", func(t *testing.T) {
		cleanDB(t, client)

		someHash := chainhash.HashH([]byte("some"))

		err := store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: nil, ChildHash: &someHash},
		})
		require.Error(t, err)

		err = store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: &someHash, ChildHash: nil},
		})
		require.Error(t, err)
	})

	t.Run("missing parent record is tolerated", func(t *testing.T) {
		cleanDB(t, client)

		parent := chainhash.HashH([]byte("missing-parent"))
		child := chainhash.HashH([]byte("missing-child"))

		err := store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: &parent, ChildHash: &child},
		})
		require.NoError(t, err)
	})

	t.Run("removes single child and keeps other entries", func(t *testing.T) {
		cleanDB(t, client)

		// Create parent.
		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		// Two distinct children, each spending a different output of the parent.
		// Creating each with WithConflicting(true) appends their hash to the
		// parent's conflictingCs list via updateParentConflictingChildren.
		child1 := newChildSpendingOutput(t, tx, 0, 1)
		child2 := newChildSpendingOutput(t, tx, 1, 2)
		seedConflictingChild(t, ctx, store, child1)
		seedConflictingChild(t, ctx, store, child2)

		// Sanity check: both children now appear in parent's conflictingCs.
		children := readConflictingChildren(t, store, client, tx.TxIDChainHash())
		require.ElementsMatch(t,
			[]string{child1.TxIDChainHash().String(), child2.TxIDChainHash().String()},
			sortedHashStrings(children),
		)

		// Remove only child1.
		err = store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: tx.TxIDChainHash(), ChildHash: child1.TxIDChainHash()},
		})
		require.NoError(t, err)

		// child2 remains.
		children = readConflictingChildren(t, store, client, tx.TxIDChainHash())
		require.Len(t, children, 1)
		assert.Equal(t, child2.TxIDChainHash().String(), children[0].String())
	})

	t.Run("batched removal across multiple parents", func(t *testing.T) {
		cleanDB(t, client)

		// Two parents: the shared `tx` fixture and the `txWithOPReturn` fixture.
		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		_, err = store.Create(ctx, txWithOPReturn, 0)
		require.NoError(t, err)

		// Single child spending one output from each parent.
		child := newChildSpendingTwoParents(t, tx, txWithOPReturn, 0, 0, 42)
		seedConflictingChild(t, ctx, store, child)

		// Sanity: each parent should list the child.
		childrenA := readConflictingChildren(t, store, client, tx.TxIDChainHash())
		childrenB := readConflictingChildren(t, store, client, txWithOPReturn.TxIDChainHash())
		require.Len(t, childrenA, 1)
		require.Len(t, childrenB, 1)
		assert.Equal(t, child.TxIDChainHash().String(), childrenA[0].String())
		assert.Equal(t, child.TxIDChainHash().String(), childrenB[0].String())

		// One RemoveFromConflictingChildren call scrubs the child from both
		// parents.
		err = store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: tx.TxIDChainHash(), ChildHash: child.TxIDChainHash()},
			{ParentHash: txWithOPReturn.TxIDChainHash(), ChildHash: child.TxIDChainHash()},
		})
		require.NoError(t, err)

		assert.Empty(t, readConflictingChildren(t, store, client, tx.TxIDChainHash()))
		assert.Empty(t, readConflictingChildren(t, store, client, txWithOPReturn.TxIDChainHash()))
	})

	t.Run("idempotent — second call succeeds", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		child1 := newChildSpendingOutput(t, tx, 0, 7)
		seedConflictingChild(t, ctx, store, child1)

		removal := []utxo.ConflictingChildRemoval{
			{ParentHash: tx.TxIDChainHash(), ChildHash: child1.TxIDChainHash()},
		}

		// First call removes the entry.
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, removal))
		assert.Empty(t, readConflictingChildren(t, store, client, tx.TxIDChainHash()))

		// Second identical call must succeed (list no longer contains the value).
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, removal))
		assert.Empty(t, readConflictingChildren(t, store, client, tx.TxIDChainHash()))
	})
}

// TestAerospike_RemoveBlockIDs covers batched trimming of blockIDs from tx
// records without deleting the tx.
func TestAerospike_RemoveBlockIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(func() { deferFn() })

	t.Run("empty slice is a no-op", func(t *testing.T) {
		cleanDB(t, client)

		require.NoError(t, store.RemoveBlockIDs(ctx, nil))
		require.NoError(t, store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{}))

		// A removal with an empty BlockIDs list should also be a no-op (skipped
		// inside the implementation).
		require.NoError(t, store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
			{TxHash: tx.TxIDChainHash(), BlockIDs: nil},
			{TxHash: tx.TxIDChainHash(), BlockIDs: []uint32{}},
		}))
	})

	t.Run("nil hash errors", func(t *testing.T) {
		cleanDB(t, client)

		err := store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
			{TxHash: nil, BlockIDs: []uint32{10}},
		})
		require.Error(t, err)
	})

	t.Run("missing tx record is tolerated", func(t *testing.T) {
		cleanDB(t, client)

		missing := chainhash.HashH([]byte("missing-tx"))
		err := store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
			{TxHash: &missing, BlockIDs: []uint32{1, 2, 3}},
		})
		require.NoError(t, err)
	})

	t.Run("removes subset of block IDs", func(t *testing.T) {
		cleanDB(t, client)

		// Create tx with blockIDs [10, 20, 30].
		_, err := store.Create(ctx, tx, 1,
			utxo.WithMinedBlockInfo(
				utxo.MinedBlockInfo{BlockID: 10, BlockHeight: 10, SubtreeIdx: 0},
				utxo.MinedBlockInfo{BlockID: 20, BlockHeight: 11, SubtreeIdx: 0},
				utxo.MinedBlockInfo{BlockID: 30, BlockHeight: 12, SubtreeIdx: 0},
			),
		)
		require.NoError(t, err)

		// Sanity — all three block IDs present.
		require.ElementsMatch(t, []uint32{10, 20, 30}, readBlockIDs(t, store, client, tx.TxIDChainHash()))

		// Remove [10, 30]; [20] must remain.
		err = store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
			{TxHash: tx.TxIDChainHash(), BlockIDs: []uint32{10, 30}},
		})
		require.NoError(t, err)

		remaining := readBlockIDs(t, store, client, tx.TxIDChainHash())
		assert.Equal(t, []uint32{20}, sortedUint32(remaining))
	})

	t.Run("batched across multiple txs", func(t *testing.T) {
		cleanDB(t, client)

		// tx with [10, 20].
		_, err := store.Create(ctx, tx, 1,
			utxo.WithMinedBlockInfo(
				utxo.MinedBlockInfo{BlockID: 10, BlockHeight: 10, SubtreeIdx: 0},
				utxo.MinedBlockInfo{BlockID: 20, BlockHeight: 11, SubtreeIdx: 0},
			),
		)
		require.NoError(t, err)

		// coinbaseTx with [30, 40, 50].
		_, err = store.Create(ctx, coinbaseTx, 1,
			utxo.WithMinedBlockInfo(
				utxo.MinedBlockInfo{BlockID: 30, BlockHeight: 12, SubtreeIdx: 0},
				utxo.MinedBlockInfo{BlockID: 40, BlockHeight: 13, SubtreeIdx: 0},
				utxo.MinedBlockInfo{BlockID: 50, BlockHeight: 14, SubtreeIdx: 0},
			),
		)
		require.NoError(t, err)

		// One batched call: trim [10] from tx, [40, 50] from coinbaseTx.
		err = store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
			{TxHash: tx.TxIDChainHash(), BlockIDs: []uint32{10}},
			{TxHash: coinbaseTx.TxIDChainHash(), BlockIDs: []uint32{40, 50}},
		})
		require.NoError(t, err)

		assert.Equal(t, []uint32{20}, sortedUint32(readBlockIDs(t, store, client, tx.TxIDChainHash())))
		assert.Equal(t, []uint32{30}, sortedUint32(readBlockIDs(t, store, client, coinbaseTx.TxIDChainHash())))
	})
}

// drainConflictingIterator drains an UnminedTxIterator and returns every
// non-skip hash it yielded. The iterator is closed on exit.
func drainConflictingIterator(t *testing.T, ctx context.Context, it utxo.UnminedTxIterator) []chainhash.Hash {
	t.Helper()

	collected := make([]chainhash.Hash, 0)
	for {
		batch, err := it.Next(ctx)
		require.NoError(t, err)
		if batch == nil {
			break
		}
		for _, item := range batch {
			if item == nil || item.Skip {
				continue
			}
			collected = append(collected, item.Hash)
		}
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	return collected
}

// TestAerospike_GetConflictingTxIterator covers the partition-parallel scan
// iterator that emits only conflicting=true records.
func TestAerospike_GetConflictingTxIterator(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(func() { deferFn() })

	t.Run("empty store returns nothing", func(t *testing.T) {
		cleanDB(t, client)

		it, err := store.GetConflictingTxIterator()
		require.NoError(t, err)

		collected := drainConflictingIterator(t, ctx, it)
		assert.Empty(t, collected)
	})

	t.Run("yields only conflicting records", func(t *testing.T) {
		cleanDB(t, client)

		// Non-conflicting parent.
		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		// Conflicting child (spends one of parent's outputs).
		child := newChildSpendingOutput(t, tx, 0, 11)
		_, err = store.Create(ctx, child, 1, utxo.WithConflicting(true))
		require.NoError(t, err)

		it, err := store.GetConflictingTxIterator()
		require.NoError(t, err)
		collected := drainConflictingIterator(t, ctx, it)

		require.Len(t, collected, 1, "only the conflicting child should be yielded, got %v", sortedHashStrings(collected))
		assert.Equal(t, child.TxIDChainHash().String(), collected[0].String())
	})

	t.Run("returns multiple conflicting records", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		// Three distinct conflicting children, each spending a different output
		// of `tx` so they produce unique txids and have real parent references.
		children := []*bt.Tx{
			newChildSpendingOutput(t, tx, 0, 100),
			newChildSpendingOutput(t, tx, 1, 101),
			newChildSpendingOutput(t, tx, 2, 102),
		}
		expected := make([]string, 0, len(children))
		for _, c := range children {
			_, err = store.Create(ctx, c, 1, utxo.WithConflicting(true))
			require.NoError(t, err)
			expected = append(expected, c.TxIDChainHash().String())
		}

		it, err := store.GetConflictingTxIterator()
		require.NoError(t, err)
		collected := drainConflictingIterator(t, ctx, it)

		assert.ElementsMatch(t, expected, sortedHashStrings(collected))
	})

	t.Run("skips coinbase even if somehow marked conflicting", func(t *testing.T) {
		cleanDB(t, client)

		// Sanity — the fixture really is a coinbase.
		require.True(t, coinbaseTx.IsCoinbase())

		// Seed one legitimate non-coinbase conflicting tx so we can prove the
		// iterator is still emitting real conflicts (and therefore the absence
		// of the coinbase below is because of the filter, not because the
		// iterator is broken).
		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		child := newChildSpendingOutput(t, tx, 0, 55)
		_, err = store.Create(ctx, child, 1, utxo.WithConflicting(true))
		require.NoError(t, err)

		// Try the straightforward path first: ask the store to create the
		// coinbase record already flagged conflicting=true. This may fail
		// because updateParentConflictingChildren walks inputs and a coinbase
		// input references the zero hash (no real parent record exists).
		var usedRawPutBins bool
		_, createErr := store.Create(ctx, coinbaseTx, 0, utxo.WithConflicting(true))
		if createErr != nil {
			t.Logf("store.Create(coinbaseTx, WithConflicting(true)) returned error (falling back to raw PutBins): %v", createErr)

			// Fall back: create the coinbase without the flag, then flip the
			// Conflicting bin directly on the record via the raw client.
			_, err = store.Create(ctx, coinbaseTx, 0)
			require.NoError(t, err)

			cbKey, keyErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), coinbaseTx.TxIDChainHash().CloneBytes())
			require.NoError(t, keyErr)

			writePolicy := util.GetAerospikeWritePolicy(tSettings, 0)
			writePolicy.RecordExistsAction = aerospike.UPDATE
			err = client.PutBins(writePolicy, cbKey, aerospike.NewBin(fields.Conflicting.String(), true))
			require.NoError(t, err)

			usedRawPutBins = true
		}

		// Verify the coinbase record really does carry conflicting=true now —
		// otherwise the assertion below would be a tautology.
		cbKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), coinbaseTx.TxIDChainHash().CloneBytes())
		require.NoError(t, err)

		rec, aeroErr := client.Get(util.GetAerospikeReadPolicy(tSettings), cbKey, fields.Conflicting.String(), fields.IsCoinbase.String())
		require.Nil(t, aeroErr)
		require.NotNil(t, rec)
		assert.Equal(t, true, rec.Bins[fields.Conflicting.String()], "coinbase record should be flagged conflicting=true (usedRawPutBins=%v)", usedRawPutBins)
		assert.Equal(t, true, rec.Bins[fields.IsCoinbase.String()], "coinbase record should be flagged isCoinbase=true")

		// Drain the iterator.
		it, err := store.GetConflictingTxIterator()
		require.NoError(t, err)
		collected := drainConflictingIterator(t, ctx, it)

		collectedSet := make(map[string]struct{}, len(collected))
		for _, h := range collected {
			collectedSet[h.String()] = struct{}{}
		}

		// Coinbase must NOT appear even though its Conflicting bin is true.
		_, cbPresent := collectedSet[coinbaseTx.TxIDChainHash().String()]
		assert.False(t, cbPresent, "coinbase tx must be filtered out of GetConflictingTxIterator; collected=%v", sortedHashStrings(collected))

		// The non-coinbase conflicting child must appear — this proves the
		// iterator is working and the coinbase exclusion above is a real
		// filter, not a dead iterator.
		_, childPresent := collectedSet[child.TxIDChainHash().String()]
		assert.True(t, childPresent, "non-coinbase conflicting tx should be yielded by GetConflictingTxIterator; collected=%v", sortedHashStrings(collected))
	})
}
