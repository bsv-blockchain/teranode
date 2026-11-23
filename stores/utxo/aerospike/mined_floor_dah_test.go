package aerospike_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	apruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// A mined record must outlive its highest block by the retention window, whatever
// height a delete-at-height writer is given. SetConflicting stamps from the node's
// cached tip, which lags the block being processed, and the pruner deletes purely
// on the stamp. These tests mine at a height far above the cached tip, as catchup
// does, and check the stamp the conflicting flag leaves behind.

const (
	floorCachedTip    uint32 = 100
	floorMinedHeight  uint32 = 900_000
	floorMinedBlockID uint32 = 100
)

// createParentAndChild stores an unmined parent and an unmined child spending its
// output 0. SetConflicting needs the parent to exist to record the conflicting
// child against it.
func createParentAndChild(ctx context.Context, t *testing.T, store *teranode_aerospike.Store) (*chainhash.Hash, *bt.Tx) {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(
		"1111111111111111111111111111111111111111111111111111111111111111",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		100000,
	))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 90000))

	_, err := store.Create(ctx, parent, floorCachedTip)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 80000))

	_, err = store.Create(ctx, child, floorCachedTip)
	require.NoError(t, err)

	return child.TxIDChainHash(), child
}

func readMasterDAH(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash) (int, bool) {
	t.Helper()

	return readRecordDAH(t, client, store, hash, 0)
}

// Mined far above the cached tip, then flagged conflicting. The conflicting stamp
// must be taken from the mined height, not from the lagging tip.
func TestSetConflictingStampsDAHAtMinedFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	retention := tSettings.GetUtxoStoreBlockHeightRetention()
	require.Greater(t, retention, uint32(0))

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	childHash, _ := createParentAndChild(ctx, t, store)

	_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{childHash}, utxo.MinedBlockInfo{
		BlockID: floorMinedBlockID, BlockHeight: floorMinedHeight, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*childHash}, true)
	require.NoError(t, err)

	dah, ok := readMasterDAH(t, client, store, childHash)
	require.True(t, ok, "a conflicting record must carry a delete-at-height")
	require.GreaterOrEqual(t, dah, int(floorMinedHeight+retention),
		"the stamp must be at least mined height + retention, not the lagging tip + retention")
}

// Flagged conflicting while unmined, so stamped from the lagging tip, then mined far
// above it. Mining must raise the stamp to the mined floor. Covers the Lua UDF and
// the filter-expression path.
func TestSetMinedRaisesConflictingStampToMinedFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	for _, useExpressions := range []bool{false, true} {
		name := "lua"
		if useExpressions {
			name = "expressions"
		}

		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Aerospike.EnableSetMinedFilterExpressions = useExpressions
			retention := tSettings.GetUtxoStoreBlockHeightRetention()
			require.Greater(t, retention, uint32(0))

			client, store, _, cleanup := initAerospike(t, tSettings, logger)
			defer cleanup()
			cleanDB(t, client)

			require.NoError(t, store.SetBlockHeight(floorCachedTip))

			childHash, _ := createParentAndChild(ctx, t, store)

			_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*childHash}, true)
			require.NoError(t, err)

			before, ok := readMasterDAH(t, client, store, childHash)
			require.True(t, ok, "setup: the conflicting flag must have stamped a delete-at-height")
			require.Less(t, before, int(floorMinedHeight), "setup: the stamp must come from the lagging tip")

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{childHash}, utxo.MinedBlockInfo{
				BlockID: floorMinedBlockID, BlockHeight: floorMinedHeight, SubtreeIdx: 0, OnLongestChain: true,
			})
			require.NoError(t, err)

			after, ok := readMasterDAH(t, client, store, childHash)
			require.True(t, ok)
			require.GreaterOrEqual(t, after, int(floorMinedHeight+retention),
				"mining must raise a stale conflicting stamp to mined height + retention")
		})
	}
}

// Mined far above the cached tip while an output is still unspent, so mining leaves
// no stamp. The last output is then spent with the lagging tip as the spending
// height. The stamp the spend writes must still be at least mined height +
// retention. Covers the Lua UDF and the filter-expression spend path.
func TestSpendStampsDAHAtMinedFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	for _, useExpressions := range []bool{false, true} {
		name := "lua"
		if useExpressions {
			name = "expressions"
		}

		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Aerospike.EnableSpendFilterExpressions = useExpressions
			// the expression spend path only applies to single-UTXO records
			tSettings.UtxoStore.UtxoBatchSize = 1
			retention := tSettings.GetUtxoStoreBlockHeightRetention()
			require.Greater(t, retention, uint32(0))

			client, store, _, cleanup := initAerospike(t, tSettings, logger)
			defer cleanup()
			cleanDB(t, client)

			require.NoError(t, store.SetBlockHeight(floorCachedTip))

			parent := bt.NewTx()
			require.NoError(t, parent.From(
				"1111111111111111111111111111111111111111111111111111111111111111",
				0,
				"76a914000000000000000000000000000000000000000088ac",
				100000,
			))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 90000))

			parentHash := parent.TxIDChainHash()

			_, err := store.Create(ctx, parent, floorCachedTip)
			require.NoError(t, err)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
				BlockID: floorMinedBlockID, BlockHeight: floorMinedHeight, SubtreeIdx: 0, OnLongestChain: true,
			})
			require.NoError(t, err)

			_, ok := readMasterDAH(t, client, store, parentHash)
			require.False(t, ok, "setup: a mined tx with an unspent output must carry no stamp yet")

			child := bt.NewTx()
			require.NoError(t, child.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 80000))

			_, err = store.Spend(ctx, child, floorCachedTip)
			require.NoError(t, err)

			dah, ok := readMasterDAH(t, client, store, parentHash)
			require.True(t, ok, "spending the last output of a mined tx must stamp a delete-at-height")
			require.GreaterOrEqual(t, dah, int(floorMinedHeight+retention),
				"the stamp must be at least mined height + retention, not the lagging tip + retention")
		})
	}
}

func floorCreatePaginatedParent(ctx context.Context, t *testing.T, store *teranode_aerospike.Store, vout uint32) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(
		"1111111111111111111111111111111111111111111111111111111111111111",
		vout,
		"76a914000000000000000000000000000000000000000088ac",
		100000,
	))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	_, err := store.Create(ctx, parent, floorCachedTip)
	require.NoError(t, err)

	return parent
}

func floorSpendOutput(ctx context.Context, t *testing.T, store *teranode_aerospike.Store, parent *bt.Tx, vout uint32) {
	t.Helper()

	spender := bt.NewTx()
	require.NoError(t, spender.From(parent.TxID(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
	spender.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
	require.NoError(t, spender.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	_, err := store.Spend(ctx, spender, floorCachedTip)
	require.NoError(t, err)
}

func readRecordDAH(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, recordIdx uint32) (int, bool) {
	t.Helper()

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, recordIdx))
	require.NoError(t, err)

	rec, err := client.Get(nil, key)
	require.NoError(t, err)
	require.NotNil(t, rec)

	v, ok := rec.Bins[fields.DeleteAtHeight.String()]
	if !ok || v == nil {
		return 0, false
	}

	return v.(int), true
}

// A paginated tx mined far above the cached tip and then fully spent at the lagging
// tip. The master is stamped at mined height + retention, and the pagination record
// must carry the same stamp, or the pruner deletes it while the master still points
// at it. Spending the master's output last exercises the spend signal, spending the
// pagination record's output last exercises the incrementSpentExtraRecs signal.
func TestSpendStampsPaginationRecordsAtMinedFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	tests := []struct {
		name       string
		spendOrder []uint32
	}{
		{name: "master output spent last", spendOrder: []uint32{1, 0}},
		{name: "pagination output spent last", spendOrder: []uint32{0, 1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UtxoBatchSize = 1 // master holds output 0, the pagination record holds output 1
			retention := tSettings.GetUtxoStoreBlockHeightRetention()
			require.Greater(t, retention, uint32(0))

			client, store, _, cleanup := initAerospike(t, tSettings, logger)
			defer cleanup()
			cleanDB(t, client)

			require.NoError(t, store.SetBlockHeight(floorCachedTip))

			parent := floorCreatePaginatedParent(ctx, t, store, 0)
			parentHash := parent.TxIDChainHash()

			_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
				BlockID: floorMinedBlockID, BlockHeight: floorMinedHeight, SubtreeIdx: 0, OnLongestChain: true,
			})
			require.NoError(t, err)

			for _, vout := range tc.spendOrder {
				floorSpendOutput(ctx, t, store, parent, vout)
			}

			want := int(floorMinedHeight + retention)

			masterDAH, ok := readRecordDAH(t, client, store, parentHash, 0)
			require.True(t, ok, "a fully spent mined tx must carry a delete-at-height")
			require.Equal(t, want, masterDAH)

			require.Eventually(t, func() bool {
				childDAH, childOK := readRecordDAH(t, client, store, parentHash, 1)

				return childOK && childDAH == want
			}, 5*time.Second, 50*time.Millisecond, "the pagination record must carry the master's stamp %d", want)
		})
	}
}

// The tx is mined on a fork at a high block, fully spent (no stamp, it is not on the
// longest chain), then mined on the longest chain at a much lower block. setMined
// stamps from the master's highest mined height, which is the fork block, so the
// pagination record must follow that stamp and not the lower block it was just mined at.
func TestSetMinedStampsPaginationRecordsAtMinedFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 1
	retention := tSettings.GetUtxoStoreBlockHeightRetention()
	require.Greater(t, retention, uint32(0))

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	parent := floorCreatePaginatedParent(ctx, t, store, 1)
	parentHash := parent.TxIDChainHash()

	_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
		BlockID: floorMinedBlockID, BlockHeight: floorMinedHeight, SubtreeIdx: 0, OnLongestChain: false,
	})
	require.NoError(t, err)

	floorSpendOutput(ctx, t, store, parent, 0)
	floorSpendOutput(ctx, t, store, parent, 1)

	_, ok := readRecordDAH(t, client, store, parentHash, 0)
	require.False(t, ok, "setup: a tx mined off the longest chain must carry no stamp")

	const lowBlockHeight uint32 = 500

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
		BlockID: floorMinedBlockID + 1, BlockHeight: lowBlockHeight, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err)

	want := int(floorMinedHeight + retention)

	masterDAH, ok := readRecordDAH(t, client, store, parentHash, 0)
	require.True(t, ok, "mining a fully spent tx on the longest chain must stamp a delete-at-height")
	require.Equal(t, want, masterDAH)

	require.Eventually(t, func() bool {
		childDAH, childOK := readRecordDAH(t, client, store, parentHash, 1)

		return childOK && childDAH == want
	}, 5*time.Second, 50*time.Millisecond, "the pagination record must carry the master's stamp %d", want)
}

// A conflicting paginated master is prunable whatever its spent state, so every page must
// carry the master's stamp. The tests below force a conflicting master and its pages to a
// stamp below the mined floor, as a pre-upgrade record or a native-ops write leaves them,
// then drive each path that raises the master and check the pages follow.

const (
	floorStaleOffset  uint32 = 10
	floorPaginatedOut        = 3
)

func floorSettings(t *testing.T) (*settings.Settings, uint32) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 1
	tSettings.UtxoStore.DisableDAHCleaner = false

	retention := tSettings.GetUtxoStoreBlockHeightRetention()
	require.Greater(t, retention, uint32(0))

	return tSettings, retention
}

// floorCreateThreePageParent stores a tx whose output 0 is on the master record and whose
// outputs 1 and 2 are each on their own pagination record.
func floorCreateThreePageParent(ctx context.Context, t *testing.T, store *teranode_aerospike.Store, vout uint32) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(
		"1111111111111111111111111111111111111111111111111111111111111111",
		vout,
		"76a914000000000000000000000000000000000000000088ac",
		100000,
	))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})

	for range floorPaginatedOut {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}

	_, err := store.Create(ctx, parent, floorCachedTip)
	require.NoError(t, err)

	return parent
}

func floorKey(t *testing.T, store *teranode_aerospike.Store, hash *chainhash.Hash, recordIdx uint32) *aerospike.Key {
	t.Helper()

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, recordIdx))
	require.NoError(t, err)

	return key
}

// floorForceStaleStamps marks the master mined at floorMinedHeight and stamps the master and
// both pages at stale. conflicting selects whether the master already carries the flag.
func floorForceStaleStamps(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, stale uint32, conflicting bool) {
	t.Helper()

	require.NoError(t, client.PutBins(nil, floorKey(t, store, hash, 0),
		aerospike.NewBin(fields.Conflicting.String(), conflicting),
		aerospike.NewBin(fields.BlockIDs.String(), []int{int(floorMinedBlockID)}),
		aerospike.NewBin(fields.BlockHeights.String(), []int{int(floorMinedHeight)}),
		aerospike.NewBin(fields.SubtreeIdxs.String(), []int{0}),
		aerospike.NewBin(fields.DeleteAtHeight.String(), int(stale)),
	))

	for page := uint32(1); page < floorPaginatedOut; page++ {
		require.NoError(t, client.PutBins(nil, floorKey(t, store, hash, page),
			aerospike.NewBin(fields.DeleteAtHeight.String(), int(stale)),
		))
	}
}

func floorRequireAllRecordsAt(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, want int) {
	t.Helper()

	masterDAH, ok := readRecordDAH(t, client, store, hash, 0)
	require.True(t, ok, "the conflicting master must keep a delete-at-height")
	require.Equal(t, want, masterDAH)

	for page := uint32(1); page < floorPaginatedOut; page++ {
		require.Eventually(t, func() bool {
			dah, pageOK := readRecordDAH(t, client, store, hash, page)

			return pageOK && dah == want
		}, 5*time.Second, 50*time.Millisecond, "page %d must carry the master's stamp %d", page, want)
	}
}

// The last output of one page is spent, which goes through incrementSpentExtraRecs on a
// conflicting master whose other page is still unspent. The master is raised to the floor
// and the pages follow. The master's stamp must not be cleared as counter drift.
func TestIncrementSpentExtraRecsRaisesConflictingMasterAndPages(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	parent := floorCreateThreePageParent(ctx, t, store, 0)
	hash := parent.TxIDChainHash()

	floorForceStaleStamps(t, client, store, hash, floorCachedTip+retention, true)

	spender := bt.NewTx()
	require.NoError(t, spender.From(parent.TxID(), 1, parent.Outputs[1].LockingScript.String(), parent.Outputs[1].Satoshis))
	spender.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
	require.NoError(t, spender.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	_, err := store.Spend(ctx, spender, floorCachedTip, utxo.IgnoreFlags{IgnoreConflicting: true})
	require.NoError(t, err)

	floorRequireAllRecordsAt(t, client, store, hash, int(floorMinedHeight+retention))
}

// SetConflicting on a paginated mined master with a stale low stamp raises the master and
// every page to the floor, so a pruner pass between the old and the new date deletes nothing.
func TestSetConflictingRaisesPaginatedMasterAndPagesToMinedFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	teranode_aerospike.ResetPrunerServiceForTests()
	t.Cleanup(teranode_aerospike.ResetPrunerServiceForTests)
	require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

	svc, err := store.GetPrunerService()
	require.NoError(t, err)

	prunerSvc, ok := svc.(*apruner.Service)
	require.True(t, ok)

	parent := floorCreateThreePageParent(ctx, t, store, 1)
	hash := parent.TxIDChainHash()
	stale := floorCachedTip + retention + floorStaleOffset

	floorForceStaleStamps(t, client, store, hash, stale, false)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*hash}, true)
	require.NoError(t, err)

	floorRequireAllRecordsAt(t, client, store, hash, int(floorMinedHeight+retention))

	n, err := prunerSvc.PruneWithPartitions(ctx, stale, "conflicting", 1)
	require.NoError(t, err)
	require.Zero(t, n, "no record of the raised conflicting tx may be pruned at its old stamp")

	for page := uint32(0); page < floorPaginatedOut; page++ {
		rec, getErr := client.Get(nil, floorKey(t, store, hash, page))
		require.NoError(t, getErr)
		require.NotNil(t, rec, "record %d must survive the pruner pass", page)
	}
}

// Unspending an output of a conflicting master that carries a stale stamp raises the master
// to the floor, and the pages take the reported stamp.
func TestUnspendRaisesConflictingMasterPagesToMinedFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	parent := floorCreateThreePageParent(ctx, t, store, 2)
	hash := parent.TxIDChainHash()

	spender := bt.NewTx()
	require.NoError(t, spender.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	spender.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
	require.NoError(t, spender.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	spends, err := store.Spend(ctx, spender, floorCachedTip)
	require.NoError(t, err)

	floorForceStaleStamps(t, client, store, hash, floorCachedTip+retention, true)

	require.NoError(t, store.Unspend(ctx, spends))

	floorRequireAllRecordsAt(t, client, store, hash, int(floorMinedHeight+retention))
}

// IncrementSpentRecordsMulti on a conflicting master with a stale stamp raises the master to
// the floor, and the pages take the reported stamp.
func TestIncrementSpentRecordsMultiStampsConflictingPagesAtMinedFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	parent := floorCreateThreePageParent(ctx, t, store, 3)
	hash := parent.TxIDChainHash()

	floorForceStaleStamps(t, client, store, hash, floorCachedTip+retention, true)

	require.NoError(t, store.IncrementSpentRecordsMulti(ctx, []*chainhash.Hash{hash}, 1, floorCachedTip))

	floorRequireAllRecordsAt(t, client, store, hash, int(floorMinedHeight+retention))
}

func floorPrunerService(ctx context.Context, t *testing.T, store *teranode_aerospike.Store) *apruner.Service {
	t.Helper()

	teranode_aerospike.ResetPrunerServiceForTests()
	t.Cleanup(teranode_aerospike.ResetPrunerServiceForTests)
	require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

	svc, err := store.GetPrunerService()
	require.NoError(t, err)

	prunerSvc, ok := svc.(*apruner.Service)
	require.True(t, ok)

	return prunerSvc
}

// floorFailPageWrites makes every write to a pagination record of hash fail, as a network error does,
// until the returned function is called. Writes to any other record go through.
func floorFailPageWrites(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash) (heal func()) {
	t.Helper()

	pageDigests := make(map[string]struct{}, floorPaginatedOut-1)
	for page := uint32(1); page < floorPaginatedOut; page++ {
		pageDigests[string(floorKey(t, store, hash, page).Digest())] = struct{}{}
	}

	store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		for _, r := range records {
			if _, isPage := pageDigests[string(r.BatchRec().Key.Digest())]; isPage {
				return aerospike.ErrNetwork
			}
		}

		return client.BatchOperate(policy, records)
	})

	healed := false
	heal = func() {
		if !healed {
			healed = true

			store.SetBatchOperateFn(nil)
		}
	}

	t.Cleanup(heal)

	return heal
}

// floorRequirePagesAt requires every pagination record of hash to carry want, or no stamp when want is 0.
func floorRequirePagesAt(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, want int) {
	t.Helper()

	for page := uint32(1); page < floorPaginatedOut; page++ {
		dah, stamped := readRecordDAH(t, client, store, hash, page)
		if want == 0 {
			require.False(t, stamped, "page %d must carry no stamp, has %d", page, dah)

			continue
		}

		require.True(t, stamped, "page %d must carry a stamp", page)
		require.Equal(t, want, dah, "page %d stamp", page)
	}
}

func floorSpendFirstOutput(ctx context.Context, t *testing.T, store *teranode_aerospike.Store, parent *bt.Tx) []*utxo.Spend {
	t.Helper()

	spender := bt.NewTx()
	require.NoError(t, spender.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	spender.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
	require.NoError(t, spender.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	spends, err := store.Spend(ctx, spender, floorCachedTip)
	require.NoError(t, err)

	return spends
}

// A SetConflicting whose page write failed after the master batch committed returns an
// error and is retried. The retry finds the master already holding the value, so no signal
// fires, and the pages must still be brought in line with the master.
func TestSetConflictingRetryRepairsPagesAfterFailedPageWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	prunerSvc := floorPrunerService(ctx, t, store)
	stale := floorCachedTip + retention + floorStaleOffset

	t.Run("pages left below the master's stamp are raised", func(t *testing.T) {
		parent := floorCreateThreePageParent(ctx, t, store, 4)
		hash := parent.TxIDChainHash()

		floorForceStaleStamps(t, client, store, hash, stale, false)

		heal := floorFailPageWrites(t, client, store, hash)

		_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*hash}, true)
		require.Error(t, err, "the failed page write must be reported")

		masterDAH, ok := readRecordDAH(t, client, store, hash, 0)
		require.True(t, ok)
		require.Equal(t, int(floorMinedHeight+retention), masterDAH, "the master committed the raise")
		floorRequirePagesAt(t, client, store, hash, int(stale))

		heal()

		_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*hash}, true)
		require.NoError(t, err)

		floorRequireAllRecordsAt(t, client, store, hash, int(floorMinedHeight+retention))

		n, err := prunerSvc.PruneWithPartitions(ctx, stale, "conflicting-retry-raise", 1)
		require.NoError(t, err)
		require.Zero(t, n, "no page may be pruned before its master")

		for page := uint32(0); page < floorPaginatedOut; page++ {
			rec, getErr := client.Get(nil, floorKey(t, store, hash, page))
			require.NoError(t, getErr)
			require.NotNil(t, rec, "record %d must survive the pruner pass", page)
		}
	})

	t.Run("pages left stamped after the master was cleared are cleared", func(t *testing.T) {
		parent := floorCreateThreePageParent(ctx, t, store, 5)
		hash := parent.TxIDChainHash()

		floorForceStaleStamps(t, client, store, hash, stale, true)

		heal := floorFailPageWrites(t, client, store, hash)

		_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*hash}, false)
		require.Error(t, err, "the failed page write must be reported")

		_, masterStamped := readRecordDAH(t, client, store, hash, 0)
		require.False(t, masterStamped, "the master committed the clear")
		floorRequirePagesAt(t, client, store, hash, int(stale))

		heal()

		_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*hash}, false)
		require.NoError(t, err)

		floorRequirePagesAt(t, client, store, hash, 0)
	})
}

// An Unspend whose page write failed after the master committed is retried. The retry is
// idempotent on the master, so no signal fires, and the pages must still be cleared.
func TestUnspendRetryClearsPagesAfterFailedPageWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	parent := floorCreateThreePageParent(ctx, t, store, 6)
	hash := parent.TxIDChainHash()

	spends := floorSpendFirstOutput(ctx, t, store, parent)

	floorForceStaleStamps(t, client, store, hash, floorCachedTip+retention, false)

	heal := floorFailPageWrites(t, client, store, hash)

	require.Error(t, store.Unspend(ctx, spends), "the failed page write must be reported")

	_, masterStamped := readRecordDAH(t, client, store, hash, 0)
	require.False(t, masterStamped, "the master committed the clear")
	floorRequirePagesAt(t, client, store, hash, int(floorCachedTip+retention))

	heal()

	require.NoError(t, store.Unspend(ctx, spends))

	floorRequirePagesAt(t, client, store, hash, 0)
}

// An Unspend that changes nothing on a paginated master still reconciles every page, once per page
// per evaluation, and leaves them as they were. The control then stamps the master and unspends
// again: the DAHUNSET signal makes the pages get cleared.
func TestUnspendWithoutASignalReconcilesEveryPage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	parent := floorCreateThreePageParent(ctx, t, store, 7)
	hash := parent.TxIDChainHash()

	spends := floorSpendFirstOutput(ctx, t, store, parent)

	pageDigests := make(map[string]struct{}, floorPaginatedOut-1)
	for page := uint32(1); page < floorPaginatedOut; page++ {
		pageDigests[string(floorKey(t, store, hash, page).Digest())] = struct{}{}
	}

	var pageWrites atomic.Int64

	store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		for _, r := range records {
			if _, isPage := pageDigests[string(r.BatchRec().Key.Digest())]; isPage {
				pageWrites.Add(1)
			}
		}

		return client.BatchOperate(policy, records)
	})
	t.Cleanup(func() { store.SetBatchOperateFn(nil) })

	require.NoError(t, store.Unspend(ctx, spends))
	require.Equal(t, int64(floorPaginatedOut-1), pageWrites.Load(), "an unspend that fires no signal must still write every page once")
	floorRequirePagesAt(t, client, store, hash, 0)

	floorSpendFirstOutput(ctx, t, store, parent)
	floorForceStaleStamps(t, client, store, hash, floorCachedTip+retention, false)

	require.NoError(t, store.Unspend(ctx, spends))
	require.Equal(t, int64(2*(floorPaginatedOut-1)), pageWrites.Load(), "a DAHUNSET signal must write every page once more")
	floorRequirePagesAt(t, client, store, hash, 0)
}

// pauseNextPageWrite holds the first page write back before it is enqueued. entered closes when that
// write is waiting. release lets it go.
func pauseNextPageWrite(t *testing.T) (entered <-chan struct{}, release func()) {
	t.Helper()

	enteredCh := make(chan struct{})
	gate := make(chan struct{})

	var (
		first atomic.Bool
		once  sync.Once
	)

	teranode_aerospike.SetChildDAHBeforeWrite(func() {
		if first.CompareAndSwap(false, true) {
			close(enteredCh)
			<-gate
		}
	})

	release = func() { once.Do(func() { close(gate) }) }

	t.Cleanup(func() {
		release()
		teranode_aerospike.SetChildDAHBeforeWrite(nil)
	})

	return enteredCh, release
}

// An Unspend reports the master's state, then writes the pages from it. SetConflicting(false) can commit
// and clear master and pages in between. The held-back page write then carries an older master version
// than the pages hold, so it must be refused, or the pruner deletes the pages of a live master.
func TestUnspendStalePageWriteCannotOverwriteAClearedMaster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	prunerSvc := floorPrunerService(ctx, t, store)
	stamp := int(floorMinedHeight + retention)

	run := func(t *testing.T, vout uint32, prepare func(hash *chainhash.Hash)) {
		parent := floorCreateThreePageParent(ctx, t, store, vout)
		hash := parent.TxIDChainHash()
		spends := floorSpendFirstOutput(ctx, t, store, parent)

		prepare(hash)

		entered, release := pauseNextPageWrite(t)

		unspendErr := make(chan error, 1)

		go func() { unspendErr <- store.Unspend(ctx, spends) }()

		select {
		case <-entered:
		case <-time.After(20 * time.Second):
			release()
			require.FailNow(t, "the unspend never reached its page write")
		}

		_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*hash}, false)
		require.NoError(t, err)

		_, masterStamped := readRecordDAH(t, client, store, hash, 0)
		require.False(t, masterStamped, "setup: SetConflicting(false) cleared the master")
		floorRequirePagesAt(t, client, store, hash, 0)

		release()
		require.NoError(t, <-unspendErr)

		_, masterStamped = readRecordDAH(t, client, store, hash, 0)
		require.False(t, masterStamped, "the master must stay unstamped")
		floorRequirePagesAt(t, client, store, hash, 0)

		n, err := prunerSvc.PruneWithPartitions(ctx, uint32(stamp), fmt.Sprintf("stale-page-%d", vout), 1)
		require.NoError(t, err)
		require.Zero(t, n, "the pruner must find nothing to delete")

		for page := uint32(0); page < floorPaginatedOut; page++ {
			rec, getErr := client.Get(nil, floorKey(t, store, hash, page))
			require.NoError(t, getErr)
			require.NotNil(t, rec, "record %d must survive the pruner pass", page)
		}
	}

	t.Run("the unspend response carried a DAHSET signal", func(t *testing.T) {
		run(t, 8, func(hash *chainhash.Hash) {
			floorForceStaleStamps(t, client, store, hash, floorCachedTip+retention, true)
		})
	})

	t.Run("the unspend finds the pages stale after a failed page write", func(t *testing.T) {
		run(t, 9, func(hash *chainhash.Hash) {
			floorForceStaleStamps(t, client, store, hash, uint32(stamp), true)

			heal := floorFailPageWrites(t, client, store, hash)
			require.Error(t, store.SetDAHForChildRecords(hash, int(floorPaginatedOut-1), uint32(stamp)), "setup: a page write must fail")
			heal()
		})
	})
}

func floorDAHVersion(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, recordIdx uint32) int {
	t.Helper()

	rec, err := client.Get(nil, floorKey(t, store, hash, recordIdx))
	require.NoError(t, err)
	require.NotNil(t, rec)

	v, ok := rec.Bins[fields.DAHVersion.String()]
	if !ok || v == nil {
		return 0
	}

	return v.(int)
}

func floorKeyDAHVersion(t *testing.T, client *uaerospike.Client, key *aerospike.Key) int {
	t.Helper()

	rec, err := client.Get(nil, key)
	require.NoError(t, err)
	require.NotNil(t, rec)

	v, ok := rec.Bins[fields.DAHVersion.String()]
	if !ok || v == nil {
		return 0
	}

	return v.(int)
}

// Every writer of a master's delete-at-height must bump its dahVer in the same operation, or a
// stale page write could pass the version filter. Each case performs one writer's real write and
// requires the master's version to have grown. The restore cases fail the page writes, so the
// writer bumps once and its restore bumps again.
func TestGoMasterDAHWritersBumpTheVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	tSettings, _ := floorSettings(t)
	tSettings.Aerospike.EnablePreserveFilterExpressions = true
	tSettings.Aerospike.EnableSetMinedFilterExpressions = true
	tSettings.Aerospike.EnableSpendFilterExpressions = true

	client, store, _, cleanup := initAerospike(t, tSettings, ulogger.NewErrorTestLogger(t))
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	t.Run("setConflicting through the Lua package", func(t *testing.T) {
		hash := floorCreateThreePageParent(ctx, t, store, 20).TxIDChainHash()
		before := floorDAHVersion(t, client, store, hash, 0)

		_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*hash}, true)
		require.NoError(t, err)

		require.Greater(t, floorDAHVersion(t, client, store, hash, 0), before)
	})

	t.Run("preserve with expressions", func(t *testing.T) {
		hash := floorCreateThreePageParent(ctx, t, store, 21).TxIDChainHash()
		require.NoError(t, client.PutBins(nil, floorKey(t, store, hash, 0), aerospike.NewBin(fields.DeleteAtHeight.String(), 5000)))

		before := floorDAHVersion(t, client, store, hash, 0)

		require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*hash}, 9000))

		_, stamped := readRecordDAH(t, client, store, hash, 0)
		require.False(t, stamped, "setup: the preserve cleared the stamp")
		require.Greater(t, floorDAHVersion(t, client, store, hash, 0), before)
	})

	t.Run("set mined with expressions", func(t *testing.T) {
		hash := floorCreateThreePageParent(ctx, t, store, 22).TxIDChainHash()
		before := floorDAHVersion(t, client, store, hash, 0)

		_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{hash}, utxo.MinedBlockInfo{BlockID: floorMinedBlockID, BlockHeight: floorMinedHeight, OnLongestChain: true})
		require.NoError(t, err)

		require.Greater(t, floorDAHVersion(t, client, store, hash, 0), before)
	})

	t.Run("spend with expressions", func(t *testing.T) {
		parent := floorCreateThreePageParent(ctx, t, store, 23)
		hash := parent.TxIDChainHash()
		before := floorDAHVersion(t, client, store, hash, 0)

		floorSpendFirstOutput(ctx, t, store, parent)

		require.Greater(t, floorDAHVersion(t, client, store, hash, 0), before)
	})

	t.Run("spend with expressions leaves the version of a page alone", func(t *testing.T) {
		parent := floorCreateThreePageParent(ctx, t, store, 25)
		hash := parent.TxIDChainHash()

		floorSpendOutput(ctx, t, store, parent, 1)

		floorRequirePageUntouched(t, client, store, hash, 1)
	})

	t.Run("setDAH batcher master write after counter drift", func(t *testing.T) {
		hash := floorCreateThreePageParent(ctx, t, store, 24).TxIDChainHash()
		require.NoError(t, client.PutBins(nil, floorKey(t, store, hash, 0),
			aerospike.NewBin(fields.SpentUtxos.String(), 1),
			aerospike.NewBin(fields.BlockIDs.String(), []int{int(floorMinedBlockID)}),
			aerospike.NewBin(fields.UnminedSince.String(), nil),
			aerospike.NewBin(fields.SpentExtraRecs.String(), 1),
		))

		before := floorDAHVersion(t, client, store, hash, 0)

		require.NoError(t, store.IncrementSpentRecordsMulti(ctx, []*chainhash.Hash{hash}, 1, floorCachedTip))

		_, stamped := readRecordDAH(t, client, store, hash, 0)
		require.False(t, stamped, "setup: the drifted stamp was cleared by the batcher")
		require.Equal(t, before+2, floorDAHVersion(t, client, store, hash, 0), "the Lua stamp and the batcher clear each bump once")
	})

	t.Run("longest chain", func(t *testing.T) {
		require.NoError(t, store.SetBlockHeight(1000))
		t.Cleanup(func() { require.NoError(t, store.SetBlockHeight(floorCachedTip)) })

		h := &childDAHHarness{t: t, client: client, store: store, dahBin: fields.DeleteAtHeight.String()}
		txID, masterKey, _ := h.seed("bump-longest-chain", 1002, 0)

		before := floorKeyDAHVersion(t, client, masterKey)

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{txID}, true))

		require.Equal(t, before+1, floorKeyDAHVersion(t, client, masterKey))
	})

	t.Run("longest chain restore", func(t *testing.T) {
		require.NoError(t, store.SetBlockHeight(1000))
		t.Cleanup(func() { require.NoError(t, store.SetBlockHeight(floorCachedTip)) })

		h := &childDAHHarness{t: t, client: client, store: store, dahBin: fields.DeleteAtHeight.String()}
		txID, masterKey, pageKey := h.seed("bump-longest-chain-restore", 1002, 0)

		before := floorKeyDAHVersion(t, client, masterKey)

		store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
			for _, r := range records {
				if string(r.BatchRec().Key.Digest()) == string(pageKey.Digest()) {
					return aerospike.ErrNetwork
				}
			}

			return client.BatchOperate(policy, records)
		})
		t.Cleanup(func() { store.SetBatchOperateFn(nil) })

		require.Error(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{txID}, true))
		require.Equal(t, 1002, h.dah(masterKey), "setup: the master was restored")
		require.Equal(t, before+2, floorKeyDAHVersion(t, client, masterKey), "the write and its restore each bump once")
	})

	h := newExpiredFailureHarness(t)

	t.Run("expired preservation", func(t *testing.T) {
		h.setup(t, 30, false)

		before := floorKeyDAHVersion(t, h.client, h.k0)

		require.NoError(t, h.run(t))

		master, _ := h.bins(t)
		require.Nil(t, master[fields.DeleteAtHeight.String()], "setup: the expiry cleared the stale stamp")
		require.Equal(t, before+1, floorKeyDAHVersion(t, h.client, h.k0))
	})

	t.Run("expired preservation restore", func(t *testing.T) {
		h.setup(t, 31, false)
		h.failFirst(t, 2, nil)

		before := floorKeyDAHVersion(t, h.client, h.k0)

		require.Error(t, h.run(t))

		master, _ := h.bins(t)
		require.Equal(t, 5, master[fields.DeleteAtHeight.String()], "setup: the master was restored")
		require.Equal(t, before+2, floorKeyDAHVersion(t, h.client, h.k0), "the write and its restore each bump once")
	})
}

// floorCreateConflictingThreePageParent stores a conflicting tx of five outputs, which a batch size of two
// splits into a master and two pages. A spend of one of the two outputs of page 1 leaves the page's
// spent state as it was, so the master is not involved.
func floorCreateConflictingThreePageParent(ctx context.Context, t *testing.T, store *teranode_aerospike.Store, vout uint32) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(
		"1111111111111111111111111111111111111111111111111111111111111111",
		vout,
		"76a914000000000000000000000000000000000000000088ac",
		100000,
	))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})

	for range 5 {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}

	_, err := store.Create(ctx, parent, floorCachedTip, utxo.WithConflicting(true))
	require.NoError(t, err)

	return parent
}

func floorSpendAndUnspendOutput(ctx context.Context, t *testing.T, store *teranode_aerospike.Store, parent *bt.Tx, vout uint32) {
	t.Helper()

	spender := bt.NewTx()
	require.NoError(t, spender.From(parent.TxID(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
	spender.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
	require.NoError(t, spender.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	spends, err := store.Spend(ctx, spender, floorCachedTip, utxo.IgnoreFlags{IgnoreConflicting: true})
	require.NoError(t, err)
	require.NoError(t, store.Unspend(ctx, spends))
}

func floorRequirePageUntouched(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, page uint32) {
	t.Helper()

	dah, stamped := readRecordDAH(t, client, store, hash, page)
	require.False(t, stamped, "page %d must carry no stamp, has %d", page, dah)
	require.Zero(t, floorDAHVersion(t, client, store, hash, page), "page %d must carry no version", page)
}

func floorRequireRecordsExist(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash) {
	t.Helper()

	for page := uint32(0); page < floorPaginatedOut; page++ {
		rec, err := client.Get(nil, floorKey(t, store, hash, page))
		require.NoError(t, err)
		require.NotNil(t, rec, "record %d must survive the pruner pass", page)
	}
}

// A page created with the conflicting flag of a conflicting transaction keeps that flag after
// SetConflicting(false), which writes only the master. A spend or unspend on the page runs the Lua
// setDeleteAtHeight on it. The page must neither stamp itself nor count its own version, or the
// DAHUNSET page write of the master is refused and the pruner deletes the page of a live master.
func TestPageOfAConflictingTxNeverStampsOrVersionsItself(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	prunerSvc := floorPrunerService(ctx, t, store)
	pruneAt := floorMinedHeight + retention + 1000

	parent := floorCreateConflictingThreePageParent(ctx, t, store, 40)
	hash := parent.TxIDChainHash()

	require.NoError(t, client.PutBins(nil, floorKey(t, store, hash, 0),
		aerospike.NewBin(fields.BlockIDs.String(), []int{int(floorMinedBlockID)}),
		aerospike.NewBin(fields.BlockHeights.String(), []int{int(floorMinedHeight)}),
		aerospike.NewBin(fields.SubtreeIdxs.String(), []int{0}),
	))

	pageRec, getErr := client.Get(nil, floorKey(t, store, hash, 1))
	require.NoError(t, getErr)
	require.Equal(t, true, pageRec.Bins[fields.Conflicting.String()], "setup: the page carries the conflicting flag")

	floorSpendAndUnspendOutput(ctx, t, store, parent, 2)

	floorRequirePageUntouched(t, client, store, hash, 1)

	_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*hash}, false)
	require.NoError(t, err)

	floorRequirePagesAt(t, client, store, hash, 0)

	n, err := prunerSvc.PruneWithPartitions(ctx, pruneAt, "conflicting-page-self", 1)
	require.NoError(t, err)
	require.Zero(t, n)
	floorRequireRecordsExist(t, client, store, hash)

	require.NoError(t, client.PutBins(nil, floorKey(t, store, hash, 1),
		aerospike.NewBin(fields.DeleteAtHeight.String(), nil),
		aerospike.NewBin(fields.DAHVersion.String(), nil),
	))

	floorSpendAndUnspendOutput(ctx, t, store, parent, 2)

	floorRequirePageUntouched(t, client, store, hash, 1)

	n, err = prunerSvc.PruneWithPartitions(ctx, pruneAt, "conflicting-page-self-2", 1)
	require.NoError(t, err)
	require.Zero(t, n)
	floorRequireRecordsExist(t, client, store, hash)
}

// startPausedUnspendRaise starts an Unspend of a conflicting master whose stale stamp it raises to the
// mined floor. The unspend response carries a DAHSET signal, and its page write is held back. finish
// releases it and waits for the unspend.
func startPausedUnspendRaise(ctx context.Context, t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, retention, vout uint32) (hash *chainhash.Hash, finish func()) {
	t.Helper()

	parent := floorCreateThreePageParent(ctx, t, store, vout)
	hash = parent.TxIDChainHash()
	spends := floorSpendFirstOutput(ctx, t, store, parent)

	floorForceStaleStamps(t, client, store, hash, floorCachedTip+retention, true)

	entered, release := pauseNextPageWrite(t)

	unspendErr := make(chan error, 1)

	go func() { unspendErr <- store.Unspend(ctx, spends) }()

	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		release()
		require.FailNow(t, "the unspend never reached its page write")
	}

	return hash, func() {
		release()
		require.NoError(t, <-unspendErr)
	}
}

// An Unspend raised a conflicting master to the floor at one block height and its page write is
// delayed. A reorg then mines the transaction at a higher block, which raises the master again and
// stamps the pages. The delayed write carries the older master version and must not lower the pages.
func TestDelayedDAHSetWriteCannotUndoALaterSetMined(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	hash, finish := startPausedUnspendRaise(ctx, t, client, store, retention, 41)

	const reorgDelta = 50

	_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{hash}, utxo.MinedBlockInfo{BlockID: floorMinedBlockID + 1, BlockHeight: floorMinedHeight + reorgDelta, OnLongestChain: true})
	require.NoError(t, err)

	want := int(floorMinedHeight + reorgDelta + retention)

	masterDAH, ok := readRecordDAH(t, client, store, hash, 0)
	require.True(t, ok, "setup: the later setMined raised the master")
	require.Equal(t, want, masterDAH)
	floorRequirePagesAt(t, client, store, hash, want)

	finish()

	floorRequirePagesAt(t, client, store, hash, want)
}

// The same delayed DAHSET write meets a preserve that cleared the master and its pages. The pages
// must stay clear.
func TestDelayedDAHSetWriteCannotUndoAPreserveClear(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	for _, expressions := range []bool{false, true} {
		store.GetSettings().Aerospike.EnablePreserveFilterExpressions = expressions

		hash, finish := startPausedUnspendRaise(ctx, t, client, store, retention, 42+boolToVout(expressions))

		require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*hash}, floorMinedHeight+10_000))

		_, stamped := readRecordDAH(t, client, store, hash, 0)
		require.False(t, stamped, "setup: the preserve cleared the master (expressions=%v)", expressions)
		floorRequirePagesAt(t, client, store, hash, 0)

		finish()

		floorRequirePagesAt(t, client, store, hash, 0)
	}
}

func boolToVout(b bool) uint32 {
	if b {
		return 1
	}

	return 0
}

// Locking a tx clears its master's stamp, and the setLocked of each page clears the page's own. A page
// write for the earlier stamp that is still in flight must not restamp the pages of the locked master.
func TestDelayedDAHSetWriteCannotUndoALock(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings, retention := floorSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	prunerSvc := floorPrunerService(ctx, t, store)

	hash, finish := startPausedUnspendRaise(ctx, t, client, store, retention, 44)

	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*hash}, true))

	_, stamped := readRecordDAH(t, client, store, hash, 0)
	require.False(t, stamped, "setup: the lock cleared the master")
	floorRequirePagesAt(t, client, store, hash, 0)

	finish()

	floorRequirePagesAt(t, client, store, hash, 0)

	n, err := prunerSvc.PruneWithPartitions(ctx, floorMinedHeight+retention, "locked-stale-page", 1)
	require.NoError(t, err)
	require.Zero(t, n, "the pruner must find nothing to delete")
	floorRequireRecordsExist(t, client, store, hash)
}
