package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
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

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 0))
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
