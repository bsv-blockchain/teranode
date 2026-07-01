package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestSpendPaginatedStampsDAHAtSpendBlockHeight is a regression test for the
// early-pruning bug on the spend path for PAGINATED transactions.
//
// A non-paginated tx gets its delete-at-height from the spend UDF / expression
// using the per-call spend block height (correct). But for a paginated tx the
// final "all spent" transition is detected while accounting the extra records
// (incrementSpentExtraRecs), and that path stamped the master DAH from the store's
// cached chain tip (s.blockHeight) instead of the spend's block height. During
// catchup/sync the cached tip lags behind the block being validated, so the DAH
// was stamped too low and the pruner deleted large/paginated txs before the
// retention window elapsed.
//
// Uses the Lua spend path (utxoBatchSize > 1). The same increment-path fix also
// covers IncrementSpentRecordsMulti, used by the filter-expression spend path and
// the setMined extra-records accounting.
func TestSpendPaginatedStampsDAHAtSpendBlockHeight(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	const addr = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
	const numOutputs = 6 // with utxoBatchSize=4 => master(4) + one pagination record(2)

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	retention := tSettings.GetUtxoStoreBlockHeightRetention()
	require.Greater(t, retention, uint32(0), "test requires a non-zero block height retention")

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

	// Cached chain tip lags far behind the block doing the spends — the catchup /
	// sync condition that triggered the bug.
	const cachedTip uint32 = 100
	const spendHeight uint32 = 900_000
	require.NoError(t, store.SetBlockHeight(cachedTip))

	// Mined, on-longest-chain, paginated parent with spendable outputs (still
	// unspent, so no DAH yet). Non-zero previous txid keeps it from being a coinbase.
	parent := bt.NewTx()
	require.NoError(t, parent.From(
		"1111111111111111111111111111111111111111111111111111111111111111",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		1_000_000,
	))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
	for range numOutputs {
		require.NoError(t, parent.PayToAddress(addr, 1000))
	}
	parentHash := parent.TxIDChainHash()
	_, err := store.Create(ctx, parent, 500, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: 1, BlockHeight: 500, SubtreeIdx: 0, OnLongestChain: true,
	}))
	require.NoError(t, err)

	// Spend every output, each in its own tx and in vout order: the master record's
	// outputs first, then the pagination record. Spending the last extra-record
	// output triggers the increment-path "all spent" transition that stamps the DAH.
	for i := 0; i < numOutputs; i++ {
		child := bt.NewTx()
		require.NoError(t, child.From(parentHash.String(), uint32(i), parent.Outputs[i].LockingScript.String(), parent.Outputs[i].Satoshis))
		require.NoError(t, child.PayToAddress(addr, 500))
		_, err := store.Spend(ctx, child, spendHeight)
		require.NoError(t, err, "spending output %d should succeed", i)
	}

	// The master record's DAH must be relative to the spend block height, not the
	// lagging cached tip.
	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(parentHash, 0))
	require.NoError(t, err)
	rec, err := client.Get(nil, key)
	require.NoError(t, err)
	require.NotNil(t, rec)

	dah, ok := rec.Bins[fields.DeleteAtHeight.String()]
	require.True(t, ok, "a fully-spent, mined, paginated tx must have a deleteAtHeight")
	require.Equal(t, int(spendHeight+retention), dah,
		"master deleteAtHeight must be spendHeight + retention, not the lagging cachedTip + retention")
}
