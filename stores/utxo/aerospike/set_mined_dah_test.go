package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestSetMinedStampsDAHAtMinedBlockHeight is a regression test for the early-pruning
// bug where setMined computed deleteAtHeight from the store's cached chain tip
// (s.blockHeight) rather than the height of the block the transaction was actually
// mined into (minedBlockInfo.BlockHeight).
//
// s.blockHeight is maintained by an asynchronous blockchain-notification
// subscription, so during catchup / sync / high load it lags behind the block being
// validated. When a transaction that was spent while still unmined is later marked
// mined, setMined stamps the DAH. Using the lagging cached height stamped the DAH far
// too low, so the pruner (which deletes records with deleteAtHeight <= currentHeight)
// removed the record long before the retention window had elapsed.
//
// Covers both the Lua UDF path and the filter-expression path.
func TestSetMinedStampsDAHAtMinedBlockHeight(t *testing.T) {
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
			// Small batch size so the tx spans the master plus a pagination record;
			// the master DAH and the pagination-record DAH are stamped by different
			// code paths and must agree.
			tSettings.UtxoStore.UtxoBatchSize = 4

			retention := tSettings.GetUtxoStoreBlockHeightRetention()
			require.Greater(t, retention, uint32(0), "test requires a non-zero block height retention")

			client, store, _, cleanup := initAerospike(t, tSettings, logger)
			defer cleanup()
			cleanDB(t, client)

			// Cached chain tip lags far behind the block being validated — the
			// catchup / sync condition that triggered the bug.
			const cachedTip uint32 = 100
			const minedHeight uint32 = 900_000
			require.NoError(t, store.SetBlockHeight(cachedTip))

			// Create the parent UNMINED with 6 spendable outputs (master holds 4,
			// pagination record 1 holds 2). A non-zero previous txid keeps this from
			// being treated as a coinbase (whose outputs would be subject to maturity
			// and could not be spent here).
			parent := bt.NewTx()
			require.NoError(t, parent.From(
				"1111111111111111111111111111111111111111111111111111111111111111",
				0,
				"76a914000000000000000000000000000000000000000088ac",
				100000,
			))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
			for range 6 {
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
			}
			parentHash := parent.TxIDChainHash()
			_, err := store.Create(ctx, parent, cachedTip)
			require.NoError(t, err)

			// Spend every output while the parent is still UNMINED. The parent becomes
			// fully spent but gets NO deleteAtHeight (the spend path only stamps a DAH
			// on already-mined txs). The DAH is therefore stamped later, by setMined.
			for i, out := range parent.Outputs {
				child := bt.NewTx()
				require.NoError(t, child.From(parentHash.String(), uint32(i), out.LockingScript.String(), out.Satoshis))
				require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))
				_, err = store.Spend(ctx, child, cachedTip)
				require.NoError(t, err, "spending output %d while unmined should succeed", i)
			}

			// Mark the parent mined at a height far above the cached tip — block
			// validation always knows and passes this height in minedBlockInfo.BlockHeight.
			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
				BlockID:        100,
				BlockHeight:    minedHeight,
				SubtreeIdx:     0,
				OnLongestChain: true,
			})
			require.NoError(t, err)

			// Both the master record and every pagination record must carry the same
			// deleteAtHeight, computed from the mined block height — not the lagging
			// cached tip.
			readDAH := func(recordIdx uint32) (interface{}, bool) {
				key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(parentHash, recordIdx))
				require.NoError(t, err)
				rec, err := client.Get(nil, key)
				require.NoError(t, err)
				require.NotNil(t, rec, "record %d must exist", recordIdx)
				v, ok := rec.Bins[fields.DeleteAtHeight.String()]
				return v, ok
			}

			masterDAH, ok := readDAH(0)
			require.True(t, ok, "setMined must stamp a deleteAtHeight on a fully-spent, mined tx")
			require.Equal(t, int(minedHeight+retention), masterDAH,
				"master deleteAtHeight must be minedBlockInfo.BlockHeight + retention, not the lagging cachedTip + 1 + retention")

			childDAH, ok := readDAH(1)
			require.True(t, ok, "pagination record must carry a deleteAtHeight")
			require.Equal(t, int(minedHeight+retention), childDAH,
				"pagination-record deleteAtHeight must match the master (minedBlockInfo.BlockHeight + retention)")
		})
	}
}
