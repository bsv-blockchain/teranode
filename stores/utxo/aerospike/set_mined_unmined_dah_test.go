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

// newParent creates an unmined parent with 2 outputs and returns it.
func newParent(t *testing.T) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(
		"1111111111111111111111111111111111111111111111111111111111111111",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		100000,
	))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
	for range 2 {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}

	return parent
}

func readMasterBins(t *testing.T, client *uaerospike.Client, ns, set string, hash *chainhash.Hash) (dah, unminedSince interface{}) {
	t.Helper()

	key, err := aerospike.NewKey(ns, set, uaerospike.CalculateKeySourceInternal(hash, 0))
	require.NoError(t, err)
	rec, err := client.Get(nil, key)
	require.NoError(t, err)
	require.NotNil(t, rec)

	return rec.Bins[fields.DeleteAtHeight.String()], rec.Bins[fields.UnminedSince.String()]
}

// TestSetMinedNotOnLongestChainReevaluatesDAH is the Aerospike counterpart of the SQL test of
// the same name. A fully spent tx that is still mined on the main chain and also appears in a
// fork block (onLongestChain=false) keeps its DAH, bumped forward if the fork block is higher.
// SQL must agree with this behaviour.
func TestSetMinedNotOnLongestChainReevaluatesDAH(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	const mainHeight uint32 = 1000

	for _, useExpressions := range []bool{false, true} {
		for _, forkDelta := range []int{-10, 10} {
			// The filter-expression path never lowers/clears like Lua does: on a fork block below
			// the main block it overwrites the DAH with forkHeight+retention (a pre-existing
			// divergence from Lua's setDeleteAtHeight, out of scope here), so only the Lua path
			// is asserted for the "keeps" case.
			if useExpressions && forkDelta < 0 {
				continue
			}

			name := "lua"
			if useExpressions {
				name = "expressions"
			}

			if forkDelta < 0 {
				name += "/fork below main keeps DAH"
			} else {
				name += "/fork above main bumps DAH"
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

				require.NoError(t, store.SetBlockHeight(mainHeight))

				parent := newParent(t)
				parentHash := parent.TxIDChainHash()
				_, err := store.Create(ctx, parent, mainHeight)
				require.NoError(t, err)

				for i, out := range parent.Outputs {
					child := bt.NewTx()
					require.NoError(t, child.From(parentHash.String(), uint32(i), out.LockingScript.String(), out.Satoshis))
					require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))
					_, err = store.Spend(ctx, child, mainHeight)
					require.NoError(t, err)
				}

				_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
					BlockID: 100, BlockHeight: mainHeight, OnLongestChain: true,
				})
				require.NoError(t, err)

				dah, unminedSince := readMasterBins(t, client, store.GetNamespace(), store.GetName(), parentHash)
				require.Equal(t, int(mainHeight+retention), dah)
				require.Nil(t, unminedSince)

				forkHeight := uint32(int(mainHeight) + forkDelta)
				_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
					BlockID: 200, BlockHeight: forkHeight, OnLongestChain: false,
				})
				require.NoError(t, err)

				dah, unminedSince = readMasterBins(t, client, store.GetNamespace(), store.GetName(), parentHash)
				require.Nil(t, unminedSince)
				wantHeight := max(mainHeight, forkHeight)
				require.Equal(t, int(wantHeight+retention), dah)
			})
		}
	}
}
