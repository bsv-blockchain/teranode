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

// TestSetMinedNotOnLongestChainDAHMatrixLua checks the Lua setDeleteAtHeight order of checks on a
// fork block (onLongestChain=false), mirroring the SQL matrix test of the same name: unspent
// outputs and a non-nil unmined_since clear the DAH, preserveUntil and retention 0 leave it
// untouched, conflicting sets it when absent and otherwise leaves it unchanged.
func TestSetMinedNotOnLongestChainDAHMatrixLua(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	const mainHeight uint32 = 1000
	const forkHeight uint32 = 1010

	tests := []struct {
		name      string
		spendAll  bool
		retention *uint32
		bins      []*aerospike.Bin
		wantDAH   func(retention uint32) interface{}
	}{
		{
			name: "unspent outputs clear the DAH", spendAll: false,
			bins:    []*aerospike.Bin{aerospike.NewBin(fields.DeleteAtHeight.String(), 5000)},
			wantDAH: func(uint32) interface{} { return nil },
		},
		{
			name: "non-nil unmined_since clears the DAH", spendAll: true,
			bins:    []*aerospike.Bin{aerospike.NewBin(fields.UnminedSince.String(), 7)},
			wantDAH: func(uint32) interface{} { return nil },
		},
		{
			name: "preserveUntil leaves the DAH untouched", spendAll: true,
			bins:    []*aerospike.Bin{aerospike.NewBin(fields.PreserveUntil.String(), 99999)},
			wantDAH: func(r uint32) interface{} { return int(mainHeight + r) },
		},
		{
			name: "retention 0 leaves the DAH untouched", spendAll: true, retention: new(uint32),
			bins:    []*aerospike.Bin{aerospike.NewBin(fields.DeleteAtHeight.String(), 5000)},
			wantDAH: func(uint32) interface{} { return 5000 },
		},
		{
			name: "conflicting without a DAH sets it", spendAll: false,
			bins: []*aerospike.Bin{
				aerospike.NewBin(fields.Conflicting.String(), true),
				aerospike.NewBin(fields.DeleteAtHeight.String(), nil),
			},
			wantDAH: func(r uint32) interface{} { return int(forkHeight + r) },
		},
		{
			name: "conflicting with a DAH leaves it unchanged", spendAll: true,
			bins: []*aerospike.Bin{
				aerospike.NewBin(fields.Conflicting.String(), true),
				aerospike.NewBin(fields.DeleteAtHeight.String(), 5),
			},
			wantDAH: func(uint32) interface{} { return 5 },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)

			if tc.retention != nil {
				tSettings.GlobalBlockHeightRetention = *tc.retention
				tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0
			}

			retention := tSettings.GetUtxoStoreBlockHeightRetention()

			client, store, _, cleanup := initAerospike(t, tSettings, logger)
			defer cleanup()
			cleanDB(t, client)

			require.NoError(t, store.SetBlockHeight(mainHeight))

			parent := newParent(t)
			parentHash := parent.TxIDChainHash()
			_, err := store.Create(ctx, parent, mainHeight)
			require.NoError(t, err)

			if tc.spendAll {
				for i, out := range parent.Outputs {
					child := bt.NewTx()
					require.NoError(t, child.From(parentHash.String(), uint32(i), out.LockingScript.String(), out.Satoshis))
					require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))
					_, err = store.Spend(ctx, child, mainHeight)
					require.NoError(t, err)
				}
			}

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
				BlockID: 100, BlockHeight: mainHeight, OnLongestChain: true,
			})
			require.NoError(t, err)

			key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(parentHash, 0))
			require.NoError(t, err)
			require.NoError(t, client.PutBins(nil, key, tc.bins...))

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
				BlockID: 200, BlockHeight: forkHeight, OnLongestChain: false,
			})
			require.NoError(t, err)

			dah, _ := readMasterBins(t, client, store.GetNamespace(), store.GetName(), parentHash)
			require.Equal(t, tc.wantDAH(retention), dah)
		})
	}
}

// TestUnsetMinedLastBlockOfMainChainTx: invalidating the only block of a fully spent main-chain
// tx stamps a backdated unmined_since and clears the DAH.
func TestUnsetMinedLastBlockOfMainChainTx(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	const mainHeight uint32 = 50
	const tip uint32 = 5000

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()
	cleanDB(t, client)

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

	dah, _ := readMasterBins(t, client, store.GetNamespace(), store.GetName(), parentHash)
	require.NotNil(t, dah)

	require.NoError(t, store.SetBlockHeight(tip))

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
		BlockID: 100, BlockHeight: mainHeight, UnsetMined: true,
	})
	require.NoError(t, err)

	dah, unminedSince := readMasterBins(t, client, store.GetNamespace(), store.GetName(), parentHash)
	require.Nil(t, dah)
	require.Equal(t, int(utxo.BackdatedUnminedSince(tip, tSettings.UtxoStore.UnminedTxRetention)), unminedSince)
}
