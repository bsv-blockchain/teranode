package aerospike_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestSpendDAHParity covers the spend path (Lua and expressions) for a record that already
// carries a deleteAtHeight when the spend makes it fully spent. Lua's setDeleteAtHeight raises
// but never lowers an existing DAH, and clears a stale DAH when the record is not eligible.
func TestSpendDAHParity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	type spendCase struct {
		name    string
		mined   bool // record already has a block ID and no unminedSince
		dah     *int
		wantDAH func(retention int) *int
	}

	cases := []spendCase{
		{name: "mined, fully spent by this spend, existing higher DAH -> kept", mined: true, dah: intPtr(highDAH),
			wantDAH: func(int) *int { return intPtr(highDAH) }},
		{name: "mined, fully spent by this spend, no DAH -> set", mined: true,
			wantDAH: func(r int) *int { return intPtr(spendHeight + r) }},
		{name: "unmined, fully spent, stale DAH -> cleared", mined: false, dah: intPtr(5),
			wantDAH: func(int) *int { return nil }},
	}

	for _, useExpressions := range []bool{false, true} {
		name := "lua"
		if useExpressions {
			name = "expressions"
		}

		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Aerospike.EnableSpendFilterExpressions = useExpressions
			tSettings.UtxoStore.UtxoBatchSize = 1 // required for the expression spend path

			retention := int(tSettings.GetUtxoStoreBlockHeightRetention())
			require.Greater(t, retention, 0)

			client, store, ctx, cleanup := initAerospike(t, tSettings, logger)
			defer cleanup()
			cleanDB(t, client)

			for i, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					parent := bt.NewTx()
					require.NoError(t, parent.From(
						"1111111111111111111111111111111111111111111111111111111111111111", uint32(i), //nolint:gosec
						"76a914000000000000000000000000000000000000000088ac", 100000))
					parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
					require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
					parentHash := parent.TxIDChainHash()

					_, err := store.Create(ctx, parent, spendHeight)
					require.NoError(t, err)

					key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parentHash[:])
					require.NoError(t, err)

					bins := []*aerospike.Bin{optionalBin(fields.DeleteAtHeight, c.dah)}
					if c.mined {
						bins = append(bins,
							aerospike.NewBin(fields.BlockIDs.String(), []int{7}),
							aerospike.NewBin(fields.BlockHeights.String(), []int{500}),
							aerospike.NewBin(fields.SubtreeIdxs.String(), []int{0}),
							aerospike.NewBin(fields.UnminedSince.String(), nil),
						)
					}

					require.NoError(t, client.PutBins(nil, key, bins...))

					child := bt.NewTx()
					require.NoError(t, child.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
					require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))
					_, err = store.Spend(ctx, child, spendHeight)
					require.NoError(t, err)

					rec, err := client.Get(nil, key)
					require.NoError(t, err)
					require.Equal(t, showInt(c.wantDAH(retention)), showInt(binInt(rec, fields.DeleteAtHeight)))
				})
			}
		})
	}
}

// TestExpiredPreservationDAHParity covers ProcessExpiredPreservations for a record that already
// carries a deleteAtHeight at expiry. The expiry path must behave like Lua setDeleteAtHeight:
// an eligible record keeps a higher existing DAH (never lowered), an ineligible one has a stale
// DAH cleared.
func TestExpiredPreservationDAHParity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = 100
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0

	client, store, ctx, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()

	const currentHeight = uint32(200)

	txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	run := func(t *testing.T, eligible bool, dah int, want *int) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		wp := util.GetAerospikeWritePolicy(tSettings, 0)
		wp.RecordExistsAction = aerospike.UPDATE

		bins := []*aerospike.Bin{
			aerospike.NewBin(fields.PreserveUntil.String(), int(currentHeight-10)),
			aerospike.NewBin(fields.DeleteAtHeight.String(), dah),
		}
		if eligible {
			bins = append(bins,
				aerospike.NewBin(fields.SpentUtxos.String(), 5),
				aerospike.NewBin(fields.RecordUtxos.String(), 5),
				aerospike.NewBin(fields.BlockIDs.String(), []int{7}),
				aerospike.NewBin(fields.UnminedSince.String(), nil),
			)
		}

		require.NoError(t, client.PutBins(wp, txKey, bins...))

		require.Eventually(t, func() bool {
			require.NoError(t, store.ProcessExpiredPreservations(ctx, currentHeight))
			rec, getErr := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
			require.NoError(t, getErr)

			return rec.Bins[fields.PreserveUntil.String()] == nil
		}, 15*time.Second, 250*time.Millisecond, "expired preservation should be processed")

		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, showInt(want), showInt(binInt(rec, fields.DeleteAtHeight)))
	}

	t.Run("eligible, existing higher DAH -> kept", func(t *testing.T) { run(t, true, highDAH, intPtr(highDAH)) })
	t.Run("eligible, existing lower DAH -> bumped", func(t *testing.T) { run(t, true, 5, intPtr(int(currentHeight)+100)) })
	t.Run("ineligible, stale DAH -> cleared", func(t *testing.T) { run(t, false, 5, nil) })
}
