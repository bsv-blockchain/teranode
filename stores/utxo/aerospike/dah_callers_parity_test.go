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
	"github.com/bsv-blockchain/teranode/util/uaerospike"
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
		{name: "mined, fully spent by this spend, existing higher DAH -> kept", mined: true, dah: dahParityInt(dahParityHighDAH),
			wantDAH: func(int) *int { return dahParityInt(dahParityHighDAH) }},
		{name: "mined, fully spent by this spend, no DAH -> set", mined: true,
			wantDAH: func(r int) *int { return dahParityInt(dahParitySpendHeight + r) }},
		{name: "unmined, fully spent, stale DAH -> cleared", mined: false, dah: dahParityInt(5),
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

					_, err := store.Create(ctx, parent, dahParitySpendHeight)
					require.NoError(t, err)

					key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parentHash[:])
					require.NoError(t, err)

					bins := []*aerospike.Bin{dahParityOptionalBin(fields.DeleteAtHeight, c.dah)}
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
					_, err = store.Spend(ctx, child, dahParitySpendHeight)
					require.NoError(t, err)

					rec, err := client.Get(nil, key)
					require.NoError(t, err)
					require.Equal(t, dahParityShow(c.wantDAH(retention)), dahParityShow(dahParityBinInt(rec, fields.DeleteAtHeight)))
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

	run := func(t *testing.T, eligible bool, dah *int, want *int) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		wp := util.GetAerospikeWritePolicy(tSettings, 0)
		wp.RecordExistsAction = aerospike.UPDATE

		bins := []*aerospike.Bin{
			aerospike.NewBin(fields.PreserveUntil.String(), int(currentHeight-10)),
			dahParityOptionalBin(fields.DeleteAtHeight, dah),
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
		require.Equal(t, dahParityShow(want), dahParityShow(dahParityBinInt(rec, fields.DeleteAtHeight)))
	}

	// Realistic: ProcessExpiredPreservations runs on records PreserveTransactions stamped, which
	// clears the DAH, so the DAH is absent at expiry.
	t.Run("realistic: DAH absent, eligible -> set", func(t *testing.T) { run(t, true, nil, dahParityInt(int(currentHeight)+100)) })
	t.Run("realistic: DAH absent, ineligible -> stays absent", func(t *testing.T) { run(t, false, nil, nil) })
	// Synthetic: a DAH present at expiry cannot normally happen (preserve clears it); these pin
	// that the expiry path still follows Lua setDeleteAtHeight if it ever does.
	t.Run("synthetic: eligible, existing higher DAH -> kept", func(t *testing.T) { run(t, true, dahParityInt(dahParityHighDAH), dahParityInt(dahParityHighDAH)) })
	t.Run("synthetic: eligible, existing lower DAH -> bumped", func(t *testing.T) { run(t, true, dahParityInt(5), dahParityInt(int(currentHeight)+100)) })
	t.Run("synthetic: ineligible, stale DAH -> cleared", func(t *testing.T) { run(t, false, dahParityInt(5), nil) })
}

// TestExpiredPreservationChildDAH covers the pagination-record follow-up of the expiry path for
// an external master: a stamped master stamps its child, a cleared master clears its child.
func TestExpiredPreservationChildDAH(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = 100
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()

	const currentHeight = uint32(200)

	run := func(t *testing.T, idx int, eligible bool, dah *int, wantMaster, wantChild *int) {
		cleanDB(t, client)

		hash := dahParityCreatePaginatedTx(ctx, t, store, idx)
		k0, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 0))
		require.NoError(t, err)
		k1, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 1))
		require.NoError(t, err)

		spent, spentExtra := 1, 0
		if eligible {
			spent, spentExtra = 4, 1
		}

		require.NoError(t, client.PutBins(nil, k0,
			aerospike.NewBin(fields.External.String(), true),
			aerospike.NewBin(fields.PreserveUntil.String(), int(currentHeight-10)),
			aerospike.NewBin(fields.SpentUtxos.String(), spent),
			aerospike.NewBin(fields.SpentExtraRecs.String(), spentExtra),
			aerospike.NewBin(fields.BlockIDs.String(), []int{7}),
			aerospike.NewBin(fields.UnminedSince.String(), nil),
			dahParityOptionalBin(fields.DeleteAtHeight, dah),
		))
		require.NoError(t, client.PutBins(nil, k1, dahParityOptionalBin(fields.DeleteAtHeight, dah)))

		require.Eventually(t, func() bool {
			require.NoError(t, store.ProcessExpiredPreservations(ctx, currentHeight))
			rec, getErr := client.Get(nil, k0)
			require.NoError(t, getErr)

			return rec.Bins[fields.PreserveUntil.String()] == nil
		}, 15*time.Second, 250*time.Millisecond)

		rec0, err := client.Get(nil, k0)
		require.NoError(t, err)
		rec1, err := client.Get(nil, k1)
		require.NoError(t, err)
		require.Equal(t, dahParityShow(wantMaster), dahParityShow(dahParityBinInt(rec0, fields.DeleteAtHeight)), "master DAH")
		require.Equal(t, dahParityShow(wantChild), dahParityShow(dahParityBinInt(rec1, fields.DeleteAtHeight)), "child DAH")
	}

	stamped := dahParityInt(int(currentHeight) + 100)

	t.Run("realistic: eligible external master stamped -> child stamped", func(t *testing.T) { run(t, 0, true, nil, stamped, stamped) })
	t.Run("synthetic: ineligible external master with stale DAH cleared -> child cleared", func(t *testing.T) {
		run(t, 1, false, dahParityInt(5), nil, nil)
	})
}

// TestSpendChildDAH covers the pagination-record follow-up of the spend path (Lua and
// expressions) for an external master: a master that gets a DAH sets its child, a master whose
// stale DAH is cleared clears its child.
func TestSpendChildDAH(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
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
			tSettings.UtxoStore.UtxoBatchSize = 1 // master holds output 0, child record holds output 1

			retention := int(tSettings.GetUtxoStoreBlockHeightRetention())

			client, store, ctx, cleanup := initAerospike(t, tSettings, logger)
			defer cleanup()

			run := func(t *testing.T, idx int, mined bool, dah *int, wantMaster, wantChild *int) {
				cleanDB(t, client)

				parent := bt.NewTx()
				require.NoError(t, parent.From(
					"1111111111111111111111111111111111111111111111111111111111111111", uint32(idx), //nolint:gosec
					"76a914000000000000000000000000000000000000000088ac", 100000))
				parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
				parentHash := parent.TxIDChainHash()

				_, err := store.Create(ctx, parent, dahParitySpendHeight)
				require.NoError(t, err)

				k0, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(parentHash, 0))
				require.NoError(t, err)
				k1, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(parentHash, 1))
				require.NoError(t, err)

				masterBins := []*aerospike.Bin{
					aerospike.NewBin(fields.External.String(), true),
					aerospike.NewBin(fields.SpentExtraRecs.String(), 1), // the child record is already fully spent
					dahParityOptionalBin(fields.DeleteAtHeight, dah),
				}
				if mined {
					masterBins = append(masterBins,
						aerospike.NewBin(fields.BlockIDs.String(), []int{7}),
						aerospike.NewBin(fields.UnminedSince.String(), nil))
				}

				require.NoError(t, client.PutBins(nil, k0, masterBins...))
				require.NoError(t, client.PutBins(nil, k1, dahParityOptionalBin(fields.DeleteAtHeight, dah)))

				child := bt.NewTx()
				require.NoError(t, child.From(parentHash.String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
				require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))
				_, err = store.Spend(ctx, child, dahParitySpendHeight)
				require.NoError(t, err)

				rec0, err := client.Get(nil, k0)
				require.NoError(t, err)
				require.Equal(t, dahParityShow(wantMaster), dahParityShow(dahParityBinInt(rec0, fields.DeleteAtHeight)), "master DAH")

				// the follow-up runs asynchronously after the spend completes
				require.Eventually(t, func() bool {
					rec1, gErr := client.Get(nil, k1)
					require.NoError(t, gErr)

					return dahParityShow(dahParityBinInt(rec1, fields.DeleteAtHeight)) == dahParityShow(wantChild)
				}, 5*time.Second, 50*time.Millisecond, "child DAH should become %s", dahParityShow(wantChild))
			}

			t.Run("mined, fully spent -> master set, child set", func(t *testing.T) {
				run(t, 0, true, nil, dahParityInt(dahParitySpendHeight+retention), dahParityInt(dahParitySpendHeight+retention))
			})
			t.Run("unmined, fully spent, stale DAH -> master cleared, child cleared", func(t *testing.T) {
				run(t, 1, false, dahParityInt(5), nil, nil)
			})
		})
	}
}
