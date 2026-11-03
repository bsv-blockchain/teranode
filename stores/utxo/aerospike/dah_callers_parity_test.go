package aerospike_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
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
		name  string
		mined bool // record already has a block ID and no unminedSince
		// noBlockIDs deletes the blockIDs bin outright (Lua: nil, i.e. no blocks, so not eligible)
		noBlockIDs bool
		dah        *int
		wantDAH    func(retention int) *int
	}

	cases := []spendCase{
		{name: "mined, fully spent by this spend, existing higher DAH -> kept", mined: true, dah: dahParityInt(dahParityHighDAH),
			wantDAH: func(int) *int { return dahParityInt(dahParityHighDAH) }},
		{name: "mined, fully spent by this spend, no DAH -> set", mined: true,
			wantDAH: func(r int) *int { return dahParityInt(dahParitySpendHeight + r) }},
		{name: "blockIDs bin missing, fully spent, stale DAH -> cleared", mined: true, noBlockIDs: true, dah: dahParityInt(5),
			wantDAH: func(int) *int { return nil }},
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

					if c.noBlockIDs {
						bins = append(bins, aerospike.NewBin(fields.BlockIDs.String(), nil))
					}

					require.NoError(t, client.PutBins(nil, key, dahParityDedupeBins(bins)...))

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

		// Eventually only absorbs the secondary-index build lag: the child follow-up is
		// synchronous inside ProcessExpiredPreservations, so once preserveUntil is cleared the
		// children have been written. assert, not require: the closure runs on its own goroutine.
		require.Eventually(t, func() bool {
			if !assert.NoError(t, store.ProcessExpiredPreservations(ctx, currentHeight)) {
				return false
			}

			rec, getErr := client.Get(nil, k0)
			if !assert.NoError(t, getErr) {
				return false
			}

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

// expiredFailureHarness wires an external, not-eligible master (stale DAH 5, children at 5) whose
// preservation has expired, plus an injectable child/restore write function.
type expiredFailureHarness struct {
	client *uaerospike.Client
	store  *teranode_aerospike.Store
	ctx    context.Context
	k0, k1 *aerospike.Key
	calls  atomic.Int32
}

const expiredCurrentHeight = uint32(200)

func newExpiredFailureHarness(t *testing.T) *expiredFailureHarness {
	t.Helper()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = 100
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, cleanup := initAerospike(t, tSettings, logger)
	t.Cleanup(cleanup)

	return &expiredFailureHarness{client: client, store: store, ctx: ctx}
}

// setup creates the master + one child. eligible makes the master fully spent (so the expiry
// RAISES a lower DAH), otherwise it is not eligible (the expiry CLEARS the stale DAH).
func (h *expiredFailureHarness) setup(t *testing.T, idx int, eligible bool) {
	t.Helper()
	cleanDB(t, h.client)

	hash := dahParityCreatePaginatedTx(h.ctx, t, h.store, idx)

	var err error
	h.k0, err = aerospike.NewKey(h.store.GetNamespace(), h.store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 0))
	require.NoError(t, err)
	h.k1, err = aerospike.NewKey(h.store.GetNamespace(), h.store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 1))
	require.NoError(t, err)

	spent, spentExtra := 1, 0
	if eligible {
		spent, spentExtra = 4, 1
	}

	require.NoError(t, h.client.PutBins(nil, h.k0,
		aerospike.NewBin(fields.External.String(), true),
		aerospike.NewBin(fields.PreserveUntil.String(), int(expiredCurrentHeight-10)),
		aerospike.NewBin(fields.SpentUtxos.String(), spent),
		aerospike.NewBin(fields.SpentExtraRecs.String(), spentExtra),
		aerospike.NewBin(fields.BlockIDs.String(), []int{7}),
		aerospike.NewBin(fields.UnminedSince.String(), nil),
		aerospike.NewBin(fields.DeleteAtHeight.String(), 5),
	))
	require.NoError(t, h.client.PutBins(nil, h.k1, aerospike.NewBin(fields.DeleteAtHeight.String(), 5)))
}

// failFirst makes the first n batch writes the store issues through its injectable seam fail
// (child follow-up writes, and the master restore which is routed through the same seam); later
// writes go through. onCall, if set, runs before each call with its 1-based number.
func (h *expiredFailureHarness) failFirst(t *testing.T, n int, onCall func(call int)) {
	t.Helper()

	h.calls.Store(0)
	h.store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		// The expiry write on the master also goes through the seam; it is not one of the writes
		// these tests fail or count, so let it through.
		if h.isMasterExpiryWrite(records) {
			return h.client.BatchOperate(policy, records)
		}

		call := int(h.calls.Add(1))
		if onCall != nil {
			onCall(call)
		}

		if n < 0 || call <= n {
			return aerospike.ErrNetwork
		}

		return h.client.BatchOperate(policy, records)
	})
	t.Cleanup(func() { h.store.SetBatchOperateFn(nil) })
}

// isMasterExpiryWrite is true for the single-record write on the master while its preservation
// is still the seeded one, i.e. the expiry write itself rather than a child write or a restore.
func (h *expiredFailureHarness) isMasterExpiryWrite(records []aerospike.BatchRecordIfc) bool {
	if len(records) != 1 || records[0].BatchRec().Key.String() != h.k0.String() {
		return false
	}

	rec, err := h.client.Get(nil, h.k0)
	if err != nil {
		return false
	}

	return rec.Bins[fields.PreserveUntil.String()] == int(expiredCurrentHeight-10)
}

// run calls ProcessExpiredPreservations until the expired record has been picked up (the
// secondary index builds in the background) and returns the last error.
func (h *expiredFailureHarness) run(t *testing.T) error {
	t.Helper()

	var lastErr error

	require.Eventually(t, func() bool {
		lastErr = h.store.ProcessExpiredPreservations(h.ctx, expiredCurrentHeight)
		rec, getErr := h.client.Get(nil, h.k0)
		if !assert.NoError(t, getErr) {
			return false
		}

		// picked up = preserveUntil changed from the seeded value, or the call reported failure
		return rec.Bins[fields.PreserveUntil.String()] != int(expiredCurrentHeight-10) || lastErr != nil
	}, 15*time.Second, 250*time.Millisecond)

	return lastErr
}

func (h *expiredFailureHarness) bins(t *testing.T) (master, child aerospike.BinMap) {
	t.Helper()

	rec0, err := h.client.Get(nil, h.k0)
	require.NoError(t, err)
	rec1, err := h.client.Get(nil, h.k1)
	require.NoError(t, err)

	return rec0.Bins, rec1.Bins
}

// TestExpiredPreservationChildUnsetFailure injects failing child-record writes into the expiry
// path for an external master whose DAH gets cleared (no longer eligible, children carry a stale
// DAH). preserveUntil is cleared by the same write, so the record would never come back from the
// index query and the children would keep a stale DAH under a master without one, which the
// pruner can delete. The path must retry the unset once, and if it still fails restore the
// master (preserveUntil and DAH) so the next run redoes the whole transition.
func TestExpiredPreservationChildUnsetFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	h := newExpiredFailureHarness(t)

	t.Run("one transient failure is retried", func(t *testing.T) {
		h.setup(t, 0, false)
		h.failFirst(t, 1, nil)

		require.NoError(t, h.run(t))
		require.GreaterOrEqual(t, int(h.calls.Load()), 2, "the unset must be retried once")

		master, child := h.bins(t)
		require.Nil(t, master[fields.DeleteAtHeight.String()])
		require.Nil(t, child[fields.DeleteAtHeight.String()], "child DAH must be cleared by the retry")
	})

	t.Run("persistent failure restores the master so the next run redoes the transition", func(t *testing.T) {
		h.setup(t, 1, false)
		h.failFirst(t, 2, nil) // unset + its retry fail; the restore write goes through

		err := h.run(t)
		require.Error(t, err, "a failed child unset must be reported")

		master, _ := h.bins(t)
		require.NotNil(t, master[fields.PreserveUntil.String()], "preserveUntil must be restored so the record stays queued")
		require.Equal(t, 5, master[fields.DeleteAtHeight.String()], "master DAH must be restored to the pre-state")

		// next run, writes healthy again: the whole transition is redone
		h.store.SetBatchOperateFn(nil)
		require.NoError(t, h.run(t))

		master, child := h.bins(t)
		require.Nil(t, master[fields.PreserveUntil.String()])
		require.Nil(t, master[fields.DeleteAtHeight.String()])
		require.Nil(t, child[fields.DeleteAtHeight.String()], "child DAH must follow the cleared master")
	})

	t.Run("a failed restore is retried once", func(t *testing.T) {
		h.setup(t, 2, false)
		h.failFirst(t, 3, nil) // unset, unset retry, first restore attempt fail; restore retry succeeds

		require.Error(t, h.run(t))

		master, _ := h.bins(t)
		require.NotNil(t, master[fields.PreserveUntil.String()], "the restore retry must have restored the master")
		require.Equal(t, 5, master[fields.DeleteAtHeight.String()])
	})

	t.Run("a restore that keeps failing is reported as a restore failure and counted", func(t *testing.T) {
		h.setup(t, 3, false)
		h.failFirst(t, -1, nil)

		before := restoreFailedMetric(t)
		err := h.run(t)
		require.Error(t, err)
		require.Contains(t, err.Error(), "restore failed", "a distinct error must tell the operator the master could not be restored")
		require.Greater(t, restoreFailedMetric(t), before, "restore failures must be counted")
	})
}

// restoreFailedMetric reads the restore-failure counter from the default registry.
func restoreFailedMetric(t *testing.T) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() == "teranode_aerospike_expired_preservation_restore_failed" {
			var total float64
			for _, m := range f.GetMetric() {
				total += m.GetCounter().GetValue()
			}

			return total
		}
	}

	return 0
}

// TestExpiredPreservationRestoreLeavesConcurrentWriter: while the child follow-up is failing,
// another writer changes the master. The restore must only apply to the known post-write state
// (preserveUntil absent and DAH as the expiry left it); otherwise someone else owns the record.
func TestExpiredPreservationRestoreLeavesConcurrentWriter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	h := newExpiredFailureHarness(t)

	t.Run("newer preserveUntil survives", func(t *testing.T) {
		h.setup(t, 0, false)
		h.failFirst(t, 2, func(call int) {
			if call == 1 {
				require.NoError(t, h.client.PutBins(nil, h.k0, aerospike.NewBin(fields.PreserveUntil.String(), 9999)))
			}
		})

		_ = h.run(t)

		master, _ := h.bins(t)
		require.Equal(t, 9999, master[fields.PreserveUntil.String()], "a concurrent newer preserveUntil must not be overwritten by the stale one")
	})

	t.Run("concurrent DAH stamp survives", func(t *testing.T) {
		h.setup(t, 1, false)
		h.failFirst(t, 2, func(call int) {
			if call == 1 {
				require.NoError(t, h.client.PutBins(nil, h.k0, aerospike.NewBin(fields.DeleteAtHeight.String(), 777)))
			}
		})

		_ = h.run(t)

		master, _ := h.bins(t)
		require.Equal(t, 777, master[fields.DeleteAtHeight.String()], "a concurrent DAH stamp must not be reset by the restore")
		require.Nil(t, master[fields.PreserveUntil.String()], "the restore must not re-preserve a record another writer owns")
	})
}

// TestExpiredPreservationChildRaiseFailure: a RAISE of the master DAH (X -> Y) whose child write
// fails leaves the children at the lower X, so they could be pruned before the master. The
// expiry path must treat it like a failed unset: retry, then restore the master.
func TestExpiredPreservationChildRaiseFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	h := newExpiredFailureHarness(t)
	h.setup(t, 0, true) // eligible, master DAH 5 -> raised to currentHeight + retention
	h.failFirst(t, 2, nil)

	require.Error(t, h.run(t), "a failed child raise must be reported")

	master, child := h.bins(t)
	require.NotNil(t, master[fields.PreserveUntil.String()], "the master must be restored so the raise is redone")
	require.Equal(t, 5, master[fields.DeleteAtHeight.String()])
	require.Equal(t, 5, child[fields.DeleteAtHeight.String()])

	h.store.SetBatchOperateFn(nil)
	require.NoError(t, h.run(t))

	master, child = h.bins(t)
	require.Equal(t, int(expiredCurrentHeight)+100, master[fields.DeleteAtHeight.String()])
	require.Equal(t, int(expiredCurrentHeight)+100, child[fields.DeleteAtHeight.String()], "children must follow the raised master")
}

// TestPreserveChildDAH covers the pagination records of a preserved external master, on the Lua
// and the expressions preserve path. The pruner selects pagination records by their own
// deleteAtHeight, so a page that keeps its DAH under a preserved master is deleted while the
// master is protected. Preserving must clear the pages' DAH, and re-preserving (the pruner re-runs
// it every cycle) must heal pages that still carry one.
func TestPreserveChildDAH(t *testing.T) {
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
			tSettings.Aerospike.EnablePreserveFilterExpressions = useExpressions
			tSettings.UtxoStore.UtxoBatchSize = 4

			client, store, ctx, cleanup := initAerospike(t, tSettings, logger)
			defer cleanup()

			setup := func(t *testing.T, idx int, external bool, masterDAH, pageDAH int) (k0, k1 *aerospike.Key, hash *chainhash.Hash) {
				cleanDB(t, client)

				hash = dahParityCreatePaginatedTx(ctx, t, store, idx)

				var err error

				k0, err = aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 0))
				require.NoError(t, err)
				k1, err = aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 1))
				require.NoError(t, err)

				require.NoError(t, client.PutBins(nil, k0,
					aerospike.NewBin(fields.External.String(), external),
					aerospike.NewBin(fields.DeleteAtHeight.String(), masterDAH),
				))
				require.NoError(t, client.PutBins(nil, k1, aerospike.NewBin(fields.DeleteAtHeight.String(), pageDAH)))

				return k0, k1, hash
			}

			t.Run("preserving an external master clears its pages' DAH", func(t *testing.T) {
				k0, k1, hash := setup(t, 0, true, 300, 300)

				require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*hash}, 999))

				rec0, err := client.Get(nil, k0)
				require.NoError(t, err)
				require.Equal(t, 999, rec0.Bins[fields.PreserveUntil.String()])
				require.Nil(t, rec0.Bins[fields.DeleteAtHeight.String()], "master DAH")

				rec1, err := client.Get(nil, k1)
				require.NoError(t, err)
				require.Nil(t, rec1.Bins[fields.DeleteAtHeight.String()], "the pruner selects pages by their own DAH, a preserved master's pages must not carry one")
			})

			t.Run("renewal heals pages that still carry a DAH under a preserved master", func(t *testing.T) {
				k0, k1, hash := setup(t, 1, true, 300, 300)

				require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*hash}, 999))
				// a page stamped after the master was preserved, or left by a failed first write
				require.NoError(t, client.PutBins(nil, k1, aerospike.NewBin(fields.DeleteAtHeight.String(), 300)))
				require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*hash}, 1000))

				rec0, err := client.Get(nil, k0)
				require.NoError(t, err)
				require.Equal(t, 1000, rec0.Bins[fields.PreserveUntil.String()])

				rec1, err := client.Get(nil, k1)
				require.NoError(t, err)
				require.Nil(t, rec1.Bins[fields.DeleteAtHeight.String()])
			})

			t.Run("a non-external master has no page follow-up", func(t *testing.T) {
				_, k1, hash := setup(t, 2, false, 300, 300)

				require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*hash}, 999))

				rec1, err := client.Get(nil, k1)
				require.NoError(t, err)
				require.NotNil(t, rec1.Bins[fields.DeleteAtHeight.String()], "only external masters are followed up, matching setMined and spend")
			})

			// The pruner skips its deletion phase only when preservation returns an error, and it
			// selects pages by their own DAH. A page unset that fails must therefore be returned, or
			// the same cycle deletes the pages of a master that was just preserved.
			t.Run("a failed page unset is returned", func(t *testing.T) {
				_, k1, hash := setup(t, 4, true, 300, 300)

				store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
					for _, r := range records {
						if _, isWrite := r.(*aerospike.BatchWrite); isWrite && r.BatchRec().Key.String() == k1.String() {
							return aerospike.ErrNetwork
						}
					}

					return client.BatchOperate(policy, records)
				})
				defer store.SetBatchOperateFn(nil)

				require.Error(t, store.PreserveTransactions(ctx, []chainhash.Hash{*hash}, 999))

				rec1, err := client.Get(nil, k1)
				require.NoError(t, err)
				require.NotNil(t, rec1.Bins[fields.DeleteAtHeight.String()], "the injected failure must have left the page DAH in place")
			})

			t.Run("a missing page is skipped and not recreated", func(t *testing.T) {
				_, k1, hash := setup(t, 3, true, 300, 300)
				existed, err := client.Delete(nil, k1)
				require.NoError(t, err)
				require.True(t, existed)

				require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*hash}, 999))

				exists, err := client.Exists(nil, k1)
				require.NoError(t, err)
				require.False(t, exists, "a child DAH write must never create a record")
			})
		})
	}
}

// TestExpiredPreservationDoesNotRecreateDeletedPages pins the second half of the gap: a page that
// was deleted while its master was preserved must not come back as a txid-less stub holding only a
// deleteAtHeight when the preservation expires, because the pruner skips records without a txid
// and would never remove it.
func TestExpiredPreservationDoesNotRecreateDeletedPages(t *testing.T) {
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

	cleanDB(t, client)

	hash := dahParityCreatePaginatedTx(ctx, t, store, 0)
	k0, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 0))
	require.NoError(t, err)
	k1, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 1))
	require.NoError(t, err)

	// an eligible external master whose preservation has expired, and whose page is gone
	require.NoError(t, client.PutBins(nil, k0,
		aerospike.NewBin(fields.External.String(), true),
		aerospike.NewBin(fields.PreserveUntil.String(), int(currentHeight-10)),
		aerospike.NewBin(fields.SpentUtxos.String(), 4),
		aerospike.NewBin(fields.SpentExtraRecs.String(), 1),
		aerospike.NewBin(fields.BlockIDs.String(), []int{7}),
		aerospike.NewBin(fields.UnminedSince.String(), nil),
	))

	existed, err := client.Delete(nil, k1)
	require.NoError(t, err)
	require.True(t, existed)

	require.Eventually(t, func() bool {
		if !assert.NoError(t, store.ProcessExpiredPreservations(ctx, currentHeight)) {
			return false
		}

		rec, getErr := client.Get(nil, k0)
		if !assert.NoError(t, getErr) {
			return false
		}

		return rec.Bins[fields.PreserveUntil.String()] == nil
	}, 15*time.Second, 250*time.Millisecond)

	rec0, err := client.Get(nil, k0)
	require.NoError(t, err)
	require.NotNil(t, rec0.Bins[fields.DeleteAtHeight.String()], "the master was stamped, so the set follow-up ran")

	exists, err := client.Exists(nil, k1)
	require.NoError(t, err)
	require.False(t, exists, "the set follow-up recreated a deleted page as a stub the pruner never removes")
}
