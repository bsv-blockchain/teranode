package aerospike_test

import (
	"context"
	"fmt"
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

func intPtr(v int) *int { return &v }

const spendHeight = 300

func optionalBin(name fields.FieldName, v *int) *aerospike.Bin {
	if v == nil {
		return aerospike.NewBin(name.String(), nil)
	}

	return aerospike.NewBin(name.String(), *v)
}

func showInt(p *int) string {
	if p == nil {
		return "<absent>"
	}

	return fmt.Sprint(*p)
}

func binInt(rec *aerospike.Record, name fields.FieldName) *int {
	v, ok := rec.Bins[name.String()]
	if !ok || v == nil {
		return nil
	}

	i, _ := v.(int)

	return &i
}

// dahValue yields an optional DAH as a function of the retention-dependent value
// minedHeight+retention (nil result = bin absent).
type dahValue func(retentionDAH int) *int

func absent(int) *int      { return nil }
func fresh(r int) *int     { return intPtr(r) }
func fixed(v int) dahValue { return func(int) *int { return intPtr(v) } }

// dahParityCase describes a master record pre-state, the setMined call, and the
// deleteAtHeight the Lua UDF (setDeleteAtHeight) produces on the master and on the
// pagination (child) record. The Lua path is the reference: besides the hand-written
// expectations below, the two paths' results are compared to each other directly.
type dahParityCase struct {
	name string

	// master pre-state
	spentUtxos       int      // of recordUtxos == 4
	spentExtraRecs   int      // of totalExtraRecs == 1
	dah              dahValue // existing master DAH; the child starts with the same value
	preserveUntil    *int
	conflicting      bool
	unminedSince     *int
	noPriorBlocks    bool // blockIDs empty before the op (first mine)
	notExternal      bool // external bin absent: Lua emits no DAHSET/DAHUNSET signal
	noTotalExtraRecs bool // totalExtraRecs bin absent: Lua treats the record as a pagination record

	onLongestChain bool

	wantDAH      dahValue
	wantChildDAH dahValue
}

// highDAH is far above any block height + retention used in these tests, so an existing DAH of
// this value is always >= the freshly computed one.
const highDAH = 1_000_000

func dahParityCases() []dahParityCase {
	return []dahParityCase{
		{name: "all spent, no DAH -> set", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true,
			wantDAH: fresh, wantChildDAH: fresh},
		{name: "all spent, higher DAH -> never lowered", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, dah: fixed(highDAH),
			wantDAH: fixed(highDAH), wantChildDAH: fixed(highDAH)},
		{name: "all spent, lower DAH -> bumped", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, dah: fixed(5),
			wantDAH: fresh, wantChildDAH: fresh},
		{name: "all spent, equal DAH -> unchanged", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, dah: fresh,
			wantDAH: fresh, wantChildDAH: fresh},
		{name: "master not all spent, stale DAH -> cleared", spentUtxos: 3, spentExtraRecs: 1, onLongestChain: true, dah: fixed(5),
			wantDAH: absent, wantChildDAH: absent},
		{name: "extra record not spent, stale DAH -> cleared", spentUtxos: 4, spentExtraRecs: 0, onLongestChain: true, dah: fixed(5),
			wantDAH: absent, wantChildDAH: absent},
		{name: "not all spent, no DAH -> stays absent", spentUtxos: 3, spentExtraRecs: 0, onLongestChain: true,
			wantDAH: absent, wantChildDAH: absent},
		{name: "all spent but not on longest chain, stale DAH -> cleared", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: false, unminedSince: intPtr(900), dah: fixed(5),
			wantDAH: absent, wantChildDAH: absent},
		{name: "all spent but not on longest chain, no DAH -> stays absent", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: false, unminedSince: intPtr(900),
			wantDAH: absent, wantChildDAH: absent},
		// unminedSince is cleared by an earlier op in the same operate; the expression must see
		// the in-flight clear, not the pre-operate bin (only matters when onLongestChain is not
		// a constant in the expression, i.e. a non-mined caller; here it pins the end state).
		{name: "unminedSince set, mined on longest chain, all spent -> set", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, unminedSince: intPtr(900),
			wantDAH: fresh, wantChildDAH: fresh},
		// First mine: blockIDs is empty before the op, so hasBlocks is only true if the
		// expression sees the ListAppend made earlier in the same operate.
		{name: "first mine (empty blockIDs), all spent, no DAH -> set", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, noPriorBlocks: true, unminedSince: intPtr(900),
			wantDAH: fresh, wantChildDAH: fresh},
		{name: "first mine (empty blockIDs), not all spent, stale DAH -> cleared", spentUtxos: 2, spentExtraRecs: 0, onLongestChain: true, noPriorBlocks: true, dah: fixed(5),
			wantDAH: absent, wantChildDAH: absent},
		{name: "first mine off the longest chain, all spent, no DAH -> stays absent", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: false, noPriorBlocks: true, unminedSince: intPtr(900),
			wantDAH: absent, wantChildDAH: absent},
		{name: "preserveUntil, stale DAH -> untouched", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, preserveUntil: intPtr(99999), dah: fixed(5),
			wantDAH: fixed(5), wantChildDAH: fixed(5)},
		{name: "preserveUntil, no DAH -> stays absent", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, preserveUntil: intPtr(99999),
			wantDAH: absent, wantChildDAH: absent},
		{name: "preserveUntil, not all spent, stale DAH -> untouched", spentUtxos: 1, spentExtraRecs: 0, onLongestChain: true, preserveUntil: intPtr(99999), dah: fixed(5),
			wantDAH: fixed(5), wantChildDAH: fixed(5)},
		{name: "conflicting, no DAH -> set even if unspent", spentUtxos: 0, spentExtraRecs: 0, onLongestChain: true, conflicting: true,
			wantDAH: fresh, wantChildDAH: fresh},
		{name: "conflicting, existing lower DAH -> unchanged (no bump)", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, conflicting: true, dah: fixed(5),
			wantDAH: fixed(5), wantChildDAH: fixed(5)},
		{name: "conflicting, existing higher DAH -> unchanged", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, conflicting: true, dah: fixed(highDAH),
			wantDAH: fixed(highDAH), wantChildDAH: fixed(highDAH)},
		{name: "conflicting, not on longest chain, no DAH -> set", spentUtxos: 0, spentExtraRecs: 0, onLongestChain: false, unminedSince: intPtr(900), conflicting: true,
			wantDAH: fresh, wantChildDAH: fresh},
		// conflicting + would-clear: a conflicting record is never cleared, even when not eligible.
		{name: "conflicting, not all spent, existing DAH -> never cleared", spentUtxos: 1, spentExtraRecs: 0, onLongestChain: true, conflicting: true, dah: fixed(5),
			wantDAH: fixed(5), wantChildDAH: fixed(5)},
		{name: "conflicting, off longest chain, existing DAH -> never cleared", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: false, unminedSince: intPtr(900), conflicting: true, dah: fixed(5),
			wantDAH: fixed(5), wantChildDAH: fixed(5)},
		// Non-external master: Lua sets/clears the master DAH but emits no signal, so the child is untouched.
		{name: "non-external, all spent, no DAH -> master set, child untouched", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, notExternal: true,
			wantDAH: fresh, wantChildDAH: absent},
		{name: "non-external, not all spent, stale DAH -> master cleared, child untouched", spentUtxos: 1, spentExtraRecs: 0, onLongestChain: true, notExternal: true, dah: fixed(5),
			wantDAH: absent, wantChildDAH: fixed(5)},
		// A master without totalExtraRecs is handled by Lua as a pagination record: DAH never changes
		// (conflicting is evaluated first, so it still sets there). The all-spent variant is not
		// tested: Lua then signals ALLSPENT and the client's follow-up fails on the missing bin.
		{name: "no totalExtraRecs, not all spent, no DAH -> stays absent", spentUtxos: 1, spentExtraRecs: 0, onLongestChain: true, noTotalExtraRecs: true,
			wantDAH: absent, wantChildDAH: absent},
		{name: "no totalExtraRecs, not all spent, stale DAH -> untouched", spentUtxos: 1, spentExtraRecs: 0, onLongestChain: true, noTotalExtraRecs: true, dah: fixed(5),
			wantDAH: fixed(5), wantChildDAH: fixed(5)},
		{name: "no totalExtraRecs, conflicting, no DAH -> set", spentUtxos: 0, spentExtraRecs: 0, onLongestChain: true, noTotalExtraRecs: true, conflicting: true,
			wantDAH: fresh, wantChildDAH: absent},
	}
}

type dahResult struct{ master, child string }

// TestSetMinedDAHParity runs identical scenarios through the Lua UDF path and the
// filter-expression path and asserts (a) each matches the expected DAH on the master record and
// the follow-up DAH on the pagination record (the Go-side reaction to the Lua DAHSET/DAHUNSET
// signals), and (b) the two paths produce identical results.
func TestSetMinedDAHParity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	for _, retentionDisabled := range []bool{false, true} {
		results := map[bool]map[string]dahResult{}

		for _, useExpressions := range []bool{false, true} {
			name := "lua"
			if useExpressions {
				name = "expressions"
			}

			if retentionDisabled {
				name += "/retention0"
			}

			results[useExpressions] = map[string]dahResult{}

			t.Run(name, func(t *testing.T) {
				logger := ulogger.NewErrorTestLogger(t)
				tSettings := test.CreateBaseTestSettings(t)
				tSettings.Aerospike.EnableSetMinedFilterExpressions = useExpressions
				tSettings.UtxoStore.UtxoBatchSize = 4

				if retentionDisabled {
					tSettings.GlobalBlockHeightRetention = 0
					tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0
				}

				retention := int(tSettings.GetUtxoStoreBlockHeightRetention())
				if retentionDisabled {
					require.Equal(t, 0, retention)
				} else {
					require.Greater(t, retention, 0)
				}

				client, store, ctx, cleanup := initAerospike(t, tSettings, logger)
				defer cleanup()
				cleanDB(t, client)

				const minedHeight = 1000

				require.NoError(t, store.SetBlockHeight(minedHeight-1))

				retentionDAH := minedHeight + retention

				for i, c := range dahParityCases() {
					if retentionDisabled {
						// Lua returns early when retention is 0 (nothing is ever touched, whatever
						// the pre-state), so the expressions path must add no DAH ops at all.
						c.wantDAH = c.dah
						c.wantChildDAH = c.dah
					}

					results[useExpressions][c.name] = runDAHParityCase(ctx, t, client, store, i, c, retentionDAH, minedHeight)
				}
			})
		}

		if !t.Failed() {
			for name, lua := range results[false] {
				require.Equal(t, lua, results[true][name], "lua and expressions must agree (retentionDisabled=%v): %s", retentionDisabled, name)
			}
		}
	}
}

func resolve(d dahValue, retentionDAH int) *int {
	if d == nil {
		return nil
	}

	return d(retentionDAH)
}

func runDAHParityCase(ctx context.Context, t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, idx int, c dahParityCase, retentionDAH int, minedHeight uint32) dahResult {
	t.Helper()

	var result dahResult

	t.Run(c.name, func(t *testing.T) {
		hash := createPaginatedTx(ctx, t, store, idx)
		key := func(recordIdx uint32) *aerospike.Key {
			k, kErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, recordIdx))
			require.NoError(t, kErr)

			return k
		}

		dah := resolve(c.dah, retentionDAH)
		priorBlocks, priorHeights, priorSubtrees := []int{7}, []int{500}, []int{0}

		if c.noPriorBlocks {
			priorBlocks, priorHeights, priorSubtrees = []int{}, []int{}, []int{}
		}

		// Force the master pre-state.
		masterBins := []*aerospike.Bin{
			aerospike.NewBin(fields.External.String(), true),
			aerospike.NewBin(fields.SpentUtxos.String(), c.spentUtxos),
			aerospike.NewBin(fields.SpentExtraRecs.String(), c.spentExtraRecs),
			aerospike.NewBin(fields.Conflicting.String(), c.conflicting),
			aerospike.NewBin(fields.BlockIDs.String(), priorBlocks),
			aerospike.NewBin(fields.BlockHeights.String(), priorHeights),
			aerospike.NewBin(fields.SubtreeIdxs.String(), priorSubtrees),
			optionalBin(fields.DeleteAtHeight, dah),
			optionalBin(fields.PreserveUntil, c.preserveUntil),
			optionalBin(fields.UnminedSince, c.unminedSince),
		}
		if c.notExternal {
			masterBins = append(masterBins, aerospike.NewBin(fields.External.String(), nil))
		}

		if c.noTotalExtraRecs {
			masterBins = append(masterBins, aerospike.NewBin(fields.TotalExtraRecs.String(), nil))
		}

		require.NoError(t, client.PutBins(nil, key(0), dedupeBins(masterBins)...))
		require.NoError(t, client.PutBins(nil, key(1), optionalBin(fields.DeleteAtHeight, dah)))

		_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{hash}, utxo.MinedBlockInfo{
			BlockID:        100,
			BlockHeight:    minedHeight,
			OnLongestChain: c.onLongestChain,
		})
		require.NoError(t, err)

		read := func(recordIdx uint32) *int {
			rec, gErr := client.Get(nil, key(recordIdx))
			require.NoError(t, gErr)
			require.NotNil(t, rec)

			return binInt(rec, fields.DeleteAtHeight)
		}

		result = dahResult{master: showInt(read(0)), child: showInt(read(1))}

		require.Equal(t, showInt(resolve(c.wantDAH, retentionDAH)), result.master, "master deleteAtHeight")
		require.Equal(t, showInt(resolve(c.wantChildDAH, retentionDAH)), result.child, "pagination record deleteAtHeight")
	})

	return result
}

// dedupeBins keeps the last bin per name, so a later "bin = nil" override wins.
func dedupeBins(bins []*aerospike.Bin) []*aerospike.Bin {
	last := map[string]int{}
	for i, b := range bins {
		last[b.Name] = i
	}

	out := make([]*aerospike.Bin, 0, len(last))

	for i, b := range bins {
		if last[b.Name] == i {
			out = append(out, b)
		}
	}

	return out
}

// createPaginatedTx stores a unique tx with 6 outputs; with utxoBatchSize 4 that is a master
// record (4 outputs) plus one pagination record.
func createPaginatedTx(ctx context.Context, t *testing.T, store *teranode_aerospike.Store, idx int) *chainhash.Hash {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From(
		"1111111111111111111111111111111111111111111111111111111111111111",
		uint32(idx), //nolint:gosec
		"76a914000000000000000000000000000000000000000088ac",
		100000,
	))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x6a})

	for range 6 {
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	return tx.TxIDChainHash()
}

// TestSetMinedDAHChildFollowUpsSurviveSiblingError mines a batch where one tx's DAH must be
// set, another's cleared, and a third does not exist. The missing tx makes the call return an
// error, but the child (pagination record) DAH follow-ups for the other two must still run:
// a retry would hit FILTERED_OUT (blockID already present) and never reconcile them, leaving a
// stale DAH on children whose master is no longer prunable.
func TestSetMinedDAHChildFollowUpsSurviveSiblingError(t *testing.T) {
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
			tSettings.Aerospike.EnableSetMinedFilterExpressions = useExpressions
			tSettings.UtxoStore.UtxoBatchSize = 4

			retention := int(tSettings.GetUtxoStoreBlockHeightRetention())

			client, store, ctx, cleanup := initAerospike(t, tSettings, logger)
			defer cleanup()
			cleanDB(t, client)

			const minedHeight = 1000

			setHash := createPaginatedTx(ctx, t, store, 0)
			clearHash := createPaginatedTx(ctx, t, store, 1)
			missing := chainhash.HashH([]byte("missing"))

			force := func(hash *chainhash.Hash, spent, spentExtra int) {
				k0, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 0))
				require.NoError(t, err)
				k1, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 1))
				require.NoError(t, err)
				require.NoError(t, client.PutBins(nil, k0,
					aerospike.NewBin(fields.External.String(), true),
					aerospike.NewBin(fields.SpentUtxos.String(), spent),
					aerospike.NewBin(fields.SpentExtraRecs.String(), spentExtra),
					aerospike.NewBin(fields.BlockIDs.String(), []int{7}),
					aerospike.NewBin(fields.UnminedSince.String(), nil),
					aerospike.NewBin(fields.DeleteAtHeight.String(), 5),
				))
				require.NoError(t, client.PutBins(nil, k1, aerospike.NewBin(fields.DeleteAtHeight.String(), 5)))
			}

			force(setHash, 4, 1)   // eligible, lower DAH -> bumped, child must follow
			force(clearHash, 1, 0) // not eligible -> cleared, child must follow

			_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{setHash, &missing, clearHash}, utxo.MinedBlockInfo{
				BlockID: 100, BlockHeight: minedHeight, OnLongestChain: true,
			})
			require.Error(t, err, "the missing tx must surface as an error")

			childDAH := func(hash *chainhash.Hash) *int {
				k, kErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 1))
				require.NoError(t, kErr)
				rec, gErr := client.Get(nil, k)
				require.NoError(t, gErr)

				return binInt(rec, fields.DeleteAtHeight)
			}

			require.Equal(t, showInt(intPtr(minedHeight+retention)), showInt(childDAH(setHash)), "child DAH of the bumped master")
			require.Equal(t, showInt(nil), showInt(childDAH(clearHash)), "child DAH of the cleared master")
		})
	}
}
