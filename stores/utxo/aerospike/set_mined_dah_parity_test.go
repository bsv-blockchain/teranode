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

// dahParityCase describes a master record pre-state, the setMined call, and the
// deleteAtHeight the Lua UDF (setDeleteAtHeight) produces on the master and on the
// pagination (child) record. The Lua path is the reference; the filter-expression
// path must produce identical results. A nil DAH means "bin absent".
type dahParityCase struct {
	name string

	// master pre-state
	spentUtxos     int  // of recordUtxos == 4
	spentExtraRecs int  // of totalExtraRecs == 1
	dah            *int // existing master DAH (child starts with the same value)
	preserveUntil  *int
	conflicting    bool
	unminedSince   *int

	onLongestChain bool

	// expectations; retention-dependent values are given as offsets from minedHeight+retention
	wantDAH      func(retentionDAH int) *int
	wantChildDAH func(retentionDAH int) *int
}

func ip(v int) *int { return &v }

// TestSetMinedDAHParity runs identical scenarios through the Lua UDF path and the
// filter-expression path and asserts both leave the same DAH on the master record and
// the same follow-up DAH on the pagination record (the Go-side reaction to the Lua
// DAHSET/DAHUNSET signals).
func TestSetMinedDAHParity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	const minedHeight = 1000

	same := func(v *int) func(int) *int { return func(int) *int { return v } }
	fresh := func(r int) *int { return ip(r) }

	cases := []dahParityCase{
		{name: "all spent, no DAH -> set", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true,
			wantDAH: fresh, wantChildDAH: fresh},
		{name: "all spent, higher DAH -> never lowered", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, dah: ip(1010 + 10_000),
			wantDAH: same(ip(1010 + 10_000)), wantChildDAH: same(ip(1010 + 10_000))},
		{name: "all spent, lower DAH -> bumped", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, dah: ip(5),
			wantDAH: fresh, wantChildDAH: fresh},
		{name: "all spent, equal DAH -> unchanged", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, dah: ip(1000 + 1),
			wantDAH: same(nil), wantChildDAH: same(nil)}, // replaced below once retention is known
		{name: "master not all spent, stale DAH -> cleared", spentUtxos: 3, spentExtraRecs: 1, onLongestChain: true, dah: ip(5),
			wantDAH: same(nil), wantChildDAH: same(nil)},
		{name: "extra record not spent, stale DAH -> cleared", spentUtxos: 4, spentExtraRecs: 0, onLongestChain: true, dah: ip(5),
			wantDAH: same(nil), wantChildDAH: same(nil)},
		{name: "not all spent, no DAH -> stays absent", spentUtxos: 3, spentExtraRecs: 0, onLongestChain: true,
			wantDAH: same(nil), wantChildDAH: same(nil)},
		{name: "all spent but not on longest chain, stale DAH -> cleared", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: false, unminedSince: ip(900), dah: ip(5),
			wantDAH: same(nil), wantChildDAH: same(nil)},
		{name: "all spent but not on longest chain, no DAH -> stays absent", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: false, unminedSince: ip(900),
			wantDAH: same(nil), wantChildDAH: same(nil)},
		{name: "unminedSince set, mined on longest chain, all spent -> set", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, unminedSince: ip(900),
			wantDAH: fresh, wantChildDAH: fresh},
		{name: "preserveUntil, stale DAH -> untouched", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, preserveUntil: ip(99999), dah: ip(5),
			wantDAH: same(ip(5)), wantChildDAH: same(ip(5))},
		{name: "preserveUntil, no DAH -> stays absent", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, preserveUntil: ip(99999),
			wantDAH: same(nil), wantChildDAH: same(nil)},
		{name: "preserveUntil, not all spent, stale DAH -> untouched", spentUtxos: 1, spentExtraRecs: 0, onLongestChain: true, preserveUntil: ip(99999), dah: ip(5),
			wantDAH: same(ip(5)), wantChildDAH: same(ip(5))},
		{name: "conflicting, no DAH -> set even if unspent", spentUtxos: 0, spentExtraRecs: 0, onLongestChain: true, conflicting: true,
			wantDAH: fresh, wantChildDAH: fresh},
		{name: "conflicting, existing lower DAH -> unchanged (no bump)", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, conflicting: true, dah: ip(5),
			wantDAH: same(ip(5)), wantChildDAH: same(ip(5))},
		{name: "conflicting, existing higher DAH -> unchanged", spentUtxos: 4, spentExtraRecs: 1, onLongestChain: true, conflicting: true, dah: ip(1010 + 10_000),
			wantDAH: same(ip(1010 + 10_000)), wantChildDAH: same(ip(1010 + 10_000))},
		{name: "conflicting, not on longest chain, no DAH -> set", spentUtxos: 0, spentExtraRecs: 0, onLongestChain: false, unminedSince: ip(900), conflicting: true,
			wantDAH: fresh, wantChildDAH: fresh},
	}

	for _, retentionDisabled := range []bool{false, true} {
		for _, useExpressions := range []bool{false, true} {
			name := "lua"
			if useExpressions {
				name = "expressions"
			}

			if retentionDisabled {
				name += "/retention0"
			}

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

				require.NoError(t, store.SetBlockHeight(minedHeight-1))

				retentionDAH := minedHeight + retention

				if retentionDisabled {
					// Lua returns early: nothing is ever touched, whatever the pre-state.
					for i, c := range cases {
						c.wantDAH = same(c.dah)
						c.wantChildDAH = same(c.dah)
						runDAHParityCase(ctx, t, client, store, i, c, retentionDAH, minedHeight)
					}

					return
				}

				for i, c := range cases {
					if i == 3 {
						c.dah = ip(retentionDAH)
						c.wantDAH = same(ip(retentionDAH))
						c.wantChildDAH = same(ip(retentionDAH))
					}

					runDAHParityCase(ctx, t, client, store, i, c, retentionDAH, minedHeight)
				}
			})
		}
	}
}

func runDAHParityCase(ctx context.Context, t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, idx int, c dahParityCase, retentionDAH int, minedHeight uint32) {
	t.Helper()

	t.Run(c.name, func(t *testing.T) {
		// Unique tx per case: 6 outputs with utxoBatchSize 4 = master (4) + 1 pagination record.
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

		hash := tx.TxIDChainHash()
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)

		key := func(recordIdx uint32) *aerospike.Key {
			k, kErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, recordIdx))
			require.NoError(t, kErr)

			return k
		}

		bin := func(name fields.FieldName, v interface{}) *aerospike.Bin { return aerospike.NewBin(name.String(), v) }

		// Force the master pre-state. The record already holds a prior block (7), so
		// "has block IDs" is true regardless of the block being mined now.
		// optional bins: nil value deletes the bin, otherwise sets it
		optional := func(name fields.FieldName, v *int) *aerospike.Bin {
			if v == nil {
				return bin(name, nil)
			}

			return bin(name, *v)
		}

		masterBins := []*aerospike.Bin{
			bin(fields.External, true),
			bin(fields.SpentUtxos, c.spentUtxos),
			bin(fields.SpentExtraRecs, c.spentExtraRecs),
			bin(fields.Conflicting, c.conflicting),
			bin(fields.BlockIDs, []int{7}),
			bin(fields.BlockHeights, []int{500}),
			bin(fields.SubtreeIdxs, []int{0}),
			optional(fields.DeleteAtHeight, c.dah),
			optional(fields.PreserveUntil, c.preserveUntil),
			optional(fields.UnminedSince, c.unminedSince),
		}
		childBins := []*aerospike.Bin{optional(fields.DeleteAtHeight, c.dah)}

		require.NoError(t, client.PutBins(nil, key(0), masterBins...))
		require.NoError(t, client.PutBins(nil, key(1), childBins...))

		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{hash}, utxo.MinedBlockInfo{
			BlockID:        100,
			BlockHeight:    minedHeight,
			OnLongestChain: c.onLongestChain,
		})
		require.NoError(t, err)

		read := func(recordIdx uint32) *int {
			rec, gErr := client.Get(nil, key(recordIdx))
			require.NoError(t, gErr)
			require.NotNil(t, rec)

			v, ok := rec.Bins[fields.DeleteAtHeight.String()]
			if !ok || v == nil {
				return nil
			}

			i, ok := v.(int)
			require.True(t, ok, "unexpected DAH type %T", v)

			return &i
		}

		show := func(p *int) string {
			if p == nil {
				return "<absent>"
			}

			return fmt.Sprint(*p)
		}

		require.Equal(t, show(c.wantDAH(retentionDAH)), show(read(0)), "master deleteAtHeight")
		require.Equal(t, show(c.wantChildDAH(retentionDAH)), show(read(1)), "pagination record deleteAtHeight")
	})
}
