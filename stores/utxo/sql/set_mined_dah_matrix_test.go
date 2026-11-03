package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSetMinedNotOnLongestChainDAHMatrix pins the SQL delete_at_height re-evaluation on a
// fork block (onLongestChain=false) to the Lua setDeleteAtHeight order of checks:
// preserve_until, conflicting, then "all spent and mined on the main chain", else clear.
func TestSetMinedNotOnLongestChainDAHMatrix(t *testing.T) {
	const mainHeight uint32 = 1000
	const forkHeight uint32 = 1010

	const staleDAH int64 = 5000

	tests := []struct {
		name      string
		spendAll  bool
		retention *uint32
		mutate    string // SQL run after mining on the main chain; may be empty
		// wantDAH returns the expected DAH (nil = NULL) given the retention.
		wantDAH func(retention uint32) *int64
	}{
		{
			name: "unspent outputs clear the DAH", spendAll: false,
			mutate:  "UPDATE transactions SET delete_at_height = 5000",
			wantDAH: func(uint32) *int64 { return nil },
		},
		{
			name: "non-NULL unmined_since clears the DAH", spendAll: true,
			mutate:  "UPDATE transactions SET unmined_since = 7",
			wantDAH: func(uint32) *int64 { return nil },
		},
		{
			name: "preserve_until leaves the DAH untouched", spendAll: true,
			mutate:  "UPDATE transactions SET preserve_until = 99999",
			wantDAH: func(r uint32) *int64 { v := int64(mainHeight + r); return &v },
		},
		{
			name: "retention 0 leaves the DAH untouched", spendAll: true,
			retention: new(uint32),
			mutate:    "UPDATE transactions SET delete_at_height = 5000",
			wantDAH:   func(uint32) *int64 { v := staleDAH; return &v },
		},
		{
			name: "conflicting without a DAH sets it", spendAll: false,
			mutate:  "UPDATE transactions SET conflicting = true, delete_at_height = NULL",
			wantDAH: func(r uint32) *int64 { v := int64(forkHeight + r); return &v },
		},
		{
			name: "conflicting with a DAH leaves it unchanged", spendAll: true,
			// Lower than the new DAH, so a generic "bump" arm would change it.
			mutate:  "UPDATE transactions SET conflicting = true, delete_at_height = 5",
			wantDAH: func(uint32) *int64 { v := int64(5); return &v },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			store, parent := setup(ctx, t)
			if tc.retention != nil {
				store.settings.GlobalBlockHeightRetention = *tc.retention
				store.settings.UtxoStore.BlockHeightRetentionAdjustment = 0
			}

			retention := store.settings.GetUtxoStoreBlockHeightRetention()
			require.NoError(t, store.SetBlockHeight(mainHeight))

			parentHash := parent.TxIDChainHash()
			_, err := store.Create(ctx, parent, mainHeight)
			require.NoError(t, err)

			if tc.spendAll {
				spendAllOutputs(ctx, t, store, parent, mainHeight)
			}

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
				BlockID: 100, BlockHeight: mainHeight, OnLongestChain: true,
			})
			require.NoError(t, err)

			if tc.mutate != "" {
				_, err = store.db.ExecContext(ctx, tc.mutate)
				require.NoError(t, err)
			}

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
				BlockID: 200, BlockHeight: forkHeight, OnLongestChain: false,
			})
			require.NoError(t, err)

			dah, _ := readDAHAndUnmined(ctx, t, store, parentHash)

			want := tc.wantDAH(retention)
			if want == nil {
				require.Nil(t, dah)
			} else {
				require.NotNil(t, dah)
				require.Equal(t, *want, *dah)
			}
		})
	}
}

// TestUnsetMinedLastBlockOfMainChainTx: invalidating the only block of a fully spent
// main-chain tx stamps a backdated unmined_since AND clears the DAH. The DAH UPDATE must run
// after the unmined_since UPDATE, otherwise it would still see unmined_since NULL and keep it.
func TestUnsetMinedLastBlockOfMainChainTx(t *testing.T) {
	const mainHeight uint32 = 50
	const tip uint32 = 5000

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, parent := setup(ctx, t)
	parentHash := parent.TxIDChainHash()

	_, err := store.Create(ctx, parent, mainHeight)
	require.NoError(t, err)
	spendAllOutputs(ctx, t, store, parent, mainHeight)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
		BlockID: 100, BlockHeight: mainHeight, OnLongestChain: true,
	})
	require.NoError(t, err)

	dah, _ := readDAHAndUnmined(ctx, t, store, parentHash)
	require.NotNil(t, dah)

	require.NoError(t, store.SetBlockHeight(tip))

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
		BlockID: 100, BlockHeight: mainHeight, UnsetMined: true,
	})
	require.NoError(t, err)

	dah, unminedSince := readDAHAndUnmined(ctx, t, store, parentHash)
	require.Nil(t, dah)
	require.NotNil(t, unminedSince)
	require.Equal(t, int64(utxo.BackdatedUnminedSince(tip, store.settings.UtxoStore.UnminedTxRetention)), *unminedSince)
}
