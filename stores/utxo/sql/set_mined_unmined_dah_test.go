package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// spendAllOutputs spends every output of parent so it is fully spent.
func spendAllOutputs(ctx context.Context, t *testing.T, store *Store, parent *bt.Tx, height uint32) {
	t.Helper()

	parentHash := parent.TxIDChainHash()

	for i, out := range parent.Outputs {
		child := bt.NewTx()
		require.NoError(t, child.From(parentHash.String(), uint32(i), out.LockingScript.String(), out.Satoshis))
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))
		_, err := store.Spend(ctx, child, height)
		require.NoError(t, err, "spending output %d should succeed", i)
	}
}

func readDAHAndUnmined(ctx context.Context, t *testing.T, store *Store, hash *chainhash.Hash) (dah, unminedSince *int64) {
	t.Helper()

	err := store.db.QueryRowContext(ctx,
		"SELECT delete_at_height, unmined_since FROM transactions WHERE hash = $1", hash[:]).Scan(&dah, &unminedSince)
	require.NoError(t, err)

	return dah, unminedSince
}

// TestSetMinedNotOnLongestChainReevaluatesDAH pins SQL to the Aerospike Lua setMined, which
// calls setDeleteAtHeight after appending the block even when onLongestChain=false. A fully
// spent tx that is still mined on the main chain (unmined_since NULL) and also shows up in a
// fork block keeps its delete_at_height, bumped forward if the fork block is higher. SQL used
// to null it unconditionally, so the tx was never pruned.
func TestSetMinedNotOnLongestChainReevaluatesDAH(t *testing.T) {
	const mainHeight uint32 = 1000

	tests := []struct {
		name       string
		forkHeight uint32
		wantDAH    func(retention uint32) int64
	}{
		{"fork block below main block keeps DAH", mainHeight - 10, func(r uint32) int64 { return int64(mainHeight + r) }},
		{"fork block above main block bumps DAH", mainHeight + 10, func(r uint32) int64 { return int64(mainHeight + 10 + r) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			store, parent := setup(ctx, t)
			retention := store.settings.GetUtxoStoreBlockHeightRetention()
			require.Greater(t, retention, uint32(0))
			require.NoError(t, store.SetBlockHeight(mainHeight))

			parentHash := parent.TxIDChainHash()
			_, err := store.Create(ctx, parent, mainHeight)
			require.NoError(t, err)
			spendAllOutputs(ctx, t, store, parent, mainHeight)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
				BlockID: 100, BlockHeight: mainHeight, OnLongestChain: true,
			})
			require.NoError(t, err)

			dah, unminedSince := readDAHAndUnmined(ctx, t, store, parentHash)
			require.NotNil(t, dah)
			require.Nil(t, unminedSince)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
				BlockID: 200, BlockHeight: tc.forkHeight, OnLongestChain: false,
			})
			require.NoError(t, err)

			dah, unminedSince = readDAHAndUnmined(ctx, t, store, parentHash)
			require.Nil(t, unminedSince, "still mined on the main chain")
			require.NotNil(t, dah, "DAH must not be nulled while the tx is still mined on the main chain")
			require.Equal(t, tc.wantDAH(retention), *dah)
		})
	}
}
