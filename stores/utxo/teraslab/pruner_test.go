//go:build teraslab

package teraslab_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

func TestQueryOldUnminedTransactions(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	err := store.SetBlockHeight(100)
	require.NoError(t, err)

	// Create an unmined tx at height 100
	tx1, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err = store.Create(ctx, tx1, 100)
	require.NoError(t, err)

	t.Run("query with cutoff above creation height returns tx", func(t *testing.T) {
		hashes, err := store.QueryOldUnminedTransactions(ctx, 200)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(hashes), 1)

		found := false
		for _, h := range hashes {
			if h.IsEqual(tx1.TxIDChainHash()) {
				found = true
				break
			}
		}
		require.True(t, found, "expected tx1 in unmined results")
	})

	t.Run("query with cutoff below creation height returns nothing", func(t *testing.T) {
		hashes, err := store.QueryOldUnminedTransactions(ctx, 50)
		require.NoError(t, err)

		for _, h := range hashes {
			require.False(t, h.IsEqual(tx1.TxIDChainHash()), "tx1 should not be in results")
		}
	})
}

func TestPreserveTransactions(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	t.Run("empty list is no-op", func(t *testing.T) {
		err := store.PreserveTransactions(ctx, nil, 1000)
		require.NoError(t, err)
	})
}

func TestProcessExpiredPreservations(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	t.Run("process with no preservations is no-op", func(t *testing.T) {
		err := store.ProcessExpiredPreservations(ctx, 1000)
		require.NoError(t, err)
	})
}
