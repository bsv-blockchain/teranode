//go:build teraslab

package teraslab_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teraslabstore "github.com/bsv-blockchain/teranode/stores/utxo/teraslab"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatchPreviousOutputsDecorateNoFetch exercises the BatchPreviousOutputsDecorate
// fast paths that never reach the client: an empty tx slice and txs whose inputs
// are all already decorated. Both must return nil without a server round trip, so
// they run against a zero-value Store with no Docker container.
func TestBatchPreviousOutputsDecorateNoFetch(t *testing.T) {
	store := &teraslabstore.Store{}
	ctx := context.Background()

	t.Run("empty slice is a no-op", func(t *testing.T) {
		require.NoError(t, store.BatchPreviousOutputsDecorate(ctx, nil))
		require.NoError(t, store.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{}))
	})

	t.Run("already-decorated inputs need no fetch", func(t *testing.T) {
		script, err := bscript.NewFromHexString("76a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac")
		require.NoError(t, err)

		tx := &bt.Tx{Inputs: []*bt.Input{{
			PreviousTxOutIndex: 0,
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   script,
		}}}
		require.NoError(t, tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{}))

		// nil txs in the slice must also be tolerated alongside real ones.
		require.NoError(t, store.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{nil, tx}))

		// The pre-existing decoration must be left untouched.
		assert.Equal(t, uint64(1000), tx.Inputs[0].PreviousTxSatoshis)
		assert.Equal(t, script, tx.Inputs[0].PreviousTxScript)
	})
}

func TestBatchDecorate(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx1, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	tx2, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000")

	_, err := store.Create(ctx, tx1, 0)
	require.NoError(t, err)
	_, err = store.Create(ctx, tx2, 0)
	require.NoError(t, err)

	t.Run("decorates multiple transactions", func(t *testing.T) {
		items := []*utxo.UnresolvedMetaData{
			{Hash: *tx1.TxIDChainHash(), Idx: 0},
			{Hash: *tx2.TxIDChainHash(), Idx: 1},
		}

		err := store.BatchDecorate(ctx, items)
		require.NoError(t, err)

		require.NotNil(t, items[0].Data)
		assert.Equal(t, uint64(215), items[0].Data.Fee)

		require.NotNil(t, items[1].Data)
		assert.True(t, items[1].Data.IsCoinbase)
	})

	t.Run("handles missing transactions gracefully", func(t *testing.T) {
		fakeHash := chainhash.Hash{}
		fakeHash[0] = 0xFF
		items := []*utxo.UnresolvedMetaData{
			{Hash: *tx1.TxIDChainHash(), Idx: 0},
			{Hash: fakeHash, Idx: 1},
		}

		err := store.BatchDecorate(ctx, items)
		require.NoError(t, err)

		require.NotNil(t, items[0].Data)
		assert.Nil(t, items[1].Data) // not found
	})

	t.Run("empty slice is no-op", func(t *testing.T) {
		err := store.BatchDecorate(ctx, []*utxo.UnresolvedMetaData{})
		require.NoError(t, err)
	})
}
