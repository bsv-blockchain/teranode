package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// Clearing inpoints on reload must not allocate: it ran once per unmined tx and
// the replacement objects were retained, ~64B per tx on a billion-tx reload.
func Test_clearUnminedTxInpoints(t *testing.T) {
	t.Run("clears in place without allocating", func(t *testing.T) {
		inpoints := subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{{0x01}}}
		tx := &utxo.UnminedTransaction{TxInpoints: &inpoints}

		allocs := testing.AllocsPerRun(100, func() {
			clearUnminedTxInpoints(tx)
		})

		require.Zero(t, allocs)
		require.Same(t, &inpoints, tx.TxInpoints)
		require.Empty(t, tx.TxInpoints.ParentTxHashes)
	})

	t.Run("nil inpoints get an empty value", func(t *testing.T) {
		tx := &utxo.UnminedTransaction{}

		clearUnminedTxInpoints(tx)

		require.NotNil(t, tx.TxInpoints)
		require.Empty(t, tx.TxInpoints.ParentTxHashes)
	})
}
