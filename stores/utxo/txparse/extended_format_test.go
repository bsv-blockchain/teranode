package txparse

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseInputReferencesFromExtendedTx(t *testing.T) {
	t.Run("single input", func(t *testing.T) {
		tx, err := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
		require.NoError(t, err)

		txBytes := tx.ExtendedBytes()
		inputs, err := ParseInputReferencesFromExtendedTx(bytes.NewReader(txBytes))
		require.NoError(t, err)
		require.Len(t, inputs, 1)
		assert.Equal(t, tx.Inputs[0].PreviousTxIDChainHash().String(), inputs[0].PreviousTxIDChainHash().String())
		assert.Equal(t, tx.Inputs[0].PreviousTxOutIndex, inputs[0].PreviousTxOutIndex)
	})

	t.Run("zero inputs", func(t *testing.T) {
		tx := bt.NewTx()
		tx.Version = 1
		txBytes := tx.ExtendedBytes()
		inputs, err := ParseInputReferencesFromExtendedTx(bytes.NewReader(txBytes))
		require.NoError(t, err)
		assert.Empty(t, inputs)
	})

	t.Run("rejects non-extended format", func(t *testing.T) {
		tx := bt.NewTx()
		tx.Version = 1
		// Use standard Bytes() instead of ExtendedBytes()
		txBytes := tx.Bytes()
		_, err := ParseInputReferencesFromExtendedTx(bytes.NewReader(txBytes))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not in extended format")
	})

	t.Run("truncated input", func(t *testing.T) {
		tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
		txBytes := tx.ExtendedBytes()
		// Truncate after the extended marker + input count (11 bytes) + partial prevTxID
		truncated := txBytes[:20]
		_, err := ParseInputReferencesFromExtendedTx(bytes.NewReader(truncated))
		require.Error(t, err)
	})

	t.Run("empty reader", func(t *testing.T) {
		_, err := ParseInputReferencesFromExtendedTx(bytes.NewReader(nil))
		require.Error(t, err)
	})
}
