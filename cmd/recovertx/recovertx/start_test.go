package recovertx

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoverTx(t *testing.T) {
	ctx := context.Background()

	// Initialize a memory UTXO store
	logger := ulogger.New("recover_tx_test")
	utxoStore := memory.New(logger)

	// Define test data
	txIDHex := "680b6d4a328b2059a27568e756e317405ee1cf7e8faccf95943bd542535d25e3"

	txID, err := chainhash.NewHashFromStr(txIDHex)
	require.NoError(t, err)

	blockHeight := uint32(100)

	spendingTxIDHex := "680b6d4a328b2059a27568e756e317405ee1cf7e8faccf95943bd542535d25e4"
	spendingTxID, err := chainhash.NewHashFromStr(spendingTxIDHex)
	require.NoError(t, err)

	spendingTxIDs := []*chainhash.Hash{spendingTxID}

	// Run the recoverTx function
	err = recoverTx(ctx, utxoStore, txID, blockHeight, spendingTxIDs)
	require.NoError(t, err)

	// Check if the UTXO has been created properly
	utxoMap := utxoStore.GetUtxoMap(*txID)
	assert.NotNil(t, utxoMap)

	utxoHash0, err := chainhash.NewHashFromStr("31a199670b8d8a658b7e23ed2812482c9e8d2fc34535f7b09b87b04ccffbf3ba")
	require.NoError(t, err)
	assert.True(t, utxoMap[*utxoHash0].Equal(*spendingTxID))

	utxoHash1, err := chainhash.NewHashFromStr("16895a69fdf18a2ba2cc5d81e130ed12ca5414be5738e6d15b8df76d72efac4d")
	require.NoError(t, err)
	assert.Nil(t, utxoMap[*utxoHash1])
}
