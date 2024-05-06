package util

import (
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTxMetaDataFromTx(t *testing.T) {
	prevTxHash, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
	satoshis := uint64(1000)

	// Create a mock transaction
	tx := bt.NewTx()
	err := tx.From(
		prevTxHash.String(),
		1,
		"76a914eb0bd5edba389198e73f8efabddfc61666969d1688ac",
		satoshis,
	)
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 700)
	require.NoError(t, err)

	// Call the function with the mock transaction
	metaData, err := TxMetaDataFromTx(tx)
	require.NoError(t, err)

	// Verify the returned metadata
	assert.Equal(t, tx, metaData.Tx)
	assert.Equal(t, uint64(tx.Size()), metaData.SizeInBytes)
	assert.Equal(t, tx.IsCoinbase(), metaData.IsCoinbase)
	assert.Len(t, metaData.ParentTxHashes, 1)
	assert.Equal(t, *prevTxHash, metaData.ParentTxHashes[0])
	assert.Equal(t, satoshis-700, metaData.Fee)
}
