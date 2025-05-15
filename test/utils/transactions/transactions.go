package transactions

import (
	"encoding/binary"
	"testing"

	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
)

// CreateTestTransactionChainWithCount generates a sequence of valid Bitcoin
// transactions for testing, starting with a coinbase transaction and creating
// a specified number of chained transactions. This utility function helps create
// realistic test scenarios with valid transaction chains.
//
// Parameters:
//   - t: Testing context for assertions
//   - count: Number of transactions to generate in the chain
//
// Returns a slice of transactions starting with the coinbase transaction.
func CreateTestTransactionChainWithCount(t *testing.T, count uint32) []*bt.Tx {
	if count < 2 {
		t.Fatalf("count must be greater than 1")
	}

	privateKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	// Create coinbase transaction
	coinbaseTx := bt.NewTx()
	err = coinbaseTx.From(
		"0000000000000000000000000000000000000000000000000000000000000000",
		0xffffffff,
		"",
		0,
	)
	require.NoError(t, err)

	blockHeight := uint32(100)
	blockHeightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockHeightBytes, blockHeight)

	arbitraryData := make([]byte, 0)
	arbitraryData = append(arbitraryData, 0x03)
	arbitraryData = append(arbitraryData, blockHeightBytes[:3]...)
	arbitraryData = append(arbitraryData, []byte("/Test miner/")...)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)

	err = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100_000_000)
	require.NoError(t, err)

	// Create the chain of transactions
	result := []*bt.Tx{coinbaseTx}

	// initialize the parent transaction, which is the coinbase transaction
	// later this will be replaced by the previous transaction in the chain
	parentTx := coinbaseTx

	for i := uint32(0); i < count-2; i++ {
		vOut := uint32(1)
		if i == 0 {
			vOut = 0
		}

		tx := CreateSpendingTx(t, parentTx, vOut, 1000, address, privateKey)
		result = append(result, tx)

		parentTx = tx
	}

	return result
}

func CreateSpendingTx(t *testing.T, prevTx *bt.Tx, vOut uint32, amount uint64, address *bscript.Address, privateKey *bec.PrivateKey) *bt.Tx {
	tx := bt.NewTx()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      prevTx.TxIDChainHash(),
		Vout:          vOut,
		LockingScript: prevTx.Outputs[vOut].LockingScript,
		Satoshis:      prevTx.Outputs[vOut].Satoshis,
	}))

	fee := bt.NewFeeQuote()

	require.NoError(t, tx.AddP2PKHOutputFromAddress(address.AddressString, amount))
	require.NoError(t, tx.ChangeToAddress(address.AddressString, fee))
	require.NoError(t, tx.FillAllInputs(t.Context(), &unlocker.Getter{PrivateKey: privateKey}))

	return tx
}
