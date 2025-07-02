package transactions

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/libsv/go-bk/bec"
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

	privateKey, publicKey := bec.PrivKeyFromBytes(bec.S256(), []byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	blockHeight := uint32(100)

	coinbaseTx := Create(t,
		WithCoinbaseData(blockHeight, "/Test miner/"),
		WithP2PKHOutputs(1, 50e8, publicKey),
	)

	// Create the chain of transactions
	result := []*bt.Tx{coinbaseTx}

	// initialize the parent transaction, which is the coinbase transaction
	// later this will be replaced by the previous transaction in the chain
	parentTx := coinbaseTx

	for i := uint32(0); i < count-2; i++ {
		vOut := uint32(1)
		if i == 0 {
			vOut = 0 // Spend the coinbase output 0 in first iteration
		}

		tx := Create(t,
			WithPrivateKey(privateKey),
			WithInput(parentTx, vOut),
			WithP2PKHOutputs(1, 1000),
			WithChangeOutput(),
		)

		result = append(result, tx)

		parentTx = tx
	}

	return result
}
