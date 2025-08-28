package testhelpers

import (
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// CreateTestBlockWithSubtrees creates a test block with mock subtrees
func CreateTestBlockWithSubtrees(_ *testing.T, height uint32) *model.Block {
	prevHash := chainhash.Hash{}
	merkleRoot := chainhash.Hash{1, 2, 3}
	subtreeHash := chainhash.Hash{4, 5, 6}

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: &merkleRoot,
			Timestamp:      1000000,
		},
		Height:           height,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{&subtreeHash},
	}
}

// CreateTestTransactions creates mock transactions for testing
func CreateTestTransactions(_ *testing.T, count int) []*bt.Tx {
	txs := make([]*bt.Tx, count)

	for i := 0; i < count; i++ {
		tx := bt.NewTx()

		// Add a simple input (except for coinbase)
		if i > 0 {
			prevHash := chainhash.Hash{byte(i)}
			_ = tx.From(prevHash.String(), 0, "", 1000)
		} else {
			// Coinbase input
			emptyHash := chainhash.Hash{}
			_ = tx.From(emptyHash.String(), 0xffffffff, "", 0)
		}

		// Add a simple output
		outputScript, _ := bscript.NewP2PKHFromPubKeyHash([]byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
		_ = tx.PayTo(outputScript, uint64(1000+i))

		txs[i] = tx
	}

	return txs
}
