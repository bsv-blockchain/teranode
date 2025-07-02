package subtree

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tx, _ = bt.NewTxFromString("010000000000000000ef0158ef6d539bf88c850103fa127a92775af48dba580c36bbde4dc6d8b9da83256d050000006a47304402200ca69c5672d0e0471cd4ff1f9993f16103fc29b98f71e1a9760c828b22cae61c0220705e14aa6f3149130c3a6aa8387c51e4c80c6ae52297b2dabfd68423d717be4541210286dbe9cd647f83a4a6b29d2a2d3227a897a4904dc31769502cb013cbe5044dddffffffff8c2f6002000000001976a914308254c746057d189221c36418ba93337de33bc988ac03002d3101000000001976a91498cde576de501ceb5bb1962c6e49a4d1af17730788ac80969800000000001976a914eb7772212c334c0bdccee75c0369aa675fc21d2088ac706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac00000000")
)

func TestNewSubtreeData(t *testing.T) {
	tx1 := tx.Clone()
	tx1.Version = 1

	tx2 := tx.Clone()
	tx2.Version = 2

	tx3 := tx.Clone()
	tx3.Version = 3

	tx4 := tx.Clone()
	tx4.Version = 4

	t.Run("create new subtree data", func(t *testing.T) {
		subtree, err := NewTree(2)
		require.NoError(t, err)

		_ = subtree.AddNode(*tx1.TxIDChainHash(), 111, 0)
		_ = subtree.AddNode(*tx2.TxIDChainHash(), 111, 0)
		_ = subtree.AddNode(*tx3.TxIDChainHash(), 111, 0)
		_ = subtree.AddNode(*tx4.TxIDChainHash(), 111, 0)

		// Test the constructor
		subtreeData := NewSubtreeData(subtree)

		// Verify the subtree data
		assert.Equal(t, subtree, subtreeData.Subtree)
		assert.Equal(t, subtree.Size(), len(subtreeData.Txs))
		assert.Equal(t, 4, len(subtreeData.Txs))

		// All transactions should be initially nil
		for i := 0; i < len(subtreeData.Txs); i++ {
			assert.Nil(t, subtreeData.Txs[i])
		}
	})

	t.Run("add transaction successfully", func(t *testing.T) {
		subtree, err := NewTree(2)
		require.NoError(t, err)

		_ = subtree.AddNode(*tx1.TxIDChainHash(), 111, 0)

		subtreeData := NewSubtreeData(subtree)

		// Add the transaction to the subtree data
		err = subtreeData.AddTx(tx1, 0)
		require.NoError(t, err)

		// Verify the transaction was added
		assert.Equal(t, tx1, subtreeData.Txs[0])
	})

	t.Run("add with coinbase tx", func(t *testing.T) {
		coinbaseTx, _ := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")

		subtree, err := NewTree(2)
		require.NoError(t, err)

		require.NoError(t, subtree.AddCoinbaseNode())
		require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 111, 0))

		subtreeData := NewSubtreeData(subtree)

		// Add the transaction to the subtree data
		err = subtreeData.AddTx(coinbaseTx, 0)
		require.NoError(t, err)

		err = subtreeData.AddTx(tx1, 1)
		require.NoError(t, err)

		// Verify the transaction was added
		assert.Equal(t, coinbaseTx, subtreeData.Txs[0])
		assert.Equal(t, tx1, subtreeData.Txs[1])
	})

	t.Run("add transaction with mismatched hash", func(t *testing.T) {
		subtree, err := NewTree(2)
		require.NoError(t, err)

		_ = subtree.AddNode(*tx1.TxIDChainHash(), 111, 0)

		subtreeData := NewSubtreeData(subtree)

		// Add the transaction should fail due to hash mismatch
		err = subtreeData.AddTx(tx2, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction hash does not match subtree node hash")

		// Verify the transaction was not added
		assert.Nil(t, subtreeData.Txs[0])
	})
}

func setupSubtreeData(t *testing.T) (*Subtree, *SubtreeData) {
	tx1 := tx.Clone()
	tx1.Version = 1

	tx2 := tx.Clone()
	tx2.Version = 2

	tx3 := tx.Clone()
	tx3.Version = 3

	tx4 := tx.Clone()
	tx4.Version = 4

	subtree, err := NewTree(2)
	require.NoError(t, err)

	_ = subtree.AddNode(*tx1.TxIDChainHash(), 111, 1)
	_ = subtree.AddNode(*tx2.TxIDChainHash(), 111, 2)
	_ = subtree.AddNode(*tx3.TxIDChainHash(), 111, 3)
	_ = subtree.AddNode(*tx4.TxIDChainHash(), 111, 4)

	subtreeData := NewSubtreeData(subtree)

	// Add transactions to the subtree data
	_ = subtreeData.AddTx(tx1, 0)
	_ = subtreeData.AddTx(tx2, 1)
	_ = subtreeData.AddTx(tx3, 2)
	_ = subtreeData.AddTx(tx4, 3)

	return subtree, subtreeData
}

func TestSerialize(t *testing.T) {
	tx1 := tx.Clone()
	tx1.Version = 1

	tx2 := tx.Clone()
	tx2.Version = 2

	tx3 := tx.Clone()
	tx3.Version = 3

	tx4 := tx.Clone()
	tx4.Version = 4

	t.Run("serialize subtree data", func(t *testing.T) {
		_, subtreeData := setupSubtreeData(t)

		// Serialize the subtree data
		serializedData, err := subtreeData.Serialize()
		require.NoError(t, err)

		// Ensure we have data
		assert.NotEmpty(t, serializedData)
	})

	t.Run("serialize with nil subtree", func(t *testing.T) {
		subtreeData := &SubtreeData{
			Subtree: nil,
			Txs:     make([]*bt.Tx, 0),
		}

		// Serialize should fail with nil subtree
		serializedData, err := subtreeData.Serialize()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot serialize, subtree is not set")
		assert.Nil(t, serializedData)
	})

	t.Run("serialize with missing transactions", func(t *testing.T) {
		subtree, err := NewTree(2)
		require.NoError(t, err)

		_ = subtree.AddNode(*tx1.TxIDChainHash(), 111, 0)
		_ = subtree.AddNode(*tx2.TxIDChainHash(), 111, 0)

		subtreeData := NewSubtreeData(subtree)

		_ = subtreeData.AddTx(tx1, 0)

		// Second transaction is missing, so serialization should fail
		serializedData, err := subtreeData.Serialize()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "subtree length does not match tx data length")
		assert.Nil(t, serializedData)
	})
}

func TestNewSubtreeDataFromBytes(t *testing.T) {
	t.Run("create from valid bytes", func(t *testing.T) {
		subtree, origData := setupSubtreeData(t)

		// Serialize the original data
		serializedData, err := origData.Serialize()
		require.NoError(t, err)

		// Create new subtree data from bytes
		newData, err := NewSubtreeDataFromBytes(subtree, serializedData)
		require.NoError(t, err)

		// Verify the new subtree data
		assert.Equal(t, subtree, newData.Subtree)
		assert.Equal(t, len(origData.Txs), len(newData.Txs))

		// Compare transactions (skipping first if it's a coinbase placeholder)
		startIdx := 0
		if subtree.Nodes[0].Hash.Equal(*CoinbasePlaceholderHash) {
			startIdx = 1
		}

		for i := startIdx; i < len(newData.Txs); i++ {
			if origData.Txs[i] != nil && newData.Txs[i] != nil {
				assert.Equal(t, origData.Txs[i].TxID(), newData.Txs[i].TxID())
			}
		}
	})

	t.Run("create from invalid bytes", func(t *testing.T) {
		subtree, _ := setupSubtreeData(t)

		// Create invalid serialized data
		invalidData := []byte("invalid data")

		// Create new subtree data from invalid bytes should fail
		newData, err := NewSubtreeDataFromBytes(subtree, invalidData)
		require.Error(t, err)
		assert.Nil(t, newData)
	})
}

func TestNewSubtreeDataFromReader(t *testing.T) {
	t.Run("create from valid reader", func(t *testing.T) {
		subtree, origData := setupSubtreeData(t)

		// Serialize the original data
		serializedData, err := origData.Serialize()
		require.NoError(t, err)

		// Create a reader from the serialized data
		reader := bytes.NewReader(serializedData)

		// Create new subtree data from reader
		newData, err := NewSubtreeDataFromReader(subtree, reader)
		require.NoError(t, err)

		// Verify the new subtree data
		assert.Equal(t, subtree, newData.Subtree)

		// Compare transactions (skipping first if it's a coinbase placeholder)
		startIdx := 0
		if subtree.Nodes[0].Hash.Equal(*CoinbasePlaceholderHash) {
			startIdx = 1
		}

		for i := startIdx; i < len(newData.Txs); i++ {
			if origData.Txs[i] != nil && newData.Txs[i] != nil {
				assert.Equal(t, origData.Txs[i].TxID(), newData.Txs[i].TxID())
			}
		}
	})

	t.Run("create from invalid reader", func(t *testing.T) {
		subtree, _ := setupSubtreeData(t)

		// Create invalid reader that returns EOF
		reader := &mockReader{err: io.EOF}

		// Create new subtree data from invalid reader
		newData, err := NewSubtreeDataFromReader(subtree, reader)
		assert.NoError(t, err) // EOF is handled specially and considered normal end of data
		assert.NotNil(t, newData)

		// Create invalid reader that returns error other than EOF
		reader = &mockReader{err: fmt.Errorf("read error")}

		// Create new subtree data from invalid reader should fail
		newData, err = NewSubtreeDataFromReader(subtree, reader)
		require.Error(t, err)
		assert.Nil(t, newData)
	})
}

// Mock reader for testing
type mockReader struct {
	err error
}

func (r *mockReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}
