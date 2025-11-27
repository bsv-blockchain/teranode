package testhelpers

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// SubtreeTestFixture contains all the data needed for subtree-related tests
type SubtreeTestFixture struct {
	Block        *model.Block
	Subtrees     []*subtreepkg.Subtree
	Transactions []*bt.Tx
	CoinbaseTx   *bt.Tx
}

// GenerateBlockWithSubtrees creates a test block with real transactions organized into subtrees.
// This is useful for benchmarking and testing subtree validation with different configurations.
func GenerateBlockWithSubtrees(t *testing.T, totalTxCount, txPerSubtree int, store blob.Store) *SubtreeTestFixture {
	t.Helper()

	// Use the existing helper to create a chain of real transactions
	// Note: CreateTestTransactionChainWithCount returns count transactions total (including coinbase)
	allTxs := transactions.CreateTestTransactionChainWithCount(t, uint32(totalTxCount))

	coinbaseTx := allTxs[0]

	// Organize transactions into subtrees
	subtrees, subtreeHashes := organizeTransactionsIntoSubtrees(t, allTxs, txPerSubtree)

	// Store subtrees if a store is provided
	if store != nil {
		storeSubtrees(t, store, subtrees, allTxs, txPerSubtree)
	}

	// Calculate merkle root from subtree hashes
	merkleRoot := calculateMerkleRootFromSubtrees(t, subtreeHashes, coinbaseTx.TxIDChainHash())

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           *nBits,
		Nonce:          0,
	}

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(totalTxCount),
		SizeInBytes:      calculateBlockSize(allTxs),
		Subtrees:         subtreeHashes,
		Height:           100,
	}

	return &SubtreeTestFixture{
		Block:        block,
		Subtrees:     subtrees,
		Transactions: allTxs,
		CoinbaseTx:   coinbaseTx,
	}
}

// organizeTransactionsIntoSubtrees splits transactions into subtrees of the specified size
func organizeTransactionsIntoSubtrees(t *testing.T, allTxs []*bt.Tx, txPerSubtree int) ([]*subtreepkg.Subtree, []*chainhash.Hash) {
	t.Helper()

	subtrees := make([]*subtreepkg.Subtree, 0)
	subtreeHashes := make([]*chainhash.Hash, 0)
	var currentSubtree *subtreepkg.Subtree
	var err error

	txIndex := 0
	for txIndex < len(allTxs) {
		// Create new subtree if needed
		if currentSubtree == nil || currentSubtree.IsComplete() {
			if currentSubtree != nil && currentSubtree.Length() > 0 {
				subtrees = append(subtrees, currentSubtree)
				hash := currentSubtree.RootHash()
				subtreeHashes = append(subtreeHashes, hash)
			}

			currentSubtree, err = subtreepkg.NewTreeByLeafCount(txPerSubtree)
			require.NoError(t, err)

			// Add coinbase placeholder to first subtree
			if len(subtrees) == 0 {
				err = currentSubtree.AddCoinbaseNode()
				require.NoError(t, err)
				txIndex++
				continue
			}
		}

		// Add transaction to subtree
		tx := allTxs[txIndex]
		txHash := tx.TxIDChainHash()
		fee := calculateTxFee(tx)
		size := uint64(len(tx.Bytes()))

		err = currentSubtree.AddNode(*txHash, fee, size)
		require.NoError(t, err)

		txIndex++
	}

	// Add the last subtree
	if currentSubtree != nil && currentSubtree.Length() > 0 {
		subtrees = append(subtrees, currentSubtree)
		hash := currentSubtree.RootHash()
		subtreeHashes = append(subtreeHashes, hash)
	}

	return subtrees, subtreeHashes
}

// storeSubtrees stores all subtrees and their transaction data in the provided store
func storeSubtrees(t *testing.T, store blob.Store, subtrees []*subtreepkg.Subtree, allTxs []*bt.Tx, txPerSubtree int) {
	t.Helper()

	ctx := context.Background()
	txIndex := 0

	for i, subtree := range subtrees {
		rootHash := subtree.RootHash()

		// Serialize and store the subtree
		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		err = store.Set(ctx, rootHash[:], fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)

		// Create subtree data from the actual transactions
		var txData []byte
		startIdx := txIndex
		if i == 0 {
			// First subtree: skip coinbase placeholder (index 0), start from index 1
			startIdx = 1
			txIndex = 1
		}

		for j := 0; j < subtree.Length() && txIndex < len(allTxs); j++ {
			if i == 0 && j == 0 {
				// Skip the coinbase placeholder slot in first subtree
				continue
			}
			tx := allTxs[txIndex]
			txData = append(txData, tx.Bytes()...)
			txIndex++
		}

		// Handle case where we're at first subtree but haven't incremented past coinbase
		if i == 0 && startIdx == 1 {
			txIndex = subtree.Length()
		}

		err = store.Set(ctx, rootHash[:], fileformat.FileTypeSubtreeData, txData)
		require.NoError(t, err)
	}
}

// calculateMerkleRootFromSubtrees computes the merkle root from subtree hashes
func calculateMerkleRootFromSubtrees(t *testing.T, subtreeHashes []*chainhash.Hash, coinbaseTxID *chainhash.Hash) *chainhash.Hash {
	t.Helper()

	if len(subtreeHashes) == 0 {
		return coinbaseTxID
	}

	if len(subtreeHashes) == 1 {
		return subtreeHashes[0]
	}

	// Create merkle tree from subtree hashes
	st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(subtreeHashes))
	require.NoError(t, err)

	for _, hash := range subtreeHashes {
		err := st.AddNode(*hash, 1, 0)
		require.NoError(t, err)
	}

	rootHash := st.RootHash()
	result, err := chainhash.NewHash(rootHash[:])
	require.NoError(t, err)

	return result
}

// calculateTxFee calculates the fee for a transaction
func calculateTxFee(tx *bt.Tx) uint64 {
	inputTotal := tx.TotalInputSatoshis()
	outputTotal := tx.TotalOutputSatoshis()
	if inputTotal > outputTotal {
		return inputTotal - outputTotal
	}
	return 0
}

// calculateBlockSize calculates the total size of all transactions
func calculateBlockSize(txs []*bt.Tx) uint64 {
	var total uint64
	for _, tx := range txs {
		total += uint64(len(tx.Bytes()))
	}
	return total
}

// ExtractTransactionsFromSubtreeData parses transaction data bytes into transactions
func ExtractTransactionsFromSubtreeData(data []byte) ([]*bt.Tx, error) {
	txs := make([]*bt.Tx, 0)
	offset := 0

	for offset < len(data) {
		tx, bytesRead, err := bt.NewTxFromStream(data[offset:])
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
		offset += bytesRead
	}

	return txs, nil
}
