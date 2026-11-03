package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
)

// mockBlockProcessor is a test implementation that always allows block processing
type mockBlockProcessor struct{}

func (m *mockBlockProcessor) CanProcessBlock(ctx context.Context, hash *chainhash.Hash, _ BlockPriority) (bool, error) {
	return true, nil
}

func (m *mockBlockProcessor) StartProcessingBlock(hash *chainhash.Hash, _ BlockPriority) bool {
	return true
}

func (m *mockBlockProcessor) IsBlockInFlight(hash *chainhash.Hash, pending func(chainhash.Hash) bool) bool {
	return pending(*hash)
}

func (m *mockBlockProcessor) FinishProcessingBlock(hash *chainhash.Hash) {}

func TestBlockPriorityQueue(t *testing.T) {
	t.Run("Priority ordering", func(t *testing.T) {
		pq := NewBlockPriorityQueue(ulogger.TestLogger{})
		mockBP := &mockBlockProcessor{}

		// Create test blocks with different priorities
		hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		hash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		hash3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")

		block1 := processBlockFound{hash: hash1, baseURL: "test1", peerID: "peer1"}
		block2 := processBlockFound{hash: hash2, baseURL: "test2", peerID: "peer2"}
		block3 := processBlockFound{hash: hash3, baseURL: "test3", peerID: "peer3"}

		// Add blocks with different priorities
		pq.Add(block3, PriorityDeepFork, 100)
		pq.Add(block2, PriorityNearFork, 200)
		pq.Add(block1, PriorityChainExtending, 300)

		// Should get chain-extending first
		b, status := pq.Get(context.Background(), mockBP)
		assert.Equal(t, GetOK, status)
		assert.Equal(t, hash1, b.hash)

		// Then near fork
		b, status = pq.Get(context.Background(), mockBP)
		assert.Equal(t, GetOK, status)
		assert.Equal(t, hash2, b.hash)

		// Finally deep fork
		b, status = pq.Get(context.Background(), mockBP)
		assert.Equal(t, GetOK, status)
		assert.Equal(t, hash3, b.hash)

		// Queue should be empty
		_, status = pq.Get(context.Background(), mockBP)
		assert.Equal(t, GetEmpty, status)
	})

	t.Run("Height ordering within same priority", func(t *testing.T) {
		pq := NewBlockPriorityQueue(ulogger.TestLogger{})
		mockBP := &mockBlockProcessor{}

		hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		hash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		hash3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")

		block1 := processBlockFound{hash: hash1, baseURL: "test1", peerID: "peer1"}
		block2 := processBlockFound{hash: hash2, baseURL: "test2", peerID: "peer2"}
		block3 := processBlockFound{hash: hash3, baseURL: "test3", peerID: "peer3"}

		// Add blocks with same priority but different heights
		pq.Add(block3, PriorityNearFork, 300)
		pq.Add(block1, PriorityNearFork, 100)
		pq.Add(block2, PriorityNearFork, 200)

		// Should get lowest height first
		b, status := pq.Get(context.Background(), mockBP)
		assert.Equal(t, GetOK, status)
		assert.Equal(t, hash1, b.hash)

		b, status = pq.Get(context.Background(), mockBP)
		assert.Equal(t, GetOK, status)
		assert.Equal(t, hash2, b.hash)

		b, status = pq.Get(context.Background(), mockBP)
		assert.Equal(t, GetOK, status)
		assert.Equal(t, hash3, b.hash)
	})

	t.Run("Queue statistics", func(t *testing.T) {
		pq := NewBlockPriorityQueue(ulogger.TestLogger{})

		hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		hash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		hash3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")

		block1 := processBlockFound{hash: hash1, baseURL: "test1", peerID: "peer1"}
		block2 := processBlockFound{hash: hash2, baseURL: "test2", peerID: "peer2"}
		block3 := processBlockFound{hash: hash3, baseURL: "test3", peerID: "peer3"}

		pq.Add(block1, PriorityChainExtending, 100)
		pq.Add(block2, PriorityNearFork, 200)
		pq.Add(block3, PriorityDeepFork, 300)

		chainExtending, nearFork, deepFork := pq.GetQueueStats()
		assert.Equal(t, 1, chainExtending)
		assert.Equal(t, 1, nearFork)
		assert.Equal(t, 1, deepFork)
	})

	t.Run("Duplicate prevention", func(t *testing.T) {
		pq := NewBlockPriorityQueue(ulogger.TestLogger{})

		hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		block1 := processBlockFound{hash: hash1, baseURL: "test1", peerID: "peer1"}

		// Add same block twice
		pq.Add(block1, PriorityChainExtending, 100)
		pq.Add(block1, PriorityDeepFork, 100) // Should be ignored

		assert.Equal(t, 1, pq.Size())
	})
}

func TestBlockClassifier(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	t.Run("Chain extending block", func(t *testing.T) {
		mockClient := new(blockchain.Mock)
		classifier := NewBlockClassifier(logger, 50, mockClient)

		// Create a proper best block header with all required fields
		prevHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		merkleRoot, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		nBits := model.NBit{0xff, 0xff, 0x00, 0x1d}

		bestHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           nBits,
			Nonce:          0,
		}

		// Calculate the actual hash of the best header
		bestHash := bestHeader.Hash()

		// Create a block that extends the best chain
		blockMerkleRoot, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  bestHash,
				HashMerkleRoot: blockMerkleRoot,
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           nBits,
				Nonce:          0,
			},
		}

		// Mock returns header
		mockClient.On("GetBestBlockHeader", ctx).Return(
			bestHeader,
			&model.BlockHeaderMeta{Height: 1000},
			nil,
		)

		priority, err := classifier.ClassifyBlock(ctx, block)
		assert.NoError(t, err)
		assert.Equal(t, PriorityChainExtending, priority)
	})

	t.Run("Near fork block", func(t *testing.T) {
		mockClient := new(blockchain.Mock)
		classifier := NewBlockClassifier(logger, 50, mockClient)

		// Create proper headers
		prevHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		merkleRoot, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		nBits := model.NBit{0xff, 0xff, 0x00, 0x1d}

		bestHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           nBits,
			Nonce:          0,
		}

		// Parent block
		parentHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		parentHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(time.Now().Unix() - 600),
			Bits:           nBits,
			Nonce:          0,
		}

		// Best block
		mockClient.On("GetBestBlockHeader", ctx).Return(
			bestHeader,
			&model.BlockHeaderMeta{Height: 1000},
			nil,
		)

		mockClient.On("GetBlockHeader", ctx, parentHash).Return(
			parentHeader,
			&model.BlockHeaderMeta{Height: 980},
			nil,
		)

		// Create a block on the fork
		blockMerkleRoot, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  parentHash,
				HashMerkleRoot: blockMerkleRoot,
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           nBits,
				Nonce:          0,
			},
		}

		priority, err := classifier.ClassifyBlock(ctx, block)
		assert.NoError(t, err)
		assert.Equal(t, PriorityNearFork, priority)
	})

	t.Run("Deep fork block", func(t *testing.T) {
		mockClient := new(blockchain.Mock)
		classifier := NewBlockClassifier(logger, 50, mockClient)

		// Create proper headers
		prevHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		merkleRoot, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		nBits := model.NBit{0xff, 0xff, 0x00, 0x1d}

		bestHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           nBits,
			Nonce:          0,
		}

		// Parent block - deep fork
		parentHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		parentHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(time.Now().Unix() - 600),
			Bits:           nBits,
			Nonce:          0,
		}

		// Best block
		mockClient.On("GetBestBlockHeader", ctx).Return(
			bestHeader,
			&model.BlockHeaderMeta{Height: 1000},
			nil,
		)

		mockClient.On("GetBlockHeader", ctx, parentHash).Return(
			parentHeader,
			&model.BlockHeaderMeta{Height: 900},
			nil,
		)

		// Create a block on the deep fork
		blockMerkleRoot, _ := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  parentHash,
				HashMerkleRoot: blockMerkleRoot,
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           nBits,
				Nonce:          0,
			},
		}

		priority, err := classifier.ClassifyBlock(ctx, block)
		assert.NoError(t, err)
		assert.Equal(t, PriorityDeepFork, priority)
	})
}

func TestForkManager(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}

	t.Run("Fork registration and management", func(t *testing.T) {
		settings := test.CreateBaseTestSettings(t)
		settings.BlockValidation.MaxParallelForks = 4
		fm := NewForkManager(logger, settings)

		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		fork := fm.RegisterFork("fork1", baseHash, 1000)
		assert.NotNil(t, fork)
		assert.Equal(t, "fork1", fork.ID)
		assert.Equal(t, baseHash, fork.BaseHash)
		assert.Equal(t, uint32(1000), fork.BaseHeight)

		// Register same fork again should return existing
		fork2 := fm.RegisterFork("fork1", baseHash, 1000)
		assert.Equal(t, fork, fork2)

		assert.Equal(t, 1, fm.GetForkCount())
	})

	t.Run("Block processing tracking", func(t *testing.T) {
		ctx := context.Background()
		settings := test.CreateBaseTestSettings(t)
		settings.BlockValidation.MaxParallelForks = 2
		fm := NewForkManager(logger, settings)

		hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		hash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")

		// Should be able to process first block
		canProcess, err := fm.CanProcessBlock(ctx, hash1, PriorityChainExtending)
		assert.NoError(t, err)
		assert.True(t, canProcess)
		assert.True(t, fm.StartProcessingBlock(hash1, PriorityChainExtending))

		// Should be able to process second block (different fork)
		canProcess, err = fm.CanProcessBlock(ctx, hash2, PriorityChainExtending)
		assert.NoError(t, err)
		assert.True(t, canProcess)
		assert.True(t, fm.StartProcessingBlock(hash2, PriorityChainExtending))

		// Should not be able to process third block (max parallel reached)
		hash3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
		canProcess, err = fm.CanProcessBlock(ctx, hash3, PriorityChainExtending)
		assert.NoError(t, err)
		assert.False(t, canProcess)

		assert.Equal(t, 2, fm.GetParallelProcessingCount())

		// Finish processing first block
		fm.FinishProcessingBlock(hash1)
		assert.Equal(t, 1, fm.GetParallelProcessingCount())

		// Now should be able to process third block
		canProcess, err = fm.CanProcessBlock(ctx, hash3, PriorityChainExtending)
		assert.NoError(t, err)
		assert.True(t, canProcess)
	})

	t.Run("Fork cleanup", func(t *testing.T) {
		settings := test.CreateBaseTestSettings(t)
		settings.BlockValidation.MaxParallelForks = 4
		fm := NewForkManager(logger, settings)

		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		fm.RegisterFork("fork1", baseHash, 1000)

		// Add block to fork
		prevHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		merkleRoot, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
		block := &model.Block{
			Height: 1001,
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // NBit is a [4]byte array
				Nonce:          0,
			},
		}
		// Calculate the actual hash of the block
		blockHash := block.Hash()

		err := fm.AddBlockToFork(block, "fork1")
		assert.NoError(t, err)

		forkID, exists := fm.GetForkForBlock(blockHash)
		assert.True(t, exists)
		assert.Equal(t, "fork1", forkID)

		// Cleanup fork
		fm.CleanupFork("fork1")
		assert.Equal(t, 0, fm.GetForkCount())

		_, exists = fm.GetForkForBlock(blockHash)
		assert.False(t, exists)
	})
}
