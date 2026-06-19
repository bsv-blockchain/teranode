// Package blockvalidation implements difficulty validation tests for BlockValidation.
package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidateBlock_IncorrectDifficultyBits tests that blocks with incorrect nBits are rejected
func TestValidateBlock_IncorrectDifficultyBits(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	// Create coinbase transaction
	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)
	coinbaseTx.Outputs = nil
	err = coinbaseTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000)
	require.NoError(t, err)

	// Create a simple subtree
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	// Create subtree data
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	// Create previous block header
	genesisHash := &chainhash.Hash{}
	prevBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  genesisHash,
		HashMerkleRoot: &chainhash.Hash{}, // Need to initialize this
		Timestamp:      uint32(time.Now().Unix()),
	}
	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)
	prevBlockHeader.Bits = *nBits
	prevBlockHeader.Nonce = 0

	// Find a nonce that satisfies the difficulty for previous block
	for {
		prevBlockHeader.Nonce++
		if ok, _, _ := prevBlockHeader.HasMetTargetDifficulty(); ok {
			break
		}
	}

	// Create the expected difficulty for the next block
	expectedNBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	// Create current block header with INCORRECT nBits (different from expected)
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevBlockHeader.Hash(),
		HashMerkleRoot: subtree.RootHash(),
		Timestamp:      uint32(time.Now().Unix() + 1),
		Nonce:          0,
	}

	// Set INCORRECT difficulty bits (different from expected)
	incorrectBits, err := model.NewNBitFromString("1f7fffff") // Different from expected
	require.NoError(t, err)
	blockHeader.Bits = *incorrectBits

	// Mine the block to meet the (incorrect) target difficulty
	for {
		blockHeader.Nonce++
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		if blockHeader.Nonce > 1000000 {
			t.Fatal("Could not find valid nonce")
		}
	}

	// Create block
	block := &model.Block{
		Header:           blockHeader,
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		Height:           1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
	}

	// Mock blockchain client
	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("GetBlockExists", mock.Anything, blockHeader.Hash()).Return(false, nil).Once()
	mockBlockchain.On("GetBlockHeaders", mock.Anything, prevBlockHeader.Hash(), mock.Anything).
		Return([]*model.BlockHeader{prevBlockHeader}, []*model.BlockHeaderMeta{{ID: 0, Height: 0}}, nil).Once()
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()

	// Mock Subscribe for blockchain notifications
	notificationChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(notificationChan, nil).Maybe()

	// Mock GetNextWorkRequired to return the expected difficulty
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, prevBlockHeader.Hash(), mock.Anything).
		Return(expectedNBits, nil).Once()

	// Mock AddBlock to store invalid block when difficulty check fails
	mockBlockchain.On("AddBlock", mock.Anything, block, "", mock.Anything).Return(nil).Once()

	// Mock GetBlock for bloom filter creation
	prevBlock := &model.Block{
		Header:     prevBlockHeader,
		CoinbaseTx: coinbaseTx,
		Subtrees:   []*chainhash.Hash{},
		Height:     0,
	}
	mockBlockchain.On("GetBlock", mock.Anything, prevBlockHeader.Hash()).Return(prevBlock, nil).Maybe()

	// Mock GetBestBlockHeader for bloom filter pruning
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(blockHeader, &model.BlockHeaderMeta{Height: 1}, nil).Maybe()

	// Mock GetBlockIsMined for parent block mining status check
	mockBlockchain.On("GetBlockIsMined", mock.Anything, prevBlockHeader.Hash()).Return(true, nil)

	// Create BlockValidation instance
	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// Store the subtree
	subtreeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	err = subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
	require.NoError(t, err)

	// Validate the block - it should fail due to incorrect difficulty bits
	err = bv.ValidateBlock(ctx, block, "test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "incorrect difficulty bits")

	// Verify all mocks were called
	mockBlockchain.AssertExpectations(t)
}

// TestValidateBlock_DoesNotMeetTargetDifficulty tests that blocks not meeting target difficulty are rejected
func TestValidateBlock_DoesNotMeetTargetDifficulty(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	// Create coinbase transaction
	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)
	coinbaseTx.Outputs = nil
	err = coinbaseTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000)
	require.NoError(t, err)

	// Create a simple subtree
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	// Create subtree data
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	// Create previous block header
	genesisHash := &chainhash.Hash{}
	prevBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  genesisHash,
		HashMerkleRoot: &chainhash.Hash{}, // Need to initialize this
		Timestamp:      uint32(time.Now().Unix()),
	}
	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)
	prevBlockHeader.Bits = *nBits
	prevBlockHeader.Nonce = 0

	// Find a nonce that satisfies the difficulty for previous block
	for {
		prevBlockHeader.Nonce++
		if ok, _, _ := prevBlockHeader.HasMetTargetDifficulty(); ok {
			break
		}
	}

	// Create the expected difficulty for the next block (high difficulty)
	expectedNBits, err := model.NewNBitFromString("18000001") // Very high difficulty
	require.NoError(t, err)

	// Create current block header with correct nBits but BAD nonce (won't meet target)
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevBlockHeader.Hash(),
		HashMerkleRoot: subtree.RootHash(),
		Timestamp:      uint32(time.Now().Unix() + 1),
		Bits:           *expectedNBits, // Correct difficulty bits
		Nonce:          12345,          // Bad nonce that won't meet the high difficulty
	}

	// Verify the block does NOT meet target difficulty
	ok, _, _ := blockHeader.HasMetTargetDifficulty()
	require.False(t, ok, "Block should not meet target difficulty with bad nonce")

	// Create block
	block := &model.Block{
		Header:           blockHeader,
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		Height:           1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
	}

	// Mock blockchain client
	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("GetBlockExists", mock.Anything, blockHeader.Hash()).Return(false, nil).Once()
	mockBlockchain.On("GetBlockHeaders", mock.Anything, prevBlockHeader.Hash(), mock.Anything).
		Return([]*model.BlockHeader{prevBlockHeader}, []*model.BlockHeaderMeta{{ID: 0, Height: 0}}, nil).Once()
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()

	// Mock Subscribe for blockchain notifications
	notificationChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(notificationChan, nil).Maybe()

	// Mock GetNextWorkRequired to return the expected difficulty
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, prevBlockHeader.Hash(), mock.Anything).
		Return(expectedNBits, nil).Once()

	// Mock AddBlock to store invalid block when difficulty target is not met
	mockBlockchain.On("AddBlock", mock.Anything, block, "", mock.Anything).Return(nil).Once()

	// Mock GetBlock for bloom filter creation
	prevBlock := &model.Block{
		Header:     prevBlockHeader,
		CoinbaseTx: coinbaseTx,
		Subtrees:   []*chainhash.Hash{},
		Height:     0,
	}
	mockBlockchain.On("GetBlock", mock.Anything, prevBlockHeader.Hash()).Return(prevBlock, nil).Maybe()

	// Mock GetBestBlockHeader for bloom filter pruning
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(blockHeader, &model.BlockHeaderMeta{Height: 1}, nil).Maybe()

	// Mock GetBlockIsMined for parent block mining status check
	mockBlockchain.On("GetBlockIsMined", mock.Anything, prevBlockHeader.Hash()).Return(true, nil)

	// Create BlockValidation instance
	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// Store the subtree
	subtreeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	err = subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
	require.NoError(t, err)

	// Validate the block - it should fail due to not meeting target difficulty
	err = bv.ValidateBlock(ctx, block, "test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not meet target difficulty")

	// Verify all mocks were called
	mockBlockchain.AssertExpectations(t)
}

// TestValidateBlock_ValidDifficulty tests that blocks with correct difficulty are accepted
func TestValidateBlock_ValidDifficulty(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	// Create coinbase transaction
	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)
	coinbaseTx.Outputs = nil
	err = coinbaseTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000)
	require.NoError(t, err)

	// Create a simple subtree
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	// Create subtree data
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	// Store TX metadata for validation
	_, err = utxoStore.Create(ctx, coinbaseTx, 0)
	require.NoError(t, err)

	// Create previous block header
	genesisHash := &chainhash.Hash{}
	prevBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  genesisHash,
		HashMerkleRoot: &chainhash.Hash{}, // Need to initialize this
		Timestamp:      uint32(time.Now().Unix()),
	}
	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)
	prevBlockHeader.Bits = *nBits
	prevBlockHeader.Nonce = 0

	// Find a nonce that satisfies the difficulty for previous block
	for {
		prevBlockHeader.Nonce++
		if ok, _, _ := prevBlockHeader.HasMetTargetDifficulty(); ok {
			break
		}
	}

	// Create the expected difficulty for the next block
	expectedNBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	// Create current block header with CORRECT nBits
	// Use coinbase hash as merkle root for a single-transaction block
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevBlockHeader.Hash(),
		HashMerkleRoot: coinbaseTx.TxIDChainHash(), // Single tx block has coinbase hash as merkle root
		Timestamp:      uint32(time.Now().Unix() + 1),
		Bits:           *expectedNBits, // Correct difficulty bits
		Nonce:          0,
	}

	// Mine the block to meet the target difficulty
	for {
		blockHeader.Nonce++
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		if blockHeader.Nonce > 10000000 {
			t.Fatal("Could not find valid nonce")
		}
	}

	// Create block with no subtrees (coinbase-only block)
	block := &model.Block{
		Header:           blockHeader,
		Subtrees:         []*chainhash.Hash{}, // No subtrees for coinbase-only block
		Height:           1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1, // Just the coinbase
		SizeInBytes:      uint64(coinbaseTx.Size()),
	}

	// Mock blockchain client
	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("GetBlockExists", mock.Anything, blockHeader.Hash()).Return(false, nil).Once()
	mockBlockchain.On("GetBlockHeaders", mock.Anything, prevBlockHeader.Hash(), mock.Anything).
		Return([]*model.BlockHeader{prevBlockHeader}, []*model.BlockHeaderMeta{{ID: 0, Height: 0}}, nil)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)

	// Mock Subscribe for blockchain notifications
	notificationChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(notificationChan, nil)

	// Mock GetNextWorkRequired to return the expected difficulty
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, prevBlockHeader.Hash(), mock.Anything).
		Return(expectedNBits, nil).Once()

	// Mock GetBlock for bloom filter creation
	prevBlock := &model.Block{
		Header:     prevBlockHeader,
		CoinbaseTx: coinbaseTx,
		Subtrees:   []*chainhash.Hash{},
		Height:     0,
	}
	mockBlockchain.On("GetBlock", mock.Anything, prevBlockHeader.Hash()).Return(prevBlock, nil).Maybe()

	// Mock GetBestBlockHeader for bloom filter pruning
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(blockHeader, &model.BlockHeaderMeta{Height: 1}, nil).Maybe()

	// Mock GetBlockIsMined for parent block mining status check
	mockBlockchain.On("GetBlockIsMined", mock.Anything, prevBlockHeader.Hash()).Return(true, nil)

	// Mock GetBlockHeaderIDs for the Valid function (called with coinbase hash for merkle root verification)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{0}, nil).Maybe()

	// Mock SetBlockSubtreesSet for subtree tracking
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()

	// Mock AddBlock for successful validation
	mockBlockchain.On("AddBlock", mock.Anything, block, "", mock.Anything).Return(nil).Once()

	// Create BlockValidation instance
	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// Store the subtree
	subtreeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	err = subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
	require.NoError(t, err)

	// Validate the block - it should succeed
	err = bv.ValidateBlock(ctx, block, "test")
	require.NoError(t, err)

	// Verify all mocks were called
	mockBlockchain.AssertExpectations(t)
}

// countingSubtreeValidationClient wraps a subtreevalidation.Interface and
// counts CheckBlockSubtrees invocations. Used to pin the PoW-before-subtrees
// ordering in ValidateBlock.
type countingSubtreeValidationClient struct {
	subtreevalidation.Interface
	checkBlockSubtreesCalls atomic.Int32
}

func (c *countingSubtreeValidationClient) CheckBlockSubtrees(ctx context.Context, block *model.Block, peerID, baseURL string) error {
	c.checkBlockSubtreesCalls.Add(1)
	return c.Interface.CheckBlockSubtrees(ctx, block, peerID, baseURL)
}

// TestValidateBlock_PoWCheckedBeforeSubtreeValidation pins the ordering
// contract that the block-path WithUnconfirmedParentsAtCandidateHeight
// option relies on: ValidateBlock must verify nBits and target difficulty
// BEFORE invoking subtree validation. A block whose header fails
// HasMetTargetDifficulty must be rejected as BlockInvalid WITHOUT
// CheckBlockSubtrees ever being called — otherwise a peer could trigger
// full (fail-open) tx validation for a garbage header at zero cost.
func TestValidateBlock_PoWCheckedBeforeSubtreeValidation(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	countingClient := &countingSubtreeValidationClient{Interface: subtreeValidationClient}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	// Create coinbase transaction
	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)
	coinbaseTx.Outputs = nil
	err = coinbaseTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000)
	require.NoError(t, err)

	// Create a simple subtree
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	// Create previous block header
	genesisHash := &chainhash.Hash{}
	prevBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  genesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
	}
	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)
	prevBlockHeader.Bits = *nBits
	prevBlockHeader.Nonce = 0

	for {
		prevBlockHeader.Nonce++
		if ok, _, _ := prevBlockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		if prevBlockHeader.Nonce > 1000000 {
			t.Fatal("Could not find valid nonce")
		}
	}

	// Very high difficulty the bad nonce below cannot meet.
	expectedNBits, err := model.NewNBitFromString("18000001")
	require.NoError(t, err)

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevBlockHeader.Hash(),
		HashMerkleRoot: subtree.RootHash(),
		Timestamp:      uint32(time.Now().Unix() + 1),
		Bits:           *expectedNBits,
		Nonce:          12345, // bad nonce — does not meet the high difficulty
	}

	ok, _, _ := blockHeader.HasMetTargetDifficulty()
	require.False(t, ok, "Block must not meet target difficulty with bad nonce")

	block := &model.Block{
		Header:           blockHeader,
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		Height:           1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
	}

	// Mock blockchain client
	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("GetBlockExists", mock.Anything, blockHeader.Hash()).Return(false, nil).Once()
	mockBlockchain.On("GetBlockHeaders", mock.Anything, prevBlockHeader.Hash(), mock.Anything).
		Return([]*model.BlockHeader{prevBlockHeader}, []*model.BlockHeaderMeta{{ID: 0, Height: 0}}, nil).Once()
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil).Maybe()

	notificationChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(notificationChan, nil).Maybe()

	mockBlockchain.On("GetNextWorkRequired", mock.Anything, prevBlockHeader.Hash(), mock.Anything).
		Return(expectedNBits, nil).Once()

	// storeInvalidBlock persists the failed block
	mockBlockchain.On("AddBlock", mock.Anything, block, "", mock.Anything).Return(nil).Once()

	prevBlock := &model.Block{
		Header:     prevBlockHeader,
		CoinbaseTx: coinbaseTx,
		Subtrees:   []*chainhash.Hash{},
		Height:     0,
	}
	mockBlockchain.On("GetBlock", mock.Anything, prevBlockHeader.Hash()).Return(prevBlock, nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(blockHeader, &model.BlockHeaderMeta{Height: 1}, nil).Maybe()
	mockBlockchain.On("GetBlockIsMined", mock.Anything, prevBlockHeader.Hash()).Return(true, nil)

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, countingClient)

	// NOTE: the subtree is deliberately NOT stored — if ValidateBlock were to
	// run subtree validation before the difficulty check, CheckBlockSubtrees
	// would be invoked (and would do real work). The counter below pins that
	// it is never reached.
	err = bv.ValidateBlock(ctx, block, "test")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "failed PoW must surface as BlockInvalid, got: %v", err)
	require.Contains(t, err.Error(), "does not meet target difficulty")

	require.Equal(t, int32(0), countingClient.checkBlockSubtreesCalls.Load(),
		"CheckBlockSubtrees must NOT be called for a block that fails the difficulty check — PoW must be verified before (fail-open) subtree validation")

	mockBlockchain.AssertExpectations(t)
}
