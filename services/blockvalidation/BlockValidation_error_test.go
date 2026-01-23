// Package blockvalidation implements error flow tests for BlockValidation.
package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testBlockValidation is a test double for BlockValidation that allows shadowing waitForPreviousBlocksToBeProcessed.
type testBlockValidation struct {
	*BlockValidation
	waitFunc     func(ctx context.Context, block *model.Block, headers []*model.BlockHeader) error
	setMinedChan chan *chainhash.Hash
}

func (tbv *testBlockValidation) ValidateBlock(ctx context.Context, block *model.Block, baseURL string, disableOptimisticMining ...bool) error {
	return tbv.BlockValidation.ValidateBlock(ctx, block, baseURL, disableOptimisticMining...)
}

func TestValidateBlock_WaitForPreviousBlocksToBeProcessed_RetryLogic(t *testing.T) {
	t.Skip("Skipping test with goroutine cleanup issues")
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 2

	coinbaseTx, childTx, _, _ := createCoinbaseAndChildTx(t)
	storeTxsInUtxoStore(t, utxoStore, coinbaseTx, childTx)
	subtree := buildAndStoreSubtree(t, subtreeStore, childTx)

	nBits, _ := model.NewNBitFromString("2000ffff")
	parentBlock, parentHash := createParentBlock(nBits)
	blockHeader := mineBlockHeader(t, parentHash, subtree.RootHash(), nBits)
	totalSize := int64(coinbaseTx.Size()) + int64(childTx.Size())
	block := createBlock(t, blockHeader, coinbaseTx, subtree.RootHash(), subtree, tSettings, totalSize)

	mockBlockchain := setupMockBlockchain(parentBlock)
	callCount := 0
	bv := &testBlockValidation{
		BlockValidation: NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient, &MockInvalidBlockHandler{}),
		waitFunc: func(ctx context.Context, block *model.Block, headers []*model.BlockHeader) error {
			callCount++
			if callCount == 1 {
				return errors.NewError("not ready")
			}
			return nil
		},
		setMinedChan: make(chan *chainhash.Hash, 1),
	}

	err := bv.ValidateBlock(ctx, block, "test")
	require.Error(t, err)

	select {
	case h := <-bv.setMinedChan:
		require.Equal(t, parentHash, h, "parent hash should be sent to setMinedChan after first failure")
	default:
		t.Log("expected parent hash to be sent to setMinedChan")
	}

	callCount = 0
	bv.waitFunc = func(ctx context.Context, block *model.Block, headers []*model.BlockHeader) error {
		return errors.NewError("not ready")
	}

	err = bv.ValidateBlock(ctx, block, "test")
	require.Error(t, err)
}

func TestValidateBlock_WaitForPreviousBlocksToBeProcessed_RetryLogic_UOM(t *testing.T) {
	t.Skip("Skipping test with goroutine cleanup issues")
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 2

	coinbaseTx, childTx, _, _ := createCoinbaseAndChildTx(t)
	storeTxsInUtxoStore(t, utxoStore, coinbaseTx, childTx)
	subtree := buildAndStoreSubtree(t, subtreeStore, childTx)

	nBits, _ := model.NewNBitFromString("2000ffff")
	parentBlock, parentHash := createParentBlock(nBits)
	blockHeader := mineBlockHeader(t, parentHash, subtree.RootHash(), nBits)
	totalSize := int64(coinbaseTx.Size()) + int64(childTx.Size())
	block := createBlock(t, blockHeader, coinbaseTx, subtree.RootHash(), subtree, tSettings, totalSize)

	mockBlockchain := setupMockBlockchain(parentBlock)
	callCount := 0
	setMinedChan := make(chan *chainhash.Hash, 1)

	mockHandler := new(MockInvalidBlockHandler)
	mockHandler.On("ReportInvalidBlock", mock.Anything, blockHeader.Hash().String(), mock.Anything).Return(nil).Once()

	tSettings.BlockValidation.OptimisticMining = true
	bv := &testBlockValidation{
		BlockValidation: NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient, mockHandler),
		waitFunc: func(ctx context.Context, block *model.Block, headers []*model.BlockHeader) error {
			callCount++
			if callCount == 1 {
				return errors.NewError("not ready")
			}
			return nil
		},
		setMinedChan: setMinedChan,
	}

	err := bv.ValidateBlock(ctx, block, "test", false)
	require.Error(t, err)

	select {
	case h := <-setMinedChan:
		require.Equal(t, parentHash, h, "parent hash should be sent to setMinedChan after first failure")
	default:
		t.Log("expected parent hash to be sent to setMinedChan")
	}

	callCount = 0
	bv.waitFunc = func(ctx context.Context, block *model.Block, headers []*model.BlockHeader) error {
		return errors.NewError("not ready")
	}

	err = bv.ValidateBlock(ctx, block, "test", false)
	require.Error(t, err)
}

// --- Helper functions for test setup and block/tx construction ---

func createCoinbaseAndChildTx(t *testing.T) (*bt.Tx, *bt.Tx, *bec.PrivateKey, *bscript.Address) {
	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseTx := bt.NewTx()
	err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	require.NoError(t, err)

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	err = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)
	require.NoError(t, err)

	childTx := bt.NewTx()
	err = childTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)
	err = childTx.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000)
	require.NoError(t, err)
	err = childTx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	return coinbaseTx, childTx, privateKey, address
}

func storeTxsInUtxoStore(t *testing.T, utxoStore utxo.Store, coinbaseTx, childTx *bt.Tx, opts ...utxo.CreateOption) {
	_, err := utxoStore.Create(context.Background(), coinbaseTx, 0, opts...)
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), childTx, 0, opts...)
	require.NoError(t, err)
}

func buildAndStoreSubtree(t *testing.T, subtreeStore interface {
	Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...bloboptions.FileOption) error
}, childTx *bt.Tx, opts ...bloboptions.FileOption) *subtreepkg.Subtree {
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*childTx.TxIDChainHash(), uint64(childTx.Size()), 0)) //nolint:gosec

	subtreeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes, opts...)
	require.NoError(t, err)

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)

	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes, opts...)
	require.NoError(t, err)

	return subtree
}

func createParentBlock(nBits *model.NBit) (*model.Block, *chainhash.Hash) {
	parentBlock := &model.Block{
		Header: &model.BlockHeader{
			HashPrevBlock: &chainhash.Hash{},
		},
	}
	parentBlock.Header = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      0,
		Bits:           *nBits,
		Nonce:          0,
	}
	parentHash := parentBlock.Header.Hash()

	return parentBlock, parentHash
}

func mineBlockHeader(t *testing.T, parentHash, merkleRoot *chainhash.Hash, nBits *model.NBit) *model.BlockHeader {
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  parentHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	return blockHeader
}

func createBlock(t *testing.T, header *model.BlockHeader, coinbaseTx *bt.Tx, subtreeRoot *chainhash.Hash, subtree *subtreepkg.Subtree, tSettings *settings.Settings, totalSize int64) *model.Block {
	block, _ := model.NewBlock(
		header,
		coinbaseTx,
		[]*chainhash.Hash{subtreeRoot},
		uint64(subtree.Length()), //nolint:gosec
		uint64(totalSize),        //nolint:gosec
		100, 0,
	)

	return block
}

func setupMockBlockchain(parentBlock *model.Block) *blockchain.Mock {
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).
		Return([]*model.Block{parentBlock}, nil).Times(50)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).
		Return([]*model.Block{}, nil).Times(2)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	subChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(subChan, nil)
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.ErrBlockNotFound)
	mockBlockchain.On("GetBlock", mock.Anything, mock.Anything).Return(nil, errors.ErrBlockNotFound)
	// Mock GetNextWorkRequired for difficulty validation - return any NBit that's passed
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, hash *chainhash.Hash, currentBlockTime ...int64) *model.NBit {
			// Return a default NBit for testing
			nBits, _ := model.NewNBitFromString("2000ffff")
			return nBits
		},
		func(ctx context.Context, hash *chainhash.Hash, currentBlockTime ...int64) error {
			return nil
		},
	)

	return mockBlockchain
}

func TestBlockValidation_ReportsInvalidBlock_OnInvalidBlock_UOM(t *testing.T) {
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)

	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	subtree, _ := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, subtree.AddCoinbaseNode())
	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}
	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	nBits, _ := model.NewNBitFromString("2000ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, _ := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()),  //nolint:gosec
		uint64(coinbaseTx.Size()), //nolint:gosec
		100, 0,
	)

	// make the block invalid
	block.Header.HashMerkleRoot = &chainhash.Hash{}

	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			break
		}

		block.Header.Nonce++
	}

	// Create a channel to signal when InvalidateBlock is called
	invalidateBlockCalled := make(chan struct{})

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("AddBlock", mock.Anything, block, mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1}, nil)
	mockBlockchain.On("InvalidateBlock", mock.Anything, block.Header.Hash()).Return([]chainhash.Hash{}, nil).Run(func(args mock.Arguments) {
		// Signal that InvalidateBlock was called
		close(invalidateBlockCalled)
	})
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	subChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(subChan, nil)
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil)
	// Mock GetNextWorkRequired for difficulty validation
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil)

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	mockHandler := new(MockInvalidBlockHandler)
	mockHandler.On("ReportInvalidBlock", mock.Anything, blockHeader.Hash().String(), mock.Anything).Return(nil).Once()

	// Use our thread-safe mock Kafka producer
	mockKafka := &SafeMockKafkaProducer{}
	mockKafka.On("Start", mock.Anything, mock.Anything).Return()

	callCount := 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings.BlockValidation.OptimisticMining = true
	tSettings.Kafka.InvalidBlocks = "test-invalid-blocks" // Enable Kafka publishing

	bv := &testBlockValidation{
		BlockValidation: NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient, mockHandler),
		waitFunc: func(ctx context.Context, block *model.Block, headers []*model.BlockHeader) error {
			callCount++
			if callCount == 1 {
				return errors.NewError("not ready")
			}

			return nil
		},
	}

	// Inject our mock Kafka producer directly into the BlockValidation struct
	bv.invalidBlockKafkaProducer = mockKafka

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	err = bv.ValidateBlock(ctx, block, "test", false)
	require.NoError(t, err)

	// Wait for the goroutine to call InvalidateBlock
	select {
	case <-invalidateBlockCalled:
		// Successfully received signal that InvalidateBlock was called
	case <-time.After(2 * time.Second):
		t.Fatal("InvalidateBlock should be called in background goroutine")
	}

	// Verify that Publish was called on the Kafka producer
	require.True(t, mockKafka.IsPublishCalled(), "Kafka Publish should be called for invalid block")
}

func TestBlockValidation_ReportsInvalidBlock_OnInvalidBlock(t *testing.T) {
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)

	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	subtree, _ := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, subtree.AddCoinbaseNode())
	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}
	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	nBits, _ := model.NewNBitFromString("2000ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, _ := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()),  //nolint:gosec
		uint64(coinbaseTx.Size()), //nolint:gosec
		100, 0,
	)

	// make the block invalid
	block.Header.HashMerkleRoot = &chainhash.Hash{}

	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			break
		}

		block.Header.Nonce++
	}

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("AddBlock", mock.Anything, block, mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1}, nil)
	mockBlockchain.On("InvalidateBlock", mock.Anything, block.Header.Hash()).Return([]chainhash.Hash{}, nil)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	subChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(subChan, nil)
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil)
	// Mock GetNextWorkRequired for difficulty validation
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil)

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	// bv := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, txMetaStore, subtreeValidationClient)

	mockHandler := new(MockInvalidBlockHandler)
	mockHandler.On("ReportInvalidBlock", mock.Anything, blockHeader.Hash().String(), mock.Anything).Return(nil).Once()

	// Use our thread-safe mock Kafka producer
	mockKafka := &SafeMockKafkaProducer{}
	mockKafka.On("Start", mock.Anything, mock.Anything).Return()

	callCount := 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Enable Kafka publishing
	tSettings.Kafka.InvalidBlocks = "test-invalid-blocks"

	bv := &testBlockValidation{
		BlockValidation: NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient, mockHandler),
		waitFunc: func(ctx context.Context, block *model.Block, headers []*model.BlockHeader) error {
			callCount++
			if callCount == 1 {
				return errors.NewError("not ready")
			}
			return nil
		},
	}

	// Inject our mock Kafka producer directly into the BlockValidation struct
	bv.invalidBlockKafkaProducer = mockKafka

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	err = bv.ValidateBlock(ctx, block, "test")
	require.Error(t, err)
}
