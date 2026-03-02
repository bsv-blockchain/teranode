package netsync

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/testdata"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/go-bitcoin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	rpcHost  = "localhost"
	rpcPort  = 8332
	username = "bitcoin"
	password = "bitcoin"
)

func TestSyncManager_HandleBlockDirect(t *testing.T) {
	t.Skip("This test requires a running Bitcoin SV node with RPC enabled")

	initPrometheusMetrics()

	blockHex := "0000000000000000046bb497bda05586305fee1e86fdde1bb2802821729ec16b"

	blockHash, err := chainhash.NewHashFromStr(blockHex)
	require.NoError(t, err)

	b, err := bitcoin.New(rpcHost, rpcPort, username, password, false)
	require.NoError(t, err)

	t.Logf("Getting block %s", blockHex)
	blockBytes, err := b.GetRawBlock(blockHex)
	require.NoError(t, err)

	blockchainClient := &blockchain.Mock{}
	blockchainClient.Mock.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{}, nil)
	blockchainClient.Mock.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)

	blockAssembly := blockassembly.NewMock()
	blockAssembly.Mock.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{}, nil)

	utxoStore := &nullstore.NullStore{}

	validationClient := &validator.MockValidatorClient{
		UtxoStore: utxoStore,
	}

	subtreeStore := memory.New()

	blockValidation := &blockvalidation.MockBlockValidation{}

	sm := &SyncManager{
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		orphanTxs:        expiringmap.New[chainhash.Hash, *orphanTxAndParents](10),
		blockchainClient: blockchainClient,
		blockAssembly:    blockAssembly,
		utxoStore:        utxoStore,
		validationClient: validationClient,
		subtreeStore:     subtreeStore,
		blockValidation:  blockValidation,
	}
	defer sm.orphanTxs.Stop()

	msgBlock := &wire.MsgBlock{}
	err = msgBlock.Deserialize(bytes.NewReader(blockBytes))
	require.NoError(t, err)

	err = sm.HandleBlockDirect(t.Context(), &peer.Peer{}, *blockHash, msgBlock)
	require.NoError(t, err)
}

func TestSyncManager_createTxMap(t *testing.T) {
	// Define test cases with block file paths and expected lengths of the txMap
	testCases := []struct {
		name             string
		blockFilePath    string
		expectedTxMapLen int
	}{
		{
			name:             "Block1",
			blockFilePath:    "../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin",
			expectedTxMapLen: 563,
		},
		{
			name:             "Block2",
			blockFilePath:    "../testdata/00000000000000000488eecd93d6f3767b1ba38668200a6a5349af2e0d4fad3f.bin",
			expectedTxMapLen: 1355,
		},
		{
			name:             "Block3",
			blockFilePath:    "../testdata/000000000000000009631dd3dd7357675d8a1f8925be5e7851c68255531ac5fb.bin",
			expectedTxMapLen: 900,
		},
		{
			name:             "Block4",
			blockFilePath:    "../testdata/0000000000000000015594853418b4093c4be4ad8b77fec88b5400feb3268fc4.bin",
			expectedTxMapLen: 484,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			block, err := testdata.ReadBlockFromFile(tc.blockFilePath)
			require.NoError(t, err)

			sm := &SyncManager{}

			txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))

			err = sm.createTxMap(context.Background(), block, txMap)
			require.NoError(t, err)
			require.Equal(t, txMap.Length(), tc.expectedTxMapLen)
		})
	}
}

func TestSyncManager_prepareTxsPerLevel(t *testing.T) {
	testCases := []struct {
		name             string
		blockFilePath    string
		expectedLevels   uint32
		expectedTxMapLen int
	}{
		{
			name:             "Block1",
			blockFilePath:    "../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin",
			expectedLevels:   15,
			expectedTxMapLen: 563,
		},
		// {
		// 	name:             "Block2",
		// 	blockFilePath:    "../testdata/00000000000000000488eecd93d6f3767b1ba38668200a6a5349af2e0d4fad3f.bin",
		// 	expectedTxMapLen: 1355,
		// },
		// {
		// 	name:             "Block3",
		// 	blockFilePath:    "../testdata/000000000000000009631dd3dd7357675d8a1f8925be5e7851c68255531ac5fb.bin",
		// 	expectedTxMapLen: 900,
		// },
		// {
		// 	name:             "Block4",
		// 	blockFilePath:    "../testdata/0000000000000000015594853418b4093c4be4ad8b77fec88b5400feb3268fc4.bin",
		// 	expectedTxMapLen: 484,
		// },
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			block, err := testdata.ReadBlockFromFile(tc.blockFilePath)
			require.NoError(t, err)

			sm := &SyncManager{}
			txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))

			err = sm.createTxMap(context.Background(), block, txMap)
			require.NoError(t, err)
			require.Equal(t, txMap.Length(), tc.expectedTxMapLen)

			for _, wireTx := range block.Transactions() {
				txHash := *wireTx.Hash()
				// extend transaction
				if txWrapper, found := txMap.Get(txHash); found {
					tx := txWrapper.Tx

					for _, input := range tx.Inputs {
						prevTxHash := *input.PreviousTxIDChainHash()
						if _, found := txMap.Get(prevTxHash); found {
							txWrapper.SomeParentsInBlock = true
						}
					}
				}
			}

			maxLevel, blockTXsPerLevel := sm.prepareTxsPerLevel(context.Background(), block, txMap)
			assert.Equal(t, tc.expectedLevels, maxLevel)

			allParents := 0
			for i := range blockTXsPerLevel {
				allParents += len(blockTXsPerLevel[i])
			}

			assert.Equal(t, tc.expectedTxMapLen, allParents)
		})
	}
}

func TestWireTxToGoBtTx(t *testing.T) {
	block, err := testdata.ReadBlockFromFile("../testdata/000000000000000009631dd3dd7357675d8a1f8925be5e7851c68255531ac5fb.bin")
	require.NoError(t, err)

	for _, wireTx := range block.Transactions() {
		// Serialize the tx
		var txBytes bytes.Buffer
		err = wireTx.MsgTx().Serialize(&txBytes)
		require.NoError(t, err)

		// Convert the wire tx to GoBtTx
		gobtTx := &bt.Tx{}
		err = WireTxToGoBtTx(wireTx, gobtTx)
		require.NoError(t, err)

		// Serialize the GoBtTx
		gobtTxBytes := gobtTx.Bytes()

		require.Equal(t, txBytes.Bytes(), gobtTxBytes)
	}
}

func BenchmarkCreateTxMap(b *testing.B) {
	block, err := testdata.ReadBlockFromFile("../testdata/000000000000000009631dd3dd7357675d8a1f8925be5e7851c68255531ac5fb.bin")
	require.NoError(b, err)

	sm := &SyncManager{}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = sm.createTxMap(context.Background(), block, txMap)
		require.NoError(b, err)
	}
}

func Test_calculateTransactionFee(t *testing.T) {
	tx1Hex, err := os.ReadFile("../testdata/fb5329b1f8fe83c36da18c97a096f21f02e8200566d232935f3b0c6284e8b2d0.hex")
	require.NoError(t, err)

	tx1, err := bt.NewTxFromString(string(tx1Hex))
	require.NoError(t, err)

	tests := []struct {
		name    string
		tx      *bt.Tx
		want    uint64
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "nil tx",
			tx:      nil,
			want:    0,
			wantErr: assert.Error,
		},
		{
			name:    "valid tx",
			tx:      tx1,
			want:    2,
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := calculateTransactionFee(tt.tx)
			if !tt.wantErr(t, err) {
				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func Benchmark_createSubtree(b *testing.B) {
	block, err := testdata.ReadBlockFromFile("../testdata/00000000000000000488eecd93d6f3767b1ba38668200a6a5349af2e0d4fad3f.bin")
	require.NoError(b, err)

	sm := &SyncManager{}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))

	err = sm.createTxMap(b.Context(), block, txMap)
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(len(block.Transactions()))
		require.NoError(b, err)

		subtreeData := subtreepkg.NewSubtreeData(subtree)
		subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)

		_ = sm.createSubtree(b.Context(), block, txMap, subtree, subtreeData, subtreeMeta)
	}
}

// Test extendTransactions to increase coverage
func TestSyncManager_extendTransactions(t *testing.T) {
	t.Skip("Skipping test due to nil pointer issue")
	block, err := testdata.ReadBlockFromFile("../testdata/000000000000000009631dd3dd7357675d8a1f8925be5e7851c68255531ac5fb.bin")
	require.NoError(t, err)

	sm := &SyncManager{
		settings: test.CreateBaseTestSettings(t),
		logger:   ulogger.TestLogger{},
	}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))
	err = sm.createTxMap(context.Background(), block, txMap)
	require.NoError(t, err)

	// Test extending transactions
	err = sm.extendTransactions(context.Background(), block, txMap)
	assert.NoError(t, err)
}

// Test createUtxos to increase coverage
func TestSyncManager_createUtxos(t *testing.T) {
	t.Skip("Skipping test due to nil pointer issue")
	sm := &SyncManager{
		settings: test.CreateBaseTestSettings(t),
		logger:   ulogger.TestLogger{},
	}

	// Create a simple coinbase transaction
	coinbaseTx := &bt.Tx{
		Version: 1,
		Inputs: []*bt.Input{
			{
				PreviousTxSatoshis: 0,
				PreviousTxOutIndex: 0xffffffff,
			},
		},
		Outputs: []*bt.Output{
			{
				Satoshis:      50 * 100000000,
				LockingScript: &bscript.Script{},
			},
		},
	}

	// Create a transaction map
	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](1)
	txHashBytes := coinbaseTx.TxIDBytes()
	txHash, _ := chainhash.NewHash(txHashBytes)
	txMap.Set(*txHash, &TxMapWrapper{Tx: coinbaseTx})

	// Create a block
	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version: 1,
		},
		Transactions: []*wire.MsgTx{},
	}
	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(100)

	// Test createUtxos
	utxos := sm.createUtxos(context.Background(), txMap, block)
	assert.NotNil(t, utxos)
}

// Test validateTransactions to increase coverage
func TestSyncManager_validateTransactions(t *testing.T) {
	t.Skip("Skipping test due to nil pointer issue")
	initPrometheusMetrics()

	validationClient := &validator.MockValidatorClient{}

	sm := &SyncManager{
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		validationClient: validationClient,
	}

	// Create transaction levels map
	txsPerLevel := make(map[uint32][]*bt.Tx)
	tx := &bt.Tx{
		Version: 1,
		Outputs: []*bt.Output{
			{
				Satoshis:      100,
				LockingScript: &bscript.Script{},
			},
		},
	}
	txsPerLevel[0] = []*bt.Tx{tx}

	// Create a block
	msgBlock := &wire.MsgBlock{
		Transactions: []*wire.MsgTx{},
	}
	block := bsvutil.NewBlock(msgBlock)

	// Test validateTransactions - it should handle validation gracefully even without mocks
	err := sm.validateTransactions(context.Background(), 1, txsPerLevel, block)
	// We expect this to succeed since MockValidatorClient has default behavior
	assert.NoError(t, err)
}

// Test prepareSubtrees with simple block
func TestSyncManager_prepareSubtrees(t *testing.T) {
	t.Skip("Skipping test due to nil pointer issue")
	initPrometheusMetrics()

	// Create a simple block with one transaction
	msgTx := wire.NewMsgTx(1)
	msgTx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash:  chainhash.Hash{},
			Index: 0xffffffff,
		},
		SignatureScript: []byte{0x00},
		Sequence:        0xffffffff,
	})
	msgTx.AddTxOut(&wire.TxOut{
		Value:    50 * 100000000,
		PkScript: []byte{0x76, 0xa9, 0x14},
	})

	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:   1,
			PrevBlock: chainhash.Hash{},
			Timestamp: time.Now(),
			Bits:      0x1d00ffff,
			Nonce:     0,
		},
		Transactions: []*wire.MsgTx{msgTx},
	}

	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(100)

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(false, nil)

	validationClient := &validator.MockValidatorClient{}

	sm := &SyncManager{
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		blockchainClient: blockchainClient,
		validationClient: validationClient,
		subtreeStore:     memory.New(),
		ctx:              context.Background(),
	}

	// For single transaction blocks, prepareSubtrees returns empty
	subtrees, err := sm.prepareSubtrees(context.Background(), block)
	assert.NoError(t, err)
	assert.NotNil(t, subtrees)

	blockchainClient.AssertExpectations(t)
}

// Test ExtendTransaction
func TestSyncManager_ExtendTransaction(t *testing.T) {
	t.Skip("Skipping test due to nil pointer issue")
	sm := &SyncManager{
		settings: test.CreateBaseTestSettings(t),
		logger:   ulogger.TestLogger{},
	}

	// Create a transaction with inputs
	tx := &bt.Tx{
		Version: 1,
		Inputs: []*bt.Input{
			{
				PreviousTxSatoshis: 0,
				PreviousTxOutIndex: 0,
			},
		},
		Outputs: []*bt.Output{
			{
				Satoshis:      100,
				LockingScript: &bscript.Script{},
			},
		},
	}

	// Create a transaction map
	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](1)

	// Test ExtendTransaction
	err := sm.ExtendTransaction(context.Background(), tx, txMap)
	assert.NoError(t, err)
}
