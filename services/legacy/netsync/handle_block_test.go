package netsync

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	subtreepkg "github.com/bitcoin-sv/teranode/pkg/go-subtree"
	txmap "github.com/bitcoin-sv/teranode/pkg/go-tx-map"
	"github.com/bitcoin-sv/teranode/pkg/go-wire"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/legacy/peer"
	"github.com/bitcoin-sv/teranode/services/legacy/testdata"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo/nullstore"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-bitcoin"
	"github.com/ordishs/go-utils/expiringmap"
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
		settings:         test.CreateBaseTestSettings(),
		logger:           ulogger.TestLogger{},
		orphanTxs:        expiringmap.New[chainhash.Hash, *orphanTxAndParents](10),
		blockchainClient: blockchainClient,
		blockAssembly:    blockAssembly,
		utxoStore:        utxoStore,
		validationClient: validationClient,
		subtreeStore:     subtreeStore,
		blockValidation:  blockValidation,
	}

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
