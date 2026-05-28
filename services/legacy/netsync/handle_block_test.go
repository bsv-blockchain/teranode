package netsync

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
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
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/go-bitcoin"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	t.Skip("This test requires a running SV Node with RPC enabled")

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

// TestSyncManager_createSubtrees_MultiSubtreeDistribution exercises the
// multi-subtree fill path: a 6-tx block (1 coinbase + 5 regular) partitioned
// per the [4, 2] shape lands as subtree 0 = coinbase placeholder + 3 regular
// (Length 4, complete) and subtree 1 = 2 regular (Length 2, complete).
func TestSyncManager_createSubtrees_MultiSubtreeDistribution(t *testing.T) {
	initPrometheusMetrics()

	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:   1,
			PrevBlock: chainhash.Hash{},
			Timestamp: time.Now(),
			Bits:      0x1d00ffff,
			Nonce:     0,
		},
	}

	coinbaseMsgTx := wire.NewMsgTx(1)
	coinbaseMsgTx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbaseMsgTx.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})
	msgBlock.Transactions = append(msgBlock.Transactions, coinbaseMsgTx)

	parentHash := chainhash.Hash{0x01}

	for i := 0; i < 5; i++ {
		regularMsgTx := wire.NewMsgTx(1)
		regularMsgTx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: parentHash, Index: uint32(i)},
			SignatureScript:  []byte{0x00, byte(i)},
			Sequence:         0xffffffff,
		})
		regularMsgTx.AddTxOut(&wire.TxOut{Value: 1000 + int64(i), PkScript: []byte{0x76, 0xa9, 0x14, byte(i)}})
		msgBlock.Transactions = append(msgBlock.Transactions, regularMsgTx)
	}

	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(100)

	require.Equal(t, 6, len(block.Transactions()))

	sm := &SyncManager{logger: ulogger.TestLogger{}}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))
	require.NoError(t, sm.createTxMap(context.Background(), block, txMap))
	require.Equal(t, 5, txMap.Length(), "createTxMap should skip the coinbase")

	for _, wrapper := range txMap.Range() {
		for _, in := range wrapper.Tx.Inputs {
			in.PreviousTxSatoshis = 5_000
			in.PreviousTxScript = &bscript.Script{0x76, 0xa9, 0x14}
		}
	}

	subtreeSize, numSubtrees, finalLeafCount, err := partitionLegacyBlock(len(block.Transactions()), 4)
	require.NoError(t, err)
	require.Equal(t, 4, subtreeSize)
	require.Equal(t, 2, numSubtrees)
	require.Equal(t, 2, finalLeafCount)

	subtreeSlices := make([]*subtreepkg.Subtree, numSubtrees)
	subtreeDatas := make([]*subtreepkg.Data, numSubtrees)
	subtreeMetas := make([]*subtreepkg.Meta, numSubtrees)

	for i := 0; i < numSubtrees; i++ {
		capacity := subtreeSize
		if i == numSubtrees-1 && finalLeafCount < subtreeSize {
			capacity = finalLeafCount
		}

		st, terr := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
		require.NoError(t, terr)

		if i == 0 {
			require.NoError(t, st.AddCoinbaseNode())
		}

		subtreeSlices[i] = st
		subtreeDatas[i] = subtreepkg.NewSubtreeData(st)
		subtreeMetas[i] = subtreepkg.NewSubtreeMeta(st)
	}

	require.NoError(t, sm.createSubtrees(context.Background(), block, txMap, subtreeSlices, subtreeDatas, subtreeMetas))

	require.Equal(t, 4, subtreeSlices[0].Length(), "subtree 0 should hold coinbase + 3 regular txs")
	require.True(t, subtreeSlices[0].IsComplete())
	require.Equal(t, 2, subtreeSlices[1].Length(), "subtree 1 should hold 2 regular txs")
	require.True(t, subtreeSlices[1].IsComplete())
}

func Benchmark_createSubtrees(b *testing.B) {
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

		_ = sm.createSubtrees(b.Context(), block, txMap,
			[]*subtreepkg.Subtree{subtree},
			[]*subtreepkg.Data{subtreeData},
			[]*subtreepkg.Meta{subtreeMeta},
		)
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
	utxos := sm.createUtxos(context.Background(), txMap, block, 0)
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
	subtrees, blockID, err := sm.prepareSubtrees(context.Background(), block)
	assert.NoError(t, err)
	assert.NotNil(t, subtrees)
	assert.Equal(t, uint32(0), blockID) // single-tx block exits early, IsFSMCurrentState=false → blockID stays 0

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

// buildOOBFixture constructs a parent (2 outputs) and a child whose only input
// references PreviousTxOutIndex == 5, plus a txMap containing both. Shared by
// the ExtendTransaction and extendFromTxMap regression tests for issue #4564.
func buildOOBFixture(t *testing.T) (*chainhash.Hash, *bt.Tx, *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) {
	t.Helper()

	parentScript := &bscript.Script{}
	parent := &bt.Tx{
		Version: 1,
		Inputs:  []*bt.Input{},
		Outputs: []*bt.Output{
			{Satoshis: 100, LockingScript: parentScript},
			{Satoshis: 200, LockingScript: parentScript},
		},
	}
	parent.SetExtended(true)
	parentHash := parent.TxIDChainHash()

	child := &bt.Tx{
		Version: 1,
		Inputs: []*bt.Input{
			{
				UnlockingScript:    &bscript.Script{},
				PreviousTxOutIndex: 5,
			},
		},
		Outputs: []*bt.Output{
			{Satoshis: 50, LockingScript: &bscript.Script{}},
		},
	}
	require.NoError(t, child.Inputs[0].PreviousTxIDAdd(parentHash))

	// No size limit: callers (e.g. the _NilParentTx test) re-Set an existing
	// key, and txmap.SyncedMap evicts a random entry on update once len == limit,
	// which can drop the child and turn the test into a flake.
	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper]()
	txMap.Set(*parentHash, &TxMapWrapper{Tx: parent})
	txMap.Set(*child.TxIDChainHash(), &TxMapWrapper{Tx: child})

	return parentHash, child, txMap
}

// TestSyncManager_ExtendTransaction_OOB verifies that ExtendTransaction returns
// a TxInvalidError (rather than panicking) when a child input references a
// parent output index that exceeds the parent's number of outputs. Regression
// test for issue #4564.
func TestSyncManager_ExtendTransaction_OOB(t *testing.T) {
	initPrometheusMetrics()

	sm := &SyncManager{
		settings: test.CreateBaseTestSettings(t),
		logger:   ulogger.TestLogger{},
	}

	parentHash, child, txMap := buildOOBFixture(t)

	err := sm.ExtendTransaction(context.Background(), child, txMap)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxInvalid), "expected TxInvalid error, got %v", err)
	require.Contains(t, err.Error(), parentHash.String())
}

// TestSyncManager_extendFromTxMap_OOB verifies the same OOB guard on the
// same-block phase-1 path. Regression test for issue #4564.
func TestSyncManager_extendFromTxMap_OOB(t *testing.T) {
	initPrometheusMetrics()

	sm := &SyncManager{
		settings: test.CreateBaseTestSettings(t),
		logger:   ulogger.TestLogger{},
	}

	parentHash, child, txMap := buildOOBFixture(t)

	err := sm.extendFromTxMap(context.Background(), child, txMap)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxInvalid), "expected TxInvalid error, got %v", err)
	require.Contains(t, err.Error(), parentHash.String())
}

// buildInRangeFixture constructs a parent (2 outputs) and a child whose only
// input references PreviousTxOutIndex == 0 (in range), plus a txMap containing
// both. Used by the nil-deref regression tests so the bounds-check passes and
// the nil-check is the one that must fire.
func buildInRangeFixture(t *testing.T) (*chainhash.Hash, *bt.Tx, *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) {
	t.Helper()

	parentScript := &bscript.Script{}
	parent := &bt.Tx{
		Version: 1,
		Inputs:  []*bt.Input{},
		Outputs: []*bt.Output{
			{Satoshis: 100, LockingScript: parentScript},
			{Satoshis: 200, LockingScript: parentScript},
		},
	}
	parent.SetExtended(true)
	parentHash := parent.TxIDChainHash()

	child := &bt.Tx{
		Version: 1,
		Inputs: []*bt.Input{
			{
				UnlockingScript:    &bscript.Script{},
				PreviousTxOutIndex: 0,
			},
		},
		Outputs: []*bt.Output{
			{Satoshis: 50, LockingScript: &bscript.Script{}},
		},
	}
	require.NoError(t, child.Inputs[0].PreviousTxIDAdd(parentHash))

	// No size limit: callers (e.g. the NilParentTx tests) re-Set entries on this
	// map, and SyncedMap's limit semantics evict a *random* key when len >= limit,
	// which would non-deterministically drop the child and turn the test flaky.
	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper]()
	txMap.Set(*parentHash, &TxMapWrapper{Tx: parent})
	txMap.Set(*child.TxIDChainHash(), &TxMapWrapper{Tx: child})

	return parentHash, child, txMap
}

// TestSyncManager_extendFromTxMap_NilParentTx verifies that extendFromTxMap
// returns a TxInvalidError (rather than panicking) when the parent's
// TxMapWrapper carries a nil Tx pointer.
func TestSyncManager_extendFromTxMap_NilParentTx(t *testing.T) {
	initPrometheusMetrics()

	sm := &SyncManager{
		settings: test.CreateBaseTestSettings(t),
		logger:   ulogger.TestLogger{},
	}

	parentHash, child, txMap := buildInRangeFixture(t)
	// Replace the parent wrapper with one that has a nil Tx — the child's hash
	// is already keyed before this mutation, so the child lookup still succeeds.
	txMap.Set(*parentHash, &TxMapWrapper{Tx: nil})

	err := sm.extendFromTxMap(context.Background(), child, txMap)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxInvalid), "expected TxInvalid error, got %v", err)
	require.Contains(t, err.Error(), "missing previous transaction")
	require.Contains(t, err.Error(), parentHash.String())
}

// TestSyncManager_extendFromTxMap_NilOutput verifies that extendFromTxMap
// returns a TxInvalidError when the referenced parent output is nil.
func TestSyncManager_extendFromTxMap_NilOutput(t *testing.T) {
	initPrometheusMetrics()

	sm := &SyncManager{
		settings: test.CreateBaseTestSettings(t),
		logger:   ulogger.TestLogger{},
	}

	parentHash, child, txMap := buildInRangeFixture(t)
	parentWrapper, _ := txMap.Get(*parentHash)
	parentWrapper.Tx.Outputs[0] = nil

	err := sm.extendFromTxMap(context.Background(), child, txMap)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxInvalid), "expected TxInvalid error, got %v", err)
	require.Contains(t, err.Error(), "nil or has nil locking script")
}

// TestSyncManager_extendFromTxMap_NilLockingScript verifies that extendFromTxMap
// returns a TxInvalidError when the referenced parent output's locking script
// is nil (which would otherwise panic on the deref into bscript.NewFromBytes).
func TestSyncManager_extendFromTxMap_NilLockingScript(t *testing.T) {
	initPrometheusMetrics()

	sm := &SyncManager{
		settings: test.CreateBaseTestSettings(t),
		logger:   ulogger.TestLogger{},
	}

	parentHash, child, txMap := buildInRangeFixture(t)
	parentWrapper, _ := txMap.Get(*parentHash)
	parentWrapper.Tx.Outputs[0].LockingScript = nil

	err := sm.extendFromTxMap(context.Background(), child, txMap)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxInvalid), "expected TxInvalid error, got %v", err)
	require.Contains(t, err.Error(), "nil or has nil locking script")
}

// TestSyncManager_ExtendTransaction_NilParentTx mirrors the same guard on the
// parallel-decoration path used by ExtendTransaction.
func TestSyncManager_ExtendTransaction_NilParentTx(t *testing.T) {
	initPrometheusMetrics()

	sm := &SyncManager{
		settings: test.CreateBaseTestSettings(t),
		logger:   ulogger.TestLogger{},
	}

	parentHash, child, txMap := buildInRangeFixture(t)
	txMap.Set(*parentHash, &TxMapWrapper{Tx: nil})

	err := sm.ExtendTransaction(context.Background(), child, txMap)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxInvalid), "expected TxInvalid error, got %v", err)
	require.Contains(t, err.Error(), parentHash.String())
}

// countingValidator tracks how many times Validate is called and optionally fails
// the first N calls. It checks context cancellation to detect cascade behavior.
type countingValidator struct {
	validator.MockValidator
	callCount      atomic.Int64
	failFirst      int
	failErr        error
	mu             sync.Mutex
	ctxCancelCount atomic.Int64 // tracks how many calls saw a cancelled context
}

func (v *countingValidator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...validator.Option) (*meta.Data, error) {
	if ctx.Err() != nil {
		v.ctxCancelCount.Add(1)
		return nil, ctx.Err()
	}

	callNum := int(v.callCount.Add(1))

	v.mu.Lock()
	shouldFail := callNum <= v.failFirst
	v.mu.Unlock()

	if shouldFail {
		return nil, v.failErr
	}

	return &meta.Data{}, nil
}

func (v *countingValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *validator.Options) (*meta.Data, error) {
	return v.Validate(ctx, tx, blockHeight)
}

func (v *countingValidator) TriggerBatcher() {}

func makeTxMap(t *testing.T, count int) *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper] {
	t.Helper()
	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](count)

	for i := 0; i < count; i++ {
		tx := bt.NewTx()
		tx.Version = 1
		_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(1000+i))
		txHash := chainhash.HashH([]byte(fmt.Sprintf("test-tx-%d", i)))
		txMap.Set(txHash, &TxMapWrapper{Tx: tx})
	}

	return txMap
}

func TestWaitForPreviousBlockMined(t *testing.T) {
	t.Run("returns immediately when parent is mined", func(t *testing.T) {
		blockchainClient := &blockchain.Mock{}
		prevHash := chainhash.HashH([]byte("prev-block"))
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(true, nil)

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 3
		tSettings.BlockValidation.IsParentMinedRetryBackoffMultiplier = 1
		tSettings.BlockValidation.IsParentMinedRetryBackoffDuration = time.Millisecond

		sm := &SyncManager{
			settings:         tSettings,
			logger:           ulogger.TestLogger{},
			blockchainClient: blockchainClient,
		}

		err := sm.waitForPreviousBlockMined(context.Background(), &prevHash, 100)
		require.NoError(t, err)
		blockchainClient.AssertNumberOfCalls(t, "GetBlockIsMined", 1)
	})

	t.Run("retries when parent is not mined yet then succeeds", func(t *testing.T) {
		blockchainClient := &blockchain.Mock{}
		prevHash := chainhash.HashH([]byte("prev-block"))

		// First two calls: not mined. Third call: mined.
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(false, nil).Times(2)
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(true, nil).Once()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 5
		tSettings.BlockValidation.IsParentMinedRetryBackoffMultiplier = 1
		tSettings.BlockValidation.IsParentMinedRetryBackoffDuration = time.Millisecond

		sm := &SyncManager{
			settings:         tSettings,
			logger:           ulogger.TestLogger{},
			blockchainClient: blockchainClient,
		}

		err := sm.waitForPreviousBlockMined(context.Background(), &prevHash, 100)
		require.NoError(t, err)
		blockchainClient.AssertNumberOfCalls(t, "GetBlockIsMined", 3)
	})

	t.Run("retries on ErrBlockNotFound then succeeds", func(t *testing.T) {
		blockchainClient := &blockchain.Mock{}
		prevHash := chainhash.HashH([]byte("prev-block"))

		// First call: block not found. Second call: mined.
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(false, errors.ErrBlockNotFound).Once()
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(true, nil).Once()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 5
		tSettings.BlockValidation.IsParentMinedRetryBackoffMultiplier = 1
		tSettings.BlockValidation.IsParentMinedRetryBackoffDuration = time.Millisecond

		sm := &SyncManager{
			settings:         tSettings,
			logger:           ulogger.TestLogger{},
			blockchainClient: blockchainClient,
		}

		err := sm.waitForPreviousBlockMined(context.Background(), &prevHash, 100)
		require.NoError(t, err)
		blockchainClient.AssertNumberOfCalls(t, "GetBlockIsMined", 2)
	})

	t.Run("fails after max retries exhausted", func(t *testing.T) {
		blockchainClient := &blockchain.Mock{}
		prevHash := chainhash.HashH([]byte("prev-block"))
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(false, nil)

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 2
		tSettings.BlockValidation.IsParentMinedRetryBackoffMultiplier = 1
		tSettings.BlockValidation.IsParentMinedRetryBackoffDuration = time.Millisecond

		sm := &SyncManager{
			settings:         tSettings,
			logger:           ulogger.TestLogger{},
			blockchainClient: blockchainClient,
		}

		err := sm.waitForPreviousBlockMined(context.Background(), &prevHash, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not mined yet")
	})
}

func TestPreValidateTransactions_AllSucceed(t *testing.T) {
	initPrometheusMetrics()

	cv := &countingValidator{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.SpendBatcherSize = 2
	tSettings.Legacy.SpendBatcherConcurrency = 2

	sm := &SyncManager{
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
		validationClient: cv,
	}

	txMap := makeTxMap(t, 10)

	err := sm.PreValidateTransactions(context.Background(), txMap, chainhash.Hash{}, 100, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(10), cv.callCount.Load(), "all 10 transactions should be validated")
}

func TestPreValidateTransactions_PartialFailure_RetriesSucceed(t *testing.T) {
	initPrometheusMetrics()

	// Fail the first 3 calls, succeed the rest. On retry, those 3 txs will be
	// retried and succeed (callCount > failFirst), so the block should pass.
	cv := &countingValidator{
		failFirst: 3,
		failErr:   errors.NewStorageError("DEVICE_OVERLOAD"),
	}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.SpendBatcherSize = 1
	tSettings.Legacy.SpendBatcherConcurrency = 1

	sm := &SyncManager{
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
		validationClient: cv,
	}

	txMap := makeTxMap(t, 10)

	err := sm.PreValidateTransactions(context.Background(), txMap, chainhash.Hash{}, 100, 0, 0)
	require.NoError(t, err, "should succeed after retrying the 3 failed transactions")

	// 10 in first pass + 3 retried = 13 total calls
	assert.Equal(t, int64(13), cv.callCount.Load())
	assert.Equal(t, int64(0), cv.ctxCancelCount.Load(), "no calls should have seen a cancelled context")
}

func TestPreValidateTransactions_AllFail_NoProgress_GivesUp(t *testing.T) {
	initPrometheusMetrics()

	cv := &countingValidator{
		failFirst: 100000, // always fail
		failErr:   errors.NewStorageError("DEVICE_OVERLOAD"),
	}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.SpendBatcherSize = 2
	tSettings.Legacy.SpendBatcherConcurrency = 2

	sm := &SyncManager{
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
		validationClient: cv,
	}

	txMap := makeTxMap(t, 5)

	err := sm.PreValidateTransactions(context.Background(), txMap, chainhash.Hash{}, 100, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no progress")

	// First pass (5) + one retry attempt (5) = 10, then gives up on no progress
	assert.Equal(t, int64(10), cv.callCount.Load())
}

func TestPreValidateTransactions_NonRetryableError_FailsImmediately(t *testing.T) {
	initPrometheusMetrics()

	// A non-retryable error (e.g. double-spend) should not be retried
	cv := &countingValidator{
		failFirst: 1,
		failErr:   errors.NewUtxoFrozenError("utxo is frozen"),
	}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.SpendBatcherSize = 1
	tSettings.Legacy.SpendBatcherConcurrency = 1

	sm := &SyncManager{
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
		validationClient: cv,
	}

	txMap := makeTxMap(t, 5)

	err := sm.PreValidateTransactions(context.Background(), txMap, chainhash.Hash{}, 100, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-retryable")

	// All 5 should run (no cascade), but no retry should happen
	assert.Equal(t, int64(5), cv.callCount.Load())
}

func TestPreValidateTransactions_ParentContextCancelled(t *testing.T) {
	initPrometheusMetrics()

	slowValidator := &countingValidator{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.SpendBatcherSize = 1
	tSettings.Legacy.SpendBatcherConcurrency = 1

	sm := &SyncManager{
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
		validationClient: slowValidator,
	}

	txMap := makeTxMap(t, 3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := sm.PreValidateTransactions(ctx, txMap, chainhash.Hash{}, 100, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context cancelled")
}

// TestSyncManager_createUtxos_MergesBlockIDsForExistingTxs verifies that when a tx
// already exists in the utxo store (e.g. created by an earlier crashed attempt or by
// the propagation path) createUtxos merges the current block's ID into the existing
// record's BlockIDs instead of silently dropping it. Without the merge, the next
// block's validOrderAndBlessed check fails with "has no block IDs" in
// model/Block.go getParentTxMetaBlockIDs.
func TestSyncManager_createUtxos_MergesBlockIDsForExistingTxs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///test_create_utxos_merge")
	require.NoError(t, err)

	utxoStore, err := utxosql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	// Build a real, signable-shaped tx without inputs (parent placeholder).
	tx := bt.NewTx()
	tx.Version = 1
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	txHash := *tx.TxIDChainHash()

	// Pre-create the tx in the store WITHOUT any MinedBlockInfo. This simulates
	// the state after a slow-path subtreeValidation run (or propagation arrival
	// before the block) that lands the tx with empty BlockIDs.
	_, err = utxoStore.Create(ctx, tx, 100)
	require.NoError(t, err)

	pre, err := utxoStore.Get(ctx, &txHash, fields.BlockIDs)
	require.NoError(t, err)
	require.Empty(t, pre.BlockIDs, "tx should start with empty BlockIDs to reproduce the bug")

	// Wire up a SyncManager just enough for createUtxos. createUtxos only
	// touches utxoStore, settings, logger and the txMap — no need for full DI.
	sm := &SyncManager{
		settings:  tSettings,
		logger:    logger,
		utxoStore: utxoStore,
	}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](1)
	txMap.Set(txHash, &TxMapWrapper{Tx: tx})

	block := bsvutil.NewBlock(&wire.MsgBlock{Header: wire.BlockHeader{Version: 1}})
	block.SetHeight(100)

	const expectedBlockID uint32 = 42
	require.NoError(t, sm.createUtxos(ctx, txMap, block, expectedBlockID))

	post, err := utxoStore.Get(ctx, &txHash, fields.BlockIDs)
	require.NoError(t, err)
	require.Contains(t, post.BlockIDs, expectedBlockID,
		"createUtxos must merge blockID %d into the pre-existing tx", expectedBlockID)
}

// newChunkingTestSetup builds the boilerplate shared by the createUtxos chunking
// tests: a SyncManager wired to a MockUtxostore that returns ErrTxExists for every
// Create call (so every tx routes through the merge path), a populated txMap and
// hashes slice, and a block at height 100. Callers wire up SetMinedMulti
// expectations on the returned mock to drive the scenario under test.
func newChunkingTestSetup(t *testing.T, totalTxs, batchSize, routines int) (
	*SyncManager,
	*txmap.SyncedMap[chainhash.Hash, *TxMapWrapper],
	*bsvutil.Block,
	*utxo.MockUtxostore,
	[]chainhash.Hash,
) {
	t.Helper()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.MaxMinedBatchSize = batchSize
	tSettings.UtxoStore.MaxMinedRoutines = routines

	mockStore := &utxo.MockUtxostore{}

	txs := make([]*bt.Tx, totalTxs)
	hashes := make([]chainhash.Hash, totalTxs)
	for i := 0; i < totalTxs; i++ {
		tx := bt.NewTx()
		tx.Version = 1
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(1000+i)))
		txs[i] = tx
		hashes[i] = *tx.TxIDChainHash()
	}

	// Every Create returns ErrTxExists so every hash flows into existingTxHashes
	// and the merge path executes — that's what we're exercising.
	mockStore.On("Create",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return((*meta.Data)(nil), errors.ErrTxExists)

	sm := &SyncManager{
		settings:  tSettings,
		logger:    logger,
		utxoStore: mockStore,
	}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](totalTxs)
	for i, h := range hashes {
		txMap.Set(h, &TxMapWrapper{Tx: txs[i]})
	}

	block := bsvutil.NewBlock(&wire.MsgBlock{Header: wire.BlockHeader{Version: 1}})
	block.SetHeight(100)

	return sm, txMap, block, mockStore, hashes
}

// recordChunksOnMock installs a SetMinedMulti expectation that records the hash
// slice passed on every call into the supplied accumulator (caller owns the mutex).
// Returns nil error for every call.
func recordChunksOnMock(mockStore *utxo.MockUtxostore, callMu *sync.Mutex, callChunks *[][]chainhash.Hash) {
	mockStore.On("SetMinedMulti",
		mock.Anything, mock.Anything, mock.Anything,
	).Run(func(args mock.Arguments) {
		chunk := args.Get(1).([]*chainhash.Hash)
		copied := make([]chainhash.Hash, len(chunk))
		for i, h := range chunk {
			copied[i] = *h
		}
		callMu.Lock()
		*callChunks = append(*callChunks, copied)
		callMu.Unlock()
	}).Return(map[chainhash.Hash][]uint32{}, nil)
}

// assertUnionCovers asserts that the union of every recorded chunk exactly equals
// the supplied hash set (no drops, no duplicates).
func assertUnionCovers(t *testing.T, callChunks [][]chainhash.Hash, hashes []chainhash.Hash) {
	t.Helper()
	seen := make(map[chainhash.Hash]int, len(hashes))
	for _, chunk := range callChunks {
		for _, h := range chunk {
			seen[h]++
		}
	}
	require.Len(t, seen, len(hashes), "expected %d unique hashes across chunks, got %d", len(hashes), len(seen))
	for _, h := range hashes {
		require.Equalf(t, 1, seen[h], "hash %s missing or duplicated across chunks", h)
	}
}

// TestSyncManager_createUtxos_ChunksExistingTxs verifies that when many pre-existing
// transactions need their blockID merged, createUtxos splits the merge across multiple
// SetMinedMulti calls bounded by MaxMinedBatchSize, instead of submitting a single
// monolithic slice that exhausts the aerospike client connection pool. See issue #936.
func TestSyncManager_createUtxos_ChunksExistingTxs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const totalTxs = 10 // 2 workers × ceil(5/4) chunks each = 4 chunks of sizes (4, 1, 4, 1)
	sm, txMap, block, mockStore, hashes := newChunkingTestSetup(t, totalTxs, 4, 2)

	var (
		callMu     sync.Mutex
		callChunks [][]chainhash.Hash
	)
	recordChunksOnMock(mockStore, &callMu, &callChunks)

	require.NoError(t, sm.createUtxos(ctx, txMap, block, 42))

	// Assert 1: at least 2 calls (proves chunking happens).
	require.GreaterOrEqual(t, len(callChunks), 2,
		"expected multiple SetMinedMulti calls; got %d (monolithic call regression)", len(callChunks))

	// Assert 2: every chunk respects MaxMinedBatchSize.
	for i, chunk := range callChunks {
		require.LessOrEqual(t, len(chunk), sm.settings.UtxoStore.MaxMinedBatchSize,
			"chunk %d size %d exceeds MaxMinedBatchSize=%d", i, len(chunk), sm.settings.UtxoStore.MaxMinedBatchSize)
	}

	// Assert 3: union of all chunks equals the input set (no duplicates, no drops).
	assertUnionCovers(t, callChunks, hashes)
}

// TestSyncManager_createUtxos_ChunkErrorReturnsWrappedProcessingError verifies that
// a failing chunked SetMinedMulti causes createUtxos to return a wrapped
// ProcessingError whose message carries the count of pre-existing txs that the
// merge attempted to mark. Short-circuit semantics are covered separately by
// TestSyncManager_createUtxos_ChunkFailureCancelsSiblings.
func TestSyncManager_createUtxos_ChunkErrorReturnsWrappedProcessingError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const totalTxs = 20
	sm, txMap, block, mockStore, _ := newChunkingTestSetup(t, totalTxs, 4, 2)

	// Every SetMinedMulti call fails. We only care that the error round-trips
	// through createUtxos wrapped as a ProcessingError carrying the tx count.
	mockStore.On("SetMinedMulti",
		mock.Anything, mock.Anything, mock.Anything,
	).Return(
		map[chainhash.Hash][]uint32{},
		errors.NewStorageError("synthetic chunk failure"),
	)

	err := sm.createUtxos(ctx, txMap, block, 42)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to merge blockID into 20 pre-existing txs",
		"expected wrapped ProcessingError, got: %v", err)
}

// TestSyncManager_createUtxos_ChunkFailureCancelsSiblings proves the
// `if mergeCtx.Err() != nil { return mergeCtx.Err() }` short-circuit at the top
// of each worker's inner loop actually suppresses sibling iterations after a
// chunk fails.
//
// Why this design (vs orchestrating worker identity): worker A vs worker B
// identity at runtime is not predictable from the input hash array — the
// `existingTxHashes` slice is appended by parallel Create() goroutines in
// arbitrary scheduler order, so subsequent worker ranges are scheduler-derived.
// We avoid that by not trying to pin which worker calls which expectation.
//
// Instead: pick a worker/chunk topology where the surviving worker has many
// remaining iterations after the trigger, so the mutation's effect dominates
// any in-flight noise.
//
//	totalTxs=32, batchSize=4, routines=2 → 8 chunks, 4 per worker.
//	exp1 .Times(2) — first 2 calls succeed.
//	exp2 .Once()   — 3rd call fails, cancels mergeCtx.
//	exp3 .Maybe()  — catch-all, increments postTriggerCount.
//
// Across every interleaving the surviving worker has at least 3 remaining
// iterations after the trigger:
//   - W_A blasts iter1+iter2 success, iter3 fails → W_B has all 4 iters remaining.
//   - W_A.iter1 + W_B.iter1 → W_A.iter2 fails → W_B has 3 iters remaining.
//   - W_B.iter1 + W_B.iter2 → W_B.iter3 fails → W_A has all 4 iters remaining.
//
// With the check intact, the surviving worker's for-loop top observes the
// cancelled mergeCtx on every remaining iteration and bails without a Called.
// Post-trigger count = 0 normally, or 1 if the sibling's next call was already
// in flight when cancellation propagated.
//
// With the check removed, the surviving worker calls SetMinedMulti for each
// remaining iteration → exp3 fires ≥ 2 times → postTriggerCount ≥ 2.
//
// Assertion: postTriggerCount ≤ 1. Mutation produces 2-4 across interleavings,
// so the bound reliably distinguishes the two cases.
func TestSyncManager_createUtxos_ChunkFailureCancelsSiblings(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const totalTxs = 32 // 2 workers × 4 chunks each = 8 chunks total
	sm, txMap, block, mockStore, _ := newChunkingTestSetup(t, totalTxs, 4, 2)

	var (
		postTriggerMu    sync.Mutex
		postTriggerCount int
	)

	mockStore.On("SetMinedMulti",
		mock.Anything, mock.Anything, mock.Anything,
	).Return(map[chainhash.Hash][]uint32{}, nil).Times(2)

	mockStore.On("SetMinedMulti",
		mock.Anything, mock.Anything, mock.Anything,
	).Return(
		map[chainhash.Hash][]uint32{},
		errors.NewStorageError("synthetic chunk failure"),
	).Once()

	// Catch-all returns nil so the surviving worker keeps iterating under
	// mutation — if it returned an error, the worker would bail on its first
	// post-trigger call (postTriggerCount=1) and the mutation would slip past
	// the `<= 1` assertion. With nil returns, the surviving worker drains all
	// its remaining iterations and postTriggerCount ≥ 3.
	mockStore.On("SetMinedMulti",
		mock.Anything, mock.Anything, mock.Anything,
	).Run(func(args mock.Arguments) {
		postTriggerMu.Lock()
		postTriggerCount++
		postTriggerMu.Unlock()
	}).Return(map[chainhash.Hash][]uint32{}, nil).Maybe()

	err := sm.createUtxos(ctx, txMap, block, 42)
	require.Error(t, err, "expected error to propagate from failing chunk")

	postTriggerMu.Lock()
	finalCount := postTriggerCount
	postTriggerMu.Unlock()

	require.LessOrEqual(t, finalCount, 1,
		"mergeCtx short-circuit should suppress sibling iterations after a chunk fails; "+
			"observed %d post-trigger call(s). Removing the short-circuit produces ≥ 2.",
		finalCount)
}

// TestSyncManager_createUtxos_ExactBatchSize covers n == MaxMinedBatchSize.
// With totalTxs=4 and batchSize=4: numChunks = ceil(4/4) = 1, numWorkers =
// min(2, 1) = 1 — one worker performs one SetMinedMulti call of size 4.
func TestSyncManager_createUtxos_ExactBatchSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const totalTxs = 4
	sm, txMap, block, mockStore, hashes := newChunkingTestSetup(t, totalTxs, 4, 2)

	var (
		callMu     sync.Mutex
		callChunks [][]chainhash.Hash
	)
	recordChunksOnMock(mockStore, &callMu, &callChunks)

	require.NoError(t, sm.createUtxos(ctx, txMap, block, 42))

	require.Len(t, callChunks, 1, "expected exactly 1 chunk for n == batchSize")
	require.Len(t, callChunks[0], totalTxs, "single chunk must cover all txs")
	assertUnionCovers(t, callChunks, hashes)
}

// TestSyncManager_createUtxos_OneOverBatchSize covers n == MaxMinedBatchSize + 1,
// the off-by-one boundary. With totalTxs=5 and batchSize=4: numChunks =
// ceil(5/4) = 2, numWorkers = min(2, 2) = 2, rangeSize = ceil(5/2) = 3 — two
// workers each emit a single chunk, sizes sum to 5. We don't pin the exact split
// (3,2) so the test is not brittle to range-balancing refactors.
func TestSyncManager_createUtxos_OneOverBatchSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const totalTxs = 5
	sm, txMap, block, mockStore, hashes := newChunkingTestSetup(t, totalTxs, 4, 2)

	var (
		callMu     sync.Mutex
		callChunks [][]chainhash.Hash
	)
	recordChunksOnMock(mockStore, &callMu, &callChunks)

	require.NoError(t, sm.createUtxos(ctx, txMap, block, 42))

	require.Len(t, callChunks, 2, "expected 2 chunks for n == batchSize+1 with 2 workers")
	for i, chunk := range callChunks {
		require.LessOrEqual(t, len(chunk), sm.settings.UtxoStore.MaxMinedBatchSize,
			"chunk %d size %d exceeds batchSize", i, len(chunk))
	}
	assertUnionCovers(t, callChunks, hashes)
}

// TestSyncManager_createUtxos_BatchSizeZeroClamped exercises the defensive clamp
// at handle_block.go:749-752. A misconfigured MaxMinedBatchSize=0 must be
// clamped to 1 — without the clamp the divide-by-zero in
// (len + batchSize - 1) / batchSize would panic. With clamp: batchSize=1,
// numChunks=3, numWorkers=min(2,3)=2.
func TestSyncManager_createUtxos_BatchSizeZeroClamped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const totalTxs = 3
	sm, txMap, block, mockStore, hashes := newChunkingTestSetup(t, totalTxs, 0, 2)

	var (
		callMu     sync.Mutex
		callChunks [][]chainhash.Hash
	)
	recordChunksOnMock(mockStore, &callMu, &callChunks)

	require.NotPanics(t, func() {
		require.NoError(t, sm.createUtxos(ctx, txMap, block, 42))
	}, "batchSize=0 must be clamped to avoid divide-by-zero")

	require.Len(t, callChunks, totalTxs, "with batchSize clamped to 1, expected one chunk per tx")
	for i, chunk := range callChunks {
		require.Len(t, chunk, 1, "chunk %d size: clamped batchSize=1 must produce chunks of size 1", i)
	}
	assertUnionCovers(t, callChunks, hashes)
}

// TestSyncManager_createUtxos_RoutinesZeroClamped exercises the defensive clamp
// at handle_block.go:754-757. A misconfigured MaxMinedRoutines=0 must be clamped
// to 1 — without the clamp, the for-loop guarded by `w < numWorkers` would never
// execute and SetMinedMulti would never be called, silently dropping the merge.
// With clamp: numWorkers=1, single worker emits ceil(10/4)=3 chunks.
func TestSyncManager_createUtxos_RoutinesZeroClamped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const totalTxs = 10
	sm, txMap, block, mockStore, hashes := newChunkingTestSetup(t, totalTxs, 4, 0)

	var (
		callMu     sync.Mutex
		callChunks [][]chainhash.Hash
	)
	recordChunksOnMock(mockStore, &callMu, &callChunks)

	require.NotPanics(t, func() {
		require.NoError(t, sm.createUtxos(ctx, txMap, block, 42))
	}, "numRoutines=0 must be clamped so the merge actually runs")

	require.Len(t, callChunks, 3, "with routines clamped to 1, expected 3 chunks (ceil(10/4))")
	for i, chunk := range callChunks {
		require.LessOrEqual(t, len(chunk), sm.settings.UtxoStore.MaxMinedBatchSize,
			"chunk %d size %d exceeds batchSize", i, len(chunk))
	}
	assertUnionCovers(t, callChunks, hashes)
}

func TestSyncManager_quickValidationAllowed(t *testing.T) {
	mainnetHighest := uint32(chaincfg.MainNetParams.Checkpoints[len(chaincfg.MainNetParams.Checkpoints)-1].Height)

	tests := []struct {
		name        string
		chainParams *chaincfg.Params
		height      uint32
		want        bool
	}{
		{
			name:        "nil chain params",
			chainParams: nil,
			height:      100,
			want:        false,
		},
		{
			name:        "regtest has no checkpoints",
			chainParams: &chaincfg.RegressionNetParams,
			height:      0,
			want:        false,
		},
		{
			name:        "mainnet height 0 is covered",
			chainParams: &chaincfg.MainNetParams,
			height:      0,
			want:        true,
		},
		{
			name:        "mainnet height equal to highest checkpoint is covered",
			chainParams: &chaincfg.MainNetParams,
			height:      mainnetHighest,
			want:        true,
		},
		{
			name:        "mainnet height one above highest checkpoint is not covered",
			chainParams: &chaincfg.MainNetParams,
			height:      mainnetHighest + 1,
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &SyncManager{chainParams: tt.chainParams}
			require.Equal(t, tt.want, sm.quickValidationAllowed(tt.height))
		})
	}
}

// TestClassifyAndCountPrewarmError verifies that classifyAndCountPrewarmError routes
// each validator error class to the correct prometheusLegacyNetsyncPrewarmErrors label,
// preserving the silent-drop semantics flagged by issue #4590 while restoring observability.
func TestClassifyAndCountPrewarmError(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name  string
		err   error
		label string
	}{
		{
			name:  "tx_invalid",
			err:   errors.NewTxInvalidError("script failed"),
			label: "tx_invalid",
		},
		{
			name:  "service",
			err:   errors.NewServiceError("validator unavailable"),
			label: "service",
		},
		{
			name:  "processing",
			err:   errors.NewProcessingError("transient processing error"),
			label: "processing",
		},
		{
			name:  "policy_conflicting",
			err:   errors.NewTxConflictingError("double-spend in mempool"),
			label: "policy",
		},
		{
			name:  "policy_already_exists",
			err:   errors.NewTxExistsError("already in mempool"),
			label: "policy",
		},
		{
			name:  "other",
			err:   errors.NewStorageError("disk full"),
			label: "other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counter := prometheusLegacyNetsyncPrewarmErrors.WithLabelValues(tt.label)
			before := testutil.ToFloat64(counter)

			classifyAndCountPrewarmError(ulogger.TestLogger{}, tt.err)

			after := testutil.ToFloat64(counter)
			require.Equal(t, before+1, after, "counter for label %q must increment by 1", tt.label)
		})
	}
}
