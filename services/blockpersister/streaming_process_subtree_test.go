package blockpersister

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestSubtreeDataWriter_OrderedWrites(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	settings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	ctx := context.Background()
	key := []byte("test-ordered-writes")
	fileType := fileformat.FileTypeDat

	storer, err := filestorer.NewFileStorer(ctx, logger, settings, blockStore, key, fileType)
	require.NoError(t, err)

	writer := NewSubtreeDataWriter(storer)

	tx1 := []byte("tx1data")
	tx2 := []byte("tx2data")
	tx3 := []byte("tx3data")

	err = writer.WriteBatch(2, [][]byte{tx3})
	require.NoError(t, err)

	err = writer.WriteBatch(0, [][]byte{tx1})
	require.NoError(t, err)

	err = writer.WriteBatch(1, [][]byte{tx2})
	require.NoError(t, err)

	err = writer.Close(ctx)
	require.NoError(t, err)

	data, err := blockStore.Get(ctx, key, fileType)
	require.NoError(t, err)

	expected := append(append(tx1, tx2...), tx3...)
	require.Equal(t, expected, data)
}

func TestSubtreeDataWriter_ConcurrentWrites(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	settings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	ctx := context.Background()
	key := []byte("test-concurrent-writes")
	fileType := fileformat.FileTypeDat

	storer, err := filestorer.NewFileStorer(ctx, logger, settings, blockStore, key, fileType)
	require.NoError(t, err)

	writer := NewSubtreeDataWriter(storer)

	batchCount := 10
	done := make(chan error, batchCount)

	for i := batchCount - 1; i >= 0; i-- {
		i := i
		go func() {
			data := []byte{byte(i)}
			done <- writer.WriteBatch(i, [][]byte{data})
		}()
	}

	for i := 0; i < batchCount; i++ {
		err := <-done
		require.NoError(t, err)
	}

	err = writer.Close(ctx)
	require.NoError(t, err)

	data, err := blockStore.Get(ctx, key, fileType)
	require.NoError(t, err)

	expected := make([]byte, batchCount)
	for i := 0; i < batchCount; i++ {
		expected[i] = byte(i)
	}
	require.Equal(t, expected, data)
}

func TestSubtreeDataWriter_ClosedWriter(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	settings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	ctx := context.Background()
	key := []byte("test-closed-writer")
	fileType := fileformat.FileTypeDat

	storer, err := filestorer.NewFileStorer(ctx, logger, settings, blockStore, key, fileType)
	require.NoError(t, err)

	writer := NewSubtreeDataWriter(storer)

	err = writer.Close(ctx)
	require.NoError(t, err)

	err = writer.WriteBatch(0, [][]byte{[]byte("data")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "writer is closed")
}

func TestSubtreeDataWriter_IncompleteSequence(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	settings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	ctx := context.Background()
	key := []byte("test-incomplete")
	fileType := fileformat.FileTypeDat

	storer, err := filestorer.NewFileStorer(ctx, logger, settings, blockStore, key, fileType)
	require.NoError(t, err)

	writer := NewSubtreeDataWriter(storer)

	err = writer.WriteBatch(0, [][]byte{[]byte("tx0")})
	require.NoError(t, err)

	err = writer.WriteBatch(2, [][]byte{[]byte("tx2")})
	require.NoError(t, err)

	err = writer.Close(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pending batches")
}

func TestSubtreeDataWriter_AbortDoesNotSaveFile(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	settings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	ctx := context.Background()
	key := []byte("test-abort-no-save")
	fileType := fileformat.FileTypeDat

	storer, err := filestorer.NewFileStorer(ctx, logger, settings, blockStore, key, fileType)
	require.NoError(t, err)

	writer := NewSubtreeDataWriter(storer)

	// Write some data
	err = writer.WriteBatch(0, [][]byte{[]byte("tx0data")})
	require.NoError(t, err)

	err = writer.WriteBatch(1, [][]byte{[]byte("tx1data")})
	require.NoError(t, err)

	// Abort instead of Close - file should NOT be saved
	writer.Abort(errors.NewProcessingError("intentional abort for test"))

	// Verify the file does NOT exist
	exists, err := blockStore.Exists(ctx, key, fileType)
	require.NoError(t, err)
	require.False(t, exists, "File should NOT exist after Abort() - incomplete files should not be finalized")
}

func TestSubtreeDataWriter_CloseWithPendingBatchesAbortsFile(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	settings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	ctx := context.Background()
	key := []byte("test-close-pending-aborts")
	fileType := fileformat.FileTypeDat

	storer, err := filestorer.NewFileStorer(ctx, logger, settings, blockStore, key, fileType)
	require.NoError(t, err)

	writer := NewSubtreeDataWriter(storer)

	// Write batch 0 and 2, but NOT batch 1 - creating a gap
	err = writer.WriteBatch(0, [][]byte{[]byte("tx0data")})
	require.NoError(t, err)

	err = writer.WriteBatch(2, [][]byte{[]byte("tx2data")})
	require.NoError(t, err)

	// Close with pending batches - should abort and NOT save the file
	err = writer.Close(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pending batches")

	// Verify the file does NOT exist (was aborted, not closed)
	exists, err := blockStore.Exists(ctx, key, fileType)
	require.NoError(t, err)
	require.False(t, exists, "File should NOT exist when Close() is called with pending batches")
}

func TestCreateSubtreeDataFileStreaming_Success(t *testing.T) {
	block, _, extendedTxs, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)

	err := subtreeStore.Del(t.Context(), block.Subtrees[0][:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	persister := New(context.Background(), ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStore, blockchainClient)

	err = persister.CreateSubtreeDataFileStreaming(context.Background(), *block.Subtrees[0], block, 0)
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeStore.Get(t.Context(), block.Subtrees[0][:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	subtreeBytes, err := subtreeStore.Get(t.Context(), block.Subtrees[0][:], fileformat.FileTypeSubtree)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewSubtreeFromBytes(subtreeBytes)
	require.NoError(t, err)

	subtreeData, err := subtreepkg.NewSubtreeDataFromBytes(subtree, subtreeDataBytes)
	require.NoError(t, err)

	require.Len(t, subtreeData.Txs, 4)

	for i, tx := range subtreeData.Txs {
		require.NotNil(t, tx, "transaction at index %d should not be nil", i)
		require.Equal(t, extendedTxs[i].TxIDChainHash().String(), tx.TxIDChainHash().String())
	}
}

func TestCreateSubtreeDataFileStreaming_AlreadyExists(t *testing.T) {
	_, blockBytes, _, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)

	persister := New(context.Background(), ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStore, blockchainClient)

	block, err := model.NewBlockFromBytes(blockBytes)
	require.NoError(t, err)

	err = persister.CreateSubtreeDataFileStreaming(context.Background(), *block.Subtrees[0], block, 0)
	require.NoError(t, err)

	err = persister.CreateSubtreeDataFileStreaming(context.Background(), *block.Subtrees[0], block, 1)
	require.NoError(t, err)
}

func TestCreateSubtreeDataFileStreaming_NoCoinbasePlaceholder(t *testing.T) {
	logger := ulogger.TestLogger{}
	settings := test.CreateBaseTestSettings(t)

	txCount := 4
	txs := make([]*bt.Tx, txCount)
	for i := 0; i < txCount; i++ {
		txs[i] = bt.NewTx()
		err := txs[i].AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)
	}

	subtree, err := subtreepkg.NewTreeByLeafCount(txCount)
	require.NoError(t, err)

	for _, tx := range txs {
		err = subtree.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size()))
		require.NoError(t, err)
	}

	mockUTXOStore := setupMockUTXOStore(txs)

	subtreeStore := memory.New()
	blockStore := memory.New()

	blockBytes, err := hex.DecodeString("010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d61900000000006657a9252aacd5c0b2940996ecff952228c3067cc38d4885efb5a4ac4247e9f337221b4d4c86041b0f2b57100401000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a010000004341041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84ac000000000100000001032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a187000000008c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c9331586c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff0200e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c2f6b52de3d7c88ac000000000100000001c33ebff2a709f13d9f9a7569ab16a32786af7d7e2de09265e41c61d078294ecf010000008a4730440220032d30df5ee6f57fa46cddb5eb8d0d9fe8de6b342d27942ae90a3231e0ba333e02203deee8060fdc70230a7f5b4ad7d7bc3e628cbe219a886b84269eaeb81e26b4fe014104ae31c31bf91278d99b8377a35bbce5b27d9fff15456839e919453fc7b3f721f0ba403ff96c9deeb680e5fd341c0fc3a7b90da4631ee39560639db462e9cb850fffffffff0240420f00000000001976a914b0dcbf97eabf4404e31d952477ce822dadbe7e1088acc060d211000000001976a9146b1281eec25ab4e1e0793ff4e08ab1abb3409cd988ac0000000001000000010b6072b386d4a773235237f64c1126ac3b240c84b917a3909ba1c43ded5f51f4000000008c493046022100bb1ad26df930a51cce110cf44f7a48c3c561fd977500b1ae5d6b6fd13d0b3f4a022100c5b42951acedff14abba2736fd574bdb465f3e6f8da12e2c5303954aca7f78f3014104a7135bfe824c97ecc01ec7d7e336185c81e2aa2c41ab175407c09484ce9694b44953fcb751206564a9c24dd094d42fdbfdd5aad3e063ce6af4cfaaea4ea14fbbffffffff0140420f00000000001976a91439aa3d569e06a1d7926dc4be1193c99bf2eb9ee088ac00000000")
	require.NoError(t, err)

	block, err := model.NewBlockFromBytes(blockBytes)
	require.NoError(t, err)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	persister := New(context.Background(), logger, settings, blockStore, subtreeStore, mockUTXOStore, nil)

	err = persister.CreateSubtreeDataFileStreaming(context.Background(), *subtree.RootHash(), block, 1)
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeStore.Get(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	subtreeData, err := subtreepkg.NewSubtreeDataFromBytes(subtree, subtreeDataBytes)
	require.NoError(t, err)

	require.Len(t, subtreeData.Txs, txCount)
}

func TestProcessSubtreeUTXOStreaming_ReadingTransactionByTransaction(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	settings := test.CreateBaseTestSettings(t)

	txCount := 16
	txs := make([]*bt.Tx, txCount)
	for i := 0; i < txCount; i++ {
		txs[i] = bt.NewTx()
		err := txs[i].AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)
	}

	subtree, err := subtreepkg.NewTreeByLeafCount(txCount)
	require.NoError(t, err)

	for i, tx := range txs {
		if i == 0 {
			err = subtree.AddCoinbaseNode()
		} else {
			err = subtree.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size()))
		}
		require.NoError(t, err)
	}

	subtreeStore := memory.New()
	blockStore := memory.New()

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	for _, tx := range txs {
		_, err = writer.Write(tx.Bytes())
		require.NoError(t, err)
	}
	err = writer.Flush()
	require.NoError(t, err)

	err = subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, buf.Bytes())
	require.NoError(t, err)

	persister := New(ctx, logger, settings, blockStore, subtreeStore, nil, nil)

	blockHash := chainhash.DoubleHashH([]byte("test-block-streaming"))
	utxoDiff, err := utxopersister.NewUTXOSet(ctx, logger, settings, blockStore, &blockHash, 2000)
	require.NoError(t, err)
	defer utxoDiff.Close()

	err = persister.ProcessSubtreeUTXOStreaming(ctx, *subtree.RootHash(), utxoDiff)
	require.NoError(t, err)
}

func TestPersistBlock_TwoPhaseStreaming(t *testing.T) {
	block, blockBytes, extendedTxs, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)

	err := subtreeStore.Del(t.Context(), block.Subtrees[0][:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	persister := New(context.Background(), ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStore, blockchainClient)

	err = persister.persistBlock(context.Background(), block.Header.Hash(), blockBytes)
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeStore.Get(t.Context(), block.Subtrees[0][:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	subtreeBytes, err := subtreeStore.Get(t.Context(), block.Subtrees[0][:], fileformat.FileTypeSubtree)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewSubtreeFromBytes(subtreeBytes)
	require.NoError(t, err)

	subtreeData, err := subtreepkg.NewSubtreeDataFromBytes(subtree, subtreeDataBytes)
	require.NoError(t, err)
	require.Len(t, subtreeData.Txs, 4)

	for i, tx := range subtreeData.Txs {
		require.NotNil(t, tx, "transaction at index %d should not be nil", i)
		require.Equal(t, extendedTxs[i].TxIDChainHash().String(), tx.TxIDChainHash().String())
	}

	blockHash := block.Header.Hash()
	utxoAdditionsBytes, err := blockStore.Get(t.Context(), blockHash[:], fileformat.FileTypeUtxoAdditions)
	require.NoError(t, err)
	require.Greater(t, len(utxoAdditionsBytes), 36)

	utxoDeletionsBytes, err := blockStore.Get(t.Context(), blockHash[:], fileformat.FileTypeUtxoDeletions)
	require.NoError(t, err)
	require.Greater(t, len(utxoDeletionsBytes), 36)
}
