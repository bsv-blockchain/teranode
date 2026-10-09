package blockpersister

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"testing"
	"testing/iotest"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
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
	"github.com/stretchr/testify/mock"
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
	// Each fixture tx needs an input: every real transaction has at least one,
	// and an input-less tx is the shape a UTXO-set snapshot reconstruction takes,
	// which the persister now refuses to serialize.
	for i := 0; i < txCount; i++ {
		txs[i] = bt.NewTx()
		err := txs[i].From("0000000000000000000000000000000000000000000000000000000000000001", uint32(i), "76a914000000000000000000000000000000000000000088ac", 2000)
		require.NoError(t, err)
		err = txs[i].AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
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

// TestCreateSubtreeDataFileStreaming_RecreatesInvalidExistingFile covers the reject side of
// validateSubtreeData: an existing subtreeData file that fails validation is deleted and rebuilt
// from the UTXO store, not reused.
func TestCreateSubtreeDataFileStreaming_RecreatesInvalidExistingFile(t *testing.T) {
	block, _, extendedTxs, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)

	key := block.Subtrees[0][:]

	good, err := subtreeStore.Get(t.Context(), key, fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	// Cut inside the last transaction's lock time, which validation rejects.
	require.NoError(t, subtreeStore.Del(t.Context(), key, fileformat.FileTypeSubtreeData))
	require.NoError(t, subtreeStore.Set(t.Context(), key, fileformat.FileTypeSubtreeData, good[:len(good)-3]))

	persister := New(t.Context(), ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStore, blockchainClient)

	require.NoError(t, persister.CreateSubtreeDataFileStreaming(t.Context(), *block.Subtrees[0], block, 0))

	mockUTXOStore.AssertCalled(t, "BatchDecorate", mock.Anything, mock.Anything, mock.Anything)

	rebuilt, err := subtreeStore.Get(t.Context(), key, fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	subtreeBytes, err := subtreeStore.Get(t.Context(), key, fileformat.FileTypeSubtree)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewSubtreeFromBytes(subtreeBytes)
	require.NoError(t, err)

	subtreeData, err := subtreepkg.NewSubtreeDataFromBytes(subtree, rebuilt)
	require.NoError(t, err)
	require.Len(t, subtreeData.Txs, len(extendedTxs))

	for i, tx := range subtreeData.Txs {
		require.NotNil(t, tx, "transaction at index %d should not be nil", i)
		require.Equal(t, extendedTxs[i].TxIDChainHash().String(), tx.TxIDChainHash().String())
	}
}

// TestProcessSubtreeUTXOStreaming_OutputMatchesPlainDecode checks that resetting the arena per
// transaction is invisible in what phase 2 writes. The additions and deletions produced by the
// arena-backed loop, where each transaction's scripts overwrite the previous one's, must be
// byte-identical to those produced from the same transactions decoded without an arena. A
// consumer that kept a script past its iteration would write the next transaction's bytes.
func TestProcessSubtreeUTXOStreaming_OutputMatchesPlainDecode(t *testing.T) {
	ctx := t.Context()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	subtreeStore, subtree, _ := storeLargeSubtree(t, 8, 4096)

	subtreeData, err := subtreeStore.Get(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	blockHash := chainhash.DoubleHashH([]byte("issue-1914-output-readback"))

	persist := func(process func(utxoDiff *utxopersister.UTXOSet)) ([]byte, []byte) {
		blockStore := memory.New()

		utxoDiff, err := utxopersister.NewUTXOSet(ctx, logger, tSettings, blockStore, &blockHash, 2000)
		require.NoError(t, err)

		process(utxoDiff)
		require.NoError(t, utxoDiff.Close())

		additions, err := blockStore.Get(ctx, blockHash[:], fileformat.FileTypeUtxoAdditions)
		require.NoError(t, err)

		deletions, err := blockStore.Get(ctx, blockHash[:], fileformat.FileTypeUtxoDeletions)
		require.NoError(t, err)

		return additions, deletions
	}

	persister := New(ctx, logger, tSettings, nil, subtreeStore, nil, nil)

	gotAdditions, gotDeletions := persist(func(utxoDiff *utxopersister.UTXOSet) {
		require.NoError(t, persister.ProcessSubtreeUTXOStreaming(ctx, *subtree.RootHash(), utxoDiff))
	})

	wantAdditions, wantDeletions := persist(func(utxoDiff *utxopersister.UTXOSet) {
		r := bytes.NewReader(subtreeData)

		for i := 0; i < subtree.Length(); i++ {
			tx := &bt.Tx{}

			_, err := tx.ReadFrom(r)
			require.NoError(t, err)
			require.NoError(t, utxoDiff.ProcessTx(tx))
		}
	})

	require.Equal(t, wantAdditions, gotAdditions)
	require.Equal(t, wantDeletions, gotDeletions)
}

// smallTx returns a distinct 1-in-1-out transaction. A zero prevHash with index 0xffffffff
// makes it a coinbase.
func smallTx(t *testing.T, prevHash chainhash.Hash, prevIndex uint32, seed byte) *bt.Tx {
	t.Helper()

	input := &bt.Input{
		PreviousTxOutIndex: prevIndex,
		PreviousTxSatoshis: 2000,
		PreviousTxScript:   bscript.NewFromBytes([]byte{bscript.Op1, seed}),
		UnlockingScript:    bscript.NewFromBytes([]byte{bscript.Op1, seed}),
		SequenceNumber:     0xffffffff,
	}
	require.NoError(t, input.PreviousTxIDAdd(&prevHash))

	tx := bt.NewTx()
	tx.Inputs = append(tx.Inputs, input)
	tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: bscript.NewFromBytes([]byte{bscript.Op1, seed})})

	return tx
}

// TestValidateSubtreeData_MatchesGoSubtree pins validateSubtreeData to the accept/reject
// behaviour of subtreepkg.NewSubtreeDataFromReader, which it replaces on the existing-file
// path. A divergence either way changes which subtreeData files the persister deletes and
// recreates.
func TestValidateSubtreeData_MatchesGoSubtree(t *testing.T) {
	coinbase := smallTx(t, chainhash.Hash{}, 0xffffffff, 0xcb)

	txs := make([]*bt.Tx, 4)
	for i := range txs {
		txs[i] = smallTx(t, chainhash.HashH([]byte{byte(i)}), uint32(i), byte(i))
	}

	newSubtree := func(t *testing.T, placeholder bool, nodes []*bt.Tx) *subtreepkg.Subtree {
		t.Helper()

		st, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)

		if placeholder {
			require.NoError(t, st.AddCoinbaseNode())
		}

		for _, tx := range nodes {
			require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size())))
		}

		return st
	}

	concat := func(extended bool, list ...*bt.Tx) []byte {
		var out []byte

		for _, tx := range list {
			if extended {
				out = append(out, tx.ExtendedBytes()...)
			} else {
				out = append(out, tx.Bytes()...)
			}
		}

		return out
	}

	plain := newSubtree(t, false, txs)
	withPlaceholder := newSubtree(t, true, txs[1:])
	full := concat(true, txs...)

	// version, one input, a zero outpoint, then an unlocking-script length of 2^30 + 1, one past
	// bt.MaxArenaAlloc. Both reject it before allocating.
	oversizedScript := append(append([]byte{0x01, 0x00, 0x00, 0x00, 0x01}, make([]byte, 36)...), 0xfe, 0x01, 0x00, 0x00, 0x40)

	cases := []struct {
		name    string
		subtree *subtreepkg.Subtree
		data    []byte
		readErr error // returned by the reader once data is exhausted, in place of io.EOF
		valid   bool
	}{
		{name: "valid extended", subtree: plain, data: full, valid: true},
		{name: "valid non-extended", subtree: plain, data: concat(false, txs...), valid: true},
		{name: "valid with extended and non-extended mixed", subtree: plain, data: append(concat(true, txs[0], txs[1]), concat(false, txs[2], txs[3])...), valid: true},
		{name: "placeholder with coinbase in file", subtree: withPlaceholder, data: concat(true, coinbase, txs[1], txs[2], txs[3]), valid: true},
		{name: "placeholder without coinbase in file", subtree: withPlaceholder, data: concat(true, txs[1], txs[2], txs[3]), valid: true},
		{name: "placeholder with a second coinbase later", subtree: withPlaceholder, data: concat(true, coinbase, txs[1], coinbase, txs[2]), valid: false},
		{name: "placeholder with coinbase after the first transaction", subtree: withPlaceholder, data: concat(true, txs[1], coinbase, txs[2], txs[3]), valid: false},
		{name: "hash mismatch", subtree: plain, data: concat(true, txs[1], txs[0], txs[2], txs[3]), valid: false},
		{name: "trailing extra transaction", subtree: plain, data: concat(true, txs[0], txs[1], txs[2], txs[3], txs[0]), valid: false},
		{name: "truncated inside a transaction", subtree: plain, data: full[:len(full)-3], valid: false},
		{name: "wrong transaction after a valid prefix", subtree: plain, data: concat(true, txs[0], txs[2]), valid: false},
		{name: "ends inside a field", subtree: plain, data: []byte{0x01, 0x00, 0x00, 0x00, 0xff, 0x01}, valid: false},
		{name: "script length above MaxArenaAlloc", subtree: plain, data: oversizedScript, valid: false},
		{name: "reader fails before the first byte", subtree: plain, readErr: io.ErrClosedPipe, valid: false},
		{name: "reader fails after a full transaction", subtree: plain, data: concat(true, txs[0]), readErr: io.ErrClosedPipe, valid: false},
		{name: "unexpected EOF on a transaction boundary", subtree: plain, data: concat(true, txs[0]), readErr: io.ErrUnexpectedEOF, valid: false},
		{name: "no nodes", subtree: &subtreepkg.Subtree{}, data: full, valid: false},

		// Shared go-subtree behaviour, pinned rather than endorsed. Issue 1917 tracks rejecting
		// a file that ends before the last node.
		{name: "truncated on a transaction boundary", subtree: plain, data: concat(true, txs[0], txs[1]), valid: true},
		{name: "empty file", subtree: plain, valid: true},
		{name: "ends inside a transaction on a field boundary", subtree: plain, data: []byte{0x01, 0x00, 0x00, 0x00, 0xff}, valid: true},
		// go-bt wraps this EOF (script length read, no script bytes) with pkg/errors.
		{name: "ends after a script length", subtree: plain, data: append(append([]byte{0x01, 0x00, 0x00, 0x00, 0x01}, make([]byte, 36)...), 0x05), valid: true},
		// The coinbase skip keys on txIndex == 1, not on node 0 being the placeholder.
		{name: "coinbase second without a placeholder", subtree: plain, data: concat(true, txs[0], coinbase), valid: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			newReader := func() io.Reader {
				if tc.readErr == nil {
					return bytes.NewReader(tc.data)
				}

				return io.MultiReader(bytes.NewReader(tc.data), iotest.ErrReader(tc.readErr))
			}

			_, refErr := subtreepkg.NewSubtreeDataFromReader(tc.subtree, newReader())
			err := validateSubtreeData(tc.subtree, newReader())

			require.Equal(t, tc.valid, refErr == nil, "go-subtree reference: %v", refErr)
			require.Equal(t, tc.valid, err == nil, "validateSubtreeData: %v", err)
		})
	}
}
