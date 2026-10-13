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
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
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

	err = persister.ProcessSubtreeUTXOStreaming(ctx, *subtree.RootHash(), nil, utxoDiff)
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
// from the UTXO store, not reused. A short file is one of them (issue 1917): reused, it would get
// DAH 0 and phase 2 would fail on its missing tail on every attempt.
func TestCreateSubtreeDataFileStreaming_RecreatesInvalidExistingFile(t *testing.T) {
	cases := []struct {
		name string
		cut  func(t *testing.T, good []byte, extendedTxs []*bt.Tx) []byte
	}{
		{
			name: "cut inside the last transaction's lock time",
			cut: func(_ *testing.T, good []byte, _ []*bt.Tx) []byte {
				return good[:len(good)-3]
			},
		},
		{
			name: "cut on the last transaction's boundary",
			cut: func(t *testing.T, good []byte, extendedTxs []*bt.Tx) []byte {
				last := extendedTxs[len(extendedTxs)-1].ExtendedBytes()
				require.True(t, bytes.HasSuffix(good, last), "subtreeData should end with the last extended transaction")

				return good[:len(good)-len(last)]
			},
		},
		{
			name: "empty file",
			cut: func(_ *testing.T, _ []byte, _ []*bt.Tx) []byte {
				return []byte{}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			block, _, extendedTxs, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)

			key := block.Subtrees[0][:]

			good, err := subtreeStore.Get(t.Context(), key, fileformat.FileTypeSubtreeData)
			require.NoError(t, err)

			require.NoError(t, subtreeStore.Del(t.Context(), key, fileformat.FileTypeSubtreeData))
			require.NoError(t, subtreeStore.Set(t.Context(), key, fileformat.FileTypeSubtreeData, tc.cut(t, good, extendedTxs)))

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
		})
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
		require.NoError(t, persister.ProcessSubtreeUTXOStreaming(ctx, *subtree.RootHash(), nil, utxoDiff))
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

// TestValidateSubtreeData_AgainstGoSubtreeAndCatchup pins validateSubtreeData, row by row,
// against two references. One is subtreepkg.NewSubtreeDataFromReader, which it replaces on the
// existing-file path. The other is that reader followed by the catchup fetcher's checks,
// model.MissingSubtreeDataTxs and model.FirstMismatchedSubtreeDataTx with the placeholder at
// index 0 exempt. A change either way changes which subtreeData files the persister deletes and
// recreates.
//
// validateSubtreeData is stricter than the reader alone in three ways, each marked readerAccepts:
// a file that ends on a transaction or field boundary before the last node (issue 1917), one that
// ends partway through a transaction after it, and a coinbase anywhere but first under the
// placeholder. It is stricter than catchup on a partial transaction after the last node and on a
// second coinbase under the placeholder, marked catchupAccepts.
func TestValidateSubtreeData_AgainstGoSubtreeAndCatchup(t *testing.T) {
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
	twoNodes := newSubtree(t, false, txs[:2])
	placeholderOnly := newSubtree(t, true, nil)
	full := concat(true, txs...)

	// version, one input, a zero outpoint, then an unlocking-script length of 2^30 + 1, one past
	// bt.MaxArenaAlloc. Both reject it before allocating.
	oversizedScript := append(append([]byte{0x01, 0x00, 0x00, 0x00, 0x01}, make([]byte, 36)...), 0xfe, 0x01, 0x00, 0x00, 0x40)

	cases := []struct {
		name      string
		subtree   *subtreepkg.Subtree
		data      []byte
		readErr   error // returned by the reader once data is exhausted, in place of io.EOF
		readFails bool  // the reject is a storage error, so the caller keeps the file
		valid     bool  // validateSubtreeData accepts it

		// Set only where validateSubtreeData rejects what a reference accepts.
		readerAccepts  bool // go-subtree's reader alone accepts it
		catchupAccepts bool // the reader followed by the catchup checks accepts it
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
		{name: "reader fails before the first byte", subtree: plain, readErr: io.ErrClosedPipe, readFails: true},
		{name: "reader fails after a full transaction", subtree: plain, data: concat(true, txs[0]), readErr: io.ErrClosedPipe, readFails: true},
		{name: "reader returns unexpected EOF on a transaction boundary", subtree: plain, data: concat(true, txs[0]), readErr: io.ErrUnexpectedEOF, readFails: true},
		// A placeholder-only subtree is complete with or without the coinbase in the file. Phase 2
		// processes the block's coinbase and reads nothing.
		{name: "placeholder only, empty file", subtree: placeholderOnly, valid: true},
		{name: "placeholder only, coinbase in file", subtree: placeholderOnly, data: concat(true, coinbase), valid: true},
		{name: "placeholder only, another transaction in file", subtree: placeholderOnly, data: concat(true, txs[1]), valid: false},
		{name: "no nodes", subtree: &subtreepkg.Subtree{}, data: full, valid: false},

		// go-bt returns a clean io.EOF when the stream ends where a transaction or a field
		// starts, so the reader stops there without having seen every node.
		{name: "truncated on a transaction boundary", subtree: plain, data: concat(true, txs[0], txs[1]), readerAccepts: true},
		{name: "empty file", subtree: plain, readerAccepts: true},
		{name: "ends inside a transaction on a field boundary", subtree: plain, data: []byte{0x01, 0x00, 0x00, 0x00, 0xff}, readerAccepts: true},
		// go-bt wraps this EOF (script length read, no script bytes) with pkg/errors.
		{name: "ends after a script length", subtree: plain, data: append(append([]byte{0x01, 0x00, 0x00, 0x00, 0x01}, make([]byte, 36)...), 0x05), readerAccepts: true},
		{name: "placeholder with an empty file", subtree: withPlaceholder, readerAccepts: true},
		{name: "placeholder with only the coinbase in file", subtree: withPlaceholder, data: concat(true, coinbase), readerAccepts: true},
		{name: "placeholder without coinbase, last node missing", subtree: withPlaceholder, data: concat(true, txs[1], txs[2]), readerAccepts: true},

		// go-subtree's reader skips any coinbase it decodes at node index 1, unchecked. Phase 2
		// takes the coinbase unchecked only at the placeholder node, so it fails on all three.
		{name: "coinbase second without a placeholder", subtree: plain, data: concat(true, txs[0], coinbase, txs[1], txs[2], txs[3]), readerAccepts: true},
		{name: "coinbase in place of the last node without a placeholder", subtree: twoNodes, data: concat(true, txs[0], coinbase, txs[1]), readerAccepts: true},
		{name: "placeholder with the coinbase twice", subtree: withPlaceholder, data: concat(true, coinbase, coinbase, txs[1], txs[2], txs[3]), readerAccepts: true, catchupAccepts: true},

		// go-bt returns a clean io.EOF for a transaction cut on a field boundary, so the reader and
		// the catchup checks accept these. Phase 2 never reads the partial transaction, but the
		// file is kept with DAH 0 and served to peers.
		{name: "complete, then ends inside a trailing transaction on a field boundary", subtree: plain, data: append(concat(true, txs...), 0x01, 0x00, 0x00, 0x00, 0xff), readerAccepts: true, catchupAccepts: true},
		{name: "placeholder only, a transaction cut on a field boundary", subtree: placeholderOnly, data: []byte{0x01, 0x00, 0x00, 0x00, 0xff}, readerAccepts: true, catchupAccepts: true},
		// The only place where how a read error is classified changes the verdict: after the last
		// node. io.ErrUnexpectedEOF is not end of file, so all three reject it.
		{name: "complete, then cut inside a trailing transaction's version", subtree: plain, data: append(concat(true, txs...), 0x01, 0x00), valid: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			newReader := func() io.Reader {
				if tc.readErr == nil {
					return bytes.NewReader(tc.data)
				}

				return io.MultiReader(bytes.NewReader(tc.data), iotest.ErrReader(tc.readErr))
			}

			data, refErr := subtreepkg.NewSubtreeDataFromReader(tc.subtree, newReader())
			err := validateSubtreeData(tc.subtree, newReader())

			require.Equal(t, tc.valid || tc.readerAccepts, refErr == nil, "go-subtree reader: %v", refErr)

			if refErr == nil {
				missing := model.MissingSubtreeDataTxs(tc.subtree, data, true)
				idx, _, _, mismatched := model.FirstMismatchedSubtreeDataTx(tc.subtree, data, true)
				require.Equal(t, tc.valid || tc.catchupAccepts, missing == 0 && !mismatched,
					"catchup checks: %d missing, mismatch at %d: %v", missing, idx, mismatched)
			}

			require.Equal(t, tc.valid, err == nil, "validateSubtreeData: %v", err)

			if err != nil {
				require.Equal(t, tc.readFails, errors.Is(err, errors.ErrStorageError), "storage error: %v", err)
			}

			// What phase 1 relies on when it reuses a file: phase 2 can read it to the end.
			if tc.valid {
				_, _, phase2Err := runPhase2(t, tc.subtree, tc.data, coinbase)
				require.NoError(t, phase2Err, "phase 2 on a file validateSubtreeData accepts")
			}
		})
	}
}

// TestProcessSubtreeUTXOStreaming_ChecksEachTxBeforeProcessingIt feeds phase 2 a file holding a
// transaction its node does not name, and checks that nothing of that transaction reaches the UTXO
// additions. A coinbase must be hash-checked too: only one opening the file under the placeholder
// is exempt, or phase 2 would persist it in another node's place and succeed. Under the
// placeholder, the block's coinbase is processed before anything is read.
func TestProcessSubtreeUTXOStreaming_ChecksEachTxBeforeProcessingIt(t *testing.T) {
	ctx := t.Context()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockHash := chainhash.DoubleHashH([]byte("phase-2-checks-before-processing"))

	coinbase := smallTx(t, chainhash.Hash{}, 0xffffffff, 0xcb)

	txs := make([]*bt.Tx, 3)
	for i := range txs {
		txs[i] = smallTx(t, chainhash.HashH([]byte{byte(i)}), uint32(i), byte(i))
	}

	newSubtree := func(t *testing.T, placeholder bool, nodes ...*bt.Tx) *subtreepkg.Subtree {
		t.Helper()

		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)

		if placeholder {
			require.NoError(t, st.AddCoinbaseNode())
		}

		for _, tx := range nodes {
			require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size())))
		}

		return st
	}

	additions := func(t *testing.T, process func(utxoDiff *utxopersister.UTXOSet)) []byte {
		t.Helper()

		blockStore := memory.New()

		utxoDiff, err := utxopersister.NewUTXOSet(ctx, logger, tSettings, blockStore, &blockHash, 2000)
		require.NoError(t, err)

		process(utxoDiff)
		require.NoError(t, utxoDiff.Close())

		b, err := blockStore.Get(ctx, blockHash[:], fileformat.FileTypeUtxoAdditions)
		require.NoError(t, err)

		return b
	}

	for _, tc := range []struct {
		name      string
		subtree   *subtreepkg.Subtree
		file      []*bt.Tx
		processed []*bt.Tx // what phase 2 should have written by the time it stops
	}{
		{name: "wrong transaction at the last node", subtree: newSubtree(t, false, txs[0], txs[1]), file: []*bt.Tx{txs[0], txs[2]}, processed: []*bt.Tx{txs[0]}},
		{name: "coinbase at the last node without a placeholder", subtree: newSubtree(t, false, txs[0], txs[1]), file: []*bt.Tx{txs[0], coinbase}, processed: []*bt.Tx{txs[0]}},
		{name: "wrong transaction where the file leaves the coinbase out", subtree: newSubtree(t, true, txs[0]), file: []*bt.Tx{txs[2]}, processed: []*bt.Tx{coinbase}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subtreeBytes, err := tc.subtree.Serialize()
			require.NoError(t, err)

			var data []byte
			for _, tx := range tc.file {
				data = append(data, tx.ExtendedBytes()...)
			}

			subtreeStore := memory.New()
			require.NoError(t, subtreeStore.Set(ctx, tc.subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))
			require.NoError(t, subtreeStore.Set(ctx, tc.subtree.RootHash()[:], fileformat.FileTypeSubtreeData, data))

			persister := New(ctx, logger, tSettings, nil, subtreeStore, nil, nil)

			got := additions(t, func(utxoDiff *utxopersister.UTXOSet) {
				err := persister.ProcessSubtreeUTXOStreaming(ctx, *tc.subtree.RootHash(), coinbase, utxoDiff)
				require.ErrorContains(t, err, "transaction hash mismatch at index 1")
			})

			want := additions(t, func(utxoDiff *utxopersister.UTXOSet) {
				for _, tx := range tc.processed {
					require.NoError(t, utxoDiff.ProcessTx(tx))
				}
			})

			require.Equal(t, want, got)
		})
	}
}

// TestPersistBlock_AdditionsIncludeTheCoinbase persists the setup block, whose only subtree opens
// with the coinbase placeholder, and checks that its utxo-additions and utxo-deletions match the
// block's four transactions processed in order, coinbase first. The existing subtreeData file is
// written by Data.Serialize, which leaves the coinbase out under the placeholder. Phase 2 has to
// take the coinbase from the block, because it is not in the file.
func TestPersistBlock_AdditionsIncludeTheCoinbase(t *testing.T) {
	for _, tc := range []struct {
		name         string
		existingFile bool
	}{
		{name: "existing file without the coinbase", existingFile: true},
		{name: "file created by phase 1, with the coinbase", existingFile: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block, blockBytes, extendedTxs, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)
			ctx := t.Context()

			if !tc.existingFile {
				require.NoError(t, subtreeStore.Del(ctx, block.Subtrees[0][:], fileformat.FileTypeSubtreeData))
			}

			persister := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStore, blockchainClient)
			require.NoError(t, persister.persistBlock(ctx, block.Header.Hash(), blockBytes))

			wantStore := memory.New()

			want, err := utxopersister.NewUTXOSet(ctx, ulogger.TestLogger{}, tSettings, wantStore, block.Header.Hash(), block.Height)
			require.NoError(t, err)

			for _, tx := range extendedTxs {
				require.NoError(t, want.ProcessTx(tx))
			}

			require.NoError(t, want.Close())

			for _, fileType := range []fileformat.FileType{fileformat.FileTypeUtxoAdditions, fileformat.FileTypeUtxoDeletions} {
				wantBytes, err := wantStore.Get(ctx, block.Header.Hash()[:], fileType)
				require.NoError(t, err)

				gotBytes, err := blockStore.Get(ctx, block.Header.Hash()[:], fileType)
				require.NoError(t, err)

				require.Equal(t, wantBytes, gotBytes, "%s", fileType)
			}
		})
	}
}

// failingReadStore serves every subtreeData file through a reader that returns the first after
// bytes of the stored file and then fails with err, as a store does on a transient I/O error.
type failingReadStore struct {
	*memory.Memory
	after int
	err   error
}

func (s *failingReadStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if fileType != fileformat.FileTypeSubtreeData {
		return s.Memory.GetIoReader(ctx, key, fileType, opts...)
	}

	b, err := s.Memory.Get(ctx, key, fileType, opts...)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(io.MultiReader(bytes.NewReader(b[:s.after]), iotest.ErrReader(s.err))), nil
}

// TestCreateSubtreeDataFileStreaming_KeepsTheFileOnReadError checks that phase 1 deletes an
// existing subtreeData file only when its contents are wrong. A read error from the store, or a
// cancelled context, says nothing about the file. Deleting it there would throw away the only copy,
// and the rebuild needs the block's transactions, which the UTXO store may already have pruned.
func TestCreateSubtreeDataFileStreaming_KeepsTheFileOnReadError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		readErr error // returned by the store's reader partway through the file
		cancel  bool  // the context is cancelled before the check, and the file is short
	}{
		{name: "store read fails", readErr: io.ErrClosedPipe},
		{name: "store read is cancelled", readErr: context.Canceled},
		{name: "context cancelled while a short file is rejected", cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block, _, extendedTxs, mockUTXOStore, subtreeStore, _, blockchainClient, tSettings := setup(t)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			key := block.Subtrees[0][:]

			original, err := subtreeStore.Get(ctx, key, fileformat.FileTypeSubtreeData)
			require.NoError(t, err)

			stored := original

			var store blob.Store = subtreeStore

			if tc.readErr != nil {
				// fail inside the second transaction
				store = &failingReadStore{Memory: subtreeStore, after: len(extendedTxs[1].ExtendedBytes()) + 10, err: tc.readErr}
			} else {
				stored = original[:len(extendedTxs[1].ExtendedBytes())]
				require.NoError(t, subtreeStore.Del(ctx, key, fileformat.FileTypeSubtreeData))
				require.NoError(t, subtreeStore.Set(ctx, key, fileformat.FileTypeSubtreeData, stored))
			}

			if tc.cancel {
				cancel()
			}

			persister := New(ctx, ulogger.TestLogger{}, tSettings, memory.New(), store, mockUTXOStore, blockchainClient)

			require.Error(t, persister.CreateSubtreeDataFileStreaming(ctx, *block.Subtrees[0], block, 1))

			kept, err := subtreeStore.Get(t.Context(), key, fileformat.FileTypeSubtreeData)
			require.NoError(t, err, "the existing subtreeData file must not be deleted")
			require.Equal(t, stored, kept)
			mockUTXOStore.AssertNotCalled(t, "BatchDecorate", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

// utxoDiffFiles runs process against a fresh UTXO diff and returns the utxo-additions and
// utxo-deletions it wrote.
func utxoDiffFiles(t *testing.T, process func(utxoDiff *utxopersister.UTXOSet)) (additions, deletions []byte) {
	t.Helper()

	ctx := t.Context()
	blockStore := memory.New()
	blockHash := chainhash.DoubleHashH([]byte("utxo-diff-files"))

	utxoDiff, err := utxopersister.NewUTXOSet(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), blockStore, &blockHash, 2000)
	require.NoError(t, err)

	process(utxoDiff)
	require.NoError(t, utxoDiff.Close())

	additions, err = blockStore.Get(ctx, blockHash[:], fileformat.FileTypeUtxoAdditions)
	require.NoError(t, err)

	deletions, err = blockStore.Get(ctx, blockHash[:], fileformat.FileTypeUtxoDeletions)
	require.NoError(t, err)

	return additions, deletions
}

// runPhase2 stores subtree and data in a fresh store, runs ProcessSubtreeUTXOStreaming on them with
// coinbaseTx, and returns what it wrote and the error it returned.
func runPhase2(t *testing.T, subtree *subtreepkg.Subtree, data []byte, coinbaseTx *bt.Tx) (additions, deletions []byte, err error) {
	t.Helper()

	ctx := t.Context()

	subtreeBytes, serr := subtree.Serialize()
	require.NoError(t, serr)

	subtreeStore := memory.New()
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, data))

	persister := New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), nil, subtreeStore, nil, nil)

	additions, deletions = utxoDiffFiles(t, func(utxoDiff *utxopersister.UTXOSet) {
		err = persister.ProcessSubtreeUTXOStreaming(ctx, *subtree.RootHash(), coinbaseTx, utxoDiff)
	})

	return additions, deletions, err
}

// TestProcessSubtreeUTXOStreaming_TakesTheCoinbaseFromTheBlock checks what phase 2 writes for a
// subtree that opens with the coinbase placeholder. The block's own coinbase is processed whether
// the file opens with a coinbase, leaves it out, or holds a competing block's. The file is keyed by
// subtree hash, and competing blocks can share their first subtree. Only a block without a
// coinbase, started from a seed, falls back to the file's.
func TestProcessSubtreeUTXOStreaming_TakesTheCoinbaseFromTheBlock(t *testing.T) {
	coinbase := smallTx(t, chainhash.Hash{}, 0xffffffff, 0xcb)
	otherCoinbase := smallTx(t, chainhash.Hash{}, 0xffffffff, 0xcc)

	txs := make([]*bt.Tx, 4)
	for i := range txs {
		txs[i] = smallTx(t, chainhash.HashH([]byte{byte(i)}), uint32(i), byte(i))
	}

	newSubtree := func(nodes ...*bt.Tx) *subtreepkg.Subtree {
		st, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		require.NoError(t, st.AddCoinbaseNode())

		for _, tx := range nodes {
			require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size())))
		}

		return st
	}

	withPlaceholder := newSubtree(txs[1:]...)
	placeholderOnly := newSubtree()

	concat := func(list ...*bt.Tx) []byte {
		var out []byte
		for _, tx := range list {
			out = append(out, tx.ExtendedBytes()...)
		}

		return out
	}

	for _, tc := range []struct {
		name       string
		subtree    *subtreepkg.Subtree
		file       []byte
		coinbaseTx *bt.Tx
		processed  []*bt.Tx
	}{
		{name: "file opens with the block's coinbase", subtree: withPlaceholder, file: concat(coinbase, txs[1], txs[2], txs[3]), coinbaseTx: coinbase, processed: []*bt.Tx{coinbase, txs[1], txs[2], txs[3]}},
		{name: "file leaves the coinbase out", subtree: withPlaceholder, file: concat(txs[1], txs[2], txs[3]), coinbaseTx: coinbase, processed: []*bt.Tx{coinbase, txs[1], txs[2], txs[3]}},
		{name: "file opens with a competing block's coinbase", subtree: withPlaceholder, file: concat(otherCoinbase, txs[1], txs[2], txs[3]), coinbaseTx: coinbase, processed: []*bt.Tx{coinbase, txs[1], txs[2], txs[3]}},
		{name: "placeholder-only subtree, empty file", subtree: placeholderOnly, coinbaseTx: coinbase, processed: []*bt.Tx{coinbase}},
		{name: "placeholder-only subtree, file holds the coinbase", subtree: placeholderOnly, file: concat(coinbase), coinbaseTx: coinbase, processed: []*bt.Tx{coinbase}},
		// Phase 1 rejects this file. Phase 2 reads nothing for a placeholder-only subtree, so it no
		// longer indexes past the nodes on it (issue 1918).
		{name: "placeholder-only subtree, file holds another transaction", subtree: placeholderOnly, file: concat(txs[1]), coinbaseTx: coinbase, processed: []*bt.Tx{coinbase}},
		{name: "block without a coinbase, file opens with one", subtree: withPlaceholder, file: concat(coinbase, txs[1], txs[2], txs[3]), processed: []*bt.Tx{coinbase, txs[1], txs[2], txs[3]}},
		{name: "block without a coinbase, file leaves it out", subtree: withPlaceholder, file: concat(txs[1], txs[2], txs[3]), processed: []*bt.Tx{txs[1], txs[2], txs[3]}},
		// Nothing is read for a placeholder-only subtree, so the fallback never sees the file's
		// coinbase. The case needs a coinbase-only block, which has no subtrees.
		{name: "block without a coinbase, placeholder-only subtree, file holds one", subtree: placeholderOnly, file: concat(coinbase)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotAdditions, gotDeletions, err := runPhase2(t, tc.subtree, tc.file, tc.coinbaseTx)
			require.NoError(t, err)

			wantAdditions, wantDeletions := utxoDiffFiles(t, func(utxoDiff *utxopersister.UTXOSet) {
				for _, tx := range tc.processed {
					require.NoError(t, utxoDiff.ProcessTx(tx))
				}
			})

			require.Equal(t, wantAdditions, gotAdditions, "utxo-additions")
			require.Equal(t, wantDeletions, gotDeletions, "utxo-deletions")
		})
	}
}

// wantSetupBlockUtxoFiles returns the utxo-additions and utxo-deletions the setup block should
// persist: its four transactions, coinbase first.
func wantSetupBlockUtxoFiles(t *testing.T, block *model.Block, extendedTxs []*bt.Tx) (additions, deletions []byte) {
	t.Helper()

	ctx := t.Context()
	store := memory.New()

	utxoDiff, err := utxopersister.NewUTXOSet(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), store, block.Header.Hash(), block.Height)
	require.NoError(t, err)

	for _, tx := range extendedTxs {
		require.NoError(t, utxoDiff.ProcessTx(tx))
	}

	require.NoError(t, utxoDiff.Close())

	additions, err = store.Get(ctx, block.Header.Hash()[:], fileformat.FileTypeUtxoAdditions)
	require.NoError(t, err)

	deletions, err = store.Get(ctx, block.Header.Hash()[:], fileformat.FileTypeUtxoDeletions)
	require.NoError(t, err)

	return additions, deletions
}

// requirePersistedFiles checks which of the block's three persisted files exist.
func requirePersistedFiles(t *testing.T, blockStore blob.Store, hash *chainhash.Hash, want bool) {
	t.Helper()

	for _, fileType := range []fileformat.FileType{fileformat.FileTypeUtxoAdditions, fileformat.FileTypeUtxoDeletions, fileformat.FileTypeBlock} {
		exists, err := blockStore.Exists(t.Context(), hash[:], fileType)
		require.NoError(t, err)
		require.Equal(t, want, exists, "%s exists", fileType)
	}
}

// TestPersistBlock_RetriesCleanlyAfterAFailedAttempt checks that a failed attempt leaves none of
// the block's files behind. A utxo file finalised from a partial diff made every retry fail with
// BLOB_EXISTS, which the Server read as "already persisted", so the block was marked persisted with
// an empty UTXO diff and no block file.
func TestPersistBlock_RetriesCleanlyAfterAFailedAttempt(t *testing.T) {
	block, blockBytes, extendedTxs, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)
	ctx := t.Context()

	failing := &failingReadStore{Memory: subtreeStore, after: 10, err: io.ErrClosedPipe}
	first := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, failing, mockUTXOStore, blockchainClient)
	require.Error(t, first.persistBlock(ctx, block.Header.Hash(), blockBytes))
	requirePersistedFiles(t, blockStore, block.Header.Hash(), false)

	retry := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStore, blockchainClient)
	require.NoError(t, retry.persistBlock(ctx, block.Header.Hash(), blockBytes))
	requirePersistedFiles(t, blockStore, block.Header.Hash(), true)

	wantAdditions, wantDeletions := wantSetupBlockUtxoFiles(t, block, extendedTxs)

	gotAdditions, err := blockStore.Get(ctx, block.Header.Hash()[:], fileformat.FileTypeUtxoAdditions)
	require.NoError(t, err)
	require.Equal(t, wantAdditions, gotAdditions)

	gotDeletions, err := blockStore.Get(ctx, block.Header.Hash()[:], fileformat.FileTypeUtxoDeletions)
	require.NoError(t, err)
	require.Equal(t, wantDeletions, gotDeletions)
}

// TestPersistBlock_ClearsUtxoFilesLeftWithoutABlockFile checks the retry after an attempt that
// stopped between finalising the utxo files and writing the block file, the last file written. The
// leftover utxo files are replaced, not trusted.
func TestPersistBlock_ClearsUtxoFilesLeftWithoutABlockFile(t *testing.T) {
	block, blockBytes, extendedTxs, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)
	ctx := t.Context()

	for _, fileType := range []fileformat.FileType{fileformat.FileTypeUtxoAdditions, fileformat.FileTypeUtxoDeletions} {
		require.NoError(t, blockStore.Set(ctx, block.Header.Hash()[:], fileType, []byte("left over from an earlier attempt")))
	}

	persister := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStore, blockchainClient)
	require.NoError(t, persister.persistBlock(ctx, block.Header.Hash(), blockBytes))
	requirePersistedFiles(t, blockStore, block.Header.Hash(), true)

	wantAdditions, _ := wantSetupBlockUtxoFiles(t, block, extendedTxs)

	gotAdditions, err := blockStore.Get(ctx, block.Header.Hash()[:], fileformat.FileTypeUtxoAdditions)
	require.NoError(t, err)
	require.Equal(t, wantAdditions, gotAdditions)
}

// TestPersistBlock_ReportsABlockAlreadyPersisted checks that a block whose block file exists is
// reported as BLOB_EXISTS before any work is done, and that the Server reads BLOB_EXISTS as
// "already persisted" only when the block file is there.
func TestPersistBlock_ReportsABlockAlreadyPersisted(t *testing.T) {
	block, blockBytes, _, mockUTXOStore, subtreeStore, blockStore, blockchainClient, tSettings := setup(t)
	ctx := t.Context()
	hash := block.Header.Hash()

	persister := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStore, blockchainClient)

	blobExists := errors.NewStorageError("error creating additions file", errors.NewBlobAlreadyExistsError("utxo-additions already exists"))
	require.False(t, persister.blockAlreadyPersisted(ctx, hash, blobExists), "BLOB_EXISTS without a block file")

	require.NoError(t, blockStore.Set(ctx, hash[:], fileformat.FileTypeBlock, blockBytes))

	err := persister.persistBlock(ctx, hash, blockBytes)
	require.True(t, errors.Is(err, errors.ErrBlobAlreadyExists), "persistBlock: %v", err)
	require.True(t, persister.blockAlreadyPersisted(ctx, hash, err))
	require.False(t, persister.blockAlreadyPersisted(ctx, hash, errors.NewProcessingError("some other failure")))

	for _, fileType := range []fileformat.FileType{fileformat.FileTypeUtxoAdditions, fileformat.FileTypeUtxoDeletions} {
		exists, err := blockStore.Exists(ctx, hash[:], fileType)
		require.NoError(t, err)
		require.False(t, exists, "%s written for a block already persisted", fileType)
	}
}

// finaliseFailingStore fails to finalise one file type, as a store does when its final write or
// rename fails.
type finaliseFailingStore struct {
	*memory.Memory
	fileType fileformat.FileType
}

func (s *finaliseFailingStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	if fileType != s.fileType {
		return s.Memory.SetFromReader(ctx, key, fileType, reader, opts...)
	}

	_, _ = io.Copy(io.Discard, reader)
	_ = reader.Close()

	return errors.NewStorageError("finalising %s failed", fileType)
}

// TestPersistBlock_FailsWhenAFileDoesNotFinalise checks that persistBlock returns an error, and
// leaves no block file, when one of its files fails to finalise. The errors used to be logged and
// dropped, and the block was marked persisted without the file.
func TestPersistBlock_FailsWhenAFileDoesNotFinalise(t *testing.T) {
	for _, fileType := range []fileformat.FileType{fileformat.FileTypeUtxoAdditions, fileformat.FileTypeUtxoDeletions, fileformat.FileTypeBlock} {
		t.Run(string(fileType), func(t *testing.T) {
			block, blockBytes, _, mockUTXOStore, subtreeStore, _, blockchainClient, tSettings := setup(t)
			ctx := t.Context()

			blockStore := &finaliseFailingStore{Memory: memory.New(), fileType: fileType}

			persister := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, subtreeStore, mockUTXOStore, blockchainClient)
			require.Error(t, persister.persistBlock(ctx, block.Header.Hash(), blockBytes))

			exists, err := blockStore.Exists(ctx, block.Header.Hash()[:], fileformat.FileTypeBlock)
			require.NoError(t, err)
			require.False(t, exists, "block file written although %s did not finalise", fileType)
		})
	}
}
