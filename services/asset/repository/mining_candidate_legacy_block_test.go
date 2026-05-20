package repository

import (
	"bytes"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestGetMiningCandidateLegacyBlockReader(t *testing.T) {
	tracing.SetupMockTracer()

	t.Run("streams header + varint + coinbase for coinbase-only block", func(t *testing.T) {
		ctx := setup(t)

		header := make([]byte, model.BlockHeaderSize)
		header[0] = 0x01

		coinbaseTxBytes := coinbase.Bytes()
		txCount := uint64(1)

		r, err := ctx.repo.GetMiningCandidateLegacyBlockReader(t.Context(), header, coinbaseTxBytes, nil, txCount)
		require.NoError(t, err)

		var buf bytes.Buffer
		_, err = io.Copy(&buf, r)
		require.NoError(t, err)

		data := buf.Bytes()

		require.True(t, len(data) >= model.BlockHeaderSize)
		require.Equal(t, header, data[:model.BlockHeaderSize])

		offset := model.BlockHeaderSize
		txCountVarInt := bt.VarInt(txCount)
		txCountBytes := txCountVarInt.Bytes()
		require.Equal(t, txCountBytes, data[offset:offset+len(txCountBytes)])
		offset += len(txCountBytes)

		require.Equal(t, coinbaseTxBytes, data[offset:])
	})

	t.Run("empty block produces varint zero", func(t *testing.T) {
		ctx := setup(t)

		r, err := ctx.repo.GetMiningCandidateLegacyBlockReader(t.Context(), []byte{}, []byte{}, nil, 0)
		require.NoError(t, err)

		var buf bytes.Buffer
		_, err = io.Copy(&buf, r)
		require.NoError(t, err)

		require.Equal(t, []byte{0x00}, buf.Bytes())
	})

	t.Run("returns error for invalid subtree hash", func(t *testing.T) {
		ctx := setup(t)

		header := make([]byte, model.BlockHeaderSize)
		coinbaseTxBytes := coinbase.Bytes()
		badHash := []byte{0x01, 0x02, 0x03}

		r, err := ctx.repo.GetMiningCandidateLegacyBlockReader(t.Context(), header, coinbaseTxBytes, [][]byte{badHash}, 2)
		require.NoError(t, err)

		var buf bytes.Buffer
		_, err = io.Copy(&buf, r)
		require.Error(t, err)
	})

	t.Run("streams transactions from subtree data store", func(t *testing.T) {
		ctx := setup(t)

		// Create a subtree with coinbase + tx1
		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, st.AddCoinbaseNode())
		require.NoError(t, st.AddNode(*tx1.TxIDChainHash(), 100, 0))

		// Create subtree data with only the non-coinbase transaction
		subtreeData := subtreepkg.NewSubtreeData(st)
		require.NoError(t, subtreeData.AddTx(tx1, 1))

		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)

		err = ctx.repo.SubtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
		require.NoError(t, err)

		// Build header and coinbase
		header := make([]byte, model.BlockHeaderSize)
		coinbaseTxBytes := coinbase.Bytes()
		txCount := uint64(2) // coinbase + tx1

		r, err := ctx.repo.GetMiningCandidateLegacyBlockReader(t.Context(), header, coinbaseTxBytes, [][]byte{st.RootHash()[:]}, txCount)
		require.NoError(t, err)

		var buf bytes.Buffer
		_, err = io.Copy(&buf, r)
		require.NoError(t, err)

		data := buf.Bytes()

		// Parse: header(80) + varint(2) + coinbase + tx1
		offset := model.BlockHeaderSize
		require.Equal(t, byte(0x02), data[offset]) // VarInt(2)
		offset++

		// Read coinbase
		parsedCoinbase, coinbaseSize, err := bt.NewTxFromStream(data[offset:])
		require.NoError(t, err)
		require.Equal(t, coinbase.TxID(), parsedCoinbase.TxID())
		offset += coinbaseSize

		// Read tx1
		parsedTx1, _, err := bt.NewTxFromStream(data[offset:])
		require.NoError(t, err)
		require.Equal(t, tx1.TxID(), parsedTx1.TxID())
	})
}

func TestWriteMiningCandidateBlock(t *testing.T) {
	tracing.SetupMockTracer()

	t.Run("writes complete block structure", func(t *testing.T) {
		ctx := setup(t)

		header := make([]byte, model.BlockHeaderSize)
		for i := range header {
			header[i] = byte(i % 256)
		}

		coinbaseTxBytes := coinbase.Bytes()
		txCount := uint64(1)

		var buf bytes.Buffer
		err := ctx.repo.writeMiningCandidateBlock(t.Context(), &buf, header, coinbaseTxBytes, nil, txCount)
		require.NoError(t, err)

		data := buf.Bytes()

		require.Equal(t, header, data[:model.BlockHeaderSize])

		offset := model.BlockHeaderSize
		require.Equal(t, byte(0x01), data[offset])
		offset++

		require.Equal(t, coinbaseTxBytes, data[offset:])
	})
}

func TestStreamSubtreeDataSkipCoinbase(t *testing.T) {
	tracing.SetupMockTracer()

	t.Run("skips coinbase and writes remaining transactions", func(t *testing.T) {
		ctx := setup(t)

		// Create subtree with coinbase placeholder + tx1
		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, st.AddCoinbaseNode())
		require.NoError(t, st.AddNode(*tx1.TxIDChainHash(), 100, 0))

		// Create subtree data including coinbase (to verify it gets skipped)
		subtreeData := subtreepkg.NewSubtreeData(st)
		require.NoError(t, subtreeData.AddTx(coinbase, 0))
		require.NoError(t, subtreeData.AddTx(tx1, 1))

		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)

		err = ctx.repo.SubtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
		require.NoError(t, err)

		var buf bytes.Buffer
		err = ctx.repo.streamSubtreeDataSkipCoinbase(t.Context(), &buf, st.RootHash())
		require.NoError(t, err)

		// Should only contain tx1, not coinbase
		parsedTx, _, err := bt.NewTxFromStream(buf.Bytes())
		require.NoError(t, err)
		require.Equal(t, tx1.TxID(), parsedTx.TxID())

		// Verify the coinbase was NOT included (buf should have exactly tx1's bytes)
		require.Equal(t, tx1.Bytes(), buf.Bytes())
	})
}

func TestStreamSubtreeTransactions(t *testing.T) {
	tracing.SetupMockTracer()

	t.Run("uses subtree data when available", func(t *testing.T) {
		ctx := setup(t)

		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, st.AddCoinbaseNode())
		require.NoError(t, st.AddNode(*tx1.TxIDChainHash(), 100, 0))

		subtreeData := subtreepkg.NewSubtreeData(st)
		require.NoError(t, subtreeData.AddTx(tx1, 1))

		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)

		err = ctx.repo.SubtreeStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes)
		require.NoError(t, err)

		var buf bytes.Buffer
		err = ctx.repo.streamSubtreeTransactions(t.Context(), &buf, st.RootHash()[:])
		require.NoError(t, err)

		parsedTx, _, err := bt.NewTxFromStream(buf.Bytes())
		require.NoError(t, err)
		require.Equal(t, tx1.TxID(), parsedTx.TxID())
	})

	t.Run("returns error for non-existent subtree", func(t *testing.T) {
		ctx := setup(t)

		fakeHash := make([]byte, 32)
		fakeHash[0] = 0xDE

		var buf bytes.Buffer
		err := ctx.repo.streamSubtreeTransactions(t.Context(), &buf, fakeHash)
		require.Error(t, err)
	})
}
