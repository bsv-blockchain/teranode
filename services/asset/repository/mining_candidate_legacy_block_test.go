package repository

import (
	"bytes"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestGetMiningCandidateLegacyBlockReader(t *testing.T) {
	tracing.SetupMockTracer()

	t.Run("streams header + varint + coinbase for coinbase-only block", func(t *testing.T) {
		ctx := setup(t)

		// Build a minimal 80-byte header
		header := make([]byte, model.BlockHeaderSize)
		header[0] = 0x01 // version byte

		coinbaseTxBytes := coinbase.Bytes()
		txCount := uint64(1) // just the coinbase

		r, err := ctx.repo.GetMiningCandidateLegacyBlockReader(t.Context(), header, coinbaseTxBytes, nil, txCount)
		require.NoError(t, err)

		// Read entire output
		var buf bytes.Buffer
		_, err = io.Copy(&buf, r)
		require.NoError(t, err)

		data := buf.Bytes()

		// Verify header
		require.True(t, len(data) >= model.BlockHeaderSize, "output too short for header")
		require.Equal(t, header, data[:model.BlockHeaderSize])

		// After the header: VarInt(1) = 0x01
		offset := model.BlockHeaderSize
		txCountVarInt := bt.VarInt(txCount)
		txCountBytes := txCountVarInt.Bytes()
		require.Equal(t, txCountBytes, data[offset:offset+len(txCountBytes)])
		offset += len(txCountBytes)

		// Then the coinbase tx
		require.Equal(t, coinbaseTxBytes, data[offset:])
	})

	t.Run("returns error for nil header", func(t *testing.T) {
		ctx := setup(t)

		// Empty header should still work — it's just bytes being written
		r, err := ctx.repo.GetMiningCandidateLegacyBlockReader(t.Context(), []byte{}, []byte{}, nil, 0)
		require.NoError(t, err)

		var buf bytes.Buffer
		_, err = io.Copy(&buf, r)
		require.NoError(t, err)

		// VarInt(0) = 0x00
		require.Equal(t, []byte{0x00}, buf.Bytes())
	})

	t.Run("returns error for invalid subtree hash", func(t *testing.T) {
		ctx := setup(t)

		header := make([]byte, model.BlockHeaderSize)
		coinbaseTxBytes := coinbase.Bytes()

		// Invalid subtree hash (too short)
		badHash := []byte{0x01, 0x02, 0x03}

		r, err := ctx.repo.GetMiningCandidateLegacyBlockReader(t.Context(), header, coinbaseTxBytes, [][]byte{badHash}, 2)
		require.NoError(t, err)

		var buf bytes.Buffer
		_, err = io.Copy(&buf, r)
		require.Error(t, err) // pipe should close with error from invalid hash
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

		// Verify structure: header(80) + varint(1) + coinbase
		require.Equal(t, header, data[:model.BlockHeaderSize])

		offset := model.BlockHeaderSize
		require.Equal(t, byte(0x01), data[offset]) // VarInt(1)
		offset++

		require.Equal(t, coinbaseTxBytes, data[offset:])
	})
}
