package repository

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// TestWriteChunkToWriter_BodylessTxIsRefusedNotPanicked covers the record that
// exists with no serialized body — the steady state once a body window has aged
// out, and for every transaction mined at or below the checkpoint when
// utxostore_skipTxBodyBelowCheckpoint is on. getTxs does not count such a record
// as missed, because the record is there, so it reaches this writer with Tx nil
// and Tx.WriteTo used to dereference it and take the streaming writer down.
func TestWriteChunkToWriter_BodylessTxIsRefusedNotPanicked(t *testing.T) {
	repo := &Repository{}

	hash := chainhash.HashH([]byte("bodyless"))
	buf := &bytes.Buffer{}

	err := repo.writeChunkToWriter(t.Context(), buf, nil,
		[]chainhash.Hash{hash}, []*meta.Data{{Tx: nil}}, 0)

	require.Error(t, err)
	require.Contains(t, err.Error(), "not retained in full")
	require.Contains(t, err.Error(), hash.String())
}

// TestWriteChunkToWriter_WithBodyStillWrites keeps the guard honest: a record
// that does carry its body is written exactly as before.
func TestWriteChunkToWriter_WithBodyStillWrites(t *testing.T) {
	repo := &Repository{}

	tx, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	buf := &bytes.Buffer{}

	require.NoError(t, repo.writeChunkToWriter(t.Context(), buf, nil,
		[]chainhash.Hash{*tx.TxIDChainHash()}, []*meta.Data{{Tx: tx}}, 0))
	require.Equal(t, tx.Bytes(), buf.Bytes())
}
