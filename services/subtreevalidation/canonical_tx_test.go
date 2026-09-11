package subtreevalidation

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// canonicalTxHex is a 1-input, 1-output transaction in minimal encoding.
const canonicalTxHex = "010000000100000000000000000000000000000000000000000000000000000000000000000000000000ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000"

// withInputCountPrefix rebuilds the fixture with the input-count CompactSize
// rewritten as the given (over-long) byte sequence.
func withInputCountPrefix(t *testing.T, prefix []byte) []byte {
	t.Helper()

	raw, err := hex.DecodeString(canonicalTxHex)
	require.NoError(t, err)

	out := append([]byte{}, raw[:4]...) // version
	out = append(out, prefix...)        // non-minimal input count
	out = append(out, raw[5:]...)       // the rest, unchanged

	return out
}

// TestCheckCanonicalTxEncoding pins issue 1421. go-bt accepts a non-minimal
// CompactSize and then re-serializes it canonically, so the txid Teranode
// computes matches the canonical one — every hash-based check downstream
// passes and the canonicalized bytes are stored, leaving no trace that this
// node accepted an encoding SV Node rejects at parse. The parse is the only
// place that can still see it, so the check must reject the wire bytes rather
// than accept the canonicalized transaction. It says nothing about whether the
// block is valid — see checkCanonicalTxEncoding for why this is a
// ProcessingError and not a TxInvalidError.
func TestCheckCanonicalTxEncoding(t *testing.T) {
	t.Run("canonical encoding is accepted", func(t *testing.T) {
		raw, err := hex.DecodeString(canonicalTxHex)
		require.NoError(t, err)

		tx, err := bt.NewTxFromBytes(raw)
		require.NoError(t, err)

		require.NoError(t, checkCanonicalTxEncoding(tx, int64(len(raw))))
	})

	t.Run("two-byte form of a one-byte count is rejected", func(t *testing.T) {
		raw := withInputCountPrefix(t, []byte{0xfd, 0x01, 0x00})

		tx, err := bt.NewTxFromBytes(raw)
		require.NoError(t, err, "go-bt accepts the non-minimal form — that is the hazard")

		err = checkCanonicalTxEncoding(tx, int64(len(raw)))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not canonically encoded")
		require.True(t, errors.Is(err, errors.ErrProcessing))
		// Must NOT be ErrTxInvalid: that routes to storeInvalidBlock and would
		// permanently invalidate a valid block over a delivery-level fault.
		require.False(t, errors.Is(err, errors.ErrTxInvalid))
	})

	t.Run("eight-byte form of a one-byte count is rejected", func(t *testing.T) {
		raw := withInputCountPrefix(t, []byte{0xff, 0x01, 0, 0, 0, 0, 0, 0, 0})

		tx, err := bt.NewTxFromBytes(raw)
		require.NoError(t, err)

		err = checkCanonicalTxEncoding(tx, int64(len(raw)))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not canonically encoded")
	})

	t.Run("nil transaction is rejected", func(t *testing.T) {
		require.Error(t, checkCanonicalTxEncoding(nil, 0))
	})
}

// TestNonMinimalTxKeepsCanonicalTxID documents the property that makes this a
// consensus hazard rather than a curiosity: the non-minimal bytes parse to a
// transaction whose txid is identical to the canonical one, so a merkle root
// built from canonical txids validates against non-canonical wire bytes.
func TestNonMinimalTxKeepsCanonicalTxID(t *testing.T) {
	raw, err := hex.DecodeString(canonicalTxHex)
	require.NoError(t, err)

	canonical, err := bt.NewTxFromBytes(raw)
	require.NoError(t, err)

	nonMinimal, err := bt.NewTxFromBytes(withInputCountPrefix(t, []byte{0xfd, 0x01, 0x00}))
	require.NoError(t, err)

	require.Equal(t, canonical.TxID(), nonMinimal.TxID(),
		"go-bt canonicalizes on re-serialization, so the merkle check cannot catch this — the parse path must")
}

// TestReadTransactionsFromSubtreeDataStreamRejectsNonCanonical drives the real
// block-path ingestion function, not just the helper: a stream whose first
// transaction carries a 3-byte input-count prefix must be rejected. go-bt
// accepts and canonicalizes that encoding, so the txid still matches and every
// hash-based check downstream would pass — this parse is the only place it can
// be caught.
func TestReadTransactionsFromSubtreeDataStreamRejectsNonCanonical(t *testing.T) {
	raw, err := hex.DecodeString(canonicalTxHex)
	require.NoError(t, err)

	tx, err := bt.NewTxFromBytes(raw)
	require.NoError(t, err)

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 1, uint64(len(raw))))

	server := &Server{logger: ulogger.TestLogger{}}
	arena := bt.NewArena(4096)

	t.Run("canonical stream is accepted", func(t *testing.T) {
		var txs []*bt.Tx

		_, err := server.readTransactionsFromSubtreeDataStream(st, bytes.NewReader(raw), &txs, arena)
		require.NoError(t, err)
		require.Len(t, txs, 1)
	})

	t.Run("non-minimal stream is rejected", func(t *testing.T) {
		var txs []*bt.Tx

		_, err := server.readTransactionsFromSubtreeDataStream(st, bytes.NewReader(withInputCountPrefix(t, []byte{0xfd, 0x01, 0x00})), &txs, arena)
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-canonical transaction")
	})
}
