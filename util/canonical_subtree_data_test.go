package util

import (
	"bytes"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestCheckCanonicalSubtreeData(t *testing.T) {
	payload := []byte{1, 2, 3, 4, 5}

	t.Run("matching sizes are accepted", func(t *testing.T) {
		require.NoError(t, CheckCanonicalSubtreeData(int64(len(payload)), payload, nil))
	})

	t.Run("longer wire form than canonical serialization is rejected", func(t *testing.T) {
		err := CheckCanonicalSubtreeData(int64(len(payload))+2, payload, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not canonically encoded")
		require.True(t, errors.Is(err, errors.ErrProcessing))
		// Must NOT be ErrTxInvalid: that routes to storeInvalidBlock and would
		// permanently invalidate a valid block over a delivery-level fault.
		require.False(t, errors.Is(err, errors.ErrTxInvalid))
	})

	// The first subtree of a block carries a coinbase the parser consumes but
	// the serialization deliberately omits. Without accounting for it, a
	// perfectly canonical payload is rejected — a hard abort on the ordinary
	// block path, reported as a peer serving malformed data.
	t.Run("consumed coinbase omitted from serialization is accounted for", func(t *testing.T) {
		coinbase := bt.NewTx()
		in := &bt.Input{PreviousTxOutIndex: 0xffffffff, UnlockingScript: bscript.NewFromBytes(make([]byte, 10))}
		require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{}))
		coinbase.Inputs = append(coinbase.Inputs, in)
		coinbase.Outputs = append(coinbase.Outputs, &bt.Output{Satoshis: 50, LockingScript: bscript.NewFromBytes(make([]byte, 25))})

		consumed := int64(len(payload) + coinbase.Size())

		require.NoError(t, CheckCanonicalSubtreeData(consumed, payload, coinbase),
			"a canonical first-subtree payload must not be rejected")

		// A non-minimal coinbase is still caught: its wire form is longer than
		// the canonical size added back.
		require.Error(t, CheckCanonicalSubtreeData(consumed+2, payload, coinbase))
	})
}

func TestCountingReaderCountsExactly(t *testing.T) {
	payload := make([]byte, 5000)
	for i := range payload {
		payload[i] = byte(i)
	}

	c := NewCountingReader(bytes.NewReader(payload))

	read, err := io.Copy(io.Discard, c)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), read)
	require.Equal(t, int64(len(payload)), c.BytesConsumed())
}
