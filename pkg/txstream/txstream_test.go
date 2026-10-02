package txstream

import (
	"bytes"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// sampleTx builds a transaction with two inputs and three outputs: a spendable one, an OP_FALSE
// OP_RETURN data output of dataLen bytes, and a bare OP_RETURN one.
func sampleTx(t *testing.T, seed byte, dataLen int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.Version = 2
	tx.LockTime = 700000 + uint32(seed)

	for i := 0; i < 2; i++ {
		prev := chainhash.Hash{seed, byte(i), 1}
		in := &bt.Input{PreviousTxOutIndex: uint32(i), SequenceNumber: 0xfffffffe}
		require.NoError(t, in.PreviousTxIDAdd(&prev))
		in.UnlockingScript = bscript.NewFromBytes([]byte{0x51, seed, byte(i)})
		in.PreviousTxSatoshis = 1000 + uint64(i)
		in.PreviousTxScript = bscript.NewFromBytes([]byte{0x76, 0xa9, seed})
		tx.Inputs = append(tx.Inputs, in)
	}

	data := make([]byte, 2+dataLen)
	data[0], data[1] = bscript.OpFALSE, bscript.OpRETURN

	for i := range dataLen {
		data[2+i] = byte(i) ^ seed
	}

	tx.Outputs = append(tx.Outputs,
		&bt.Output{Satoshis: 500, LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, seed})},
		&bt.Output{Satoshis: 0, LockingScript: bscript.NewFromBytes(data)},
		&bt.Output{Satoshis: 0, LockingScript: bscript.NewFromBytes([]byte{bscript.OpRETURN, seed})},
	)

	return tx
}

// Standard and extended forms both parse to the transaction go-bt reads, with the id go-bt computes
// by serializing it again.
func TestReadsBothFormsWithTheIdGoBtComputes(t *testing.T) {
	var stream bytes.Buffer

	want := []*bt.Tx{sampleTx(t, 1, 300), sampleTx(t, 2, 0), sampleTx(t, 3, 70000)}

	stream.Write(want[0].Bytes())
	stream.Write(want[1].ExtendedBytes())
	stream.Write(want[2].Bytes())

	r := NewReader(&stream)

	for i, w := range want {
		got, id, size, err := r.Next(Options{})
		require.NoError(t, err, i)

		require.Equal(t, *w.TxIDChainHash(), *id, i)
		require.Equal(t, *id, *got.TxIDChainHash(), i)
		require.Equal(t, int64(len(w.Bytes())), size, i)
		require.Equal(t, w.Bytes(), got.Bytes(), i)
		require.Equal(t, i == 1, got.IsExtended(), i)

		if i == 1 {
			require.Equal(t, w.ExtendedBytes(), got.ExtendedBytes())
		}
	}

	_, _, _, err := r.Next(Options{})
	require.ErrorIs(t, err, io.EOF)
}

// A transaction cut short is an error, never a clean end of stream.
func TestATruncatedTransactionIsNotACleanEnd(t *testing.T) {
	raw := sampleTx(t, 4, 100).Bytes()

	_, _, _, err := NewReader(bytes.NewReader(raw[:len(raw)-3])).Next(Options{})
	require.Error(t, err)
	require.NotErrorIs(t, err, io.EOF)
}

// With a writer from BeforeOutputs, the outputs and lock time are copied to it byte for byte, data
// scripts are not kept, and the id and size are still those of the whole transaction.
func TestOutputsAreCopiedAndDataScriptsNotKept(t *testing.T) {
	tx := sampleTx(t, 5, 100000)
	raw := tx.Bytes()

	var copied bytes.Buffer

	var seenInputs int

	got, id, size, err := NewReader(bytes.NewReader(raw)).Next(Options{
		BeforeOutputs: func(partial *bt.Tx) (io.Writer, error) {
			seenInputs = len(partial.Inputs)
			require.Empty(t, partial.Outputs)

			return &copied, nil
		},
		SkipDataScripts: true,
	})
	require.NoError(t, err)

	require.Equal(t, 2, seenInputs)
	require.Equal(t, *tx.TxIDChainHash(), *id)
	require.Equal(t, int64(len(raw)), size)

	outputsStart := len(raw) - (bt.VarInt(3).Length() + tx.Outputs[0].Size() + tx.Outputs[1].Size() + tx.Outputs[2].Size() + 4)
	require.Equal(t, raw[outputsStart:], copied.Bytes())

	require.Equal(t, tx.Outputs[0].LockingScript, got.Outputs[0].LockingScript)
	require.Same(t, &DataOutputScript, got.Outputs[1].LockingScript)
	require.Equal(t, uint64(0), got.Outputs[1].Satoshis)
	require.Equal(t, tx.Outputs[2].LockingScript, got.Outputs[2].LockingScript, "a bare OP_RETURN is not an OP_FALSE OP_RETURN")
}

// Without a writer there is nowhere for the bytes to go, so data scripts are kept.
func TestDataScriptsAreKeptWithoutAWriter(t *testing.T) {
	tx := sampleTx(t, 6, 50)

	got, _, _, err := NewReader(bytes.NewReader(tx.Bytes())).Next(Options{
		BeforeOutputs:   func(*bt.Tx) (io.Writer, error) { return nil, nil },
		SkipDataScripts: true,
	})
	require.NoError(t, err)
	require.Equal(t, tx.Outputs[1].LockingScript, got.Outputs[1].LockingScript)
}
