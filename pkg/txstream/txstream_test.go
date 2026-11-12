package txstream

import (
	"bytes"
	"io"
	"runtime"
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

// With StandardOnly, a transaction in the extended form is refused with ErrExtendedForm before
// any output is read, and a standard one is read as before. A block on the wire carries the
// standard form only (review of 2026-10-12).
func TestStandardOnlyRefusesTheExtendedForm(t *testing.T) {
	var stream bytes.Buffer

	standard := sampleTx(t, 1, 10)
	stream.Write(standard.Bytes())
	stream.Write(sampleTx(t, 2, 10).ExtendedBytes())

	r := NewReader(&stream)

	got, _, _, err := r.Next(Options{StandardOnly: true})
	require.NoError(t, err)
	require.Equal(t, standard.Bytes(), got.Bytes())

	called := false

	_, _, _, err = r.Next(Options{StandardOnly: true, BeforeOutputs: func(*bt.Tx) (io.Writer, error) {
		called = true
		return nil, nil
	}})
	require.ErrorAs(t, err, new(ExtendedFormError))
	require.False(t, called, "refused before any output is read")
}

// A declared script length is the peer's word, not proof the bytes exist. A 61-byte transaction
// declaring a 512 MiB locking script, and then ending, must fail without allocating the 512 MiB:
// the script is read in growing chunks, and is allocated whole only once a quarter of it has
// arrived. The 64 MiB cases pin that quarter: just under it the whole script is never allocated,
// just over it it is.
func TestADeclaredScriptLengthIsNotAllocatedBeforeItsBytesArrive(t *testing.T) {
	var header bytes.Buffer

	header.Write([]byte{1, 0, 0, 0})             // version
	header.WriteByte(1)                          // one input
	header.Write(make([]byte, 32+4))             // previous outpoint
	header.Write([]byte{1, 0x51})                // unlocking script OP_TRUE
	header.Write([]byte{0xff, 0xff, 0xff, 0xff}) // sequence
	header.WriteByte(1)                          // one output
	header.Write(make([]byte, 8))                // satoshis

	declare512MiB := []byte{0xfe, 0x00, 0x00, 0x00, 0x20}
	declare64MiB := []byte{0xfe, 0x00, 0x00, 0x00, 0x04}

	for _, tc := range []struct {
		name     string
		declared []byte
		sent     int
		below    uint64 // TotalAlloc must stay under this, when set
		atLeast  uint64 // TotalAlloc must reach this, when set
	}{
		{name: "512 MiB declared, none of it sent", declared: declare512MiB, sent: 0, below: 1 << 20},
		{name: "512 MiB declared, 1 MiB of it sent", declared: declare512MiB, sent: 1 << 20, below: 4 << 20},
		{name: "64 MiB declared, just under a quarter sent", declared: declare64MiB, sent: 16<<20 - 1<<10, below: 32 << 20},
		{name: "64 MiB declared, just over a quarter sent", declared: declare64MiB, sent: 16<<20 + 1<<10, atLeast: 64 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := append(bytes.Clone(header.Bytes()), tc.declared...)
			stream = append(stream, make([]byte, tc.sent)...)

			var before, after runtime.MemStats

			runtime.ReadMemStats(&before)
			_, _, _, err := NewReader(bytes.NewReader(stream)).Next(Options{})
			runtime.ReadMemStats(&after)

			require.ErrorIs(t, err, io.ErrUnexpectedEOF)

			allocated := after.TotalAlloc - before.TotalAlloc
			if tc.below > 0 {
				require.Less(t, allocated, tc.below, "allocated for bytes that never arrived")
			}

			if tc.atLeast > 0 {
				require.GreaterOrEqual(t, allocated, tc.atLeast, "the whole script was not allocated once a quarter arrived")
			}
		})
	}
}

// A script larger than several growth steps, read partly as a proof prefix and partly straight into
// its final buffer, comes back byte for byte.
func TestAScriptSpanningManyChunksReadsWhole(t *testing.T) {
	tx := sampleTx(t, 7, 5*scriptChunk+123)

	got, id, _, err := NewReader(bytes.NewReader(tx.Bytes())).Next(Options{})
	require.NoError(t, err)
	require.Equal(t, *tx.TxIDChainHash(), *id)
	require.Equal(t, tx.Outputs[1].LockingScript, got.Outputs[1].LockingScript)
}
