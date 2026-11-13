package utxopersister

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func frameTestWrappers() []*UTXOWrapper {
	big := make([]byte, maxExactScriptAlloc+10)
	for i := range big {
		big[i] = byte(i)
	}

	return []*UTXOWrapper{
		{TxID: chainhash.HashH([]byte("empty")), Height: 3},
		{
			TxID:     chainhash.HashH([]byte("coinbase")),
			Height:   42,
			Coinbase: true,
			UTXOs: []*UTXO{
				{Index: 0, Value: 5000000000, Script: []byte{0x51}},
				{Index: 7, Value: 0, Script: []byte{}},
			},
		},
		{
			TxID:   chainhash.HashH([]byte("large-script")),
			Height: 100,
			UTXOs:  []*UTXO{{Index: 200, Value: 1, Script: big}},
		},
	}
}

func requireSameWrapper(t *testing.T, want, got *UTXOWrapper) {
	t.Helper()

	require.Equal(t, want.TxID, got.TxID)
	require.Equal(t, want.Height, got.Height)
	require.Equal(t, want.Coinbase, got.Coinbase)
	require.Len(t, got.UTXOs, len(want.UTXOs))

	for i := range want.UTXOs {
		require.Equal(t, want.UTXOs[i].Index, got.UTXOs[i].Index)
		require.Equal(t, want.UTXOs[i].Value, got.UTXOs[i].Value)
		require.Equal(t, len(want.UTXOs[i].Script), len(got.UTXOs[i].Script))
		require.True(t, bytes.Equal(want.UTXOs[i].Script, got.UTXOs[i].Script))
	}
}

// Framing then decoding must produce exactly what the streaming decoder does,
// and the frame must be exactly the record's bytes.
func TestReadUTXOWrapperFrame_RoundTrip(t *testing.T) {
	var stream bytes.Buffer

	wrappers := frameTestWrappers()
	for _, w := range wrappers {
		stream.Write(w.Bytes())
	}

	r := bytes.NewReader(stream.Bytes())

	for _, w := range wrappers {
		frame, maxIndex, err := ReadUTXOWrapperFrame(r, nil)
		require.NoError(t, err)
		require.Equal(t, w.Bytes(), frame)

		var wantMax uint32
		for _, u := range w.UTXOs {
			wantMax = max(wantMax, u.Index)
		}

		require.Equal(t, wantMax, maxIndex)

		got, err := DecodeUTXOWrapperFrame(frame)
		require.NoError(t, err)
		requireSameWrapper(t, w, got)
	}

	_, _, err := ReadUTXOWrapperFrame(r, nil)
	require.ErrorIs(t, err, io.EOF)
}

// The end-of-stream contract matches FromReader: a record stream followed by
// the 16-byte footer ends with *ErrRecordBoundary carrying the footer bytes.
func TestReadUTXOWrapperFrame_FooterBoundary(t *testing.T) {
	w := frameTestWrappers()[1]
	footer := bytes.Repeat([]byte{0xab}, FooterSize)

	r := bytes.NewReader(append(w.Bytes(), footer...))

	_, _, err := ReadUTXOWrapperFrame(r, nil)
	require.NoError(t, err)

	_, _, err = ReadUTXOWrapperFrame(r, nil)

	var boundary *ErrRecordBoundary

	require.True(t, errors.As(err, &boundary), "got %v", err)
	require.Equal(t, footer, boundary.FooterBytes[:])
}

// Only a tail of exactly FooterSize bytes after the last record is the footer.
// A reader of a pipe cannot seek to check, so this gate is all that stops a
// partly written footer, padded with zeros, decoding as plausible counts.
func TestReadUTXOWrapperFrame_ShortTailIsNotFooter(t *testing.T) {
	record := frameTestWrappers()[1].Bytes()

	for tail := 1; tail < 32; tail++ {
		if tail == FooterSize {
			continue
		}

		r := bytes.NewReader(append(append([]byte{}, record...), make([]byte, tail)...))

		_, _, err := ReadUTXOWrapperFrame(r, nil)
		require.NoError(t, err)

		_, _, err = ReadUTXOWrapperFrame(r, nil)
		require.Error(t, err, "tail of %d bytes", tail)

		var boundary *ErrRecordBoundary
		require.False(t, errors.As(err, &boundary), "a %d-byte tail must not be taken for the %d-byte footer", tail, FooterSize)
	}
}

// A record cut anywhere inside must fail rather than yield a short frame.
func TestReadUTXOWrapperFrame_TruncatedRecordFails(t *testing.T) {
	full := frameTestWrappers()[1].Bytes()

	for cut := 33; cut < len(full); cut++ {
		_, _, err := ReadUTXOWrapperFrame(bytes.NewReader(full[:cut]), nil)
		require.Error(t, err, "cut at %d", cut)

		var boundary *ErrRecordBoundary
		require.False(t, errors.As(err, &boundary), "cut at %d must not look like the footer boundary", cut)
	}
}

// Reading into a reused buffer must give the same frames as fresh reads: the
// buffer is reset per record and grown when a record does not fit.
func TestReadUTXOWrapperFrame_ReusedBuffer(t *testing.T) {
	var stream bytes.Buffer

	wrappers := frameTestWrappers()
	for _, w := range wrappers {
		stream.Write(w.Bytes())
	}

	r := bytes.NewReader(stream.Bytes())
	buf := make([]byte, 0, 8) // too small for any record

	for _, w := range wrappers {
		frame, _, err := ReadUTXOWrapperFrame(r, buf)
		require.NoError(t, err)
		require.Equal(t, w.Bytes(), frame)

		buf = frame
	}
}

// Scripts alias the frame (no per-script copy) but are capacity-limited, so an
// append on one script can never overwrite the next field.
func TestDecodeUTXOWrapperFrame_ScriptsAliasFrame(t *testing.T) {
	frame := frameTestWrappers()[1].Bytes()

	w, err := DecodeUTXOWrapperFrame(frame)
	require.NoError(t, err)

	s := w.UTXOs[0].Script
	require.Equal(t, len(s), cap(s))

	frame[len(frame)-len(w.UTXOs[1].Script)-16-1] ^= 0xff // the first script's single byte
	require.NotEqual(t, byte(0x51), w.UTXOs[0].Script[0], "script must be a view of the frame")
}

// A malformed frame fails cleanly, and a bogus UTXO count cannot force an
// allocation larger than the frame could possibly hold.
func TestDecodeUTXOWrapperFrame_RejectsMalformed(t *testing.T) {
	good := frameTestWrappers()[1].Bytes()

	for name, frame := range map[string][]byte{
		"empty":          {},
		"short header":   good[:39],
		"trailing bytes": append(append([]byte{}, good...), 0x00),
		"truncated":      good[:len(good)-1],
		"huge count":     wrapperHeader(0xFFFFFFFF),
		"huge script":    wrapperWithScriptLen(1, 0xFFFFFFFF),
	} {
		_, err := DecodeUTXOWrapperFrame(frame)
		require.Error(t, err, name)
	}
}

// FuzzUTXOWrapperFrame pins the frame path to the streaming decoder: on any
// input, framing + decoding succeeds exactly when NewUTXOWrapperFromBytes does,
// with the same content, and the frame is exactly the bytes consumed.
func FuzzUTXOWrapperFrame(f *testing.F) {
	for _, w := range frameTestWrappers()[:2] {
		f.Add(w.Bytes())
	}

	f.Add(wrapperHeader(0xFFFFFFFF))
	f.Add(wrapperWithScriptLen(1, 0xFFFFFFFF))
	f.Add([]byte{})
	f.Add(make([]byte, 40))

	f.Fuzz(func(t *testing.T, data []byte) {
		want, wantErr := NewUTXOWrapperFromBytes(data)

		frame, _, frameErr := ReadUTXOWrapperFrame(bytes.NewReader(data), nil)
		if wantErr != nil {
			require.Error(t, frameErr, "frame read succeeded where the streaming decoder failed")
			return
		}

		require.NoError(t, frameErr)
		require.Equal(t, data[:len(frame)], frame)

		got, err := DecodeUTXOWrapperFrame(frame)
		require.NoError(t, err)
		requireSameWrapper(t, want, got)
	})
}

// benchStream is n mainnet-shaped records (1-3 outputs with 25-byte scripts).
func benchStream(n int) []byte {
	script := make([]byte, 25)

	var stream bytes.Buffer

	for i := 0; i < n; i++ {
		w := &UTXOWrapper{TxID: chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16)}), Height: 100}
		for j := 0; j <= i%3; j++ {
			w.UTXOs = append(w.UTXOs, &UTXO{Index: uint32(j), Value: 1000, Script: script}) //nolint:gosec // small index
		}

		stream.Write(w.Bytes())
	}

	return stream.Bytes()
}

// The seeder's reader cost per record: the old reader decoded every record in
// the reader goroutine; the new one only frames it (decoding moves to workers).
func BenchmarkSeederReaderPerRecord(b *testing.B) {
	const n = 100_000

	stream := benchStream(n)

	b.Run("decode-in-reader", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			r := bufio.NewReader(bytes.NewReader(stream))
			for k := 0; k < n; k++ {
				if _, err := NewUTXOWrapperFromReader(context.Background(), r); err != nil {
					b.Fatal(err)
				}
			}
		}

		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/record")
	})

	b.Run("frame-in-reader", func(b *testing.B) {
		b.ReportAllocs()

		var scratch []byte

		for i := 0; i < b.N; i++ {
			r := bufio.NewReader(bytes.NewReader(stream))
			for k := 0; k < n; k++ {
				frame, _, err := ReadUTXOWrapperFrame(r, scratch)
				if err != nil {
					b.Fatal(err)
				}

				scratch = frame
				_ = bytes.Clone(frame) // the accepted-record copy readUTXOFrames sends
			}
		}

		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/record")
	})
}

// A script length just over maxExactScriptAlloc with no bytes behind it must
// cost only a bounded chunk, on both decoders. This pins the bound itself: the
// hostile-size test above uses a 32 MiB claim with a 4 MB ceiling, so any bound
// up to 32 MiB would slip past it.
func TestScriptLengthOverExactAllocBound_AllocatesOnlyAChunk(t *testing.T) {
	const (
		claim   = 1 << 20           // 1 MiB claimed, nothing present
		ceiling = uint64(256 << 10) // well above one 64 KiB chunk, well below the claim
	)

	data := wrapperWithScriptLen(1, claim)

	for name, decode := range map[string]func() error{
		"NewUTXOWrapperFromBytes": func() error { _, err := NewUTXOWrapperFromBytes(data); return err },
		"ReadUTXOWrapperFrame":    func() error { _, _, err := ReadUTXOWrapperFrame(bytes.NewReader(data), nil); return err },
	} {
		var err error

		alloc := totalAllocDelta(func() { err = decode() })

		require.Error(t, err, name)
		require.Less(t, alloc, ceiling, "%s allocated %d bytes for a %d-byte input claiming a %d-byte script", name, alloc, len(data), claim)
	}
}
