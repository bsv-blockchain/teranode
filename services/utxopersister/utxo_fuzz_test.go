package utxopersister

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// FuzzUTXOWrapperFromBytes exercises the UTXO-set wrapper deserializer against
// arbitrary bytes. A persisted .utxo-set file is untrusted at read time — it
// can be corrupted, truncated, or seeded/imported from elsewhere — and a single
// bad record historically took down the whole core sidecar via the
// ServiceManager errgroup. The deserializer must therefore:
//
//   - never panic, and
//   - never make an allocation sized from an untrusted length/count field
//     (a header claiming 0xFFFFFFFF UTXOs or a 0xFFFFFFFF-byte script must not
//     trigger a multi-GB allocation), and
//   - when it does parse a wrapper, re-serialize to exactly the bytes it
//     consumed (no over- or under-read).
//
// The no-panic guarantee is enforced by the fuzzing engine itself; the
// round-trip invariant is checked below. The allocation bound is pinned
// deterministically by TestUTXOWrapperFromBytes_HostileSizesAreBounded.
func FuzzUTXOWrapperFromBytes(f *testing.F) {
	// Valid seeds — round-trip real wrappers through Bytes().
	f.Add((&UTXOWrapper{
		TxID:   chainhash.HashH([]byte("seed-single")),
		Height: 1,
		UTXOs:  []*UTXO{{Index: 0, Value: 100, Script: []byte{0x76, 0xa9, 0x88, 0xac}}},
	}).Bytes())
	f.Add((&UTXOWrapper{
		TxID:     chainhash.HashH([]byte("seed-coinbase-multi")),
		Height:   42,
		Coinbase: true,
		UTXOs: []*UTXO{
			{Index: 0, Value: 5000000000, Script: []byte{0x51}},
			{Index: 7, Value: 0, Script: nil},
		},
	}).Bytes())

	// Hostile seeds — headers that claim enormous sizes with no body behind them.
	f.Add(wrapperHeader(0xFFFFFFFF))           // numUTXOs = 4 billion, no UTXOs
	f.Add(wrapperWithScriptLen(1, 0xFFFFFFFF)) // 1 UTXO whose script claims 4 GB
	f.Add([]byte{})                            // empty
	f.Add(make([]byte, 32))                    // bare txid, truncated before header
	f.Add(make([]byte, 40))                    // header present, zero UTXOs, clean

	f.Fuzz(func(t *testing.T, data []byte) {
		w, err := NewUTXOWrapperFromBytes(data)
		if err != nil {
			return // a clean error on garbage is the expected outcome
		}

		require.NotNil(t, w)

		// A wrapper parsed from `data` must re-serialize to exactly the bytes
		// it consumed, which are a prefix of the input. If Bytes() is not a
		// prefix of data, the reader and writer disagree on the wire format.
		encoded := w.Bytes()
		require.True(t, bytes.HasPrefix(data, encoded),
			"re-serialized wrapper (%d bytes) is not a prefix of the %d-byte input", len(encoded), len(data))
	})
}

// TestUTXOWrapperFromBytes_HostileSizesAreBounded pins that deserializing a
// header which claims an enormous UTXO count or script length does NOT allocate
// proportionally to that untrusted field. Before the fix, FromReader did
// `make([]*UTXO, numUTXOs)` and the per-UTXO readers did `make([]byte, l)`, so a
// tiny corrupt/truncated file could force a multi-GB allocation and OOM the core
// sidecar. After the fix the allocation tracks the actual bytes available, so a
// 40-to-56 byte input must allocate well under a few MB before erroring out.
func TestUTXOWrapperFromBytes_HostileSizesAreBounded(t *testing.T) {
	const (
		hugeCount = 1 << 25         // 33M; pre-sizing []*UTXO would cost ~256 MB
		hugeLen   = 1 << 25         // 33M; pre-sizing the script would cost ~32 MB
		maxAlloc  = uint64(4 << 20) // 4 MB — far above a bounded parse, far below the bug
	)

	cases := []struct {
		name string
		data []byte
	}{
		{"huge utxo count, no body", wrapperHeader(hugeCount)},
		{"huge script length, no body", wrapperWithScriptLen(1, hugeLen)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			alloc := totalAllocDelta(func() {
				_, err := NewUTXOWrapperFromBytes(tc.data)
				// Must reject the truncated body rather than succeed.
				require.Error(t, err, "hostile header must not parse as a valid wrapper")
			})

			require.Less(t, alloc, maxAlloc,
				"parsing a %d-byte input allocated %d bytes — allocation is sized from an untrusted length/count field, not the available input",
				len(tc.data), alloc)
		})
	}
}

// wrapperHeader builds a UTXOWrapper byte stream containing only the 40-byte
// header (32-byte txid + 4-byte encoded height + 4-byte UTXO count) with the
// given count and no UTXO records behind it.
func wrapperHeader(numUTXOs uint32) []byte {
	b := make([]byte, 40)
	// bytes 0-31: txid (zero is fine)
	// bytes 32-35: encoded height/coinbase (zero)
	binary.LittleEndian.PutUint32(b[36:40], numUTXOs)
	return b
}

// wrapperWithScriptLen builds a header declaring numUTXOs records followed by a
// single partial UTXO (index + value + script length) whose script length is
// scriptLen, with no script bytes behind it.
func wrapperWithScriptLen(numUTXOs, scriptLen uint32) []byte {
	b := wrapperHeader(numUTXOs)
	utxo := make([]byte, 16) // index(4) + value(8) + scriptLen(4)
	binary.LittleEndian.PutUint32(utxo[12:16], scriptLen)
	return append(b, utxo...)
}

// totalAllocDelta runs fn and returns how many bytes were allocated during it.
// runtime.TotalAlloc is monotonic, so the delta is a stable measure unaffected
// by GC. The call is single-goroutine so the reading is not racy.
func totalAllocDelta(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}
