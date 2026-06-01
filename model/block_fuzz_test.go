package model

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// FuzzNewBlockFromBytes exercises the public block deserializer against
// arbitrary bytes. Blocks arrive from untrusted peers, so parsing must never
// crash the process. NewBlockFromBytes already wraps panics in a recover(), so
// this target mainly guards the class recover() CANNOT catch — a pathological
// allocation sized from an untrusted length field (e.g. the subtree count),
// which OOMs rather than panics. The inner reader is fuzzed without the recover
// wrapper by FuzzReadBlockFromReader below to surface masked panics too.
func FuzzNewBlockFromBytes(f *testing.F) {
	for _, b := range seedBlockBytes(f) {
		f.Add(b)
	}
	f.Add([]byte{})
	f.Add(make([]byte, 92)) // minimum-size all-zero block

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic or OOM; an error on garbage is the expected outcome.
		_, _ = NewBlockFromBytes(data)
	})
}

// FuzzReadBlockFromReader fuzzes the block body parser directly — without the
// recover() that NewBlockFromBytes wraps it in — so that any panic in the
// varint / subtree-list / coinbase / bump parsing surfaces as a fuzz failure
// instead of being silently converted to an error in production.
func FuzzReadBlockFromReader(f *testing.F) {
	// Seed with the post-header body (everything after the 80-byte header) of
	// the valid test blocks.
	for _, b := range seedBlockBytes(f) {
		if len(b) > 80 {
			f.Add(b[80:])
		}
	}
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, body []byte) {
		_, _ = readBlockFromReader(&Block{}, bytes.NewReader(body))
	})
}

// TestBlock_HostileSubtreeLengthIsBounded pins that a block body declaring an
// enormous subtree count does NOT pre-allocate proportionally to that untrusted
// field. Before the fix, readBlockFromReader did
// `make([]*chainhash.Hash, 0, block.subtreeLength)`, so a tiny body claiming
// ~33M subtrees forced a ~256 MB allocation before the first missing hash
// failed the read — a cheap OOM vector for the core sidecar via the
// ServiceManager errgroup. (The CoinbaseBUMP read just below it was already
// bounded; the subtree list was not.)
func TestBlock_HostileSubtreeLengthIsBounded(t *testing.T) {
	const (
		hugeSubtrees = 1 << 25         // pre-sizing []*chainhash.Hash would cost ~256 MB
		maxAlloc     = uint64(4 << 20) // 4 MB — far above a bounded parse, far below the bug
	)

	var body bytes.Buffer
	require.NoError(t, wire.WriteVarInt(&body, 0, 1))            // transaction count
	require.NoError(t, wire.WriteVarInt(&body, 0, 1))            // size in bytes
	require.NoError(t, wire.WriteVarInt(&body, 0, hugeSubtrees)) // subtree count, with no hashes following

	data := body.Bytes()

	alloc := allocDelta(func() {
		// readBlockFromReader is exercised directly here to isolate the
		// post-header body parsing. A bare &Block{} (no Header) is intentional
		// and safe: readBlockFromReader does not read block.Header — the public
		// NewBlockFromBytes/NewBlockFromReader set it before calling in.
		_, err := readBlockFromReader(&Block{}, bytes.NewReader(data))
		require.Error(t, err, "a body claiming a huge subtree count with no hashes must error")
	})

	require.Less(t, alloc, maxAlloc,
		"parsing a %d-byte body allocated %d bytes — the subtree list is pre-sized from an untrusted count, not the available input",
		len(data), alloc)
}

// seedBlockBytes loads the raw .block fixtures used as valid fuzz seeds.
func seedBlockBytes(tb testing.TB) [][]byte {
	tb.Helper()

	paths, err := filepath.Glob("testdata/*.block")
	require.NoError(tb, err)

	out := make([][]byte, 0, len(paths))

	for _, p := range paths {
		b, err := os.ReadFile(p)
		require.NoError(tb, err)
		out = append(out, b)
	}

	return out
}

// allocDelta runs fn and returns the bytes allocated during it. TotalAlloc is
// monotonic so the delta is GC-stable; the call is single-goroutine so the
// reading is not racy.
func allocDelta(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}
