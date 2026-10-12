package subtreeprocessor

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// buildSubtreeBytes constructs an on-disk subtree byte stream of the shape
// DeserializeHashesFromReaderIntoBuckets expects:
//
//	[ 48-byte header ]
//	[ 8-byte numLeaves         ]
//	[ numLeaves * 48-byte leaves ]
//	[ 8-byte numConflicting    ]
//	[ numConflicting * 32-byte conflicting hashes ]
//
// Each leaf is 32-byte hash + 8-byte fee + 8-byte size (fee/size are
// zero-filled here — the deserializer skips them).
func buildSubtreeBytes(leaves []chainhash.Hash, conflicting []chainhash.Hash) []byte {
	var buf bytes.Buffer

	// 48-byte header (opaque to the reader)
	buf.Write(make([]byte, 48))

	// numLeaves
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(leaves)))
	buf.Write(lenBuf[:])

	// leaves: 32-byte hash + 16 zero bytes
	zero16 := make([]byte, 16)
	for _, h := range leaves {
		buf.Write(h[:])
		buf.Write(zero16)
	}

	// numConflicting + hashes
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(conflicting)))
	buf.Write(lenBuf[:])
	for _, h := range conflicting {
		buf.Write(h[:])
	}

	return buf.Bytes()
}

func TestDeserializeHashesFromReaderIntoBuckets_HappyPath(t *testing.T) {
	leaves := []chainhash.Hash{
		chainhash.HashH([]byte("a")),
		chainhash.HashH([]byte("b")),
		chainhash.HashH([]byte("c")),
	}
	conflicting := []chainhash.Hash{
		chainhash.HashH([]byte("x")),
	}

	data := buildSubtreeBytes(leaves, conflicting)

	buckets := make(map[uint16][]chainhash.Hash, 8)
	conflictingOut := make([]chainhash.Hash, 0)

	err := DeserializeHashesFromReaderIntoBuckets(
		bytes.NewReader(data),
		8,
		128*1024*1024,
		&buckets,
		&conflictingOut,
	)
	require.NoError(t, err)

	total := 0
	for _, hs := range buckets {
		total += len(hs)
	}
	require.Equal(t, len(leaves), total, "all leaves should be bucketed")
	require.Equal(t, conflicting, conflictingOut)
}

func TestDeserializeHashesFromReaderIntoBuckets_EmptySubtree(t *testing.T) {
	// An empty subtree (zero leaves, zero conflicting) is valid input —
	// no leaf-data read, no allocation, returns cleanly.
	data := buildSubtreeBytes(nil, nil)

	buckets := make(map[uint16][]chainhash.Hash, 8)
	conflictingOut := make([]chainhash.Hash, 0)

	err := DeserializeHashesFromReaderIntoBuckets(
		bytes.NewReader(data),
		8,
		128*1024*1024,
		&buckets,
		&conflictingOut,
	)
	require.NoError(t, err)
	require.Empty(t, conflictingOut)
}

func TestDeserializeHashesFromReaderIntoBuckets_NumLeavesExceedsCap(t *testing.T) {
	// Forge a header that claims a billion leaves. The deserializer must
	// reject before allocating; if the bounds check is missing it would
	// either OOM or hang trying to read 48 GiB from the reader.
	var buf bytes.Buffer
	buf.Write(make([]byte, 48)) // header

	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], 1_000_000_000) // 1B leaves → 48GB
	buf.Write(lenBuf[:])

	buckets := make(map[uint16][]chainhash.Hash, 8)
	conflictingOut := make([]chainhash.Hash, 0)

	// Cap at 128 MiB — the production default.
	err := DeserializeHashesFromReaderIntoBuckets(
		bytes.NewReader(buf.Bytes()),
		8,
		128*1024*1024,
		&buckets,
		&conflictingOut,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max")
}

func TestDeserializeHashesFromReaderIntoBuckets_NumLeavesOverflowSafe(t *testing.T) {
	// numLeaves = max uint64 must not panic on the bound check. The check
	// is overflow-safe by dividing the cap by the per-record size rather
	// than multiplying numLeaves by it.
	var buf bytes.Buffer
	buf.Write(make([]byte, 48))

	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], ^uint64(0))
	buf.Write(lenBuf[:])

	buckets := make(map[uint16][]chainhash.Hash, 8)
	conflictingOut := make([]chainhash.Hash, 0)

	err := DeserializeHashesFromReaderIntoBuckets(
		bytes.NewReader(buf.Bytes()),
		8,
		128*1024*1024,
		&buckets,
		&conflictingOut,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max")
}

func TestDeserializeHashesFromReaderIntoBuckets_NumConflictingExceedsCap(t *testing.T) {
	// Valid leaves section, but the conflicting trailer claims more nodes
	// than the cap permits. Must reject without allocating.
	leaves := []chainhash.Hash{chainhash.HashH([]byte("ok"))}

	var buf bytes.Buffer
	buf.Write(make([]byte, 48))

	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(leaves)))
	buf.Write(lenBuf[:])
	zero16 := make([]byte, 16)
	for _, h := range leaves {
		buf.Write(h[:])
		buf.Write(zero16)
	}

	// Forged conflicting count: way over cap/32.
	binary.LittleEndian.PutUint64(lenBuf[:], 1_000_000_000)
	buf.Write(lenBuf[:])

	buckets := make(map[uint16][]chainhash.Hash, 8)
	conflictingOut := make([]chainhash.Hash, 0)

	err := DeserializeHashesFromReaderIntoBuckets(
		bytes.NewReader(buf.Bytes()),
		8,
		128*1024*1024,
		&buckets,
		&conflictingOut,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicting")
}

func TestDeserializeHashesFromReaderIntoBuckets_TruncatedStream(t *testing.T) {
	// Header claims more leaves than the stream actually carries. Must
	// surface a clean read error rather than panic.
	var buf bytes.Buffer
	buf.Write(make([]byte, 48))

	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], 10)
	buf.Write(lenBuf[:])
	// Only write 5 leaves' worth (240 bytes) instead of 10 (480 bytes).
	buf.Write(make([]byte, 5*48))

	buckets := make(map[uint16][]chainhash.Hash, 8)
	conflictingOut := make([]chainhash.Hash, 0)

	err := DeserializeHashesFromReaderIntoBuckets(
		bytes.NewReader(buf.Bytes()),
		8,
		128*1024*1024,
		&buckets,
		&conflictingOut,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "io.ReadFull on short stream should propagate ErrUnexpectedEOF")
}

func TestDeserializeHashesFromReaderIntoBuckets_PoolReuse(t *testing.T) {
	// Two back-to-back calls of different sizes. The second call should
	// reuse the pool buffer (high-water mark) — we can't observe pool
	// internals directly but we can verify both calls succeed and the
	// data parses correctly with no cross-contamination from the first
	// call's bytes. ElementsMatch (not just length) locks the invariant
	// in: a pool-corruption bug that preserved length but altered bytes
	// would otherwise slip through, because io.ReadFull only overwrites
	// the requested range and any prior bytes in the pooled backing
	// array would survive a length-only check.
	first := []chainhash.Hash{
		chainhash.HashH([]byte("first-a")),
		chainhash.HashH([]byte("first-b")),
	}
	second := []chainhash.Hash{
		chainhash.HashH([]byte("second-only")),
	}

	run := func(leaves []chainhash.Hash) []chainhash.Hash {
		data := buildSubtreeBytes(leaves, nil)
		buckets := make(map[uint16][]chainhash.Hash, 8)
		conflictingOut := make([]chainhash.Hash, 0)
		err := DeserializeHashesFromReaderIntoBuckets(
			bytes.NewReader(data),
			8,
			128*1024*1024,
			&buckets,
			&conflictingOut,
		)
		require.NoError(t, err)

		var out []chainhash.Hash
		for _, hs := range buckets {
			out = append(out, hs...)
		}
		return out
	}

	got1 := run(first)
	require.ElementsMatch(t, first, got1)

	got2 := run(second)
	require.ElementsMatch(t, second, got2, "second call must not leak entries from the first")
}

func TestDeserializeHashesFromReaderIntoBuckets_NonPositiveCapRejected(t *testing.T) {
	// A 0 or negative cap (misconfigured setting) must be rejected before
	// any read or allocation. Without this check, uint64(int64(-1)) wraps
	// to ^uint64(0) and disables every subsequent bound.
	cases := []int64{0, -1, -128 * 1024 * 1024}
	for _, cap := range cases {
		buckets := make(map[uint16][]chainhash.Hash, 8)
		conflictingOut := make([]chainhash.Hash, 0)
		err := DeserializeHashesFromReaderIntoBuckets(
			bytes.NewReader(buildSubtreeBytes(nil, nil)),
			8,
			cap,
			&buckets,
			&conflictingOut,
		)
		require.Error(t, err, "cap=%d must be rejected", cap)
		require.Contains(t, err.Error(), "must be positive")
	}
}
