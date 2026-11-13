package subtreeprocessor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
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

func TestDeserializeHashesFromReaderIntoBuckets_ConflictingCountVsLeaves(t *testing.T) {
	run := func(numLeaves, numConflicting int) ([]chainhash.Hash, error) {
		leaves := make([]chainhash.Hash, numLeaves)
		for i := range leaves {
			leaves[i] = chainhash.HashH([]byte{byte(i)})
		}

		// conflicting nodes are leaves of the subtree; past the leaves, fresh hashes
		conflicting := make([]chainhash.Hash, numConflicting)
		for i := range conflicting {
			if i < numLeaves {
				conflicting[i] = leaves[i]
			} else {
				conflicting[i] = chainhash.HashH([]byte{0xff, byte(i)})
			}
		}

		buckets := make(map[uint16][]chainhash.Hash, 8)
		conflictingOut := make([]chainhash.Hash, 0)

		err := DeserializeHashesFromReaderIntoBuckets(
			bytes.NewReader(buildSubtreeBytes(leaves, conflicting)),
			8, 128*1024*1024, &buckets, &conflictingOut,
		)

		return conflictingOut, err
	}

	t.Run("count equal to leaves is accepted", func(t *testing.T) {
		out, err := run(3, 3)
		require.NoError(t, err)
		require.Len(t, out, 3)
	})

	for _, tc := range []struct{ leaves, conflicting int }{{3, 4}, {0, 1}} {
		t.Run(fmt.Sprintf("%d conflicting for %d leaves is rejected", tc.conflicting, tc.leaves), func(t *testing.T) {
			out, err := run(tc.leaves, tc.conflicting)
			require.Error(t, err)
			require.Contains(t, err.Error(), "exceeds leaf count")
			require.ErrorIs(t, err, subtreepkg.ErrConflictingCountExceedsLeaves, "the same sentinel go-subtree's readers return")
			require.Empty(t, out, "nothing is read from a rejected trailer")
		})
	}
}

func TestForEachSubtreeLeafHash_ReadsEveryLeafOfASerializedSubtree(t *testing.T) {
	// 4,096 leaves span several 64 KiB read buffers (1,365 leaves each), so this covers the
	// chunk boundaries as well as the go-subtree serialization the files really hold.
	const leafCount = 4096

	st, err := subtreepkg.NewTreeByLeafCount(leafCount)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for i := 1; i < leafCount; i++ {
		require.NoError(t, st.AddNode(chainhash.HashH([]byte(fmt.Sprintf("leaf-%d", i))), uint64(i), uint64(i))) //nolint:gosec // test index
	}

	require.NoError(t, st.AddConflictingNode(chainhash.HashH([]byte("leaf-7"))))

	data, err := st.Serialize()
	require.NoError(t, err)

	got := make([]chainhash.Hash, 0, leafCount)
	require.NoError(t, forEachSubtreeLeafHash(bytes.NewReader(data), 128*1024*1024, func(hash chainhash.Hash) {
		got = append(got, hash)
	}))

	want := make([]chainhash.Hash, 0, leafCount)
	for i := range st.Nodes {
		want = append(want, st.Nodes[i].Hash)
	}

	require.Equal(t, want, got)
}

func TestForEachSubtreeLeafHash_EmptySubtree(t *testing.T) {
	calls := 0
	require.NoError(t, forEachSubtreeLeafHash(bytes.NewReader(buildSubtreeBytes(nil, nil)), 128*1024*1024, func(chainhash.Hash) {
		calls++
	}))
	require.Zero(t, calls)
}

func TestForEachSubtreeLeafHash_RejectsBadInput(t *testing.T) {
	claimLeaves := func(numLeaves uint64, leafBytes int) []byte {
		var buf bytes.Buffer
		buf.Write(make([]byte, 48))

		var lenBuf [8]byte
		binary.LittleEndian.PutUint64(lenBuf[:], numLeaves)
		buf.Write(lenBuf[:])
		buf.Write(make([]byte, leafBytes))

		return buf.Bytes()
	}

	tests := []struct {
		name      string
		data      []byte
		maxBody   int64
		errText   string
		wantCalls int
	}{
		{name: "header claims more leaves than the cap", data: claimLeaves(1_000_000_000, 0), maxBody: 128 * 1024 * 1024, errText: "more than"},
		{name: "leaf count at max uint64", data: claimLeaves(^uint64(0), 0), maxBody: 128 * 1024 * 1024, errText: "more than"},
		{name: "stream ends before the last leaf", data: claimLeaves(10, 5*48), maxBody: 128 * 1024 * 1024, errText: "unable to read leaves"},
		{name: "stream ends inside the header", data: []byte{1, 2, 3}, maxBody: 128 * 1024 * 1024, errText: "header"},
		{name: "cap too small for an empty subtree", data: buildSubtreeBytes(nil, nil), maxBody: 0, errText: "invalid maxSubtreeBodyBytes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			err := forEachSubtreeLeafHash(bytes.NewReader(tt.data), tt.maxBody, func(chainhash.Hash) {
				calls++
			})
			require.ErrorContains(t, err, tt.errText)
			require.Equal(t, tt.wantCalls, calls, "no leaf of a rejected chunk reaches fn")
		})
	}
}
