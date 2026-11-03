package netsync

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestNewPipelineDedupMap_BoundedRegardlessOfDeclaredTxCount is FIX 1.
//
// Before this change, pipelineBlockSink sized its duplicate-transaction map
// directly from the peer's declared transaction count:
// txmap.NewSplitSwissMapUint64(uint32(stream.TxCount())). That count is
// checked only against the wire payload ceiling (services/legacy/config.go
// maxWireBlockPayload, 4,000,000,000 bytes) against a 10-byte minimum
// transaction size, so a peer may declare up to 400,000,000 transactions in a
// body it then never sends. go-tx-map's NewSplitSwissMapUint64 pre-sizes all
// 1024 buckets eagerly from whatever length it is given (tx_map.go), so
// sizing from that declared count allocates roughly 19 GB before a single
// transaction byte arrives — the block's hash and proof of work are what gate
// the streaming path, never its declared size, so any sync peer qualifies.
//
// This test builds a real blockTxStream declaring 10,000,000 transactions:
// large enough to prove the point without actually reproducing a ~19 GB
// allocation on the machine running this test (10,000,000 scales to roughly
// 490 MB by the same formula, linear in the declared count; 400,000,000
// scales the same 490 MB up by 40x to the ~19 GB above). It then proves the
// dedup map the pipeline sink now constructs allocates a bounded, small
// amount regardless of that declared count.
//
// testing.AllocsPerRun was considered and rejected for the allocation
// assertion: it counts malloc calls, not bytes, and a single oversized
// backing array is still one malloc call whatever its size, so a regression
// back to sizing from the declared count would not move that number.
// runtime.MemStats.TotalAlloc is measured instead, which does capture bytes.
func TestNewPipelineDedupMap_BoundedRegardlessOfDeclaredTxCount(t *testing.T) {
	const declaredCount = 10_000_000
	const declaredPayloadLen = declaredCount * minSerializedTxSize // exactly the boundary newBlockTxStream allows

	var buf bytes.Buffer
	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, declaredCount), "building the fixture's declared-count varint")

	stream, err := newBlockTxStream(&buf, declaredPayloadLen)
	require.NoError(t, err, "the stream must accept a declaration right at its own size boundary")
	require.Equal(t, uint64(declaredCount), stream.TxCount(), "sanity: the stream must report the declared count back, or the rest of this test proves nothing")

	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	dedup := newPipelineDedupMap(stream.TxCount(), nil, nil)

	runtime.ReadMemStats(&after)

	require.NotNil(t, dedup, "newPipelineDedupMap must return a usable map")

	allocated := after.TotalAlloc - before.TotalAlloc

	// dedupInitialCapacity (1<<20) costs roughly 50 MB. Sizing from the
	// declared 10,000,000 instead costs roughly 490 MB, by
	// NewSplitSwissMapUint64's own headroom formula
	// ((length+length/5)/1024 per bucket, 1024 buckets, ~41 bytes/slot). 150
	// MB sits cleanly between the two, so this bound catches a regression
	// back to sizing from a peer-declared count without being sensitive to
	// ordinary allocator or GC noise.
	const bound = 150 << 20

	require.Lessf(t, allocated, uint64(bound),
		"constructing the pipeline dedup map allocated %d bytes while the stream declared %d transactions: it must be bounded by a constant, never by what a peer declared",
		allocated, declaredCount)
}

// A block's duplicate map is sized for the transactions the block declares, not for a million. On
// mainnet on 2026-09-25 every block got the million-slot map, about 48 MB, whatever its size: that
// was 16 GB of every 90 GB the node allocated, for blocks of a few thousand transactions.
func TestNewPipelineDedupMap_SizedForASmallBlock(t *testing.T) {
	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	dedup := newPipelineDedupMap(5_000, nil, nil)

	runtime.ReadMemStats(&after)

	require.NotNil(t, dedup)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(4<<20), "a 5,000-transaction block needs well under 4 MB of map")
}

// A map sized for a small declaration still takes every transaction a block brings, growing as it
// goes, and still catches a duplicate.
func TestNewPipelineDedupMap_GrowsPastItsSize(t *testing.T) {
	dedup := newPipelineDedupMap(2, nil, nil)

	for i := uint64(0); i < 50_000; i++ {
		var h chainhash.Hash
		binary.LittleEndian.PutUint64(h[:], i)
		require.NoError(t, dedup.Put(h, i))
	}

	var first chainhash.Hash
	require.Error(t, dedup.Put(first, 0), "the first hash again is a duplicate")
}

// BenchmarkPipelineDedupMapPerEntry measures the heap one in-memory dedup entry keeps live once the
// map has grown from its capped initial size by real Puts, which is how a streamed block fills it.
// The cap is raised past the entry count so the map stays in memory, which is the cost
// legacy_streamDedupMaxInMemoryEntries multiplies into a per-block heap ceiling: about 47 bytes live
// and 63 at the peak of a grow. Run with -benchtime=1x: one map of entries is the measurement, not
// the iteration count.
func BenchmarkPipelineDedupMapPerEntry(b *testing.B) {
	const entries = 10_000_000

	tSettings := &settings.Settings{Legacy: settings.LegacySettings{StreamDedupMaxInMemoryEntries: entries + 1}}

	growth := func(from, to uint64) float64 {
		if to < from {
			return 0
		}

		return float64(to-from) / entries
	}

	for range b.N {
		var before, after runtime.MemStats

		runtime.GC()
		runtime.ReadMemStats(&before)

		dedup := newPipelineDedupMap(entries, tSettings, nil)

		for i := uint64(0); i < entries; i++ {
			var h chainhash.Hash
			binary.LittleEndian.PutUint64(h[:], i)
			binary.LittleEndian.PutUint64(h[8:], i*0x9E3779B97F4A7C15)

			if err := dedup.Put(h, i); err != nil {
				b.Fatal(err)
			}
		}

		if dedup.disk != nil {
			b.Fatal("the map moved to disk; this measures the in-memory map")
		}

		runtime.ReadMemStats(&after)
		peak := after.HeapInuse

		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(dedup)

		b.ReportMetric(growth(before.HeapInuse, after.HeapInuse), "live-B/entry")
		b.ReportMetric(growth(before.HeapInuse, peak), "peak-B/entry")

		if err := dedup.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
