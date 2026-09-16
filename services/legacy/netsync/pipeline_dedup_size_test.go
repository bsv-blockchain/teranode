package netsync

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-wire"
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

	dedup := newPipelineDedupMap()

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
