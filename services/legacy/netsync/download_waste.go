package netsync

import (
	"sync/atomic"
	"time"
)

// downloadWaste counts, since the process started, the block bytes received and every way
// download bandwidth was lost. The 30-second queue report logs it and the recorder reads it, so
// a night's losses can be read back without searching the log. The zero value is ready to use.
type downloadWaste struct {
	// received is block body bytes read off the wire.
	received atomic.Int64
	// dupDrained is copies drained unwritten because another copy was converting.
	dupDrained atomic.Int64
	// dupConverted is copies converted in full for a block already parked.
	dupConverted atomic.Int64
	// streamsFailed is block bodies cut part way, and bytesWasted the bytes of those and of
	// drained duplicates.
	streamsFailed atomic.Int64
	bytesWasted   atomic.Int64
	// droppedOwing is peers that left while still owing blocks, and blocksOwedAtDrop how many.
	droppedOwing     atomic.Int64
	blocksOwedAtDrop atomic.Int64
	// reAskedQuiet is blocks made askable of another peer because the peers owing them sent no
	// block bytes for the retry window. It is the one routine way a block reaches a second peer.
	reAskedQuiet atomic.Int64

	// lastReceived and lastAt are the received total at the previous report, for its rate. Only
	// the report reads and writes them, from one goroutine.
	lastReceived int64
	lastAt       time.Time
}

// rateSinceLast is the bytes a second received since the previous call, and resets the mark.
func (w *downloadWaste) rateSinceLast(now time.Time) float64 {
	total := w.received.Load()
	defer func() { w.lastReceived, w.lastAt = total, now }()

	if w.lastAt.IsZero() {
		return 0
	}

	elapsed := now.Sub(w.lastAt).Seconds()
	if elapsed <= 0 {
		return 0
	}

	return float64(total-w.lastReceived) / elapsed
}
