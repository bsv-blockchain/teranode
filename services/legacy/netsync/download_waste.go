package netsync

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	// localFaultDrained is copies drained because this node failed to store the block; the
	// peer was kept and the block asked for again.
	localFaultDrained atomic.Int64
	// streamsFailed is block bodies cut part way, and bytesWasted the bytes of those and of
	// every drained copy, duplicate or local fault.
	streamsFailed atomic.Int64
	bytesWasted   atomic.Int64
	// droppedOwing is peers that left while still owing blocks, and blocksOwedAtDrop how many.
	droppedOwing     atomic.Int64
	blocksOwedAtDrop atomic.Int64
	// reAskedQuiet is blocks made askable of another peer because the peers owing them sent no
	// block bytes for the retry window. With rescued and the frontier race, these are the
	// routine ways a block reaches a second peer.
	reAskedQuiet atomic.Int64
	// rescued is blocks asked of another peer because their owner would land them later than the
	// chain needs them: behind a slow queue, or arriving slowly (see THE WATCHER, watcher.go).
	rescued atomic.Int64

	// lastReceived and lastAt are the received total at the previous report, for its rate. Only
	// the report reads and writes them, from one goroutine.
	lastReceived int64
	lastAt       time.Time

	// published is each counter's value at the previous publish, so publish adds only the
	// increase. Written by one goroutine, the report's, like lastReceived.
	published struct {
		received, dupDrained, dupConverted, localFaultDrained, streamsFailed, bytesWasted,
		droppedOwing, blocksOwedAtDrop, reAskedQuiet int64
	}
}

// publish adds to each Prometheus counter what its atomic gained since the previous call.
// The atomics stay the source of truth, for the report's log line and the recorder, and are
// copied out here rather than at every site that bumps one: received grows per read chunk
// through a pointer, and a float add there would sit on the read path.
func (w *downloadWaste) publish() {
	if prometheusLegacyNetsyncDownloadReceivedBytes == nil {
		return
	}

	addIncrease := func(c prometheus.Counter, cur *atomic.Int64, last *int64) {
		v := cur.Load()
		if d := v - *last; d > 0 {
			c.Add(float64(d))
		}

		*last = v
	}

	p := &w.published
	addIncrease(prometheusLegacyNetsyncDownloadReceivedBytes, &w.received, &p.received)
	addIncrease(prometheusLegacyNetsyncDownloadDupDrained, &w.dupDrained, &p.dupDrained)
	addIncrease(prometheusLegacyNetsyncDownloadDupConverted, &w.dupConverted, &p.dupConverted)
	addIncrease(prometheusLegacyNetsyncDownloadLocalFaultDrained, &w.localFaultDrained, &p.localFaultDrained)
	addIncrease(prometheusLegacyNetsyncDownloadStreamsCut, &w.streamsFailed, &p.streamsFailed)
	addIncrease(prometheusLegacyNetsyncDownloadBytesWasted, &w.bytesWasted, &p.bytesWasted)
	addIncrease(prometheusLegacyNetsyncDownloadPeersDroppedOwing, &w.droppedOwing, &p.droppedOwing)
	addIncrease(prometheusLegacyNetsyncDownloadBlocksOwedAtDrop, &w.blocksOwedAtDrop, &p.blocksOwedAtDrop)
	addIncrease(prometheusLegacyNetsyncDownloadBlocksReasked, &w.reAskedQuiet, &p.reAskedQuiet)
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
