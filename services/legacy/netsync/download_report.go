package netsync

import (
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// How a block's bytes were admitted, recorded by admitPipelineSink on the block's stream.
const (
	admitConverted    = "converted as it arrived"
	admitRawDuplicate = "drained unwritten: another copy was being converted"
	admitRawTimedOut  = "drained: the wait for an admission slot timed out"
	admitLocalFault   = "drained: this node failed to store the block; the peer is not at fault"
	admitNotOwed      = "drained unwritten: this peer does not owe the block"
)

// Thresholds for reporting a block's download. Below them a download says nothing new.
const (
	reportSlowDownload = 5 * time.Second
	reportSlowAdmit    = time.Second
)

// noteAdmission records on the block's live stream how long it waited for an admission slot and
// which path its bytes took. Safe on a nil registry and for a hash with no live stream.
func (r *streamRegistry) noteAdmission(hash chainhash.Hash, wait time.Duration, path string) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for s := range r.active {
		if s.hash == hash && s.path == "" {
			s.admitWait = wait
			s.path = path

			return
		}
	}
}

// report is one line on this block's download, and whether it is worth logging: a download of
// reportSlowDownload or more, an admission wait of reportSlowAdmit or more, or a raw fallback.
// tip is the committed height when the download finished. A height of zero means the node
// does not know the block's height, which is every block above the last checkpoint, where
// the header cache is empty; the line then leaves the height and the lead out rather than
// claim a block at height 0 hundreds of thousands of blocks behind the chain.
func (s *blockStream) report(now time.Time, tip int32) (string, bool) {
	// From the first byte, the admission wait included, as the line reports that wait too.
	firstByte := s.start.Add(-s.waited)
	took := now.Sub(firstByte)
	path := s.path

	if path == "" {
		path = admitConverted
	}

	interesting := took >= reportSlowDownload || s.admitWait >= reportSlowAdmit || path != admitConverted
	if !interesting {
		return "", false
	}

	requested := "no request on record"
	if !s.requestedAt.IsZero() {
		requested = fmt.Sprintf("bytes began %s after it was requested", firstByte.Sub(s.requestedAt).Round(time.Second))
	}

	rate := 0.0
	if secs := took.Seconds(); secs > 0 {
		rate = float64(s.read.Load()) / secs / 1e6
	}

	where := "height not known"
	if s.height > 0 {
		where = fmt.Sprintf("height %d, %d ahead of the chain", s.height, s.height-tip)
	}

	return fmt.Sprintf("%s, %.1f MB in %s at %.1f MB/s; %s; waited %s for an admission slot; %s",
		where, float64(s.total)/1e6, took.Round(time.Second), rate, requested,
		s.admitWait.Round(time.Second), path), true
}
