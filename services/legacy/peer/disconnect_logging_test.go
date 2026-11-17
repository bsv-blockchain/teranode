package peer

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestDisconnectLogsOnceFromTheInitiatingCaller pins that one disconnect
// produces one log line.
//
// A peer is routinely torn down from two directions at once: something decides
// to drop it, and its own read loop then hits the closed socket and reports the
// error. Before this guard both logged, so a single disconnect appeared twice.
//
// Two things depend on getting this right. The line is the anchor the
// disconnect-rate measurements count, so duplicates inflate the number those
// measurements exist to reduce. And a deliberate quiet teardown — a feeler probe
// hanging up on purpose, logged at debug — would still be reported at warning
// level by the losing caller, making a probe that worked look like a lost peer.
func TestDisconnectLogsOnceFromTheInitiatingCaller(t *testing.T) {
	p := &Peer{logger: ulogger.TestLogger{}, quit: make(chan struct{})}

	var (
		mu    sync.Mutex
		lines int
	)

	count := func(string, ...interface{}) {
		mu.Lock()
		defer mu.Unlock()
		lines++
	}

	p.DisconnectWithLogFunc("first, and the only one that should be reported", count)
	p.DisconnectWithLogFunc("second, arriving from the read loop", count)
	p.DisconnectWithLogFunc("third, for good measure", count)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, lines, "one disconnect must produce exactly one log line")
}

// TestDisconnectLogsOnceUnderConcurrency covers the case that actually happens:
// the deliberate teardown and the read loop's error racing each other.
func TestDisconnectLogsOnceUnderConcurrency(t *testing.T) {
	p := &Peer{logger: ulogger.TestLogger{}, quit: make(chan struct{})}

	var (
		mu    sync.Mutex
		lines int
		wg    sync.WaitGroup
	)

	count := func(string, ...interface{}) {
		mu.Lock()
		defer mu.Unlock()
		lines++
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			p.DisconnectWithLogFunc("racing teardown", count)
		}()
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, lines, "concurrent teardowns must still report one disconnect")
}
