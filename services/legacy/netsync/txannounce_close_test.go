package netsync

import (
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newTxAnnounceTestManager builds a minimal SyncManager with only the fields the
// tx-announce batcher guard needs (DC15 / review C1). It uses a real go-batcher
// with a no-op flush fn — no mocking.
func newTxAnnounceTestManager(t *testing.T) *SyncManager {
	t.Helper()

	sm := &SyncManager{logger: ulogger.TestLogger{}}
	sm.txAnnounceBatcher = batcher.NewWithDeduplicationAndPool[TxHashAndFee](
		10, time.Second, func(_ []*TxHashAndFee) {}, true,
		batcher.WithName("test_tx_announce"),
		batcher.WithLogger(ulogger.TestLogger{}),
	)

	return sm
}

// TestAnnounceTx_PutAfterCloseIsNoop verifies the core C1 fix: a Put through
// announceTx after closeTxAnnounceBatcher has drained/closed the batcher is a
// safe no-op rather than a panic (go-batcher v2.0.4 panics on Put-after-Close).
func TestAnnounceTx_PutAfterCloseIsNoop(t *testing.T) {
	sm := newTxAnnounceTestManager(t)

	// A Put before close is accepted.
	require.NotPanics(t, func() { sm.announceTx(&TxHashAndFee{Fee: 1}) })

	// Quiesce + drain (what Stop() does).
	sm.closeTxAnnounceBatcher()

	// A Put after close must NOT panic — the guard turns it into a no-op.
	require.NotPanics(t, func() { sm.announceTx(&TxHashAndFee{Fee: 2}) })

	// closeTxAnnounceBatcher is idempotent.
	require.NotPanics(t, func() { sm.closeTxAnnounceBatcher() })
}

// TestAnnounceTx_ConcurrentWithClose exercises the race the txmeta Kafka listener
// creates: many announceTx calls running concurrently with the shutdown close.
// The RWMutex pairing must ensure no Put ever reaches a closed batcher (no panic).
func TestAnnounceTx_ConcurrentWithClose(t *testing.T) {
	sm := newTxAnnounceTestManager(t)

	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func(n uint64) {
			defer wg.Done()
			// Each goroutine mimics the txmeta listener calling announceTx.
			sm.announceTx(&TxHashAndFee{Fee: n})
		}(uint64(i))
	}

	// Concurrently close (drain) the batcher while the Puts are in flight.
	require.NotPanics(t, func() { sm.closeTxAnnounceBatcher() })

	wg.Wait()
}
