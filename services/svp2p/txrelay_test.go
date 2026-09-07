package svp2p

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// recordingRelay collects every RelayTxs-shaped call newTxAnnouncer's flush
// makes, so tests can assert positively on what actually reached it.
type recordingRelay struct {
	mu      sync.Mutex
	batches [][]protocol.TxHashAndFee
}

func (r *recordingRelay) relay(batch []protocol.TxHashAndFee) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.batches = append(r.batches, batch)
}

func (r *recordingRelay) all() []protocol.TxHashAndFee {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []protocol.TxHashAndFee
	for _, b := range r.batches {
		out = append(out, b...)
	}

	return out
}

// TestTxAnnouncerFlushesAfterTimeout pins the batcher's own timeout
// (txAnnounceBatchTimeout, ported verbatim from legacy's 1*time.Second): a
// single Put, with no other traffic to fill the batch, still reaches relay
// once the timeout fires — it does not wait forever for a full batch.
func TestTxAnnouncerFlushesAfterTimeout(t *testing.T) {
	relay := &recordingRelay{}
	// nil canRelay: these tests are not about the FSM RUNNING gate (see
	// TestTxAnnouncer_SuppressesRelayWhileNotRunning), so they get the
	// "no gate" default.
	a := newTxAnnouncer(ulogger.TestLogger{}, relay.relay, nil)
	t.Cleanup(func() { a.close(ulogger.TestLogger{}) })

	hash := chainhash.Hash{0x01}
	a.put(hash, 500, 250)

	require.Eventually(t, func() bool {
		for _, tx := range relay.all() {
			if tx.TxHash == hash {
				return true
			}
		}

		return false
	}, 3*time.Second, 10*time.Millisecond, "a single queued tx must be flushed after the batcher's timeout")

	all := relay.all()
	require.Len(t, all, 1)
	require.Equal(t, uint64(500), all[0].Fee)
	require.Equal(t, uint64(250), all[0].Size)
}

// TestTxAnnouncerPutAfterCloseIsNoop pins the Put-vs-Close race guard ported
// from legacy's txAnnounceMu/txAnnounceClosed (netsync/manager.go:477-485,
// "go-batcher v2.0.4 PANICS on Put-after-Close"): once close has run, put
// must silently do nothing rather than panic on a closed batcher.
func TestTxAnnouncerPutAfterCloseIsNoop(t *testing.T) {
	relay := &recordingRelay{}
	a := newTxAnnouncer(ulogger.TestLogger{}, relay.relay, nil) // nil canRelay: no gate

	a.close(ulogger.TestLogger{})

	require.NotPanics(t, func() {
		a.put(chainhash.Hash{0x02}, 1, 1)
	})

	// Give any wrongly-still-alive batcher goroutine a moment, then confirm
	// nothing was relayed — the put must never have reached the batcher.
	time.Sleep(50 * time.Millisecond)
	require.Empty(t, relay.all())
}

// TestTxAnnouncerCloseIsIdempotent mirrors legacy's own alreadyClosed
// short-circuit (netsync/manager.go closeTxAnnounceBatcher): a second close
// call must not panic or hang.
func TestTxAnnouncerCloseIsIdempotent(t *testing.T) {
	relay := &recordingRelay{}
	a := newTxAnnouncer(ulogger.TestLogger{}, relay.relay, nil) // nil canRelay: no gate

	require.NotPanics(t, func() {
		a.close(ulogger.TestLogger{})
		a.close(ulogger.TestLogger{})
	})
}

// TestTxAnnouncerCloseDrainsQueuedItems proves close() flushes whatever was
// already queued rather than discarding it — legacy's own
// util.DrainBatcher-based drain semantics.
func TestTxAnnouncerCloseDrainsQueuedItems(t *testing.T) {
	relay := &recordingRelay{}
	a := newTxAnnouncer(ulogger.TestLogger{}, relay.relay, nil) // nil canRelay: no gate

	hash := chainhash.Hash{0x03}
	a.put(hash, 1, 1)

	a.close(ulogger.TestLogger{})

	var found bool
	for _, tx := range relay.all() {
		if tx.TxHash == hash {
			found = true
		}
	}

	require.True(t, found, "close must drain an already-queued item into relay, not discard it")
}

// TestTxAnnouncer_SuppressesRelayWhileNotRunning is Important 1's TDD test:
// spec §7's FSM RUNNING gate must be enforced at the tx announcement
// relay's shared choke point (txAnnouncer.put), not only in Task 13's
// Kafka-listener gate, because Task 14 added a second producer that gate
// does not cover. Paired with a positive-arrival barrier (F4): a tx queued
// while not RUNNING must never leave, even after the RUNNING flip, and a
// fresh tx queued after the flip must leave — proving the suppression was
// real rather than an unreached code path.
func TestTxAnnouncer_SuppressesRelayWhileNotRunning(t *testing.T) {
	relay := &recordingRelay{}

	var running atomic.Bool

	a := newTxAnnouncer(ulogger.TestLogger{}, relay.relay, running.Load)
	t.Cleanup(func() { a.close(ulogger.TestLogger{}) })

	suppressedHash := chainhash.Hash{0xAA}
	a.put(suppressedHash, 100, 100)

	// Long enough for the batcher's own 1 second timeout to have fired if
	// canRelay had not suppressed the Put before it ever reached the
	// batcher.
	time.Sleep(1500 * time.Millisecond)
	require.Empty(t, relay.all(), "no tx inv must leave while the FSM is not RUNNING")

	// The barrier: flip to RUNNING and put a fresh tx.
	running.Store(true)

	freshHash := chainhash.Hash{0xBB}
	a.put(freshHash, 200, 200)

	require.Eventually(t, func() bool {
		for _, tx := range relay.all() {
			if tx.TxHash == freshHash {
				return true
			}
		}

		return false
	}, 3*time.Second, 10*time.Millisecond, "a tx queued after the RUNNING flip must be relayed")

	for _, tx := range relay.all() {
		require.NotEqual(t, suppressedHash, tx.TxHash, "a tx suppressed while not RUNNING must never leave later either")
	}
}
