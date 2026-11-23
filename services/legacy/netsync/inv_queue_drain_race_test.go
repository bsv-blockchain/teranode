package netsync

import (
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_ConcurrentInvDrainsLoseNoAnnouncement is ChiR2.
//
// The drain reads the front of the announcement queue without consuming it and
// consumes it several branches later. That is deliberate: an item the drain
// cannot deal with has to stay put, because the queue is the only record that a
// block was announced and outside headers-first mode an inv is not guaranteed
// to come again. But inv messages are dispatched one goroutine per message
// (blockHandler's "go sm.handleInvMsg(msg)"), so two drains for the same peer
// share one queue, and txmap.SyncedSlice synchronises each call and not a pair
// of them. Interleaved, both drains peek item x, both deal with it, and the
// second Shift throws item y away without ever looking at it: exactly the loss
// the peek was introduced to prevent.
//
// Concurrency is the subject here, so the goroutines are the point of the test
// rather than an incidental detail. The end state asserted is per round: the
// queue is empty and every hash that went in came out in one of the two
// getdatas. A lost hash is in neither.
func TestSyncManager_ConcurrentInvDrainsLoseNoAnnouncement(t *testing.T) {
	sm := newRaceManager(t)
	sm.requestedTxns = expiringmap.New[chainhash.Hash, struct{}](time.Minute)

	t.Cleanup(func() { sm.requestedTxns.Stop() })

	peer, _, _ := connectRacePeer(t, 70, 1000)
	state := registerInvPeer(sm, peer, 1000)

	// Transaction announcements, because the tx branch of the drain needs no
	// ledger and no chain lookup: the interleaving is the same either way, and
	// this keeps each round cheap enough to run enough of them to catch it.
	const rounds, perRound = 40, 64

	for r := 0; r < rounds; r++ {
		announced := make([]chainhash.Hash, perRound)

		for i := range announced {
			announced[i] = chainhash.Hash{byte(r), byte(i), 0xaa}
			state.requestQueue.Append(&wire.InvVect{Type: wire.InvTypeTx, Hash: announced[i]})
		}

		var (
			wg  sync.WaitGroup
			got [2]*wire.MsgGetData
		)

		for g := 0; g < 2; g++ {
			wg.Add(1)

			go func(g int) {
				defer wg.Done()

				got[g] = sm.drainRequestQueue(peer, state)
			}(g)
		}

		wg.Wait()

		requested := make(map[chainhash.Hash]struct{}, perRound)

		for _, msg := range got {
			for _, iv := range msg.InvList {
				requested[iv.Hash] = struct{}{}
			}
		}

		require.Zero(t, state.requestQueue.Length(), "round %d: the queue should be fully drained", r)

		for _, hash := range announced {
			_, asked := requested[hash]
			require.True(t, asked, "round %d: announcement %s was consumed without being requested", r, hash)
		}
	}
}
