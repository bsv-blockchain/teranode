package peer

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// newStallTestPeer builds a minimal *Peer with just the channels the stall
// signalling path touches. It deliberately avoids the full newPeer constructor
// (which needs a network connection) — signalReceived only reads the
// association, stream type and stallControl/quit channels.
func newStallTestPeer(st wire.StreamType) *Peer {
	p := &Peer{
		stallControl: make(chan stallControlMsg, 1),
		quit:         make(chan struct{}),
	}
	p.SetStreamType(st)
	return p
}

// TestSignalReceivedFanOutClearsSiblingStreamDeadline reproduces the
// BlockPriority cross-stream stall-deadline orphan.
//
// Teranode sends getheaders/getdata on the GENERAL stream, which arms the
// response deadline on the GENERAL stream's peer (peer.go maybeAddDeadline via
// the outHandler's sccSendMessage). The svnode's BlockPriority policy delivers
// the headers/block response on DATA1, a separate Peer with its own stall
// handler. If the receive signal is delivered only to the DATA1 peer, the
// GENERAL peer's deadline is never cleared and it false-disconnects with a
// spurious "<cmd> timeout" (observed live: a 90s "headers timeout" while a
// fat block was downloading). The receiving stream must therefore propagate
// the receive signal to every stream peer in the association.
func TestSignalReceivedFanOutClearsSiblingStreamDeadline(t *testing.T) {
	general := newStallTestPeer(wire.StreamTypeGeneral)
	data1 := newStallTestPeer(wire.StreamTypeData1)

	assoc := NewAssociation([]byte{0x00, 0x01, 0x02}, general)
	assoc.SetPolicy(wire.BlockPriorityStreamPolicy)
	require.True(t, assoc.AddStream(wire.StreamTypeData1, data1))
	general.SetAssociation(assoc)
	data1.SetAssociation(assoc)

	// The headers response arrives on the DATA1 stream peer.
	data1.signalReceived(wire.NewMsgHeaders())

	// The GENERAL peer — which armed the getheaders deadline — must observe the
	// receive so its stall handler can clear the pending response.
	select {
	case got := <-general.stallControl:
		require.Equal(t, sccReceiveMessage, got.command)
		require.Equal(t, wire.CmdHeaders, got.message.Command())
	case <-time.After(time.Second):
		t.Fatal("GENERAL peer never observed the DATA1-delivered response; " +
			"its stall deadline is orphaned and would fire a spurious timeout")
	}
}

// TestSignalReceivedNonMultistreamNotifiesSelf verifies the non-association
// path is unchanged: a peer with no association notifies only its own stall
// handler.
func TestSignalReceivedNonMultistreamNotifiesSelf(t *testing.T) {
	p := newStallTestPeer(wire.StreamTypeGeneral)

	p.signalReceived(wire.NewMsgHeaders())

	select {
	case got := <-p.stallControl:
		require.Equal(t, sccReceiveMessage, got.command)
		require.Equal(t, wire.CmdHeaders, got.message.Command())
	case <-time.After(time.Second):
		t.Fatal("peer did not observe its own received message")
	}
}

// TestExpiredStallResponseSuppressesNonBlockWhileBlockInFlight covers the
// block-in-flight suppression: a headers deadline that has expired must not
// trigger a disconnect while a (not-yet-expired) block response is still
// awaited, because under BlockPriority the headers reply is head-of-line
// blocked behind the multi-minute block on the shared DATA1 stream.
func TestExpiredStallResponseSuppressesNonBlockWhileBlockInFlight(t *testing.T) {
	now := time.Unix(1_000_000, 0)

	t.Run("no pending responses", func(t *testing.T) {
		cmd, stalled := expiredStallResponse(map[string]time.Time{}, now, 0)
		require.False(t, stalled)
		require.Empty(t, cmd)
	})

	t.Run("expired headers, no block pending -> disconnect", func(t *testing.T) {
		pending := map[string]time.Time{
			wire.CmdHeaders: now.Add(-time.Second),
		}
		cmd, stalled := expiredStallResponse(pending, now, 0)
		require.True(t, stalled)
		require.Equal(t, wire.CmdHeaders, cmd)
	})

	t.Run("expired headers, block in flight -> suppressed", func(t *testing.T) {
		pending := map[string]time.Time{
			wire.CmdHeaders: now.Add(-time.Second),    // expired
			wire.CmdBlock:   now.Add(4 * time.Minute), // block still has time
		}
		cmd, stalled := expiredStallResponse(pending, now, 0)
		require.False(t, stalled, "headers must not stall while a block is in flight")
		require.Empty(t, cmd)
	})

	t.Run("expired block -> disconnect even with headers pending", func(t *testing.T) {
		pending := map[string]time.Time{
			wire.CmdHeaders: now.Add(-time.Second),
			wire.CmdBlock:   now.Add(-time.Second), // block itself stalled
		}
		cmd, stalled := expiredStallResponse(pending, now, 0)
		require.True(t, stalled)
		require.Equal(t, wire.CmdBlock, cmd, "an expired block deadline is a real stall")
	})

	t.Run("offset defers an otherwise-expired deadline", func(t *testing.T) {
		pending := map[string]time.Time{
			wire.CmdHeaders: now.Add(-time.Second),
		}
		// A 2s handler offset pushes the effective deadline past now.
		cmd, stalled := expiredStallResponse(pending, now, 2*time.Second)
		require.False(t, stalled)
		require.Empty(t, cmd)
	})
}

// TestSignalReceivedSelfNotifiedWhenRemovedFromStreams covers the teardown
// edge: a peer that still references its association but has already been
// removed from the stream set (RemoveStream) must still notify its own stall
// handler so its deadlines clear.
func TestSignalReceivedSelfNotifiedWhenRemovedFromStreams(t *testing.T) {
	general := newStallTestPeer(wire.StreamTypeGeneral)

	assoc := NewAssociation([]byte{0x09}, general)
	assoc.SetPolicy(wire.BlockPriorityStreamPolicy)
	general.SetAssociation(assoc)

	// Simulate mid-teardown: general is no longer in the association's streams.
	assoc.RemoveStream(wire.StreamTypeGeneral)
	require.Empty(t, assoc.StreamPeers())

	general.signalReceived(wire.NewMsgHeaders())

	select {
	case got := <-general.stallControl:
		require.Equal(t, sccReceiveMessage, got.command)
	case <-time.After(time.Second):
		t.Fatal("peer removed from streams did not notify its own stall handler")
	}
}

// TestClearBlockResponseGroup guards that the post-block deadline refresh only
// fires when a block fetch was actually outstanding — an unsolicited relayed tx
// must not refresh (and thereby perpetually defer) an unrelated pending
// deadline such as a stalled getheaders.
func TestClearBlockResponseGroup(t *testing.T) {
	now := time.Unix(2_000_000, 0)

	t.Run("block completion refreshes queued non-block deadline", func(t *testing.T) {
		pending := map[string]time.Time{
			wire.CmdBlock:   now.Add(time.Minute),  // block was in flight
			wire.CmdHeaders: now.Add(-time.Second), // expired while suppressed
		}
		refreshed := clearBlockResponseGroup(pending, now)
		require.True(t, refreshed)
		require.NotContains(t, pending, wire.CmdBlock, "block group is cleared")
		require.Equal(t, now.Add(stallResponseTimeout*3), pending[wire.CmdHeaders],
			"headers restored to full 90s budget")
	})

	t.Run("unsolicited tx (no block pending) does not refresh", func(t *testing.T) {
		expired := now.Add(-time.Second)
		pending := map[string]time.Time{
			wire.CmdHeaders: expired, // genuinely stalled getheaders
		}
		refreshed := clearBlockResponseGroup(pending, now)
		require.False(t, refreshed, "a relayed tx with no block pending must not refresh")
		require.Equal(t, expired, pending[wire.CmdHeaders],
			"stalled getheaders deadline is left untouched so it can still fire")
	})

	t.Run("tx as a genuine getdata response refreshes", func(t *testing.T) {
		pending := map[string]time.Time{
			wire.CmdTx:  now.Add(time.Minute),
			wire.CmdInv: now.Add(-time.Second),
		}
		refreshed := clearBlockResponseGroup(pending, now)
		require.True(t, refreshed)
		require.NotContains(t, pending, wire.CmdTx)
		require.Equal(t, now.Add(stallResponseTimeoutBlocks), pending[wire.CmdInv])
	})
}

// TestShouldExtendBlockDeadline guards the wall-clock cap on throughput-based
// block-deadline extension: a healthy download is extended only while within
// MaxBlockDownloadTime, so a peer dribbling bytes forever cannot hold the sync
// slot indefinitely.
func TestShouldExtendBlockDeadline(t *testing.T) {
	now := time.Unix(3_000_000, 0)
	start := now.Add(-time.Minute) // fetch in flight for 1 minute

	t.Run("healthy block within cap extends", func(t *testing.T) {
		require.True(t, shouldExtendBlockDeadline(wire.CmdBlock, true, start, now))
	})

	t.Run("non-block command never extends", func(t *testing.T) {
		require.False(t, shouldExtendBlockDeadline(wire.CmdHeaders, true, start, now))
	})

	t.Run("unhealthy throughput does not extend", func(t *testing.T) {
		require.False(t, shouldExtendBlockDeadline(wire.CmdBlock, false, start, now))
	})

	t.Run("no fetch in flight does not extend", func(t *testing.T) {
		require.False(t, shouldExtendBlockDeadline(wire.CmdBlock, true, time.Time{}, now))
	})

	t.Run("past the wall-clock cap stops extending despite healthy throughput", func(t *testing.T) {
		overCap := now.Add(-MaxBlockDownloadTime - time.Second)
		require.False(t, shouldExtendBlockDeadline(wire.CmdBlock, true, overCap, now),
			"a slow-drip peer must be rotated once the cap is exceeded")
	})
}

// TestResponseStallBudget guards the per-command refresh budgets so that, after
// a block fetch completes, a head-of-line-blocked response is restored to its
// original allowance (notably headers' 90s) rather than the 30s base.
func TestResponseStallBudget(t *testing.T) {
	require.Equal(t, stallResponseTimeout*3, responseStallBudget(wire.CmdHeaders),
		"headers must keep their extended load budget on refresh")

	for _, cmd := range []string{wire.CmdBlock, wire.CmdMerkleBlock, wire.CmdTx, wire.CmdNotFound, wire.CmdInv} {
		require.Equal(t, stallResponseTimeoutBlocks, responseStallBudget(cmd),
			"block-family response %s must get the block budget", cmd)
	}

	require.Equal(t, stallResponseTimeout, responseStallBudget(wire.CmdVerAck),
		"other responses fall back to the base budget")
}
