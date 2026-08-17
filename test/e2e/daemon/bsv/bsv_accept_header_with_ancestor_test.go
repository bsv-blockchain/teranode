package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// TestBSVAcceptHeaderWithAncestor ports bsv-accept-header-with-ancestor.py, which
// walks a node through headers-first block announcement: announce a header, get a
// getdata, supply the block, then check that a header whose parent is unknown draws
// no getdata until the parent's header has been supplied too.
//
// None of that sequence can run against Teranode, because it does not survive the
// first step. An unsolicited headers message costs the peer its connection:
// SyncManager.handleHeadersMsg (services/legacy/netsync/manager.go:2054) treats any
// headers arriving outside headers-first mode as misbehaviour -
//
//	if !sm.headersFirstMode.Load() {
//	        reason := fmt.Sprintf("Got %d unrequested headers from %s", ...)
//	        peer.DisconnectWithWarning(reason)
//
// - and headers-first mode is an initial-sync state, so a steady-state node
// disconnects. Announcing a new block by header is how BSV peers have propagated
// blocks since BIP130, which makes this an interop divergence rather than a
// testing inconvenience. See the headers-announcement-disconnects gap.
//
// So the port asserts the disconnect as a tripwire, and pairs it with the control
// that gives it meaning: the same exchange in the opposite direction - the peer
// asking with getheaders and the node answering with headers - works, and the peer
// keeps its connection. Without that control a disconnect here would be equally
// consistent with the harness sending a malformed message.
func TestBSVAcceptHeaderWithAncestor(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	// A chain longer than genesis, so the control below has a header to be given.
	td.MineBlocks(t, 1)

	t.Run("announcing a block header disconnects the peer", func(t *testing.T) {
		p := wirepeer.Connect(t, td)
		defer p.Close()

		// Upstream's step 1: build B0 on the current tip. Upstream then expects a
		// getdata for it.
		parent := blockOf(t, td, tipHeader(t, td))
		_, b0 := td.CreateTestBlock(t, parent, nextNonce(t))
		announced := asMsgBlock(t, b0)

		headers := wire.NewMsgHeaders()
		require.NoError(t, headers.AddBlockHeader(&announced.Header), "build the headers announcement")

		p.Send(t, headers)

		p.AssertDisconnected(t, 15*time.Second,
			"TRIPWIRE: Teranode now tolerates an unsolicited headers announcement. If it has learned to "+
				"answer one with a getdata, revisit the headers-announcement-disconnects gap and port "+
				"the rest of bsv-accept-header-with-ancestor.py, which needs this connection to survive")

		// Upstream's step 2 is a getdata naming the announced block. Nothing arrives,
		// and the connection is gone - asserted separately from the disconnect so a
		// future node that disconnects but still asks is not mistaken for this one.
		require.Empty(t, p.Received(wire.CmdGetData),
			"the node should not have asked for the announced block")

		// The node dropped the peer rather than merely ignoring it: nothing is left
		// connected.
		require.Eventually(t, func() bool { return len(tryPeers(td)) == 0 },
			rpcSettle, rpcPoll, "the announcing peer should be gone from the node's peer list")
	})

	t.Run("the same exchange pulled by the peer works and costs nothing", func(t *testing.T) {
		// The control. getheaders is the peer asking the node for headers - the
		// opposite direction to the announcement above, and the direction Teranode
		// supports. If this failed too, the subtest above would be evidence about the
		// harness rather than about announcement handling.
		p := wirepeer.Connect(t, td)
		defer p.Close()

		msg := wire.NewMsgGetHeaders()
		msg.ProtocolVersion = wire.ProtocolVersion
		msg.BlockLocatorHashes = append(msg.BlockLocatorHashes, td.Settings.ChainCfgParams.GenesisHash)

		p.Send(t, msg)

		got := p.WaitForHeaders(t, 30*time.Second)
		require.NotEmpty(t, got.Headers, "the node should answer getheaders with headers")

		p.AssertStillConnected(t, 2*time.Second,
			"asking for headers must not cost the peer its connection, or the disconnect above says "+
				"nothing specific about announcement")
	})
}
