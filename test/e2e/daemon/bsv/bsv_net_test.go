package bsv

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// pingPongBytes is the wire size of a ping and of a pong: 24 bytes of message
// header plus an 8-byte nonce. Upstream hard-codes the same 32 ("note ping and
// pong are 32 bytes each") and asserts the byte counters move by exactly that.
const pingPongBytes = 32

// TestBSVNet ports net.py, which covers the RPCs implemented in bitcoin-sv's
// rpc/net.cpp.
//
// Upstream drives two full nodes and reads six RPCs. Teranode implements exactly
// one of them: getpeerinfo. getconnectioncount, getnettotals, getnetworkinfo,
// addnode and ping are wired to handleUnimplemented in services/rpc/Server.go,
// and setnetworkactive and getauthconninfo are not in the dispatch table at all
// (verified by probe, and locked in by the last subtest below). So the port
// keeps the assertions that survive on getpeerinfo alone and waives the rest
// against the net-rpcs-unimplemented gap in registry.yaml.
//
// Reproduced from upstream:
//   - the node reports one getpeerinfo entry per connected peer, so the
//     connection count is observable (_test_connection_count, substituting
//     len(getpeerinfo) for the unimplemented getconnectioncount)
//   - each entry carries per-peer bytessent/bytesrecv counters
//     (_test_getnettotals, first half)
//   - a ping/pong round trip moves those counters by exactly 32 bytes each
//     (_test_getnettotals, the ping_results check)
//   - the counters are per-peer: traffic with one peer leaves the other's
//     untouched (upstream's zip over before/after peer_info)
//   - a closed connection stops being reported (_test_getnetworkinginfo's
//     "wait a bit for all sockets to close", minus the setnetworkactive that
//     caused the close upstream)
//
// Not reproduced, all waived in registry.yaml:
//   - node-wide totals: getnettotals' totalbytesrecv/totalbytessent, and their
//     agreement with the sum over getpeerinfo
//   - authenticated connections: the authconn field on getpeerinfo and the
//     whole getauthconninfo RPC (pubkey length, compressed) - a bitcoin-sv
//     feature with no Teranode counterpart
//   - getnetworkinfo's networkactive/connections, and toggling connectivity
//     with setnetworkactive
//   - manual peer administration: addnode and getaddednodeinfo, including the
//     -24 "Node has not been added" error
//   - the direction of the ping: upstream tells the node to ping its peers,
//     whereas here the test peer pings the node. Same counters, same +32, but
//     it exercises the node's pong reply rather than its ping request.
func TestBSVNet(t *testing.T) {
	// P2P is enabled purely for speed, not because the port needs it: with the
	// legacy service alone every getpeerinfo call takes 9.76s instead of 0.5ms,
	// for the same answer. See the getpeerinfo-stalls-without-p2p-service gap.
	// Polling assertions cannot work against a 10s floor per poll.
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	// Both peers are connected once, up front, and the subtests below run in
	// order against them - the counter subtest needs two live peers and the
	// disconnect subtest ends one. Upstream is order-dependent in the same way.
	first := wirepeer.Connect(t, td)
	defer first.Close()

	second := wirepeer.Connect(t, td)
	defer second.Close()

	var firstID, secondID int32

	t.Run("getpeerinfo reports every connected peer", func(t *testing.T) {
		var connected []peerInfo

		require.Eventually(t, func() bool {
			connected = tryPeers(td)
			return len(connected) == 2
		}, rpcSettle, rpcPoll, "the node should report both connected peers, got %+v", connected)

		require.NotEqual(t, connected[0].ID, connected[1].ID, "peers must have distinct ids")
		require.NotEqual(t, connected[0].Addr, connected[1].Addr, "peers must have distinct addresses")

		for _, info := range connected {
			require.True(t, info.Inbound, "peer %d dialled the node, so the node should see it as inbound", info.ID)
		}

		firstID, secondID = connected[0].ID, connected[1].ID
	})

	t.Run("a ping round trip moves that peer's byte counters by 32", func(t *testing.T) {
		before := byID(peers(t, td))
		require.Len(t, before, 2, "both peers should still be connected")

		// getpeerinfo does not say which entry is which wirepeer, so rather than
		// guess an id, ping from one peer and require that exactly one entry
		// moved - which is itself the per-peer assertion upstream makes by
		// zipping before against after.
		first.Send(t, wire.NewMsgPing(0xfeedface))
		first.Wait(t, rpcSettle, "pong", func(m wire.Message) bool { return m.Command() == wire.CmdPong })

		var after map[int32]peerInfo

		require.Eventually(t, func() bool {
			after = byID(tryPeers(td))
			return len(after) == 2 && moved(before, after) == 1
		}, rpcSettle, rpcPoll, "exactly one peer's counters should have moved\n before: %+v\n after: %+v", before, after)

		for id, was := range before {
			is := after[id]
			if is.BytesRecv == was.BytesRecv && is.BytesSent == was.BytesSent {
				continue
			}

			// The ping we sent is what the node received, and the pong it sent
			// back is what it sent - 32 bytes each way, and nothing else.
			require.Equal(t, was.BytesRecv+pingPongBytes, is.BytesRecv,
				"peer %d: the node should have received exactly one ping", id)
			require.Equal(t, was.BytesSent+pingPongBytes, is.BytesSent,
				"peer %d: the node should have sent exactly one pong", id)
		}
	})

	t.Run("a closed connection stops being reported", func(t *testing.T) {
		before := byID(peers(t, td))
		require.Len(t, before, 2, "both peers should still be connected")

		second.Close()

		var remaining []peerInfo

		require.Eventually(t, func() bool {
			remaining = tryPeers(td)
			return len(remaining) == 1
		}, rpcSettle, rpcPoll, "the closed peer should be dropped, got %+v", remaining)

		require.Contains(t, []int32{firstID, secondID}, remaining[0].ID,
			"the surviving peer should be one of the two that connected")
	})

	// A tripwire, not an upstream assertion. Each of these RPCs, once
	// implemented, unblocks a named piece of net.py that this port waives. The
	// test fails when one starts working, which is the moment to come back and
	// finish the port rather than leave the waiver standing on a stale fact.
	t.Run("the net RPCs this port waives are still unavailable", func(t *testing.T) {
		for _, unavailable := range []struct {
			method  string
			params  []any
			unlocks string
		}{
			{"getconnectioncount", nil, "_test_connection_count, asserted directly instead of via getpeerinfo"},
			{"getnettotals", nil, "_test_getnettotals' node-wide totals"},
			{"getnetworkinfo", nil, "_test_getnetworkinginfo's networkactive and connections"},
			{"setnetworkactive", []any{false}, "_test_getnetworkinginfo's connectivity toggle"},
			{"addnode", []any{"127.0.0.1:18333", "add"}, "_test_getaddednodeinfo"},
			{"getaddednodeinfo", []any{"127.0.0.1:18333"}, "_test_getaddednodeinfo"},
			{"getauthconninfo", nil, "_test_getauthconninfo and the authconn field"},
			{"ping", nil, "_test_getnettotals' node-initiated ping"},
		} {
			_, err := td.CallRPC(td.Ctx, unavailable.method, unavailable.params)
			require.Error(t, err,
				"%s now answers - implement the waived assertions it unlocks in net.py (%s) and update the "+
					"net-rpcs-unimplemented gap in registry.yaml", unavailable.method, unavailable.unlocks)
		}
	})
}

// moved counts how many peers' byte counters changed between two getpeerinfo
// snapshots. Peers present in only one snapshot are ignored, so a connection
// arriving or leaving mid-poll does not read as traffic.
func moved(before, after map[int32]peerInfo) int {
	n := 0

	for id, was := range before {
		is, ok := after[id]
		if !ok {
			continue
		}

		if is.BytesRecv != was.BytesRecv || is.BytesSent != was.BytesSent {
			n++
		}
	}

	return n
}
