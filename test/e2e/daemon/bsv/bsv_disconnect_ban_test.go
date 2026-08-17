package bsv

import (
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

const (
	// rpcSettle bounds how long an asynchronous ban may take to become visible
	// over RPC. Bans are applied through channels, so every observation here
	// polls rather than reading once.
	rpcSettle = 20 * time.Second
	rpcPoll   = 250 * time.Millisecond
)

// TestBSVDisconnectBan is the Teranode port of bitcoin-sv's disconnect_ban.py.
//
// Upstream runs two connected nodes and drives the ban list from node 1 while
// watching its peer count fall to zero. This port uses one node plus a wirepeer,
// which reproduces the ban-list assertions without depending on multi-node P2P
// sync (see the invalidblockrequest-port-red gap).
//
// It runs against a daemon with both the legacy and the modern P2P service,
// because handleSetBan's only working ban leg is the P2P one.
//
// Reproduced from upstream:
//   - setban add puts exactly one address on the ban list
//   - clearbanned empties the ban list
//   - setban rejects an invalid subnet with an error and adds nothing
//   - setban remove takes an address back off the ban list
//   - a subnet can be banned in CIDR form
//
// Not reproduced. Two of these are Teranode defects found while writing the
// port, recorded as the setban-address-format and listbanned-duplicate-entries
// gaps; the rest are architectural. See waived_assertions in registry.yaml.
//   - upstream asserts getpeerinfo drops to zero after banning the peer's
//     address. Teranode leaves the peer connected.
//   - upstream asserts len(listbanned()) exactly. Teranode reports every ban
//     twice when both peer services run, so this port compares distinct
//     addresses.
//   - upstream asserts RPC error codes -23 (already banned) and -30 (unban
//     failed). Teranode neither rejects a duplicate ban nor fails an unban of an
//     address that was never banned.
//   - upstream stops and restarts the node to check bans persist. A TestDaemon
//     cannot be replaced within one process.
//   - the entire disconnectnode half of the upstream file: Teranode does not
//     implement that RPC.
func TestBSVDisconnectBan(t *testing.T) {
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	const (
		banIP     = "127.0.0.1"
		banSubnet = "10.0.0.0/24"
	)

	// One daemon for all subtests: a TestDaemon cannot be stopped and replaced
	// in the same process. Each subtest leaves the ban list empty behind it,
	// which is what upstream's own clearbanned calls are doing too.
	t.Run("setban add bans a single IP address", func(t *testing.T) {
		t.Cleanup(func() { wirepeer.ClearBanned(t, td) })

		peer := wirepeer.Connect(t, td)
		defer peer.Close()

		// Upstream waits for the peer count before banning; the ban is only
		// meaningful once the node has registered the connection.
		var connected []peerInfo

		require.Eventually(t, func() bool {
			connected = tryPeers(td)

			return len(connected) == 1
		}, rpcSettle, rpcPoll, "the node should report the wirepeer as connected; got %+v", connected)

		_, err := td.CallRPC(td.Ctx, "setban", []any{banIP, "add"})
		require.NoError(t, err, "setban %s add", banIP)

		banned := wirepeer.WaitForBan(t, td, banIP, rpcSettle)
		require.Equal(t, []string{banIP}, distinct(banned),
			"banning one address should put exactly that address on the list; raw list %+v", banned)
	})

	t.Run("clearbanned empties the ban list", func(t *testing.T) {
		_, err := td.CallRPC(td.Ctx, "setban", []any{banSubnet, "add"})
		require.NoError(t, err, "setban %s add", banSubnet)

		wirepeer.WaitForBan(t, td, banSubnet, rpcSettle)

		// ClearBanned polls until the list is empty, which is upstream's
		// assert_equal(len(listbanned()), 0) with the asynchrony allowed for.
		wirepeer.ClearBanned(t, td)
		require.Empty(t, wirepeer.ListBanned(t, td), "clearbanned should leave no entries")
	})

	t.Run("setban rejects an invalid subnet", func(t *testing.T) {
		t.Cleanup(func() { wirepeer.ClearBanned(t, td) })

		// Upstream expects RPC error -30 "Error: Invalid IP/Subnet". Teranode
		// reports -8 "Invalid IP or subnet". The code and wording differ; that
		// the node refuses a /42 prefix is the property being tested.
		_, err := td.CallRPC(td.Ctx, "setban", []any{"127.0.0.1/42", "add"})
		require.Error(t, err, "a /42 prefix is not a valid subnet and must be rejected")
		require.Contains(t, err.Error(), "Invalid IP or subnet", "the error should say why it was rejected")

		require.Empty(t, wirepeer.ListBanned(t, td), "a rejected setban must not add a ban")
	})

	t.Run("setban remove unbans a subnet", func(t *testing.T) {
		t.Cleanup(func() { wirepeer.ClearBanned(t, td) })

		// Upstream bans a subnet, then removes exactly that subnet and asserts
		// the list is empty. Ban two entries so the removal is shown to be
		// targeted rather than a disguised clearbanned.
		for _, addr := range []string{banIP, banSubnet} {
			_, err := td.CallRPC(td.Ctx, "setban", []any{addr, "add"})
			require.NoError(t, err, "setban %s add", addr)

			wirepeer.WaitForBan(t, td, addr, rpcSettle)
		}

		_, err := td.CallRPC(td.Ctx, "setban", []any{banSubnet, "remove"})
		require.NoError(t, err, "setban %s remove", banSubnet)

		var remaining []string

		require.Eventually(t, func() bool {
			remaining = distinct(wirepeer.TryListBanned(td))

			return slices.Equal(remaining, []string{banIP})
		}, rpcSettle, rpcPoll,
			"setban remove should drop only the named subnet; list is %+v", remaining)
	})
}

// distinct returns the sorted, deduplicated addresses of a ban list. Teranode
// reports each ban once per peer service, so raw lengths are not comparable
// with upstream's; the set of banned addresses is.
func distinct(banned []string) []string {
	out := slices.Clone(banned)
	slices.Sort(out)

	return slices.Compact(out)
}
