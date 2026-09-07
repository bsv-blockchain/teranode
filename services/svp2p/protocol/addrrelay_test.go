package protocol

import (
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// addrAt builds one `addr` message entry. Every field the ported decision
// order reads is explicit: the services filter reads Services, the timestamp
// rules read Timestamp, and the routability tests read IP.
func addrAt(ip string, port uint16, services wire.ServiceFlag, nTime int64) *wire.NetAddress {
	return &wire.NetAddress{
		Timestamp: time.Unix(nTime, 0),
		Services:  services,
		IP:        net.ParseIP(ip),
		Port:      port,
	}
}

func addrIPs(addrs []Address) []string {
	if len(addrs) == 0 {
		return nil
	}

	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.IP().String())
	}

	return out
}

// TestProcessAddrEntries covers ProcessAddrMessage's decision order
// (net_processing.cpp:2270-2368, bitcoin-sv@879fc8b42) plus the empty-list
// disconnect the legacy service adds (OnAddr, services/legacy/peer_server.go:1795-1801).
func TestProcessAddrEntries(t *testing.T) {
	const now int64 = 1_800_000_000

	// The connection's own remote address, which is what the unsolicited-addr
	// fence compares against (net_processing.cpp:2308-2310).
	peerAddr := NewAddress(net.ParseIP("45.10.0.9"), 8333, wire.SFNodeNetwork)

	fresh := now - 60
	stale := now - 3600

	tests := []struct {
		name          string
		entries       []*wire.NetAddress
		inbound       bool
		requestedAddr bool
		wantErr       bool
		wantScore     int
		wantStore     []string
		wantForward   []string
		wantKnown     []string
		wantTimes     map[string]int64
	}{
		{
			// services/legacy/peer_server.go:1795-1801: "Command [addr] from
			// peer does not contain any addresses" -> DisconnectWithWarning.
			name:    "empty list disconnects",
			entries: nil,
			inbound: true,
			wantErr: true,
		},
		{
			// net_processing.cpp:2284-2287: Misbehaving(pfrom, 20,
			// "oversized-addr") and a returned error.
			name:          "oversized list scores twenty and disconnects",
			entries:       oversizedAddrList(now),
			requestedAddr: true,
			wantErr:       true,
			wantScore:     scoreOversizedAddr,
		},
		{
			// net_processing.cpp:2348: a timestamp more than ten minutes in
			// the future is replaced by "five days ago", so the entry is one
			// of the first evicted. The legacy port applies the same rule
			// (services/legacy/peer_server.go:1810-1816).
			name:          "future timestamp is penalized to five days ago",
			entries:       []*wire.NetAddress{addrAt("45.20.0.5", 8333, wire.SFNodeNetwork, now+3600)},
			requestedAddr: true,
			wantStore:     []string{"45.20.0.5"},
			wantKnown:     []string{"45.20.0.5"},
			wantTimes:     map[string]int64{"45.20.0.5": now - addrTimePenaltyBackdate},
		},
		{
			// net_processing.cpp:2348, the other half of the same test:
			// nTime <= 100000000 is the CAddress default, which the legacy
			// port does not handle at all.
			name:          "default timestamp is penalized to five days ago",
			entries:       []*wire.NetAddress{addrAt("45.20.0.6", 8333, wire.SFNodeNetwork, caddressDefaultNTime)},
			requestedAddr: true,
			wantStore:     []string{"45.20.0.6"},
			wantKnown:     []string{"45.20.0.6"},
			wantTimes:     map[string]int64{"45.20.0.6": now - addrTimePenaltyBackdate},
		},
		{
			// net_processing.cpp:2344: (nServices & REQUIRED_SERVICES) !=
			// REQUIRED_SERVICES -> continue. REQUIRED_SERVICES is NODE_NETWORK
			// (net.h:147).
			name:          "entry without NODE_NETWORK is skipped entirely",
			entries:       []*wire.NetAddress{addrAt("45.20.0.7", 8333, 0, fresh)},
			requestedAddr: true,
		},
		{
			// net_processing.cpp:2357-2359: "Do not store addresses outside
			// our network" — an unroutable address is neither stored nor
			// forwarded, but IS marked known.
			name:          "unroutable entry is marked known but never stored",
			entries:       []*wire.NetAddress{addrAt("10.0.0.1", 8333, wire.SFNodeNetwork, fresh)},
			requestedAddr: true,
			wantKnown:     []string{"10.0.0.1"},
		},
		{
			// net_processing.cpp:2353-2356: forwarded when the stamp is newer
			// than ten minutes ago, the batch is at most ten entries, and the
			// address is routable.
			name:          "fresh routable entry in a small batch is forwarded",
			entries:       []*wire.NetAddress{addrAt("45.20.0.5", 8333, wire.SFNodeNetwork, fresh)},
			requestedAddr: true,
			wantStore:     []string{"45.20.0.5"},
			wantForward:   []string{"45.20.0.5"},
			wantKnown:     []string{"45.20.0.5"},
		},
		{
			// The nSince half of the same condition: an hour-old stamp is
			// stored but not forwarded.
			name:          "stale entry is stored but not forwarded",
			entries:       []*wire.NetAddress{addrAt("45.20.0.5", 8333, wire.SFNodeNetwork, stale)},
			requestedAddr: true,
			wantStore:     []string{"45.20.0.5"},
			wantKnown:     []string{"45.20.0.5"},
		},
		{
			// The vAddr.size() <= 10 half: an eleven-entry batch forwards
			// nothing at all, however fresh the entries are.
			name:          "batch larger than ten forwards nothing",
			entries:       freshAddrList(11, fresh),
			requestedAddr: true,
			wantStore:     addrListIPs(11),
			wantKnown:     addrListIPs(11),
		},
		{
			// net_processing.cpp:2302-2332: an unsolicited ADDR from an
			// INBOUND peer may only insert the connecting IP. The reported
			// entry's own port is used, not the connection's.
			name: "unsolicited inbound keeps only the peer's own address",
			entries: []*wire.NetAddress{
				addrAt("45.40.0.1", 8333, wire.SFNodeNetwork, fresh),
				addrAt("45.10.0.9", 9333, wire.SFNodeNetwork, fresh),
				addrAt("45.40.0.2", 8333, wire.SFNodeNetwork, fresh),
			},
			inbound:     true,
			wantStore:   []string{"45.10.0.9"},
			wantForward: []string{"45.10.0.9"},
			wantKnown:   []string{"45.10.0.9"},
		},
		{
			// net_processing.cpp:2325-2331: nothing reported its own address,
			// so nothing is processed — and it is not an error.
			name: "unsolicited inbound that reports no own address is ignored",
			entries: []*wire.NetAddress{
				addrAt("45.40.0.1", 8333, wire.SFNodeNetwork, fresh),
			},
			inbound: true,
		},
		{
			// The fence is INBOUND only (net_processing.cpp:2302): an
			// unsolicited addr on an outbound connection is processed whole.
			name: "unsolicited outbound is processed in full",
			entries: []*wire.NetAddress{
				addrAt("45.40.0.1", 8333, wire.SFNodeNetwork, fresh),
			},
			wantStore:   []string{"45.40.0.1"},
			wantForward: []string{"45.40.0.1"},
			wantKnown:   []string{"45.40.0.1"},
		},
		{
			// requestedAddr true lifts the fence for an inbound peer too.
			name: "solicited inbound is processed in full",
			entries: []*wire.NetAddress{
				addrAt("45.40.0.1", 8333, wire.SFNodeNetwork, fresh),
				addrAt("45.40.0.2", 8333, wire.SFNodeNetwork, fresh),
			},
			inbound:       true,
			requestedAddr: true,
			wantStore:     []string{"45.40.0.1", "45.40.0.2"},
			wantForward:   []string{"45.40.0.1", "45.40.0.2"},
			wantKnown:     []string{"45.40.0.1", "45.40.0.2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := processAddrEntries(tt.entries, peerAddr, tt.inbound, tt.requestedAddr, now)

			if tt.wantErr {
				require.Error(t, got.err)
			} else {
				require.NoError(t, got.err)
			}

			require.Equal(t, tt.wantScore, got.score)
			require.Equal(t, tt.wantStore, addrIPs(got.store))
			require.Equal(t, tt.wantForward, addrIPs(got.forward))
			require.Equal(t, tt.wantKnown, addrIPs(got.known))

			for ip, want := range tt.wantTimes {
				found := false

				for _, a := range got.store {
					if a.IP().String() == ip {
						require.Equal(t, want, a.NTime())

						found = true
					}
				}

				require.True(t, found, "no stored entry for %s", ip)
			}
		})
	}
}

func oversizedAddrList(now int64) []*wire.NetAddress {
	out := make([]*wire.NetAddress, 0, wire.MaxAddrPerMsg+1)
	for i := 0; i <= wire.MaxAddrPerMsg; i++ {
		out = append(out, addrAt("45.20.0.5", uint16(8333+i), wire.SFNodeNetwork, now)) //nolint:gosec // bounded by MaxAddrPerMsg
	}

	return out
}

// freshAddrList builds n routable entries on distinct /24s so no two share a
// group, all stamped nTime.
func freshAddrList(n int, nTime int64) []*wire.NetAddress {
	out := make([]*wire.NetAddress, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, addrAt(addrListIP(i), 8333, wire.SFNodeNetwork, nTime))
	}

	return out
}

// addrListIP builds a routable IPv4 address on its own /16, so no two
// entries share a network group. The RFC 5737 documentation ranges
// (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24) are deliberately NOT used:
// CNetAddr::IsRoutable rejects all three (addrman.go isRFC5737), which would
// make every routability assertion below vacuous.
func addrListIP(i int) string {
	return net.IPv4(45, byte(1+i%250), 0, 1).String() //nolint:gosec // bounded by the modulo
}

func addrListIPs(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, addrListIP(i))
	}

	return out
}

// TestSelectGetAddrResponse covers ProcessGetAddrMessage
// (net_processing.cpp:4096-4129) and the legacy port's best-local-address
// addition (OnGetAddr, services/legacy/peer_server.go:1739-1779).
func TestSelectGetAddrResponse(t *testing.T) {
	cached := []Address{
		NewAddressAtTime(net.ParseIP("45.40.0.1"), 8333, wire.SFNodeNetwork, 1),
		NewAddressAtTime(net.ParseIP("45.40.0.2"), 8333, wire.SFNodeNetwork, 2),
	}

	routableLocal := NewAddressAtTime(net.ParseIP("45.30.0.20"), 8333, wire.SFNodeNetwork, 3)
	loopbackLocal := NewAddressAtTime(net.ParseIP("127.0.0.1"), 8333, wire.SFNodeNetwork, 3)
	portlessLocal := NewAddressAtTime(net.ParseIP("45.30.0.20"), 0, wire.SFNodeNetwork, 3)

	tests := []struct {
		name      string
		inbound   bool
		sentAddr  bool
		cached    []Address
		bestLocal Address
		known     []Address
		want      []string
	}{
		{
			// net_processing.cpp:4103-4109, the fingerprinting defense.
			name:      "outbound peer is ignored",
			inbound:   false,
			cached:    cached,
			bestLocal: routableLocal,
		},
		{
			// net_processing.cpp:4114-4119, fSentAddr.
			name:      "repeated getaddr on the same connection is ignored",
			inbound:   true,
			sentAddr:  true,
			cached:    cached,
			bestLocal: routableLocal,
		},
		{
			// services/legacy/peer_server.go:1766-1777: the cache is trimmed
			// by one entry when the best local address is appended, so the
			// reply cannot outgrow the max send size.
			name:      "inbound peer gets the cache trimmed by one plus our local address",
			inbound:   true,
			cached:    cached,
			bestLocal: routableLocal,
			want:      []string{"45.40.0.2", "45.30.0.20"},
		},
		{
			// "If the port is 0 that indicates no worthy address was found,
			// therefore we do not broadcast it" (peer_server.go:1769-1771).
			name:      "a portless local address is not advertised",
			inbound:   true,
			cached:    cached,
			bestLocal: portlessLocal,
			want:      []string{"45.40.0.1", "45.40.0.2"},
		},
		{
			// net_processing.cpp:1851 advertises a local address only when
			// addr.IsRoutable(); a loopback local address is not worthy.
			name:      "an unroutable local address is not advertised",
			inbound:   true,
			cached:    cached,
			bestLocal: loopbackLocal,
			want:      []string{"45.40.0.1", "45.40.0.2"},
		},
		{
			// CNode::PushAddress (net.h:1241-1252) skips an address already
			// in addrKnown; legacy filters the same way in pushAddrMsg
			// (peer_server.go:513-531).
			name:      "addresses the peer already knows are filtered out",
			inbound:   true,
			cached:    cached,
			bestLocal: portlessLocal,
			known:     []Address{cached[0]},
			want:      []string{"45.40.0.2"},
		},
		{
			name:    "an empty cache and no local address sends nothing",
			inbound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			known := newKnownAddrSet(knownAddrCap)
			for _, a := range tt.known {
				known.mark(a)
			}

			got := selectGetAddrResponse(tt.inbound, tt.sentAddr, tt.cached, tt.bestLocal, known)
			require.Equal(t, tt.want, addrIPs(got))
		})
	}
}

// TestSelectGetAddrResponseCapsAtMaxAddrToSend proves the reply never exceeds
// MAX_ADDR_TO_SEND (net.h:92), which is also go-wire's own encode limit.
func TestSelectGetAddrResponseCapsAtMaxAddrToSend(t *testing.T) {
	cached := make([]Address, 0, wire.MaxAddrPerMsg+50)
	for i := 0; i < wire.MaxAddrPerMsg+50; i++ {
		cached = append(cached, NewAddressAtTime(net.ParseIP(addrListIP(i%200)), uint16(8333+i), wire.SFNodeNetwork, 1)) //nolint:gosec // bounded loop
	}

	got := selectGetAddrResponse(true, false, cached, Address{}, newKnownAddrSet(knownAddrCap))
	require.Len(t, got, wire.MaxAddrPerMsg)
}

// TestSelectAddrRelayTargets covers RelayAddress (net_processing.cpp:998-1041).
func TestSelectAddrRelayTargets(t *testing.T) {
	const now int64 = 1_800_000_000

	addr := NewAddressAtTime(net.ParseIP("45.40.0.77"), 8333, wire.SFNodeNetwork, now)

	var nodeKey [32]byte
	nodeKey[0] = 0x5a

	candidates := func(n int) []addrRelayCandidate {
		out := make([]addrRelayCandidate, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, addrRelayCandidate{addr: addrListIP(i) + ":8333", inbound: true})
		}

		return out
	}

	t.Run("a reachable address goes to two peers", func(t *testing.T) {
		got := selectAddrRelayTargets(candidates(5), addr, true, now, nodeKey)
		require.Len(t, got, addrRelayNodesReachable)
	})

	t.Run("an unreachable address goes to one peer", func(t *testing.T) {
		got := selectAddrRelayTargets(candidates(5), addr, false, now, nodeKey)
		require.Len(t, got, addrRelayNodesUnreachable)
	})

	t.Run("outbound peers are never relay targets", func(t *testing.T) {
		outbound := candidates(5)
		for i := range outbound {
			outbound[i].inbound = false
		}

		require.Empty(t, selectAddrRelayTargets(outbound, addr, true, now, nodeKey))
	})

	t.Run("fewer candidates than the relay count is not padded", func(t *testing.T) {
		got := selectAddrRelayTargets(candidates(1), addr, true, now, nodeKey)
		require.Len(t, got, 1)
	})

	t.Run("the pick is stable for a day and changes across days", func(t *testing.T) {
		pool := candidates(20)

		first := selectAddrRelayTargets(pool, addr, true, now, nodeKey)
		again := selectAddrRelayTargets(pool, addr, true, now+3600, nodeKey)
		require.Equal(t, first, again)

		// net_processing.cpp:1006-1009: "send to the same nodes for 24 hours
		// at a time". Step a long way past the bucket edge so the day index
		// is certainly different.
		later := selectAddrRelayTargets(pool, addr, true, now+5*addrRelayDaySeconds, nodeKey)
		require.NotEqual(t, first, later)
	})

	t.Run("a different node key picks differently", func(t *testing.T) {
		pool := candidates(20)

		var other [32]byte
		other[0] = 0x17

		require.NotEqual(t,
			selectAddrRelayTargets(pool, addr, true, now, nodeKey),
			selectAddrRelayTargets(pool, addr, true, now, other),
		)
	})
}

// TestKnownAddrSetEvictsOldestFirst proves the per-peer known-address set is
// bounded, the exact-set stand-in for CNode::addrKnown's rolling bloom filter
// (net.h:968).
func TestKnownAddrSetEvictsOldestFirst(t *testing.T) {
	set := newKnownAddrSet(2)

	a := NewAddressAtTime(net.ParseIP("45.40.0.1"), 8333, wire.SFNodeNetwork, 1)
	b := NewAddressAtTime(net.ParseIP("45.40.0.2"), 8333, wire.SFNodeNetwork, 1)
	c := NewAddressAtTime(net.ParseIP("45.40.0.3"), 8333, wire.SFNodeNetwork, 1)

	set.mark(a)
	set.mark(b)
	require.True(t, set.has(a))
	require.True(t, set.has(b))

	set.mark(c)
	require.False(t, set.has(a))
	require.True(t, set.has(b))
	require.True(t, set.has(c))
}

// TestKnownAddrSetKeyIncludesThePort mirrors CService::GetKey
// (netaddress.cpp:450): two ports on one IP are different keys.
func TestKnownAddrSetKeyIncludesThePort(t *testing.T) {
	set := newKnownAddrSet(knownAddrCap)

	set.mark(NewAddressAtTime(net.ParseIP("45.40.0.1"), 8333, wire.SFNodeNetwork, 1))
	require.False(t, set.has(NewAddressAtTime(net.ParseIP("45.40.0.1"), 8334, wire.SFNodeNetwork, 1)))
}

// TestKnownAddrSetNilReceiver matches knownBlockSet's own nil contract: a
// state built by a zero-value literal answers "not known" rather than
// panicking.
func TestKnownAddrSetNilReceiver(t *testing.T) {
	var set *knownAddrSet

	require.False(t, set.has(NewAddressAtTime(net.ParseIP("45.40.0.1"), 8333, wire.SFNodeNetwork, 1)))
	set.mark(NewAddressAtTime(net.ParseIP("45.40.0.1"), 8333, wire.SFNodeNetwork, 1))
}
