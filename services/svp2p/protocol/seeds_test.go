package protocol

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestDNSSeedingNeeded is ThreadDNSAddressSeed's opening decision
// (net.cpp:1713-1735): query the seeds only when address need is acute.
func TestDNSSeedingNeeded(t *testing.T) {
	tests := []struct {
		name          string
		addrManSize   int
		relevantPeers int
		force         bool
		want          bool
	}{
		{name: "empty table", addrManSize: 0, relevantPeers: 0, want: true},
		{name: "empty table with peers", addrManSize: 0, relevantPeers: 5, want: true},
		{name: "table but one peer", addrManSize: 10, relevantPeers: 1, want: true},
		{name: "table and two peers", addrManSize: 10, relevantPeers: 2, want: false},
		{name: "forced", addrManSize: 10, relevantPeers: 8, force: true, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, dnsSeedingNeeded(tc.addrManSize, tc.relevantPeers, tc.force))
		})
	}
}

// TestSeedFromDNS_AddsResolvedAddressesToAddrMan is the body of the loop
// (net.cpp:1737-1775): one CAddress per resolved IP on the default port,
// carrying the required services and a random age of 3 to 7 days.
func TestSeedFromDNS_AddsResolvedAddressesToAddrMan(t *testing.T) {
	m := newTestManager(t, nil)
	addrMan := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
	addrMan.makeDeterministic()

	m.dnsLookup = func(_ context.Context, host string) ([]net.IP, error) {
		require.Equal(t, "seed.example", host)

		return []net.IP{net.ParseIP("45.1.0.1"), net.ParseIP("45.2.0.1")}, nil
	}

	now := time.Unix(1_700_000_000, 0)
	found := m.seedFromDNS(context.Background(), addrMan, []chaincfg.DNSSeed{{Host: "seed.example"}}, 18333, now)

	require.Equal(t, 2, found)
	require.Equal(t, 2, addrMan.Size())

	for _, a := range addrMan.GetAddr() {
		require.Equal(t, uint16(18333), a.Port())
		require.Equal(t, wire.SFNodeNetwork, a.NServices())
		require.GreaterOrEqual(t, a.NTime(), now.Unix()-7*24*3600)
		require.LessOrEqual(t, a.NTime(), now.Unix()-3*24*3600)
	}
}

// A seed that does not resolve contributes nothing and stops nothing.
func TestSeedFromDNS_SkipsUnresolvableSeed(t *testing.T) {
	m := newTestManager(t, nil)
	addrMan := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
	addrMan.makeDeterministic()

	m.dnsLookup = func(_ context.Context, host string) ([]net.IP, error) {
		if host == "dead.example" {
			return nil, errors.New(errors.ERR_ERROR, "no such host")
		}

		return []net.IP{net.ParseIP("45.3.0.1")}, nil
	}

	seeds := []chaincfg.DNSSeed{{Host: "dead.example"}, {Host: "live.example"}}
	found := m.seedFromDNS(context.Background(), addrMan, seeds, 8333, time.Now())

	require.Equal(t, 1, found)
	require.Equal(t, 1, addrMan.Size())
}

// TestFixedSeedsFor pins the generated table to its SVNode source
// (chainparams.cpp pnSeed6_main / pnSeed6_test at 879fc8b42).
func TestFixedSeedsFor(t *testing.T) {
	require.Len(t, fixedSeedsFor(wire.MainNet, 8333), 921)
	require.Empty(t, fixedSeedsFor(wire.RegTestNet, 18444), "regtest has no fixed seeds")

	testnet := fixedSeedsFor(wire.TestNet, 18333)
	require.Len(t, testnet, 17)
	require.Equal(t, "35.184.35.1", testnet[0].IP().String())
	require.Equal(t, uint16(18333), testnet[0].Port())
	require.Equal(t, wire.SFNodeNetwork, testnet[0].NServices())
}

// TestOutboundDialerAddsFixedSeedsWhenTableStaysEmpty is net.cpp:1855-1866:
// "Add seed nodes if DNS seeds are all down (an infrastructure attack?)" —
// once, after the grace period, when the table is still empty.
func TestOutboundDialerAddsFixedSeedsWhenTableStaysEmpty(t *testing.T) {
	a := newOutboundTestPeer(t, "45.1.0.1")

	h := newOutboundHarness(t, outboundHarnessOptions{target: 8}, a)
	h.m.fixedSeedGrace = 0

	// The reachable seed rides with 63 unroutable ones (bulkAddresses' own
	// note): CAddrMan.Select over a one-entry table walks most of the
	// 1024x64 new-bucket slots per draw, which under -race took longer than
	// any sane deadline. SVNode's real tables hold 17-921 fixed seeds.
	filler := bulkAddresses(63, 8333, wire.SFNodeNetwork, testNow/1_000_000)
	h.m.fixedSeeds = func() []Address { return append([]Address{a.addr}, filler...) }
	h.start(t)

	require.Eventually(t, func() bool { return establishedCount(h.m) == 1 }, 15*time.Second, 20*time.Millisecond,
		"the fixed seed must be dialed once the table has stayed empty past the grace period")
	// Bucket collisions may drop a few fillers; what matters is that the table
	// was seeded and the reachable one was dialed.
	require.GreaterOrEqual(t, h.addrMan.Size(), 2)
}

// TestOutboundDialerSeedsFromDNSOnStart wires ThreadDNSAddressSeed into
// Start: an empty table and a resolvable seed give the dialer its first peer.
func TestOutboundDialerSeedsFromDNSOnStart(t *testing.T) {
	a := newOutboundTestPeer(t, "45.1.0.1")

	h := newOutboundHarness(t, outboundHarnessOptions{target: 8}, a)

	params := *h.m.tSettings.ChainCfgParams
	params.DNSSeeds = []chaincfg.DNSSeed{{Host: "seed.example"}}
	params.DefaultPort = "8333"
	h.m.tSettings.ChainCfgParams = &params

	h.m.dnsSeedDelay = 0
	h.m.dnsLookup = func(_ context.Context, host string) ([]net.IP, error) {
		require.Equal(t, "seed.example", host)

		return []net.IP{a.addr.IP()}, nil
	}
	h.start(t)

	require.Eventually(t, func() bool { return establishedCount(h.m) == 1 }, 5*time.Second, 20*time.Millisecond)
}

// The operator switch: legacy_disableDNSSeed leaves the table alone.
func TestOutboundDialerDNSSeedingDisabled(t *testing.T) {
	a := newOutboundTestPeer(t, "45.1.0.1")

	h := newOutboundHarness(t, outboundHarnessOptions{target: 8}, a)
	h.m.tSettings.Legacy.DisableDNSSeed = true

	params := *h.m.tSettings.ChainCfgParams
	params.DNSSeeds = []chaincfg.DNSSeed{{Host: "seed.example"}}
	h.m.tSettings.ChainCfgParams = &params

	h.m.dnsSeedDelay = 0
	h.m.dnsLookup = func(_ context.Context, _ string) ([]net.IP, error) {
		t.Fatal("DNS must not be queried when legacy_disableDNSSeed is set")

		return nil, nil
	}
	h.start(t)

	time.Sleep(30 * outboundTestTick)
	require.Equal(t, 0, h.addrMan.Size())
}
