package protocol

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"net"
	"strconv"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
)

// This file is net.cpp CConnman::ThreadDNSAddressSeed (net.cpp:1713-1777) and
// the fixed-seed fallback inside ThreadOpenConnections (net.cpp:1855-1866).
// Together they are how a node with an empty address table finds its first
// peer; without them svp2p could only bootstrap from peers.json or
// legacy_connect_peers (parity-watchlist scenario 8).
//
// Feelers, one-shots and addnode slots remain uncarried (outbound.go).

const (
	// defaultDNSSeedDelay is ThreadDNSAddressSeed's opening sleep when the
	// table is NOT empty (net.cpp:1720): give the existing addresses eleven
	// seconds to yield connections before spending a DNS query.
	defaultDNSSeedDelay = 11 * time.Second

	// dnsSeedRelevantPeers is the "nRelevant >= 2" test (net.cpp:1731).
	dnsSeedRelevantPeers = 2

	// defaultFixedSeedGrace is `GetTime() - nStart > 60` (net.cpp:1856).
	defaultFixedSeedGrace = 60 * time.Second

	// dnsSeedMinAge and dnsSeedAgeSpread give "a random age between 3 and 7
	// days old" (net.cpp:1757-1758).
	dnsSeedMinAge    = 3 * 24 * 3600
	dnsSeedAgeSpread = 4 * 24 * 3600
)

// dnsLookupFunc resolves a seed host. The default is the system resolver;
// tests inject their own.
type dnsLookupFunc func(ctx context.Context, host string) ([]net.IP, error)

func defaultDNSLookup(ctx context.Context, host string) ([]net.IP, error) {
	return net.DefaultResolver.LookupIP(ctx, "ip", host)
}

// dnsSeedingNeeded is the opening decision of ThreadDNSAddressSeed
// (net.cpp:1714-1735): "only query DNS seeds if address need is acute". An
// empty table always needs them; a populated one only when fewer than two
// full-node peers are connected, unless forced.
func dnsSeedingNeeded(addrManSize, relevantPeers int, force bool) bool {
	if addrManSize == 0 || force {
		return true
	}

	return relevantPeers < dnsSeedRelevantPeers
}

// relevantPeerCount is the nRelevant loop (net.cpp:1725-1730): established
// peers advertising the required services.
//
// Locking: snapshots the registry under mu, then reads each peer with the
// peer's own lock, never both at once (the package lock order).
func (m *PeerManager) relevantPeerCount() int {
	m.mu.Lock()
	peers := make([]*Peer, 0, len(m.peers))

	for p := range m.peers {
		peers = append(peers, p)
	}
	m.mu.Unlock()

	n := 0

	for _, p := range peers {
		select {
		case <-p.Established():
		default:
			continue
		}

		if p.Services()&outboundRequiredServices == outboundRequiredServices {
			n++
		}
	}

	return n
}

// runDNSSeeding is the goroutine body Start launches in place of
// threadDNSAddressSeed. It waits the eleven seconds only when the table
// already holds addresses, exactly as the C++ does.
func (m *PeerManager) runDNSSeeding(ctx context.Context, addrMan *AddrMan) {
	if addrMan.Size() > 0 {
		select {
		case <-time.After(m.dnsSeedDelay):
		case <-m.quit:
			return
		case <-ctx.Done():
			return
		}

		if !dnsSeedingNeeded(addrMan.Size(), m.relevantPeerCount(), false) {
			m.logger.Infof("[svp2p] P2P peers available, skipped DNS seeding")
			return
		}
	}

	port, err := m.defaultPort()
	if err != nil {
		m.logger.Warnf("[svp2p] DNS seeding skipped: %v", err)
		return
	}

	m.logger.Infof("[svp2p] loading addresses from DNS seeds (could take a while)")

	found := m.seedFromDNS(ctx, addrMan, m.tSettings.ChainCfgParams.DNSSeeds, port, time.Now())

	m.logger.Infof("[svp2p] %d addresses found from DNS seeds", found)
}

// seedFromDNS is the seed loop (net.cpp:1737-1775). Every resolved IP becomes
// one address on the default port with the required services and a random age
// of 3 to 7 days, and the batch is added with the seed's own first address as
// its source (the C++ resolves seed.name a second time for that; one lookup is
// enough here, and avoids the [::] source the C++ TODO warns about). Returns
// the number of addresses found.
func (m *PeerManager) seedFromDNS(ctx context.Context, addrMan *AddrMan, seeds []chaincfg.DNSSeed, port uint16, now time.Time) int {
	found := 0

	for _, seed := range seeds {
		ips, err := m.dnsLookup(ctx, seed.Host)
		if err != nil || len(ips) == 0 {
			m.logger.Debugf("[svp2p] DNS seed %s yielded nothing: %v", seed.Host, err)
			continue
		}

		addrs := make([]Address, 0, len(ips))

		for _, ip := range ips {
			nTime := now.Unix() - dnsSeedMinAge - int64(randBelow(dnsSeedAgeSpread))
			addrs = append(addrs, NewAddressAtTime(ip, port, outboundRequiredServices, nTime))
		}

		addrMan.AddMany(addrs, ips[0], 0)

		found += len(addrs)
	}

	return found
}

// addFixedSeedsIfStarved is the fallback at net.cpp:1855-1866, run once per
// process when the table has stayed empty past the grace period. The source
// is 127.0.0.1, as in the C++ (`LookupHost("127.0.0.1", local, false)`).
func (m *PeerManager) addFixedSeedsIfStarved(addrMan *AddrMan, started time.Time) {
	if m.fixedSeedsAdded || addrMan.Size() > 0 || time.Since(started) <= m.fixedSeedGrace {
		return
	}

	m.fixedSeedsAdded = true

	seeds := m.fixedSeeds()
	if len(seeds) == 0 {
		return
	}

	m.logger.Infof("[svp2p] adding %d fixed seed nodes as DNS doesn't seem to be available", len(seeds))
	addrMan.AddMany(seeds, net.IPv4(127, 0, 0, 1), 0)
}

// defaultFixedSeeds reads the generated table for the configured network.
func (m *PeerManager) defaultFixedSeeds() []Address {
	port, err := m.defaultPort()
	if err != nil {
		m.logger.Warnf("[svp2p] fixed seeds skipped: %v", err)
		return nil
	}

	return fixedSeedsFor(m.tSettings.ChainCfgParams.Net, port)
}

// fixedSeedsFor is convertSeed6 (net.cpp:1667-1685): the generated table for
// net, each entry carrying the required services and "a time slightly in the
// past" — the C++ uses one week; the same here.
func fixedSeedsFor(network wire.BitcoinNet, port uint16) []Address {
	var table []seedSpec6

	switch network {
	case wire.MainNet:
		table = fixedSeedsMain[:]
	case wire.TestNet:
		table = fixedSeedsTest[:]
	default:
		return nil
	}

	nTime := time.Now().Unix() - 7*24*3600
	out := make([]Address, 0, len(table))

	for _, spec := range table {
		ip := net.IP(spec.addr[:])
		if v4 := ip.To4(); v4 != nil {
			ip = v4
		}

		p := spec.port
		if p == 0 {
			p = port
		}

		out = append(out, NewAddressAtTime(ip, p, outboundRequiredServices, nTime))
	}

	return out
}

func (m *PeerManager) defaultPort() (uint16, error) {
	port, err := strconv.ParseUint(m.tSettings.ChainCfgParams.DefaultPort, 10, 16)
	if err != nil {
		return 0, err
	}

	return uint16(port), nil
}

// randBelow is GetRand(n) for the seed age: cryptographic randomness is not
// needed, but the package already avoids math/rand for lint reasons.
func randBelow(n int64) int64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}

	return int64(binary.LittleEndian.Uint64(b[:]) % uint64(n)) //nolint:gosec // n is a small positive constant
}
