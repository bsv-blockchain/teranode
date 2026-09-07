package protocol

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/bsv-blockchain/go-wire"
)

// This file is net.cpp CConnman::ThreadOpenConnections (net.cpp:1816) reduced
// to the part svp2p needs: the addrman-fed outbound connection loop. The
// retry/backoff machinery for a NAMED peer already exists in
// PeerManager.dialLoop (legacy_connect_peers), and SVNode keeps those two
// apart the same way — ThreadOpenConnections' `-connect` branch
// (net.cpp:1817-1836) returns without ever reaching the loop below, which is
// why legacy_connect_peers stays dominant when it is set.
//
// DNS seeds (SVNode's ThreadDNSAddressSeed) and the fixed-seed fallback the
// loop reaches when the table is empty live in seeds.go. Still not carried:
// feeler connections (net.cpp:1897-1918), one-shots (ProcessOneShot,
// net.cpp:1801) and addnode slots (net.cpp:2050-2090). Each is listed in the
// task's residual ledger rather than half-built.
//
// LOCKING (the package contract, restated because this file adds a caller of
// it): all sync-state machines are caller-locked under PeerManager.syncMu, the
// lock order is peer lock then manager lock, and NO blocking call may run
// under syncMu. A dial blocks, so it never runs under syncMu or mu: the loop
// snapshots the registry under mu, releases it, reads the address table
// (AddrMan is self-synchronising and outside the sync-state graph), and hands
// the dial itself to a fresh goroutine.

const (
	// defaultOpenConnectionsTick is ThreadOpenConnections' own loop sleep
	// (net.cpp:1846, `interruptNet.sleep_for(500ms)`).
	defaultOpenConnectionsTick = 500 * time.Millisecond

	// outboundMaxSelectTries is nTries' cap: "If we didn't find an appropriate
	// destination after trying 100 addresses fetched from addrman, stop this
	// loop, and let the outer loop run again" (net.cpp:1932-1939).
	outboundMaxSelectTries = 100

	// outboundRecentTryWindow and outboundRecentTryTries are "only consider
	// very recently tried nodes after 30 failed attempts" (net.cpp:1954-1957).
	outboundRecentTryWindow = int64(600)
	outboundRecentTryTries  = 30

	// outboundOddPortTries is "do not allow non-default ports, unless after 50
	// invalid addresses selected already" (net.cpp:1963-1968).
	outboundOddPortTries = 50

	// outboundRequiredServices is REQUIRED_SERVICES (net.h:147): "only connect
	// to full nodes" (net.cpp:1946-1950).
	//
	// SVNode's second, softer services clause (net.cpp:1958-1961, "only
	// consider nodes missing relevant services after 40 failed attempts")
	// is NOT carried because it is dead: nRelevantServices is also NODE_NETWORK
	// (init.cpp:2007), so every address that clause would defer has already
	// been refused by this one.
	outboundRequiredServices = wire.SFNodeNetwork

	// outboundCountFailureGroups is the fCountFailure argument
	// ThreadOpenConnections passes to ConnectNode:
	// `setConnected.size() >= std::min(nMaxConnections - 1, 2)`
	// (net.cpp:1988). svp2p has no maximum-connections setting, and any sane
	// cap leaves that min() at 2, so the comparison is against 2 directly:
	// early-startup dial failures must not count against an address before we
	// have any working connectivity of our own to blame.
	outboundCountFailureGroups = 2
)

// outboundSlots is what ThreadOpenConnections computes under cs_vNodes before
// it touches the address table (net.cpp:1876-1895), plus the two fences
// OpenNetworkConnection applies afterwards (net.cpp:2156-2166).
type outboundSlots struct {
	// connected is every outbound address we hold OR are dialing, keyed by
	// ip:port. It merges two C++ mechanisms that svp2p cannot keep apart:
	// CConnman::FindNode's duplicate-connection fence (net.cpp:2163-2166),
	// and the CSemaphoreGrant semOutbound that ThreadOpenConnections takes
	// BEFORE ConnectNode and moves into the node it created (net.cpp:1852,
	// :2178-2180). That grant is why an in-flight dial already occupies an
	// outbound slot in SVNode, and it is why a dial still in flight is counted
	// here.
	connected map[string]struct{}

	// setConnected is `std::set<std::vector<uint8_t>> setConnected`
	// (net.cpp:1880), keyed by the group bytes: "Only connect out to one peer
	// per network group (/16 for IPv4)". Netgroups for INBOUND peers are
	// deliberately excluded, and for the reason C++ gives at net.cpp:1885-1893
	// — an inbound peer is attacker controlled, so counting its group would
	// let an attacker stop us connecting to particular hosts.
	setConnected map[string]struct{}

	// local stands in for IsLocal(addr) (net.cpp:285), whose C++ backing store
	// is mapLocalHost. svp2p has no mapLocalHost, so the stand-in is our own
	// listen addresses: the one set of addresses we know are ours. A listener
	// bound to a wildcard address is therefore not recognised here; the
	// version-nonce self-connection check (ErrSelfConnection) is what catches
	// that case, one round-trip later.
	local map[string]struct{}
}

// nOutbound is ThreadOpenConnections' own nOutbound counter (net.cpp:1878).
func (s outboundSlots) nOutbound() int {
	return len(s.connected)
}

// outboundSlotsSnapshot builds outboundSlots from the connection registry.
//
// Locking: Peer.Info takes the PEER lock, so the registry is snapshotted under
// mu and released before any Info call — the package lock order is peer lock
// then manager lock, so holding mu across Info would invert it.
func (m *PeerManager) outboundSlotsSnapshot() outboundSlots {
	m.mu.Lock()

	peers := make([]*Peer, 0, len(m.peers))
	for p := range m.peers {
		peers = append(peers, p)
	}

	slots := outboundSlots{
		connected:    make(map[string]struct{}, len(m.outboundDials)+len(peers)),
		setConnected: make(map[string]struct{}, len(peers)),
		local:        make(map[string]struct{}, len(m.listeners)),
	}

	for addr := range m.outboundDials {
		slots.connected[addr] = struct{}{}
	}

	for _, ln := range m.listeners {
		slots.local[ln.Addr().String()] = struct{}{}
	}

	m.mu.Unlock()

	for _, p := range peers {
		info := p.Info()
		if info.Inbound {
			continue
		}

		slots.connected[info.Addr] = struct{}{}
	}

	for addr := range slots.connected {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}

		ip := net.ParseIP(host)
		if ip == nil {
			continue
		}

		slots.setConnected[string(newNetAddr(ip).group())] = struct{}{}
	}

	return slots
}

// openConnectionsLoop is ThreadOpenConnections' "Initiate network connections"
// loop (net.cpp:1838-1991). Started by Start only when there is an address
// table to read, a positive target, and no legacy_connect_peers list.
func (m *PeerManager) openConnectionsLoop(ctx context.Context, addrMan *AddrMan, target int) {
	// nStart (net.cpp:1841), for the fixed-seed grace period.
	started := time.Now()

	for {
		// The sleep is at the TOP of the C++ loop too (net.cpp:1846), which
		// is what keeps a node from hammering the table on startup.
		select {
		case <-time.After(m.outboundTick):
		case <-m.quit:
			return
		case <-ctx.Done():
			return
		}

		// net.cpp:1855-1866: fixed seeds once the table has stayed empty for
		// a minute — "DNS doesn't seem to be available".
		m.addFixedSeedsIfStarved(addrMan, started)

		slots := m.outboundSlotsSnapshot()

		// net.cpp:1908-1918: at the target SVNode considers a feeler
		// connection instead. Feelers are not carried, so this is a plain
		// `continue`.
		if slots.nOutbound() >= target {
			continue
		}

		addrConnect, ok := m.selectOutboundCandidate(addrMan, slots, time.Now().Unix())
		if !ok {
			continue
		}

		m.openNetworkConnection(ctx, addrMan, addrConnect, len(slots.setConnected))
	}
}

// selectOutboundCandidate is the candidate walk at net.cpp:1919-1972 —
// "Choose an address to connect to based on most recently seen".
//
// Locking: reads no manager state. AddrMan.Select takes AddrMan's own lock and
// must not run under syncMu; nothing here holds either manager lock.
func (m *PeerManager) selectOutboundCandidate(addrMan *AddrMan, slots outboundSlots, nANow int64) (Address, bool) {
	nTries := 0

	for {
		// fFeeler selects new-table-only in C++; feelers are not carried, so
		// this is always the full table.
		addr, ok := addrMan.Select(false)
		if !ok {
			return Address{}, false
		}

		na := newNetAddr(addr.IP())
		key := addr.String()

		// "if we selected an invalid address, restart" — note that all three
		// of these BREAK out of the walk in C++, they do not continue
		// (net.cpp:1923-1927).
		if !na.isValid() {
			return Address{}, false
		}

		if _, dup := slots.setConnected[string(na.group())]; dup {
			return Address{}, false
		}

		if _, mine := slots.local[key]; mine {
			return Address{}, false
		}

		nTries++
		if nTries > outboundMaxSelectTries {
			return Address{}, false
		}

		// IsLimited(addr) (net.cpp:1941) is the -onlynet network filter. svp2p
		// has no such setting, so there is nothing to test.

		// "only connect to full nodes"
		if addr.NServices()&outboundRequiredServices != outboundRequiredServices {
			continue
		}

		// "only consider very recently tried nodes after 30 failed attempts"
		if nANow-addrMan.NLastTry(addr) < outboundRecentTryWindow && nTries < outboundRecentTryTries {
			continue
		}

		// "do not allow non-default ports, unless after 50 invalid addresses
		// selected already"
		if strconv.Itoa(int(addr.Port())) != m.tSettings.ChainCfgParams.DefaultPort && nTries < outboundOddPortTries {
			continue
		}

		// OpenNetworkConnection's own two fences (net.cpp:2156-2166), applied
		// here so a refused address costs one walk step rather than a whole
		// tick: a banned address, and one we already hold or are dialing.
		// IsLocal is tested above, where the C++ walk tests it.
		if m.banList.IsBanned(key) {
			continue
		}

		if _, held := slots.connected[key]; held {
			continue
		}

		return addr, true
	}
}

// openNetworkConnection is CConnman::OpenNetworkConnection (net.cpp:2145) plus
// the addrman bookkeeping ConnectNode does inside it (net.cpp:410, :437). The
// duplicate check is a check-and-claim under mu rather than OpenNetworkConnection's
// bare FindNode, because ThreadOpenConnections dials on its own thread while
// this loop must not block: claiming the address is what stops two ticks from
// racing onto one peer.
func (m *PeerManager) openNetworkConnection(ctx context.Context, addrMan *AddrMan, addr Address, nGroups int) {
	key := addr.String()

	// `if (interruptNet) return false;` (net.cpp:2151), checked again after
	// the dial below: Stop disconnects the peers it can see and then waits on
	// the peer goroutines, so a dial that lands after that snapshot must not
	// register a new peer for Stop to wait on for ever.
	if m.shuttingDown(ctx) {
		return
	}

	m.mu.Lock()

	if _, dup := m.outboundDials[key]; dup {
		m.mu.Unlock()
		return
	}

	m.outboundDials[key] = struct{}{}
	m.mu.Unlock()

	// fCountFailure: net.cpp:1988.
	fCountFailure := nGroups >= outboundCountFailureGroups

	m.wg.Add(1)

	go func() {
		defer m.wg.Done()

		defer func() {
			m.mu.Lock()
			delete(m.outboundDials, key)
			m.mu.Unlock()
		}()

		nc, err := m.dialTCP(key)

		// CConnman::ConnectNode marks the attempt on BOTH legs: after the
		// socket is up (net.cpp:410) and after it failed (net.cpp:437).
		addrMan.Attempt(addr, fCountFailure, time.Now().Unix())

		if err != nil {
			m.logger.Debugf("[svp2p] outbound dial %s failed: %v", key, err)
			return
		}

		if m.shuttingDown(ctx) {
			_ = nc.Close()
			return
		}

		// The address is marked Good once the handshake completes, not here:
		// that is ProcessVersionMessage's own order (net_processing.cpp:1872),
		// and GetAddrRequest is where this port keeps that block.
		_ = m.runPeer(ctx, nc, false, nil)
	}()
}

// shuttingDown is `interruptNet` (net.cpp:2150): the node is stopping, so no
// new connection may be opened.
func (m *PeerManager) shuttingDown(ctx context.Context) bool {
	select {
	case <-m.quit:
		return true
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
