package p2p

import (
	"net"
	"net/netip"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// admissionBans records the ban rules this process has applied to the message
// bus's connection gater, so a sync can lift exactly the rules no ban asks for
// any more. The gater is the primary enforcement of bans: a blocked address,
// subnet or peer ID is refused on every new connection, inbound and outbound,
// and its open connections are closed when the rule is added. The bus keeps
// rules in memory only, so they are re-applied from the ban list and the
// registry's banned set on every start (see syncAdmissionBans).
//
// mu is held across the whole of a sync and across every direct Block* or
// Unblock* call, so a sync that read the bans before a concurrent ban landed
// cannot lift the rule that ban applies, and a sever (severPeer) cannot lift
// a block a ban holds.
type admissionBans struct {
	mu      sync.Mutex
	addrs   map[netip.Addr]struct{}
	subnets map[string]*net.IPNet
	peers   map[string]struct{}
	// failed holds the rules whose last Block* call failed, so a rule that
	// keeps failing is logged as an error once and then at debug level on
	// each retry, rather than once per sweep.
	failed map[string]struct{}
}

func (a *admissionBans) initLocked() {
	if a.addrs == nil {
		a.addrs = make(map[netip.Addr]struct{})
		a.subnets = make(map[string]*net.IPNet)
		a.peers = make(map[string]struct{})
		a.failed = make(map[string]struct{})
	}
}

// banNetworkAddr returns the single address a ban network covers, unmapped
// from IPv4-mapped IPv6, and whether it covers exactly one. Single addresses
// go to BlockAddr, which the gater checks with a map lookup per connection;
// wider networks go to BlockSubnet, which it scans on every connection.
func banNetworkAddr(n *net.IPNet) (netip.Addr, bool) {
	ones, bits := n.Mask.Size()
	if bits == 0 || ones != bits {
		return netip.Addr{}, false
	}

	addr, ok := netip.AddrFromSlice(n.IP)
	if !ok {
		return netip.Addr{}, false
	}

	return addr.Unmap(), true
}

// admissionEnforced reports whether bans reach the bus's connection gater
// (p2p_enforce_bans_at_admission). When it is off, no Block* or Unblock*
// call is made: bans are enforced on messages only, and an operator
// disconnect acts at the application layer only.
func (s *Server) admissionEnforced() bool {
	return s.P2PClient != nil && s.settings != nil && s.settings.P2P.EnforceBansAtAdmission
}

// syncAdmissionBans makes the bus's admission rules match the bans in force:
// every IP and subnet on the ban list and every libp2p peer ID in the
// registry's banned set is blocked, and every rule applied earlier that no
// ban asks for any more is lifted. It is level-triggered and idempotent, so
// every trigger calls the same thing: startup, each ban-list event (whose
// delivery order is not guaranteed), the operator ban RPCs, and the periodic
// sweep, which is what catches changes that raise no event (an expiry, a
// Clear by the legacy service, which shares the list, a registry ban that
// lapsed).
//
// A Block* call that fails is logged and left unapplied, so the next sync
// retries it; the application-layer checks (the gossip gate, the sweep) keep
// enforcing the ban meanwhile. A failure never stops a ban from being
// recorded, because the ban list and the registry are written before this
// runs.
func (s *Server) syncAdmissionBans() {
	if !s.admissionEnforced() {
		return
	}

	a := &s.admission

	a.mu.Lock()
	defer a.mu.Unlock()

	a.initLocked()

	// Every rule some ban asks for, keyed as failed is, so failures of rules
	// no ban wants any more can be forgotten.
	wanted := make(map[string]struct{})

	if s.banList != nil {
		wantAddrs := make(map[netip.Addr]struct{})
		wantSubnets := make(map[string]*net.IPNet)

		for _, n := range s.banList.ActiveNetworks() {
			if addr, ok := banNetworkAddr(n); ok {
				wantAddrs[addr] = struct{}{}
				wanted[addr.String()] = struct{}{}
			} else {
				wantSubnets[n.String()] = n
				wanted[n.String()] = struct{}{}
			}
		}

		for addr := range a.addrs {
			if _, ok := wantAddrs[addr]; !ok {
				s.unblockAddrLocked(addr)
			}
		}

		for key, n := range a.subnets {
			if _, ok := wantSubnets[key]; !ok {
				s.unblockSubnetLocked(key, n)
			}
		}

		for addr := range wantAddrs {
			s.blockAddrLocked(addr)
		}

		for key, n := range wantSubnets {
			s.blockSubnetLocked(key, n)
		}
	}

	// The mirror keeps its last good set across a failed refresh and an
	// invalidation, so a registry outage never lifts a peer block here.
	wantPeers := make(map[string]struct{})

	for _, id := range s.bannedPeers.bannedIDs() {
		// The registry also lists wire-protocol peers, whose IDs are not
		// libp2p peer IDs; the gater cannot hold them.
		if _, err := peer.Decode(id); err == nil {
			wantPeers[id] = struct{}{}
			wanted[id] = struct{}{}
		}
	}

	for id := range a.peers {
		if _, ok := wantPeers[id]; !ok {
			s.unblockPeerLocked(id)
		}
	}

	for id := range wantPeers {
		s.blockPeerLocked(id)
	}

	for rule := range a.failed {
		if _, ok := wanted[rule]; !ok {
			delete(a.failed, rule)
		}
	}
}

// blockBannedPeer applies a ban this process has just recorded (onPeerBanned)
// at admission at once, without waiting for the next sync: the peer ID, and
// each IP the ban list accepted for it. Both are already in the registry's
// banned set and on the ban list, so a concurrent sync wants them too.
func (s *Server) blockBannedPeer(peerID string, ips []net.IP) {
	if !s.admissionEnforced() {
		return
	}

	a := &s.admission

	a.mu.Lock()
	defer a.mu.Unlock()

	a.initLocked()

	for _, ip := range ips {
		if addr, ok := netip.AddrFromSlice(ip); ok {
			s.blockAddrLocked(addr.Unmap())
		}
	}

	s.blockPeerLocked(peerID)
}

// severPeer closes the open connections to peerID through the bus. The bus
// has no plain disconnect, so this blocks the peer, which closes them, and
// lifts the block straight away unless a ban already holds it. The peer may
// reconnect afterwards: banning, not severing, is what keeps it out.
func (s *Server) severPeer(peerID string) {
	if !s.admissionEnforced() {
		return
	}

	a := &s.admission

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, banned := a.peers[peerID]; banned {
		return
	}

	if err := s.P2PClient.BlockPeer(peerID); err != nil {
		s.logger.Errorf("[severPeer] closing the connections to %s failed: %v", peerID, err)
		return
	}

	if err := s.P2PClient.UnblockPeer(peerID); err != nil {
		s.logger.Errorf("[severPeer] lifting the temporary block on %s failed: %v", peerID, err)
	}
}

func (s *Server) blockAddrLocked(addr netip.Addr) {
	a := &s.admission
	if _, ok := a.addrs[addr]; ok {
		return
	}

	if err := s.P2PClient.BlockAddr(net.IP(addr.AsSlice())); err != nil {
		s.logAdmissionFailureLocked(addr.String(), err)
		return
	}

	delete(a.failed, addr.String())
	a.addrs[addr] = struct{}{}
}

func (s *Server) blockSubnetLocked(key string, n *net.IPNet) {
	a := &s.admission
	if _, ok := a.subnets[key]; ok {
		return
	}

	if err := s.P2PClient.BlockSubnet(n); err != nil {
		s.logAdmissionFailureLocked(key, err)
		return
	}

	delete(a.failed, key)
	a.subnets[key] = n
}

func (s *Server) blockPeerLocked(peerID string) {
	a := &s.admission
	if _, ok := a.peers[peerID]; ok {
		return
	}

	if err := s.P2PClient.BlockPeer(peerID); err != nil {
		s.logAdmissionFailureLocked(peerID, err)
		return
	}

	delete(a.failed, peerID)
	a.peers[peerID] = struct{}{}
}

// The Unblock* calls fail only on input the matching Block* call accepted, so
// a failure is logged and the rule is forgotten rather than retried forever.

func (s *Server) unblockAddrLocked(addr netip.Addr) {
	if err := s.P2PClient.UnblockAddr(net.IP(addr.AsSlice())); err != nil {
		s.logger.Warnf("[syncAdmissionBans] unblocking address %s failed: %v", addr, err)
	}

	delete(s.admission.addrs, addr)
}

func (s *Server) unblockSubnetLocked(key string, n *net.IPNet) {
	if err := s.P2PClient.UnblockSubnet(n); err != nil {
		s.logger.Warnf("[syncAdmissionBans] unblocking subnet %s failed: %v", key, err)
	}

	delete(s.admission.subnets, key)
}

func (s *Server) unblockPeerLocked(peerID string) {
	if err := s.P2PClient.UnblockPeer(peerID); err != nil {
		s.logger.Warnf("[syncAdmissionBans] unblocking peer %s failed: %v", peerID, err)
	}

	delete(s.admission.peers, peerID)
}

// logAdmissionFailureLocked logs a failed Block* call as an error the first
// time a rule fails and at debug level while it keeps failing.
func (s *Server) logAdmissionFailureLocked(rule string, err error) {
	if _, seen := s.admission.failed[rule]; seen {
		s.logger.Debugf("[syncAdmissionBans] blocking %s at connection admission still fails (application-layer ban checks still apply): %v", rule, err)
		return
	}

	s.admission.failed[rule] = struct{}{}
	s.logger.Errorf("[syncAdmissionBans] blocking %s at connection admission failed; application-layer ban checks still apply, and the next sync retries: %v", rule, err)
}
