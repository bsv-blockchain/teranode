package protocol

import (
	"net"
	"sort"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// Address message handling: the net_processing.cpp getaddr/addr port.
//
// LOCKING. Everything in this file is a pure decision function: it performs no
// I/O, takes no lock, and touches neither Conn nor Peer. The callers
// (PeerManager.GetAddr and PeerManager.Addr in manager.go) snapshot every
// input under the lock that owns it, run these functions, and send the result
// with no lock held. That is the Phase 2 locking contract this package works
// under: the sync-state machines are caller-locked under PeerManager.syncMu,
// the package lock order is peer lock then manager lock, and no blocking call
// — a Conn.Send, an AddrMan mutation, a log write — may run under syncMu.
// AddrMan itself is self-synchronising and deliberately NOT covered by syncMu
// (addrman.go's own LOCKING note), so the caller reaches it outside syncMu too.

// scoreOversizedAddr is Misbehaving(pfrom, 20, "oversized-addr")
// (net_processing.cpp:2285).
const scoreOversizedAddr = 20

// addrTimeWindow is the ten minutes the addr handler uses for BOTH of its
// timestamp tests: `nSince = nNow - 10 * 60` for the forwarding freshness
// test, and `addr.nTime > nNow + 10 * 60` for the future-stamp penalty
// (net_processing.cpp:2334-2348). The legacy port uses the same ten minutes
// for the penalty half (services/legacy/peer_server.go:1810-1813).
const addrTimeWindow int64 = 10 * 60

// addrTimePenaltyBackdate is the "five days ago" a penalized timestamp is
// moved to (net_processing.cpp:2349, `addr.nTime = nNow - 5 * 24 * 60 * 60`;
// the legacy port's own `now.Add(-1 * time.Hour * 24 * 5)`,
// services/legacy/peer_server.go:1814). The point is eviction order, not
// accuracy: a backdated entry is one of the first CAddrMan drops when space
// is needed.
const addrTimePenaltyBackdate int64 = 5 * 24 * 60 * 60

// addrTimePenaltySource is the two-hour penalty AddNewAddresses applies to
// every address learned from a peer rather than from the peer itself
// (net_processing.cpp:2362, `connman.AddNewAddresses(vAddrOk, peerAddr, 2 * 60
// * 60)`). It is CAddrMan::Add's nTimePenalty parameter (addrman.go Add).
const addrTimePenaltySource int64 = 2 * 60 * 60

// addrForwardBatchMax is the `vAddr.size() <= 10` half of the forwarding gate
// (net_processing.cpp:2353): a batch larger than this forwards nothing at all,
// however fresh its entries are. It is what stops a peer from using a single
// large addr message to fan out through us.
const addrForwardBatchMax = 10

// The two relay widths from RelayAddress (net_processing.cpp:1000-1001):
// `unsigned int nRelayNodes = fReachable ? 2 : 1;`.
const (
	addrRelayNodesReachable   = 2
	addrRelayNodesUnreachable = 1
)

// addrRelayDaySeconds is the 24-hour bucket RelayAddress hashes the clock into
// (net_processing.cpp:1010, `(GetTime() + hashAddr) / (24 * 60 * 60)`), so the
// same address goes to the same peers for a day at a time and those peers'
// own addrKnown sets suppress the repeats.
const addrRelayDaySeconds int64 = 24 * 60 * 60

// knownAddrCap bounds knownAddrSet. SVNode sizes the equivalent per-node
// structure at `CRollingBloomFilter addrKnown { 5000, 0.001 }` (net.h:968);
// this is that capacity as an exact set, so it never produces the bloom's
// false positives. The legacy service uses an exact set too, at 10000
// (services/legacy/peer_server.go:75 maxKnownAddresses); the smaller SVNode
// figure is taken because SVNode is the fidelity reference and because an
// addr reply is capped at MAX_ADDR_TO_SEND (1000) entries, so 5000 already
// covers five full replies.
const knownAddrCap = 5000

// requiredAddrServices is REQUIRED_SERVICES (net.h:147,
// `ServiceFlags(NODE_NETWORK)`): an addr entry that does not advertise it is
// skipped outright (net_processing.cpp:2344).
const requiredAddrServices = wire.SFNodeNetwork

// ErrUnsupportedMessage ends a connection that sent a message this port
// refuses to serve — `mempool` and the filter/cfilter families (spec §6's
// last paragraph). It carries its own error code because the peer is not
// misbehaving: the legacy service disconnects these without scoring anything
// (DisconnectWithWarning at OnMemPool, services/legacy/peer_server.go:886,
// and at the three filter handlers, :1716/:1724/:1733).
var ErrUnsupportedMessage = errors.New(errors.ERR_NETWORK_INVALID_RESPONSE, "svp2p: unsupported message")

// addrKey is CService::GetKey (netaddress.cpp:450) as a comparable Go map key:
// the 16 CNetAddr bytes plus the port, so two ports on one IP are two keys.
// This is deliberately NOT the addrman's own map key, which is the IP alone
// (see AddrMan.mapAddr's doc comment).
type addrKey struct {
	ip   netAddr
	port uint16
}

func keyOfAddress(a Address) addrKey {
	return addrKey{ip: newNetAddr(a.IP()), port: a.Port()}
}

// knownAddrSet is CNode::addrKnown (net.h:968): the addresses already sent to,
// or already received from, one peer. It is the address-side twin of
// knownBlockSet — same FIFO eviction, same nil-receiver contract, different
// key — and it is bounded for the same reason: a peer that streams addr
// messages must not be able to grow our per-connection state without limit.
type knownAddrSet struct {
	set      map[addrKey]struct{}
	order    []addrKey
	capacity int
}

func newKnownAddrSet(capacity int) *knownAddrSet {
	return &knownAddrSet{set: make(map[addrKey]struct{}), capacity: capacity}
}

// has is CRollingBloomFilter::contains (net.h:1245). A nil receiver answers
// false, matching knownBlockSet.has: "not yet known" is the right answer for a
// set that was never given anything to know.
func (k *knownAddrSet) has(a Address) bool {
	if k == nil {
		return false
	}

	_, ok := k.set[keyOfAddress(a)]

	return ok
}

// mark is CNode::AddAddressKnown (net.h:1237), evicting the oldest entry when
// the set is at its cap.
func (k *knownAddrSet) mark(a Address) {
	if k == nil {
		return
	}

	key := keyOfAddress(a)

	if _, ok := k.set[key]; ok {
		return
	}

	if len(k.order) >= k.capacity {
		oldest := k.order[0]
		k.order = k.order[1:]

		delete(k.set, oldest)
	}

	k.set[key] = struct{}{}
	k.order = append(k.order, key)
}

// selectGetAddrResponse is ProcessGetAddrMessage
// (net_processing.cpp:4096-4129, bitcoin-sv@879fc8b42) composed with the
// legacy service's best-local-address addition (OnGetAddr,
// services/legacy/peer_server.go:1739-1780). cached is AddrMan.GetAddr's
// output; bestLocal is this connection's own local address, or a zero Address
// when there is none to advertise. It returns the addresses to send, in send
// order, and nothing at all when the request must be ignored.
//
// Two gates come first, in SVNode's order:
//
//  1. "This asymmetric behavior for inbound and outbound connections was
//     introduced to prevent a fingerprinting attack: an attacker can send
//     specific fake addresses to users' AddrMan and later request them by
//     sending getaddr messages. Making nodes which are behind NAT and can
//     only make outgoing connections ignore the getaddr message mitigates
//     the attack." (net_processing.cpp:4102-4108). The legacy port carries
//     the same rule and the same reason (peer_server.go:1748-1752).
//  2. "Only send one GetAddr response per connection to reduce resource waste
//     and discourage addr stamping of INV announcements."
//     (net_processing.cpp:4111-4113, pfrom->fSentAddr; legacy's sentAddrs,
//     peer_server.go:1753-1758).
//
// The caller owns the fSentAddr write, because only the caller can do it under
// the lock that guards it.
func selectGetAddrResponse(inbound, sentAddr bool, cached []Address, bestLocal Address, known *knownAddrSet) []Address {
	if !inbound || sentAddr {
		return nil
	}

	out := cached

	// "Add our best net address for peers to discover us. If the port is 0
	// that indicates no worthy address was found, therefore we do not
	// broadcast it. We also must trim the cache by one entry if we insert a
	// record to prevent sending past the max send size."
	// (services/legacy/peer_server.go:1766-1777). SVNode has no counterpart
	// inside ProcessGetAddrMessage; its own self-advertisement is a
	// PushAddress at version time, gated on addr.IsRoutable()
	// (net_processing.cpp:1846-1857) — so routability is what stands in here
	// for legacy's addrmgr GetBestLocalAddress "worthy address" test, which
	// this port has no equivalent of. A loopback or RFC1918 local address is
	// therefore never advertised.
	if bestLocal.Port() != 0 && newNetAddr(bestLocal.IP()).isRoutable() {
		if len(out) > 0 {
			out = out[1:]
		}

		out = append(out, bestLocal)
	}

	// CNode::PushAddress skips an address already in addrKnown, "only to save
	// space from duplicates" (net.h:1241-1245); legacy filters the same way in
	// pushAddrMsg (peer_server.go:513-522).
	filtered := make([]Address, 0, len(out))

	for _, a := range out {
		if known.has(a) {
			continue
		}

		filtered = append(filtered, a)
	}

	// MAX_ADDR_TO_SEND (net.h:92) — also go-wire's own encode limit, so
	// exceeding it would not merely be unfaithful, it would fail to
	// serialize. C++ replaces a random existing entry once the vector is
	// full (net.h:1246-1250); truncation is equivalent in distribution here,
	// because AddrMan.GetAddr already returns a randomly ordered selection
	// (addrman.go GetAddr's own random walk) rather than a sorted one.
	if len(filtered) > wire.MaxAddrPerMsg {
		filtered = filtered[:wire.MaxAddrPerMsg]
	}

	if len(filtered) == 0 {
		return nil
	}

	return filtered
}

// addrProcessResult is what one addr message produced, split by what the
// caller must do with each part. Nothing here has been applied yet: the
// caller marks known, feeds store into the addrman, forwards forward, and
// scores score — each under the lock that owns it.
type addrProcessResult struct {
	// store is vAddrOk (net_processing.cpp:2331): the reachable entries that
	// go into the addrman with the source peer.
	store []Address

	// forward is the entries RelayAddress is called for
	// (net_processing.cpp:2353-2356).
	forward []Address

	// known is every entry AddAddressKnown was called for
	// (net_processing.cpp:2350) — which is wider than store: an unroutable
	// entry is marked known and then dropped.
	known []Address

	// score is the misbehavior delta to apply.
	score int

	// err is non-nil when this peer must be disconnected.
	err error
}

// processAddrEntries is ProcessAddrMessage (net_processing.cpp:2270-2368,
// bitcoin-sv@879fc8b42) with the legacy service's empty-list disconnect in
// front of it (OnAddr, services/legacy/peer_server.go:1795-1801). peerAddr is
// the connection's own remote address, which the unsolicited-addr fence
// compares entries against. now stands in for GetAdjustedTime(); this port has
// no peer time offset, so it is plain wall-clock seconds, exactly as
// AddrMan.now is.
//
// Two SVNode gates are deliberately NOT carried, because neither is reachable
// in this port:
//
//   - The old-style-address version gate, `pfrom->nVersion <
//     CADDR_TIME_VERSION && connman.GetAddressCount() > 1000`
//     (net_processing.cpp:2278-2281), and the legacy port's simpler
//     unconditional form (peer_server.go:1789-1792). CADDR_TIME_VERSION is
//     31402 (version.h:24) and this port refuses any peer below
//     MinPeerProtoVersion, 31800 (handshake.go), so no established peer can
//     fail it.
//   - fOneShot's disconnect after processing (net_processing.cpp:2363-2365).
//     This port has no one-shot connection concept — the dialer only makes
//     persistent connections (manager.go dialLoop) — so there is no state to
//     read it from.
//   - The `interruptMsgProc` bail-out inside the entry loop
//     (net_processing.cpp:2340-2342). That is SVNode's shutdown flag for a
//     message-processing thread this port does not have: here the whole call
//     runs on one peer's Run goroutine, which its own context cancellation
//     already tears down, and the loop is bounded at MaxAddrPerMsg entries of
//     pure computation.
func processAddrEntries(entries []*wire.NetAddress, peerAddr Address, inbound, requestedAddr bool, now int64) addrProcessResult {
	// "A message that has no addresses is invalid" — the legacy service
	// disconnects on it (services/legacy/peer_server.go:1795-1801). SVNode has
	// no such test; it is carried because it is the current behavior of the
	// service this port replaces, and because an empty addr is either a bug
	// or a probe on the sender's side either way.
	if len(entries) == 0 {
		return addrProcessResult{err: errors.New(errors.ERR_NETWORK_INVALID_RESPONSE, "svp2p: addr message contains no addresses")}
	}

	// net_processing.cpp:2283-2287. go-wire's own decoder already refuses a
	// count above MaxAddrPerMsg (go-wire msg_addr.go Bsvdecode), which fails
	// the connection in transport before this is ever reached, so this is a
	// second fence rather than the only one — kept because the machine is
	// callable independently of that decoder and must not rely on it.
	if len(entries) > wire.MaxAddrPerMsg {
		return addrProcessResult{
			score: scoreOversizedAddr,
			err:   errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: oversized addr message (%d entries)", len(entries), ErrProtocolViolation),
		}
	}

	addrs := make([]Address, 0, len(entries))

	for _, na := range entries {
		if na == nil {
			continue
		}

		addrs = append(addrs, AddressFromNetAddress(na))
	}

	// net_processing.cpp:2298-2332. "To avoid malicious flooding of our
	// address table, only allow unsolicited ADDR messages to insert the
	// connecting IP. We need to allow this IP to be inserted, or there is no
	// way for that node to tell the network about itself if its behind a
	// NAT." The comparison is on the CNetAddr part only — "Server listen port
	// will be different. We want to compare IPs and then use provided port"
	// (:2308-2309) — so the reported entry's own port survives.
	//
	// requestedAddr is the caller's `pfrom->fGetAddr.exchange(false)`
	// (:2290-2291): true only when WE sent this peer a getaddr and it has not
	// been answered yet. This port sends getaddr on outbound connections only
	// (manager.go Established, net_processing.cpp:1867-1871), so in practice
	// the fence fires for every inbound addr — which is precisely its intent.
	if !requestedAddr && inbound {
		peerIP := newNetAddr(peerAddr.IP())
		ownAddr, reportedOwnAddr := Address{}, false

		for _, a := range addrs {
			if newNetAddr(a.IP()) == peerIP {
				ownAddr = a
				reportedOwnAddr = true

				break
			}
		}

		if !reportedOwnAddr {
			// "Today unsolicited ADDRs are not illegal, but we should consider
			// misbehaving on this because a few unsolicited ADDRs are ok from
			// a DOS perspective but lots are not." (:2325-2328) — SVNode
			// scores nothing and returns success, and so does this.
			return addrProcessResult{}
		}

		// "Get rid of every address the remote node tried to inject except
		// itself." (:2318-2320)
		addrs = []Address{ownAddr}
	}

	// net_processing.cpp:2333-2361.
	nSince := now - addrTimeWindow

	var out addrProcessResult

	for _, a := range addrs {
		// net_processing.cpp:2344-2346.
		if a.NServices()&requiredAddrServices != requiredAddrServices {
			continue
		}

		// net_processing.cpp:2348-2350: a stamp at or below the CAddress
		// default, or more than ten minutes in the future, is moved to five
		// days ago. The legacy port carries only the future half
		// (peer_server.go:1810-1816).
		nTime := a.NTime()
		if nTime <= caddressDefaultNTime || nTime > now+addrTimeWindow {
			nTime = now - addrTimePenaltyBackdate
		}

		a = NewAddressAtTime(a.IP(), a.Port(), a.NServices(), nTime)

		// net_processing.cpp:2350. Marked for EVERY entry that got this far,
		// before either of the tests below — which is why known is wider than
		// store, and why the caller must apply it BEFORE selecting relay
		// targets: it is what stops the source peer being handed its own
		// address back (RelayAddress reaches every inbound peer, and
		// CNode::PushAddress filters on addrKnown, net.h:1245).
		out.known = append(out.known, a)

		// IsReachable(addr) (net_processing.cpp:2352) tests the address
		// against the networks this node is configured to reach. This port has
		// no per-network reachability configuration, so routability stands in
		// for it: CNetAddr::IsRoutable is the same test with every supported
		// network enabled, which is what an unconfigured node has.
		reachable := newNetAddr(a.IP()).isRoutable()

		// net_processing.cpp:2353-2356.
		if nTime > nSince && len(addrs) <= addrForwardBatchMax && reachable {
			out.forward = append(out.forward, a)
		}

		// net_processing.cpp:2357-2360, "Do not store addresses outside our
		// network".
		if reachable {
			out.store = append(out.store, a)
		}
	}

	return out
}

// addrRelayCandidate is one connected peer as RelayAddress's node sort sees it
// — the address-side counterpart of relayCandidate (relay.go). It carries the
// peer handle to send on, the peer's own remote address (which is what stands
// in for CNode::id in the per-node hash, see selectAddrRelayTargets), and
// whether the connection is inbound.
type addrRelayCandidate struct {
	peer *Peer
	sync *SyncPeer

	// addr is the peer's remote address in host:port form (SyncPeer.Addr).
	addr string

	// inbound gates the candidate: RelayAddress only ever considers inbound
	// peers (net_processing.cpp:1017).
	inbound bool
}

// selectAddrRelayTargets is RelayAddress (net_processing.cpp:998-1041,
// bitcoin-sv@879fc8b42). SVNode IS the source of truth for this behavior: the
// legacy Go service dropped addr forwarding entirely (it has no counterpart to
// this function at all — services/legacy/peer_server.go's OnAddr ends at
// addrManager.AddAddresses), so there is no legacy citation to give and none
// is implied.
//
// "Relay to a limited number of other nodes. Use deterministic randomness to
// send to the same nodes for 24 hours at a time so the addrKnowns of the
// chosen nodes prevent repeats." (:1006-1009). The width is 2 for a reachable
// address and 1 otherwise (:1000-1001), and only inbound peers at or above
// CADDR_TIME_VERSION are eligible (:1017) — the version half of that test is
// dropped for the same reason processAddrEntries drops its own version gate:
// MinPeerProtoVersion (31800) already exceeds CADDR_TIME_VERSION (31402).
//
// ONE REDUCTION, named rather than hidden. C++ mixes the inputs with
// CSipHasher, seeded from CConnman's own nSeed0/nSeed1 via
// GetDeterministicRandomizer(RANDOMIZER_ID_ADDRESS_RELAY) (net.cpp:3429). This
// port has no siphash dependency, so it mixes the same inputs — a node-global
// secret, the address hash, the day bucket, and the per-node identity — with
// the double-SHA256 hasher addrman.go already carries for CAddrMan's own
// bucket derivation. Nothing observes the resulting choice off this node, and
// nothing round-trips it, so a different mixing function costs no
// interoperability; what it must preserve is the two properties the C++
// comment names, and it does: the pick is stable for a day, and it is
// unpredictable to an attacker who does not know nodeKey.
//
// nodeKey is that node-global secret (PeerManager.addrRelaySeed). now stands
// in for GetTime().
func selectAddrRelayTargets(candidates []addrRelayCandidate, addr Address, reachable bool, now int64, nodeKey [32]byte) []addrRelayCandidate {
	nRelayNodes := addrRelayNodesUnreachable
	if reachable {
		nRelayNodes = addrRelayNodesReachable
	}

	// net_processing.cpp:1004-1011: hashAddr is CNetAddr::GetHash, the cheap
	// hash of the 16 IP bytes (netaddress.cpp:309-315). The day bucket mixes
	// hashAddr in as well, so different addresses roll over on different
	// hours rather than every node re-picking every peer at midnight.
	var ha addrHasher

	ip := newNetAddr(addr.IP()).ip
	ha.rawBytes(ip[:])

	hashAddr := ha.cheapHash()
	day := (now + int64(hashAddr)) / addrRelayDaySeconds //nolint:gosec // wraparound is as acceptable here as the C++ int64 addition it ports

	type scored struct {
		hashKey   uint64
		candidate addrRelayCandidate
	}

	best := make([]scored, 0, len(candidates))

	for _, c := range candidates {
		if !c.inbound {
			continue
		}

		var h addrHasher

		h.uint256(nodeKey)
		h.uint64(hashAddr << 32)
		h.uint64(uint64(day)) //nolint:gosec // the day index is only ever mixed, never compared as a number
		h.varBytes([]byte(c.addr))

		best = append(best, scored{hashKey: h.cheapHash(), candidate: c})
	}

	// C++ keeps the top nRelayNodes in a two-slot insertion sort over
	// ForEachNode (:1016-1035); an ordinary descending sort over the same keys
	// selects the same peers. Ties break on the peer address so the result is
	// a total order even when two hash keys collide, which the C++ insertion
	// sort leaves to node iteration order.
	sort.Slice(best, func(i, j int) bool {
		if best[i].hashKey != best[j].hashKey {
			return best[i].hashKey > best[j].hashKey
		}

		return best[i].candidate.addr < best[j].candidate.addr
	})

	if len(best) > nRelayNodes {
		best = best[:nRelayNodes]
	}

	if len(best) == 0 {
		return nil
	}

	out := make([]addrRelayCandidate, 0, len(best))
	for _, b := range best {
		out = append(out, b.candidate)
	}

	return out
}

// addrMessageFor builds the wire.MsgAddr for a list of addresses, or nil when
// there is nothing to send. AddAddress only refuses past MaxAddrPerMsg, which
// every caller here has already capped for, so a refusal stops the message
// rather than being ignored.
func addrMessageFor(addrs []Address) *wire.MsgAddr {
	if len(addrs) == 0 {
		return nil
	}

	msg := wire.NewMsgAddr()

	for _, a := range addrs {
		if err := msg.AddAddress(a.NetAddress()); err != nil {
			break
		}
	}

	if len(msg.AddrList) == 0 {
		return nil
	}

	return msg
}

// bestLocalAddress converts a connection's own local address into the
// CAddress the getaddr reply advertises, or a zero Address when there is
// none. A nil NetAddress, or one with no IP, yields the zero Address, whose
// port 0 is exactly the "no worthy address was found" sentinel
// selectGetAddrResponse tests (services/legacy/peer_server.go:1769-1771).
func bestLocalAddress(local *wire.NetAddress, services wire.ServiceFlag, now int64) Address {
	if local == nil || local.IP == nil {
		return Address{}
	}

	return NewAddressAtTime(local.IP, local.Port, services, now)
}

// addrFromNetAddr converts a connection's remote address into the CAddress
// the unsolicited-addr fence compares entries against. A nil or IP-less
// address yields a zero Address, which no entry can match.
func addrFromNetAddr(remote *wire.NetAddress) Address {
	if remote == nil || remote.IP == nil {
		return Address{}
	}

	return NewAddress(remote.IP, remote.Port, remote.Services)
}

// sourceIPOf is the `peerAddr` AddNewAddresses is given as the source of every
// stored entry (net_processing.cpp:2362). AddrMan buckets a new entry by its
// source group, so this is what makes one peer unable to fill the tables.
func sourceIPOf(peerAddr Address) net.IP {
	if peerAddr.IP() == nil {
		return net.IPv4zero
	}

	return peerAddr.IP()
}
