package legacy

import (
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/version"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// defaultFeelerInterval is the fallback mean gap between probes, used when
// legacy_feelerInterval is not positive. Matches svnode's FEELER_INTERVAL
// (net.h:88).
const defaultFeelerInterval = 120 * time.Second

// feelerBudget returns how many peer slots to reserve for feeler probes.
//
// The reservation is what makes a probe paid for rather than borrowed. svnode
// expresses the same idea as arithmetic: its inbound ceiling is
// nMaxConnections - (nMaxOutbound + nMaxFeeler) (net.cpp:1261), so the feeler's
// permit comes out of the inbound share and never out of the outbound target.
// Teranode has one joint ceiling over inbound and automatic outbound rather
// than two separate ones, so the faithful translation is to lower that joint
// ceiling and leave the automatic outbound target completely alone.
//
// Three cases return zero, and each disables the probe loop and the reservation
// together, so the node can never end up paying for a feature that is off:
//
//   - A configured budget of zero or less. This is the single rollback lever.
//   - Connect-only mode. There the node's entire connectivity is the configured
//     list, and MaxPeers has already been resized to the length of that list, so
//     every slot is spoken for. The node also stops discovering peers for
//     itself — newAddressFunc is not installed — so there is nothing for a
//     verified address to feed. Reserving a slot would strand a configured peer
//     for nothing.
//   - A budget that would leave no room for an ordinary peer. Reserving the
//     node's whole capacity for probing is never what an operator meant.
//
// Each of the three names itself in the log before returning zero. startFeeler's
// own "[Feeler] Disabled" reports only that the loop did not start, so without
// these an operator who did not expect feelers to be off cannot tell which case
// they are in, and the settings documentation promises they can. The
// configured-zero test comes first so that the deliberate rollback lever is what
// gets named when it is combined with either of the others.
func feelerBudget(logger ulogger.Logger, configured int, connectOnly bool, maxPeers, targetOutbound int) int {
	if configured <= 0 {
		logger.Infof("[Feeler] Disabled: legacy_maxFeelerPeers is %d, so no probe runs and no peer slot is reserved", configured)
		return 0
	}

	if connectOnly {
		logger.Infof("[Feeler] Disabled: connect-only mode (legacy_connect_peers is set), so the node dials only its configured list and a verified address has nothing to feed")
		return 0
	}

	// The reservation must leave room for the WHOLE automatic outbound tier, not
	// merely for one peer. svnode takes its feeler allowance out of the inbound
	// share and never touches nMaxOutbound, so probing can never cost it a peer
	// it chose to dial. Teranode has one combined ceiling instead of two, so the
	// same guarantee has to be asserted here: if reserving would push the
	// admission ceiling below the outbound target, the node would sit
	// permanently below target, dialling and being refused in a loop, and the
	// operator would see connection churn with no obvious cause.
	//
	// Giving up the probe is the right way to lose that argument. Real peers are
	// what the node is for; the feeler only exists to make finding them easier.
	if maxPeers-configured < targetOutbound {
		logger.Warnf("[Feeler] Disabled: reserving %d of %d peer slots would leave less than the automatic outbound target of %d", configured, maxPeers, targetOutbound)
		return 0
	}

	return configured
}

// setFeelerBudget fixes the slot reservation against the outbound target the
// connection manager will actually chase. connmgr.New substitutes its default
// for a configured zero, so the number judged here has to be the manager's,
// not the caller's.
//
// Must be called after connmgr.New has returned, and before peerHandler starts.
// That ordering is what makes the field safe to read without synchronisation:
// every reader of feelerSlots runs later than this write. Those readers are
// startFeeler and feelerHandler in this file, and handleAddPeerMsg and
// handleQuery in peer_server.go, none of which runs until peerHandler does.
func (s *server) setFeelerBudget(logger ulogger.Logger, configured int, connectOnly bool, maxPeers int) {
	// Unreachable on the real path: newServer assigns s.connManager immediately
	// above its only call. It is logged rather than left silent because it is the
	// one way the ordering contract in the doc comment can be broken, and a
	// silent zero here looks exactly like an operator having switched feelers
	// off. This is the fourth and last way the budget can come out zero, and like
	// the three in feelerBudget it says which one it is.
	if s.connManager == nil {
		logger.Warnf("[Feeler] Disabled: no connection manager, so the outbound target the reservation is judged against is unknown")

		s.feelerSlots = 0

		return
	}

	s.feelerSlots = feelerBudget(logger, configured, connectOnly, maxPeers, int(s.connManager.TargetOutbound()))
}

// peerAdmissionCeiling is how many inbound and automatic outbound peers the
// node will admit: MaxPeers less the slots held back for feeler probes.
//
// Named (addnode) peers are not counted against this, and are not meant to be:
// they have their own budget and are additive, which is what
// CountExcludingPermanent exists to express. Not counted is not the same as not
// gated, though — handleAddPeerMsg applies the comparison to every peer it
// admits, named ones included — so a node whose inbound and automatic tiers are
// already full still turns a named peer away, and the reservation makes that
// bite one peer sooner. connectNodeAdmitted, the runtime addnode door,
// deliberately does not apply the ceiling to a permanent request, so the two
// doors disagree on this point. That predates the feeler and nothing tracks it —
// the TODO beside the check in handleAddPeerMsg asks a different question, what
// to do with a permanent peer once it has been refused.
func peerAdmissionCeiling(maxPeers, feelerSlots int) int {
	ceiling := maxPeers - feelerSlots
	if ceiling < 0 {
		return 0
	}

	return ceiling
}

// feelerAllowed reports whether the automatic outbound tier is at its target,
// which is the only condition under which svnode probes (net.cpp:1865).
//
// The reason for the gate is supply, not politeness. Below target the node is
// short of real peers and the replenishment loop is trying to close that gap; a
// probe launched then competes for exactly the dials the node is missing, and
// on a busy address book it can lose the race for a good address to itself.
//
// The target is read off the connection manager rather than recomputed from
// configuration on purpose. connmgr.New substitutes its own default when the
// caller leaves the target unset, so a recomputed target would be zero, every
// count would clear it, and the node would probe from a cold start with no
// outbound peers at all.
func (s *server) feelerAllowed() bool {
	if s.connManager == nil {
		return false
	}

	return s.connManager.AutomaticOutboundCount() >= int(s.connManager.TargetOutbound())
}

// feelerPollInterval is how often the feeler loop wakes to ask whether it is
// time to probe. svnode's connection thread reaches the same decision on a
// 500ms sleep (net.cpp:1802); a second is the same idea, one wakeup cheaper.
//
// It is also the floor under legacy_feelerInterval: the deadline is only ever
// examined on a tick, so any mean below a second is served at a second, and the
// realised mean of a sub-second setting is higher than the one configured. That
// only matters to a test winding the interval down, and it is written into the
// setting's own documentation.
const feelerPollInterval = time.Second

// feelerCandidateTries bounds one selection pass. It matches newAddressFunc, so
// the probe and the real dial path give up after the same effort, and their
// escalation thresholds below line up with each other.
const feelerCandidateTries = 100

// defaultFeelerHandshakeTimeout is the fallback when
// legacy_feelerHandshakeTimeout is not positive.
//
// Deliberately tighter than svnode's DEFAULT_P2P_HANDSHAKE_TIMEOUT_INTERVAL of
// sixty seconds (net.h:86). A connection we have already decided to hang up on
// the moment it answers has no reason to be given a full minute to answer.
//
// It must also sit inside peer.NegotiateTimeout, for the reason
// feelerHandshakeTimeout gives.
const defaultFeelerHandshakeTimeout = 25 * time.Second

// feelerHandshakeTimeout settles the probe deadline from the configured value.
//
// Extracted from the probe so both guarantees the setting's documentation makes
// are reachable from a test, the same reason feelerBudget is its own function.
//
// Two configured values are unusable, and the guards fire in the order written:
//
//   - Not positive. A timer built from zero or less fires immediately, so every
//     probe would report a timeout before the far side could possibly answer.
//   - At or beyond peer.NegotiateTimeout. The failure this deadline bounds is a
//     host that accepts a TCP connection and then says nothing at all, and the
//     peer package covers that case too: AssociateConnection starts its own
//     negotiation timer within microseconds of the probe building this one, so a
//     deadline equal to or longer than it cannot be relied on to fire first.
//     Whichever fires first owns the teardown, because a disconnect is only
//     reported once. The peer package reports its own timeout at warning level,
//     on the same line the disconnect-rate measurements count, so if it wins, a
//     probe that did its job looks like a lost peer; the probe hangs up quietly
//     at debug instead. The five-second margin the default leaves is what makes
//     the answer never in doubt.
func feelerHandshakeTimeout(logger ulogger.Logger, configured time.Duration) time.Duration {
	timeout := configured
	if timeout <= 0 {
		timeout = defaultFeelerHandshakeTimeout
		logger.Warnf("[Feeler] legacy_feelerHandshakeTimeout must be positive, using %s", timeout)
	}

	if timeout >= peer.NegotiateTimeout {
		timeout = peer.NegotiateTimeout - time.Second
		logger.Warnf("[Feeler] legacy_feelerHandshakeTimeout must be less than the %s peer negotiate timeout, using %s", peer.NegotiateTimeout, timeout)
	}

	return timeout
}

// poissonNext returns an exponentially distributed delay with the given mean.
//
// A fixed period would be a fingerprint. An observer who sees probes at t,
// t+120s, t+240s can recognise the node across address changes and predict the
// next one; it would also synchronise a fleet of nodes started together. A
// memoryless gap leaks neither. svnode randomises its feeler pacing the same
// way, in PoissonNextSend (net.cpp:3326), and for the same reason.
//
// Unbounded above, as svnode's is, but not unguarded: see boundedDuration.
func poissonNext(mean time.Duration) time.Duration {
	return boundedDuration(rand.ExpFloat64()*float64(mean), mean)
}

// boundedDuration turns a nanosecond count into a Duration, falling back when
// the value will not fit an int64.
//
// At the default two-minute mean an exponential draw cannot come close to
// overflowing an int64 of nanoseconds. The mean is operator-settable, though,
// and ExpFloat64 reaches about 745, so a mean beyond roughly a hundred and
// forty days puts the product past MaxInt64 — and converting a float that does
// not fit is undefined in Go. The two architectures teranode ships on do
// genuinely different things with it, both measured rather than assumed:
//
//   - amd64 produces the indefinite value, MinInt64, for an overflow in either
//     direction. A negative gap parks the deadline permanently in the past, so
//     the probe fires on every single tick.
//   - arm64 saturates, so a positive overflow becomes MaxInt64 — a deadline
//     some 292 years out, which stops the feeler for the life of the process.
//
// Neither is a pacing anyone asked for, and they fail in opposite directions,
// so the out-of-range case returns the fallback instead. The comparison is
// written as a negated in-range test so that NaN, which loses every ordinary
// comparison, takes the fallback too.
func boundedDuration(ns float64, fallback time.Duration) time.Duration {
	if !(ns > math.MinInt64 && ns < math.MaxInt64) {
		return fallback
	}

	return time.Duration(ns)
}

// startFeeler launches the probe loop, unless feelers are switched off.
//
// Separated from peerHandler so that everything except the single call line is
// reachable from a test. peerHandler itself cannot be constructed in a unit
// test: it starts the sync manager, which needs a blockchain client, a
// validator, a UTXO store, a subtree store and three validation clients.
func (s *server) startFeeler() {
	if s.feelerSlots <= 0 {
		s.logger.Infof("[Feeler] Disabled")
		return
	}

	// One token per reserved slot. A probe holds a token for its whole life, so
	// the number in flight can never exceed the number of peer slots held back
	// for them, and at the default budget of one this is svnode's single feeler
	// exactly. Created here rather than inside the loop so that the loop and the
	// probes it starts share one channel.
	s.feelerTokens = make(chan struct{}, s.feelerSlots)
	for i := 0; i < s.feelerSlots; i++ {
		s.feelerTokens <- struct{}{}
	}

	// Settled once, here, rather than on each probe. The setting is fixed at
	// startup, so a node configured badly used to repeat feelerHandshakeTimeout's
	// warning for the life of the process, roughly every two minutes at the
	// shipped pacing, and emit two or three lines per probe instead of the one
	// info line the feature is meant to be checkable by. A startup mistake
	// belongs in the startup log.
	//
	// Held on the server rather than passed to feelerProbe so the probe's
	// signature stays as it is: runOneProbe and every probe test drive
	// feelerProbe directly, and a parameter would have those tests supplying the
	// value the production path is supposed to be responsible for.
	//
	// Safe to read without synchronisation for the same reason feelerSlots is:
	// this write is ordered ahead of every probe by the goroutine that starts
	// below, and nothing writes it again.
	s.feelerHandshake = feelerHandshakeTimeout(s.logger, s.settings.Legacy.FeelerHandshakeTimeout)

	s.wg.Add(1)

	go s.feelerHandler()
}

// feelerHandler probes one unverified address at a time, so that the addresses
// the node will later dial for real are addresses something has checked.
//
// The problem it solves: the address book only learns that an address works as
// a side effect of a connection the node wanted anyway. Nothing ever checks an
// address the node is not already using, so the pool of known-reachable
// addresses only ever decays. When a peer is lost — constantly, during a long
// initial block download — the replacement is drawn from that decaying pool and
// the node can spend a long time dialling hosts that stopped answering months
// ago. svnode states the goal in one line at net.cpp:1855: "Increase the number
// of connectable addresses in the tried table."
//
// Three properties of the control flow below are svnode's, not incidental:
//
//   - Below target, the deadline is NOT re-rolled. A node that has been waiting
//     while short of peers fires as soon as the tier refills, rather than
//     starting its wait over. svnode gets the same effect by skipping the
//     whole feeler block while below target, so nNextFeeler is left alone
//     (net.cpp:1865).
//   - The deadline is re-rolled at the decision, not after the probe. A slow
//     probe does not shorten the following gap, and a long stretch below target
//     does not bank up a burst of probes (net.cpp:1869).
//   - There is no pre-dial sleep. svnode adds a random 0-1s before a feeler
//     dial (net.cpp:1934) "to avoid synchronization", in the words of its own
//     comment two lines above — the same anti-lockstep reason this loop draws its
//     gap from a distribution rather than using a fixed period. Getting it from
//     that draw alone is enough, because the gap is random per process and so
//     nothing lines up across a fleet. It is not because our firing is finer
//     grained than svnode's: the deadline is computed at nanosecond granularity
//     but only examined on a feelerPollInterval tick, so firing is quantised to
//     whole seconds, which is coarser than svnode's 500ms loop.
//
// It must be run in a goroutine.
func (s *server) feelerHandler() {
	defer s.wg.Done()

	interval := s.settings.Legacy.FeelerInterval
	if interval <= 0 {
		interval = defaultFeelerInterval
		s.logger.Warnf("[Feeler] legacy_feelerInterval must be positive, using %s (set legacy_maxFeelerPeers to 0 to disable feelers)", interval)
	}

	s.logger.Infof("[Feeler] Starting with %d slot(s), mean interval %s, handshake deadline %s", s.feelerSlots, interval, s.feelerHandshake)

	deadline := time.Now().Add(poissonNext(interval))

	ticker := time.NewTicker(feelerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
		}

		if !s.feelerAllowed() || time.Now().Before(deadline) {
			continue
		}

		deadline = time.Now().Add(poissonNext(interval))

		select {
		case <-s.feelerTokens:
			go s.feelerProbe()
		default:
			// Every slot is already probing. Skip this one; the deadline has
			// already moved on, so the pace is unchanged.
		}
	}
}

// feelerProbe dials one unverified address, waits for it to identify itself,
// records what it learned, and hangs up.
//
// It never builds a serverPeer and never registers with the connection manager.
// (It does read that manager — feelerAllowed and countFailedDial both ask it for
// counts — but it never hands it a connection to keep alive.)
// Membership of state.outboundPeers is the netgroup claim and the peer count,
// so a probe registered as an ordinary peer would take both from a real one.
// The connection manager's job is to keep connections alive: handed a probe,
// it would count it against the outbound target and dial a replacement when
// it hung up.
//
// svnode can afford to route its feeler through its ordinary path because that
// path has no equivalent accounting to corrupt — its feeler permit is added to
// the outbound semaphore rather than counted against a target. Ours is asked to
// claim nothing at all, so it drives a bare peer and calls the address book
// itself. Not, as this comment used to say, because svnode's probes live under a
// second: they use the ordinary handshake and can sit until
// DEFAULT_P2P_HANDSHAKE_TIMEOUT_INTERVAL, which is sixty seconds (net.h:86) —
// longer than ours.
func (s *server) feelerProbe() {
	defer func() { s.feelerTokens <- struct{}{} }()

	na, netAddr := s.feelerCandidate()
	if na == nil {
		// The one path out of a probe with no info line, and the only one that
		// should have none: nothing was drawn, so there is no address to report
		// an outcome against. Every path that names an address goes through
		// logProbe. This used to be two paths, the other being an address that
		// would not resolve, which spent the whole interval saying nothing;
		// feelerCandidate now skips those and keeps looking.
		s.logger.Debugf("[Feeler] No candidate address in this pass")

		return
	}

	addrString := addrmgr.NetAddressKey(na)

	s.feelerAttempted.Add(1)

	conn, err := bsvdDial(netAddr)

	// Everything past the dial wants to write to the address book, and by now
	// the node may be shutting down: peerHandler stops the address manager
	// immediately after its loop exits, so a write that loses that race is
	// silently lost. Give up rather than write — on both arms, because the
	// failure arm records against the book too.
	if s.shuttingDown() {
		if conn != nil {
			_ = conn.Close()
		}

		s.logProbe(addrString, "abandoned, shutting down", "")

		return
	}

	if err != nil {
		// A dial that produced nothing is the only evidence the book ever gets
		// that an address is dead. recordFailedDial is wired into the connection
		// manager's own Dial closure, which a direct dial bypasses, so without
		// this call the probe would only ever teach the book good news.
		s.recordFailedDial(netAddr)
		s.logProbe(addrString, fmt.Sprintf("dial failed: %v", err), "")

		return
	}

	if s.banList.IsBanned(conn.RemoteAddr().String()) {
		s.logProbe(addrString, "resolved to a banned address", "")
		_ = conn.Close()

		return
	}

	// The snapshot feelerCandidate filtered against was taken before the dial,
	// which can have taken the full connect timeout to complete, so by now it is
	// stale. svnode re-checks the same thing at the same point, with FindNode
	// inside OpenNetworkConnection (net.cpp:2118), for exactly this reason.
	//
	// The window is not theoretical. Probing is only allowed at target, so a
	// probe starts; an outbound peer then drops; AutomaticOutboundCount counts
	// established peers only, so replenishment dials immediately and can draw the
	// very address this probe is sitting on, because Good has not run yet. The
	// probe is invisible to the connection manager and cannot be deduped against,
	// so the collision has to be given up from this side. Two connections to one
	// host is a good way to lose the first, and here the first is a real peer.
	//
	// This narrows the window rather than closing it: a dial already in flight is
	// in neither book. Closing it properly needs a reservation shared across two
	// subsystems, which is not worth it for a probe.
	if snap := s.feelerPeerSnapshot(); occupiedByAPeer(snap, na) {
		s.logProbe(addrString, "a peer took the host while we dialled", "")
		_ = conn.Close()

		return
	}

	res := &feelerResult{done: make(chan struct{})}

	p, err := peer.NewOutboundPeer(quietProbeLogger{s.logger}, s.settings, s.feelerPeerConfig(res), addrString)
	if err != nil {
		s.logProbe(addrString, fmt.Sprintf("cannot build a peer: %v", err), "")
		_ = conn.Close()

		return
	}

	p.AssociateConnection(conn)

	// After the TCP connect, matching outboundPeerConnected and svnode, which
	// records the attempt on both arms of ConnectNode.
	s.attemptIfRunning(na)

	gone := make(chan struct{})

	go func() {
		p.WaitForDisconnect()
		close(gone)
	}()

	// Already settled by startFeeler. Resolving it here instead re-ran both of
	// feelerHandshakeTimeout's guards on every probe, so a badly configured node
	// warned about a startup mistake for ever.
	//
	// Guarded anyway, because settling it there leaves the zero value of the
	// field as the one input to this timer that nothing checks, and a timer built
	// from zero is ready the instant the select below is reached. The probe would
	// then report a timeout before the far side could answer, having already
	// written an Attempt against the address at the TCP connect above -- so every
	// probe would discourage the address it was sent to verify. The invariant that
	// makes that unreachable in production is a call-graph fact rather than
	// anything the compiler holds: startFeeler writes the field before starting
	// the only goroutine that calls this. A fallback is cheaper than trusting it.
	//
	// Silent by design. feelerHandshakeTimeout has already said its piece once at
	// startup, and repeating it per probe is exactly what settling the value there
	// removed.
	handshake := s.feelerHandshake
	if handshake <= 0 {
		handshake = defaultFeelerHandshakeTimeout
	}

	timer := time.NewTimer(handshake)
	defer timer.Stop()

	// Each arm records only why its own wait ended, and the verdict is reached
	// once, afterwards, for all of them. That shape is deliberate.
	//
	// A version can land in the same instant the far side hangs up or the deadline
	// expires, and a select picks uniformly among cases that are already ready. An
	// arm that decided its own outcome would therefore throw away a version the
	// probe already had, on a coin toss: the address would go unpromoted, and
	// worse, a non-BSV host that answers and drops at once would go unbanned and
	// be drawn again for as long as it kept doing it — which is the whole loop the
	// ban exists to close.
	//
	// Reaching the verdict in one place downstream of the select means no arm can
	// be written that skips it. The reason attached to res.done is unreachable for
	// that arm, and says what it would mean if it were.
	fallback := "no version received"

	select {
	case <-res.done:

	case <-gone:
		fallback = "hung up before its version"

	case <-timer.C:
		fallback = "timed out"

	case <-s.quit:
		fallback = "abandoned, shutting down"
	}

	outcome := s.judgeVersion(na, addrString, res, fallback)

	// Debug rather than Info, because "Disconnecting (%s) reason:" is the line
	// the disconnect-rate measurements key on, and a probe hanging up on purpose
	// is not a peer the node lost.
	p.DisconnectWithLogFunc("feeler probe complete", s.logger.Debugf)

	s.logProbe(addrString, outcome, res.userAgent())
}

// quietProbeLogger is the logger a probe's own peer gets. It moves that peer's
// warnings and errors down to debug, and leaves everything else alone.
//
// A probe is not a peer the node lost, and its troubles are nobody's business but
// the feeler's. Without this, probing a non-BSV host reliably produced two loud
// lines: the read loop meets a command the BSV parser cannot handle, logs an
// ERROR, and disconnects at WARN with "reason: malformed message" — and that WARN
// is the exact line the disconnect-rate measurements count, so the feeler
// inflated the very number this series exists to drive down, on every probe of
// the fork clients it is designed to seek out and ban.
//
// The log-once guard in DisconnectWithLogFunc cannot help here. It works, but in
// the wrong direction: the read loop's teardown ran first and won, so the guard
// suppressed the feeler's deliberate debug line and let the warning through.
// Observed on the first mainnet soak, against a /Bitcoin Cash Node:29.1.0/ host.
//
// The feeler's own info line still reports the outcome, so nothing worth knowing
// is lost by quietening the peer underneath it.
type quietProbeLogger struct {
	ulogger.Logger
}

func (l quietProbeLogger) Warnf(format string, args ...interface{}) {
	l.Logger.Debugf(format, args...)
}

func (l quietProbeLogger) Errorf(format string, args ...interface{}) {
	l.Logger.Debugf(format, args...)
}

// logProbe reports how one probe ended.
//
// At info, and from every path that has already counted an attempt, because the
// counters in this line are the only place the feeler's work is visible on a
// running node. These arms used to log at debug, which made the claim that "each
// probe logs a single line at info" true only of probes that got as far as a
// handshake — and it hid the most interesting outcome of all. A dial that
// produced nothing is the feature's whole point: it is how a decayed address book
// is discovered. On the first mainnet soak eight attempts produced two info
// lines, so six probes, including every dead address found, left no trace.
//
// One line per probe at the default two-minute mean is about thirty an hour,
// which is nothing next to what the legacy layer already logs per block.
func (s *server) logProbe(addrString, outcome, userAgent string) {
	s.logger.Infof("[Feeler] Probe %s: %s (user agent %q, attempted %d, verified %d)",
		addrString, outcome, userAgent, s.feelerAttempted.Load(), s.feelerVerified.Load())
}

// attemptIfRunning records a dial attempt against na, unless the node has begun
// shutting down. It reports whether the write happened.
//
// Shutdown is re-checked here rather than leaning on the check after the dial:
// the steps between are short but not free. Skipping the write is not a loss —
// peerHandler stops the address manager immediately after its loop exits, so the
// book this would land in has already been saved.
//
// countFailedDial is what stops a spell of broken local networking blaming the
// whole address book.
func (s *server) attemptIfRunning(na *wire.NetAddress) bool {
	if s.shuttingDown() {
		return false
	}

	s.addrManager.Attempt(na, s.countFailedDial())

	return true
}

// judgeVersion decides a probe's outcome once its wait is over, and carries out
// whatever that outcome asks of the address book.
//
// fallback is what to report when no version ever arrived. Callers pass the
// reason their own wait ended and let this function overrule it, because a
// version that landed in the same instant as a hang-up or a timeout is still a
// version: res.done is re-read here rather than trusted to have won its select.
func (s *server) judgeVersion(na *wire.NetAddress, addrString string, res *feelerResult, fallback string) string {
	select {
	case <-res.done:
	default:
		return fallback
	}

	switch {
	case s.shuttingDown():
		return "abandoned, shutting down"

	// Not banned, unlike a non-BSV user agent: this is a BSV node, just one on a
	// protocol the node will not speak, so the address is honestly reachable and
	// only unusable. It must still not be promoted, because the ordinary outbound
	// path refuses it too (peer_server.go, OnVersion) and a probe laxer than the
	// path it feeds would verify addresses the node then declines to use. Good
	// can evict an existing tried entry, so promoting one would cost a usable
	// peer for an unusable address.
	//
	// The check has to be here rather than left to the peer package. That package
	// applies the same rule, but only after it has invoked this OnVersion
	// listener (peer.go:2783 against the callback at :2774), so res.done is
	// already closed by the time it rejects.
	//
	// Ordered before the user-agent test to match the ordinary path exactly.
	// serverPeer.OnVersion returns at its protocol check (peer_server.go:797)
	// before it ever looks at the user agent, so a non-BSV client on a protocol
	// we do not speak is dropped there and never banned. Testing the user agent
	// first here would have the probe ban a host the ordinary path only drops,
	// and a ban is the widest thing this feature does — process-wide, for a day,
	// with sweeps in two layers.
	case res.protocolVersion() < int32(peer.MinAcceptableProtocolVersion):
		return "answered on a protocol we do not accept"

	case !isBSVUserAgent(res.userAgent()):
		s.banNonBSVHost(addrString)

		return "answered but is not a BSV node"

	default:
		// Promotes the address from new to tried, which is the entire point of
		// the exercise. svnode's feeler clears the same bar at the same moment,
		// in ProcessVersionMessage rather than on verack, and so does teranode's
		// own outbound path in OnVersion.
		s.addrManager.Good(na)
		s.feelerVerified.Add(1)

		return "verified"
	}
}

// feelerHostKey is how the probe names a host when asking whether the node is
// already talking to it.
//
// It has to match how peerState.connectedHosts names one, which is the host part
// of a peer's dial string. NetAddressKey builds that same string, so taking its
// host part keeps the two in step. On a split failure the whole address is
// returned, which cannot match any host key, so an unparseable address is
// treated as unheld rather than silently matching everything.
//
// This comment used to claim the pairing also covers OnionCat, where
// NetAddressKey renders the .onion name rather than the raw IP. Nothing on
// either side of the comparison can be an onion address, so that was a
// statement about a case this layer cannot reach. connectedHosts is built from
// serverPeer.Addr, which for an outbound peer is a net.Addr from
// addrStringToNetAddr and for an inbound one is conn.RemoteAddr, and all three
// connection-manager entry points go through addrStringToNetAddr, which refuses
// .onion via bsvdLookup (config.go:794). The probe side cannot either, since
// feelerCandidate now resolves before returning. The function is left as it is
// because it has no onion-specific branch to remove: the rendering happens
// inside NetAddressKey, and matching whatever NetAddressKey produces is the
// property that matters.
func feelerHostKey(na *wire.NetAddress) string {
	key := addrmgr.NetAddressKey(na)

	host, _, err := net.SplitHostPort(key)
	if err != nil {
		return key
	}

	return host
}

// occupiedByAPeer reports whether a snapshot of the node's peers already covers
// na, either at its host or anywhere in its network segment. It is the same pair
// of tests feelerCandidate applies at selection, asked again once the socket is
// up against a snapshot that is no longer stale.
func occupiedByAPeer(snap feelerSnapshot, na *wire.NetAddress) bool {
	if _, held := snap.hosts[feelerHostKey(na)]; held {
		return true
	}

	_, occupied := snap.outboundGroups[addrmgr.GroupKey(na)]

	return occupied
}

// shuttingDown reports whether the server has begun shutting down. Used by the
// probe to stop before it writes to an address book that is about to be saved.
func (s *server) shuttingDown() bool {
	select {
	case <-s.quit:
		return true
	default:
		return false
	}
}

// banNonBSVHost bans a host that answered a probe with a user agent that is not
// a BSV node's, for the same duration and under the same disable lever as the
// ordinary peer path, which reaches the same decision in OnVersion.
//
// Without it the address stays in the new table and nothing stops the next
// selection pass drawing it again, so the node would spend probe slots
// re-establishing a fact it already knows about a fork client it can never
// promote. feelerCandidate consults the ban list, so one ban takes the address
// out of the draw for good.
//
// svnode does the same thing by the same means where an operator has asked for
// it: a user agent matching -banclientua is pushed straight to the ban score
// threshold as "invalid-UA" (net_processing.cpp:1725, under the guard at
// :1722). The only difference is
// that for teranode the BSV-only rule is the protocol requirement rather than an
// operator option.
//
// Written straight to the shared ban list rather than through BanPeer, because
// that path needs a serverPeer and the peer handler and a probe has neither.
// handleAddPeerMsg tests the ban list before it tests its own peerState copy, so
// a ban recorded here still turns the host away at the door.
func (s *server) banNonBSVHost(addrString string) {
	if cfg.DisableBanning {
		return
	}

	// Given up on rather than guessed at, matching handleBanPeerMsg. The ban
	// list keys on a bare host, and IsBanned strips the port before it looks
	// one up, so a key that still carried a port could never be found again.
	host, _, err := net.SplitHostPort(addrString)
	if err != nil {
		s.logger.Debugf("[Feeler] Cannot ban %s, its address will not split: %v", addrString, err)
		return
	}

	s.logger.Infof("[Feeler] Banning %s for %v: not a BSV node", host, cfg.BanDuration)

	if err := s.banList.Add(s.ctx, host, time.Now().Add(cfg.BanDuration)); err != nil {
		s.logger.Errorf("[Feeler] Failed to add ban for %s: %v", host, err)
	}
}

// feelerResult carries what the probe learned out of the peer callback.
type feelerResult struct {
	mtx      sync.Mutex
	ua       string
	protocol int32
	once     sync.Once
	done     chan struct{}
}

func (r *feelerResult) userAgent() string {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	return r.ua
}

// protocolVersion returns the version the remote announced.
func (r *feelerResult) protocolVersion() int32 {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	return r.protocol
}

// feelerPeerConfig is a throwaway peer configuration with exactly one listener.
//
// Nothing here can register the connection anywhere: no server callbacks, no
// sync manager, no association. Multistream is off because a probe has no use
// for a second TCP stream and asking for one would make the remote set one up
// for a peer that is about to vanish.
func (s *server) feelerPeerConfig(res *feelerResult) *peer.Config {
	return &peer.Config{
		Listeners: peer.MessageListeners{
			OnVersion: func(_ *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
				res.mtx.Lock()
				res.ua = msg.UserAgent
				res.protocol = msg.ProtocolVersion
				res.mtx.Unlock()

				res.once.Do(func() { close(res.done) })

				// Never a reject, even for a node we will not promote. A reject
				// is written to the wire and then fails negotiation, which the
				// peer package reports by disconnecting at warning level — and
				// that warning is the same line the disconnect-rate measurements
				// count. We hang up ourselves instead, quietly.
				return nil
			},
		},
		AddrMe:            addrMe,
		HostToNetAddress:  s.addrManager.HostToNetAddress,
		Proxy:             cfg.Proxy,
		UserAgentName:     userAgentName,
		UserAgentVersion:  version.String(),
		UserAgentComments: cfg.UserAgentComments,
		ChainParams:       s.settings.ChainCfgParams,
		Services:          s.services,
		DisableRelayTx:    true,
		ProtocolVersion:   peer.MaxProtocolVersion,
		TrickleInterval:   cfg.TrickleInterval,
	}
}

// feelerCandidate picks an address worth probing and resolves it, or returns
// nil for both if this pass found nothing suitable.
//
// It hands back the resolved net.Addr as well as the address book's own record
// of the address because the probe needs both, and they have to be the same
// address: the net.Addr is what gets dialled, the wire.NetAddress is what the
// book is afterwards told about. Resolving here rather than in the probe is
// what stops an address this layer cannot dial costing a whole probe interval;
// the reasoning, including why the resolve leads the other tests rather than
// following them, is at the resolve step in the loop below.
//
// The escalation thresholds mirror newAddressFunc exactly, so the probe and the
// dial path judge an address the same way. Two deliberate differences from
// svnode:
//
//   - An occupied netgroup skips the candidate rather than abandoning the whole
//     pass. svnode breaks out on the first unlucky draw (net.cpp:1882), which
//     throws away a two-minute slot; newAddressFunc continues, and agreeing with
//     the sibling function in this file matters more than copying the quirk.
//   - No service-flag filter. svnode has one (net.cpp:1902); teranode's dial
//     path does not, and a probe that is stricter than the thing it feeds would
//     verify addresses the node then declines to use.
func (s *server) feelerCandidate() (*wire.NetAddress, net.Addr) {
	snap := s.feelerPeerSnapshot()

	for tries := 0; tries < feelerCandidateTries; tries++ {
		// Drawn from the new table only. This is the point of the whole
		// exercise: a probe exists to move an address into tried, so drawing
		// one that is already there achieves nothing. svnode restricts the same
		// way, with Select(newOnly) at addrman.cpp:337.
		//
		// The manager answers with values read under its own mutex rather than
		// with the KnownAddress itself, because the loop below runs long after
		// the draw and peer goroutines are writing those same fields through
		// Attempt and Good the whole time.
		ka := s.addrManager.UnverifiedAddress()
		if ka == nil {
			return nil, nil
		}

		na := ka.NetAddress

		addrString := addrmgr.NetAddressKey(na)

		// Resolved at selection rather than in the probe, so an address this
		// layer cannot dial costs one try out of feelerCandidateTries rather
		// than the whole probe interval. The probe used to resolve after
		// selection and give up on failure, before it had counted an attempt or
		// written anything, so a bad draw spent the slot in silence and left the
		// entry exactly as it found it: with no Attempt recorded, ka.lastattempt
		// does not move, chance() keeps full weight, and the same entry is
		// eligible for the very next draw.
		//
		// OnionCat is the class that always fails, and it is reachable rather
		// than hypothetical. IsRoutable admits it deliberately, exempting it
		// from the RFC4193 rejection (addrmgr/network.go:230), so gossip puts it
		// in the new table; NetAddressKey renders it as <base32>.onion:port
		// (addrmgr/addrmanager.go:915); and bsvdLookup refuses any .onion host
		// unconditionally, whatever the proxy configuration (config.go:794).
		// There is no onion dial path in this layer to give it: cfg.dial is
		// net.DialTimeout or a SOCKS dialer (config.go:745, :764), bsvdDial is
		// only ever handed an already-resolved net.Addr (config.go:783), and
		// nothing outside a test ever builds the onionAddr type.
		//
		// First of the tests, ahead of the ban check, because it is the only one
		// that establishes the address is an IP at all. BanList.IsBanned falls
		// through to net.ParseIP and logs at error when that fails
		// (internal/banlist/ban_list.go:228), so asking it about an OnionCat
		// address costs an error line, and asking it once per try would cost up
		// to feelerCandidateTries of them. Ordering it first costs an ordinary
		// entry nothing: NetAddressKey renders every non-OnionCat address as an
		// IP literal, which addrStringToNetAddr answers from net.ParseIP without
		// a lookup (peer_server.go:4328).
		netAddr, err := addrStringToNetAddr(addrString)
		if err != nil {
			s.logger.Debugf("[Feeler] Skipping %s, cannot resolve: %v", addrString, err)

			continue
		}

		// Filtered at selection rather than at dial time, unlike svnode, which
		// only notices a ban inside OpenNetworkConnection (net.cpp:2113) and so
		// burns the whole slot on it.
		if s.banList.IsBanned(addrString) {
			continue
		}

		// Never a host the node is already talking to, and never a netgroup an
		// automatic outbound peer occupies. Both tests live in occupiedByAPeer,
		// which the probe asks again once its socket is up; sharing them is what
		// stops the two copies drifting apart.
		//
		// Unlike svnode, which abandons the whole pass on a netgroup collision
		// (net.cpp:1882), this skips the address and keeps looking. A collision is
		// a property of the address, not of the moment, so giving up the slot for
		// two minutes over one would waste the probe.
		if occupiedByAPeer(snap, na) {
			continue
		}

		// Only allow recently attempted nodes after 30 failed tries.
		if tries < 30 && time.Since(ka.LastAttempt) < 10*time.Minute {
			continue
		}

		// Allow nondefault ports after 50 failed tries.
		if tries < 50 && fmt.Sprintf("%d", na.Port) != activeNetParams.DefaultPort {
			continue
		}

		return na, netAddr
	}

	return nil, nil
}
