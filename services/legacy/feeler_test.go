package legacy

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/internal/banlist"
	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/connmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// TestFeelerBudget pins the three ways the reservation is refused.
//
// Each of them matters for a different reason. A configured zero is the single
// rollback lever, and it has to switch off the reservation as well as the
// probing, or an operator who disabled feelers would still be paying an inbound
// slot for them. Connect-only mode has MaxPeers resized to the configured list
// and never dials anything else, so a verified address has nothing to feed and
// reserving there would strand a peer the operator explicitly asked for. And a
// budget that consumes the node's whole capacity is never what anyone meant.
func TestFeelerBudget(t *testing.T) {
	tests := []struct {
		name           string
		configured     int
		connectOnly    bool
		maxPeers       int
		targetOutbound int
		want           int
	}{
		// The shipped shape: legacy_config_MaxPeers = 20 in settings.conf against
		// the manager's default target of 8, so the reserved slot comes out of the
		// inbound share and the outbound tier is untouched.
		{name: "shipped defaults", configured: 1, maxPeers: 20, targetOutbound: 8, want: 1},
		{name: "operator raises the budget", configured: 3, maxPeers: 125, targetOutbound: 8, want: 3},
		{name: "zero is the disable lever", configured: 0, maxPeers: 125, targetOutbound: 8, want: 0},
		{name: "negative is treated as disabled", configured: -1, maxPeers: 125, targetOutbound: 8, want: 0},
		{name: "connect-only reserves nothing", configured: 1, connectOnly: true, maxPeers: 4, targetOutbound: 4, want: 0},
		{name: "never reserve the whole capacity", configured: 1, maxPeers: 1, targetOutbound: 1, want: 0},

		// The reservation must never push the admission ceiling below the
		// automatic outbound target. A node in that state sits permanently below
		// target, dialling and being refused in a loop — connection churn with no
		// obvious cause, and a reserved slot the probe can never use either.
		{name: "a tight cap gives up the probe rather than the tier", configured: 1, maxPeers: 8, targetOutbound: 8, want: 0},
		{name: "one spare slot above the tier is enough", configured: 1, maxPeers: 9, targetOutbound: 8, want: 1},
		{name: "a raised target squeezes the probe out", configured: 2, maxPeers: 10, targetOutbound: 9, want: 0},
		{name: "a raised target with room still probes", configured: 2, maxPeers: 12, targetOutbound: 9, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, feelerBudget(ulogger.TestLogger{}, tt.configured, tt.connectOnly, tt.maxPeers, tt.targetOutbound))
		})
	}
}

// TestFeelerBudgetNamesTheReasonItRefused holds the promise the settings
// documentation makes.
//
// legacy_settings.md and the legacy_maxFeelerPeers longdesc both tell an
// operator they can read the reason feelers are off out of the startup log. Two
// of the three refusals used to return zero without saying anything, so the only
// line such a node produced was startFeeler's bare "[Feeler] Disabled" — which
// reports that the loop did not start and nothing about why. An operator in
// connect-only mode who did not expect feelers to be off had nowhere to look.
//
// Each case is asserted on a token an operator can act on: the setting name for
// the two configuration cases, and the tier being protected for the cap case.
// The last subtest fixes the precedence, so that a node which is both in
// connect-only mode and has the lever pulled reports the lever.
func TestFeelerBudgetNamesTheReasonItRefused(t *testing.T) {
	tests := []struct {
		name           string
		configured     int
		connectOnly    bool
		maxPeers       int
		targetOutbound int
		wantLogged     string
		notLogged      string
	}{
		{
			name: "the disable lever names itself", configured: 0, maxPeers: 125, targetOutbound: 8,
			wantLogged: "legacy_maxFeelerPeers",
		},
		{
			name: "connect-only names the setting that caused it", configured: 1, connectOnly: true, maxPeers: 4, targetOutbound: 4,
			wantLogged: "legacy_connect_peers",
		},
		{
			name: "a tight cap names the tier it protected", configured: 1, maxPeers: 8, targetOutbound: 8,
			wantLogged: "automatic outbound target",
		},
		{
			name: "the lever wins when both apply", configured: 0, connectOnly: true, maxPeers: 4, targetOutbound: 4,
			wantLogged: "legacy_maxFeelerPeers", notLogged: "legacy_connect_peers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingLogger{Logger: ulogger.TestLogger{}}

			require.Equal(t, 0, feelerBudget(rec, tt.configured, tt.connectOnly, tt.maxPeers, tt.targetOutbound),
				"every case here is a refusal; the point of the test is what it said while refusing")

			require.Contains(t, rec.logged(), tt.wantLogged,
				"a refused reservation has to name its reason, or the documented diagnosis is not available")

			if tt.notLogged != "" {
				require.NotContains(t, rec.logged(), tt.notLogged,
					"only one reason should be reported, or an operator cannot tell which lever to move")
			}
		})
	}

	t.Run("a granted reservation says nothing about being disabled", func(t *testing.T) {
		rec := &recordingLogger{Logger: ulogger.TestLogger{}}

		require.Equal(t, 1, feelerBudget(rec, 1, false, 20, 8), "the shipped defaults grant the slot")
		require.NotContains(t, rec.logged(), "Disabled",
			"a node running feelers must not log that they are off")
	})
}

// TestPeerAdmissionCeilingReservesFeelerSlots is the test that proves a probe is
// paid for rather than borrowed.
//
// The arithmetic half is straightforward. The behavioural half drives the real
// door, handleAddPeerMsg, with a node whose cap is three and one slot reserved:
// the third ordinary peer must be turned away, and the same node with no
// reservation must let it in. If the comparand there ever drifts back to the
// raw MaxPeers, the feeler becomes a slot the node quietly overspends.
func TestPeerAdmissionCeilingReservesFeelerSlots(t *testing.T) {
	require.Equal(t, 124, peerAdmissionCeiling(125, 1))
	require.Equal(t, 125, peerAdmissionCeiling(125, 0))
	require.Equal(t, 0, peerAdmissionCeiling(1, 1))
	require.Equal(t, 0, peerAdmissionCeiling(1, 4), "the ceiling never goes negative")

	// cfg is a package-level variable read by handleAddPeerMsg; save and restore
	// it so this test does not leak into the rest of the package.
	origCfg := cfg
	defer func() { cfg = origCfg }()

	cfg = &config{MaxPeers: 3, MaxPeersPerIP: 8}

	for _, tc := range []struct {
		name        string
		feelerSlots int
		admitted    bool
	}{
		{name: "one slot reserved: the third peer is refused", feelerSlots: 1, admitted: false},
		{name: "no reservation: the third peer is admitted", feelerSlots: 0, admitted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &server{
				logger:      ulogger.TestLogger{},
				settings:    settings.NewSettings(),
				banList:     banlist.New(nil, "", ulogger.TestLogger{}),
				feelerSlots: tc.feelerSlots,
			}

			state := newTestPeerState()

			// Seeded directly rather than through handleAddPeerMsg, because a
			// peer that has not handshaked has no peer ID yet and both would
			// land on the same map key. The door itself is what this test is
			// driving, and it is driven once, with the third peer.
			for i, addr := range []string{"8.8.8.8:8333", "1.1.1.1:8333"} {
				sp := newTestOutboundPeer(t, srv, addr)
				state.outboundPeers.Set(int32(i+1), sp)
			}

			require.Equal(t, 2, state.CountExcludingPermanent())

			third := newTestOutboundPeer(t, srv, "9.9.9.9:8333")
			require.Equal(t, tc.admitted, srv.handleAddPeerMsg(state, third))

			if tc.admitted {
				require.Equal(t, 3, state.CountExcludingPermanent())
			} else {
				require.Equal(t, 2, state.CountExcludingPermanent(),
					"the reserved slot must not be handed to an ordinary peer")

				_, tracked := state.outboundPeers.Get(third.ID())
				require.False(t, tracked)
			}
		})
	}
}

// TestFeelerGateRequiresTierAtTarget pins svnode's precondition: probe only once
// the automatic outbound tier is already full (net.cpp:1865). Below target the
// node still needs real peers, and a probe would be competing for exactly the
// dials it is short of.
//
// The zero-target case is the reason feelerAllowed reads the target off the
// connection manager rather than recomputing it. New substitutes its own default
// when the caller leaves the target unset, so a node that recomputed would see a
// target of zero, decide it was at target with no peers at all, and probe from a
// cold start — the precise opposite of the rule.
func TestFeelerGateRequiresTierAtTarget(t *testing.T) {
	t.Run("no connection manager", func(t *testing.T) {
		srv := &server{logger: ulogger.TestLogger{}}
		require.False(t, srv.feelerAllowed())
	})

	t.Run("below target then at target", func(t *testing.T) {
		cmgr, conns := startTestConnManager(t, 2)

		srv := &server{logger: ulogger.TestLogger{}, connManager: cmgr}

		require.False(t, srv.feelerAllowed(), "no automatic outbound peers yet")

		conns(t, 1)
		require.False(t, srv.feelerAllowed(), "one peer short of the target of two")

		conns(t, 1)
		require.True(t, srv.feelerAllowed(), "the tier is at target")
	})

	t.Run("unset target must not read as zero", func(t *testing.T) {
		cmgr, err := connmgr.New(ulogger.TestLogger{}, &connmgr.Config{
			Dial: func(net.Addr) (net.Conn, error) { return nil, errNoTestDial },
		})
		require.NoError(t, err)

		srv := &server{logger: ulogger.TestLogger{}, connManager: cmgr}

		require.Equal(t, uint32(8), cmgr.TargetOutbound(), "New substitutes its own default")
		require.False(t, srv.feelerAllowed(),
			"a node with no outbound peers must never be judged to be at target")
	})
}

var errNoTestDial = net.UnknownNetworkError("test dialer is never expected to run")

// newTestPeerState builds the peer bookkeeping handleAddPeerMsg expects.
func newTestPeerState() *peerState {
	return &peerState{
		inboundPeers:    txmap.NewSyncedMap[int32, *serverPeer](),
		outboundPeers:   txmap.NewSyncedMap[int32, *serverPeer](),
		persistentPeers: txmap.NewSyncedMap[int32, *serverPeer](),
		banned:          txmap.NewSyncedMap[string, time.Time](),
	}
}

// newTestOutboundPeer builds an automatic outbound serverPeer at the given
// address, with no live connection behind it.
func newTestOutboundPeer(t *testing.T, srv *server, addr string) *serverPeer {
	t.Helper()

	p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, settings.NewSettings(), &peer.Config{}, addr)
	require.NoError(t, err)

	return &serverPeer{Peer: p, server: srv}
}

// startTestConnManager returns a running connection manager with the given
// target, plus a helper that establishes n automatic outbound connections
// through the real Connect path and waits for them to register.
func startTestConnManager(t *testing.T, target uint32) (*connmgr.ConnManager, func(*testing.T, int)) {
	t.Helper()

	var (
		mtx     sync.Mutex
		closers []net.Conn
		nextIP  int
	)

	cmgr, err := connmgr.New(ulogger.TestLogger{}, &connmgr.Config{
		TargetOutbound: target,
		Dial: func(net.Addr) (net.Conn, error) {
			ours, theirs := net.Pipe()

			// Dials run on their own goroutines, so the bookkeeping needs a lock.
			mtx.Lock()
			closers = append(closers, ours, theirs)
			mtx.Unlock()

			return ours, nil
		},
	})
	require.NoError(t, err)

	cmgr.Start()

	t.Cleanup(func() {
		cmgr.Stop()
		cmgr.Wait()

		mtx.Lock()
		defer mtx.Unlock()

		for _, c := range closers {
			_ = c.Close()
		}
	})

	establish := func(t *testing.T, n int) {
		t.Helper()

		before := cmgr.AutomaticOutboundCount()

		for i := 0; i < n; i++ {
			nextIP++

			addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(
				net.IPv4(10, 0, 0, byte(nextIP)).String(), "8333"))
			require.NoError(t, err)

			req := &connmgr.ConnReq{}
			req.SetAddr(addr)

			go cmgr.Connect(req)
		}

		require.Eventually(t, func() bool {
			return cmgr.AutomaticOutboundCount() >= before+n
		}, 5*time.Second, 5*time.Millisecond, "connections did not register")
	}

	return cmgr, establish
}

// TestPoissonNextIsExponential pins the shape of the pacing, not just its
// average.
//
// A fixed two-minute period would be a fingerprint: an observer who sees probes
// exactly two minutes apart can recognise the node across address changes and
// predict the next one, and a fleet started together would probe in lockstep.
// svnode randomises for the same reason. A single-sample test would pass
// happily against a constant, so this checks the mean AND the spread.
func TestPoissonNextIsExponential(t *testing.T) {
	const (
		draws = 20000
		mean  = time.Millisecond
	)

	var (
		total   time.Duration
		sawLong bool
		sawTiny bool
	)

	for i := 0; i < draws; i++ {
		d := poissonNext(mean)
		total += d

		require.GreaterOrEqual(t, d, time.Duration(0), "a delay must never be negative")

		if d > 3*mean {
			sawLong = true
		}

		if d < mean/10 {
			sawTiny = true
		}
	}

	observed := float64(total) / draws

	require.InEpsilon(t, float64(mean), observed, 0.05,
		"the sample mean must sit within five percent of the configured mean")
	require.True(t, sawLong, "an exponential draw must sometimes run well over its mean")
	require.True(t, sawTiny, "an exponential draw must sometimes come in well under its mean")
}

// TestFeelerSkipsCandidatesAlreadyHeldOrOccupied covers the two exclusions that
// keep a probe from taking anything away from a real peer.
//
// The earlier sketch on stu/legacy-svnode-align claimed the netgroup filter
// alone guaranteed the node would never open a second connection to a peer it
// already held. That is not true: the netgroup set is derived from the automatic
// outbound list only, so inbound and named peers are invisible to it. Both
// filters are checked here separately, against the same address.
func TestFeelerSkipsCandidatesAlreadyHeldOrOccupied(t *testing.T) {
	swapTestConfig(t, "")

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)

	tests := []struct {
		name string
		snap feelerSnapshot
		want bool
	}{
		{
			name: "host already connected",
			snap: feelerSnapshot{hosts: map[string]struct{}{"8.8.8.8": {}}},
			want: false,
		},
		{
			name: "netgroup already occupied",
			snap: feelerSnapshot{outboundGroups: map[string]struct{}{addrmgr.GroupKey(na): {}}},
			want: false,
		},
		{
			name: "nothing in the way",
			snap: feelerSnapshot{},
			want: true,
		},
	}

	t.Run("a banned address is never a candidate", func(t *testing.T) {
		srv := newFeelerTestServer(t)
		serveFeelerSnapshot(srv, feelerSnapshot{})

		srv.banList = bannedTestBanList(t, "8.8.8.8")
		srv.addrManager.AddAddress(na, testSourceAddr())

		require.Nil(t, candidateOf(srv),
			"a banned address would be dropped the moment it answered, so it is not worth a probe")
	})

	t.Run("an already verified address is never a candidate", func(t *testing.T) {
		srv := newFeelerTestServer(t)
		serveFeelerSnapshot(srv, feelerSnapshot{})

		srv.addrManager.AddAddress(na, testSourceAddr())
		srv.addrManager.Good(na)

		require.Equal(t, 1, srv.addrManager.NumAddresses(),
			"the address is still known, it has just moved into tried")
		require.Nil(t, candidateOf(srv),
			"probing an address that is already verified achieves nothing")
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newFeelerTestServer(t)
			serveFeelerSnapshot(srv, tt.snap)

			srv.addrManager.AddAddress(na, testSourceAddr())

			got := candidateOf(srv)

			if !tt.want {
				require.Nil(t, got, "the candidate should have been skipped")
				return
			}

			require.NotNil(t, got)
			require.Equal(t, "8.8.8.8:8333", addrmgr.NetAddressKey(got))
		})
	}
}

// TestFeelerCandidateSkipsWhatItCannotResolve covers the rule that a candidate
// the node has no way of dialling is skipped at selection instead of being
// handed to the probe.
//
// OnionCat is the concrete case, and it is not hypothetical: IsRoutable admits
// it on purpose (addrmgr/network.go:230), so gossip lands it in the new table,
// and bsvdLookup then refuses the .onion host NetAddressKey renders for it
// (config.go:794) whatever the proxy configuration. Handed to the probe, such
// an address returned before an attempt was recorded, which cost the whole
// probe interval and left the entry untouched and equally likely to be drawn
// next time.
//
// The second subtest is what makes the first mean anything: a pass that gave up
// on the first unresolvable draw would pass "never returns the onion" too.
func TestFeelerCandidateSkipsWhatItCannotResolve(t *testing.T) {
	swapTestConfig(t, "")

	// Inside fd87:d87e:eb43::/48, which is what IsOnionCatTor matches and what
	// ipString turns back into a <base32>.onion name.
	onion := wire.NewNetAddressIPPort(net.ParseIP("fd87:d87e:eb43:1:2:3:4:5"), 8333, wire.SFNodeNetwork)
	dialable := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)

	require.True(t, addrmgr.IsRoutable(onion),
		"the premise of this test is that the address book accepts OnionCat addresses")
	require.Contains(t, addrmgr.NetAddressKey(onion), ".onion",
		"the premise of this test is that such an address is named by its .onion host")

	t.Run("an unresolvable address is the only thing in the book", func(t *testing.T) {
		srv := newFeelerTestServer(t)
		serveFeelerSnapshot(srv, feelerSnapshot{})

		srv.addrManager.AddAddress(onion, testSourceAddr())

		require.NotNil(t, srv.addrManager.UnverifiedAddress(),
			"the address book holds it, which is the whole problem")
		require.Nil(t, candidateOf(srv),
			"an address this layer cannot dial must never reach the probe")
	})

	t.Run("the pass keeps looking past one", func(t *testing.T) {
		srv := newFeelerTestServer(t)
		serveFeelerSnapshot(srv, feelerSnapshot{})

		srv.addrManager.AddAddress(onion, testSourceAddr())
		srv.addrManager.AddAddress(dialable, testSourceAddr())

		// Repeated because the draw is random and weighted: one pass that
		// happened to pick the dialable address first would prove nothing. Fifty
		// passes without a single onion is the assertion.
		for i := 0; i < 50; i++ {
			na, netAddr := srv.feelerCandidate()

			require.NotNil(t, na, "the dialable address is still there to be found")
			require.Equal(t, "8.8.8.8:8333", addrmgr.NetAddressKey(na))
			require.NotNil(t, netAddr, "a candidate is returned already resolved")
			require.Equal(t, "8.8.8.8:8333", netAddr.String(),
				"the resolved address and the book's record must be the same host")
		}
	})

	// The resolve sits ahead of the ban check for this reason, so the ordering
	// is worth a test of its own. BanList.IsBanned falls through to
	// net.ParseIP and logs at error when that fails, so an OnionCat address
	// reaching it costs an error line; with the pass now continuing rather than
	// giving up, it would cost one per try.
	t.Run("an unresolvable address never reaches the ban list", func(t *testing.T) {
		rec := &recordingLogger{Logger: ulogger.TestLogger{}}

		srv := newFeelerTestServer(t)
		srv.banList = emptyWritableBanList(t, rec)
		serveFeelerSnapshot(srv, feelerSnapshot{})

		srv.addrManager.AddAddress(onion, testSourceAddr())

		require.Nil(t, candidateOf(srv))

		_, errs := rec.counts()
		require.Zero(t, errs, "an address the node cannot dial must not be quizzed about bans: %s", rec.logged())
	})
}

// TestFeelerCandidateDoesNotRaceTheAddressBook drives a selection pass against
// the same address-book writes a running node makes, and is meant to be read as
// a -race test: without the detector it asserts nothing beyond "does not panic".
//
// The feeler is a new long-lived goroutine that reads address-book entries up to
// feelerCandidateTries times a pass, while peer read loops and the connection
// manager write those same entries through Attempt and Good. KnownAddress says
// of itself that no accessor on it is safe for concurrent access, so the entry
// has to be answered on the manager's side of its own mutex rather than handed
// out to be read afterwards.
//
// t.Parallel is deliberately absent: the concurrency under test is inside the
// test, not between it and its siblings.
func TestFeelerCandidateDoesNotRaceTheAddressBook(t *testing.T) {
	swapTestConfig(t, "")

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})

	// Enough entries that selection keeps drawing rather than settling on one,
	// and all in different /16s so the netgroup filter does not thin them.
	addrs := make([]*wire.NetAddress, 0, 32)

	for i := 0; i < 32; i++ {
		na := wire.NewNetAddressIPPort(net.ParseIP(fmt.Sprintf("8.%d.8.1", i)), 8333, wire.SFNodeNetwork)
		srv.addrManager.AddAddress(na, testSourceAddr())
		addrs = append(addrs, na)
	}

	var wg sync.WaitGroup

	writing := make(chan struct{})

	// The writer stands in for the peer read loops and the connection manager:
	// Attempt writes ka.lastattempt, Good writes it and ka.attempts, both under
	// the manager's mutex.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-writing:
				return
			default:
			}

			for _, na := range addrs {
				srv.addrManager.Attempt(na, true)
				srv.addrManager.Good(na)
			}
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(writing)

		for i := 0; i < 200; i++ {
			srv.feelerCandidate()
		}
	}()

	wg.Wait()
}

// TestStartFeelerHonoursTheDisableLever pins the second half of the rollback
// lever. Setting the budget to zero has to stop the goroutine from starting as
// well as stop the slot being reserved; a version that reserved nothing but
// still ran the loop would be a disabled feature that still probes.
//
// Observed through the server's wait group, which is what startFeeler adds to.
// A count of zero cannot be observed through the probe rate, because a token
// channel of capacity zero would hand out no probes either way.
func TestStartFeelerHonoursTheDisableLever(t *testing.T) {
	t.Run("disabled: nothing is started", func(t *testing.T) {
		srv := newFeelerTestServer(t)
		srv.feelerSlots = 0

		srv.startFeeler()

		require.True(t, waitGroupSettles(&srv.wg, 5*time.Second),
			"a disabled feeler must not leave a goroutine running")
	})

	t.Run("enabled: the loop is running", func(t *testing.T) {
		srv := newFeelerTestServer(t)
		serveFeelerSnapshot(srv, feelerSnapshot{})

		srv.startFeeler()

		require.False(t, waitGroupSettles(&srv.wg, time.Second),
			"an enabled feeler must leave its loop running until shutdown")
	})
}

// waitGroupSettles reports whether the wait group drained within the timeout.
func waitGroupSettles(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// TestFeelerWaitsForTheOutboundTier pins the gate where it is actually applied,
// in the probe loop, rather than only in the predicate it calls.
//
// svnode probes only once its outbound connections are all up (net.cpp:1865),
// and the reason is supply: below target the node is short of real peers and
// the replenishment loop is trying to close that gap, so a probe launched then
// is competing for exactly the dials the node is missing.
//
// The second half matters as much as the first. svnode does not restart its
// wait when it finds itself below target, so a node that has been held back
// fires as soon as the tier fills. Asserting only that nothing happens below
// target would be satisfied by a feeler that never ran at all.
func TestFeelerWaitsForTheOutboundTier(t *testing.T) {
	ln, served := startFeelerTestListener(t, "/Bitcoin SV:1.1.0/")

	swapTestConfig(t, ln.Addr().String())

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})

	cmgr, establish := startTestConnManager(t, 2)
	srv.connManager = cmgr

	establish(t, 1)
	require.False(t, srv.feelerAllowed(), "one peer short of the target of two")

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	startFeelerLoop(t, srv)

	require.Never(t, func() bool {
		return srv.feelerAttempted.Load() > 0
	}, 2*time.Second, 25*time.Millisecond,
		"a node below its outbound target must not spend dials on probing")

	establish(t, 1)
	require.True(t, srv.feelerAllowed())

	select {
	case <-served:
	case <-time.After(20 * time.Second):
		t.Fatal("the probe did not start once the outbound tier reached target")
	}
}

// TestFeelerRecordsAFailedDial covers the half of the story that is easy to
// forget: the probe has to teach the book bad news as well as good.
//
// recordFailedDial is wired into the connection manager's own dial closure, and
// a probe dials directly, so it bypasses that wiring entirely. Without the
// explicit call the feeler would only ever mark addresses good, and a host that
// had stopped answering would keep its full selection weight for ever -- which
// is the exact bug PR 1601 fixed for the main dial path.
func TestFeelerRecordsAFailedDial(t *testing.T) {
	// A dial that always fails, standing in for a host that has gone away.
	swapTestConfig(t, "")

	cfg.dial = func(string, string, time.Duration) (net.Conn, error) {
		return nil, errDeadHost
	}

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})
	atLeastTwoAutomaticPeers(t, srv)

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	require.True(t, srv.addrManager.UnverifiedAddress().LastAttempt.IsZero(),
		"the address starts out with no attempt against it")

	// One probe, run inline, so the assertions below are made after the probe
	// has finished writing rather than while it still might.
	runOneProbe(srv)

	ka := srv.addrManager.UnverifiedAddress()
	require.NotNil(t, ka, "a failed dial must not move the address anywhere")
	require.False(t, ka.LastAttempt.IsZero(),
		"a dial that produced nothing must be recorded against the address")
	require.Positive(t, ka.Attempts,
		"with the node connected elsewhere, the failure counts against the address")
	require.Equal(t, uint64(1), srv.feelerAttempted.Load())
	require.Equal(t, uint64(0), srv.feelerVerified.Load())
}

var errDeadHost = net.UnknownNetworkError("the host under test is not answering")

// TestFeelerSnapshotQueryReportsPeers drives the peer handler's answer to the
// probe's one question, which is the half of the exchange the end-to-end tests
// stub out.
//
// It also pins the difference between the two halves of the snapshot. The
// netgroup set covers automatic outbound peers only, because that is the tier
// whose diversity the node is protecting. The host set covers every tier,
// because a second connection to a host is a problem whichever tier the first
// one is in — and the earlier sketch got exactly this wrong, claiming the
// netgroup check made the host check unnecessary.
func TestFeelerSnapshotQueryReportsPeers(t *testing.T) {
	srv := &server{logger: ulogger.TestLogger{}, settings: settings.NewSettings()}
	state := newTestPeerState()

	outbound := newTestOutboundPeer(t, srv, "8.8.8.8:8333")
	inbound := newTestOutboundPeer(t, srv, "1.1.1.1:8333")
	named := newTestOutboundPeer(t, srv, "9.9.9.9:8333")

	state.outboundPeers.Set(1, outbound)
	state.inboundPeers.Set(2, inbound)
	state.persistentPeers.Set(3, named)

	reply := make(chan feelerSnapshot, 1)
	srv.handleQuery(state, getFeelerSnapshotMsg{reply: reply})

	snap := <-reply

	require.Contains(t, snap.hosts, "8.8.8.8")
	require.Contains(t, snap.hosts, "1.1.1.1", "an inbound peer still occupies its host")
	require.Contains(t, snap.hosts, "9.9.9.9", "a named peer still occupies its host")
	require.Len(t, snap.hosts, 3)

	require.Contains(t, snap.outboundGroups, addrmgr.GroupKey(outbound.NA()))
	require.NotContains(t, snap.outboundGroups, addrmgr.GroupKey(inbound.NA()),
		"only the automatic outbound tier claims a netgroup")
	require.NotContains(t, snap.outboundGroups, addrmgr.GroupKey(named.NA()))
}

// TestFeelerPromotesAnAddressEndToEnd drives the whole probe: the real dial
// path, a real handshake against a real listener, and the address book.
//
// Promotion is observed through exported API alone. Moving an address from new
// to tried leaves the total unchanged while emptying the new table, so
// "UnverifiedAddress returns nothing while NumAddresses is still one" is exactly
// the promotion, with no test-only accessor needed.
//
// The book entry has to be a routable address, because the address manager
// refuses to store loopback, while the socket has to be loopback because that is
// where the test listener is. cfg.dial bridges the two: it is a real production
// field, set by loadConfig, so redirecting it exercises the production dial path
// without adding a seam to production code.
func TestFeelerPromotesAnAddressEndToEnd(t *testing.T) {
	ln, served := startFeelerTestListener(t, "/Bitcoin SV:1.1.0/")

	swapTestConfig(t, ln.Addr().String())

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})
	atFeelerTarget(t, srv)

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	require.Equal(t, 1, srv.addrManager.NumAddresses())
	require.NotNil(t, srv.addrManager.UnverifiedAddress(), "the address starts out unverified")

	startFeelerLoop(t, srv)

	select {
	case <-served:
	case <-time.After(20 * time.Second):
		t.Fatal("the probe never reached the listener")
	}

	require.Eventually(t, func() bool {
		return srv.addrManager.UnverifiedAddress() == nil
	}, 20*time.Second, 10*time.Millisecond,
		"a verified address must leave the new table")

	require.Equal(t, 1, srv.addrManager.NumAddresses(),
		"promotion moves the address between tables, it does not add or drop one")
	require.Equal(t, uint64(1), srv.feelerVerified.Load())
}

// TestFeelerDoesNotPromoteNonBSVPeer is the counterpart, and it guards against
// the sketch's worst defect: it installed no version listener at all and marked
// every address that completed a handshake as good. A BTC or BCH node that
// answered would have been promoted, and because promotion can evict an existing
// tried entry, that does not merely waste a probe -- it pushes out a real BSV
// peer.
func TestFeelerDoesNotPromoteNonBSVPeer(t *testing.T) {
	for _, tt := range []struct {
		name           string
		disableBanning bool
		wantBanned     bool
	}{
		{name: "bans by default so it is not probed again", wantBanned: true},
		{name: "disable banning still rejects without a ban", disableBanning: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ln, served := startFeelerTestListener(t, "/Satoshi:0.21.0/")

			swapTestConfig(t, ln.Addr().String())
			cfg.DisableBanning = tt.disableBanning

			srv := newFeelerTestServer(t)
			serveFeelerSnapshot(srv, feelerSnapshot{})

			// Two established automatic peers, so countFailedDial has the evidence it
			// needs to hold an address responsible. Below that threshold an attempt is
			// recorded but never counted, and the attempt tally would stay at zero for
			// reasons that have nothing to do with this probe.
			atLeastTwoAutomaticPeers(t, srv)

			na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
			srv.addrManager.AddAddress(na, testSourceAddr())

			// One probe, run inline, so the assertions below see a settled state rather
			// than racing the loop. runOneProbe returns when the probe has finished
			// deciding, which is what makes the promotion check meaningful: polling for
			// the attempt instead would look at the address before the user agent had
			// even been read, and would pass whatever the code did.
			runOneProbe(srv)

			select {
			case <-served:
			case <-time.After(5 * time.Second):
				t.Fatal("the probe never reached the listener")
			}

			require.Equal(t, uint64(1), srv.feelerAttempted.Load())
			require.Equal(t, uint64(0), srv.feelerVerified.Load(),
				"a node that is not a BSV node must never be promoted")

			ka := srv.addrManager.UnverifiedAddress()
			require.NotNil(t, ka, "the address must stay in the new table")
			require.Positive(t, ka.Attempts, "the attempt is still recorded against it")
			require.Equal(t, tt.wantBanned, srv.banList.IsBanned("8.8.8.8"))

			if tt.wantBanned {
				require.Nil(t, candidateOf(srv),
					"a banned non-BSV address must not be drawn again")
			}
		})
	}
}

// testSourceAddr is the "who told us about this address" address. It only has
// to be routable and distinct from the address under test.
func testSourceAddr() *wire.NetAddress {
	return wire.NewNetAddressIPPort(net.ParseIP("173.194.115.1"), 8333, wire.SFNodeNetwork)
}

// emptyWritableBanList returns a ban list backed by an in-memory database,
// which Add needs because it writes through to storage.
//
// The optional logger is for the one test that asserts on what the ban list
// itself logged; everything else takes the default.
func emptyWritableBanList(t *testing.T, logger ...ulogger.Logger) *p2p.BanList {
	t.Helper()

	banLogger := ulogger.Logger(ulogger.TestLogger{})
	if len(logger) > 0 {
		banLogger = logger[0]
	}

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, settings.NewSettings())
	require.NoError(t, err)

	bl := banlist.New(store.GetDB(), util.SqliteMemory, banLogger)
	require.NoError(t, bl.Init(t.Context()))

	t.Cleanup(bl.Stop)

	return bl
}

// bannedTestBanList returns a ban list already holding the given address.
func bannedTestBanList(t *testing.T, ip string) *p2p.BanList {
	t.Helper()

	bl := emptyWritableBanList(t)
	require.NoError(t, bl.Add(t.Context(), ip, time.Now().Add(time.Hour)))

	return bl
}

// swapTestConfig replaces the package-level legacy config for the duration of a
// test. When redirectTo is set, every dial goes there instead of the address
// asked for, which is what lets a probe of a routable book entry land on a
// loopback listener.
//
// cfg.dial is a real production field, set by loadConfig, so this exercises the
// production dial path rather than a test-only injection point.
func swapTestConfig(t *testing.T, redirectTo string) {
	t.Helper()

	orig := cfg

	// MaxPeers matches what ships rather than bsvd's compiled-in 125:
	// settings.conf sets legacy_config_MaxPeers = 20 and the reflection loader in
	// config.go applies it to this field on every real run.
	c := &config{
		MaxPeers:        20,
		MaxPeersPerIP:   5,
		TrickleInterval: 10 * time.Second,
		BanDuration:     24 * time.Hour,
	}

	c.dial = func(network, addr string, timeout time.Duration) (net.Conn, error) {
		if redirectTo != "" {
			addr = redirectTo
		}

		return net.DialTimeout(network, addr, timeout)
	}

	cfg = c

	// Registered as a cleanup rather than returned, so it runs LAST. Cleanups
	// run in reverse order of registration and after every deferred call, and
	// the test server is built after this, so its shutdown -- which waits for
	// probes still reading cfg -- is guaranteed to happen first.
	t.Cleanup(func() { cfg = orig })
}

// newFeelerTestServer builds the smallest server the probe path needs.
func newFeelerTestServer(t *testing.T) *server {
	t.Helper()

	tSettings := settings.NewSettings()
	tSettings.Legacy.FeelerInterval = time.Millisecond

	srv := &server{
		ctx:         t.Context(),
		logger:      ulogger.TestLogger{},
		settings:    tSettings,
		addrManager: addrmgr.New(ulogger.TestLogger{}, t.TempDir(), nil),
		// A writable ban list rather than banlist.New(nil, ...). A nil database
		// makes Add dereference nil and panic rather than error, so the cheaper
		// construction turned any test whose probe met a non-BSV user agent into
		// a panicking binary instead of a failing assertion — one edited string
		// literal away, for whoever extends these tests next.
		banList:     emptyWritableBanList(t),
		quit:        make(chan struct{}),
		query:       make(chan interface{}),
		feelerSlots: 1,
		services:    wire.SFNodeNetwork,
	}

	t.Cleanup(func() {
		beginFeelerShutdown(srv)
		srv.wg.Wait()
		drainFeelerProbes(t, srv)
	})

	return srv
}

// beginFeelerShutdown puts the node into shutting-down state, and is safe to
// call twice because the cleanup calls it too.
//
// The check-then-close is not safe against concurrent closers in general. It is
// safe here because the only two callers are a test body and its own cleanup,
// which run in sequence on the same goroutine.
func beginFeelerShutdown(srv *server) {
	select {
	case <-srv.quit:
		return
	default:
	}

	close(srv.quit)
}

// drainFeelerProbes waits until no probe is in flight, by taking every slot
// token. A probe holds its token for its whole life, so holding them all means
// nothing is still running.
//
// The probe loop must already have stopped, or it will keep taking tokens back.
// This matters because probe goroutines are deliberately not tracked by the
// server's wait group, so waiting on that alone can return while a probe is
// still reading package state the test is about to put back.
func drainFeelerProbes(t *testing.T, srv *server) {
	t.Helper()

	for i := 0; i < cap(srv.feelerTokens); i++ {
		select {
		case <-srv.feelerTokens:
		case <-time.After(45 * time.Second):
			t.Error("a feeler probe never finished")
			return
		}
	}
}

// serveFeelerSnapshot answers the probe's peer-set query with a fixed snapshot,
// standing in for the peer handler.
func serveFeelerSnapshot(srv *server, snap feelerSnapshot) {
	go func() {
		for {
			select {
			case <-srv.quit:
				return
			case q := <-srv.query:
				if msg, ok := q.(getFeelerSnapshotMsg); ok {
					msg.reply <- snap
				}
			}
		}
	}()
}

// atFeelerTarget gives the server a connection manager that is at its outbound
// target, which is the condition feelerAllowed requires.
func atFeelerTarget(t *testing.T, srv *server) {
	t.Helper()

	cmgr, establish := startTestConnManager(t, 1)
	establish(t, 1)

	srv.connManager = cmgr

	require.True(t, srv.feelerAllowed())
}

// atLeastTwoAutomaticPeers puts the node at target with two established
// automatic outbound peers, which is also the threshold countFailedDial needs
// before it will hold an address responsible for a failure.
func atLeastTwoAutomaticPeers(t *testing.T, srv *server) {
	t.Helper()

	cmgr, establish := startTestConnManager(t, 2)
	establish(t, 2)

	srv.connManager = cmgr

	require.True(t, srv.feelerAllowed())
	require.True(t, srv.countFailedDial())
}

// candidateOf runs one selection pass and returns only the address book's
// record of what it picked, for tests that have nothing to say about the
// resolved net.Addr alongside it.
func candidateOf(srv *server) *wire.NetAddress {
	na, _ := srv.feelerCandidate()

	return na
}

// runOneProbe runs exactly one probe on the calling goroutine and returns when
// it has finished.
//
// Used where a test needs to read the address book afterwards. Reading it is
// safe at any time, since UnverifiedAddress answers under the manager's mutex,
// but it is only meaningful once the probe has finished: an assertion made
// alongside a probe in flight would be asserting on a moment rather than on an
// outcome. The pacing loop around this is covered separately.
func runOneProbe(srv *server) {
	srv.feelerTokens = make(chan struct{}, 1)

	// feelerHandshake is left exactly as the caller set it, including unset.
	// This helper used to fill in a zero so a probe would not build a timer that
	// fires immediately; feelerProbe now falls back on its own, so backfilling
	// here would hide the very path that guard exists for.
	srv.feelerProbe()
}

// startFeelerLoop starts the feeler exactly as peerHandler does.
func startFeelerLoop(t *testing.T, srv *server) {
	t.Helper()

	srv.startFeeler()

	require.NotNil(t, srv.feelerTokens, "the feeler must have started")
}

// startFeelerTestListener accepts one connection and answers it the way a
// remote node would: it reads the probe's version message and replies with its
// own, carrying the given user agent, then a verack.
//
// Deliberately a raw wire responder rather than a peer.Peer. Two peers built in
// this process share the sent-nonce cache the protocol uses to spot a node that
// has dialled itself, so an in-process peer would recognise the probe's own
// nonce and hang up on it as a self-connection.
func startFeelerTestListener(t *testing.T, userAgent string) (net.Listener, <-chan struct{}) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	net2 := settings.NewSettings().ChainCfgParams.Net
	served := make(chan struct{})

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}

		defer func() { _ = conn.Close() }()

		msg, _, err := wire.ReadMessage(conn, wire.ProtocolVersion, net2)
		if err != nil {
			return
		}

		if _, ok := msg.(*wire.MsgVersion); !ok {
			return
		}

		me := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 8333, wire.SFNodeNetwork)
		you := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 8333, wire.SFNodeNetwork)

		reply := wire.NewMsgVersion(me, you, rand.Uint64(), 0)
		reply.UserAgent = userAgent
		reply.Services = wire.SFNodeNetwork

		if err := wire.WriteMessage(conn, reply, wire.ProtocolVersion, net2); err != nil {
			return
		}

		if err := wire.WriteMessage(conn, wire.NewMsgVerAck(), wire.ProtocolVersion, net2); err != nil {
			return
		}

		close(served)

		// Hold the connection open so the probe is the side that hangs up.
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	return ln, served
}

// TestFeelerTokensCapProbesInFlight is the test for the whole point of the
// reservation.
//
// The issue asks for slots "reserved from the peer cap rather than taken out of
// the automatic tier". Reserving them is only half of it — the probes actually
// have to respect the reservation. If more probes can be in flight than slots
// were held back, the node is quietly using peer capacity it never reserved, and
// the accounting PR 1601 fixed is wrong again by a different route.
//
// The token channel is that cap. This drains it and shows a probe cannot start,
// then returns a token and shows one can.
//
// Note what this does NOT cover: it exercises the channel directly, not
// feelerHandler, so deleting feelerHandler's acquisition and letting every tick
// start a probe would leave this test green. That is covered separately, by
// TestFeelerHandlerWaitsForASlotToken, which drives the real loop.
func TestFeelerTokensCapProbesInFlight(t *testing.T) {
	srv := &server{
		logger:       ulogger.TestLogger{},
		feelerSlots:  2,
		feelerTokens: make(chan struct{}, 2),
	}
	for i := 0; i < srv.feelerSlots; i++ {
		srv.feelerTokens <- struct{}{}
	}

	// Two probes may run at once, because two slots were reserved.
	acquired := 0

	for i := 0; i < srv.feelerSlots; i++ {
		select {
		case <-srv.feelerTokens:
			acquired++
		default:
		}
	}

	require.Equal(t, 2, acquired, "both reserved slots must be available to probes")

	// A third must not, however long the loop has been waiting to fire. This is
	// the non-blocking default branch in feelerHandler: skip, do not queue.
	select {
	case <-srv.feelerTokens:
		require.Fail(t, "a third probe started with only two slots reserved")
	default:
	}

	// A finished probe returns its token and the next one may start.
	srv.feelerTokens <- struct{}{}

	select {
	case <-srv.feelerTokens:
	default:
		require.Fail(t, "a returned token must let the next probe start")
	}
}

// TestSetFeelerBudgetUsesTheManagersEffectiveTarget pins a guard that used to
// judge itself against the wrong number.
//
// feelerBudget refuses a reservation that would leave the automatic outbound
// tier unable to reach its target. It used to be handed the target computed
// from configuration, before connmgr.New had a say -- and New substitutes its
// own default of eight for a configured zero. The guard therefore compared
// against zero in exactly the case it was written for: no reservation can look
// like it starves a tier that is aiming for nothing. With MaxPeers at eight the
// node reserved a slot anyway, dropped its admission ceiling to seven, and was
// left dialling for an eighth peer its own door would refuse, indefinitely.
//
// Reading the target off the manager is what its own accessor documentation
// asks callers to do, and what feelerAllowed already did.
func TestSetFeelerBudgetUsesTheManagersEffectiveTarget(t *testing.T) {
	cmgr, err := connmgr.New(ulogger.TestLogger{}, &connmgr.Config{
		Dial: func(net.Addr) (net.Conn, error) { return nil, errNoTestDial },
	})
	require.NoError(t, err)
	require.Equal(t, uint32(8), cmgr.TargetOutbound(),
		"New substitutes its default for an unset target, which is the whole trap")

	srv := &server{logger: ulogger.TestLogger{}, connManager: cmgr}

	srv.setFeelerBudget(ulogger.TestLogger{}, 1, false, 8)
	require.Equal(t, 0, srv.feelerSlots,
		"reserving one of eight leaves seven, below the eight the manager will chase")

	srv.setFeelerBudget(ulogger.TestLogger{}, 1, false, 1)
	require.Equal(t, 0, srv.feelerSlots,
		"a reservation that consumes the node's whole capacity is refused")

	// 20 is what actually ships: settings.conf sets legacy_config_MaxPeers = 20,
	// which the reflection loader in config.go puts on this very field. The 125 in
	// config.go is only bsvd's compiled-in fallback, and is overridden on every
	// real run. So the shipped shape is a cap of 20 against a target of 8: the
	// reserved slot comes wholly out of the inbound share, taking it from 12 to
	// 11, and the outbound tier is untouched.
	srv.setFeelerBudget(ulogger.TestLogger{}, 1, false, 20)
	require.Equal(t, 1, srv.feelerSlots,
		"the shipped defaults leave the outbound tier untouched, so the slot is granted")
}

// TestSetFeelerBudgetNamesAMissingConnectionManager covers the fourth and last
// way the budget can come out zero.
//
// newServer assigns connManager immediately above the only call, so this is
// unreachable in production. It is worth a line anyway: it is the one way the
// ordering contract in setFeelerBudget's doc comment can be broken, and a
// silent zero there is indistinguishable from an operator having pulled the
// legacy_maxFeelerPeers lever. Whoever reorders those two statements should be
// told what they cost, not left reading an address book that stops being
// verified.
func TestSetFeelerBudgetNamesAMissingConnectionManager(t *testing.T) {
	rec := &recordingLogger{Logger: ulogger.TestLogger{}}
	srv := &server{logger: ulogger.TestLogger{}}

	srv.setFeelerBudget(rec, 1, false, 20)

	require.Equal(t, 0, srv.feelerSlots,
		"without a manager there is no target to judge the reservation against, so nothing is reserved")
	require.Contains(t, rec.logged(), "no connection manager",
		"the last silent path has to name itself too, or a broken call order looks like a config choice")
}

// TestFeelerHandlerWaitsForASlotToken pins the enforcement site itself.
//
// The reservation is only worth anything if the loop actually respects it. The
// earlier test for this drove a channel it built by hand rather than the loop,
// so deleting the acquisition in feelerHandler and letting every tick start a
// probe left the whole package green -- the cap was the one part of "paid for
// rather than borrowed" that nothing checked.
//
// This starts the real loop with its only slot already spoken for and shows no
// probe runs, then returns the token and shows one does. The second half is
// what makes the first half mean anything: without it, a loop that had simply
// died would pass.
func TestFeelerHandlerWaitsForASlotToken(t *testing.T) {
	swapTestConfig(t, "")

	cfg.dial = func(string, string, time.Duration) (net.Conn, error) {
		return nil, errDeadHost
	}

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})
	atLeastTwoAutomaticPeers(t, srv)

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	// Built here rather than by startFeeler, and left empty. Filling it and
	// then draining it would leave a window in which the loop could take the
	// token before the test removed it.
	srv.feelerTokens = make(chan struct{}, 1)

	srv.wg.Add(1)

	go srv.feelerHandler()

	require.Never(t, func() bool {
		return srv.feelerAttempted.Load() > 0
	}, 3*time.Second, 25*time.Millisecond,
		"with its only slot taken the loop must skip its turn rather than probe anyway")

	srv.feelerTokens <- struct{}{}

	require.Eventually(t, func() bool {
		return srv.feelerAttempted.Load() > 0
	}, 20*time.Second, 25*time.Millisecond,
		"a returned token must let the loop probe, or the silence above proved nothing")
}

// TestBoundedDurationRefusesAConversionThatDoesNotFit pins the guard under
// poissonNext, deterministically and on either architecture.
//
// It is tested here rather than through poissonNext because poissonNext cannot
// show the bug on every platform: at a mean of MaxInt64 the guard returns the
// mean and an unguarded arm64 saturates to the same MaxInt64, so the two are
// indistinguishable. Feeding the conversion directly makes the fallback visible
// wherever the suite runs -- deleting the guard yields MaxInt64 on arm64 and
// MinInt64 on amd64, and neither is the fallback.
func TestBoundedDurationRefusesAConversionThatDoesNotFit(t *testing.T) {
	fallback := 120 * time.Second

	require.Equal(t, 5*time.Second, boundedDuration(float64(5*time.Second), fallback),
		"a value that fits is converted, not replaced")
	require.Equal(t, fallback, boundedDuration(1e30, fallback),
		"past MaxInt64: amd64 would report a negative gap, arm64 a 292-year one")
	require.Equal(t, fallback, boundedDuration(-1e30, fallback),
		"past MinInt64")
	require.Equal(t, fallback, boundedDuration(math.NaN(), fallback),
		"NaN loses every ordinary comparison, so the test has to be written to catch it")
}

func TestDefaultFeelerHandshakeTimeoutBeatsPeerNegotiateTimeout(t *testing.T) {
	require.Less(t, defaultFeelerHandshakeTimeout, peer.NegotiateTimeout)
}

// TestFeelerHandshakeTimeoutGuardsBothConfiguredEdges pins the two promises the
// setting's own documentation makes about values it cannot use.
//
// Both are worth a test rather than a comment. A non-positive deadline fires
// before the far side can answer, so every probe would report a timeout. And a
// deadline at or beyond peer.NegotiateTimeout loses the race to the peer
// package, whose hang-up is logged at warning level on the line the
// disconnect-rate measurements count -- exactly the noise the deadline exists to
// keep out.
func TestFeelerHandshakeTimeoutGuardsBothConfiguredEdges(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{name: "zero falls back to the default", configured: 0, want: defaultFeelerHandshakeTimeout},
		{name: "negative falls back to the default", configured: -time.Second, want: defaultFeelerHandshakeTimeout},
		{name: "equal to the peer timeout is brought inside it", configured: peer.NegotiateTimeout, want: peer.NegotiateTimeout - time.Second},
		{name: "beyond the peer timeout is brought inside it", configured: peer.NegotiateTimeout + time.Hour, want: peer.NegotiateTimeout - time.Second},
		{name: "the shipped default is left alone", configured: defaultFeelerHandshakeTimeout, want: defaultFeelerHandshakeTimeout},
		{name: "a short testing value is left alone", configured: 250 * time.Millisecond, want: 250 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, feelerHandshakeTimeout(ulogger.TestLogger{}, tc.configured))
		})
	}
}

// TestFeelerProbeUsesTheConfiguredHandshakeTimeout pins the second hop, which
// the settings loader test cannot see: that the loaded value actually reaches
// the probe's timer.
//
// The listener accepts the connection and then says nothing, which is the
// failure the deadline is for. With the setting honoured the probe gives up
// after the configured moment; with the setting ignored in favour of any of the
// constants around it, the probe sits there for tens of seconds. The generous
// budget is deliberate -- it is not measuring the timeout, only proving the
// configured one is the one being used.
func TestFeelerProbeUsesTheConfiguredHandshakeTimeout(t *testing.T) {
	ln := startMuteFeelerTestListener(t)

	swapTestConfig(t, ln.Addr().String())

	srv := newFeelerTestServer(t)
	srv.settings.Legacy.FeelerHandshakeTimeout = 250 * time.Millisecond
	serveFeelerSnapshot(srv, feelerSnapshot{})

	// Settled the way startFeeler settles it, on a server whose loop was never
	// started. This is the hop under test: runOneProbe used to fill the field in,
	// which made the helper rather than the production path the thing carrying
	// the configured value here.
	srv.feelerHandshake = feelerHandshakeTimeout(srv.logger, srv.settings.Legacy.FeelerHandshakeTimeout)

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	done := make(chan struct{})

	go func() {
		defer close(done)
		runOneProbe(srv)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the probe ignored the configured handshake timeout")
	}

	require.Equal(t, uint64(1), srv.feelerAttempted.Load())
	require.Equal(t, uint64(0), srv.feelerVerified.Load(),
		"a host that never identified itself must never be promoted")
}

// TestFeelerHandshakeTimeoutIsSettledOnce pins where the setting is validated.
//
// legacy_feelerHandshakeTimeout is fixed at startup and both of
// feelerHandshakeTimeout's guards log at warn, so validating it inside the probe
// reported a startup mistake as permanent runtime noise: a warning roughly every
// two minutes for the life of the process, on the level this series is trying to
// keep clean, and two or three lines per probe rather than the one info line the
// feature is meant to be checkable by.
func TestFeelerHandshakeTimeoutIsSettledOnce(t *testing.T) {
	t.Run("startFeeler warns once and keeps the corrected value", func(t *testing.T) {
		rec := &recordingLogger{Logger: ulogger.TestLogger{}}

		srv := newFeelerTestServer(t)
		srv.logger = rec
		srv.settings.Legacy.FeelerHandshakeTimeout = -time.Second

		// No connection manager, so feelerAllowed is false and the loop this
		// starts never probes. Only the startup path is under test here.
		startFeelerLoop(t, srv)

		require.Equal(t, defaultFeelerHandshakeTimeout, srv.feelerHandshake,
			"the corrected deadline has to be carried, not recomputed")

		warns, _ := rec.counts()
		require.Equal(t, 1, warns, "the guard belongs in the startup log: %s", rec.logged())
	})

	t.Run("a probe never re-validates it", func(t *testing.T) {
		// A listener that answers, so the probe reaches the timer and then ends
		// on the version rather than on the deadline. That leaves the warn count
		// as the only thing separating the two versions of this code, instead of
		// how long the probe sat there.
		ln, served := startFeelerTestListener(t, "/Bitcoin SV:1.1.0/")

		swapTestConfig(t, ln.Addr().String())

		rec := &recordingLogger{Logger: ulogger.TestLogger{}}

		srv := newFeelerTestServer(t)
		srv.logger = rec
		srv.settings.Legacy.FeelerHandshakeTimeout = -time.Second
		serveFeelerSnapshot(srv, feelerSnapshot{})

		na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
		srv.addrManager.AddAddress(na, testSourceAddr())

		// Settled the way startFeeler settles it, on a server whose loop was
		// never started.
		srv.feelerHandshake = defaultFeelerHandshakeTimeout

		runOneProbe(srv)

		select {
		case <-served:
		case <-time.After(5 * time.Second):
			t.Fatal("the listener was never reached, so the probe never built its timer")
		}

		require.Equal(t, uint64(1), srv.feelerVerified.Load(),
			"the probe has to get past the handshake for this to be measuring anything")

		warns, _ := rec.counts()
		require.Zero(t, warns, "a startup misconfiguration must not be re-reported per probe: %s", rec.logged())
	})
}

// TestFeelerProbeWaitsWhenItsDeadlineWasNeverSettled covers the one input to the
// probe's timer that nothing else checks: the zero value of s.feelerHandshake.
//
// startFeeler settles the field before starting the only goroutine that probes,
// so production cannot reach the timer with a zero. That invariant is a
// call-graph fact rather than anything the compiler holds, and the tree already
// steps around it: TestFeelerHandlerWaitsForASlotToken starts feelerHandler
// directly with the field unset, and its only assertion reads feelerAttempted,
// which the probe increments before the dial and long before the timer -- so it
// drives this path without being able to see it.
//
// The cost of the invariant breaking is the least visible failure this feature
// has. A timer built from zero is ready the moment the select is reached, so the
// probe reports a timeout before the far side could answer, having already
// written an Attempt against the address back at the TCP connect. Every probe
// would discourage the address it was sent to verify: chance() multiplies an
// attempted address's weight by 0.01 for ten minutes and divides it by 1.5 per
// counted failure, and isBad condemns a never-successful address at three.
//
// Asserted on the end state rather than on the resolved duration. The listener
// holds its version back longer than an unguarded probe could possibly wait, so
// "was the address promoted" is the whole question.
func TestFeelerProbeWaitsWhenItsDeadlineWasNeverSettled(t *testing.T) {
	ln := startSlowFeelerTestListener(t, "/Bitcoin SV:1.1.0/", 250*time.Millisecond)

	swapTestConfig(t, ln.Addr().String())

	srv := newFeelerTestServer(t)
	serveFeelerSnapshot(srv, feelerSnapshot{})

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	require.Zero(t, srv.feelerHandshake,
		"the unset field is the state under test, so nothing may have settled it")

	// Synchronous: everything below is asserted on a finished probe.
	runOneProbe(srv)

	require.Equal(t, uint64(1), srv.feelerVerified.Load(),
		"a probe whose deadline was never settled must still wait for the version")

	require.Nil(t, srv.addrManager.UnverifiedAddress(),
		"and the address it verified must leave the new table")
}

// startSlowFeelerTestListener answers like startFeelerTestListener but holds its
// version back for delay first, so a probe that did not wait cannot be mistaken
// for one that did.
func startSlowFeelerTestListener(t *testing.T, userAgent string, delay time.Duration) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	net2 := settings.NewSettings().ChainCfgParams.Net

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}

		defer func() { _ = conn.Close() }()

		msg, _, err := wire.ReadMessage(conn, wire.ProtocolVersion, net2)
		if err != nil {
			return
		}

		if _, ok := msg.(*wire.MsgVersion); !ok {
			return
		}

		time.Sleep(delay)

		me := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 8333, wire.SFNodeNetwork)
		you := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 8333, wire.SFNodeNetwork)

		reply := wire.NewMsgVersion(me, you, rand.Uint64(), 0)
		reply.UserAgent = userAgent
		reply.Services = wire.SFNodeNetwork

		if err := wire.WriteMessage(conn, reply, wire.ProtocolVersion, net2); err != nil {
			return
		}

		if err := wire.WriteMessage(conn, wire.NewMsgVerAck(), wire.ProtocolVersion, net2); err != nil {
			return
		}

		// Hold the connection open so the probe is the side that hangs up.
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	return ln
}

// startMuteFeelerTestListener accepts one connection, reads the probe's version
// message so the probe is genuinely waiting on a reply, and then holds the
// connection open without answering.
//
// Holding it open is the whole point: a listener that closed instead would end
// the probe through its disconnect arm, which would satisfy a timing assertion
// whatever the deadline was set to.
func startMuteFeelerTestListener(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	net2 := settings.NewSettings().ChainCfgParams.Net

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}

		defer func() { _ = conn.Close() }()

		if _, _, err := wire.ReadMessage(conn, wire.ProtocolVersion, net2); err != nil {
			return
		}

		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	return ln
}

// TestFeelerProbeWritesNothingWhileShuttingDown covers the guard immediately
// after the dial, on both of its arms.
//
// peerHandler stops the address manager the moment its loop exits, so a write
// that loses that race is silently lost. The guard gives up instead. Before this
// test all three of the probe's shutdown checks could be deleted with the whole
// package staying green, which is why each of them now has one.
//
// Both arms matter and they are separate code paths: the failure arm records an
// attempt through recordFailedDial, the success arm goes on to build a peer and
// can promote. Each subtest is paired with its running counterpart, because
// "nothing was written" is only evidence if something is written when the node
// is up.
func TestFeelerProbeWritesNothingWhileShuttingDown(t *testing.T) {
	t.Run("failed dial", func(t *testing.T) {
		for _, tt := range []struct {
			name         string
			shuttingDown bool
			wantRecorded bool
		}{
			{name: "running records the failure", wantRecorded: true},
			{name: "shutting down records nothing", shuttingDown: true},
		} {
			t.Run(tt.name, func(t *testing.T) {
				swapTestConfig(t, "")

				cfg.dial = func(string, string, time.Duration) (net.Conn, error) {
					return nil, errDeadHost
				}

				srv := newFeelerTestServer(t)
				serveFeelerSnapshot(srv, feelerSnapshot{})
				atLeastTwoAutomaticPeers(t, srv)

				na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
				srv.addrManager.AddAddress(na, testSourceAddr())

				if tt.shuttingDown {
					beginFeelerShutdown(srv)
				}

				runOneProbe(srv)

				ka := srv.addrManager.UnverifiedAddress()
				require.NotNil(t, ka, "a failed dial must not move the address anywhere")
				require.Equal(t, tt.wantRecorded, !ka.LastAttempt.IsZero(),
					"a dial failure is recorded against the address only while the node is running")
			})
		}
	})

	t.Run("successful dial", func(t *testing.T) {
		for _, tt := range []struct {
			name         string
			shuttingDown bool
			wantRecorded bool
			wantPromoted bool
		}{
			{name: "running promotes", wantRecorded: true, wantPromoted: true},
			{name: "shutting down writes nothing", shuttingDown: true},
		} {
			t.Run(tt.name, func(t *testing.T) {
				ln, _ := startFeelerTestListener(t, "/Bitcoin SV:1.1.0/")

				swapTestConfig(t, ln.Addr().String())

				srv := newFeelerTestServer(t)
				serveFeelerSnapshot(srv, feelerSnapshot{})
				atLeastTwoAutomaticPeers(t, srv)

				na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
				srv.addrManager.AddAddress(na, testSourceAddr())

				if tt.shuttingDown {
					beginFeelerShutdown(srv)
				}

				runOneProbe(srv)

				if tt.wantPromoted {
					require.Nil(t, srv.addrManager.UnverifiedAddress(),
						"a verified address must leave the new table")
					require.Equal(t, uint64(1), srv.feelerVerified.Load())

					return
				}

				ka := srv.addrManager.UnverifiedAddress()
				require.NotNil(t, ka, "an abandoned probe must leave the address where it was")
				require.Equal(t, tt.wantRecorded, !ka.LastAttempt.IsZero(),
					"an abandoned probe must not record an attempt either")
				require.Equal(t, uint64(0), srv.feelerVerified.Load())
			})
		}
	})
}

// TestAttemptIfRunningRefusesToWriteWhileShuttingDown pins the second of the
// three shutdown checks, the one covering the attempt recorded after the TCP
// connect.
//
// It is its own function rather than a step in a probe because the check it
// guards is only reachable when shutdown begins *between* the dial and this
// write. Driving a whole probe cannot land in that window on purpose, so the
// window is tested where it lives.
func TestAttemptIfRunningRefusesToWriteWhileShuttingDown(t *testing.T) {
	for _, tt := range []struct {
		name         string
		shuttingDown bool
		wantWritten  bool
	}{
		{name: "running writes", wantWritten: true},
		{name: "shutting down does not", shuttingDown: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			swapTestConfig(t, "")

			srv := newFeelerTestServer(t)
			atLeastTwoAutomaticPeers(t, srv)

			na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
			srv.addrManager.AddAddress(na, testSourceAddr())

			require.True(t, srv.addrManager.UnverifiedAddress().LastAttempt.IsZero(),
				"the address starts out with no attempt against it")

			if tt.shuttingDown {
				beginFeelerShutdown(srv)
			}

			require.Equal(t, tt.wantWritten, srv.attemptIfRunning(na),
				"attemptIfRunning must report whether it wrote")

			require.Equal(t, tt.wantWritten, !srv.addrManager.UnverifiedAddress().LastAttempt.IsZero(),
				"and the report must match what reached the address book")
		})
	}
}

// TestJudgeVersionHonoursAVersionThatRacedTheTeardown is the test for the fix to
// the probe's select.
//
// A version can land in the same instant the far side hangs up, or the handshake
// deadline expires. A select picks uniformly among cases that are already ready,
// so before this the hang-up and timeout arms could throw away a version the
// probe already had. The promotion that goes missing is a nuisance; the ban that
// goes missing is the problem, because the address stays in the new table and the
// feeler spends its whole allowance rediscovering the same BTC and BCH nodes.
//
// Measured before the fix, with a delay widening the window the probe reaches
// the select through: 23 of 40 probes of a BSV host failed to promote it and 20
// of 40 probes of a BTC host failed to ban it. A clean coin toss, as advertised.
//
// So each arm is driven here with a version already in hand, and has to reach the
// same verdict the version arm would.
func TestJudgeVersionHonoursAVersionThatRacedTheTeardown(t *testing.T) {
	for _, fallback := range []string{
		"hung up before its version",
		"timed out",
	} {
		t.Run(fallback, func(t *testing.T) {
			t.Run("a BSV version is still a promotion", func(t *testing.T) {
				swapTestConfig(t, "")

				srv := newFeelerTestServer(t)

				na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
				srv.addrManager.AddAddress(na, testSourceAddr())

				res := settledFeelerResult("/Bitcoin SV:1.1.0/")

				require.Equal(t, "verified", srv.judgeVersion(na, "8.8.8.8:8333", res, fallback))
				require.Nil(t, srv.addrManager.UnverifiedAddress(),
					"a verified address must leave the new table however its wait ended")
				require.Equal(t, uint64(1), srv.feelerVerified.Load())
			})

			t.Run("a non-BSV version is still a ban", func(t *testing.T) {
				swapTestConfig(t, "")

				srv := newFeelerTestServer(t)

				na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
				srv.addrManager.AddAddress(na, testSourceAddr())

				res := settledFeelerResult("/Satoshi:0.21.0/")

				require.Equal(t, "answered but is not a BSV node",
					srv.judgeVersion(na, "8.8.8.8:8333", res, fallback))
				require.True(t, srv.banList.IsBanned("8.8.8.8"),
					"a host that identifies itself as non-BSV must be banned whichever arm woke")
				require.Equal(t, uint64(0), srv.feelerVerified.Load())
			})
		})
	}

	// The counterpart, and the half that stops the re-check swallowing the honest
	// case: with no version in hand the arm's own reason must survive untouched.
	t.Run("no version means the arm keeps its own reason", func(t *testing.T) {
		swapTestConfig(t, "")

		srv := newFeelerTestServer(t)

		na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
		srv.addrManager.AddAddress(na, testSourceAddr())

		res := &feelerResult{done: make(chan struct{})}

		require.Equal(t, "hung up before its version",
			srv.judgeVersion(na, "8.8.8.8:8333", res, "hung up before its version"))
		require.NotNil(t, srv.addrManager.UnverifiedAddress(),
			"a host that never answered must stay where it was")
		require.Equal(t, uint64(0), srv.feelerVerified.Load())
	})
}

// TestJudgeVersionRefusesToPromoteWhileShuttingDown pins the last of the three
// shutdown checks, the one inside the verdict.
//
// It is reachable only when shutdown begins after a version has arrived, and it
// is the belt to the select's own quit arm: with both ready that select is a
// coin toss too, so the promotion has to be refused on either outcome.
func TestJudgeVersionRefusesToPromoteWhileShuttingDown(t *testing.T) {
	for _, tt := range []struct {
		name         string
		shuttingDown bool
		wantOutcome  string
	}{
		{name: "running promotes", wantOutcome: "verified"},
		{name: "shutting down abandons", shuttingDown: true, wantOutcome: "abandoned, shutting down"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			swapTestConfig(t, "")

			srv := newFeelerTestServer(t)

			na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
			srv.addrManager.AddAddress(na, testSourceAddr())

			if tt.shuttingDown {
				beginFeelerShutdown(srv)
			}

			res := settledFeelerResult("/Bitcoin SV:1.1.0/")

			require.Equal(t, tt.wantOutcome,
				srv.judgeVersion(na, "8.8.8.8:8333", res, "no version received"))

			require.Equal(t, !tt.shuttingDown, srv.addrManager.UnverifiedAddress() == nil,
				"the address moves out of the new table only while the node is running")
		})
	}
}

// settledFeelerResult is a feelerResult that has already taken a version, as one
// woken by res.done would be, announcing a protocol the node accepts.
func settledFeelerResult(userAgent string) *feelerResult {
	return settledFeelerResultAt(userAgent, int32(wire.ProtocolVersion))
}

// settledFeelerResultAt is the same, at a chosen protocol version.
func settledFeelerResultAt(userAgent string, protocol int32) *feelerResult {
	res := &feelerResult{done: make(chan struct{})}
	res.ua = userAgent
	res.protocol = protocol
	res.once.Do(func() { close(res.done) })

	return res
}

// TestJudgeVersionRefusesAnUnacceptableProtocol pins the probe against being
// laxer than the path it feeds.
//
// The ordinary outbound path drops a peer below MinAcceptableProtocolVersion
// without ever marking its address good. The probe has to apply the same rule
// itself: the peer package applies it only after invoking the OnVersion listener
// this probe promotes from, so res.done is already closed by the time the peer
// package rejects. Promotion can evict an existing tried entry, so verifying an
// address the node then declines to dial costs a usable peer.
//
// Not a ban, unlike a non-BSV user agent. The host is a BSV node on a protocol
// we will not speak, so the address is honestly reachable and merely unusable.
func TestJudgeVersionRefusesAnUnacceptableProtocol(t *testing.T) {
	for _, tt := range []struct {
		name        string
		protocol    int32
		wantOutcome string
		wantVerdict bool
	}{
		{name: "current protocol promotes", protocol: int32(wire.ProtocolVersion), wantOutcome: "verified", wantVerdict: true},
		{name: "the minimum we accept promotes", protocol: int32(peer.MinAcceptableProtocolVersion), wantOutcome: "verified", wantVerdict: true},
		{name: "one below the minimum does not", protocol: int32(peer.MinAcceptableProtocolVersion) - 1, wantOutcome: "answered on a protocol we do not accept"},
		{name: "an unset protocol does not", protocol: 0, wantOutcome: "answered on a protocol we do not accept"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			swapTestConfig(t, "")

			srv := newFeelerTestServer(t)

			na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
			srv.addrManager.AddAddress(na, testSourceAddr())

			res := settledFeelerResultAt("/Bitcoin SV:1.1.0/", tt.protocol)

			require.Equal(t, tt.wantOutcome,
				srv.judgeVersion(na, "8.8.8.8:8333", res, "no version received"))

			require.Equal(t, tt.wantVerdict, srv.addrManager.UnverifiedAddress() == nil,
				"only an acceptable protocol moves the address out of the new table")

			require.False(t, srv.banList.IsBanned("8.8.8.8"),
				"a BSV node on an old protocol is unusable, not hostile")
		})
	}

	// Parity with the ordinary path, which is the whole point of applying this
	// rule in the probe at all. serverPeer.OnVersion returns at its protocol
	// check before it ever looks at the user agent, so a non-BSV client on a
	// protocol we do not speak is dropped there and never banned. The probe has
	// to reach the same verdict, because a ban is the widest thing this feature
	// does - process-wide, for a day, with sweeps in two layers.
	t.Run("a non-BSV client on a bad protocol is dropped, not banned", func(t *testing.T) {
		swapTestConfig(t, "")

		srv := newFeelerTestServer(t)

		na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
		srv.addrManager.AddAddress(na, testSourceAddr())

		res := settledFeelerResultAt("/Satoshi:0.21.0/", int32(peer.MinAcceptableProtocolVersion)-1)

		require.Equal(t, "answered on a protocol we do not accept",
			srv.judgeVersion(na, "8.8.8.8:8333", res, "no version received"))

		require.False(t, srv.banList.IsBanned("8.8.8.8"),
			"the protocol check must be reached first, exactly as in OnVersion")

		require.NotNil(t, srv.addrManager.UnverifiedAddress(),
			"and the address is still not promoted")
	})
}

// TestFeelerHostKeyMatchesThePeerSnapshot pins the two halves of the
// "never a host we are already talking to" rule against drifting apart.
//
// peerState.connectedHosts keys on the host part of a peer's dial string, which
// NetAddressKey builds. The filter used to key on na.IP.String() instead. For
// IPv4 and IPv6 literals the two happen to agree, so nothing showed; for an
// OnionCat address they never do — the dial string carries the .onion name and
// the raw IP the fd87:d87e:eb43: form — and the rule was silently blind to every
// Tor peer the node had.
func TestFeelerHostKeyMatchesThePeerSnapshot(t *testing.T) {
	onion := make(net.IP, net.IPv6len)
	copy(onion, net.ParseIP("fd87:d87e:eb43::"))

	for i := 6; i < net.IPv6len; i++ {
		onion[i] = byte(i)
	}

	for _, tt := range []struct {
		name string
		ip   net.IP
	}{
		{name: "ipv4", ip: net.ParseIP("8.8.8.8")},
		{name: "ipv6", ip: net.ParseIP("2001:4860:4860::8888")},
		{name: "onioncat", ip: onion},
	} {
		t.Run(tt.name, func(t *testing.T) {
			na := wire.NewNetAddressIPPort(tt.ip, 8333, wire.SFNodeNetwork)

			// Exactly how connectedHosts derives its key, from exactly the string
			// a peer is dialled with.
			wantHost, _, err := net.SplitHostPort(addrmgr.NetAddressKey(na))
			require.NoError(t, err)

			require.Equal(t, wantHost, feelerHostKey(na),
				"the probe must name a host the same way the peer snapshot does")

			require.True(t, occupiedByAPeer(feelerSnapshot{
				hosts: map[string]struct{}{wantHost: {}},
			}, na), "a host the node is talking to must read as occupied")
		})
	}

	t.Run("onioncat is not matched by its raw ip", func(t *testing.T) {
		na := wire.NewNetAddressIPPort(onion, 8333, wire.SFNodeNetwork)

		require.NotEqual(t, na.IP.String(), feelerHostKey(na),
			"this is the disagreement the bug lived in; if these ever match, "+
				"the onioncat case above has stopped testing anything")

		require.False(t, occupiedByAPeer(feelerSnapshot{
			hosts: map[string]struct{}{na.IP.String(): {}},
		}, na), "the raw-IP key is the one that never appears in a snapshot")
	})
}

// TestFeelerProbeDropsAHostAPeerTookDuringTheDial pins the re-check between the
// dial and the handshake.
//
// The snapshot feelerCandidate filters against is taken before the dial, which
// can take the whole connect timeout, so it is stale by the time the socket is
// up. svnode re-checks at the same point, in OpenNetworkConnection. The window
// is real: probing is only allowed at target, so a probe starts, an outbound
// drops, and replenishment — which counts established peers only — immediately
// dials the same address the probe is sitting on, because Good has not run yet.
//
// The probe must give the host up rather than hold a second connection to a peer
// the node is mid-download from.
func TestFeelerProbeDropsAHostAPeerTookDuringTheDial(t *testing.T) {
	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)

	for _, tt := range []struct {
		name         string
		taken        bool
		wantVerified uint64
	}{
		{name: "nobody took it, the probe runs", wantVerified: 1},
		{name: "a peer took it, the probe gives up", taken: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ln, _ := startFeelerTestListener(t, "/Bitcoin SV:1.1.0/")

			swapTestConfig(t, ln.Addr().String())

			srv := newFeelerTestServer(t)
			atLeastTwoAutomaticPeers(t, srv)
			srv.addrManager.AddAddress(na, testSourceAddr())

			// The snapshot the candidate filter sees is empty either way, so the
			// address is always selected. What changes is the snapshot served to
			// the re-check after the dial — which is the whole point: a filter
			// that passed, then a peer that arrived.
			snaps := []feelerSnapshot{{}}
			if tt.taken {
				snaps = append(snaps, feelerSnapshot{
					hosts: map[string]struct{}{feelerHostKey(na): {}},
				})
			}

			serveFeelerSnapshotSequence(srv, snaps)

			runOneProbe(srv)

			require.Equal(t, tt.wantVerified, srv.feelerVerified.Load(),
				"a host a peer has taken must not be probed through to promotion")

			require.Equal(t, tt.taken, srv.addrManager.UnverifiedAddress() != nil,
				"and the address must stay put when the probe gives up")
		})
	}
}

// serveFeelerSnapshotSequence answers the probe's snapshot queries with each
// snapshot in turn, repeating the last one once the list runs out. It stands in
// for a peer handler whose answer changes while the probe is dialling.
func serveFeelerSnapshotSequence(srv *server, snaps []feelerSnapshot) {
	go func() {
		i := 0

		for {
			select {
			case <-srv.quit:
				return
			case q := <-srv.query:
				if msg, ok := q.(getFeelerSnapshotMsg); ok {
					msg.reply <- snaps[i]

					if i < len(snaps)-1 {
						i++
					}
				}
			}
		}
	}()
}

// recordingLogger counts what was logged at each level, so a test can assert on
// the level a line came out at rather than on its text. It also keeps the
// rendered lines, for the tests that do care what was said.
type recordingLogger struct {
	ulogger.Logger

	mtx   sync.Mutex
	warns int
	errs  int
	lines []string
}

func (l *recordingLogger) record(format string, args ...interface{}) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Infof(format string, args ...interface{}) {
	l.mtx.Lock()
	l.record(format, args...)
	l.mtx.Unlock()
}

func (l *recordingLogger) Warnf(format string, args ...interface{}) {
	l.mtx.Lock()
	l.warns++
	l.record(format, args...)
	l.mtx.Unlock()
}

func (l *recordingLogger) Errorf(format string, args ...interface{}) {
	l.mtx.Lock()
	l.errs++
	l.record(format, args...)
	l.mtx.Unlock()
}

func (l *recordingLogger) counts() (int, int) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.warns, l.errs
}

// logged returns every line recorded so far, joined, so a test can assert that
// one particular reason was named without pinning the exact wording of all of
// them.
func (l *recordingLogger) logged() string {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return strings.Join(l.lines, "\n")
}

// TestQuietProbeLoggerKeepsAProbeOutOfTheDisconnectCount is the test for the
// third thing the first mainnet soak found.
//
// Probing a non-BSV host produced two loud lines: the probe peer's read loop met
// a command the BSV parser cannot handle, logged an ERROR, and disconnected at
// WARN with "reason: malformed message". That WARN is the exact line the
// disconnect-rate measurements key on, so the feeler inflated the number this
// series exists to drive down - on every probe of the fork clients it is built to
// find and ban.
//
// The log-once guard cannot fix it: the read loop's teardown ran first and won,
// so the guard suppressed the feeler's own quiet line and let the warning stand.
// The fix is to quieten the probe peer's logger instead.
func TestQuietProbeLoggerKeepsAProbeOutOfTheDisconnectCount(t *testing.T) {
	rec := &recordingLogger{Logger: ulogger.TestLogger{}}

	quiet := quietProbeLogger{rec}

	quiet.Warnf("Disconnecting (%s) reason: %s", "1.2.3.4:8333", "malformed message")
	quiet.Errorf("Can't read message from %s: %v", "1.2.3.4:8333", errDeadHost)

	warns, errs := rec.counts()
	require.Zero(t, warns, "a probe peer must never log at warn: that level is the disconnect-rate anchor")
	require.Zero(t, errs, "nor at error")

	// The counterpart: the wrapper must pass everything else straight through,
	// or quietening the probe would blind the feeler's own reporting too.
	plain := &recordingLogger{Logger: ulogger.TestLogger{}}
	plain.Warnf("a real peer problem")
	plain.Errorf("another")

	warns, errs = plain.counts()
	require.Equal(t, 1, warns, "the underlying logger must still count what is sent straight to it")
	require.Equal(t, 1, errs)
}

// TestFeelerProbeOfANonBSVHostStaysQuiet drives the whole path against a listener
// that answers with a Bitcoin Cash user agent and then sends a command the BSV
// parser cannot handle, which is what the mainnet host actually did.
//
// The assertion is on the level, not the text: no warning may escape a probe,
// because a probe is not a peer the node lost.
func TestFeelerProbeOfANonBSVHostStaysQuiet(t *testing.T) {
	ln := startForkClientListener(t, "/Bitcoin Cash Node:29.1.0(EB32.0)/")

	swapTestConfig(t, ln.Addr().String())

	srv := newFeelerTestServer(t)
	rec := &recordingLogger{Logger: ulogger.TestLogger{}}
	srv.logger = rec

	serveFeelerSnapshot(srv, feelerSnapshot{})
	atLeastTwoAutomaticPeers(t, srv)

	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	srv.addrManager.AddAddress(na, testSourceAddr())

	runOneProbe(srv)

	require.Equal(t, uint64(0), srv.feelerVerified.Load(),
		"a Bitcoin Cash node must never be promoted")

	warns, errs := rec.counts()
	require.Zero(t, warns,
		"probing a fork client must not add to the disconnect-rate measurement")
	require.Zero(t, errs, "nor log an error for a host behaving exactly as expected")
}

// startForkClientListener answers with the given user agent and then sends a
// message whose command the BSV parser does not know, which is what the mainnet
// Bitcoin Cash host did and what made the probe's read loop shout.
func startForkClientListener(t *testing.T, userAgent string) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	tSettings := settings.NewSettings()
	net2 := tSettings.ChainCfgParams.Net

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}

		defer func() { _ = conn.Close() }()

		msg, _, err := wire.ReadMessage(conn, wire.ProtocolVersion, net2)
		if err != nil {
			return
		}

		if _, ok := msg.(*wire.MsgVersion); !ok {
			return
		}

		me := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 8333, wire.SFNodeNetwork)
		you := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 8333, wire.SFNodeNetwork)

		reply := wire.NewMsgVersion(me, you, rand.Uint64(), 0)
		reply.UserAgent = userAgent
		reply.Services = wire.SFNodeNetwork

		if err := wire.WriteMessage(conn, reply, wire.ProtocolVersion, net2); err != nil {
			return
		}

		if err := wire.WriteMessage(conn, wire.NewMsgVerAck(), wire.ProtocolVersion, net2); err != nil {
			return
		}

		// A command go-wire has no case for. Written by hand, because the wire
		// package will only marshal messages it knows about.
		_, _ = conn.Write(rawWireMessage(uint32(net2), "xversion"))

		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	return ln
}

// rawWireMessage builds a bitcoin message header with an empty payload for an
// arbitrary command string.
func rawWireMessage(magic uint32, command string) []byte {
	out := make([]byte, 0, 24)

	out = binary.LittleEndian.AppendUint32(out, magic)

	cmd := make([]byte, 12)
	copy(cmd, command)
	out = append(out, cmd...)

	out = binary.LittleEndian.AppendUint32(out, 0)

	first := sha256.Sum256(nil)
	second := sha256.Sum256(first[:])
	out = append(out, second[:4]...)

	return out
}
