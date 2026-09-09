package connmgr

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// freshAddrFn returns a GetNewAddress that never repeats an address.
//
// This matters more than it looks. NewConnReq suppresses a dial whose address
// is already connected or already pending, so a test that hands out one fixed
// address would see "no extra dials" no matter what the replenishment loop
// decided — it would pass identically against the unfixed code. Every dial in
// these tests is therefore given a unique address, leaving the pending
// accounting as the only thing that can hold the dial count down.
func freshAddrFn(counter *atomic.Int32) func() (net.Addr, error) {
	return func() (net.Addr, error) {
		n := counter.Add(1)

		return &net.TCPAddr{IP: net.IPv4(10, byte(n>>16), byte(n>>8), byte(n)), Port: 18555}, nil
	}
}

// startHandlers runs connHandler and replenishHandler without going through
// Start, which would also fire TargetOutbound unsolicited NewConnReq calls and
// make dial counts ambiguous. connHandler calls wg.Done on exit, so the wait
// group is balanced here to match.
func startHandlers(t *testing.T, cm *ConnManager) {
	t.Helper()

	cm.wg.Add(1)

	go cm.connHandler()
	go cm.replenishHandler()

	t.Cleanup(func() { close(cm.quit) })
}

// TestReplenishDeficit locks in the fix for the dead periodic peer-replenishment
// backstop. The original ticker used a monotonic id counter as the loop bound and
// subtracted two uint32 values, which both (a) never dialed after startup and
// (b) underflowed to ~4 billion when open > target. replenishDeficit must return
// the real, non-negative deficit and never a huge underflowed number.
//
// The pending cases additionally lock in that in-flight dials count against the
// deficit. Without that, a short replenishment interval re-dials a connection
// that is merely slow to complete its TCP handshake, over and over, until it
// resolves — a dial storm rather than a top-up.
func TestReplenishDeficit(t *testing.T) {
	tests := []struct {
		name        string
		established int
		pending     int
		target      int
		want        int
	}{
		{name: "cold start dials full target", established: 0, target: 8, want: 8},
		{name: "below target dials the gap", established: 3, target: 8, want: 5},
		{name: "at target dials nothing", established: 8, target: 8, want: 0},
		{name: "above target dials nothing (no underflow)", established: 12, target: 8, want: 0},
		{name: "single below target", established: 7, target: 8, want: 1},
		{name: "zero target", established: 0, target: 0, want: 0},
		{name: "in-flight dials count toward target", established: 3, pending: 5, target: 8, want: 0},
		{name: "in-flight dials only partly cover the gap", established: 3, pending: 2, target: 8, want: 3},
		{name: "all slots in flight dials nothing", established: 0, pending: 8, target: 8, want: 0},
		{name: "more in flight than target dials nothing", established: 2, pending: 12, target: 8, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replenishDeficit(tt.established, tt.pending, tt.target)
			require.Equal(t, tt.want, got, "replenishDeficit(established=%d, pending=%d, target=%d)",
				tt.established, tt.pending, tt.target)
			require.GreaterOrEqual(t, got, 0, "deficit must never be negative")
			require.LessOrEqual(t, got, tt.target, "deficit must be bounded to at most target dials per tick")
		})
	}
}

// TestReplenishNoDialStormWhileDialsInFlight is the load-bearing test of this
// change. replenishDeficit subtracting pending is only useful if the running
// loop actually feeds it the in-flight count, so this exercises the real
// ticker against dials that never complete.
//
// A peer that is unreachable but not yet timed out holds its dial open for tens
// of seconds. Every dial here is wedged open for the whole test to model that.
// With the pending accounting in place the loop must launch exactly
// TargetOutbound dials and then sit still; without it each tick would see eight
// apparently empty slots and dial eight more addresses, so over the observation
// window the count would climb by roughly target dials per tick and the node
// would burn through its entire address table in a couple of minutes.
func TestReplenishNoDialStormWhileDialsInFlight(t *testing.T) {
	const (
		target = 8
		// Long enough that startup's TargetOutbound dials are all registered
		// as pending before the first tick — the loop is not expected to be
		// correct in the microseconds between launching a dial and it becoming
		// visible, and the debounce window exists precisely because it is not.
		interval = 200 * time.Millisecond
		// Seven ticks. Unfixed, that is ~56 extra dials against the 8 expected.
		observe = 1400 * time.Millisecond
	)

	var (
		addrCount atomic.Int32
		dials     atomic.Int32
	)

	// Released on cleanup only, so every dial stays in flight for the duration.
	release := make(chan struct{})

	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound:    target,
		ReplenishInterval: interval,
		GetNewAddress:     freshAddrFn(&addrCount),
		Dial: func(_ net.Addr) (net.Conn, error) {
			dials.Add(1)
			<-release

			return nil, errors.New("test shutting down")
		},
	})
	require.NoError(t, err)

	cmgr.Start()

	t.Cleanup(func() {
		cmgr.Stop()
		close(release)
	})

	require.Eventually(t, func() bool {
		return dials.Load() == target
	}, 5*time.Second, 10*time.Millisecond, "startup should launch exactly one dial per outbound slot")

	time.Sleep(observe)

	require.Equal(t, int32(target), dials.Load(),
		"in-flight dials must count against the deficit; extra dials here are the dial storm this fix exists to prevent")

	established, pending := cmgr.automaticCounts()
	require.Equal(t, 0, established, "no dial completed, so nothing should be established")
	require.Equal(t, target, pending, "every slot should still be accounted for as in flight")
}

// TestReplenishWakeRefillsBeforeTick proves the wake channel, not the ticker,
// is what patches a freed slot. The replenishment interval is set far beyond
// the lifetime of the test, so a tick can never fire: if the slot is refilled
// at all, only the wake signal can have caused it.
//
// This is the behaviour the whole change is for. Under the old fixed one-minute
// ticker a peer lost just after a tick left its slot empty for nearly a minute,
// and during initial block download — where peers churn constantly — that is a
// large share of the download spent below target.
func TestReplenishWakeRefillsFreedSlotBeforeAnyTick(t *testing.T) {
	const target = 4

	connected := make(chan *ConnReq, 64)

	var addrCount atomic.Int32

	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound: target,
		// No tick will occur during this test. The refill has to come from the
		// wake channel or not at all.
		ReplenishInterval: 10 * time.Minute,
		Dial:              mockDialer,
		GetNewAddress:     freshAddrFn(&addrCount),
		OnConnection: func(c *ConnReq, conn net.Conn) {
			connected <- c
		},
	})
	require.NoError(t, err)

	cmgr.Start()
	defer cmgr.Stop()

	var first *ConnReq

	for i := 0; i < target; i++ {
		select {
		case c := <-connected:
			if first == nil {
				first = c
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d initial connections established", i, target)
		}
	}

	require.Equal(t, target, cmgr.AutomaticOutboundCount())

	// Free a slot the way the server's phantom-connection eviction does.
	cmgr.Remove(first.ID())

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("freed outbound slot was not refilled, so the wake signal did not reach the replenishment loop")
	}

	require.Eventually(t, func() bool {
		return cmgr.AutomaticOutboundCount() >= target
	}, 5*time.Second, 10*time.Millisecond, "connection manager did not return to target")
}

// TestSignalReplenishNeverBlocks locks in that the wake signal can never stall
// connHandler. connHandler is the single serialisation point for all connection
// state, so if a burst of disconnects could block on this send, every other
// connection event would queue behind it — the node would stop processing
// connections entirely rather than merely dial slowly.
//
// The burst runs on its own goroutine and completion is asserted against a
// deadline, so a regression fails as a reported test failure rather than as an
// opaque whole-binary timeout.
func TestSignalReplenishNeverBlocks(t *testing.T) {
	// A positive interval is what enables the event-driven path at all: at 0 the
	// rollback lever turns signalReplenish into a no-op (see TestReplenishIntervalRollbackDisablesWake).
	cmgr, err := New(ulogger.TestLogger{}, &Config{Dial: mockDialer, ReplenishInterval: time.Second})
	require.NoError(t, err)

	// Nothing is draining the channel, so all but the first send must be
	// dropped rather than block.
	done := make(chan struct{})

	go func() {
		defer close(done)

		for i := 0; i < 100000; i++ {
			cmgr.signalReplenish()
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("signalReplenish blocked with no reader draining the wake channel")
	}

	require.Len(t, cmgr.replenishWake, 1, "wake is a coalescing signal, not a queue")
}

// TestAutomaticCountsExcludePermanent locks in the tier split. A permanent
// (addnode) peer must not occupy one of the automatic outbound slots: if it did,
// configuring a single addnode would silently drop the node to seven
// independently chosen outbound peers and cost it one of the address groups the
// outbound diversity rules exist to spread across.
func TestAutomaticCountsExcludePermanent(t *testing.T) {
	cmgr, err := New(ulogger.TestLogger{}, &Config{Dial: mockDialer})
	require.NoError(t, err)

	addr := func(last int) net.Addr {
		return &net.TCPAddr{IP: net.IPv4(127, 0, 0, byte(last)), Port: 18555}
	}

	// Three established: two automatic, one addnode.
	cmgr.conns.Set(1, &ConnReq{id: 1, addr: addr(1)})
	cmgr.conns.Set(2, &ConnReq{id: 2, addr: addr(2)})
	cmgr.conns.Set(3, &ConnReq{id: 3, addr: addr(3), Permanent: true})

	// Three in flight: one automatic, two addnode retries.
	cmgr.pending.Set(4, &ConnReq{id: 4, addr: addr(4)})
	cmgr.pending.Set(5, &ConnReq{id: 5, addr: addr(5), Permanent: true})
	cmgr.pending.Set(6, &ConnReq{id: 6, addr: addr(6), Permanent: true})

	established, pending := cmgr.automaticCounts()
	require.Equal(t, 2, established, "only non-permanent established conns count toward the target")
	require.Equal(t, 1, pending, "only non-permanent in-flight dials count toward the target")

	require.Equal(t, 2, cmgr.AutomaticOutboundCount())

	// With eight wanted and three automatic accounted for, five dials are owed —
	// the three addnode requests must not reduce that.
	require.Equal(t, 5, replenishDeficit(established, pending, 8))
}

// TestPermanentRetriesUnaffectedByReplenish covers the other half of the tier
// split: excluding addnode peers from the automatic books must not have cost
// them their own retry path. A permanent request whose dial fails still has to
// be retried indefinitely on its backoff schedule, and it must neither register
// in the automatic count nor wake the replenishment loop — an addnode peer that
// is simply down is not evidence that an automatic slot needs filling.
func TestPermanentRetriesUnaffectedByReplenish(t *testing.T) {
	var dials atomic.Int32

	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound: 8,
		RetryDuration:  5 * time.Millisecond,
		// nil GetNewAddress: no automatic dialling can occur, so every dial
		// counted below is unambiguously the permanent request retrying.
		GetNewAddress: nil,
		Dial: func(_ net.Addr) (net.Conn, error) {
			dials.Add(1)

			return nil, errors.New("addnode peer is down")
		},
	})
	require.NoError(t, err)

	startHandlers(t, cmgr)

	permanent := &ConnReq{Permanent: true}
	permanent.SetAddr(&net.TCPAddr{IP: net.ParseIP("203.0.113.50"), Port: 8333})

	go cmgr.Connect(permanent)

	// Retries must keep coming. The exact spacing is not asserted: the package
	// clamps maxRetryDuration to 2ms for tests, so every backoff step collapses
	// to the same value and only the retry count is observable here.
	require.Eventually(t, func() bool {
		return dials.Load() >= 4
	}, 5*time.Second, 5*time.Millisecond, "a failing permanent request must keep retrying on its own backoff")

	require.GreaterOrEqual(t, permanent.retryCount.Load(), uint32(3),
		"each permanent retry must advance the backoff counter")

	require.Equal(t, 0, cmgr.AutomaticOutboundCount(),
		"a permanent request must never occupy an automatic outbound slot")

	established, pending := cmgr.automaticCounts()
	require.Equal(t, 0, established)
	require.Equal(t, 0, pending, "a retrying addnode request must not be counted as an automatic dial in flight")

	require.Empty(t, cmgr.replenishWake,
		"a failing permanent request must not wake the automatic replenishment loop")
}

// TestReplenishIntervalZeroFallsBackToOneMinute covers the rollback lever. Every
// part of this change is meant to be switchable back to the old behaviour from
// settings, and for the replenishment cadence the off position is an unset
// interval, which must restore the historical one-minute ticker.
//
// A one-minute ticker cannot be waited out in a unit test, so the assertion is
// that no tick-driven dial happens within a window that a short interval would
// have filled many times over. The fast-interval sub-test is what gives that
// its meaning: it runs the identical harness with a 50ms interval and requires
// dials, proving the "no dials" observation reflects the cadence and not a
// harness that could never dial at all.
func TestReplenishIntervalZeroFallsBackToOneMinute(t *testing.T) {
	// Only the ticker is under test, so nothing here may signal the wake
	// channel; the books are left empty and untouched.
	newIdleManager := func(t *testing.T, interval time.Duration, dials *atomic.Int32) *ConnManager {
		t.Helper()

		var addrCount atomic.Int32

		cm, err := New(ulogger.TestLogger{}, &Config{
			TargetOutbound:    2,
			ReplenishInterval: interval,
			GetNewAddress:     freshAddrFn(&addrCount),
			Dial: func(addr net.Addr) (net.Conn, error) {
				dials.Add(1)

				return mockDialer(addr)
			},
		})
		require.NoError(t, err)

		startHandlers(t, cm)

		return cm
	}

	t.Run("unset interval does not dial on the old one-minute cadence", func(t *testing.T) {
		var dials atomic.Int32

		newIdleManager(t, 0, &dials)

		time.Sleep(1500 * time.Millisecond)

		require.Zero(t, dials.Load(),
			"an unset ReplenishInterval must fall back to the one-minute ticker, which cannot have fired yet")
	})

	t.Run("short interval dials within the same window", func(t *testing.T) {
		var dials atomic.Int32

		newIdleManager(t, 50*time.Millisecond, &dials)

		require.Eventually(t, func() bool {
			return dials.Load() > 0
		}, 1500*time.Millisecond, 10*time.Millisecond,
			"control case: the harness must dial when the interval is short, otherwise the test above proves nothing")
	})
}

// TestReplenishBacksOffWhenTheNetworkIsDown is the counterpart to the dial-storm
// test above, for the case where dials FAIL instead of hanging.
//
// After maxFailedAttempts consecutive failures we assume the network, not the
// peer, is down, and handleFailedConn schedules the next attempt a full
// RetryDuration out. That backoff is worthless on its own now that the periodic
// pass is much shorter than RetryDuration: a dial that fails instantly
// (ENETUNREACH, no default route) is removed from cm.pending immediately, so
// every pass sees the full deficit and launches another round. Left unfixed
// that is tens of dials a second against a dead network, each one drawing and
// penalising an address, eroding the address book below the re-seed floor
// exactly when the node can least afford it.
func TestReplenishBacksOffWhenTheNetworkIsDown(t *testing.T) {
	const (
		target = 8
		// Far shorter than RetryDuration, as it is in production (2s vs 5s).
		interval      = 20 * time.Millisecond
		retryDuration = 10 * time.Second
	)

	var (
		addrCount atomic.Int32
		dials     atomic.Int32
	)

	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound:    target,
		ReplenishInterval: interval,
		RetryDuration:     retryDuration,
		GetNewAddress:     freshAddrFn(&addrCount),
		Dial: func(_ net.Addr) (net.Conn, error) {
			dials.Add(1)

			// Instant failure, as an unreachable network gives.
			return nil, errors.New("network is unreachable")
		},
	})
	require.NoError(t, err)

	cmgr.Start()
	t.Cleanup(cmgr.Stop)

	// Let the failures accumulate past the threshold and the backoff engage.
	require.Eventually(t, func() bool {
		return cmgr.replenishBackoffUntil.Load() != 0
	}, 5*time.Second, 5*time.Millisecond,
		"consecutive dial failures past maxFailedAttempts must engage the replenishment backoff")

	// Let the dials that were already in flight when the backoff engaged land
	// before sampling. The deadline is armed by one failing dial, while the
	// others from the same pass are still somewhere between their reservation
	// and the Dial call, so they can raise the counter after Eventually has
	// observed the deadline. Sampling straight after it would race those
	// stragglers rather than test the backoff. Waiting for two readings a
	// couple of ticker periods apart to agree is what "in flight" means here.
	var settled atomic.Int32

	require.Eventually(t, func() bool {
		before := dials.Load()

		time.Sleep(2 * interval)

		if dials.Load() != before {
			return false
		}

		settled.Store(before)

		return true
	}, 5*time.Second, interval,
		"the dial count never went quiet after the backoff engaged")

	// Many ticker periods, but well inside RetryDuration.
	time.Sleep(50 * interval)

	require.Equal(t, settled.Load(), dials.Load(),
		"the periodic pass must honour the network-down backoff instead of dialling straight through it")
}

// TestReplenishIntervalRollbackDisablesWake pins the rollback lever's full
// scope. legacy_replenishInterval=0 has to back out the whole change, not just
// the cadence: with the event-driven path still live an operator could not tell
// "dials too often" from "dials on every connection event", and the lever would
// be untestable in the field.
func TestReplenishIntervalRollbackDisablesWake(t *testing.T) {
	cmgr, err := New(ulogger.TestLogger{}, &Config{Dial: mockDialer})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		cmgr.signalReplenish()
	}

	require.Empty(t, cmgr.replenishWake,
		"at the rollback value the wake channel must never be signalled, leaving only the one-minute ticker")
}
