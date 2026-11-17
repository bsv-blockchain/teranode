package connmgr

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// countingDialer wraps mockDialer so a test can assert how many dials a single
// event actually cost, rather than only that the node recovered.
func countingDialer(dials *atomic.Int32) func(net.Addr) (net.Conn, error) {
	return func(addr net.Addr) (net.Conn, error) {
		dials.Add(1)

		return mockDialer(addr)
	}
}

// TestOneLostPeerCostsOneDial is the ceiling test for the replenishment work.
//
// Every other replenishment test asserts a floor — that the node climbs back TO
// target. None asserted the ceiling, that it stops AT target, and that gap hid a
// bug in which every single disconnect cost two replacement dials and left the
// node sitting one connection above TargetOutbound.
//
// The mechanism was a window in which a slot belonged to nobody. handleFailedConn
// deleted the dead request from cm.pending and launched the replacement, but the
// replacement registered itself by sending on cm.requests — a channel serviced by
// connHandler, which was at that moment still inside handleFailedConn and could
// not service anything. The replenishment loop runs on its own goroutine, so it
// read the books during that window, saw an empty slot that was already being
// filled, and dialed a second address for it. Reserving the slot in cm.pending at
// the moment the replacement is decided closes the window.
//
// The interval is set far beyond the test's lifetime so no periodic pass can run:
// anything this test observes is the event-driven wake path, which is where the
// bug lived. A unique address per dial matters for the same reason it does in
// replenish_test.go — a repeated address would be suppressed by the dedup checks
// and hold the dial count down for reasons that have nothing to do with the
// accounting under test.
func TestOneLostPeerCostsOneDial(t *testing.T) {
	const target = 4

	connected := make(chan *ConnReq, 64)

	var (
		addrCount atomic.Int32
		dials     atomic.Int32
	)

	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound:    target,
		ReplenishInterval: 10 * time.Minute,
		Dial:              countingDialer(&dials),
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

	require.Eventually(t, func() bool {
		return cmgr.AutomaticOutboundCount() == target
	}, 5*time.Second, 10*time.Millisecond, "connection manager never reached target")

	baseline := dials.Load()

	// Disconnect, not Remove: this is the re-pend plus handleFailedConn path that
	// a real peer loss takes. Remove deliberately does not dial again.
	cmgr.Disconnect(first.ID())

	// Wait for the refill, then keep watching. The second dial arrived within
	// milliseconds of the first, so a test that stopped at "back to target" would
	// pass against the bug.
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("freed outbound slot was never refilled")
	}

	require.Never(t, func() bool {
		return cmgr.AutomaticOutboundCount() > target
	}, time.Second, 10*time.Millisecond,
		"connection manager exceeded TargetOutbound: a freed slot was dialed more than once")

	require.Equal(t, int32(1), dials.Load()-baseline,
		"one lost peer must cost exactly one replacement dial")
}

// slowCloseConn is a net.Conn whose Close blocks for a fixed delay.
//
// handleDisconnected calls connReq.conn.Close() while it is part-way through
// moving a request from the connected book to the pending one, so blocking in
// Close holds the connection manager in exactly the state a test needs to
// observe: a disconnected request counted by neither book.
type slowCloseConn struct {
	io.Reader
	io.Writer

	rAddr net.Addr
	delay time.Duration
}

func (c *slowCloseConn) Close() error {
	time.Sleep(c.delay)

	return nil
}

func (c *slowCloseConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *slowCloseConn) RemoteAddr() net.Addr               { return c.rAddr }
func (c *slowCloseConn) SetDeadline(_ time.Time) error      { return nil }
func (c *slowCloseConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *slowCloseConn) SetWriteDeadline(_ time.Time) error { return nil }

// TestOneLostPeerCostsOneDialWithPeriodicPass is TestOneLostPeerCostsOneDial
// with the periodic replenishment pass left switched on, which is how every
// node actually runs it — legacy_replenishInterval defaults to two seconds.
//
// The sibling test above deliberately parks the interval beyond its own
// lifetime so that only the event-driven wake can act, and that hid a second
// window with the same shape as the one the wake path had. Retiring a
// disconnected peer and re-pending it for a retry were two separate steps, with
// a socket close, a callback and a target comparison in between; for the whole
// of that gap the request was in neither book, so a periodic pass landing there
// read a free slot that connHandler was already about to fill, and dialed a
// second address for it. Both halves now happen together under dialMu.
//
// Blocking the socket close is what turns the race from a coin toss into a
// certainty: it holds the books open across many ticks, so if the gap exists at
// all this test finds it every run rather than once in a while.
func TestOneLostPeerCostsOneDialWithPeriodicPass(t *testing.T) {
	const (
		target      = 4
		interval    = 20 * time.Millisecond
		closeDelay  = 500 * time.Millisecond
		observeFor  = 1500 * time.Millisecond
		connectWait = 5 * time.Second
	)

	connected := make(chan *ConnReq, 64)

	var (
		addrCount atomic.Int32
		dials     atomic.Int32
		// Replacements close instantly; only the connections made while the
		// node is climbing to target hold the books open.
		fastClose atomic.Bool
	)

	dialer := func(addr net.Addr) (net.Conn, error) {
		dials.Add(1)

		r, w := io.Pipe()

		delay := closeDelay
		if fastClose.Load() {
			delay = 0
		}

		return &slowCloseConn{Reader: r, Writer: w, rAddr: addr, delay: delay}, nil
	}

	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound:    target,
		ReplenishInterval: interval,
		Dial:              dialer,
		GetNewAddress:     freshAddrFn(&addrCount),
		OnConnection: func(c *ConnReq, _ net.Conn) {
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
		case <-time.After(connectWait):
			t.Fatalf("only %d of %d initial connections established", i, target)
		}
	}

	require.Eventually(t, func() bool {
		return cmgr.AutomaticOutboundCount() == target
	}, connectWait, 10*time.Millisecond, "connection manager never reached target")

	fastClose.Store(true)

	baseline := dials.Load()

	cmgr.Disconnect(first.ID())

	select {
	case <-connected:
	case <-time.After(connectWait):
		t.Fatal("freed outbound slot was never refilled")
	}

	// Long enough for the blocked close to return and for any second dial it
	// let through to complete and be entered in the book.
	time.Sleep(observeFor)

	require.LessOrEqual(t, cmgr.AutomaticOutboundCount(), target,
		"connection manager exceeded TargetOutbound: a freed slot was dialed more than once")
	require.Equal(t, int32(1), dials.Load()-baseline,
		"one lost peer must cost exactly one replacement dial")
}
