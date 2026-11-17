package connmgr

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestPromotionIsSerialisedAgainstMeasurement closes the third window of the
// same shape as the two in target_ceiling_test.go.
//
// The rule the whole design rests on is that a slot counts as occupied while
// its request is visible in cm.conns or cm.pending, and automaticCounts is what
// reads that. It walks conns first and pending second, so those two walks are
// not one observation: while handleConnected promoted a request with two
// unlocked map operations, a measurement whose conns walk had just finished and
// whose pending walk had not yet started counted the request in NEITHER book.
// The replenishment pass then read a deficit that did not exist and dialed a
// second address for a slot that had just been filled. It was caught in a churn
// test as a stable seventh connection against a target of six.
//
// The window between the two walks is a few instructions wide, so reproducing
// it by racing is a coin toss and makes a poor guard. What this test asserts
// instead is the discipline that closes it: the promotion is taken under
// dialMu, the same lock every measurement takes, so a promotion cannot land
// between two halves of one measurement. Holding dialMu from the test must
// therefore hold the promotion, and that is deterministic.
func TestPromotionIsSerialisedAgainstMeasurement(t *testing.T) {
	var (
		addrCount atomic.Int32
		release   = make(chan struct{})
		dialed    = make(chan struct{})
	)

	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound: 1,
		// No periodic pass and no wake: this test drives the single dial itself.
		ReplenishInterval: time.Hour,
		GetNewAddress:     freshAddrFn(&addrCount),
		Dial: func(addr net.Addr) (net.Conn, error) {
			close(dialed)
			<-release

			return mockDialer(addr)
		},
	})
	require.NoError(t, err)

	cmgr.wg.Add(1)

	go cmgr.connHandler()

	t.Cleanup(func() { close(cmgr.quit) })

	go cmgr.NewConnReq()

	select {
	case <-dialed:
	case <-time.After(5 * time.Second):
		t.Fatal("NewConnReq never reached the dialer")
	}

	require.Equal(t, 1, cmgr.pending.Length(), "the dial in flight must be registered as pending")

	// Stand in for a replenishment pass that is part-way through its
	// measurement. Nothing may move between the books until it is done.
	cmgr.dialMu.Lock()

	close(release)

	// The dial returns and connHandler picks up the handleConnected message,
	// where it must now block. Generous next to the microseconds the promotion
	// itself takes, so a pass here is not a pass by luck.
	time.Sleep(250 * time.Millisecond)

	stillPending := cmgr.pending.Length()
	alreadyConnected := cmgr.conns.Length()

	cmgr.dialMu.Unlock()

	// Non-vacuity: the promotion really was in flight and completes the moment
	// the lock is released. If this fails the assertions below proved nothing.
	require.Eventually(t, func() bool {
		return cmgr.conns.Length() == 1 && cmgr.pending.Length() == 0
	}, 5*time.Second, time.Millisecond,
		"the promotion never completed after dialMu was released, so the hold above proved nothing")

	require.Equal(t, 0, alreadyConnected,
		"a request was promoted into the connected book while a measurement held dialMu; "+
			"a measurement that had already walked conns and not yet walked pending would count it in neither book")
	require.Equal(t, 1, stillPending,
		"a request was removed from the pending book while a measurement held dialMu")
}
