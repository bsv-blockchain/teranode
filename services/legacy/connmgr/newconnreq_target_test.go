package connmgr

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestNewConnReqRespectsTargetOutbound is the ceiling test for NewConnReq
// itself.
//
// The two sibling ceiling tests in target_ceiling_test.go cover the paths that
// go through handleFailedConn's replacement arm and through the replenishment
// pass. Neither covers the third dial driver: the bare NewConnReq that
// handleFailedConn schedules with time.AfterFunc once maxFailedAttempts
// consecutive dials have failed. That one used to reserve and dial without ever
// consulting the books.
//
// It mattered because the same arm also arms the replenishment backoff for
// exactly RetryDuration and schedules the timer for exactly RetryDuration, so
// the two fire together: the pass fills the freed slot, the timer fills it
// again, and the node is left one connection above target with nothing to shed
// it. Under a churn of dial failures the excess accumulated — a target of six
// was observed sitting at fourteen.
func TestNewConnReqRespectsTargetOutbound(t *testing.T) {
	const (
		target   = 3
		interval = 10 * time.Millisecond
	)

	var (
		addrCount atomic.Int32
		dials     atomic.Int32
	)

	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound:    target,
		ReplenishInterval: interval,
		RetryDuration:     20 * time.Millisecond,
		GetNewAddress:     freshAddrFn(&addrCount),
		Dial:              countingDialer(&dials),
	})
	require.NoError(t, err)

	cmgr.Start()

	defer cmgr.Stop()

	require.Eventually(t, func() bool { return cmgr.AutomaticOutboundCount() == target },
		5*time.Second, 10*time.Millisecond, "connection manager never reached target")

	baseline := dials.Load()

	// Exactly what the max-failed-attempts timer does.
	cmgr.NewConnReq()

	require.Never(t, func() bool { return cmgr.AutomaticOutboundCount() > target },
		time.Second, 10*time.Millisecond,
		"a bare NewConnReq while already at target pushed the node above TargetOutbound")

	require.Equal(t, int32(0), dials.Load()-baseline,
		"NewConnReq must not dial when the automatic outbound tier is already at target")
}

// TestNewConnReqDialsWhenBelowTarget is the floor half of the test above: the
// target check must not turn NewConnReq into a no-op, or the max-failed-attempts
// recovery timer would never dial again.
func TestNewConnReqDialsWhenBelowTarget(t *testing.T) {
	const target = 3

	var (
		addrCount atomic.Int32
		dials     atomic.Int32
		connected = make(chan *ConnReq, 8)
	)

	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound: target,
		// No periodic pass and no wake, so the only thing that can dial here is
		// the explicit NewConnReq below.
		ReplenishInterval: 10 * time.Minute,
		RetryDuration:     20 * time.Millisecond,
		GetNewAddress:     freshAddrFn(&addrCount),
		Dial:              countingDialer(&dials),
		OnConnection:      func(c *ConnReq, _ net.Conn) { connected <- c },
	})
	require.NoError(t, err)

	cm := cmgr

	cm.wg.Add(1)

	go cm.connHandler()

	t.Cleanup(func() { close(cm.quit) })

	cm.NewConnReq()

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("NewConnReq did not dial while the tier was empty")
	}

	require.Equal(t, int32(1), dials.Load(), "NewConnReq below target must dial exactly once")
}
