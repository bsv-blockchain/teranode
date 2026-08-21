package connmgr

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestFailedOneShotDialDoesNotHoldAnOutboundSlot covers the one path through
// handleFailedConn that takes neither of its branches.
//
// In connect-only mode the server supplies no GetNewAddress, because a node
// pinned to named peers must not discover its own. A request that is also not
// permanent can still reach the connection manager there: `addnode <ip> onetry`
// builds exactly that, a one-shot dial with Permanent false. When its dial
// fails, the permanent arm does not apply and the replacement arm is guarded on
// GetNewAddress, so the request stays in cm.pending — and it is kept there on
// purpose, because that entry is the only handle Remove has for cancelling it.
//
// What must not happen is that it keeps counting as an outbound dial in flight.
// It is dead: nothing will retry it and nothing will be dialed in its place. If
// it counted, every failed one-shot dial would retire one automatic slot for
// the life of the process, and enough of them would starve the tier — the same
// starvation this PR fixes on the duplicate-address returns, reached by a
// different path.
//
// So this asserts both halves: the slot is free, and the request is still
// there to be cancelled.
func TestFailedOneShotDialDoesNotHoldAnOutboundSlot(t *testing.T) {
	cmgr, err := New(ulogger.TestLogger{}, &Config{
		TargetOutbound: 2,
		RetryDuration:  time.Millisecond,
		// Connect-only: no address source, which is what makes this path
		// distinct from every other dial failure in the manager.
		GetNewAddress: nil,
		Dial: func(net.Addr) (net.Conn, error) {
			return nil, errors.New("dial refused")
		},
	})
	require.NoError(t, err)

	cmgr.Start()
	defer cmgr.Stop()

	req := &ConnReq{Permanent: false}
	req.SetAddr(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 18555})

	cmgr.Connect(req)

	require.Eventually(t, func() bool {
		return req.State() == ConnFailing
	}, 5*time.Second, 10*time.Millisecond, "the one-shot dial never failed")

	// The dead request must not be counted as an outbound dial in flight.
	require.Eventually(t, func() bool {
		_, pending := cmgr.automaticCounts()
		return pending == 0
	}, 5*time.Second, 10*time.Millisecond,
		"a failed one-shot dial is still counted as pending, so it holds an outbound slot nothing can ever fill")

	established, pending := cmgr.automaticCounts()
	require.Equal(t, 0, established)
	require.Equal(t, 0, pending)
	require.Equal(t, 2, replenishDeficit(established, pending, 2),
		"the whole target must still be dialable; a dead request must not reduce the deficit")

	// The other half of the contract: the request is retained so that Remove
	// can still cancel it. Deleting it outright would free the slot too, but at
	// the cost of the only handle an operator has.
	require.Equal(t, 1, cmgr.pending.Length(), "the request must be retained as the handle Remove needs")

	cmgr.Remove(req.ID())

	require.Eventually(t, func() bool {
		return req.State() == ConnCanceled
	}, 5*time.Second, 10*time.Millisecond, "Remove could not cancel the retained request")
}
