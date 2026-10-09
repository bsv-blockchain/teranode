package daemon

import (
	"fmt"
	"net"
	"syscall"
	"testing"

	teranodeErrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// occupyPort binds and holds a real TCP listener on port so tests can
// deterministically reproduce the getFreePort()/bind TOCTOU collision instead
// of relying on winning a real race against another process.
func occupyPort(t *testing.T, port int) net.Listener {
	t.Helper()

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	require.NoError(t, err, "failed to occupy port %d for the test", port)

	t.Cleanup(func() { _ = l.Close() })

	return l
}

// bindLikeLibp2p mimics the part of the real failure mode we care about:
// something else (here, occupyPort) has already taken the port by the time
// this "later bind" runs, so it fails with the same EADDRINUSE-wrapped error
// shape the real p2p.NewServer -> go-p2p-message-bus -> libp2p.New chain
// produces (see the vendored client.go: `fmt.Errorf("failed to create host: %w", err)`,
// wrapped again by our own `errors.NewServiceError("failed to create p2p client", err)`).
func bindLikeLibp2p(port int) error {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		wrapped := teranodeErrors.NewProcessingError("failed to create host", err)
		return teranodeErrors.NewServiceError("failed to create p2p client", wrapped)
	}

	_ = l.Close()

	return nil
}

// TestIsAddrInUse_MatchesRealBindCollision proves isAddrInUse recognizes the
// actual error chain produced by a real TOCTOU collision (getFreePort found
// the port free; something else took it before the "later" bind), wrapped
// exactly the way our production error chain wraps it.
func TestIsAddrInUse_MatchesRealBindCollision(t *testing.T) {
	port, err := getFreePort()
	require.NoError(t, err)

	occupyPort(t, port) // simulate a parallel process winning the TOCTOU race

	bindErr := bindLikeLibp2p(port)
	require.Error(t, bindErr, "expected the bind to collide with the port occupied above")

	require.True(t, isAddrInUse(bindErr), "isAddrInUse must recognize a real EADDRINUSE collision, got: %v", bindErr)
}

// TestIsAddrInUse_DoesNotMatchUnrelatedErrors proves the detection does not
// widen to swallow genuine, unrelated startup failures - those must still
// fail fast rather than be retried.
func TestIsAddrInUse_DoesNotMatchUnrelatedErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"plain configuration error", teranodeErrors.NewConfigurationError("p2p_port not set in config")},
		{"wrapped unrelated syscall error", teranodeErrors.NewServiceError("failed to create p2p client", teranodeErrors.NewProcessingError("failed to create host", syscall.ECONNREFUSED))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.False(t, isAddrInUse(tt.err), "isAddrInUse must not match: %v", tt.err)
		})
	}
}

// TestAttemptP2PPort_OldPathFails demonstrates the bug being fixed: a single
// getFreePort() draw followed by a later bind, with no retry, fails outright
// when another process has taken the port in between - deterministically
// reproduced here via occupyPort instead of a real race.
func TestAttemptP2PPort_OldPathFails(t *testing.T) {
	port, err := getFreePort()
	require.NoError(t, err)

	occupyPort(t, port)

	err = bindLikeLibp2p(port)
	require.Error(t, err, "the old single-attempt path must fail when the drawn port is already taken")
	require.True(t, isAddrInUse(err))
}

// TestAttemptP2PPort_RetriesOnCollisionAndRecovers proves the fix: when the
// first port attemptP2PPort draws loses the TOCTOU race, it redraws a fresh
// port and recovers instead of failing outright.
//
// The race is reproduced deterministically: on the very first callback
// invocation, before doing the "later bind" (bindLikeLibp2p), the test itself
// occupies the exact port attemptP2PPort just drew - standing in for another
// process winning the window between getFreePort()'s own probe-listener
// Close() and this later bind. Every subsequent attempt is left alone, so it
// must succeed once a fresh, uncontended port is drawn.
func TestAttemptP2PPort_RetriesOnCollisionAndRecovers(t *testing.T) {
	var (
		attempts    int
		portsSeen   []int
		firstPort   int
		succeededOn int
	)

	startErr := attemptP2PPort(maxP2PPortAttempts, func(port int) error {
		attempts++
		portsSeen = append(portsSeen, port)

		if attempts == 1 {
			firstPort = port
			occupyPort(t, port) // simulate another process winning the race for this exact port
		}

		bindErr := bindLikeLibp2p(port)
		if bindErr == nil {
			succeededOn = port
		}

		return bindErr
	})

	require.NoError(t, startErr, "attemptP2PPort must recover once it redraws a free port")
	require.GreaterOrEqual(t, attempts, 2, "must have retried at least once")
	require.NotEqual(t, firstPort, succeededOn, "the successful attempt must use a freshly redrawn port, not the occupied one")

	for _, p := range portsSeen[:len(portsSeen)-1] {
		require.Equal(t, firstPort, p, "every failed attempt before recovery must have collided with the occupied port")
	}
}

// TestAttemptP2PPort_FailsFastOnUnrelatedError proves attemptP2PPort does not
// retry a genuine, unrelated failure - it must return immediately after a
// single attempt so a real bug still fails fast and loudly.
func TestAttemptP2PPort_FailsFastOnUnrelatedError(t *testing.T) {
	wantErr := teranodeErrors.NewConfigurationError("missing config ChainCfgParams.TopicPrefix")

	attempts := 0

	gotErr := attemptP2PPort(maxP2PPortAttempts, func(_ int) error {
		attempts++
		return wantErr
	})

	require.Equal(t, 1, attempts, "an unrelated error must not be retried")
	require.ErrorIs(t, gotErr, wantErr)
}

// TestAttemptP2PPort_GivesUpAfterMaxAttempts proves the retry is bounded: if
// every attempt keeps colliding, attemptP2PPort gives up rather than looping
// forever, and reports the final collision error.
func TestAttemptP2PPort_GivesUpAfterMaxAttempts(t *testing.T) {
	attempts := 0
	collision := teranodeErrors.NewServiceError("failed to create p2p client",
		teranodeErrors.NewProcessingError("failed to create host", syscall.EADDRINUSE))

	gotErr := attemptP2PPort(maxP2PPortAttempts, func(_ int) error {
		attempts++
		return collision
	})

	require.Equal(t, maxP2PPortAttempts, attempts)
	require.True(t, isAddrInUse(gotErr))
}
