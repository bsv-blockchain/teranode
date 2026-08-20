package blockchain

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// TestBlockAssemblyFullNotificationRoundTrip checks that what the producer writes is what the
// consumers read.
//
// Block assembly builds the notification with NewBlockAssemblyFullNotification and both clients
// read it back with blockAssemblyFullFromNotification, so this is the contract that stops the two
// ends drifting apart on the metadata key or its encoding.
func TestBlockAssemblyFullNotificationRoundTrip(t *testing.T) {
	for _, full := range []bool{true, false} {
		notification := NewBlockAssemblyFullNotification(full)

		require.Equal(t, model.NotificationType_BlockAssemblyFull, notification.GetType())
		require.Equal(t, full, blockAssemblyFullFromNotification(notification))
	}
}

// TestBlockAssemblyFullFromNotification checks how the flag is parsed, including the values that
// must fail safe.
//
// A value the node cannot parse has to read as "not full". Failing the other way would refuse
// transactions on a malformed notification, which turns a bad message into an outage.
func TestBlockAssemblyFullFromNotification(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		expected bool
	}{
		{name: "true", metadata: map[string]string{"full": "true"}, expected: true},
		{name: "false", metadata: map[string]string{"full": "false"}, expected: false},
		{name: "parses the alternate bool spellings", metadata: map[string]string{"full": "1"}, expected: true},
		{name: "missing key fails safe", metadata: map[string]string{}, expected: false},
		{name: "unparseable value fails safe", metadata: map[string]string{"full": "yes please"}, expected: false},
		{name: "wrong key fails safe", metadata: map[string]string{"blockAssemblyFull": "true"}, expected: false},
		{name: "nil metadata fails safe", metadata: nil, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notification := &Notification{
				Type:     model.NotificationType_BlockAssemblyFull,
				Metadata: &NotificationMetadata{Metadata: tt.metadata},
			}

			require.Equal(t, tt.expected, blockAssemblyFullFromNotification(notification))
		})
	}
}

// TestClientIsBlockAssemblyFull covers the gRPC Client's copy of the flag.
//
// Every other test in this feature drives a LocalClient, so without this the Client path — the one
// that actually runs in a deployed node — is only covered by the shared parser above. This drives
// the same cached field the subscription handler writes.
func TestClientIsBlockAssemblyFull(t *testing.T) {
	c := &Client{}

	require.False(t, c.IsBlockAssemblyFull(),
		"a client must default to accepting transactions before it hears anything")

	c.blockAssemblyFull.set(blockAssemblyFullFromNotification(NewBlockAssemblyFullNotification(true)), time.Now())
	require.True(t, c.IsBlockAssemblyFull())

	c.blockAssemblyFull.set(blockAssemblyFullFromNotification(NewBlockAssemblyFullNotification(false)), time.Now())
	require.False(t, c.IsBlockAssemblyFull())
}

// fullAt reports the cached decision at an instant, discarding the one-shot expiry signal that only
// the clients act on. The expiry signal has its own subtest below.
func fullAt(s *blockAssemblyFullState, when time.Time) bool {
	full, _ := s.isFull(when)

	return full
}

// TestBlockAssemblyFullStateExpiry checks that a cached refusal lapses when block assembly stops
// re-announcing it.
//
// This is what stops transaction ingress wedging. Block assembly only publishes while it has a limit
// configured, so nothing announces the change when an operator sets
// blockassembly_maxTransactionsInMemory back to 0, or when block assembly stops altogether. Without
// the expiry every ingress point would keep refusing transactions until it was itself restarted.
func TestBlockAssemblyFullStateExpiry(t *testing.T) {
	// A fixed base instant, so the test states the ages it means rather than sleeping.
	base := time.Unix(1_700_000_000, 0)

	t.Run("an expiry reports itself exactly once", func(t *testing.T) {
		var s blockAssemblyFullState

		s.set(true, base)

		full, expired := s.isFull(base.Add(blockAssemblyFullTTL + time.Nanosecond))
		require.False(t, full)
		require.True(t, expired,
			"the call that observes the lapse must report it, so fail-open is not silent")

		// Ingress calls this per transaction. Reporting on every one of them would turn a single
		// lapse into a log flood on a node that is already under pressure.
		for range 100 {
			full, expired = s.isFull(base.Add(time.Hour))
			require.False(t, full)
			require.False(t, expired, "the expiry must be reported once, not once per transaction")
		}

		// A later re-announcement must still take effect: the expiry cleared the cached flag, it did
		// not latch the client into ignoring block assembly.
		s.set(true, base.Add(2*time.Hour))

		full, expired = s.isFull(base.Add(2 * time.Hour))
		require.True(t, full, "a fresh refusal after an expiry must be honoured again")
		require.False(t, expired)
	})

	t.Run("a fresh refusal is honoured", func(t *testing.T) {
		var s blockAssemblyFullState

		s.set(true, base)

		require.True(t, fullAt(&s, base))
		require.True(t, fullAt(&s, base.Add(blockAssemblyFullTTL)),
			"the refusal must still hold at exactly the TTL")
	})

	t.Run("a refusal nobody repeats expires", func(t *testing.T) {
		var s blockAssemblyFullState

		s.set(true, base)

		require.False(t, fullAt(&s, base.Add(blockAssemblyFullTTL+time.Nanosecond)),
			"once block assembly stops announcing, ingress must reopen rather than refuse forever")
		require.False(t, fullAt(&s, base.Add(time.Hour)))
	})

	t.Run("the heartbeat keeps a genuine refusal alive", func(t *testing.T) {
		var s blockAssemblyFullState

		now := base
		s.set(true, now)

		// Block assembly re-announces well inside the TTL, as its heartbeat does every 10s.
		for range 100 {
			now = now.Add(10 * time.Second)
			s.set(true, now)

			require.True(t, fullAt(&s, now),
				"a re-announced refusal must never expire, however long block assembly stays full")
		}
	})

	t.Run("not-full needs no freshness", func(t *testing.T) {
		var s blockAssemblyFullState

		s.set(false, base)

		require.False(t, fullAt(&s, base.Add(time.Hour)))
	})

	t.Run("clearing then re-refusing restarts the clock", func(t *testing.T) {
		var s blockAssemblyFullState

		s.set(true, base)
		s.set(false, base.Add(time.Second))
		s.set(true, base.Add(2*time.Second))

		require.True(t, fullAt(&s, base.Add(2*time.Second+blockAssemblyFullTTL)))
		require.False(t, fullAt(&s, base.Add(2*time.Second+blockAssemblyFullTTL+time.Nanosecond)))
	})

	t.Run("the TTL leaves room for the publisher heartbeat", func(t *testing.T) {
		// blockassembly.txIngressHeartbeatInterval is 10s. The two live in different packages, so
		// this pins the relationship the expiry depends on.
		require.Greater(t, blockAssemblyFullTTL, 10*time.Second,
			"the TTL must exceed block assembly's re-announce period or a full node would flap")
	})
}

// TestBlockAssemblyFullStateReportsTransitions checks the changed result the clients use to decide
// whether to log, so a repeated heartbeat does not produce a log line every time.
func TestBlockAssemblyFullStateReportsTransitions(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)

	var s blockAssemblyFullState

	require.True(t, s.set(true, base), "the first refusal is a transition")
	require.False(t, s.set(true, base.Add(10*time.Second)), "a repeated heartbeat is not a transition")
	require.True(t, s.set(false, base.Add(20*time.Second)), "clearing is a transition")
	require.False(t, s.set(false, base.Add(30*time.Second)), "staying clear is not a transition")
}

// TestLocalClientIsBlockAssemblyFull checks that the LocalClient picks the flag up from the
// notifications passed through SendNotification, and ignores unrelated notification types.
func TestLocalClientIsBlockAssemblyFull(t *testing.T) {
	c := &LocalClient{}

	require.False(t, c.IsBlockAssemblyFull())

	require.NoError(t, c.SendNotification(t.Context(), NewBlockAssemblyFullNotification(true)))
	require.True(t, c.IsBlockAssemblyFull())

	// an unrelated notification must not disturb the flag
	require.NoError(t, c.SendNotification(t.Context(), &Notification{Type: model.NotificationType_Block}))
	require.True(t, c.IsBlockAssemblyFull())

	require.NoError(t, c.SendNotification(t.Context(), NewBlockAssemblyFullNotification(false)))
	require.False(t, c.IsBlockAssemblyFull())
}
