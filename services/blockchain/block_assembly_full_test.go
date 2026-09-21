package blockchain

import (
	"sync"
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

	t.Run("a refusal renewed during the expiry check survives it", func(t *testing.T) {
		// The flag and its timestamp are one atomic so that a reader cannot expire a refusal that
		// block assembly renewed while the reader was deciding. Held as two fields, a reader that
		// had already loaded the stale timestamp could still win the clearing CompareAndSwap after
		// the heartbeat had refreshed it, and reopen ingress against a block assembly that is
		// genuinely full.
		//
		// The renewal here stands in for that heartbeat landing mid-decision.
		var s blockAssemblyFullState

		s.set(true, base)

		stale := base.Add(blockAssemblyFullTTL + time.Nanosecond)

		// Block assembly re-announces before the reader commits its verdict.
		s.set(true, stale)

		full, expired := s.isFull(stale)
		require.True(t, full, "a refusal renewed at this instant must be honoured, not expired")
		require.False(t, expired)
	})

	t.Run("a heartbeat racing the expiry never loses the refusal", func(t *testing.T) {
		// The interleaving the packing exists to prevent: a reader loads a timestamp that is past
		// the TTL, block assembly's heartbeat renews the refusal, and only then does the reader
		// commit its clearing swap. Held as two fields that swap wins, because it only checks that
		// the flag is still "full" and not that the reading it judged is still current. Ingress
		// then reopens against a block assembly that never stopped being full, and stays open
		// until the next heartbeat.
		//
		// Packed, the swap is against the exact instant the reader judged, so the renewal makes it
		// fail.
		//
		// The window is only the few nanoseconds a reader spends between loading the timestamp and
		// committing its swap, so this hunts for it rather than arranging it. Measured against the
		// two-field version, one reader in roughly 370 lands inside it; at this iteration count the
		// old code loses a refusal many times over, and missing every one of them is not a
		// probability worth writing down.
		const (
			iterations = 20_000
			readers    = 3
		)

		fresh := base.Add(blockAssemblyFullTTL + time.Nanosecond)

		for range iterations {
			var s blockAssemblyFullState

			// A refusal that is stale by exactly one nanosecond, so every reader below decides to
			// expire it.
			s.set(true, base)

			var wg sync.WaitGroup

			wg.Add(1 + readers)

			// Block assembly's heartbeat.
			go func() {
				defer wg.Done()
				s.set(true, fresh)
			}()

			// Ingress points deciding whether to accept a transaction.
			for range readers {
				go func() {
					defer wg.Done()
					s.isFull(fresh)
				}()
			}

			wg.Wait()

			full, _ := s.isFull(fresh)
			require.True(t, full,
				"the heartbeat renewed the refusal, so no reader may have cleared it")
		}
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
