package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDecoupleTracingSpan_DetachesFromCancellation pins that a decoupled
// context outlives its caller's cancellation whether or not tracing is
// enabled. Callers rely on it to finish work the caller may abandon, such as
// block validation's updateSubtreesDAH; with tracing disabled the fast path
// used to hand back the caller's own context, so that work was cancelled with
// it, but only on nodes with tracing off.
func TestDecoupleTracingSpan_DetachesFromCancellation(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "tracing disabled"
		if enabled {
			name = "tracing enabled"
		}

		t.Run(name, func(t *testing.T) {
			defer SetTracingEnabled(IsTracingEnabled())

			if enabled {
				require.NoError(t, initTestTracer())
				defer func() { _ = ShutdownTracer(context.Background()) }()
			}

			SetTracingEnabled(enabled)

			type key struct{}

			parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "v"))

			decoupled, _, endFn := DecoupleTracingSpan(parent, "test-service", "decoupled-operation")
			defer endFn()

			cancel()

			require.ErrorIs(t, parent.Err(), context.Canceled)
			require.NoError(t, decoupled.Err(), "the decoupled context must not be cancelled with its caller")

			select {
			case <-decoupled.Done():
				t.Fatal("the decoupled context's Done channel closed with its caller's")
			default:
			}
		})
	}
}

// TestDecoupleTracingSpan_DisabledUncancellableCostsNoMore pins that
// detaching adds no allocation on the disabled fast path for a context that
// can never be cancelled (the validator decouples per transaction from one
// already wrapped in context.WithoutCancel). The fast path's one allocation
// predates the detach.
func TestDecoupleTracingSpan_DisabledUncancellableCostsNoMore(t *testing.T) {
	defer SetTracingEnabled(IsTracingEnabled())
	SetTracingEnabled(false)

	ctx := context.WithoutCancel(context.Background())

	allocs := testing.AllocsPerRun(1000, func() {
		_, _, endFn := DecoupleTracingSpan(ctx, "test-service", "decoupled-operation")
		endFn()
	})

	require.LessOrEqual(t, allocs, 1.0)
}
