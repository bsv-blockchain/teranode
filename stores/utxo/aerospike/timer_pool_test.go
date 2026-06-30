package aerospike

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAcquireBatchTimer_Fires verifies a freshly acquired timer fires after the
// requested duration — the leak-guard release path depends on this.
func TestAcquireBatchTimer_Fires(t *testing.T) {
	timer := acquireBatchTimer(20 * time.Millisecond)
	defer releaseBatchTimer(timer)

	select {
	case <-timer.C:
		// fired as expected
	case <-time.After(time.Second):
		t.Fatal("pooled timer did not fire within the timeout")
	}
}

// TestAcquireBatchTimer_Reuse verifies the pool actually recycles a timer
// instead of allocating a fresh one each call.
func TestAcquireBatchTimer_Reuse(t *testing.T) {
	first := acquireBatchTimer(time.Hour)
	releaseBatchTimer(first)

	second := acquireBatchTimer(time.Hour)
	defer releaseBatchTimer(second)

	require.Same(t, first, second, "expected the released timer to be reused from the pool")
}

// TestAcquireBatchTimer_NoStaleTickAfterFire is the safety-critical case: a timer
// that already fired, was released (Stop, not drained), and re-acquired for a long
// deadline must NOT deliver the old tick. A stale tick would make a healthy caller
// spuriously return a "batch did not complete" timeout error.
func TestAcquireBatchTimer_NoStaleTickAfterFire(t *testing.T) {
	expired := acquireBatchTimer(time.Millisecond)

	// Let it fire, but do not drain the channel.
	time.Sleep(20 * time.Millisecond)

	releaseBatchTimer(expired)

	reused := acquireBatchTimer(time.Hour)
	defer releaseBatchTimer(reused)

	select {
	case <-reused.C:
		t.Fatal("recycled timer delivered a stale tick after being reset to a long deadline")
	case <-time.After(50 * time.Millisecond):
		// no stale tick — correct
	}
}

// BenchmarkBatchTimer_Pooled measures the steady-state allocation of an
// acquire/release cycle (the batcher submit-wait pattern). After warm-up it
// should report 0 allocs/op — the whole point of the pool.
func BenchmarkBatchTimer_Pooled(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		t := acquireBatchTimer(time.Hour)
		releaseBatchTimer(t)
	}
}

// BenchmarkBatchTimer_Fresh is the baseline: the previous per-op
// time.NewTimer + Stop pattern, which allocates a timer every call.
func BenchmarkBatchTimer_Fresh(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		t := time.NewTimer(time.Hour)
		t.Stop()
	}
}

// TestAcquireBatchTimer_ResetDeadline verifies a reused timer honours its new,
// shorter deadline (not a leftover long one from a prior use).
func TestAcquireBatchTimer_ResetDeadline(t *testing.T) {
	long := acquireBatchTimer(time.Hour)
	releaseBatchTimer(long)

	short := acquireBatchTimer(20 * time.Millisecond)
	defer releaseBatchTimer(short)

	select {
	case <-short.C:
		// fired at the new short deadline
	case <-time.After(time.Second):
		t.Fatal("recycled timer did not honour its new shorter deadline")
	}
}
