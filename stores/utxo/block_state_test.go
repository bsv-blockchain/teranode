package utxo

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBlockStateHolderSnapshot pins issue 1443: a reader must never observe a
// pair that no single writer published. The writer publishes pairs with the
// fixed relation MedianTime == Height + 1000 via SetPair; concurrent readers
// assert every snapshot satisfies the relation. Held in two independent
// atomics, the pair can tear here — a reader loads the new height, the writer
// advances, and the second load returns the median time of the next tip — so
// the relation breaks. One atomic pointer load cannot produce that.
func TestBlockStateHolderSnapshot(t *testing.T) {
	var holder BlockStateHolder

	holder.SetPair(1, 1001)

	const iterations = 200_000

	var (
		wg        sync.WaitGroup
		tornMu    sync.Mutex
		torn      []BlockState
		samples   atomic.Uint64
		tornFound atomic.Bool
	)

	stop := make(chan struct{})

	for r := 0; r < 4; r++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				got := holder.Load()
				samples.Add(1)

				// Assert after wg.Wait rather than here: testify's FailNow is
				// only valid on the goroutine running the test.
				if got.MedianTime != got.Height+1000 {
					tornMu.Lock()
					torn = append(torn, got)
					tornMu.Unlock()

					tornFound.Store(true)

					return
				}
			}
		}()
	}

	// Gated on the sample count rather than a fixed iteration count, for the
	// reason given in tests.SetBlockStateSnapshotUnderConcurrency: on one core
	// the readers may never be scheduled during a fixed-length writer loop, so
	// a bare count assertion would fail this case on correct code.
	const minSamples = uint64(1_000)

	deadline := time.Now().Add(30 * time.Second)

	for i := uint32(2); ; i++ {
		holder.SetPair(i, i+1000)

		if tornFound.Load() {
			break
		}

		if i >= iterations && samples.Load() >= minSamples {
			break
		}

		if time.Now().After(deadline) {
			break
		}
	}

	close(stop)
	wg.Wait()

	require.Empty(t, torn, "torn snapshots observed: %v", torn)
	// Vacuity guard; see the writer loop above.
	require.NotZero(t, samples.Load(), "readers observed no snapshots at all")
}

// TestBlockStateHolderSingleFieldSetters pins the carry-forward semantics of
// the single-field setters: each preserves the other field, and the zero
// holder loads as the zero BlockState.
func TestBlockStateHolderSingleFieldSetters(t *testing.T) {
	var holder BlockStateHolder

	require.Equal(t, BlockState{}, holder.Load())

	holder.SetHeight(7)
	require.Equal(t, BlockState{Height: 7}, holder.Load())

	holder.SetMedianTime(99)
	require.Equal(t, BlockState{Height: 7, MedianTime: 99}, holder.Load())

	holder.SetHeight(8)
	require.Equal(t, BlockState{Height: 8, MedianTime: 99}, holder.Load())

	holder.SetPair(10, 200)
	require.Equal(t, BlockState{Height: 10, MedianTime: 200}, holder.Load())
}

// TestBlockStateHolderConcurrentSingleFieldSetters pins the CAS carry-forward:
// two goroutines each hammering ONE field must not erase each other's final
// value.
func TestBlockStateHolderConcurrentSingleFieldSetters(t *testing.T) {
	var holder BlockStateHolder

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := uint32(1); i <= 50_000; i++ {
			holder.SetHeight(i)
		}
	}()

	go func() {
		defer wg.Done()

		for i := uint32(1); i <= 50_000; i++ {
			holder.SetMedianTime(i)
		}
	}()

	wg.Wait()

	got := holder.Load()
	require.Equal(t, uint32(50_000), got.Height)
	require.Equal(t, uint32(50_000), got.MedianTime)
}
