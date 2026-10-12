package subtreevalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// runLoadProcessPipeline overlaps the read-only LOAD of each batch with the
// PROCESS of the previous batch while keeping PROCESS strictly serial and
// in-order. These tests pin that contract (it is the consensus-safety property
// of the CheckBlockSubtrees batch pipeline) plus the arena-release guarantee on
// the abort paths.

// TestRunLoadProcessPipeline_ProcessesInOrderSerially asserts process is called
// once per batch, in batch order, and never concurrently with itself — even
// though load runs ahead. The injected per-process sleep means an overlapping
// (buggy) implementation would be caught by the concurrency guard.
func TestRunLoadProcessPipeline_ProcessesInOrderSerially(t *testing.T) {
	const numBatches = 6

	var (
		mu        sync.Mutex
		order     []int
		inProcess bool
		overlap   bool
	)

	load := func(_ context.Context, _ int) ([]*bt.Tx, []*bt.Arena, error) {
		return nil, nil, nil
	}

	process := func(idx int, _ []*bt.Tx, _ []*bt.Arena) error {
		mu.Lock()
		if inProcess {
			overlap = true
		}
		inProcess = true
		order = append(order, idx)
		mu.Unlock()

		time.Sleep(5 * time.Millisecond)

		mu.Lock()
		inProcess = false
		mu.Unlock()

		return nil
	}

	err := runLoadProcessPipeline(context.Background(), numBatches, load, process, func([]*bt.Arena) {})
	require.NoError(t, err)

	require.False(t, overlap, "process must never run concurrently with itself")
	require.Equal(t, []int{0, 1, 2, 3, 4, 5}, order, "process must be called in batch order")
}

// TestRunLoadProcessPipeline_ProcessErrorReleasesPendingBatches is the core
// new-risk guard: when process fails on an early batch, every batch the
// producer already loaded ahead must be released exactly once, and the process
// error must propagate. A naive pipeline that drops the load-ahead batches on
// the floor would leak their arenas here.
func TestRunLoadProcessPipeline_ProcessErrorReleasesPendingBatches(t *testing.T) {
	const numBatches = 8

	var (
		mu          sync.Mutex
		loadedCount int
		released    = map[int]int{}
		processed   []int
	)

	wantErr := errors.NewProcessingError("process boom")

	load := func(_ context.Context, idx int) ([]*bt.Tx, []*bt.Arena, error) {
		mu.Lock()
		loadedCount++
		mu.Unlock()
		// One sentinel arena per batch so release can be attributed by identity.
		return nil, []*bt.Arena{bt.NewArena(1)}, nil
	}

	// Track which loaded batch each arena belongs to via a side map keyed on
	// pointer identity captured at load time.
	arenaToBatch := map[*bt.Arena]int{}

	loadTracking := func(ctx context.Context, idx int) ([]*bt.Tx, []*bt.Arena, error) {
		txs, arenas, err := load(ctx, idx)
		mu.Lock()
		for _, a := range arenas {
			arenaToBatch[a] = idx
		}
		mu.Unlock()
		return txs, arenas, err
	}

	process := func(idx int, _ []*bt.Tx, _ []*bt.Arena) error {
		mu.Lock()
		processed = append(processed, idx)
		mu.Unlock()

		if idx == 1 {
			return wantErr
		}
		time.Sleep(2 * time.Millisecond)
		return nil
	}

	release := func(arenas []*bt.Arena) {
		mu.Lock()
		for _, a := range arenas {
			released[arenaToBatch[a]]++
		}
		mu.Unlock()
	}

	err := runLoadProcessPipeline(context.Background(), numBatches, loadTracking, process, release)
	require.ErrorIs(t, err, wantErr)

	mu.Lock()
	defer mu.Unlock()

	// PROCESS must stop at the failing batch: batch 0 succeeds, batch 1 errors,
	// and no later batch is processed. This is the consensus-relevant property —
	// no UTXO mutation may run after a fatal error — and it is what guards the
	// drain check in runLoadProcessPipeline. Asserting only the arena balance
	// below would still pass if the abort guard were removed and batches 2..N
	// were processed (and released) after the error.
	require.Equal(t, []int{0, 1}, processed, "process must stop at the failing batch; no batch may be processed after the error")

	// Every batch the producer loaded must have had its arenas released exactly
	// once — no leak, no double release.
	for b := 0; b < loadedCount; b++ {
		require.Equal(t, 1, released[b], "batch %d arenas must be released exactly once (loaded=%d)", b, loadedCount)
	}
	require.Equal(t, loadedCount, len(released), "released set must equal loaded set")
}

// TestRunLoadProcessPipeline_LoadErrorAborts asserts a LOAD failure aborts the
// run, returns that error, and releases any batches already processed/loaded
// without leaking. loadSubtreeBatch releases its own arenas on failure, so the
// failing batch contributes nil arenas (modelled here by returning nil).
func TestRunLoadProcessPipeline_LoadErrorAborts(t *testing.T) {
	const numBatches = 6

	wantErr := errors.NewStorageError("load boom")

	var (
		mu        sync.Mutex
		processed []int
		releasedN int
		loadCalls int
	)

	load := func(_ context.Context, idx int) ([]*bt.Tx, []*bt.Arena, error) {
		mu.Lock()
		loadCalls++
		mu.Unlock()
		if idx == 3 {
			// Mirror loadSubtreeBatch: it releases its own arenas before
			// returning an error, so the failing batch yields nil arenas.
			return nil, nil, wantErr
		}
		return nil, []*bt.Arena{bt.NewArena(1)}, nil
	}

	process := func(idx int, _ []*bt.Tx, _ []*bt.Arena) error {
		mu.Lock()
		processed = append(processed, idx)
		mu.Unlock()
		return nil
	}

	release := func(arenas []*bt.Arena) {
		mu.Lock()
		releasedN += len(arenas)
		mu.Unlock()
	}

	err := runLoadProcessPipeline(context.Background(), numBatches, load, process, release)
	require.ErrorIs(t, err, wantErr)

	mu.Lock()
	defer mu.Unlock()

	// Batches before the failing one are processed in order; the failing batch
	// and everything after it are not processed.
	require.Equal(t, []int{0, 1, 2}, processed)
	// Every non-failing batch that was loaded contributes one arena; all must
	// be released (processed ones after process, the failing batch none).
	require.Equal(t, countNonFailingLoaded(loadCalls, 3), releasedN)
}

// countNonFailingLoaded returns how many loaded batches carried a (releasable)
// arena given the failing batch index — every loaded batch except the failing
// one contributes exactly one arena.
func countNonFailingLoaded(loadCalls, failIdx int) int {
	n := 0
	for i := 0; i < loadCalls; i++ {
		if i != failIdx {
			n++
		}
	}
	return n
}

// TestRunLoadProcessPipeline_OverlapsLoadWithProcess proves the overlap
// deterministically (no wall-clock thresholds): while process(N) runs, it
// blocks until load(N+1) has started. If LOAD did not run a batch ahead of
// PROCESS this would deadlock on the timeout and the test would fail — so a
// success is positive proof that load(N+1) overlaps process(N).
func TestRunLoadProcessPipeline_OverlapsLoadWithProcess(t *testing.T) {
	const numBatches = 4

	loadStarted := make([]chan struct{}, numBatches)
	for i := range loadStarted {
		loadStarted[i] = make(chan struct{})
	}

	var (
		mu       sync.Mutex
		overlaps int
	)

	load := func(_ context.Context, idx int) ([]*bt.Tx, []*bt.Arena, error) {
		close(loadStarted[idx])
		return nil, nil, nil
	}

	process := func(idx int, _ []*bt.Tx, _ []*bt.Arena) error {
		// While processing batch idx, the producer must already be loading
		// batch idx+1 (one batch ahead). Wait for that load to start; the
		// timeout only guards against a hang if the overlap never happens.
		if idx+1 < numBatches {
			select {
			case <-loadStarted[idx+1]:
				mu.Lock()
				overlaps++
				mu.Unlock()
			case <-time.After(2 * time.Second):
				t.Errorf("load of batch %d did not start during process of batch %d — no overlap", idx+1, idx)
			}
		}

		return nil
	}

	err := runLoadProcessPipeline(context.Background(), numBatches, load, process, func([]*bt.Arena) {})
	require.NoError(t, err)

	require.Equal(t, numBatches-1, overlaps, "every process(N) must overlap load(N+1)")
}

// TestRunLoadProcessPipeline_ProcessPanicReleasesBatch asserts that if process
// panics, the panicking batch's arenas are still released (the release is
// deferred), and the panic propagates to the caller. Without the deferred
// release the arenas would leak from the pool on a process panic.
func TestRunLoadProcessPipeline_ProcessPanicReleasesBatch(t *testing.T) {
	const numBatches = 4

	panicArena := bt.NewArena(1)

	var (
		mu       sync.Mutex
		released = map[*bt.Arena]bool{}
	)

	load := func(_ context.Context, idx int) ([]*bt.Tx, []*bt.Arena, error) {
		if idx == 0 {
			return nil, []*bt.Arena{panicArena}, nil
		}
		return nil, []*bt.Arena{bt.NewArena(1)}, nil
	}

	process := func(idx int, _ []*bt.Tx, _ []*bt.Arena) error {
		if idx == 0 {
			panic("process boom")
		}
		return nil
	}

	release := func(arenas []*bt.Arena) {
		mu.Lock()
		defer mu.Unlock()
		for _, a := range arenas {
			released[a] = true
		}
	}

	require.Panics(t, func() {
		_ = runLoadProcessPipeline(context.Background(), numBatches, load, process, release)
	})

	mu.Lock()
	defer mu.Unlock()
	require.True(t, released[panicArena], "panicking batch's arenas must be released via defer")
}
