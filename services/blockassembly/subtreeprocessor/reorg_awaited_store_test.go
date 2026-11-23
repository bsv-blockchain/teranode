package subtreeprocessor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 2b: a fork reorg stores the subtrees it built before it writes any
// longest-chain mark, and announces them only after the marks. With blockassembly_reorgStoreDeadline
// set, a store that fails or does not finish in time fails the reorg before the marks; a reset
// follows (failedForkReorgNeedsReset). With the deadline at 0 the reorg waits as it always did and
// never fails on a store.

// storeRecorder answers store requests and records, for each request, whether it asked to
// announce and how many longest-chain marks had been written when it arrived.
type storeRecorder struct {
	mu        sync.Mutex
	sc        atomic.Pointer[reorgScenario]
	awaited   []int // marks written when each request with a Result arrived
	announced []int // marks written when each request that announces arrived
	answer    func(req NewSubtreeRequest)
}

func (r *storeRecorder) consume(req NewSubtreeRequest) {
	marks := 0
	if sc := r.sc.Load(); sc != nil {
		marks = sc.utxo.markCount(true) + sc.utxo.markCount(false)
	}

	r.mu.Lock()
	if req.Result != nil {
		r.awaited = append(r.awaited, marks)
	}

	if !req.SkipNotification {
		r.announced = append(r.announced, marks)
	}
	r.mu.Unlock()

	r.answer(req)
}

func newAwaitedStoreScenario(t *testing.T, deadline time.Duration, answer func(req NewSubtreeRequest)) (*reorgScenario, *storeRecorder, *phaseLogger) {
	t.Helper()

	r := &storeRecorder{answer: answer}
	cfg := smallScenarioConfig()
	cfg.reorgStoreDeadline = deadline
	cfg.storeConsumer = r.consume

	sc := newReorgScenario(t, cfg)
	r.sc.Store(sc)

	logger := &phaseLogger{}
	sc.stp.logger = logger

	return sc, r, logger
}

// failAwaited answers a store the reorg waits for as failed, and every other one as stored.
func failAwaited(req NewSubtreeRequest) {
	if req.Result != nil {
		req.Result <- StoreResult{Err: errors.NewStorageError("injected store failure")}

		if req.ErrChan != nil {
			req.ErrChan <- nil
		}

		return
	}

	answerStored(req)
}

func TestReorgScenario_AwaitedStore_StoresBeforeTheMarksAndAnnouncesAfter(t *testing.T) {
	sc, r, _ := newAwaitedStoreScenario(t, 5*time.Second, answerStored)

	require.NoError(t, sc.run(t))

	r.mu.Lock()
	defer r.mu.Unlock()

	require.NotEmpty(t, r.awaited, "the reorg waits for the stores of the subtrees it built")

	for _, marks := range r.awaited {
		require.Zero(t, marks, "every awaited store is sent before the first longest-chain mark")
	}

	require.NotEmpty(t, r.announced, "the reorg announces its subtrees")

	for _, marks := range r.announced {
		require.Positive(t, marks, "nothing is announced before the marks")
	}
}

func TestReorgScenario_AwaitedStore_FailedStoreFailsTheReorgBeforeTheMarks(t *testing.T) {
	sc, r, logger := newAwaitedStoreScenario(t, 5*time.Second, failAwaited)

	require.Error(t, sc.run(t))

	require.Zero(t, sc.utxo.markCount(true)+sc.utxo.markCount(false), "no longest-chain mark is written")
	require.True(t, sc.stp.failedForkReorgNeedsReset.Load(), "only a reset may follow")
	require.NotContains(t, logger.phases("reorgBlocks"), "processed_at", "no forward block is marked processed")

	r.mu.Lock()
	defer r.mu.Unlock()

	require.Empty(t, r.announced, "nothing is announced")
}

func TestReorgScenario_AwaitedStore_StoreThatDoesNotFinishInTimeFailsTheReorg(t *testing.T) {
	never := func(req NewSubtreeRequest) {
		if req.Result != nil {
			// the write never settles: answer ErrChan only, as a store whose write hangs does not
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}

			return
		}

		answerStored(req)
	}

	sc, _, _ := newAwaitedStoreScenario(t, 200*time.Millisecond, never)

	err := sc.run(t)
	require.ErrorContains(t, err, "did not finish")
	require.Zero(t, sc.utxo.markCount(true)+sc.utxo.markCount(false))
}

func TestReorgScenario_AwaitedStore_NoDeadlineNeverFailsOnAStore(t *testing.T) {
	sc, r, _ := newAwaitedStoreScenario(t, 0, failAwaited)

	require.NoError(t, sc.run(t), "with the deadline at 0 a failed store does not fail the reorg")
	require.Positive(t, sc.utxo.markCount(true)+sc.utxo.markCount(false))

	r.mu.Lock()
	defer r.mu.Unlock()

	require.Empty(t, r.awaited, "with the deadline at 0 the reorg waits on ErrChan as before")

	for _, marks := range r.announced {
		require.Positive(t, marks, "nothing is announced before the marks")
	}
}
