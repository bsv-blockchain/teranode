package blockchain

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
)

// Test_SendFSMEvent_ConcurrentTransitions_NoRace hammers SendFSMEvent from many
// goroutines, reproducing the production pattern where Run and CatchUpBlocks
// arrive as separate concurrent gRPC requests. Before SendFSMEvent was
// serialised, its read-modify-write of stateChangeTimestamp (and the FSM
// transition) was a data race; this test fails under -race if that regresses.
//
// Regtest params are used so the RUN gate (no hard-coded checkpoints) is a
// no-op and no store reads are required. Invalid transitions from the current
// shared state return an error and are ignored — the point is the concurrent
// access to stateChangeTimestamp on the successful transitions.
func Test_SendFSMEvent_ConcurrentTransitions_NoRace(t *testing.T) {
	store := &fsmGateStore{}
	b := newTestBlockchainForGate(t, &chaincfg.RegressionNetParams, store)

	// A small non-zero delay widens the read->write window on stateChangeTimestamp
	// so the race is reliably detected when serialisation is absent.
	b.settings.BlockChain.FSMStateChangeDelay = time.Millisecond
	b.finiteStateMachine = b.NewFiniteStateMachine()

	ctx := context.Background()
	run := &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN}
	stop := &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_STOP}

	const goroutines, iterations = 8, 40

	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				_, _ = b.SendFSMEvent(ctx, run)  // IDLE -> RUNNING (when valid)
				_, _ = b.SendFSMEvent(ctx, stop) // RUNNING -> IDLE (when valid)
			}
		}()
	}

	wg.Wait()

	// Reaching here cleanly under -race is the real assertion; sanity-check the
	// FSM still holds a valid terminal state for these two events.
	require.Contains(t, []string{
		blockchain_api.FSMStateType_IDLE.String(),
		blockchain_api.FSMStateType_RUNNING.String(),
	}, b.finiteStateMachine.Current())
}
