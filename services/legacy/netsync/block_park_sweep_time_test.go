package netsync

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// slowSweepManager builds a sweep whose own clock jumps a minute every time it
// is read, so the first item of each half of the tick is handled and the budget
// is then spent. That stands in for the real cost, which is not CPU: every
// expiry is a store delete waiting on a write permit from a pool shared with
// subtree and transaction writes, carrying its own ten-second deadline, and
// every lookup is a call to the blockchain service.
func slowSweepManager(t *testing.T, entries int) (*SyncManager, *blockPark, time.Time, *int) {
	t.Helper()

	park, _ := newTestPark(t, "")

	var (
		mu      sync.Mutex
		lookups int
	)

	client := &blockchain2.Mock{}

	// GetBlockHeader, not GetBlockExists: the sweep needs to know whether a
	// parent is usable, and existence alone cannot say, because invalidation is
	// a flag on the row rather than a delete.
	client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) {
			mu.Lock()
			lookups++
			mu.Unlock()
		}).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		ctx:              context.Background(),
		blockchainClient: client,
		blockPark:        park,
	}

	parked := time.Now()

	for i := 0; i < entries; i++ {
		var hash, prev chainhash.Hash

		binary.LittleEndian.PutUint32(hash[:], uint32(i))
		binary.LittleEndian.PutUint32(prev[4:], uint32(i))

		park.entries[hash] = &parkedBlock{hash: hash, prevBlock: prev, parkedAt: parked, height: int32(i + 1)}
		park.children[prev] = append(park.children[prev], hash)
	}

	// Every one of them below the chain's tip, so the eviction half has a full
	// burst to work through. This used to be done by backdating parkedAt past a
	// thirty-minute expiry; nothing expires on a clock any more, and what makes
	// a block droppable is the chain having gone past it.
	sm.noteCommittedHeight(int32(entries + 1))

	reads := 0
	sm.parkSweepNow = func() time.Time {
		reads++

		return parked.Add(time.Duration(reads) * time.Minute)
	}

	return sm, park, parked, &lookups
}

// TestParkSweep_StopsAtItsTimeBudget is the failure the count caps were supposed
// to prevent and could not.
//
// Both halves of the sweep cap how many items they take, and neither caps how
// long they take. Every expiry does a sequential store Del carrying the park's
// store timeout, waiting on the blob store's process-wide write permits, and
// every stuck-candidate lookup is a call to the blockchain service on its own
// deadline. Expiries arrive in bursts, because blocks parked together age out
// together, so one tick could hold the block-commit goroutine for minutes: the
// block queue fills, the outer message loop blocks on it, and disconnects,
// headers, invs and transaction dispatch stall for every peer.
//
// The end state pinned here is that a tick which has spent its budget stops, in
// both halves, whatever its count budget still allows.
func TestParkSweep_StopsAtItsTimeBudget(t *testing.T) {
	const entries = 8

	sm, park, parked, lookups := slowSweepManager(t, entries)

	sm.sweepParkedBlocks(parked.Add(parkStuckThreshold + time.Second))

	require.Equal(t, entries-1, park.Len(),
		"a tick out of time must give up exactly the block it had started on and stop, not work through the whole burst")

	require.Equal(t, 1, *lookups,
		"and the parent lookups beside it must stop at the same budget")
}

// TestParkSweep_KeepsWhatItDidNotReach is the half of the time budget that is
// not free.
//
// blockPark.EvictBelow takes its entries OUT of the index and hands them back,
// so a caller that abandons them abandons the only record of them: the blob
// stays on disk still charged against the park's byte budget with nothing
// tracking it. So the tick puts back what it did not reach, and the next tick,
// where the chain is no further back, carries on.
func TestParkSweep_KeepsWhatItDidNotReach(t *testing.T) {
	const entries = 8

	sm, park, parked, _ := slowSweepManager(t, entries)

	sm.sweepParkedBlocks(parked.Add(parkStuckThreshold + time.Second))
	require.Equal(t, entries-1, park.Len(), "sanity: one gone, the rest put back")

	// Every entry the first tick put back is still expired, still indexed under
	// its parent, and still swept.
	for _, entry := range park.entries {
		require.Equal(t, parked, entry.parkedAt, "a block put back keeps the age it was parked at, or it never expires")
		require.Contains(t, park.children[entry.prevBlock], entry.hash, "a block put back must still be reachable from its parent, or the drain will never find it")
	}

	for tick := 2; tick <= entries; tick++ {
		sm.sweepParkedBlocks(parked.Add(time.Duration(tick) * parkSweepInterval))
	}

	require.Zero(t, park.Len(), "the burst must still drain, one tick's worth at a time")
}

// TestParkSweep_WholeBurstFitsWhenTheTicksAreFast guards the other direction:
// the time budget must not turn an ordinary tick into a slow drip. With the
// store answering promptly, which is every tick that is not in the contended
// state, the whole burst goes in one tick exactly as it did before.
func TestParkSweep_WholeBurstFitsWhenTheTicksAreFast(t *testing.T) {
	const entries = 8

	sm, park, parked, _ := slowSweepManager(t, entries)
	sm.parkSweepNow = nil

	sm.sweepParkedBlocks(parked.Add(parkStuckThreshold + time.Second))

	require.Zero(t, park.Len(), "a tick inside its budget must clear the whole burst")
}
