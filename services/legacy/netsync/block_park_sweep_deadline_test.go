package netsync

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestBlockPark_ASlowChainLookupCannotHoldTheSweep is ChiR4.
//
// The sweep asks the blockchain service about one parent per stuck candidate,
// sequentially, up to parkSweepRPCBudget per tick, and it runs on the single
// goroutine that commits blocks in order. The park already states the rule for
// the other half of its I/O: "an undeadlined one is head-of-line blocking for
// every block queued behind it", which is why every store call goes through
// storeCtx. The chain lookups went out on sm.ctx, which carries no deadline, so
// the tick was capped in count and not in time, and time is the quantity that
// rule is about.
//
// The end state pinned here is the tick finishing while the blockchain service
// is still not answering, and every block still parked so the next tick can try
// again. Without a deadline the sweep sits in the first lookup for as long as
// the service takes.
func TestBlockPark_ASlowChainLookupCannotHoldTheSweep(t *testing.T) {
	const (
		candidates   = 3
		lookupBudget = 50 * time.Millisecond
		// Long enough that an undeadlined sweep is unmistakably stuck, short
		// enough that the test does not hang if it is.
		unanswered = 10 * time.Second
	)

	client := &blockchain2.Mock{}
	client.On("GetBlockExists", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			ctx, ok := args.Get(0).(context.Context)
			require.True(t, ok)

			// A blockchain service that never answers. Only the caller's
			// deadline can end this call.
			select {
			case <-ctx.Done():
			case <-time.After(unanswered):
			}
		}).
		Return(false, errors.NewContextCanceledError("the lookup ran out of time"))

	park := newIndexOnlyPark()
	park.storeTimeout = lookupBudget

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		ctx:              context.Background(),
		blockchainClient: client,
		blockPark:        park,
	}

	parked := time.Now()

	for i := 0; i < candidates; i++ {
		var hash, prev chainhash.Hash

		binary.LittleEndian.PutUint32(hash[:], uint32(i))
		binary.LittleEndian.PutUint32(prev[4:], uint32(i))

		park.entries[hash] = &parkedBlock{hash: hash, prevBlock: prev, parkedAt: parked}
	}

	start := time.Now()
	sm.sweepParkedBlocks(parked.Add(parkStuckThreshold + time.Second))
	elapsed := time.Since(start)

	// Every candidate is allowed its own budget, so the tick may cost the sum of
	// them. What it may not do is cost what one unanswered lookup costs.
	require.Less(t, elapsed, unanswered,
		"the sweep held the block-commit goroutine for a lookup that never answered")

	require.Equal(t, candidates, park.Len(),
		"a lookup that ran out of time means could-not-check, so every block stays parked for the next tick")
}
