package netsync

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
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

// newIndexOnlyPark builds a park with nothing but an index, which is all
// StuckCandidates touches. No blobs, no store: the ordering question is purely
// about which entries the sweep hands back.
func newIndexOnlyPark() *blockPark {
	return &blockPark{
		logger:   ulogger.TestLogger{},
		entries:  make(map[chainhash.Hash]*parkedBlock),
		children: make(map[chainhash.Hash][]chainhash.Hash),
	}
}

// TestBlockPark_AFullParkIsAskedAboutBeforeAnyOfItExpires is the sweep's whole
// reason for existing, asserted as what the sweep does rather than as arithmetic
// over the constants that shape it.
//
// The sweep is the ONLY thing that ever commits a parked block whose parent was
// already in the chain when the node started: a block recovered from disk never
// sees a commit event for that parent, so nothing else will ever look at it. It
// gets a bounded number of ticks to work through the whole park, one
// tick every parkSweepInterval, parkSweepRPCBudget parents per tick. If a full
// pass over a full park does not fit in that window, then after a restart with a
// full park the blocks it never reached expire and are downloaded a second time
// — which is the entire cost the park exists to avoid.
//
// It is driven through sweepParkedBlocks, the production entry point, and the
// assertion is made on what actually reached the blockchain service, so it
// covers the round-robin, the per-tick budget and the wiring between them
// together. A sweep that re-sampled the index at random would leave blocks
// unasked about however long it ran.
func TestBlockPark_AFullParkIsAskedAboutBeforeAnyOfItExpires(t *testing.T) {
	var (
		mu   sync.Mutex
		seen = make(map[chainhash.Hash]struct{}, maxParkedEntries)
	)

	client := &blockchain2.Mock{}
	// GetBlockHeader, not GetBlockExists: the sweep needs to know whether a
	// parent is usable, and existence alone cannot say, because invalidation is
	// a flag on the row rather than a delete.
	client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			hash, ok := args.Get(1).(*chainhash.Hash)
			require.True(t, ok)

			mu.Lock()
			seen[*hash] = struct{}{}
			mu.Unlock()
		}).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		ctx:              context.Background(),
		blockchainClient: client,
		blockPark:        newIndexOnlyPark(),
	}

	// A full park, as a restart can find one: every block waiting on a different
	// parent, none of them yet looked at.
	parked := time.Now()

	for i := 0; i < maxParkedEntries; i++ {
		var hash, prev chainhash.Hash

		binary.LittleEndian.PutUint32(hash[:], uint32(i))
		binary.LittleEndian.PutUint32(prev[4:], uint32(i))

		sm.blockPark.entries[hash] = &parkedBlock{hash: hash, prevBlock: prev, parkedAt: parked}
	}

	// The ticks a block gets between becoming a sweep candidate and running out
	// of time. Only as many as a full pass needs are used, so the test says "a
	// full pass fits" rather than "a full pass happens eventually".
	ticks := maxParkedEntries / parkSweepRPCBudget

	require.LessOrEqual(t, time.Duration(ticks)*parkSweepInterval, parkFullPassBudget,
		"a full pass has to fit between a block becoming a candidate and its time running out")

	for tick := 0; tick < ticks; tick++ {
		sm.sweepParkedBlocks(parked.Add(parkStuckThreshold + time.Second + time.Duration(tick)*parkSweepInterval))
	}

	require.Equal(t, maxParkedEntries, sm.blockPark.Len(),
		"nothing may have expired while the sweep was still working through the park")

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, seen, maxParkedEntries,
		"every parked block's parent must have been asked about within %d ticks of %d; whatever the sweep does not reach expires and is downloaded again",
		ticks, parkSweepRPCBudget)
}

// TestBlockPark_ARecoveredBlockKeepsTheAgeItHadBeforeTheRestart closes the
// second half of the same hole. Stamping parkedAt at recovery time would mean a
// node that restarts often never asks about any parent: every recovered block
// would start its parkStuckThreshold wait again, and a parent that turned up
// quietly while the node was down would go unnoticed. The blob's own
// modification time is when the block was parked, and it survives the restart
// because it is on disk.
//
// This used to assert against the thirty-minute expiry, which no longer exists.
// The age still decides something, and it is the lookup gate beside it.
func TestBlockPark_ARecoveredBlockKeepsTheAgeItHadBeforeTheRestart(t *testing.T) {
	park, dir := newTestPark(t, "")

	blocks := minedBlocks(t, 1)
	msgBlock := blocks[0].MsgBlock()
	hash := msgBlock.BlockHash()

	require.Equal(t, parkAccepted,
		park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock))

	// The node ran for a while, then restarted.
	aged := time.Now().Add(-parkStuckThreshold - time.Minute)

	for _, name := range parkDirEntries(t, dir) {
		require.NoError(t, os.Chtimes(filepath.Join(dir, name), aged, aged))
	}

	fresh, _ := newTestPark(t, "")
	fresh.dir = dir
	fresh.store = park.store
	fresh.Recover(context.Background())

	require.Equal(t, 1, fresh.Len(), "the block must be adopted before anything can be asked about it")

	candidates := fresh.StuckCandidates(time.Now(), parkSweepRPCBudget)

	require.Len(t, candidates, 1,
		"a block parked before the restart is due a parent lookup at once, not after starting its wait again")
	require.True(t, candidates[0].hash.IsEqual(&hash))
}

// TestBlockPark_ExpiryIsRatedPerTickLikeTheLookupsBesideIt pins the second of the
// sweep's two caps.
//
// The lookup half was given a per-tick budget with a paragraph of arithmetic
// behind it; the eviction half directly above it had none, and it is the more
// expensive item — each block dropped costs a store delete carrying
// legacy_parkStoreTimeout, on the one goroutine that commits blocks in order. It
// is also the half that arrives in bursts, because the frontier moving past a
// run of parked blocks overtakes all of them at once. An uncapped pass could
// hand the whole index to that goroutine in a single tick, and with blockQueue
// full the outer loop blocks on it and every peer's dispatch stalls behind it.
//
// The asymmetry was the tell, so this asserts the rate rather than the mechanism:
// a park holding more expired blocks than one tick's budget must give up exactly
// the budget, and the remainder must still be there afterwards.
func TestBlockPark_ExpiryIsRatedPerTickLikeTheLookupsBesideIt(t *testing.T) {
	park, _ := newTestPark(t, "")

	// Comfortably more than one tick's worth, and every one of them below the
	// floor, so nothing but the budget decides how many go.
	const parked = parkSweepExpiryBudget + 40

	const floor = int32(parked + 1)

	park.mu.Lock()

	for i := 0; i < parked; i++ {
		var h, prev chainhash.Hash

		binary.LittleEndian.PutUint32(h[:4], uint32(i))
		h[31] = 0xb1
		binary.LittleEndian.PutUint32(prev[:4], uint32(i))
		prev[31] = 0xb2

		entry := &parkedBlock{hash: h, prevBlock: prev, height: int32(i + 1)}
		park.entries[h] = entry
		park.children[prev] = append(park.children[prev], h)
	}

	park.mu.Unlock()

	require.Equal(t, parked, park.Len(), "harness check: every block is parked")

	// Lengths compared as counts, not with require.Len on the slice: a failure
	// there prints every parkedBlock it holds and buries the message.
	first := park.EvictBelow(floor, parkSweepExpiryBudget)
	require.Equal(t, parkSweepExpiryBudget, len(first),
		"one tick must give up exactly its budget, however many are ready")
	require.Equal(t, parked-parkSweepExpiryBudget, park.Len(),
		"the rest must still be parked, to be given up on the next tick")

	// And the remainder is not stranded: the next tick takes what is left.
	second := park.EvictBelow(floor, parkSweepExpiryBudget)
	require.Equal(t, parked-parkSweepExpiryBudget, len(second),
		"the following tick must take the remainder rather than leaving it behind")
	require.Zero(t, park.Len())
}
