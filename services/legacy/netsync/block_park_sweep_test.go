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
	// A stand-in for "a full park". There is no entry cap any more to size this
	// against, so this is simply a park deep enough that the round-robin has
	// several ticks of real work to do.
	const parkSize = 4096

	var (
		mu   sync.Mutex
		seen = make(map[chainhash.Hash]struct{}, parkSize)
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

	for i := 0; i < parkSize; i++ {
		var hash, prev chainhash.Hash

		binary.LittleEndian.PutUint32(hash[:], uint32(i))
		binary.LittleEndian.PutUint32(prev[4:], uint32(i))

		sm.blockPark.entries[hash] = &parkedBlock{hash: hash, prevBlock: prev, parkedAt: parked}
	}

	// The ticks a block gets between becoming a sweep candidate and running out
	// of time. Only as many as a full pass needs are used, so the test says "a
	// full pass fits" rather than "a full pass happens eventually".
	ticks := parkSize / parkSweepRPCBudget

	require.LessOrEqual(t, time.Duration(ticks)*parkSweepInterval, parkFullPassBudget,
		"a full pass has to fit between a block becoming a candidate and its time running out")

	for tick := 0; tick < ticks; tick++ {
		sm.sweepParkedBlocks(parked.Add(parkStuckThreshold + time.Second + time.Duration(tick)*parkSweepInterval))
	}

	require.Equal(t, parkSize, sm.blockPark.Len(),
		"nothing evicts any more, so the sweep must not have dropped anything either")

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, seen, parkSize,
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
	fresh.Recover(context.Background(), nil)

	require.Equal(t, 1, fresh.Len(), "the block must be adopted before anything can be asked about it")

	candidates := fresh.StuckCandidates(time.Now(), parkSweepRPCBudget)

	require.Len(t, candidates, 1,
		"a block parked before the restart is due a parent lookup at once, not after starting its wait again")
	require.True(t, candidates[0].hash.IsEqual(&hash))
}

// TestParkSweep_AbandonsAParentThatNeverArrives is the fix for the leak fix
// round 1 confirmed: a parked block whose parent is genuinely never coming had
// no reclaim path at all. Not Delete, which needs the parent positively
// adjudicated; not the restart scan, which only discards a blob that will not
// decode; not the store's own retention, which the park disables for its own
// blobs so nothing there can prune it either. Past parkAbandonAfter with the
// parent still absent, the sweep must drop it through the ordinary Delete path
// itself.
func TestParkSweep_AbandonsAParentThatNeverArrives(t *testing.T) {
	park, dir := newTestPark(t, "")

	blocks := minedBlocks(t, 1)
	msgBlock := blocks[0].MsgBlock()
	hash := msgBlock.BlockHash()

	require.Equal(t, parkAccepted,
		park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock))

	// Comfortably past the abandonment window, not merely past the stuck
	// threshold that only gates whether a lookup is made at all.
	park.mu.Lock()
	park.entries[hash].parkedAt = time.Now().Add(-parkAbandonAfter - time.Minute)
	park.mu.Unlock()

	client := &blockchain2.Mock{}
	client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		ctx:              context.Background(),
		blockchainClient: client,
		blockPark:        park,
	}

	sm.sweepParkedBlocks(time.Now())

	require.Zero(t, park.Len(), "a block whose parent never arrives must eventually be reclaimed")
	require.Empty(t, parkDirEntries(t, dir),
		"and its blob must go with it, or the leak just moves from the index onto the disk")
}

// TestParkSweep_DoesNotAbandonAMerelySlowParent is the other half, and the one
// that actually matters: a fix that reclaims orphaned blocks by age is only
// safe if it leaves alone every block that is still going to commit. This is
// the "still syncing" case parkAbandonAfter exists to not catch — well past
// parkStuckThreshold, nowhere near parkAbandonAfter — and it must survive the
// sweep untouched.
func TestParkSweep_DoesNotAbandonAMerelySlowParent(t *testing.T) {
	park, dir := newTestPark(t, "")

	blocks := minedBlocks(t, 1)
	msgBlock := blocks[0].MsgBlock()
	hash := msgBlock.BlockHash()

	require.Equal(t, parkAccepted,
		park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock))

	park.mu.Lock()
	park.entries[hash].parkedAt = time.Now().Add(-parkStuckThreshold - time.Minute)
	park.mu.Unlock()

	client := &blockchain2.Mock{}
	client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		ctx:              context.Background(),
		blockchainClient: client,
		blockPark:        park,
	}

	sm.sweepParkedBlocks(time.Now())

	require.Equal(t, 1, park.Len(), "a block merely waiting on a slow parent must not be dropped")
	require.NotEmpty(t, parkDirEntries(t, dir), "and its blob must still be there for it")
}
