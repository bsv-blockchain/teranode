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
// is read, so the tick's budget is spent after its first lookup. That stands in
// for the real cost, which is not CPU: every lookup is a call to the blockchain
// service, on its own deadline, and a contended one is slow rather than free.
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

	reads := 0
	sm.parkSweepNow = func() time.Time {
		reads++

		return parked.Add(time.Duration(reads) * time.Minute)
	}

	return sm, park, parked, &lookups
}

// TestParkSweep_LookupsStopAtTheTimeBudget is the failure the count cap was
// supposed to prevent and could not on its own.
//
// The sweep's lookup half caps how many parents it asks about, and that alone
// does not cap how long it takes: every lookup is a call to the blockchain
// service on its own deadline, and a contended one is slow rather than free. A
// bounded number of them, each allowed several seconds, is still a long tick in
// the worst case, during which nothing newly stuck is looked at and every
// commit this tick has already posted waits behind it.
//
// The end state pinned here is that a tick which has spent its time budget
// stops, whatever its count budget still allows: with a clock that jumps a
// minute on every read, the very first lookup already exhausts
// parkSweepTimeBudget, so no second lookup happens this tick.
func TestParkSweep_LookupsStopAtTheTimeBudget(t *testing.T) {
	const entries = 8

	sm, park, parked, lookups := slowSweepManager(t, entries)

	sm.sweepParkedBlocks(parked.Add(parkStuckThreshold + time.Second))

	require.Equal(t, 1, *lookups,
		"a tick out of time must stop after the lookup it had already started, not work through the whole burst")

	require.Equal(t, entries, park.Len(),
		"StuckCandidates never removes what it hands over, so stopping early must not have lost anything from the index")
}
