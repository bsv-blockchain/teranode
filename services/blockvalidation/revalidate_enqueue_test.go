package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestEnqueueRevalidation_DoesNotBlockWhenWorkerNeverStarted pins that a re-queue can always
// give up.
//
// enqueueRevalidation guards its blocking send with revalidateWorkerStopped, which is closed by
// the worker's defer — so the guard only works if the worker ran at all. A BlockValidation whose
// worker was never launched leaves that channel open forever, and once the two-slot buffer is
// full the send has no consumer and no escape: the caller's goroutine parks permanently. Every
// header-context failure adds another, unbounded.
//
// NewBlockValidation always launches start(), and start()'s early return is unreachable today
// (processSubtreesNotSet's errgroup closures return nil unconditionally, so g.Wait() cannot
// fail), so this is not a live production leak. It is reachable from any BlockValidation
// assembled directly, which several tests do, and it becomes live the moment either of those two
// facts changes — neither of which is a change anyone would expect to hang a caller.
func TestEnqueueRevalidation_DoesNotBlockWhenWorkerNeverStarted(t *testing.T) {
	bv := &BlockValidation{
		logger: ulogger.TestLogger{},
		// Same shape NewBlockValidation builds: a two-slot channel and an open stop signal.
		// The one thing missing is the worker that would drain it.
		revalidateBlockChan:     make(chan revalidateBlockData, 2),
		revalidateWorkerStopped: make(chan struct{}),
	}

	data := revalidateBlockData{block: testBlockForRequeue(t)}

	// Fill the buffer, so the next send has to block.
	bv.revalidateBlockChan <- data
	bv.revalidateBlockChan <- data

	returned := make(chan struct{})

	go func() {
		defer close(returned)

		bv.enqueueRevalidation(data)
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("enqueueRevalidation parked forever: with no worker to drain the channel and no " +
			"stop signal, the send has neither a consumer nor an escape")
	}

	require.Len(t, bv.revalidateBlockChan, 2,
		"the buffer still holds exactly what was put in it: the third block was dropped, not queued")
}

// TestEnqueueRevalidation_StillQueuesWhenThereIsRoom guards the other direction: the give-up path
// must not swallow a re-queue that the buffer could have taken. ReValidateBlock's whole contract
// is that the block reaches the worker.
func TestEnqueueRevalidation_StillQueuesWhenThereIsRoom(t *testing.T) {
	bv := &BlockValidation{
		logger:                  ulogger.TestLogger{},
		revalidateBlockChan:     make(chan revalidateBlockData, 2),
		revalidateWorkerStopped: make(chan struct{}),
	}

	bv.enqueueRevalidation(revalidateBlockData{block: testBlockForRequeue(t)})

	require.Len(t, bv.revalidateBlockChan, 1, "a re-queue with buffer space must be queued, not dropped")
}

// testBlockForRequeue returns the smallest block enqueueRevalidation can log: it calls
// block.String(), which hashes the header, and BlockHeader.Bytes clones both hashes unguarded.
func testBlockForRequeue(t *testing.T) *model.Block {
	t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *nBits,
		},
	}
}

// TestReValidateBlockFromScratch_CarriesEveryOptionThroughTheRequeue pins the option carry that
// decides what KIND of validation the retry gets.
//
// The end-to-end AddBlock test cannot see this. Flipping disableOptimisticMining in the queued
// data leaves both of its subtests green, because the optimistic branch also reaches AddBlock —
// just earlier, before full validation, which is the branch catchup deliberately opted out of.
// Dropping isRevalidation is likewise invisible there while making every operator reconsider fail
// as "already exists as invalid" and leaving the invalid flag uncleared.
//
// Asserting the queued struct directly is what separates them: it pins the retry's shape at the
// point the decision is made, rather than inferring it from an outcome both shapes produce.
func TestReValidateBlockFromScratch_CarriesEveryOptionThroughTheRequeue(t *testing.T) {
	bv := &BlockValidation{
		logger:                  ulogger.TestLogger{},
		revalidateBlockChan:     make(chan revalidateBlockData, 1),
		revalidateWorkerStopped: make(chan struct{}),
	}

	block := testBlockForRequeue(t)

	// Every option set to the NON-default value, so a field that is rebuilt from the global
	// settings instead of carried shows up as false rather than coincidentally matching.
	bv.ReValidateBlockFromScratch(block, "http://peer1:8000", &ValidateBlockOptions{
		PeerID:                  "peer-123",
		IsRevalidation:          true,
		DisableOptimisticMining: true,
		IsCatchupMode:           true,
	})

	// ReValidateBlockFromScratch waits out the result-grace window on its own goroutine before
	// enqueuing, so the send lands after revalidateFromScratchDelay rather than immediately.
	select {
	case queued := <-bv.revalidateBlockChan:
		require.Equal(t, block, queued.block)
		require.Equal(t, "http://peer1:8000", queued.baseURL)
		require.Equal(t, "peer-123", queued.peerID)

		require.True(t, queued.useFullValidation,
			"the retry must re-enter ValidateBlockWithOptions: reValidateBlock has no AddBlock on any path")
		require.True(t, queued.isRevalidation,
			"an operator reconsider must stay a reconsider, or the retry fails as already-invalid every attempt")
		require.True(t, queued.disableOptimisticMining,
			"a caller that opted out of optimistic mining must not get the AddBlock-before-Valid branch back")
		require.True(t, queued.isCatchupMode,
			"catchup mode must survive the re-queue")

		// The run that failed is deliberately NOT carried: the retry has to re-fetch.
		require.Nil(t, queued.blockHeaders,
			"the cached headers that failed must not be handed back to the retry")
		require.Nil(t, queued.blockHeaderIDs)
		require.Zero(t, queued.retries, "the worker owns the retry budget, not the enqueuer")
	case <-time.After(10 * time.Second):
		t.Fatal("ReValidateBlockFromScratch never enqueued the block")
	}
}
