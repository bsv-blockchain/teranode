package netsync

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/require"
)

// gatedWriteStore holds one blob write open until the test lets it go, so the
// window between registering a parked entry and its bytes reaching the disk can
// be observed from another goroutine. Everything else is delegated.
//
// The park makes exactly one SetFromReader call per block and the file store
// writes its checksum sidecar internally, below this wrapper, so started is
// closed once. The sync.Once is there so that if that ever stops being true the
// test fails on its assertions rather than panicking on a double close.
type gatedWriteStore struct {
	blob.Store

	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (s *gatedWriteStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, r io.ReadCloser, opts ...options.FileOption) error {
	s.once.Do(func() { close(s.started) })

	<-s.release

	return s.Store.SetFromReader(ctx, key, fileType, r, opts...)
}

// parkMidWrite starts a Park that is stopped inside the blob store's write and
// returns as soon as the park has registered the entry.
//
// The caller MUST finish it with finishPark, or the goroutine parking the block
// outlives the test and the serializing goroutine inside blockPark.write
// outlives it too.
func parkMidWrite(t *testing.T) (*blockPark, *gatedWriteStore, chan parkResult, *wire.MsgBlock) {
	t.Helper()

	park, _ := newTestPark(t, "")

	gate := &gatedWriteStore{
		Store:   park.store,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	park.store = gate

	msgBlock := minedBlocks(t, 1)[0].MsgBlock()

	done := make(chan parkResult, 1)

	go func() {
		done <- park.Park(context.Background(),
			parkedBlock{hash: msgBlock.BlockHash(), prevBlock: msgBlock.Header.PrevBlock}, msgBlock)
	}()

	select {
	case <-gate.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the park never reached the blob write")
	}

	require.Equal(t, 1, park.Len(), "the entry must be registered before the write, not after it")

	return park, gate, done, msgBlock
}

// finishPark lets the held write complete and waits for Park to return.
func finishPark(t *testing.T, gate *gatedWriteStore, done chan parkResult) {
	t.Helper()

	close(gate.release)

	select {
	case result := <-done:
		require.Equal(t, parkAccepted, result)
	case <-time.After(10 * time.Second):
		t.Fatal("Park never returned after the write was released")
	}
}

// TestBlockPark_ADrainDuringTheWriteLeavesTheBlockParked is the race the
// register-before-write ordering exists to close.
//
// The entry has to be in children before the bytes are on disk, or a parent that
// commits while a child is mid-write drains a park that does not yet mention the
// child, and the child is only recovered by a later sweep. Registering it early
// is not enough on its own: taken by the drain in that window it would be read
// back off a blob that is not there. So it is registered AND flagged, and the
// flag is what the drain refuses.
//
// Refuses and LEAVES IN PLACE. Taking it out of children would put the entry
// back in exactly the hole the early registration was closing.
func TestBlockPark_ADrainDuringTheWriteLeavesTheBlockParked(t *testing.T) {
	park, gate, done, msgBlock := parkMidWrite(t)

	hash := msgBlock.BlockHash()
	prev := msgBlock.Header.PrevBlock

	require.Empty(t, park.TakeChildren(prev),
		"a drain must not take a block whose bytes are not on disk yet")
	require.True(t, park.Has(hash),
		"and it must be left where it is, or the drain that closed this window reopens it")

	require.Empty(t, park.StuckCandidates(time.Now().Add(parkStuckThreshold+time.Second), 8),
		"the sweep must not spend a parent lookup on a block it would then have to refuse to take")

	_, taken := park.Take(hash)
	require.False(t, taken,
		"Take removes the entry, so taking a block mid-write would strand it: the flag could never be cleared")

	finishPark(t, gate, done)

	drained := park.TakeChildren(prev)
	require.Len(t, drained, 1, "once the write has landed the block is takeable")
	require.True(t, drained[0].hash.IsEqual(&hash))
	require.False(t, drained[0].writing, "the flag is cleared when the write lands")
}

// TestBlockPark_AFailedWriteLeavesTheParkAsItWas checks the rollback that the
// new ordering makes necessary.
//
// The entry, its parent->child edge and its byte charge all go in before the
// write. A write that fails has to undo all three, or the park carries an entry
// with no blob behind it for the life of the process, still holding its bytes
// against the budget.
func TestBlockPark_AFailedWriteLeavesTheParkAsItWas(t *testing.T) {
	park, _ := newTestPark(t, "")
	park.store = failingWriteStore{Store: park.store}

	msgBlock := minedBlocks(t, 1)[0].MsgBlock()

	result := park.Park(context.Background(),
		parkedBlock{hash: msgBlock.BlockHash(), prevBlock: msgBlock.Header.PrevBlock}, msgBlock)

	require.Equal(t, parkUnavailable, result)
	require.Zero(t, park.Len(), "a failed write must not leave an entry behind")
	require.Zero(t, park.Bytes(), "nor its byte charge")
	require.Empty(t, park.TakeChildren(msgBlock.Header.PrevBlock),
		"nor its parent->child edge, which would make the parent drain a block that is not there")
}
