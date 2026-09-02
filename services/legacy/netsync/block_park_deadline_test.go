package netsync

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// stallingStore is a blob store that never completes a read or a delete of its
// own accord, which is what a store with no free permits looks like from the
// caller's side: file.acquireReadPermit and file.acquireWritePermit both park on
// a semaphore and only give up when the CALLER's context says so, or after
// their own 25 second fallback.
//
// Waiting on ctx.Done() rather than sleeping means the test measures exactly the
// thing that matters — whether the park handed the store a deadline at all.
type stallingStore struct {
	blob.Store
}

func (s stallingStore) GetIoReader(ctx context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (io.ReadCloser, error) {
	<-ctx.Done()

	return nil, errors.NewServiceUnavailableError("[test] no read permit ever came free", ctx.Err())
}

func (s stallingStore) SetFromReader(ctx context.Context, _ []byte, _ fileformat.FileType, _ io.ReadCloser, _ ...options.FileOption) error {
	<-ctx.Done()

	return errors.NewServiceUnavailableError("[test] no write permit ever came free", ctx.Err())
}

func (s stallingStore) Del(ctx context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) error {
	<-ctx.Done()

	return errors.NewServiceUnavailableError("[test] no write permit ever came free", ctx.Err())
}

// newStalledPark builds a park over a store that never answers, with a short
// deadline so the test does not have to wait out a realistic one.
func newStalledPark(timeout time.Duration) *blockPark {
	return &blockPark{
		logger:       ulogger.TestLogger{},
		store:        stallingStore{},
		dir:          "/nonexistent",
		maxBytes:     1 << 30,
		storeTimeout: timeout,
		entries:      make(map[chainhash.Hash]*parkedBlock),
		children:     make(map[chainhash.Hash][]chainhash.Hash),
	}
}

// TestBlockPark_EveryStoreOperationCarriesTheConfiguredDeadline is the whole of
// the setting's promise: it is the ceiling on time the park can spend inside the
// blob store while holding up the single goroutine that commits blocks in order.
//
// The write always carried it. The read-back and the delete did not: both ran on
// sm.ctx, which has no deadline, so each inherited the file store's own 25 second
// permit wait — up to fifty seconds of head-of-line blocking per drained block,
// entirely outside a ceiling the operator had set to ten seconds.
//
// The assertion is that the call RETURNS, not that a particular error comes
// back: an operation that hands the store no deadline never returns at all while
// the store is starved, and that is the defect.
func TestBlockPark_EveryStoreOperationCarriesTheConfiguredDeadline(t *testing.T) {
	const (
		deadline = 250 * time.Millisecond
		// Generous enough that a slow CI box cannot fail this by being slow, and
		// far short of the 25 seconds an undeadlined call would take.
		patience = 8 * time.Second
	)

	hash := chainhash.Hash{0x01}

	t.Run("read", func(t *testing.T) {
		park := newStalledPark(deadline)

		var (
			readErr error
			done    = make(chan struct{})
		)

		go func() {
			defer close(done)

			_, readErr = park.Read(context.Background(), hash)
		}()

		select {
		case <-done:
		case <-time.After(patience):
			t.Fatal("reading a parked block back never gave up: the park handed the store no deadline, so it waits on the store's own 25 second permit timeout while every queued block waits behind it")
		}

		require.Error(t, readErr, "a read that gave up must say so")
	})

	t.Run("delete", func(t *testing.T) {
		park := newStalledPark(deadline)

		done := make(chan struct{})

		go func() {
			defer close(done)

			park.Delete(context.Background(), parkedBlock{hash: hash})
		}()

		select {
		case <-done:
		case <-time.After(patience):
			t.Fatal("deleting a parked blob never gave up: the park handed the store no deadline, so it waits on the store's own 25 second permit timeout while every queued block waits behind it")
		}
	})

	t.Run("write", func(t *testing.T) {
		park := newStalledPark(deadline)

		blocks := minedBlocks(t, 1)
		msgBlock := blocks[0].MsgBlock()

		var (
			result parkResult
			done   = make(chan struct{})
		)

		go func() {
			defer close(done)

			result = park.Park(context.Background(),
				parkedBlock{hash: msgBlock.BlockHash(), prevBlock: msgBlock.Header.PrevBlock}, msgBlock)
		}()

		select {
		case <-done:
		case <-time.After(patience):
			t.Fatal("parking a block never gave up waiting for the store")
		}

		require.Equal(t, parkUnavailable, result, "a write that timed out must leave the block to be downloaded again")
	})
}

// TestBlockPark_ADrainedBlockCannotSpendMoreThanTwoDeadlinesInTheStore is the
// operator-facing version of the same promise. Reading a block back and then
// deleting its blob are the two store operations one drained block makes, and
// they are what the commit goroutine pays for a block that came off disk.
func TestBlockPark_ADrainedBlockCannotSpendMoreThanTwoDeadlinesInTheStore(t *testing.T) {
	const deadline = 250 * time.Millisecond

	park := newStalledPark(deadline)
	entry := parkedBlock{hash: chainhash.Hash{0x02}}

	var wg sync.WaitGroup

	wg.Add(1)

	start := time.Now()

	go func() {
		defer wg.Done()

		_, _ = park.Read(context.Background(), entry.hash)
		park.Delete(context.Background(), entry)
	}()

	waited := make(chan struct{})

	go func() {
		wg.Wait()
		close(waited)
	}()

	select {
	case <-waited:
	case <-time.After(8 * time.Second):
		t.Fatal("a drained block whose store never answers must give up after two deadlines, not two 25 second permit waits")
	}

	// Four deadlines of slack for a loaded CI box, still an order of magnitude
	// below the 50 seconds an undeadlined pair would take.
	require.Less(t, time.Since(start), 4*deadline+5*time.Second,
		"reading back and deleting one parked block must cost at most two configured deadlines")
}
