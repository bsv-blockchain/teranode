package blockvalidation

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// blockingSubtreeStore stands in for a slow subtree store: the first read parks
// until it is released, which is how a block ends up holding its subtree mutex
// across store I/O the way GetAndValidateSubtrees does in production.
type blockingSubtreeStore struct {
	entered chan struct{}
	release chan struct{}
}

func (s *blockingSubtreeStore) GetIoReader(ctx context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (io.ReadCloser, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}

	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return nil, errors.ErrNotFound
}

// TestLastValidatedBlocksEviction_DoesNotStallTheCache pins the whole point of
// attempting rather than forcing the release on eviction.
//
// expiringmap.clean() holds the map's write lock for its entire sweep and calls
// the eviction function inside it. The eviction function releases a block's
// subtree nodes, which needs that block's subtree mutex — and
// GetAndValidateSubtrees holds that mutex across store reads with retries and
// backoff. If the eviction waits for it, the cleaner parks under the map lock
// and every Get, Set and Delete on lastValidatedBlocks queues behind one block's
// I/O.
//
// So: expire a block that is mid-reload, and require the cache to stay
// answerable. Then free the block and require the entry to be evicted after all,
// because declining must defer the release, never abandon it.
func TestLastValidatedBlocksEviction_DoesNotStallTheCache(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	store := &blockingSubtreeStore{entered: make(chan struct{}, 1), release: make(chan struct{})}

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1,
		Bits:           *nBits,
	}

	subtreeHash := chainhash.HashH([]byte("stalled-subtree"))

	block, err := model.NewBlock(header, coinbase, []*chainhash.Hash{&subtreeHash}, 2, 123, 0, 0)
	require.NoError(t, err)

	// Hold the block's subtree mutex the way a real validation does: inside
	// GetAndValidateSubtrees, parked on a store read.
	reloadDone := make(chan struct{})

	go func() {
		defer close(reloadDone)

		_ = block.GetAndValidateSubtrees(context.Background(), ulogger.TestLogger{}, store,
			tSettings.Block.GetAndValidateSubtreesConcurrency)
	}()

	select {
	case <-store.entered:
	case <-time.After(10 * time.Second):
		close(store.release)
		<-reloadDone
		t.Fatal("the reload never reached the store, so the block never held its mutex")
	}

	const ttl = 200 * time.Millisecond

	evictions := make(chan bool, 16)

	m := expiringmap.New[chainhash.Hash, *model.Block](ttl).
		WithEvictionFunction(func(k chainhash.Hash, b *model.Block) bool {
			// Delegates to the function NewBlockValidation registers, so
			// restoring that to a forced release fails this test rather than
			// leaving it green against a locally built stand-in.
			released := evictLastValidatedBlock(k, b)

			select {
			case evictions <- released:
			default:
			}

			return released
		})
	defer m.Stop()

	blockHash := *block.Hash()
	m.Set(blockHash, block)

	// Wait for the cleaner to have tried this entry at least once while the
	// block is busy.
	select {
	case released := <-evictions:
		require.False(t, released, "sanity: the busy block must have been declined")
	case <-time.After(10 * time.Second):
		close(store.release)
		<-reloadDone
		t.Fatal("the cleaner never attempted the expired entry")
	}

	// The assertion that matters: the cleaner released the map lock instead of
	// waiting on the block, so the cache still answers.
	answered := make(chan struct{})

	go func() {
		defer close(answered)

		_, _ = m.Get(blockHash)
	}()

	select {
	case <-answered:
	case <-time.After(5 * time.Second):
		close(store.release)
		<-reloadDone
		t.Fatal("lastValidatedBlocks stalled: the cleaner is parked under the map lock waiting for a block mid-reload")
	}

	// Presence is checked with Len, not Get: Get reports an entry past its expiry
	// as absent even while it is still in the map, so it cannot distinguish
	// "declined, awaiting the next tick" from "evicted".
	require.Equal(t, 1, m.Len(), "a declined eviction must leave the entry in place for the next tick")

	// Free the block; the deferred release must now go through.
	close(store.release)
	<-reloadDone

	require.Eventually(t, func() bool {
		return m.Len() == 0
	}, 10*time.Second, ttl, "declining must defer the eviction to a later tick, not abandon it")
}
