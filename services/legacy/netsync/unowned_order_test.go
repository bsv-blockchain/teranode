package netsync

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// The download pass walks about 128 upcoming blocks after every commit and asks, for each, whether
// to request it. Almost every one is already parked or already requested, which two map reads
// answer. The disk check used to run before both, reading each block's record file and statting
// its subtree files: 4.2 s of a 54 s profile on mainnet, on the serial path behind every commit.
// The in-memory checks now run first and the disk is read only for the few blocks they leave.

// countingStore counts the reads the park makes against its blob store.
type countingStore struct {
	blob.Store
	reads atomic.Int32
}

func (c *countingStore) Get(ctx context.Context, key []byte, ft fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	c.reads.Add(1)

	return c.Store.Get(ctx, key, ft, opts...)
}

func (c *countingStore) Exists(ctx context.Context, key []byte, ft fileformat.FileType, opts ...options.FileOption) (bool, error) {
	c.reads.Add(1)

	return c.Store.Exists(ctx, key, ft, opts...)
}

func orderManager(t *testing.T) (*SyncManager, *countingStore) {
	t.Helper()

	park, _ := newTestPark(t, "")
	counting := &countingStore{Store: park.store}
	park.store = counting

	sm := &SyncManager{
		ctx:            context.Background(),
		logger:         ulogger.TestLogger{},
		blockPark:      park,
		blockDownloads: newBlockDownloadTracker(blockRequestAssignmentTTL),
	}

	return sm, counting
}

func TestDownloadPassDoesNotReadTheDiskForAParkedBlock(t *testing.T) {
	sm, counting := orderManager(t)
	hash := chainhash.Hash{0x11}

	require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: hash, prevBlock: chainhash.Hash{0x10}}))

	require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 100, hash: hash}}))
	require.Zero(t, counting.reads.Load(), "the park's own index answers first")
}

func TestDownloadPassDoesNotReadTheDiskForABlockAlreadyRequested(t *testing.T) {
	sm, counting := orderManager(t)
	hash := chainhash.Hash{0x12}

	require.True(t, sm.blockDownloads.Add(newTestPeer(t, "10.0.0.1:8333"), hash))

	require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 100, hash: hash}}))
	require.Zero(t, counting.reads.Load(), "a block a peer was just asked for is skipped from the ledger")
}

// The rule the order must keep: a block already on disk is never handed to another peer. Its owner
// asked long enough ago to be forgiven, and it is not in the park, so only the disk check stands
// between it and a re-request.
func TestDownloadPassNeverRequestsABlockItHoldsOnDisk(t *testing.T) {
	sm, _ := orderManager(t)
	hash := heldRecord(t, sm, 0x13)

	past := time.Now().Add(-2 * blockRequestRetryInterval)
	sm.blockDownloads.now = func() time.Time { return past }
	owner := newTestPeer(t, "10.0.0.1:8333")
	require.True(t, sm.blockDownloads.Add(owner, hash))
	sm.blockDownloads.now = time.Now

	require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 100, hash: hash}}), "held on disk, so not requested")
	require.Equal(t, 1, sm.blockDownloads.CountForPeer(owner), "and its owner is not forgiven on another peer's behalf")
}
