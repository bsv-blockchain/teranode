package blockvalidation

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReloadSubtreesForInvalidBlock_IsRaceFreeAgainstConcurrentRelease pins the
// synchronisation on the invalid-block reload.
//
// The reload replaced the block's SubtreeSlices header and then filled its
// entries with no lock held, while the lastValidatedBlocks TTL cleaner runs
// ReleaseSubtreeNodes on the same block under the block's subtree mutex — the
// block stays reachable in that cache between the Get and the Delete around
// this call. Taking the mutex on one side only is no synchronisation at all.
//
// The consequence is not academic: entries the reload has just filled get nil-ed
// underneath it, UpdateTxMinedStatus then reports "missing subtree %d of %d",
// and the invalid block's transactions are never un-mined — leaving stale mined
// flags in the UTXO store.
//
// Run under -race, which is what fails this test if either side stops
// synchronising. Only the absence of a race is asserted: which of the two
// writers lands last is legitimately unspecified.
func TestReloadSubtreesForInvalidBlock_IsRaceFreeAgainstConcurrentRelease(t *testing.T) {
	subtreeStore := blobmemory.New()

	hashes := make([]*chainhash.Hash, 0, 2)

	for _, label := range []string{"race-a", "race-b"} {
		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)

		if label == "race-a" {
			require.NoError(t, st.AddCoinbaseNode())
			require.NoError(t, st.AddNode(chainhash.HashH([]byte(label)), 1, 0))
		} else {
			require.NoError(t, st.AddNode(chainhash.HashH([]byte(label+"-0")), 1, 0))
			require.NoError(t, st.AddNode(chainhash.HashH([]byte(label+"-1")), 1, 0))
		}

		serialized, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(context.Background(), st.RootHash()[:], fileformat.FileTypeSubtree, serialized))

		hashes = append(hashes, st.RootHash())
	}

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

	block, err := model.NewBlock(header, coinbase, hashes, 4, 123, 0, 0)
	require.NoError(t, err)

	u := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: subtreeStore}

	ctx := context.Background()

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < 50; i++ {
			// assert, not require: require's FailNow must run on the test goroutine.
			assert.NoError(t, u.reloadSubtreesForInvalidBlock(ctx, block))
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < 50; i++ {
			// The TTL cleaner's eviction, which takes the block's subtree mutex.
			_ = block.ReleaseSubtreeNodes(nil)
		}
	}()

	wg.Wait()
}

// failingGetStore fails every Get with err, standing in for a blob backend
// that cannot be reached. When onlyType is set, only Gets of that file type
// fail with err and every other Get falls through to the wrapped store.
// When onlyKey is set, only Gets of that key fail.
type failingGetStore struct {
	blob.Store

	err      error
	onlyType fileformat.FileType
	onlyKey  []byte
}

func (s *failingGetStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	if s.onlyType != "" && fileType != s.onlyType {
		return s.Store.Get(ctx, key, fileType, opts...)
	}

	if s.onlyKey != nil && !bytes.Equal(key, s.onlyKey) {
		return s.Store.Get(ctx, key, fileType, opts...)
	}

	return nil, s.err
}

// TestReloadSubtreesForInvalidBlock_FailsOnReadErrorSkipsMissing pins how the
// revert of an invalid block treats a subtree it cannot load (GAP-BV-057).
//
// A subtree absent from the store is skipped: no retry can bring it back, and
// failing on it would keep the block in the mined-not-set sweep forever. Any
// other read failure may be transient, so it is returned and the revert is
// retried rather than finished with that subtree's tags still in place.
func TestReloadSubtreesForInvalidBlock_FailsOnReadErrorSkipsMissing(t *testing.T) {
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

	subtreeHash := chainhash.HashH([]byte("absent-subtree"))

	block, err := model.NewBlock(header, coinbase, []*chainhash.Hash{&subtreeHash}, 2, 123, 0, 0)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("missing subtree is skipped", func(t *testing.T) {
		u := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: blobmemory.New()}

		require.NoError(t, u.reloadSubtreesForInvalidBlock(ctx, block))
		require.Len(t, block.SubtreeSlices, 1)
		require.Nil(t, block.SubtreeSlices[0])
	})

	t.Run("read failure is returned", func(t *testing.T) {
		readErr := errors.NewStorageError("subtree store unreachable")
		u := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: &failingGetStore{Store: blobmemory.New(), err: readErr}}

		err := u.reloadSubtreesForInvalidBlock(ctx, block)
		require.Error(t, err)
		require.ErrorIs(t, err, readErr)
	})

	t.Run("read failure on the subtree-to-check fallback is returned", func(t *testing.T) {
		// The subtree file is absent, so the reload falls back to the
		// subtree-to-check file, and that read fails with a non-not-found error.
		readErr := errors.NewStorageError("subtree-to-check read failed")
		store := &failingGetStore{Store: blobmemory.New(), err: readErr, onlyType: fileformat.FileTypeSubtreeToCheck}
		u := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: store}

		err := u.reloadSubtreesForInvalidBlock(ctx, block)
		require.Error(t, err)
		require.ErrorIs(t, err, readErr)
	})

	t.Run("storage error wrapping a not-found is returned, not skipped", func(t *testing.T) {
		// errors.Is walks the chain, so this matches ErrNotFound too. It is
		// still the store failing while it looked, not a missing subtree.
		readErr := errors.NewStorageError("subtree read failed", errors.ErrNotFound)
		u := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: &failingGetStore{Store: blobmemory.New(), err: readErr}}

		err := u.reloadSubtreesForInvalidBlock(ctx, block)
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})
}

// TestReloadSubtreesForInvalidBlock_ClosesLoadedSubtreesOnReadError pins that a
// read failure part-way through the reload closes the subtrees already loaded.
// They never reach the block, and an mmap-backed subtree has no finalizer, so
// without the close each retry of the revert leaves a temp backing file and its
// mapping behind until process exit. The mmap directory is the observable: it
// must be empty once the reload has returned its error.
func TestReloadSubtreesForInvalidBlock_ClosesLoadedSubtreesOnReadError(t *testing.T) {
	ctx := context.Background()
	subtreeStore := blobmemory.New()

	hashes := make([]*chainhash.Hash, 0, 2)

	for _, label := range []string{"loaded-first", "fails-second"} {
		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, st.AddNode(chainhash.HashH([]byte(label+"-0")), 1, 0))
		require.NoError(t, st.AddNode(chainhash.HashH([]byte(label+"-1")), 1, 0))

		serialized, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtree, serialized))

		hashes = append(hashes, st.RootHash())
	}

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

	block, err := model.NewBlock(header, coinbase, hashes, 4, 123, 0, 0)
	require.NoError(t, err)

	mmapDir := t.TempDir()
	readErr := errors.NewStorageError("subtree store unreachable")

	u := &BlockValidation{
		logger:       ulogger.TestLogger{},
		subtreeStore: &failingGetStore{Store: subtreeStore, err: readErr, onlyKey: hashes[1][:]},
		mmapDir:      mmapDir,
	}

	err = u.reloadSubtreesForInvalidBlock(ctx, block)
	require.ErrorIs(t, err, readErr)

	entries, err := os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.Empty(t, entries, "the subtree loaded before the read failure must be closed, removing its mmap backing file")
}
