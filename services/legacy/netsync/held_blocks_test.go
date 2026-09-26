package netsync

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// Use the package's existing newTestPark (block_park_test.go:57). It builds a
// park over a REAL file-backed blob store through the production constructor,
// and returns the park plus its directory.
//
// Do NOT hand-roll a blockPark struct literal here. The billing map `charged`
// is written on every admit and a literal that omits it panics on a nil map.
// And the in-memory blob store is not usable at all: its key derivation ignores
// WithSubDirectory and WithNoHashPrefix, which the park always passes, so a
// test writing without those options lands on the same key by accident and
// passes for the wrong reason.
//
// convertedRecordWithSubtrees is the package's existing helper
// (block_park_recover_subtrees_test.go) for building a converted record whose
// hash is its own header hash, which is what ReadConverted checks against.

// TestHoldsBlock_FindsACompleteConvertedRecordOnDisk is the reverse of
// TestHoldsBlock_ExcludesARecordWithAMissingSubtreeFile: a record whose
// subtree file is genuinely present must still answer true. Without this, a
// hasCompleteRecord that returned false unconditionally would also pass the
// missing-subtree test.
func TestHoldsBlock_FindsACompleteConvertedRecordOnDisk(t *testing.T) {
	ctx := context.Background()

	subtreeStoreURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	subtreeStore, err := file.New(ulogger.TestLogger{}, subtreeStoreURL)
	require.NoError(t, err)

	park, _ := newTestPark(t, "")
	sm := &SyncManager{logger: ulogger.TestLogger{}, blockPark: park, subtreeStore: subtreeStore}

	blk, hash := convertedRecordWithSubtrees(t, 1, 100)

	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))
	require.NoError(t, subtreeStore.Set(ctx, blk.Subtrees[0][:], fileformat.FileTypeSubtreeToCheck, []byte("structure")))

	require.False(t, park.Has(hash), "the entry map knows nothing about it, which is the state a restart is in")
	require.True(t, sm.holdsBlock(ctx, hash),
		"a converted record written by the pipeline path, with its subtree file present, must be found from the files, or every pipelined block is downloaded twice")
}

// TestHoldsBlock_ExcludesARecordWithAMissingSubtreeFile is the test that would
// have caught the defect: a record naming a subtree whose structure file is
// NOT on disk. Before this fix, holdsBlock only checked the record's own
// existence, so it answered true for a block this node can never commit —
// nothing had adopted the record (recovery had discarded it, or the file was
// pruned out from under it), so the download pass skipped this height on
// every pass and the chain stopped there for the life of the process.
func TestHoldsBlock_ExcludesARecordWithAMissingSubtreeFile(t *testing.T) {
	ctx := context.Background()

	subtreeStoreURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	subtreeStore, err := file.New(ulogger.TestLogger{}, subtreeStoreURL)
	require.NoError(t, err)

	park, _ := newTestPark(t, "")
	sm := &SyncManager{logger: ulogger.TestLogger{}, blockPark: park, subtreeStore: subtreeStore}

	// A record naming one subtree, but its structure file is never written.
	blk, hash := convertedRecordWithSubtrees(t, 1, 100)
	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))

	require.False(t, sm.holdsBlock(ctx, hash),
		"the record exists but its subtree file does not, so this is not a block the node can commit and must be requested again")
}

func TestHoldsBlock_IsSafeWithNoPark(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}

	require.False(t, sm.holdsBlock(context.Background(), chainhash.Hash{0x04}),
		"no park means nothing is held, which is the safe direction: we re-ask rather than skip")
}

// heldRecord puts a complete converted block on disk for sm, as the pipeline sink leaves one: its
// record in the park's store and its subtree file in the subtree store. It returns the block's hash.
func heldRecord(t *testing.T, sm *SyncManager, seed byte) chainhash.Hash {
	t.Helper()

	if sm.subtreeStore == nil {
		sm.subtreeStore = memory.New()
	}

	hash := parkedRecord(t, sm.blockPark, chainhash.Hash{0x0e, seed}, seed)
	record, err := sm.blockPark.ReadConverted(context.Background(), hash)
	require.NoError(t, err)

	for _, root := range record.Subtrees {
		require.NoError(t, sm.subtreeStore.Set(context.Background(), root[:], fileformat.FileTypeSubtreeToCheck, []byte("structure")))
	}

	return hash
}
