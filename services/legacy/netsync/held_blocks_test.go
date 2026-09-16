package netsync

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
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

func TestHoldsBlock_FindsAWholeBlockOnDisk(t *testing.T) {
	park, _ := newTestPark(t, "")
	sm := &SyncManager{logger: ulogger.TestLogger{}, blockPark: park}
	hash := chainhash.Hash{0x01}

	require.False(t, sm.holdsBlock(context.Background(), hash),
		"nothing has been written, so nothing is held")

	require.NoError(t, sm.blockPark.store.Set(context.Background(), hash[:], parkFileType, []byte("body"), parkOpts...))

	require.True(t, sm.holdsBlock(context.Background(), hash),
		"a whole block written by the streaming path must be found")
}

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

	require.True(t, sm.holdsBlock(ctx, hash),
		"a converted record written by the pipeline path, with its subtree file present, must be found too, or every pipelined block is downloaded twice")
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

// TestHoldsBlock_DoesNotConsultTheEntryMap is the point of the whole task. The
// park's own Has() reads the in-memory map, which is empty on a restarting node
// before recovery and cannot answer for a block on disk.
func TestHoldsBlock_DoesNotConsultTheEntryMap(t *testing.T) {
	park, _ := newTestPark(t, "")
	sm := &SyncManager{logger: ulogger.TestLogger{}, blockPark: park}
	hash := chainhash.Hash{0x03}

	require.NoError(t, sm.blockPark.store.Set(context.Background(), hash[:], parkFileType, []byte("body"), parkOpts...))

	require.False(t, sm.blockPark.Has(hash),
		"the entry map knows nothing about it, which is the state a restart is in")
	require.True(t, sm.holdsBlock(context.Background(), hash),
		"and the files must answer anyway")
}

func TestHoldsBlock_IsSafeWithNoPark(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}

	require.False(t, sm.holdsBlock(context.Background(), chainhash.Hash{0x04}),
		"no park means nothing is held, which is the safe direction: we re-ask rather than skip")
}
