package netsync

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// A converted record can reach disk with nothing ever telling the park about it. On 2026-09-23
// mainnet stopped at height 650,021 for good: the record for 650,022 was written by a peer's
// read loop in the same instant that peer was disconnected, so the on-disk message that admits
// a record to the park was never sent. holdsBlock then said "held" on every download pass, so
// the block was never asked for again, and the drain never offered it because the park did not
// list it. Only a restart's recovery scan adopts such a record. The download pass now adopts it
// itself, so the park sweep commits it once its parent is in the chain.

func strandedRecordManager(t *testing.T) (*SyncManager, *blockPark, *file.File) {
	t.Helper()

	subtreeStoreURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	subtreeStore, err := file.New(ulogger.TestLogger{}, subtreeStoreURL)
	require.NoError(t, err)

	park, _ := newTestPark(t, "")

	sm := &SyncManager{ctx: context.Background(), logger: ulogger.TestLogger{}, blockPark: park, subtreeStore: subtreeStore}

	return sm, park, subtreeStore
}

// ageRecord backdates a record's file past the stranded threshold, which is what a record that
// nothing announced looks like by the time the pass finds it.
func ageRecord(t *testing.T, park *blockPark, blkHash string) {
	t.Helper()

	old := time.Now().Add(-2 * strandedRecordAge)
	require.NoError(t, os.Chtimes(filepath.Join(park.dir, blkHash+"."+string(fileformat.FileTypeBlock)), old, old))
}

func TestDownloadPassAdoptsACompleteRecordThePArkDoesNotList(t *testing.T) {
	ctx := context.Background()
	sm, park, subtreeStore := strandedRecordManager(t)

	blk, hash := convertedRecordWithSubtrees(t, 1, 650022)
	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))
	require.NoError(t, subtreeStore.Set(ctx, blk.Subtrees[0][:], fileformat.FileTypeSubtreeToCheck, []byte("structure")))

	require.False(t, park.Has(hash), "written but never admitted: the state mainnet was stuck in")

	ageRecord(t, park, hash.String())

	unowned := sm.unownedBlocks([]wantedBlock{{height: 650022, hash: hash}})

	require.Empty(t, unowned, "a block whose complete record is on disk is not downloaded again")
	require.True(t, park.Has(hash), "and the park now lists it, so the sweep can commit it")

	child, ok := park.FirstChildFor(*blk.Header.HashPrevBlock)
	require.True(t, ok, "indexed under its parent, which is how the drain finds it")
	require.Equal(t, hash, child.hash)
	require.Equal(t, int32(650022), child.height, "with the height the record carries")
}

func TestDownloadPassDoesNotAdoptARecordMissingASubtreeFile(t *testing.T) {
	ctx := context.Background()
	sm, park, _ := strandedRecordManager(t)

	blk, hash := convertedRecordWithSubtrees(t, 1, 650022)
	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))

	unowned := sm.unownedBlocks([]wantedBlock{{height: 650022, hash: hash}})

	require.Len(t, unowned, 1, "a record that cannot be committed is asked for again")
	require.False(t, park.Has(hash))
}

func TestDownloadPassLeavesAnAlreadyParkedBlockAlone(t *testing.T) {
	ctx := context.Background()
	sm, park, subtreeStore := strandedRecordManager(t)

	blk, hash := convertedRecordWithSubtrees(t, 1, 650022)
	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))
	require.NoError(t, subtreeStore.Set(ctx, blk.Subtrees[0][:], fileformat.FileTypeSubtreeToCheck, []byte("structure")))

	require.True(t, park.AdoptWritten(parkedBlock{hash: hash, prevBlock: *blk.Header.HashPrevBlock}))

	require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 650022, hash: hash}}))
	require.True(t, park.Has(hash))
}

// A record written a moment ago is not stranded: its announcement is on its way, and adopting it
// first makes the announcement find it already parked and skip scheduling the drain. The first
// deploy of this fix adopted records like that every few seconds.
func TestDownloadPassLeavesAFreshRecordToItsAnnouncement(t *testing.T) {
	ctx := context.Background()
	sm, park, subtreeStore := strandedRecordManager(t)

	blk, hash := convertedRecordWithSubtrees(t, 1, 650022)
	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))
	require.NoError(t, subtreeStore.Set(ctx, blk.Subtrees[0][:], fileformat.FileTypeSubtreeToCheck, []byte("structure")))

	require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 650022, hash: hash}}), "held, so not downloaded again")
	require.False(t, park.Has(hash), "but not adopted: it is not stranded yet")
}

// A block being committed has been taken out of the park but its record is still on disk. It is
// in flight, not stranded. The first deploy of this fix put such blocks back in the park, where
// they were read back after their files had gone.
func TestDownloadPassLeavesABlockInFlightAlone(t *testing.T) {
	ctx := context.Background()
	sm, park, subtreeStore := strandedRecordManager(t)

	blk, hash := convertedRecordWithSubtrees(t, 1, 650022)
	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))
	require.NoError(t, subtreeStore.Set(ctx, blk.Subtrees[0][:], fileformat.FileTypeSubtreeToCheck, []byte("structure")))
	ageRecord(t, park, hash.String())

	sm.dispatcher = &blockDispatcher{frontier: []*frontierEntry{{hash: hash, height: 650022}}}

	require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 650022, hash: hash}}))
	require.False(t, park.Has(hash), "a block being committed is not put back in the park")
}
