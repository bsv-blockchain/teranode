package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/stretchr/testify/require"
)

// These tests pin the park's ownership of "handed out". A block taken out of the
// park (Take, TakeChildren, TakeChildrenForProof) is the taker's until the taker
// gives it back (Restore) or settles it (Delete). Before this the park only knew
// whether a hash was in its index, so three in-use marks elsewhere (the
// dispatcher's settling slot, parkCommitting, handedOver) each closed one window
// between a take and a mark, and Restore, which every put-back goes through,
// checked none of them.

// The on-disk handler adopts X and posts it to the consumer. Before the consumer
// reads that post, its drain step takes X and dispatches it. The post is then
// received. The round 2 review's lens 3 reproduced this: Restore put X back in
// the park while the dispatcher held it, a second entry later read after the
// first copy's commit had deleted the record.
func TestBlockPark_RestoreRefusesABlockTheDispatcherHolds(t *testing.T) {
	ctx := context.Background()
	sm, park, subtreeStore := strandedRecordManager(t)
	sm.quit = make(chan struct{})
	sm.drainAsync.Store(true)

	blk, hash := convertedRecordWithSubtrees(t, 1, 650022)
	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))
	require.NoError(t, subtreeStore.Set(ctx, blk.Subtrees[0][:], fileformat.FileTypeSubtreeToCheck, []byte("s")))
	require.NoError(t, subtreeStore.Set(ctx, blk.Subtrees[0][:], fileformat.FileTypeSubtreeData, []byte("d")))

	entry := parkedBlock{hash: hash, prevBlock: *blk.Header.HashPrevBlock, height: 650022, size: 10}
	require.True(t, park.AdoptWritten(entry))

	// The consumer's drain step claims it and dispatches it.
	taken, ok := park.Take(hash)
	require.True(t, ok)
	sm.dispatcher = &blockDispatcher{frontier: []*frontierEntry{{hash: taken.hash}}}

	// The on-disk handler's own hand-off, received now.
	sm.receiveParkCommit(parkCommit{entry: entry, parentHeight: 650021})

	require.False(t, park.Has(hash), "a block the dispatcher holds must not be put back in the park as a second entry")
	require.True(t, sm.blockCommitting(hash), "it is still the dispatcher's")

	// The dispatcher's own put-back, with the claim it took, still works.
	sm.dispatcher = nil
	park.Restore(taken)
	require.True(t, park.Has(hash), "the taker gives it back")
	require.False(t, sm.blockCommitting(hash))
}

// A block taken, committed and deleted must not come back from a put-back that
// was already on its way.
func TestBlockPark_RestoreRefusesABlockWhoseRecordWasDeleted(t *testing.T) {
	ctx := context.Background()
	_, park, _ := strandedRecordManager(t)

	blk, hash := convertedRecordWithSubtrees(t, 1, 650022)
	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))

	entry := parkedBlock{hash: hash, prevBlock: *blk.Header.HashPrevBlock, size: 10}
	require.True(t, park.AdoptWritten(entry))

	taken, ok := park.Take(hash)
	require.True(t, ok)

	park.Delete(ctx, taken)

	park.Restore(taken)
	require.False(t, park.Has(hash), "the taker's own put-back after its delete must be refused")

	park.Restore(entry)
	require.False(t, park.Has(hash), "and so must anyone else's")
	require.Zero(t, park.Bytes(), "nothing is billed for a block that is gone")
}

// Every take marks the hash, and nothing may adopt a handed-out block: not the
// on-disk handler, not the download pass's stranded-record adoption.
func TestBlockPark_AdoptRefusesAHandedOutBlock(t *testing.T) {
	ctx := context.Background()
	sm, park, subtreeStore := strandedRecordManager(t)

	blk, hash := convertedRecordWithSubtrees(t, 1, 650022)
	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))
	require.NoError(t, subtreeStore.Set(ctx, blk.Subtrees[0][:], fileformat.FileTypeSubtreeToCheck, []byte("s")))
	require.NoError(t, subtreeStore.Set(ctx, blk.Subtrees[0][:], fileformat.FileTypeSubtreeData, []byte("d")))
	ageRecord(t, park, hash.String())

	parent := *blk.Header.HashPrevBlock
	entry := parkedBlock{hash: hash, prevBlock: parent, height: 650022, size: 10}

	takes := map[string]func() parkedBlock{
		"Take": func() parkedBlock {
			taken, ok := park.Take(hash)
			require.True(t, ok)

			return taken
		},
		"TakeChildren": func() parkedBlock {
			taken := park.TakeChildren(parent)
			require.Len(t, taken, 1)

			return taken[0]
		},
		"TakeChildrenForProof": func() parkedBlock {
			taken := park.TakeChildrenForProof(parent)
			require.Len(t, taken, 1)

			return taken[0]
		},
	}

	require.True(t, park.AdoptWritten(entry))

	for name, take := range takes {
		taken := take()

		require.True(t, sm.blockCommitting(hash), "%s: a handed-out block is in use", name)
		require.False(t, park.AdoptWritten(entry), "%s: the on-disk handler must not adopt a handed-out block", name)
		require.False(t, park.adoptStranded(ctx, hash, subtreeStore), "%s: the download pass must not adopt a handed-out block", name)
		require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 650022, hash: hash}}), "%s: nor download it again", name)
		require.False(t, park.Has(hash), name)

		// A copy without the claim cannot settle it either: the record stays for
		// the taker.
		park.Delete(ctx, entry)

		exists, err := park.store.Exists(ctx, hash[:], fileformat.FileTypeBlock, parkOpts...)
		require.NoError(t, err)
		require.True(t, exists, "%s: a delete by someone who does not hold the block must leave its record", name)

		park.Restore(taken)
		require.True(t, park.Has(hash), "%s: the taker's put-back ends the hand-out", name)
		require.False(t, sm.blockCommitting(hash), name)
	}
}
