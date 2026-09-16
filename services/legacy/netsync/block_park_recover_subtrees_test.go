package netsync

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestRecover_ChecksEverySubtreeNotJustTheFirst pins the gap the code's own
// comment admits. A record naming three subtrees whose second file is gone was
// adopted, then committed, then failed inside validation, landing on the same
// destructive path a genuinely bad block does.
func TestRecover_ChecksEverySubtreeNotJustTheFirst(t *testing.T) {
	ctx := context.Background()

	subtreeStoreURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	subtreeStore, err := file.New(ulogger.TestLogger{}, subtreeStoreURL)
	require.NoError(t, err)

	park, _ := newTestPark(t, "")

	blk, hash := convertedRecordWithSubtrees(t, 3, 100)

	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))

	// Write the FIRST and THIRD subtree structure files, leaving the second
	// missing. A check that stops at Subtrees[0] sees a complete record.
	for i, st := range blk.Subtrees {
		if i == 1 {
			continue
		}

		require.NoError(t, subtreeStore.Set(ctx, st[:], fileformat.FileTypeSubtree, []byte("structure")))
	}

	park.Recover(ctx, subtreeStore)

	require.False(t, park.Has(hash),
		"a record whose second subtree file is gone must be discarded, not adopted")
}

// TestRecover_KeepsTheHeightItAlreadyRead pins a leak rather than a crash. The
// record carries its height, recovery reads it for the quick-validation test,
// and then drops it, so every recovered entry is skipped by eviction for ever
// and its slot never comes back.
func TestRecover_KeepsTheHeightItAlreadyRead(t *testing.T) {
	ctx := context.Background()

	subtreeStoreURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	subtreeStore, err := file.New(ulogger.TestLogger{}, subtreeStoreURL)
	require.NoError(t, err)

	park, _ := newTestPark(t, "")

	blk, hash := convertedRecordWithSubtrees(t, 1, 424242)

	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))
	require.NoError(t, subtreeStore.Set(ctx, blk.Subtrees[0][:], fileformat.FileTypeSubtree, []byte("structure")))

	park.Recover(ctx, subtreeStore)

	park.mu.Lock()
	entry, ok := park.entries[hash]
	park.mu.Unlock()

	require.True(t, ok, "the record must have been adopted")
	require.Equal(t, int32(424242), entry.height,
		"the height is on disk and was already read; dropping it leaks the entry's slot across every restart")
}

// convertedRecordWithSubtrees builds a converted record naming n subtree
// hashes, and returns it with the hash it MUST be stored under.
//
// That hash is the header's own, not an arbitrary one. Recovery reads the
// record back through ReadConverted, which checks the decoded header's hash
// against the key it was asked for, so a record filed under any other key is
// discarded as unusable and the test would pass for the wrong reason.
func convertedRecordWithSubtrees(t *testing.T, n int, height uint32) (*model.Block, chainhash.Hash) {
	t.Helper()

	subtrees := make([]*chainhash.Hash, 0, n)

	for i := 0; i < n; i++ {
		h := chainhash.Hash{0x50, byte(i)}
		subtrees = append(subtrees, &h)
	}

	// Vary the merkle root by n so two fixtures in one test cannot collide.
	header := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{byte(n)},
	}

	blk, err := model.NewBlock(header, coinbaseTx(t), subtrees, uint64(n), 0, height, 0)
	require.NoError(t, err)

	return blk, *blk.Header.Hash()
}
