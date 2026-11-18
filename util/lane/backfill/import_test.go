package backfill

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// numberedExample numbers the design example and returns its manifest and the assignment an
// operator would hand the import: the manifest's positions, the existing leased position and the
// coinbases.
func numberedExample(t *testing.T) (Source, *Manifest, MapAssignment) {
	t.Helper()

	src, facts := exampleSource()

	m, err := CreateManifest(filepath.Join(t.TempDir(), "m"), testHeader())
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	_, err = Number(context.Background(), m, src, facts, MemorySeen{})
	require.NoError(t, err)

	assign := MapAssignment{}

	for _, rec := range m.Blocks() {
		ids, err := m.BlockTxIDs(rec.Seq)
		require.NoError(t, err)
		require.NoError(t, assign.AddBlock(rec, ids))
	}

	suffix, first, err := m.SuffixTxIDs()
	require.NoError(t, err)
	require.NoError(t, assign.AddSuffix(first, suffix))

	for txid, p := range facts.existing {
		require.NoError(t, assign.Add(txid, p))
	}

	for _, b := range src.Blocks {
		p, err := CoinbasePosition(b.BlockID)
		require.NoError(t, err)
		require.NoError(t, assign.Add(b.Coinbase, p))
	}

	return src, m, assign
}

func TestImportBlockBuildsRowsAndProofFromTheAssignment(t *testing.T) {
	src, m, assign := numberedExample(t)

	imported, err := ImportBlock(src.Blocks[0], assign)
	require.NoError(t, err)
	require.Equal(t, 4, imported.Members, "A, B, C and the coinbase; the removed transaction has none")

	// Rows: the coinbase lane holds block ID 7, the backfill lane 1,000,001 to 1,000,003 whole.
	require.Equal(t, []lane.MapRow{
		{Lane: lane.CoinbaseLane, First: 7, Last: 7, Tag: lane.RowTagRange},
		{Lane: lane.BackfillLane, First: 1_000_001, Last: 1_000_003, Tag: lane.RowTagRange},
	}, imported.Rows)
	require.NoError(t, lane.CheckMapRows(imported.Rows))

	// The proof file says which subtree holds each transaction.
	require.Equal(t, 0, imported.Proof.SubtreeOf(lane.Pack(lane.BackfillLane, 1_000_001)), "A")
	require.Equal(t, 1, imported.Proof.SubtreeOf(lane.Pack(lane.BackfillLane, 1_000_002)), "B")
	require.Equal(t, 0, imported.Proof.SubtreeOf(lane.Pack(lane.CoinbaseLane, 7)), "the coinbase at the placeholder")

	encoded, err := lane.EncodeProofFile(imported.Proof)
	require.NoError(t, err)
	_, err = lane.DecodeProofFile(encoded)
	require.NoError(t, err)

	ids, err := m.BlockTxIDs(0)
	require.NoError(t, err)
	require.NoError(t, CheckBackfillStretch(imported, m.Blocks()[0], ids, src.Blocks[0]))
}

// A block with a transaction that kept its leased position imports it in its own lane, with a hole
// where the backfill numbering left none.
func TestImportBlockKeepsAnExistingPosition(t *testing.T) {
	src, m, assign := numberedExample(t)

	imported, err := ImportBlock(src.Blocks[1], assign)
	require.NoError(t, err)
	require.Equal(t, 4, imported.Members)

	union := imported.Proof.LaneUnion()
	require.True(t, union[3].Equals(bitmapOf(41_000)))
	require.True(t, union[lane.BackfillLane].Equals(bitmapOf(1_000_004, 1_000_005)))

	ids, err := m.BlockTxIDs(1)
	require.NoError(t, err)
	require.NoError(t, CheckBackfillStretch(imported, m.Blocks()[1], ids, src.Blocks[1]))
}

// A transaction an earlier segment numbered keeps its backfill position; it shares the lane with
// this segment's stretch and is not this record's to check.
func TestCheckBackfillStretchIgnoresAnEarlierSegment(t *testing.T) {
	src, m, assign := numberedExample(t)

	src.Blocks[0].Subtrees[1].TxIDs = append(src.Blocks[0].Subtrees[1].TxIDs, h("earlier"))
	require.NoError(t, assign.Add(h("earlier"), lane.Pack(lane.BackfillLane, 5)))

	imported, err := ImportBlock(src.Blocks[0], assign)
	require.NoError(t, err)
	require.Equal(t, 5, imported.Members)

	ids, err := m.BlockTxIDs(0)
	require.NoError(t, err)
	require.NoError(t, CheckBackfillStretch(imported, m.Blocks()[0], ids, src.Blocks[0]))
}

func TestCheckBackfillStretchCatchesAWrongAssignment(t *testing.T) {
	src, m, assign := numberedExample(t)

	// B and C in different subtrees, with their positions swapped: the stretch is still whole,
	// but each sits in the subtree holding the other's position.
	src.Blocks[0].Subtrees[1].TxIDs = []chainhash.Hash{h("B")}
	src.Blocks[0].Subtrees = append(src.Blocks[0].Subtrees, SourceSubtree{Hash: h("st9"), TxIDs: []chainhash.Hash{h("C")}})
	assign[h("B")], assign[h("C")] = assign[h("C")], assign[h("B")]

	imported, err := ImportBlock(src.Blocks[0], assign)
	require.NoError(t, err)

	ids, err := m.BlockTxIDs(0)
	require.NoError(t, err)
	require.Error(t, CheckBackfillStretch(imported, m.Blocks()[0], ids, src.Blocks[0]))

	// A missing transaction leaves the stretch short.
	src, m, assign = numberedExample(t)
	delete(assign, h("B"))

	imported, err = ImportBlock(src.Blocks[0], assign)
	require.NoError(t, err)

	ids, err = m.BlockTxIDs(0)
	require.NoError(t, err)
	require.Error(t, CheckBackfillStretch(imported, m.Blocks()[0], ids, src.Blocks[0]))
}

func TestImportBlockRefusesSharedPositions(t *testing.T) {
	src, _, assign := numberedExample(t)
	assign[h("C")] = assign[h("A")]

	_, err := ImportBlock(src.Blocks[0], assign)
	require.Error(t, err)

	src, _, assign = numberedExample(t)
	src.Blocks[0].Subtrees[1].TxIDs = append(src.Blocks[0].Subtrees[1].TxIDs, h("A"))

	_, err = ImportBlock(src.Blocks[0], assign)
	require.Error(t, err, "a transaction twice in one block")

	// A transaction the assignment does not hold is still refused when the block lists it twice:
	// gone1 has no position, and appears again in the second subtree.
	src, _, assign = numberedExample(t)
	_, held := assign[h("gone1")]
	require.False(t, held)
	src.Blocks[0].Subtrees[1].TxIDs = append(src.Blocks[0].Subtrees[1].TxIDs, h("gone1"))

	_, err = ImportBlock(src.Blocks[0], assign)
	require.Error(t, err, "an unheld transaction twice in one block")
}

// BSV mainnet has two pairs of blocks whose coinbases are the same transaction. The assignment is
// keyed by txid, so it gives that coinbase one position: the later block's coinbase-lane position.
// The earlier block leaves it out instead of claiming the later block's position, and the later
// block places it.
func TestImportBlockLeavesADuplicateCoinbaseToItsOwnBlock(t *testing.T) {
	shared := h("shared coinbase")

	earlier := SourceBlock{
		BlockID: 91_812, Height: 91_812, Hash: h("block 91812"), Coinbase: shared,
		Subtrees: []SourceSubtree{{Hash: h("st a"), TxIDs: []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue, h("a1")}}},
	}
	later := SourceBlock{
		BlockID: 91_842, Height: 91_842, Hash: h("block 91842"), Coinbase: shared,
		Subtrees: []SourceSubtree{{Hash: h("st b"), TxIDs: []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue, h("b1")}}},
	}

	cb, err := CoinbasePosition(later.BlockID)
	require.NoError(t, err)

	assign := MapAssignment{}
	require.NoError(t, assign.Add(shared, cb))
	require.NoError(t, assign.Add(h("a1"), lane.Pack(lane.BackfillLane, 10)))
	require.NoError(t, assign.Add(h("b1"), lane.Pack(lane.BackfillLane, 11)))

	first, err := ImportBlock(earlier, assign)
	require.NoError(t, err)
	require.Equal(t, 1, first.Members)
	require.Equal(t, []lane.MapRow{{Lane: lane.BackfillLane, First: 10, Last: 10, Tag: lane.RowTagRange}}, first.Rows)

	second, err := ImportBlock(later, assign)
	require.NoError(t, err)
	require.Equal(t, 2, second.Members)
	require.Equal(t, []lane.MapRow{
		{Lane: lane.CoinbaseLane, First: 91_842, Last: 91_842, Tag: lane.RowTagRange},
		{Lane: lane.BackfillLane, First: 11, Last: 11, Tag: lane.RowTagRange},
	}, second.Rows)

	// A coinbase holding a position in another lane, as a live coinbase can, is placed as usual.
	require.NoError(t, assign.Add(h("live cb"), lane.Pack(3, 500)))
	earlier.Coinbase = h("live cb")

	imported, err := ImportBlock(earlier, assign)
	require.NoError(t, err)
	require.Equal(t, 2, imported.Members)
}

// CheckBackfillStretch refuses a record for another block, whichever side names the wrong one.
func TestCheckBackfillStretchRefusesAnotherBlock(t *testing.T) {
	src, m, assign := numberedExample(t)

	imported, err := ImportBlock(src.Blocks[0], assign)
	require.NoError(t, err)

	ids, err := m.BlockTxIDs(0)
	require.NoError(t, err)

	rec := m.Blocks()[0]
	require.NoError(t, CheckBackfillStretch(imported, rec, ids, src.Blocks[0]))

	other := rec
	other.BlockID++
	require.Error(t, CheckBackfillStretch(imported, other, ids, src.Blocks[0]), "the record names another block")

	wrong := imported
	wrong.BlockID++
	require.Error(t, CheckBackfillStretch(wrong, rec, ids, src.Blocks[0]), "the import is of another block")

	moved := src.Blocks[0]
	moved.BlockID++
	require.Error(t, CheckBackfillStretch(imported, rec, ids, moved), "the source block is another block")
}

// A manifest run that would pass the last position of the lane is refused, never wrapped into a
// position Pack cannot hold.
func TestMapAssignmentRefusesARunPastTheLane(t *testing.T) {
	a := MapAssignment{}

	require.Error(t, a.AddSuffix(0, txids(1, 1)), "position 0")
	require.Error(t, a.AddSuffix(lane.MaxPosition, txids(1, 2)), "the second txid would pass the lane")
	require.Error(t, a.AddBlock(lane.ConversionBlock{First: lane.MaxPosition + 1, Count: 1}, txids(1, 1)))

	require.NotPanics(t, func() { _ = a.AddSuffix(lane.MaxPosition-1, txids(2, 3)) })
	require.Empty(t, a, "a refused run adds nothing")

	require.NoError(t, a.AddSuffix(lane.MaxPosition-1, txids(3, 2)))
	require.Len(t, a, 2)
}

func TestMapAssignmentRefusesConflicts(t *testing.T) {
	a := MapAssignment{}
	require.NoError(t, a.Add(h("x"), lane.Pack(1, 5)))
	require.NoError(t, a.Add(h("x"), lane.Pack(1, 5)))
	require.Error(t, a.Add(h("x"), lane.Pack(1, 6)))
	require.Error(t, a.Add(h("y"), lane.Pack(1, 0)))
	require.Error(t, a.AddBlock(lane.ConversionBlock{Count: 2}, txids(1, 1)))

	_, err := CoinbasePosition(0)
	require.Error(t, err)

	p, err := CoinbasePosition(9)
	require.NoError(t, err)
	require.Equal(t, lane.Pack(254, 9), p)
}

func bitmapOf(ps ...uint64) *roaring64.Bitmap {
	bm := roaring64.New()
	bm.AddMany(ps)

	return bm
}
