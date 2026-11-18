package backfill

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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
