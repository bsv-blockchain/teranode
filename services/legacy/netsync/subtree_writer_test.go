package netsync

import (
	"context"
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// writerFixture builds a writer over a REAL in-memory blob store. Nothing here is
// faked: a passing assertion means bytes are in the store under that key.
func writerFixture(t *testing.T) (*subtreeWriter, *blobmemory.Memory) {
	t.Helper()

	store := blobmemory.New()
	w := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)

	return w, store
}

// TestSubtreeWriter_PutsAllThreeArtefactsInTheStore is the test the previous
// version could not make: it asserts the bytes exist, not that a method was called.
func TestSubtreeWriter_PutsAllThreeArtefactsInTheStore(t *testing.T) {
	ctx := context.Background()
	w, store := writerFixture(t)

	st, data, meta := oneSubtree(t, 8)
	root := st.RootHash()

	require.NoError(t, w.Emit(ctx)(0, st, data, meta))

	for _, ft := range []fileformat.FileType{
		fileformat.FileTypeSubtreeToCheck,
		fileformat.FileTypeSubtreeData,
		fileformat.FileTypeSubtreeMeta,
	} {
		exists, err := store.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.True(t, exists, "%s must be in the store under the subtree's own root hash", ft)
	}
}

// TestSubtreeWriter_StructureFileRoundTrips proves the bytes are the real
// serialisation and not a placeholder: reading them back must reproduce the
// subtree, including its node hashes.
func TestSubtreeWriter_StructureFileRoundTrips(t *testing.T) {
	ctx := context.Background()
	w, store := writerFixture(t)

	st, data, meta := oneSubtree(t, 8)
	root := st.RootHash()

	require.NoError(t, w.Emit(ctx)(0, st, data, meta))

	raw, err := store.Get(ctx, root[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)

	got, err := subtreepkg.NewSubtreeFromBytes(raw)
	require.NoError(t, err)

	require.Equal(t, st.Length(), got.Length(), "leaf count must survive the round trip")
	require.Equal(t, root.String(), got.RootHash().String(), "root hash must survive the round trip")
}

// TestSubtreeWriter_NamesTheStructureToCheck pins the trust claim the file name
// makes. Netsync has validated nothing, so the writer names every structure file
// FileTypeSubtreeToCheck and never FileTypeSubtree, the already-validated name
// that block validation writes after it validates. A writer that wrote the
// other name would make CheckBlockSubtrees skip the subtree's transactions on
// the full route without anything having created them.
func TestSubtreeWriter_NamesTheStructureToCheck(t *testing.T) {
	ctx := context.Background()

	w, store := writerFixture(t)
	st, data, meta := oneSubtree(t, 8)
	require.NoError(t, w.Emit(ctx)(0, st, data, meta))

	toCheck, err := store.Exists(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.True(t, toCheck, "the structure must be marked for checking")

	promoted, err := store.Exists(ctx, st.RootHash()[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, promoted, "netsync must never write the already-validated name")
}

// TestSubtreeWriter_WritesTheStructureLast pins the ordering that makes a crash
// decidable. The structure file is what everything else is found by, so its
// presence has to mean the transactions and inpoints are already whole.
func TestSubtreeWriter_WritesTheStructureLast(t *testing.T) {
	ctx := context.Background()
	w, _ := writerFixture(t)

	st, data, meta := oneSubtree(t, 8)
	require.NoError(t, w.Emit(ctx)(0, st, data, meta))

	written := w.written
	require.Len(t, written, 3)
	require.Equal(t, fileformat.FileTypeSubtreeToCheck, written[2].FileType,
		"the structure file must be written last: its presence is the marker that the other two are complete")
}

// TestSubtreeWriter_DeleteAllRemovesTheFilesFromTheStore is the merkle-failure
// path. Nothing reads these files yet, because no block references them until the
// block is handed over, so a failed root check deletes exactly what it wrote.
func TestSubtreeWriter_DeleteAllRemovesTheFilesFromTheStore(t *testing.T) {
	ctx := context.Background()
	w, store := writerFixture(t)

	st, data, meta := oneSubtree(t, 8)
	root := st.RootHash()

	require.NoError(t, w.Emit(ctx)(0, st, data, meta))
	require.NoError(t, w.DeleteAll(ctx))

	for _, ft := range []fileformat.FileType{
		fileformat.FileTypeSubtreeToCheck,
		fileformat.FileTypeSubtreeData,
		fileformat.FileTypeSubtreeMeta,
	} {
		exists, err := store.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.False(t, exists, "%s must be gone from the store after DeleteAll", ft)
	}

	require.Empty(t, w.written, "and the record of what was written must be cleared")
}

// TestSubtreeWriter_AnAlreadyPresentFileIsNotAnError pins idempotency. Two peers
// can deliver blocks sharing an identical run of transactions, which produces the
// same subtree under the same key; the existing writeSubtree treats that as
// success and so must this.
func TestSubtreeWriter_AnAlreadyPresentFileIsNotAnError(t *testing.T) {
	ctx := context.Background()
	w, _ := writerFixture(t)

	st, data, meta := oneSubtree(t, 8)

	require.NoError(t, w.Emit(ctx)(0, st, data, meta))
	require.NoError(t, w.Emit(ctx)(0, st, data, meta),
		"writing the same subtree twice must succeed, not fail")
}

// oneSubtree builds a single complete subtree with its data and meta, for tests
// that only need something well-formed to hand the writer.
func oneSubtree(t *testing.T, leaves int) (*subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) {
	t.Helper()

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(leaves)
	require.NoError(t, err)

	require.NoError(t, st.AddCoinbaseNode())

	data := subtreepkg.NewSubtreeData(st)
	meta := subtreepkg.NewSubtreeMeta(st)

	for i := 1; i < leaves; i++ {
		tx, hash := streamTx(t, i)

		require.NoError(t, st.AddNode(*hash, 0, uint64(tx.Size())))
		require.NoError(t, data.AddTx(tx, st.Length()-1))
		require.NoError(t, meta.SetTxInpointsFromTx(tx))
	}

	return st, data, meta
}
