package subtreeprocessor

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func newReverseHelpersStp(t *testing.T, store interface {
	subtreeStoreAccess
}) *SubtreeProcessor {
	t.Helper()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	newSubtreeChan := make(chan NewSubtreeRequest, 1)

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), store.blob(), nil, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	return stp
}

// subtreeStoreAccess lets the helper above accept either the real
// blob_memory store or the regressionFailingStore wrapper below without
// importing the whole blob.Store interface into the test surface.
type subtreeStoreAccess interface {
	blob() *blob_memory.Memory
}

type plainBlob struct{ s *blob_memory.Memory }

func (p plainBlob) blob() *blob_memory.Memory { return p.s }

// TestFindReMinedInMoveForward verifies the carry-over filter's lookup helper:
// it returns exactly the candidates that appear in a moveForward subtree,
// skips nil/missing entries without erroring, never reports the coinbase
// placeholder, and does no store reads at all when there is nothing to look up.
func TestFindReMinedInMoveForward(t *testing.T) {
	t.Run("nil block list returns empty set, no errors", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		got, err := stp.findReMinedInMoveForward(t.Context(), nil, []chainhash.Hash{chainhash.HashH([]byte("x"))})
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})

	t.Run("no candidates does no store reads", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		// The subtree does not exist, so any read would fail. With no
		// candidates the helper must return before touching the store: this
		// is the common case (a moveBack block with no conflicting nodes) and
		// the moveForward blocks can hold billions of txs.
		phantom := chainhash.HashH([]byte("no-candidates-phantom"))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{&phantom}},
		}, nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})

	t.Run("block with no subtrees contributes nothing", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: nil},
		}, []chainhash.Hash{chainhash.HashH([]byte("x"))})
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("nil block entry is tolerated", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{nil}, []chainhash.Hash{chainhash.HashH([]byte("x"))})
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("returns only the candidates that are re-mined", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s, txs := buildSubtreeWithTxs(t, "carry-over-subset", 4)
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s)))

		notMined := chainhash.HashH([]byte("carry-over-subset-not-mined"))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
		}, []chainhash.Hash{txs[1], notMined, txs[3]})
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Contains(t, got, txs[1])
		require.Contains(t, got, txs[3])
		require.NotContains(t, got, notMined)
		require.NotContains(t, got, txs[0], "txs that were not candidates must not be returned")
	})

	t.Run("nil subtree-hash entry inside block.Subtrees is skipped", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s, txs := buildSubtreeWithTxs(t, "carry-over-nil-subtree-hash", 2)
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s)))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{nil, s.RootHash()}},
		}, txs)
		require.NoError(t, err)
		for _, txHash := range txs {
			require.Contains(t, got, txHash)
		}
	})

	t.Run("coinbase placeholder is never reported", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		// buildSubtreeWithTxs prepends a coinbase placeholder; even when it
		// is asked for, it must not be reported as re-mined.
		s, txs := buildSubtreeWithTxs(t, "carry-over-coinbase", 2)
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s)))

		candidates := append([]chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue}, txs...)

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
		}, candidates)
		require.NoError(t, err)
		require.Len(t, got, len(txs), "coinbase placeholder must be filtered out")
		require.NotContains(t, got, subtreepkg.CoinbasePlaceholderHashValue)
	})

	t.Run("matches across multiple blocks and subtrees", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s1, txs := buildSubtreeWithTxs(t, "carry-over-multi", 3)
		s2, txs2 := buildSubtreeWithTxs(t, "carry-over-multi-2", 2)

		require.NoError(t, store.Set(t.Context(), s1.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s1)))
		require.NoError(t, store.Set(t.Context(), s2.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s2)))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s1.RootHash()}},
			{Subtrees: []*chainhash.Hash{s2.RootHash()}},
		}, []chainhash.Hash{txs[0], txs2[1]})
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Contains(t, got, txs[0])
		require.Contains(t, got, txs2[1])
	})

	t.Run("subtreeToCheck fallback used when FileTypeSubtree missing", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s, txs := buildSubtreeWithTxs(t, "carry-over-tocheck", 2)
		// Store ONLY under FileTypeSubtreeToCheck — exercise the fallback
		// path in the helper's GetIoReader chain.
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtreeToCheck, mustSerialize(t, s)))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
		}, txs)
		require.NoError(t, err)
		for _, txHash := range txs {
			require.Contains(t, got, txHash)
		}
	})

	t.Run("missing subtree surfaces an error", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		phantom := chainhash.HashH([]byte("phantom-subtree"))
		_, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{&phantom}},
		}, []chainhash.Hash{chainhash.HashH([]byte("x"))})
		require.Error(t, err, "absent subtree must fail loudly so the reorg doesn't silently skip carry-over txs")
	})

	t.Run("malformed subtree blob surfaces an error", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		corrupt := chainhash.HashH([]byte("carry-over-corrupt"))
		// Just the file magic and a few bytes; deserialize will fail.
		require.NoError(t, store.Set(t.Context(), corrupt[:], fileformat.FileTypeSubtree, []byte("S-1.0   \x00\x00")))

		_, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{&corrupt}},
		}, []chainhash.Hash{chainhash.HashH([]byte("x"))})
		require.Error(t, err)
	})
}

// TestCloneReverseCascadedSet_RoundTrips checks the per-reorg state surface
// exposed to processConflictingTransactions: nil when no active reorg, an
// independent copy when populated (mutating the copy must not affect the
// processor state).
func TestCloneReverseCascadedSet_RoundTrips(t *testing.T) {
	store := blob_memory.New()
	stp := newReverseHelpersStp(t, plainBlob{store})

	require.Nil(t, stp.cloneReverseCascadedSet(),
		"outside an active reorg the helper must return nil to signal no cascade")

	h1 := chainhash.HashH([]byte("clone-cascade-h1"))
	h2 := chainhash.HashH([]byte("clone-cascade-h2"))

	stp.reverseCascadedConflictingSet = map[chainhash.Hash]struct{}{
		h1: {},
		h2: {},
	}

	got := stp.cloneReverseCascadedSet()
	require.Len(t, got, 2)
	require.Contains(t, got, h1)
	require.Contains(t, got, h2)

	// Mutating the returned copy must not affect the processor state — the
	// downstream filter mutates its own conflictingSet during cascade
	// expansion in dequeueDuringBlockMovement.
	delete(got, h1)
	require.Contains(t, stp.reverseCascadedConflictingSet, h1,
		"clone must be deep enough that caller deletion doesn't leak into reorg state")

	stp.reverseCascadedConflictingSet = nil
	require.Nil(t, stp.cloneReverseCascadedSet(),
		"after reorg exit (set cleared) helper must return nil again")
}

// buildSubtreeWithTxs creates a subtree with `count` deterministic non-
// coinbase tx hashes derived from `seed`, plus a leading coinbase placeholder.
// Returns the subtree and the slice of tx hashes (coinbase excluded) in
// insertion order.
func buildSubtreeWithTxs(t *testing.T, seed string, count int) (*subtreepkg.Subtree, []chainhash.Hash) {
	t.Helper()

	leafCount := 2
	for leafCount < count+1 {
		leafCount *= 2
	}

	s, err := subtreepkg.NewTreeByLeafCount(leafCount)
	require.NoError(t, err)
	require.NoError(t, s.AddCoinbaseNode())

	hashes := make([]chainhash.Hash, count)

	for i := 0; i < count; i++ {
		h := chainhash.HashH([]byte(seed))
		// Mix the index in so multiple txs per seed are distinct.
		for j := 0; j <= i; j++ {
			h = chainhash.HashH(h[:])
		}

		hashes[i] = h
		require.NoError(t, s.AddNode(h, uint64(i+1), 100))
	}

	return s, hashes
}

func mustSerialize(t *testing.T, s *subtreepkg.Subtree) []byte {
	t.Helper()

	b, err := s.Serialize()
	require.NoError(t, err)

	return b
}
