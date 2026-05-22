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

// TestCollectMoveForwardTxHashes_BuildsTxSet verifies the carry-over filter's
// tx-extraction helper: every non-coinbase node across every subtree of every
// moveForward block ends up in the returned set, nil/missing entries are
// skipped without erroring, and the set survives an empty input.
func TestCollectMoveForwardTxHashes_BuildsTxSet(t *testing.T) {
	t.Run("nil block list returns empty set, no errors", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		got, err := stp.collectMoveForwardTxHashes(t.Context(), nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})

	t.Run("block with no subtrees contributes nothing", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		got, err := stp.collectMoveForwardTxHashes(t.Context(), []*model.Block{
			{Subtrees: nil},
		})
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("nil block entry is tolerated", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		got, err := stp.collectMoveForwardTxHashes(t.Context(), []*model.Block{nil})
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("nil subtree-hash entry inside block.Subtrees is skipped", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s, txs := buildSubtreeWithTxs(t, "carry-over-nil-subtree-hash", 2)
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s)))

		got, err := stp.collectMoveForwardTxHashes(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{nil, s.RootHash()}},
		})
		require.NoError(t, err)
		for _, txHash := range txs {
			require.Contains(t, got, txHash)
		}
	})

	t.Run("coinbase placeholder is excluded", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		// buildSubtreeWithTxs prepends a coinbase placeholder; it should
		// not appear in the returned tx set.
		s, txs := buildSubtreeWithTxs(t, "carry-over-coinbase", 2)
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s)))

		got, err := stp.collectMoveForwardTxHashes(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
		})
		require.NoError(t, err)
		require.Len(t, got, len(txs), "coinbase placeholder must be filtered out")
		require.NotContains(t, got, subtreepkg.CoinbasePlaceholderHashValue)
		for _, txHash := range txs {
			require.Contains(t, got, txHash)
		}
	})

	t.Run("multiple blocks dedupe per-tx", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s1, txs := buildSubtreeWithTxs(t, "carry-over-dedup", 3)
		s2, txs2 := buildSubtreeWithTxs(t, "carry-over-dedup-2", 2)

		require.NoError(t, store.Set(t.Context(), s1.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s1)))
		require.NoError(t, store.Set(t.Context(), s2.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s2)))

		got, err := stp.collectMoveForwardTxHashes(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s1.RootHash()}},
			{Subtrees: []*chainhash.Hash{s2.RootHash()}},
		})
		require.NoError(t, err)
		require.Len(t, got, len(txs)+len(txs2))
	})

	t.Run("subtreeToCheck fallback used when FileTypeSubtree missing", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s, txs := buildSubtreeWithTxs(t, "carry-over-tocheck", 2)
		// Store ONLY under FileTypeSubtreeToCheck — exercise the fallback
		// path in the helper's GetIoReader chain.
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtreeToCheck, mustSerialize(t, s)))

		got, err := stp.collectMoveForwardTxHashes(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
		})
		require.NoError(t, err)
		for _, txHash := range txs {
			require.Contains(t, got, txHash)
		}
	})

	t.Run("missing subtree surfaces an error", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		phantom := chainhash.HashH([]byte("phantom-subtree"))
		_, err := stp.collectMoveForwardTxHashes(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{&phantom}},
		})
		require.Error(t, err, "absent subtree must fail loudly so the reorg doesn't silently skip carry-over txs")
	})

	t.Run("malformed subtree blob surfaces an error", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		corrupt := chainhash.HashH([]byte("carry-over-corrupt"))
		// Just the file magic and a few bytes; deserialize will fail.
		require.NoError(t, store.Set(t.Context(), corrupt[:], fileformat.FileTypeSubtree, []byte("S-1.0   \x00\x00")))

		_, err := stp.collectMoveForwardTxHashes(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{&corrupt}},
		})
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
