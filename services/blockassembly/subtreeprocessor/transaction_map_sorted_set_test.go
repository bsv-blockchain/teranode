package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newTxMapTestProcessor returns a processor over an in-memory subtree store.
func newTxMapTestProcessor(t *testing.T) (*SubtreeProcessor, *blob_memory.Memory) {
	t.Helper()

	ctx := context.Background()
	subtreeStore := blob_memory.New()
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.NewErrorTestLogger(t), tSettings, utxoStoreURL)
	require.NoError(t, err)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, subtreeStore, nil, utxoStore, make(chan NewSubtreeRequest, 1))
	require.NoError(t, err)

	return stp, subtreeStore
}

// storeSubtree stores a subtree of the given leaf hashes (a power of two count) and
// returns its root hash.
func storeSubtree(t *testing.T, store *blob_memory.Memory, hashes []chainhash.Hash) chainhash.Hash {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(len(hashes))
	require.NoError(t, err)

	for _, h := range hashes {
		if h.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			require.NoError(t, st.AddCoinbaseNode())
		} else {
			require.NoError(t, st.AddNode(h, 1, 1))
		}
	}

	b, err := st.Serialize()
	require.NoError(t, err)
	require.NoError(t, store.Set(context.Background(), st.RootHash()[:], fileformat.FileTypeSubtree, b))

	return *st.RootHash()
}

// TestCreateTransactionMap_MatchesReferenceSet pins what callers rely on: the set
// holds every leaf of every block subtree including the coinbase placeholder, Length
// is the distinct count (a hash present in two subtrees counts once), Exists is exact,
// and it is frozen when returned, whatever the size hint.
func TestCreateTransactionMap_MatchesReferenceSet(t *testing.T) {
	const perSubtree = 1024

	stp, store := newTxMapTestProcessor(t)

	all := randHashes(4*perSubtree, 77)

	var (
		roots = make(map[chainhash.Hash]int)
		ref   = make(map[chainhash.Hash]struct{})
	)

	for s := 0; s < 4; s++ {
		leaves := append([]chainhash.Hash(nil), all[s*perSubtree:(s+1)*perSubtree]...)
		if s == 0 {
			leaves[0] = subtreepkg.CoinbasePlaceholderHashValue
		}

		if s == 3 {
			leaves[5] = all[perSubtree+9] // a tx that also sits in subtree 1
		}

		for _, h := range leaves {
			ref[h] = struct{}{}
		}

		roots[storeSubtree(t, store, leaves)] = s
	}

	_, hasCoinbase := ref[subtreepkg.CoinbasePlaceholderHashValue]
	require.True(t, hasCoinbase)
	require.Less(t, len(ref), 4*perSubtree, "test needs a duplicate across subtrees")

	for _, hint := range []uint64{0, 1, 4 * perSubtree, 1 << 30} {
		set, conflicting, err := stp.CreateTransactionMap(context.Background(), roots, len(roots), hint)
		require.NoError(t, err)
		require.Empty(t, conflicting)

		require.Equal(t, len(ref), set.Length(), "hint %d", hint)
		require.True(t, set.Exists(subtreepkg.CoinbasePlaceholderHashValue))
		require.Error(t, set.Put(all[0]), "the returned set must be frozen")

		for h := range ref {
			require.True(t, set.Exists(h))
		}

		for _, h := range randHashes(5000, 78) {
			require.False(t, set.Exists(h))
		}

		visited := 0
		set.Iter(func(h chainhash.Hash) bool {
			_, ok := ref[h]
			require.True(t, ok)

			visited++

			return false
		})
		require.Equal(t, len(ref), visited, "Iter must visit every distinct hash")

		// the filter used when draining the queue behaves like a plain map lookup
		nodes := make([]subtreepkg.Node, 0, 2*len(ref))
		for h := range ref {
			nodes = append(nodes, subtreepkg.Node{Hash: h})
		}

		for _, h := range randHashes(len(ref), 79) {
			nodes = append(nodes, subtreepkg.Node{Hash: h})
		}

		keep := filterDrainedNodes(nodes, set, nil)
		for i, n := range nodes {
			_, inBlock := ref[n.Hash]
			require.Equal(t, !inBlock, keep[i])
		}
	}
}

func TestCreateTransactionMap_TinyBlocks(t *testing.T) {
	stp, store := newTxMapTestProcessor(t)

	// coinbase only
	cb := storeSubtree(t, store, []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue})
	set, _, err := stp.CreateTransactionMap(context.Background(), map[chainhash.Hash]int{cb: 0}, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, set.Length())
	require.True(t, set.Exists(subtreepkg.CoinbasePlaceholderHashValue))

	// no subtrees at all
	set, _, err = stp.CreateTransactionMap(context.Background(), map[chainhash.Hash]int{}, 0, 0)
	require.NoError(t, err)
	require.Equal(t, 0, set.Length())
	require.False(t, set.Exists(chainhash.Hash{}))
}

func TestCreateTransactionMap_MissingSubtreeErrors(t *testing.T) {
	stp, _ := newTxMapTestProcessor(t)

	missing := randHashes(1, 3)[0]
	set, conflicting, err := stp.CreateTransactionMap(context.Background(), map[chainhash.Hash]int{missing: 0}, 1, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error getting subtrees")
	require.Nil(t, set)
	require.Nil(t, conflicting)
}
