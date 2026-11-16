package subtreevalidation

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// indexedConflictingStore answers ConflictingTxHashes from a fixed set and counts
// the per-transaction reads made through BatchDecorate.
type indexedConflictingStore struct {
	utxo.Store

	conflicting []chainhash.Hash
	indexed     bool
	indexErr    error

	decorateCalls atomic.Int32
}

func (s *indexedConflictingStore) ConflictingTxHashes(_ context.Context) ([]chainhash.Hash, bool, error) {
	return s.conflicting, s.indexed, s.indexErr
}

func (s *indexedConflictingStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	s.decorateCalls.Add(1)

	flagged := make(map[chainhash.Hash]bool, len(s.conflicting))
	for _, h := range s.conflicting {
		flagged[h] = true
	}

	for _, item := range items {
		item.Data = &utxometa.Data{Conflicting: flagged[item.Hash]}
	}

	return nil
}

// TestCachedSubtreeConflictingNodes_EmptyIndexReadsNothing pins the steady
// state: with nothing conflicting in the store, a block of already-validated
// subtrees costs one index read and nothing else. No transaction is read, and
// no subtree file is opened (the subtree here was never stored).
func TestCachedSubtreeConflictingNodes_EmptyIndexReadsNothing(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	store := &indexedConflictingStore{indexed: true}
	server.utxoStore = store

	notStored := chainhash.HashH([]byte("never-stored-subtree"))

	conflicting, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{notStored})
	require.NoError(t, err)
	require.Empty(t, conflicting)
	require.Zero(t, store.decorateCalls.Load(), "no transaction may be read from the UTXO store")
}

// TestCachedSubtreeConflictingNodes_IndexIsMatchedAgainstTheSubtrees pins that a
// non-empty conflicting set is intersected with the subtrees' transactions, in
// subtree order, without reading any transaction.
func TestCachedSubtreeConflictingNodes_IndexIsMatchedAgainstTheSubtrees(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	a := chainhash.HashH([]byte("a"))
	b := chainhash.HashH([]byte("b"))
	c := chainhash.HashH([]byte("c"))
	elsewhere := chainhash.HashH([]byte("conflicting-but-not-in-this-block"))

	first := storeCachedSubtreeOf(t, server, []chainhash.Hash{a, b})
	second := storeCachedSubtreeOf(t, server, []chainhash.Hash{c})

	store := &indexedConflictingStore{indexed: true, conflicting: []chainhash.Hash{elsewhere, c, b}}
	server.utxoStore = store

	conflicting, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{first, second})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{b, c}, conflicting)
	require.Zero(t, store.decorateCalls.Load(), "no transaction may be read from the UTXO store")
}

// TestCachedSubtreeConflictingNodes_UnindexedStoreReadsEachTransaction pins the
// fallback for a store that cannot answer from its index yet: correct, by
// reading every transaction's flag.
func TestCachedSubtreeConflictingNodes_UnindexedStoreReadsEachTransaction(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	a := chainhash.HashH([]byte("a"))
	b := chainhash.HashH([]byte("b"))

	root := storeCachedSubtreeOf(t, server, []chainhash.Hash{a, b})

	store := &indexedConflictingStore{indexed: false, conflicting: []chainhash.Hash{b}}
	server.utxoStore = store

	conflicting, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{root})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{b}, conflicting)
	require.NotZero(t, store.decorateCalls.Load(), "the fallback reads each transaction")
}

// TestCachedSubtreeConflictingNodes_IndexReadFailureFailsClosed pins that a
// failed index read is never taken as "nothing is conflicting".
func TestCachedSubtreeConflictingNodes_IndexReadFailureFailsClosed(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	root := storeCachedSubtreeOf(t, server, []chainhash.Hash{chainhash.HashH([]byte("a"))})

	server.utxoStore = &indexedConflictingStore{indexErr: errors.NewProcessingError("index read failed")}

	_, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{root})
	require.Error(t, err)
}
