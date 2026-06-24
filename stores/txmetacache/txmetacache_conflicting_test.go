package txmetacache

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// conflictingStore wraps NullStore to return controllable metadata.
// This lets us simulate a store where a tx is conflicting while
// the cache may still hold a stale non-conflicting entry.
type conflictingStore struct {
	*nullstore.NullStore
	data map[chainhash.Hash]*meta.Data
}

func newConflictingStore(t testing.TB) *conflictingStore {
	t.Helper()
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)
	require.NoError(t, ns.SetBlockHeight(100))
	return &conflictingStore{
		NullStore: ns,
		data:      make(map[chainhash.Hash]*meta.Data),
	}
}

func (s *conflictingStore) Get(_ context.Context, hash *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
	if d, ok := s.data[*hash]; ok {
		return d, nil
	}
	return &meta.Data{}, nil
}

func (s *conflictingStore) GetMeta(_ context.Context, hash *chainhash.Hash, data *meta.Data) error {
	if d, ok := s.data[*hash]; ok {
		*data = *d
		return nil
	}
	*data = meta.Data{}
	return nil
}

func (s *conflictingStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	for _, it := range items {
		if it == nil {
			continue
		}
		if d, ok := s.data[it.Hash]; ok {
			it.Data = d
		}
	}
	return nil
}

func (s *conflictingStore) SetConflicting(_ context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	for _, h := range txHashes {
		if d, ok := s.data[h]; ok {
			d.Conflicting = setValue
		}
	}
	return nil, txHashes, nil
}

// testHash generates a deterministic hash from an index.
func testHash(i int) chainhash.Hash {
	return chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
}

// testMeta creates non-conflicting metadata.
func testMeta() *meta.Data {
	return &meta.Data{
		Fee:         100,
		SizeInBytes: 250,
		TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
		BlockIDs:    []uint32{},
	}
}

// conflictingMeta creates metadata with Conflicting=true.
func conflictingMeta() *meta.Data {
	return &meta.Data{
		Fee:         100,
		SizeInBytes: 250,
		Conflicting: true,
		TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
		BlockIDs:    []uint32{},
	}
}

// TestGetMeta_ShouldNotCacheConflictingTransactions proves that GetMeta
// caches conflicting transactions from the store on a cache miss, violating
// the guard that exists in Create() (line 506).
//
// Expected behavior: a conflicting tx fetched from the store should NOT be
// added to the cache, so that subsequent cache-only reads (GetMetaCached)
// return a miss, forcing a fresh store lookup.
//
// Actual behavior (bug): GetMeta caches the result without checking the
// Conflicting flag, so the conflicting tx sits in the cache.
func TestGetMeta_ShouldNotCacheConflictingTransactions(t *testing.T) {
	ctx := context.Background()
	hash := testHash(1)

	store := newConflictingStore(t)
	store.data[hash] = conflictingMeta()

	c, err := NewTxMetaCache(ctx, test.CreateBaseTestSettings(t), ulogger.TestLogger{}, store, Unallocated)
	require.NoError(t, err)
	cache := c.(*TxMetaCache)

	// GetMeta should fetch from the store on cache miss
	got := &meta.Data{}
	err = cache.GetMeta(ctx, &hash, got)
	require.NoError(t, err)
	require.True(t, got.Conflicting, "store should return conflicting metadata")

	// Now check whether the conflicting entry leaked into the cache.
	// It should NOT be in the cache (like Create() enforces), so
	// GetMetaCached should return found=false.
	_, found := cache.GetMetaCached(ctx, hash)

	// The cache should not contain conflicting transactions.
	// Either found=false or an ErrNotFound error — both mean "not in cache".
	require.False(t, found, "conflicting tx should NOT be cached by GetMeta, but it was")
}

// TestBatchDecorate_ShouldNotCacheConflictingTransactions proves that
// BatchDecorate caches ALL results from the store — including conflicting
// transactions — without checking the Conflicting flag.
//
// Expected behavior: conflicting transactions should be excluded from the
// cache population that happens inside BatchDecorate.
//
// Actual behavior (bug): BatchDecorate uses SetCacheMultiValuesRaw to
// blindly cache every result, so conflicting entries end up in the cache.
func TestBatchDecorate_ShouldNotCacheConflictingTransactions(t *testing.T) {
	ctx := context.Background()
	hash := testHash(2)

	store := newConflictingStore(t)
	store.data[hash] = conflictingMeta()

	c, err := NewTxMetaCache(ctx, test.CreateBaseTestSettings(t), ulogger.TestLogger{}, store, Unallocated)
	require.NoError(t, err)
	cache := c.(*TxMetaCache)

	// Call BatchDecorate — this fetches from the store and re-caches
	items := []*utxo.UnresolvedMetaData{
		{Hash: hash},
	}
	err = cache.BatchDecorate(ctx, items)
	require.NoError(t, err)
	require.NotNil(t, items[0].Data, "BatchDecorate should populate the item")
	require.True(t, items[0].Data.Conflicting, "store should return conflicting metadata")

	// Verify the conflicting entry should NOT be in the cache
	_, found := cache.GetMetaCached(ctx, hash)

	// Conflicting transactions must not be cached by BatchDecorate.
	// Either found=false or an ErrNotFound error — both mean "not in cache".
	require.False(t, found, "conflicting tx should NOT be cached by BatchDecorate, but it was")
}

// TestSetConflicting_ShouldInvalidateCache proves that SetConflicting
// does not invalidate or update existing cache entries.
//
// Scenario: a transaction is cached as Conflicting=false (as happens
// during normal validation). Later, it is marked conflicting in the store.
// The cache still holds the stale Conflicting=false entry, so subtree
// validation reads the wrong value from the cache and stores the subtree
// without the conflicting flag.
//
// Expected behavior: after SetConflicting, a GetMetaCached call should
// either return found=false (entry evicted) or return the updated
// Conflicting=true value.
//
// Actual behavior (bug): the cache entry is untouched; GetMetaCached
// still returns Conflicting=false.
func TestSetConflicting_ShouldInvalidateCache(t *testing.T) {
	ctx := context.Background()
	hash := testHash(3)

	// The mock store starts with Conflicting=false
	store := newConflictingStore(t)
	store.data[hash] = testMeta()

	c, err := NewTxMetaCache(ctx, test.CreateBaseTestSettings(t), ulogger.TestLogger{}, store, Unallocated)
	require.NoError(t, err)
	cache := c.(*TxMetaCache)

	// Populate the cache with the non-conflicting metadata.
	// This simulates a tx being cached during initial validation.
	err = cache.SetCache(&hash, testMeta())
	require.NoError(t, err)

	// Verify the tx is in the cache with Conflicting=false
	cached, found := cache.GetMetaCached(ctx, hash)
	require.True(t, found, "tx should be in cache after SetCache")
	require.False(t, cached.Conflicting, "cached tx should be non-conflicting initially")

	// Now mark the transaction as conflicting (as the validator would do
	// when a double-spend is detected). This updates the store but the
	// TxMetaCache.SetConflicting just delegates without touching the cache.
	_, _, err = cache.SetConflicting(ctx, []chainhash.Hash{hash}, true)
	require.NoError(t, err)

	// Verify the store has the updated value
	require.True(t, store.data[hash].Conflicting, "store should have Conflicting=true after SetConflicting")

	// Now read from cache — this is what subtree validation's
	// processTxMetaUsingCache does.
	cachedAfter, found := cache.GetMetaCached(ctx, hash)

	// After SetConflicting, the cache should either not contain the entry
	// or should reflect the updated Conflicting=true value.
	// Either found=false or an ErrNotFound error — both mean "not in cache".
	if found {
		require.True(t, cachedAfter.Conflicting,
			"cache still returns Conflicting=false after SetConflicting updated the store to true — "+
				"subtree validation will store subtrees without the conflicting flag")
	}
	// If not found, that's correct — forces a store lookup which
	// returns the correct value.
}

// forwardingStore lets a test configure the exact return values of SetConflicting
// so we can verify that TxMetaCache.SetConflicting forwards them unchanged.
type forwardingStore struct {
	*nullstore.NullStore
	spendsToReturn   []*utxo.Spend
	childrenToReturn []chainhash.Hash
	lastInput        []chainhash.Hash
}

func newForwardingStore(t testing.TB, spends []*utxo.Spend, children []chainhash.Hash) *forwardingStore {
	t.Helper()
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)
	require.NoError(t, ns.SetBlockHeight(100))
	return &forwardingStore{
		NullStore:        ns,
		spendsToReturn:   spends,
		childrenToReturn: children,
	}
}

func (s *forwardingStore) SetConflicting(_ context.Context, txHashes []chainhash.Hash, _ bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	s.lastInput = append([]chainhash.Hash(nil), txHashes...)
	return s.spendsToReturn, s.childrenToReturn, nil
}

// TestSetConflicting_ForwardsReturnValues proves that TxMetaCache.SetConflicting
// does not silently discard the affected-spends slice and the spending-children
// slice returned by the underlying store. Before the fix, the wrapper returned
// (nil, txHashes, nil) — echoing the input hashes back as "children" and dropping
// the spends. A recursive caller like MarkConflictingRecursively would then see
// its own input fed back, the visited dedup would reject it, and BFS would
// terminate after one level with no cascade.
func TestSetConflicting_ForwardsReturnValues(t *testing.T) {
	ctx := context.Background()

	parentHash := testHash(1)
	childHash := testHash(2)
	grandchildHash := testHash(3)

	parentSpendTxID := parentHash
	expectedSpends := []*utxo.Spend{{TxID: &parentSpendTxID, Vout: 0}}
	expectedChildren := []chainhash.Hash{childHash, grandchildHash}

	store := newForwardingStore(t, expectedSpends, expectedChildren)

	c, err := NewTxMetaCache(ctx, test.CreateBaseTestSettings(t), ulogger.TestLogger{}, store, Unallocated)
	require.NoError(t, err)
	cache := c.(*TxMetaCache)

	spends, children, err := cache.SetConflicting(ctx, []chainhash.Hash{parentHash}, true)
	require.NoError(t, err)

	require.Equal(t, expectedSpends, spends,
		"wrapper must forward affected-parent-spends from underlying store")
	require.Equal(t, expectedChildren, children,
		"wrapper must forward spending-children from underlying store, not echo the input")
	require.Equal(t, []chainhash.Hash{parentHash}, store.lastInput,
		"underlying store should have received exactly the input hashes")
}
