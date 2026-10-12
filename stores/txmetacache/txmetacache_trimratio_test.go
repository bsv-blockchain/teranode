package txmetacache

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// trimRatioOf digs the trimRatio actually configured on the Preallocated
// buckets backing a utxo.Store returned by NewTxMetaCache. It fails the test
// if the store isn't the shape NewTxMetaCache is known to produce for a
// non-Pointer bucketType.
func trimRatioOf(t *testing.T, store any) int {
	t.Helper()

	txMetaCache, ok := store.(*TxMetaCache)
	require.True(t, ok, "expected *TxMetaCache, got %T", store)

	backend, ok := txMetaCache.cache.(*improvedCacheBackend)
	require.True(t, ok, "expected *improvedCacheBackend, got %T", txMetaCache.cache)

	return backend.cache.trimRatio
}

// TestNewTxMetaCache_TrimRatioFromTaggedSetting proves the tagged
// subtreevalidation_txMetaCacheTrimRatio setting now reaches the Preallocated
// bucket's Init call. Setting it via NewSettings() (not a struct literal)
// exercises the exact path that hid the original bug: the settings loader
// wires the tag, and NewTxMetaCache/New must actually use the loaded value.
func TestNewTxMetaCache_TrimRatioFromTaggedSetting(t *testing.T) {
	const key = "subtreevalidation_txMetaCacheTrimRatio"

	gocore.Config().Set(key, "7")
	t.Cleanup(func() { gocore.Config().Set(key, "") })

	tSettings := settings.NewSettings()
	require.Equal(t, 7, tSettings.SubtreeValidation.TxMetaCacheTrimRatio)

	utxoStore, err := nullstore.NewNullStore()
	require.NoError(t, err)

	ctx := context.Background()
	store, err := NewTxMetaCache(ctx, tSettings, ulogger.TestLogger{}, utxoStore, Preallocated)
	require.NoError(t, err)

	require.Equal(t, 7, trimRatioOf(t, store))
}

// TestNewTxMetaCache_UntaggedKeyHasNoEffect proves the old untagged
// "txMetaCacheTrimRatio" gocore key (previously read directly by
// improved_cache.go, bypassing the typed settings system) no longer
// influences the cache. Only the tagged setting's value (or its default)
// should apply.
func TestNewTxMetaCache_UntaggedKeyHasNoEffect(t *testing.T) {
	const untaggedKey = "txMetaCacheTrimRatio"

	gocore.Config().Set(untaggedKey, "99")
	t.Cleanup(func() { gocore.Config().Set(untaggedKey, "") })

	tSettings := settings.NewSettings()
	require.NotEqual(t, 99, tSettings.SubtreeValidation.TxMetaCacheTrimRatio,
		"the untagged key must not leak into the tagged setting")

	utxoStore, err := nullstore.NewNullStore()
	require.NoError(t, err)

	ctx := context.Background()
	store, err := NewTxMetaCache(ctx, tSettings, ulogger.TestLogger{}, utxoStore, Preallocated)
	require.NoError(t, err)

	require.Equal(t, tSettings.SubtreeValidation.TxMetaCacheTrimRatio, trimRatioOf(t, store),
		"the cache must use the tagged setting's value, not the untagged gocore key")
	require.NotEqual(t, 99, trimRatioOf(t, store), "the untagged key must have no effect on the cache")
}
