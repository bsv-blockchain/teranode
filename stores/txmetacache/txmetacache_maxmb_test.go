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

// chunksPerBucketOf digs the number of chunks allocated to bucket 0 of the
// Preallocated backend behind a utxo.Store returned by NewTxMetaCache. Since
// bucketPreallocated.Init allocates exactly maxBucketBytes/ChunkSize chunks
// up front, this is a direct, deterministic proxy for the maxMB actually
// used to construct the cache (maxMB*1024*1024/BucketsCount, assuming no
// floor bump — see the maxMB choices below).
func chunksPerBucketOf(t *testing.T, store any) int {
	t.Helper()

	txMetaCache, ok := store.(*TxMetaCache)
	require.True(t, ok, "expected *TxMetaCache, got %T", store)

	backend, ok := txMetaCache.cache.(*improvedCacheBackend)
	require.True(t, ok, "expected *improvedCacheBackend, got %T", txMetaCache.cache)

	bp, ok := backend.cache.buckets[0].(*bucketPreallocated)
	require.True(t, ok, "expected *bucketPreallocated, got %T", backend.cache.buckets[0])

	return len(bp.chunks)
}

// TestNewTxMetaCache_MaxMBFromTaggedSetting proves the tagged txMetaCacheMaxMB
// setting now reaches the cache size. Setting it via NewSettings() (not a
// struct literal) exercises the exact path that hid these bugs: the settings
// loader wires the tag, and NewTxMetaCache must actually use the loaded
// value rather than re-reading gocore directly.
func TestNewTxMetaCache_MaxMBFromTaggedSetting(t *testing.T) {
	const key = "txMetaCacheMaxMB"

	gocore.Config().Set(key, "1")
	t.Cleanup(func() { gocore.Config().Set(key, "") })

	tSettings := settings.NewSettings()
	require.Equal(t, 1, tSettings.SubtreeValidation.TxMetaCacheMaxMB)

	utxoStore, err := nullstore.NewNullStore()
	require.NoError(t, err)

	ctx := context.Background()
	store, err := NewTxMetaCache(ctx, tSettings, ulogger.TestLogger{}, utxoStore, Preallocated)
	require.NoError(t, err)

	// maxBytes = 1 MB, maxBucketBytes = maxBytes/BucketsCount, chunks = maxBucketBytes/ChunkSize.
	wantChunks := (1 * 1024 * 1024 / BucketsCount) / ChunkSize
	require.Equal(t, wantChunks, chunksPerBucketOf(t, store))

	// A different configured value must produce a different chunk count,
	// proving the setting (not some fixed default) drives the size.
	gocore.Config().Set(key, "2")
	tSettings2 := settings.NewSettings()
	require.Equal(t, 2, tSettings2.SubtreeValidation.TxMetaCacheMaxMB)

	store2, err := NewTxMetaCache(ctx, tSettings2, ulogger.TestLogger{}, utxoStore, Preallocated)
	require.NoError(t, err)

	wantChunks2 := (2 * 1024 * 1024 / BucketsCount) / ChunkSize
	require.NotEqual(t, wantChunks, wantChunks2, "test setup: the two configured sizes must yield different chunk counts")
	require.Equal(t, wantChunks2, chunksPerBucketOf(t, store2))
}

// TestNewTxMetaCache_MaxMBOverrideWinsOverSetting proves the caller-supplied
// maxMBOverride still takes precedence over the tagged setting when
// provided, exactly as it did before the config bypass was removed.
func TestNewTxMetaCache_MaxMBOverrideWinsOverSetting(t *testing.T) {
	const key = "txMetaCacheMaxMB"

	gocore.Config().Set(key, "1")
	t.Cleanup(func() { gocore.Config().Set(key, "") })

	tSettings := settings.NewSettings()
	require.Equal(t, 1, tSettings.SubtreeValidation.TxMetaCacheMaxMB)

	utxoStore, err := nullstore.NewNullStore()
	require.NoError(t, err)

	ctx := context.Background()

	const overrideMB = 4
	store, err := NewTxMetaCache(ctx, tSettings, ulogger.TestLogger{}, utxoStore, Preallocated, overrideMB)
	require.NoError(t, err)

	wantChunks := (overrideMB * 1024 * 1024 / BucketsCount) / ChunkSize
	require.Equal(t, wantChunks, chunksPerBucketOf(t, store),
		"maxMBOverride must win over the tagged setting's configured value")
}
