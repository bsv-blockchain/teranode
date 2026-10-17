// Package txmetacache — the restored tx_meta_cache_size gauge (notes §75, Q53).
package txmetacache

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newCacheForSizeTest(t *testing.T, name string) *TxMetaCache {
	t.Helper()

	utxoStoreURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	logger := ulogger.NewErrorTestLogger(t)

	utxoStore, err := sql.New(context.Background(), logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = utxoStore.Close(context.Background()) })

	c, err := NewTxMetaCache(context.Background(), settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
	require.NoError(t, err)

	return c.(*TxMetaCache)
}

func TestSizeGaugeIsRestoredAndTracksValidEntries(t *testing.T) {
	c := newCacheForSizeTest(t, "size_gauge")
	require.NotNil(t, prometheusBlockValidationTxMetaCacheSize, "size gauge must be registered")

	updateTxMetaCachePrometheusMetrics()
	// The gauge is a package-global sum over every registered cache, and other
	// tests in this binary register their own; assert the delta this cache
	// contributes, not an absolute value.
	baseline := testutil.ToFloat64(prometheusBlockValidationTxMetaCacheSize)

	h := chainhash.DoubleHashH([]byte("size-gauge-tx"))
	require.NoError(t, c.SetCache(&h, &meta.Data{}))

	updateTxMetaCachePrometheusMetrics()
	require.Equal(t, baseline+1, testutil.ToFloat64(prometheusBlockValidationTxMetaCacheSize))
}
