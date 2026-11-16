package txmetacache

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// multiForwardStore counts SpendAndCreateMulti calls reaching a real store, so a
// test can tell the cache forwarded the list rather than running the shared
// default over itself.
type multiForwardStore struct {
	utxo.Store
	multiCalls int
}

func (m *multiForwardStore) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	m.multiCalls++
	return m.Store.SpendAndCreateMulti(ctx, txs, blockHeight, opts...)
}

// newMultiCache opens a sqlitememory UTXO store and a TxMetaCache over it.
func newMultiCache(t *testing.T) (*TxMetaCache, *multiForwardStore) {
	t.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)

	base, err := sql.New(ctx, ulogger.NewErrorTestLogger(t), tSettings, storeURL)
	require.NoError(t, err)

	inner := &multiForwardStore{Store: base}

	c, err := NewTxMetaCache(ctx, tSettings, ulogger.TestLogger{}, inner, Unallocated)
	require.NoError(t, err)

	return c.(*TxMetaCache), inner
}

// The wrapper reaches the inner store's SpendAndCreateMulti once, and caches
// created, non-conflicting records only: never a record that already existed or
// a conflicting one. A record created mined is not covered here: the SQL store's
// Create returns its metadata without BlockIDs, so on that store the unmined
// check cannot see the block, on the single-transaction path as much as here.
func TestTxMetaCache_SpendAndCreateMultiForwardsAndCaches(t *testing.T) {
	ctx := context.Background()

	const height = 100

	t.Run("created is cached, existing is not", func(t *testing.T) {
		c, inner := newMultiCache(t)

		w := tests.BuildMultiWorkload(t, 0x01, 1, 2)
		w.StoreRoots(t, inner, height-1)

		// The second transaction's record already exists, created by propagation.
		_, _, err := inner.SpendAndCreate(ctx, w.Txs[1], height-1)
		require.NoError(t, err)

		results, err := c.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Equal(t, 1, inner.multiCalls, "the list is forwarded to the inner store")
		require.Equal(t, utxo.MultiTxCreated, results[0].Status, "%v", results[0].Err)
		require.Equal(t, utxo.MultiTxExisted, results[1].Status, "%v", results[1].Err)

		stored, err := inner.Get(ctx, w.Txs[0].TxIDChainHash())
		require.NoError(t, err)

		cached, ok := c.GetMetaCached(ctx, *w.Txs[0].TxIDChainHash())
		require.True(t, ok, "a created record is cached")
		require.Equal(t, stored.Fee, cached.Fee)

		_, ok = c.GetMetaCached(ctx, *w.Txs[1].TxIDChainHash())
		require.False(t, ok, "an existing record is not cached")
	})

	t.Run("conflicting is not cached", func(t *testing.T) {
		c, inner := newMultiCache(t)

		w := tests.BuildMultiWorkload(t, 0x02, 1, 1)
		w.StoreRoots(t, inner, height-1)

		results, err := c.SpendAndCreateMulti(ctx, w.Txs, height, utxo.WithConflicting(true), utxo.WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Equal(t, utxo.MultiTxCreated, results[0].Status, "%v", results[0].Err)

		_, ok := c.GetMetaCached(ctx, *w.Txs[0].TxIDChainHash())
		require.False(t, ok, "a conflicting record is not cached")
	})
}

// With WithTXIDs the cache keys on the supplied txids, as the store does.
func TestTxMetaCache_SpendAndCreateMultiCachesUnderSuppliedTxIDs(t *testing.T) {
	ctx := context.Background()
	c, inner := newMultiCache(t)

	w := tests.BuildMultiWorkload(t, 0x04, 1, 1)
	w.StoreRoots(t, inner, 99)

	ids := []chainhash.Hash{{0x42}}

	results, err := c.SpendAndCreateMulti(ctx, w.Txs, 100, utxo.WithTXIDs(ids), utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxCreated, results[0].Status, "%v", results[0].Err)

	_, err = inner.Get(ctx, &ids[0])
	require.NoError(t, err, "the store holds the record under the supplied txid")

	_, ok := c.GetMetaCached(ctx, ids[0])
	require.True(t, ok)

	_, ok = c.GetMetaCached(ctx, *w.Txs[0].TxIDChainHash())
	require.False(t, ok, "nothing is cached under the computed txid")
}
