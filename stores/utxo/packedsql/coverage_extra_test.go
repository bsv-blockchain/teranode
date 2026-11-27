package packedsql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

func newUnbatchedTestStore(t *testing.T) *Store {
	t.Helper()

	dsn, cleanup, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)

	t.Cleanup(func() { _ = cleanup() })

	storeURL, err := url.Parse(dsn)
	require.NoError(t, err)

	storeURL.Scheme = "packedsql"

	tSettings := settings.NewSettings()
	tSettings.UtxoStore.PackedSQLPartitions = 4
	tSettings.UtxoStore.StoreBatcherSize = 1
	tSettings.UtxoStore.PackedSQLSpendWorkers = 0

	store, err := New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store
}

func TestCreateAndSpendUnbatched(t *testing.T) {
	store := newUnbatchedTestStore(t)
	require.Nil(t, store.createBatcher)
	require.Empty(t, store.spendChans)

	single := newExtendedTx(t, 1, 900_000)
	_, err := store.Create(context.Background(), single, 100)
	require.NoError(t, err)

	multi := newExtendedTx(t, int(store.pageSize)+2, 901_000)
	_, err = store.Create(context.Background(), multi, 100)
	require.NoError(t, err)

	_, err = store.Create(context.Background(), multi, 100)
	require.True(t, errors.Is(err, errors.ErrTxExists))

	spends, err := store.Spend(context.Background(), newSpendingTx(t, single, 0), 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
}

func TestUnspendPageSlot(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, int(store.pageSize)+2, 910_000)
	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	pageVout := store.pageSize + 1

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, pageVout), 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	require.NoError(t, store.Unspend(context.Background(), spends))
	require.Equal(t, make([]byte, slotSpendSize), slotSpend(t, store, parent, pageVout))

	var pageSpent int
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT spent_count FROM packed_tx_pages WHERE hash = $1 AND page = 1`,
		parent.TxIDChainHash()[:]).Scan(&pageSpent))
	require.Zero(t, pageSpent)
}

func TestGetUtxosOnFrozenMultiPageTx(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, int(store.pageSize)+2, 920_000)
	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 0), 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	utxoHash1, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[1], 1)
	require.NoError(t, err)

	require.NoError(t, store.FreezeUTXOs(context.Background(),
		[]*utxo.Spend{{TxID: parent.TxIDChainHash(), Vout: 1, UTXOHash: utxoHash1}}, store.settings))

	md, err := store.Get(context.Background(), parent.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)
	require.Len(t, md.SpendingDatas, int(store.pageSize)+2)
	require.NotNil(t, md.SpendingDatas[0])
	require.NotNil(t, md.SpendingDatas[1])
	require.Nil(t, md.SpendingDatas[2])
	require.Nil(t, md.SpendingDatas[int(store.pageSize)+1])
}
