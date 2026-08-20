package pruner_test

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/packedsql"
	packedpruner "github.com/bsv-blockchain/teranode/stores/utxo/packedsql/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func newStoreAndPool(t *testing.T, defensive bool) (*packedsql.Store, *pgxpool.Pool, *settings.Settings) {
	t.Helper()

	dsn, cleanup, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)

	t.Cleanup(func() { _ = cleanup() })

	storeURL, err := url.Parse(dsn)
	require.NoError(t, err)

	storeURL.Scheme = "packedsql"

	tSettings := settings.NewSettings()
	tSettings.UtxoStore.PackedSQLPartitions = 4
	tSettings.Pruner.UTXODefensiveEnabled = defensive

	store, err := packedsql.New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(context.Background()) })

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)

	t.Cleanup(pool.Close)

	return store, pool, tSettings
}

func newParentTx(t *testing.T, satoshis uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tests.Tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tests.Tx.Outputs[0].LockingScript,
		Satoshis:      tests.Tx.Outputs[0].Satoshis,
	}))

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis))

	return tx
}

func newChildTx(t *testing.T, parent *bt.Tx) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	return tx
}

func TestNewServiceValidation(t *testing.T) {
	tSettings := settings.NewSettings()

	_, err := packedpruner.NewService(tSettings, packedpruner.Options{})
	require.Error(t, err)

	_, err = packedpruner.NewService(nil, packedpruner.Options{Logger: ulogger.TestLogger{}})
	require.Error(t, err)

	_, err = packedpruner.NewService(tSettings, packedpruner.Options{Logger: ulogger.TestLogger{}})
	require.Error(t, err)
}

type countingObserver struct {
	height  atomic.Int64
	records atomic.Int64
}

func (o *countingObserver) OnPruneComplete(height uint32, recordsProcessed int64) {
	o.height.Store(int64(height))
	o.records.Store(recordsProcessed)
}

func TestPruneDeletesTombstoned(t *testing.T) {
	store, pool, tSettings := newStoreAndPool(t, false)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		tx := newParentTx(t, 810_000+uint64(i)*1000)

		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)

		dah := int64(150)
		if i == 4 {
			dah = 900
		}

		_, err = pool.Exec(ctx,
			`UPDATE packed_txs SET delete_at_height = $2 WHERE hash = $1`, tx.TxIDChainHash()[:], dah)
		require.NoError(t, err)
	}

	svc, err := packedpruner.NewService(tSettings, packedpruner.Options{Logger: ulogger.TestLogger{}, Pool: pool})
	require.NoError(t, err)

	svc.Start(ctx)

	observer := &countingObserver{}
	svc.AddObserver(observer)

	deleted, err := svc.Prune(ctx, 200, "testblock")
	require.NoError(t, err)
	require.Equal(t, int64(4), deleted)
	require.Equal(t, int64(200), observer.height.Load())
	require.Equal(t, int64(4), observer.records.Load())

	var remaining int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM packed_txs WHERE delete_at_height IS NOT NULL`).Scan(&remaining))
	require.Equal(t, 1, remaining)
}

func TestPruneDefensiveMode(t *testing.T) {
	store, pool, tSettings := newStoreAndPool(t, true)
	ctx := context.Background()

	parent := newParentTx(t, 820_000)

	_, err := store.Create(ctx, parent, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	child := newChildTx(t, parent)

	_, _, err = store.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)

	var dah *int64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT delete_at_height FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah)

	svc, err := packedpruner.NewService(tSettings, packedpruner.Options{Logger: ulogger.TestLogger{}, Pool: pool})
	require.NoError(t, err)

	deleted, err := svc.Prune(ctx, uint32(*dah), "testblock")
	require.NoError(t, err)
	require.Zero(t, deleted)

	retention := tSettings.GetUtxoStoreBlockHeightRetention()

	_, err = store.SetMinedMulti(ctx,
		[]*chainhash.Hash{child.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 102, OnLongestChain: true})
	require.NoError(t, err)

	deleted, err = svc.Prune(ctx, 102+retention+retention, "testblock")
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)

	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&n))
	require.Zero(t, n)
}
