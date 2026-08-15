package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func tableCount(t *testing.T, pool *pgxpool.Pool, table string, hash []byte) int {
	t.Helper()

	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM `+table+` WHERE hash = $1`, hash).Scan(&n))

	return n
}

func TestDeleteRemovesEverything(t *testing.T) {
	store := newTestStore(t)

	tx := newExtendedTx(t, int(store.pageSize)+1, 800_000)
	_, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	utxoHash0, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	require.NoError(t, store.FreezeUTXOs(context.Background(),
		[]*utxo.Spend{{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0}}, store.settings))

	_, err = store.pool.Exec(context.Background(),
		`INSERT INTO conflicting_children (hash, child_hash) VALUES ($1, $2)`,
		tx.TxIDChainHash()[:], make([]byte, 32))
	require.NoError(t, err)

	require.NoError(t, store.Delete(context.Background(), tx.TxIDChainHash()))

	h := tx.TxIDChainHash()[:]
	require.Zero(t, tableCount(t, store.pool, "packed_txs", h))
	require.Zero(t, tableCount(t, store.pool, "packed_tx_pages", h))
	require.Zero(t, tableCount(t, store.pool, "utxo_overrides", h))
	require.Zero(t, tableCount(t, store.pool, "conflicting_children", h))
}

func TestPrunerProviderSingleton(t *testing.T) {
	store := newTestStore(t)

	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	svc1, err := store.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, svc1)

	svc2, err := store.GetPrunerService()
	require.NoError(t, err)
	require.Same(t, svc1, svc2)

	deleted, err := svc1.Prune(context.Background(), 200, "testblock")
	require.NoError(t, err)
	require.Zero(t, deleted)
}
