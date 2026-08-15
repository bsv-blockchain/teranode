package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	packedpruner "github.com/bsv-blockchain/teranode/stores/utxo/packedsql/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
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

func newTestPruner(t *testing.T, store *Store, defensive bool) *packedpruner.Service {
	t.Helper()

	tSettings := store.settings
	tSettings.Pruner.UTXODefensiveEnabled = defensive

	svc, err := packedpruner.NewService(tSettings, packedpruner.Options{
		Logger: ulogger.TestLogger{},
		Pool:   store.pool,
	})
	require.NoError(t, err)

	return svc
}

func TestPrunerDeletesTombstoned(t *testing.T) {
	store := newTestStore(t)

	for i := 0; i < 5; i++ {
		tx := newExtendedTx(t, 1, 810_000+uint64(i)*1000)
		_, err := store.Create(context.Background(), tx, 100)
		require.NoError(t, err)

		dah := int64(150)
		if i == 4 {
			dah = 900
		}

		_, err = store.pool.Exec(context.Background(),
			`UPDATE packed_txs SET delete_at_height = $2 WHERE hash = $1`, tx.TxIDChainHash()[:], dah)
		require.NoError(t, err)
	}

	svc := newTestPruner(t, store, false)

	deleted, err := svc.Prune(context.Background(), 200, "testblock")
	require.NoError(t, err)
	require.Equal(t, int64(4), deleted)

	var remaining int
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM packed_txs WHERE delete_at_height IS NOT NULL`).Scan(&remaining))
	require.Equal(t, 1, remaining)
}

func TestPrunerDefensiveMode(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 1, 820_000)
	_, err := store.Create(context.Background(), parent, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	child := newSpendingTx(t, parent, 0)
	_, _, err = store.SpendAndCreate(context.Background(), child, 101)
	require.NoError(t, err)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT delete_at_height FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah)

	svc := newTestPruner(t, store, true)

	deleted, err := svc.Prune(context.Background(), uint32(*dah), "testblock")
	require.NoError(t, err)
	require.Zero(t, deleted)

	retention := store.settings.GetUtxoStoreBlockHeightRetention()

	_, err = store.SetMinedMulti(context.Background(),
		[]*chainhash.Hash{child.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 102, OnLongestChain: true})
	require.NoError(t, err)

	deleted, err = svc.Prune(context.Background(), 102+retention+retention, "testblock")
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)

	require.Zero(t, tableCount(t, store.pool, "packed_txs", parent.TxIDChainHash()[:]))
}
