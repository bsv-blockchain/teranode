package postgres

// TestPruneCascade_PlanShape is the EXPLAIN gate for the page-ordered cascade:
// the spends arm of the batch DELETE must plan as a Bitmap Index Scan feeding a
// Bitmap Heap Scan (victim row-locations sorted by physical page before the
// heap is visited). If a planner change ever regresses this to per-hash index
// scans — the exact random-page pattern the rewrite removed — this test fails
// before a benchmark has to discover it. The plan is pinned with the same
// SET LOCAL statements the pruner uses.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// makePlanShapeParent builds a UNIQUE 2-output P2PKH parent (the canonical
// testExtendedTx is a fixed tx, so it can only be created once per DB).
func makePlanShapeParent(t *testing.T, seed int) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	tx.Version = 1
	var prev [32]byte
	prev[0] = byte(seed)
	prev[1] = byte(seed >> 8)
	prev[2] = 0x9C // sentinel: plan-shape test txs
	prevHash, err := chainhash.NewHash(prev[:])
	require.NoError(t, err)
	require.NoError(t, tx.From(prevHash.String(), 0, "76a914000000000000000000000000000000000000000088ac", 50_000))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
	for v := 0; v < 2; v++ {
		b := make([]byte, 25)
		b[0], b[1], b[2] = 0x76, 0xa9, 0x14
		for i := 0; i < 20; i++ {
			b[3+i] = byte(seed + v*31 + i)
		}
		b[23], b[24] = 0x88, 0xac
		s := bscript.Script(b)
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 10_000, LockingScript: &s})
	}
	return tx
}

func TestPruneCascade_PlanShape(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Seed unique fully-spent mined parents so all three tables have rows and
	// statistics. 300 is plenty: the plan settings are pinned, so the assertion
	// is about shape under those settings, not about cost-model tipping points.
	for i := 0; i < 300; i++ {
		parent := makePlanShapeParent(t, i)
		_, err := store.Create(ctx, parent, 40, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
			BlockID: 40, BlockHeight: 40, OnLongestChain: true,
		}))
		require.NoError(t, err)
		child := getSpendingTx(t, parent, 0, 1)
		_, err = store.Create(ctx, child, 50)
		require.NoError(t, err)
		_, err = store.Spend(ctx, child, 50)
		require.NoError(t, err)
	}
	_, err := store.pool.Exec(ctx, `ANALYZE spends_p00, txs_p00, pending_deletes_p00`)
	require.NoError(t, err)

	// Use hashes that actually reside in the p00 leaves (hash partitioning
	// spreads the 300 parents across all 8), mirroring the per-partition
	// statement deleteTombstonedBatch runs.
	rows, err := store.pool.Query(ctx, `SELECT hash FROM txs_p00 LIMIT 200`)
	require.NoError(t, err)
	var hashes [][]byte
	for rows.Next() {
		var h []byte
		require.NoError(t, rows.Scan(&h))
		hashes = append(hashes, h)
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.NotEmpty(t, hashes, "at least some seeded parents must land in partition 00")

	// The exact statement shape deleteTombstonedBatch executes (partition 00),
	// under the same pinned settings, EXPLAINed with the real hash array.
	explainSQL := fmt.Sprintf(`EXPLAIN (FORMAT TEXT) %s`, fmt.Sprintf(`
		WITH del_spends AS (DELETE FROM %[2]s WHERE prev_tx_hash = ANY($1::bytea[]) RETURNING 1),
		del_pd     AS (DELETE FROM %[3]s WHERE hash = ANY($1::bytea[]))
		DELETE FROM %[1]s WHERE hash = ANY($1::bytea[])`,
		"txs_p00", "spends_p00", "pending_deletes_p00"))

	pgxTx, err := store.pool.Begin(ctx)
	require.NoError(t, err)
	defer pgxTx.Rollback(ctx) //nolint:errcheck
	_, err = pgxTx.Exec(ctx, `SET LOCAL enable_indexscan = off`)
	require.NoError(t, err)
	_, err = pgxTx.Exec(ctx, `SET LOCAL enable_seqscan = off`)
	require.NoError(t, err)

	planRows, err := pgxTx.Query(ctx, explainSQL, hashes)
	require.NoError(t, err)
	var plan strings.Builder
	for planRows.Next() {
		var line string
		require.NoError(t, planRows.Scan(&line))
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	planRows.Close()
	require.NoError(t, planRows.Err())
	planText := plan.String()

	// The gate: the spends delete must use the bitmap pair. (The heap scan
	// implies a feeding index scan, but assert both so a partial regression is
	// loud.)
	require.Contains(t, planText, "Bitmap Heap Scan on spends_p00",
		"spends arm must be a Bitmap Heap Scan (page-ordered); plan was:\n%s", planText)
	require.Contains(t, planText, "Bitmap Index Scan",
		"bitmap must be built from the index; plan was:\n%s", planText)
	require.NotContains(t, planText, "Seq Scan on spends_p00",
		"spends arm must never seq-scan; plan was:\n%s", planText)
}
