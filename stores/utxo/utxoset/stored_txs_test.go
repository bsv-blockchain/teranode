package utxoset

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// dataOnlyTx is a transaction whose only output is OP_FALSE OP_RETURN data, the shape a block
// path below the checkpoint leaves out of its create wave. seed makes each one distinct.
func dataOnlyTx(t *testing.T, seed byte) *bt.Tx {
	t.Helper()

	tx := mkTx(t, 0, 0)
	require.NoError(t, tx.AddOpReturnOutput([]byte{'d', 'a', 't', 'a', seed}))

	return tx
}

// A data transaction an earlier failed attempt at a block stored with no block information is
// found by the identity probe, whatever leaf it lands in, and transactions the store never saw
// are left out without failing the read.
func TestStoredTxsFindsAnUnminedDataTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	// Enough distinct transactions to land in several leaves.
	var stored []*bt.Tx

	leaves := map[int16]bool{}

	for i := byte(0); i < 24; i++ {
		tx := dataOnlyTx(t, i)
		_, err := s.Create(ctx, tx, 200)
		require.NoError(t, err)
		require.True(t, identExists(t, s, ctx, tx), "an unmined create leaves an identity row")

		stored = append(stored, tx)
		leaves[LeafFor(tx.TxIDChainHash()[:])] = true
	}

	require.Greater(t, len(leaves), 1, "the set must span more than one leaf")

	asked := make([]*chainhash.Hash, 0, 2+len(stored))
	asked = append(asked, &chainhash.Hash{0xee}, &chainhash.Hash{0x01, 0x02})

	for _, tx := range stored {
		asked = append(asked, tx.TxIDChainHash())
	}

	got, err := s.StoredTxs(ctx, asked)
	require.NoError(t, err)
	require.Len(t, got, len(stored))

	want := make([]chainhash.Hash, 0, len(stored))
	for _, tx := range stored {
		want = append(want, *tx.TxIDChainHash())
	}

	require.ElementsMatch(t, want, got)

	none, err := s.StoredTxs(ctx, []*chainhash.Hash{{0xee}, nil})
	require.NoError(t, err)
	require.Empty(t, none, "a transaction the store never saw is absent, not an error")
}

// Marking a transaction mined leaves its identity row where it is, so the probe still finds it.
// The one place the probe does not look is tx_mined, where the stamp moves a mined transaction
// 288 blocks deep. A transaction is there only once a block containing it is that far down the
// chain, so it is mined already and a retry's mark is not what records it. The move is
// simulated by dropping the row, as the containment-read tests do. Pinned so a change to where
// a transaction lives is seen here.
func TestStoredTxsSeesTheIdentityTableOnly(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := dataOnlyTx(t, 0x77)
	_, err := s.Create(ctx, tx, 200)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 201, OnLongestChain: true})
	require.NoError(t, err)

	got, err := s.StoredTxs(ctx, hashes(tx))
	require.NoError(t, err)
	require.Len(t, got, 1, "marking it mined does not move it off the identity table")

	dropIdentityRow(t, s, ctx, tx)

	got, err = s.StoredTxs(ctx, hashes(tx))
	require.NoError(t, err)
	require.Empty(t, got, "a transaction the deep stamp moved is not on the identity table")
}

// explainStoredTxs runs storedTxsSQL under EXPLAIN ANALYZE with perLeaf absent hashes in every
// leaf and returns the plan.
func explainStoredTxs(t *testing.T, s *Store, ctx context.Context, perLeaf int) string {
	t.Helper()

	arrays := make([]any, NumLeaves)
	for i := range arrays {
		keys := make([][]byte, 0, perLeaf)

		for k := 0; k < perLeaf; k++ {
			key := make([]byte, 32)
			key[0] = byte(i)
			key[1] = byte(k)
			key[2] = byte(k >> 8)
			key[31] = 0xff
			keys = append(keys, key)
		}

		arrays[i] = keys
	}

	rows, err := s.pool.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) "+storedTxsSQL, arrays...)
	require.NoError(t, err)

	defer rows.Close()

	var plan []string

	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan = append(plan, line)
	}

	require.NoError(t, rows.Err())

	return strings.Join(plan, "\n")
}

// The probe is one statement whose every branch is pruned to one identity partition. With a
// handful of hashes per leaf against a populated table each branch is an index-only scan of
// that partition's primary key. With a thousand per leaf against small partitions the planner
// may read the partition whole instead, because that is fewer pages than a thousand descents;
// that plan is logged, not failed.
func TestStoredTxsPlanReadsThePrimaryKey(t *testing.T) {
	s, ctx := newTestStore(t)

	// An empty table is seq-scanned whatever the query, because there is nothing to scan. Fill
	// the identity table directly with enough rows that the planner has a real choice.
	_, err := s.pool.Exec(ctx, `
INSERT INTO tx_ident (leaf, txid, created_height)
SELECT get_byte(d, 0) & 7, d, 1
  FROM (SELECT sha256(int4send(i)) AS d FROM generate_series(1, 200000) AS i) AS g`)
	require.NoError(t, err)

	_, err = s.pool.Exec(ctx, `VACUUM ANALYZE tx_ident`)
	require.NoError(t, err)

	small := explainStoredTxs(t, s, ctx, 50)
	t.Logf("StoredTxs plan, 50 hashes per leaf, 200,000 identity rows:\n%s", small)

	for leaf := 0; leaf < NumLeaves; leaf++ {
		require.Contains(t, small, fmt.Sprintf("Index Only Scan using tx_ident_l%d_pkey", leaf),
			"every leaf branch probes its own partition's primary key")
	}

	require.NotContains(t, small, "Seq Scan")

	t.Logf("StoredTxs plan, 1,000 hashes per leaf, 200,000 identity rows:\n%s", explainStoredTxs(t, s, ctx, 1000))
}
