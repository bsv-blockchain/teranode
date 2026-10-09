package utxoset

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// planNode is the part of one EXPLAIN (FORMAT JSON) node the plan assertions read.
type planNode struct {
	NodeType     string     `json:"Node Type"`
	RelationName string     `json:"Relation Name"`
	Plans        []planNode `json:"Plans"`
}

// walkPlan calls visit on every node of the plan, parents before children.
func walkPlan(n planNode, visit func(planNode)) {
	visit(n)

	for _, c := range n.Plans {
		walkPlan(c, visit)
	}
}

// explainUnderNoNestloop plans sql with the given arguments inside a transaction that has
// switched nested loops off, and returns the root plan node.
//
// enable_nestloop = off is how the test stands in for the bad row estimates mainnet produced
// at a 288-block window boundary: a fresh partition with no statistics makes a hash join look
// cheap, and the planner is then free to build one wherever the statement lets it. A fenced
// LATERAL leaves it no such freedom, because a lateral reference can only be satisfied by a
// nested loop, so the setting changes the plan of an unfenced join and of nothing else.
func explainUnderNoNestloop(t *testing.T, s *Store, ctx context.Context, sql string, args ...any) planNode {
	t.Helper()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)

	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `SET LOCAL enable_nestloop = off`)
	require.NoError(t, err)

	var raw []byte
	require.NoError(t, tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&raw))

	var plans []struct {
		Plan planNode `json:"Plan"`
	}
	require.NoError(t, json.Unmarshal(raw, &plans))
	require.Len(t, plans, 1)

	return plans[0].Plan
}

// populateMembershipWindows writes n synthetic transactions straight into tx_mined and
// tx_body, spread evenly over six 288-block containment windows (and so 36 body windows),
// and returns their ids. Raw SQL rather than the store's write path, because the test needs
// enough live windows for a seq scan of every one to be visible in a plan, and nothing about
// the rows' payload matters to the planner.
func populateMembershipWindows(t *testing.T, s *Store, ctx context.Context, base uint32, n int) [][]byte {
	t.Helper()

	const span = 6 * TxMinedPartitionBlocks

	for h := base; h < base+span; h += TxBodyPartitionBlocks {
		require.NoError(t, s.ensureTxMinedPartition(ctx, h))
		require.NoError(t, s.ensureTxBodyPartition(ctx, h))
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height, flags)
		SELECT decode(lpad(to_hex(i), 64, '0'), 'hex'), $1::int + (i % $3::int), 1 + (i % $3::int), 0,
		       $1::int + (i % $3::int), 0
		  FROM generate_series(1, $2::int) AS g(i)`, base, n, span)
	require.NoError(t, err)

	_, err = s.pool.Exec(ctx, `
		INSERT INTO tx_body (created_height, txid, raw_tx)
		SELECT $1::int + (i % $3::int), decode(lpad(to_hex(i), 64, '0'), 'hex'), '\x00'::bytea
		  FROM generate_series(1, $2::int) AS g(i)`, base, n, span)
	require.NoError(t, err)

	rows, err := s.pool.Query(ctx, `
		SELECT decode(lpad(to_hex(i), 64, '0'), 'hex')
		  FROM generate_series(1, $1::int, $2::int) AS g(i)`, n, n/1000)
	require.NoError(t, err)

	var keys [][]byte

	for rows.Next() {
		var k []byte
		require.NoError(t, rows.Scan(&k))

		keys = append(keys, k)
	}

	rows.Close()
	require.NoError(t, rows.Err())

	return keys
}

// TestMinedReadsNeverScanEveryBodyWindow pins the plan shape of the two by-txid containment
// reads under the condition that stalled setTxMined on mainnet for minutes at a time.
//
// minedByTxidSQL fenced tx_mined behind a LATERAL with OFFSET 0 but joined tx_body outside the
// fence. When the estimates went wrong at a window boundary, Postgres hash-joined the keys'
// containment rows against tx_body and seq-scanned every live body window for each 1024-key
// batch: 30 to 260 seconds, temp spills, and once ENOSPC. SetMinedMulti's read-back went
// through that statement, and only wanted block ids. Both statements are now held to: no hash
// or merge join anywhere, and no seq scan of any tx_body or tx_mined partition. The block-id
// read is held to more than that: it must not name tx_body at all.
func TestMinedReadsNeverScanEveryBodyWindow(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	keys := populateMembershipWindows(t, s, ctx, 576_000, 60_000)
	require.GreaterOrEqual(t, len(keys), 1000)

	assertFenced := func(t *testing.T, root planNode) (touchesBody bool) {
		walkPlan(root, func(n planNode) {
			require.NotEqual(t, "Hash Join", n.NodeType, "a hash join reads the whole of one side")
			require.NotEqual(t, "Merge Join", n.NodeType, "a merge join reads the whole of one side")

			if strings.HasPrefix(n.RelationName, "tx_body") {
				touchesBody = true
			}

			if strings.HasPrefix(n.RelationName, "tx_body") || strings.HasPrefix(n.RelationName, "tx_mined") {
				require.NotEqual(t, "Seq Scan", n.NodeType, "seq scan of %s", n.RelationName)
			}
		})

		return touchesBody
	}

	t.Run("minedByTxidSQL", func(t *testing.T) {
		root := explainUnderNoNestloop(t, s, ctx, minedByTxidSQL, keys, int32(0))
		require.True(t, assertFenced(t, root), "this read joins the body, so its plan must name tx_body")
	})

	t.Run("minedIDsByTxidSQL", func(t *testing.T) {
		root := explainUnderNoNestloop(t, s, ctx, minedIDsByTxidSQL, keys, int32(0))
		require.False(t, assertFenced(t, root), "the block-id read-back must never touch tx_body")
	})
}

// minedIDsViaLookup is minedIDsByTxid as it was before it had its own query: the whole
// containment read, payload and body included, reduced to block ids. It is the reference the
// equivalence test holds the narrow read to.
func minedIDsViaLookup(t *testing.T, s *Store, ctx context.Context, txids [][]byte,
	floor int32) map[chainhash.Hash][]uint32 {
	t.Helper()

	hs := make([]chainhash.Hash, 0, len(txids))

	for _, txid := range txids {
		var h chainhash.Hash

		copy(h[:], txid)

		hs = append(hs, h)
	}

	res := newLookupResult(len(hs))
	require.NoError(t, s.readMinedInto(ctx, hs, &res, floor))
	require.Empty(t, res.failed)

	out := make(map[chainhash.Hash][]uint32, len(res.found))
	for h, d := range res.found {
		out[h] = d.BlockIDs
	}

	return out
}

// TestMinedIDsByTxidMatchesTheFullContainmentRead holds the block-id read-back to the answer
// the full containment read gives, for transactions mined in several blocks, across a
// containment window boundary, at a floor that excludes the lower window, and after an
// un-mine. Same keys, same ids, same (mined_height, block_id) order.
func TestMinedIDsByTxidMatchesTheFullContainmentRead(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	const base = 700_000 // window 2430 covers 699,840 to 700,127

	txs := mkStoredTxs(t, s, base, 1_000, 6)

	// A transaction stored but never mined, and one never stored, must be absent from both.
	unminedTx := mkStoredTxs(t, s, base, 9_000, 1)[0]
	absent := mkTx(t, 1, 99_999)

	all := append(append(txHashes(txs), unminedTx.TxIDChainHash()), absent.TxIDChainHash())

	txids := make([][]byte, 0, len(all))
	for _, h := range all {
		txids = append(txids, h[:])
	}

	check := func(label string, floor int32) {
		t.Helper()

		got, err := s.minedIDsByTxid(ctx, txids, floor)
		require.NoError(t, err, label)
		require.Equal(t, minedIDsViaLookup(t, s, ctx, txids, floor), got, label)
		require.NotContains(t, got, *unminedTx.TxIDChainHash(), label)
		require.NotContains(t, got, *absent.TxIDChainHash(), label)
	}

	// Block ids deliberately descend as height rises, so an ORDER BY block_id alone would
	// differ, and two share a height, so height alone would leave the order undefined.
	blocks := []utxo.MinedBlockInfo{
		{BlockID: 90, BlockHeight: base + 5, SubtreeIdx: 1, OnLongestChain: true},
		{BlockID: 80, BlockHeight: base + 5, SubtreeIdx: 2},
		{BlockID: 70, BlockHeight: base + 100, SubtreeIdx: 0},
		{BlockID: 60, BlockHeight: base + 200, SubtreeIdx: 3}, // next containment window
	}

	for i, b := range blocks {
		_, err := s.SetMinedMulti(ctx, txHashes(txs[:len(txs)-i]), b)
		require.NoError(t, err)
	}

	check("several blocks across a window boundary", 0)

	got, err := s.minedIDsByTxid(ctx, txids, 0)
	require.NoError(t, err)
	require.Equal(t, []uint32{80, 90, 70, 60}, got[*txs[0].TxIDChainHash()],
		"(mined_height, block_id) order: block id breaks the tie at one height, height wins otherwise")

	upper := int32((base + 200) / TxMinedPartitionBlocks * TxMinedPartitionBlocks)
	check("floor above the lower window", upper)

	got, err = s.minedIDsByTxid(ctx, txids, upper)
	require.NoError(t, err)
	require.Equal(t, []uint32{60}, got[*txs[0].TxIDChainHash()], "the floor hides the lower window")

	_, err = s.SetMinedMulti(ctx, txHashes(txs[:2]),
		utxo.MinedBlockInfo{BlockID: 80, BlockHeight: base + 5, UnsetMined: true})
	require.NoError(t, err)

	check("after un-mining one block", 0)

	got, err = s.minedIDsByTxid(ctx, txids, 0)
	require.NoError(t, err)
	require.Equal(t, []uint32{90, 70, 60}, got[*txs[0].TxIDChainHash()], "block 80 taken back")
}
