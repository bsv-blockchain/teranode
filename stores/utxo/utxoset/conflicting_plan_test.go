package utxoset

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// TestConflictingStatementsNeverScanTheirTables pins the plans of the two statements
// SetConflicting runs. Both used to join an unnest of transaction ids against the UTXO table and
// the spend journal on the id, with the packed key range as a join filter. Neither table has an
// index on the id, so the planner's only fast path was the key range, and nothing made it take
// it: on Hetzner mainnet on 2026-10-23 the generic plan hash-joined against a sequential scan of
// all eight UTXO partitions, 4.56 billion rows, to flag 16 transactions, and every block with
// conflicting transactions took about 40 minutes in block assembly.
//
// enable_nestloop = off stands in for the estimates that pick a hash join (see
// explainUnderNoNestloop). A fenced LATERAL can only be planned as a nested loop, so the setting
// changes an unfenced join and nothing else.
func TestConflictingStatementsNeverScanTheirTables(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 10_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	// A spend, so a journal partition exists for the plan to scan or not.
	child := mkSpender(t, parent, 0)
	_, _, err = s.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)

	ip, err := subtree.NewTxInpointsFromTx(child)
	require.NoError(t, err)

	named := []chainhash.Hash{*child.TxIDChainHash()}
	p := s.planConflicting(named, map[chainhash.Hash]subtree.TxInpoints{named[0]: ip})

	assertKeyed := func(t *testing.T, root planNode) {
		walkPlan(root, func(n planNode) {
			if strings.HasPrefix(n.RelationName, "utxo") || strings.HasPrefix(n.RelationName, "spend_journal") ||
				strings.HasPrefix(n.RelationName, "tx_ident") || strings.HasPrefix(n.RelationName, "tx_mined") {
				require.NotEqual(t, "Seq Scan", n.NodeType, "seq scan of %s", n.RelationName)
				require.NotEqual(t, "Parallel Seq Scan", n.NodeType, "seq scan of %s", n.RelationName)
			}
		})
	}

	// The UTXO update must not be a join, and no filter on a UTXO partition may hold an array
	// that comes from a sub-select. Postgres hashes only constant arrays, so a filter over a
	// sub-select's array is scanned element by element for every candidate row, which made the
	// statement quadratic in outputs (see setConflictingSQL). A constant array prints as a
	// quoted literal, '{...}', so any other ANY operand is that failure.
	assertLinear := func(t *testing.T, root planNode) {
		walkPlan(root, func(n planNode) {
			require.NotContains(t, n.NodeType, "Join", "the UTXO flag update joins: %s", n.NodeType)
			require.NotEqual(t, "Nested Loop", n.NodeType, "the UTXO flag update joins")

			if !strings.HasPrefix(n.RelationName, "utxo") {
				return
			}

			for _, f := range []string{n.Filter, n.JoinFilter} {
				for _, operand := range strings.Split(f, "ANY (")[1:] {
					require.True(t, strings.HasPrefix(operand, "'"),
						"filter on %s matches a non-constant array: %s", n.RelationName, f)
				}
			}
		})
	}

	t.Run("setConflictingSQL", func(t *testing.T) {
		groups := p.leaves
		require.NotEmpty(t, groups)

		for _, g := range groups {
			root := explainUnderNoNestloop(t, s, ctx, setConflictingSQL,
				g.leaf, g.txids, g.lo, g.hi, true, FlagConflicting)
			assertKeyed(t, root)

			// The top node is the UTXO update. Its row source, the ARRAY sub-select aside (an
			// InitPlan, whose fenced LATERAL is a nested loop by construction), must be one
			// keyed scan of the leaf's partition, with no join.
			require.Equal(t, "ModifyTable", root.NodeType)
			require.Equal(t, "utxo", root.RelationName)

			for _, c := range root.Plans {
				if c.ParentRelationship == "InitPlan" || c.ParentRelationship == "SubPlan" {
					continue
				}

				assertLinear(t, c)
			}
		}
	})

	t.Run("conflictingReadSQL", func(t *testing.T) {
		root := explainUnderNoNestloop(t, s, ctx, conflictingReadSQL,
			p.kRef, p.kTxid, p.kLo, p.kHi,
			p.pRef, p.pChild, p.pLeaf, p.pTxid, p.pUkey, p.pVout)
		assertKeyed(t, root)
	})
}

// TestDeleteStatementNeverScansItsTables pins deleteTxSQL's plan at 16 keys with nested loops
// off. Every arm used to join the keys with USING, the shape that on Hetzner mainnet on
// 2026-10-23 hash-joined on the transaction id over a sequential scan of every UTXO partition.
// Delete passes one hash today, but deleteIn takes a batch, so the plan is checked at the size a
// batch caller would bring.
func TestDeleteStatementNeverScansItsTables(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 10_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	// A spend, so a journal partition exists for the plan to scan or not.
	child := mkSpender(t, parent, 0)
	_, _, err = s.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)

	keys := []chainhash.Hash{*parent.TxIDChainHash(), *child.TxIDChainHash()}
	for i := 0; len(keys) < 16; i++ {
		keys = append(keys, chainhash.HashH([]byte{byte(i)}))
	}

	leaves := make([]int16, 0, len(keys))
	txids := make([][]byte, 0, len(keys))
	los := make([][16]byte, 0, len(keys))
	his := make([][16]byte, 0, len(keys))

	for i := range keys {
		leaves = append(leaves, LeafFor(keys[i][:]))
		txids = append(txids, keys[i][:])
		los = append(los, Pack(keys[i][:], 0))
		his = append(his, Pack(keys[i][:], ^uint32(0)))
	}

	root := explainUnderNoNestloop(t, s, ctx, deleteTxSQL, leaves, txids, los, his)

	walkPlan(root, func(n planNode) {
		for _, table := range []string{"utxo", "spend_journal", "tx_ident", "tx_mined", "tx_body", "preserved_parent"} {
			if strings.HasPrefix(n.RelationName, table) {
				require.NotEqual(t, "Seq Scan", n.NodeType, "seq scan of %s", n.RelationName)
				require.NotEqual(t, "Parallel Seq Scan", n.NodeType, "seq scan of %s", n.RelationName)
			}
		}

		require.NotContains(t, n.NodeType, "Hash Join", "a delete arm joins")
	})
}

// TestConflictingPlanLeafGroupsComeFromTheFlatKeys pins that the per-leaf flip takes each
// transaction's key range from the same kLo and kHi the read uses, in ascending leaf order with
// every named transaction in exactly one group.
func TestConflictingPlanLeafGroupsComeFromTheFlatKeys(t *testing.T) {
	named := make([]chainhash.Hash, 0, 64)
	for i := 0; i < 64; i++ {
		named = append(named, chainhash.HashH([]byte{byte(i)}))
	}

	p := (&Store{}).planConflicting(named, map[chainhash.Hash]subtree.TxInpoints{})

	index := make(map[string]int, len(p.kTxid))
	for i, txid := range p.kTxid {
		index[string(txid)] = i
	}

	seen := 0
	prev := int16(-1)

	for _, g := range p.leaves {
		require.Greater(t, g.leaf, prev, "leaf groups run in ascending leaf order")
		prev = g.leaf

		require.Len(t, g.lo, len(g.txids))
		require.Len(t, g.hi, len(g.txids))

		for j, txid := range g.txids {
			i, ok := index[string(txid)]
			require.True(t, ok)
			require.Equal(t, g.leaf, LeafFor(txid))
			require.Equal(t, p.kLo[i], g.lo[j], "the flip's range is the read's range")
			require.Equal(t, p.kHi[i], g.hi[j], "the flip's range is the read's range")

			seen++
		}
	}

	require.Equal(t, len(named), seen, "every named transaction is in exactly one group")
}
