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

	t.Run("setConflictingSQL", func(t *testing.T) {
		root := explainUnderNoNestloop(t, s, ctx, setConflictingSQL,
			p.kLeaf, p.kTxid, p.kLo, p.kHi, true, FlagConflicting)
		assertKeyed(t, root)
	})

	t.Run("conflictingReadSQL", func(t *testing.T) {
		root := explainUnderNoNestloop(t, s, ctx, conflictingReadSQL,
			p.kRef, p.kTxid, p.kLo, p.kHi,
			p.pRef, p.pChild, p.pLeaf, p.pTxid, p.pUkey, p.pVout)
		assertKeyed(t, root)
	})
}
