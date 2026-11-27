//go:build bvgaps

package subtreevalidation

// The part of GAP-BV-047 still open: a block reusing a subtree this node already validated takes
// its txs' stored verdicts outside the window checkCachedSubtreesAgainstCandidateChain reads them
// in (cachedVerdictsNeedReading), so a time-locked tx is not re-checked against the candidate's
// own median time. The node, its helpers and the other GAP-BV-047 tests, now regression tests,
// are in stored_tx_block_context_recheck_test.go.

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/stretchr/testify/require"
)

// TestKnownDefect_CachedSubtreeUnderOlderParent_RejectsTimeLockedTx proves that a subtree this
// node validated earlier is not re-checked for time-based finality when a candidate block with an
// older parent median-time-past reuses it.
//
// Spec: BV-BODY-020 (tracked as GAP-BV-047).
//
// Scenario: Given a tx with a time nLockTime of one hour ago, final against the node's tip MTP
// (now), validated for height 101 and held in a cached subtree. When a candidate block above
// CSVHeight, whose parent is genesis and so whose parent MTP is the genesis timestamp, reuses that
// subtree. Then the block must be rejected: TX_INVALID caused by NON_FINAL.
//
// Today: checkCachedSubtreesAgainstCandidateChain (check_block_subtrees.go) returns early when the
// store flags no tx of the cached subtrees Conflicting, and re-checks at the block's height and
// candidate MTP only the conflicting ones; its doc comment assumes the rest "does not change
// between blocks". The non-conflicting tx is never re-evaluated and the block is accepted.
//
// Controls (passing today): the same time-locked spend stored as a conflicting record in the
// cached subtree IS re-checked against the genesis parent MTP and rejected.
//
// Note: the candidate's height (above CSVHeight) and its genesis parent are not a coherent chain
// position. The height is chosen only so the time basis is the parent's MTP, and
// checkCachedSubtreesAgainstCandidateChain reads nothing of the block but its height and parent.
func TestKnownDefect_CachedSubtreeUnderOlderParent_RejectsTimeLockedTx(t *testing.T) {
	const validatedForHeight = uint32(101)

	// final against the store's MTP (now), not against the genesis parent's MTP
	lockedUntilTime := uint32(time.Now().Add(-time.Hour).Unix()) //nolint:gosec

	t.Run("control: a conflicting time-locked record in a cached subtree is rejected against the genesis parent's MTP", func(t *testing.T) {
		// Given a conflicting record of a tx locked until an hour ago, in a cached subtree
		node := newRealSubtreeValidationNode(t)
		require.Greater(t, int64(lockedUntilTime), node.settings.ChainCfgParams.GenesisBlock.Header.Timestamp.Unix(), "precondition")

		tx := node.txLockedUntilTime(t, node.parent, lockedUntilTime)
		node.storeAsConflictingRecord(t, tx, validatedForHeight)

		// When a candidate block whose parent is genesis reuses the cached subtree
		err := node.validateBlockReusingCachedSubtree(t, tx)

		// Then the record is re-checked against the genesis MTP and rejected
		requireBlockRejectsTxAsNonFinal(t, err,
			"a conflicting record locked until time %d must be rejected as non-final against the genesis parent's MTP", lockedUntilTime)
	})

	t.Run("a time-locked tx in a cached subtree is rejected against the genesis parent's MTP", func(t *testing.T) {
		// Given a tx locked until an hour ago, accepted for 101 and held in a cached subtree
		node := newRealSubtreeValidationNode(t)
		funding := node.mineSpendableCoinbase(t)
		tx := node.txLockedUntilTime(t, funding, lockedUntilTime)
		node.acceptIntoMempool(t, tx, validatedForHeight)

		// When a candidate block whose parent is genesis reuses the cached subtree
		err := node.validateBlockReusingCachedSubtree(t, tx)

		// Then the tx is re-evaluated against the genesis MTP and rejected
		requireBlockRejectsTxAsNonFinal(t, err,
			"tx %s (nLockTime %d) was final against the tip's MTP; a block whose parent MTP is the genesis timestamp reuses its cached subtree and must reject it as non-final",
			tx.TxIDChainHash().String(), lockedUntilTime)
	})
}

// txLockedUntilTime spends funding's output 0 with nLockTime set to a unix timestamp.
func (node *realSubtreeValidationNode) txLockedUntilTime(t *testing.T, funding *bt.Tx, lockedUntilTime uint32) *bt.Tx {
	t.Helper()

	return node.txWithEnforcedLockTime(t, funding, lockedUntilTime)
}

// validateBlockReusingCachedSubtree stores a subtree of tx and the node's other tx as already
// validated, then runs checkCachedSubtreesAgainstCandidateChain for a block above CSVHeight whose
// parent is genesis, so finality is measured against the genesis MTP.
func (node *realSubtreeValidationNode) validateBlockReusingCachedSubtree(t *testing.T, tx *bt.Tx) error {
	const heightAboveCSVHeight = 24

	server, subtreeStore := node.newServer(t)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, 0))
	require.NoError(t, subtree.AddNode(*node.other.TxIDChainHash(), 0, 0))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	root := *subtree.RootHash()
	require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtree, subtreeBytes))

	blockHeight := uint32(node.settings.ChainCfgParams.CSVHeight) + heightAboveCSVHeight //nolint:gosec

	block, err := model.NewBlock(node.candidateBlockHeader(), &bt.Tx{Version: 1}, []*chainhash.Hash{&root}, 3, 1000, blockHeight, 0)
	require.NoError(t, err)

	return server.checkCachedSubtreesAgainstCandidateChain(t.Context(), block, []chainhash.Hash{root})
}
