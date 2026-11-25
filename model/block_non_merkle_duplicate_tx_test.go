//go:build bvgaps

package model

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// requireNonMerkleDuplicateRejectedAsInvalid asserts the block was rejected by the duplicate rule
// and classified ERR_BLOCK_INVALID, not ERR_BLOCK_CORRUPT.
func requireNonMerkleDuplicateRejectedAsInvalid(t *testing.T, valid bool, err error) {
	t.Helper()

	require.False(t, valid)
	require.Error(t, err)
	require.ErrorContains(t, err, "duplicate transaction", "the rejection must come from the duplicate rule")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid),
		"a non-merkle-pattern duplicate in a bound body must be ERR_BLOCK_INVALID, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err), "must not be ERR_BLOCK_CORRUPT")
}

// TestKnownDefect_BoundBodyWithNonMerkleDuplicateTx_IsInvalidNotCorrupt checks how a duplicate
// transaction is classified when it is not the CVE-2012-2459 odd-node duplication pattern.
//
// Spec: BV-BODY-004 (tracked as GAP-BV-051).
//
// Scenario: Given a block with two full 4-leaf subtrees [coinbase, a, b, X] and [c, d, e, X], where
// X sits at flat positions 3 and 7 in different subtrees (both subtrees have an even leaf count, so
// no tree level has an odd number of nodes and X is not a trailing odd-node duplicate), and the
// header merkle root is computed over exactly this list so the body is bound, When the block is
// validated, Then it is rejected by the duplicate rule as ERR_BLOCK_INVALID and not as
// ERR_BLOCK_CORRUPT. CORRUPT is reserved for the odd-node pattern, where the root is unchanged and
// an honest body exists for the same header; any other duplicate in a bound body means the header
// commits to an invalid block that cannot be repaired by re-downloading.
//
// Today: model/Block.go getAndValidateSubtreesWithDedup / checkDuplicateTransactions return
// NewBlockCorruptError for any duplicate, regardless of binding or pattern.
func TestKnownDefect_BoundBodyWithNonMerkleDuplicateTx_IsInvalidNotCorrupt(t *testing.T) {
	const (
		subtreeCount       = 2
		leavesPerSubtree   = 4
		duplicateLeafIndex = 3
		validationTimeout  = 10 * time.Second
		// Zero placeholder header IDs for the current chain, kept from the original fixture.
		currentChainHeaderIDCount = 11
	)

	// Given: a bound body whose header root commits to the list containing the duplicate.
	duplicateTxHash := randCorruptTestHash(t)

	block, subtrees, subtreeStore := dedupLoadFixtureN(t, &duplicateTxHash, subtreeCount, nil)

	// Independently prove the body is bound and that the duplicate is not the CVE pattern.
	subtreeRoots := []*chainhash.Hash{subtrees[0].RootHash(), subtrees[1].RootHash()}
	require.Equal(t, testBodyMerkleRoot(t, block.CoinbaseTx, subtreeRoots, subtrees[0]).String(), block.Header.HashMerkleRoot.String(),
		"fixture precondition: header root commits to exactly this body")
	require.Len(t, subtrees[0].Nodes, leavesPerSubtree)
	require.Len(t, subtrees[1].Nodes, leavesPerSubtree)
	require.Equal(t, duplicateTxHash, subtrees[0].Nodes[duplicateLeafIndex].Hash)
	require.Equal(t, duplicateTxHash, subtrees[1].Nodes[duplicateLeafIndex].Hash)

	tSettings := test.CreateBaseTestSettings(t)

	ctx, cancel := context.WithTimeout(context.Background(), validationTimeout)
	defer cancel()

	// When
	valid, err := block.Valid(ctx, ulogger.TestLogger{}, subtreeStore, nil, txmap.NewSyncedMap[chainhash.Hash, []uint32](),
		regtestGenesisParentChain(t, block.Header), make([]uint32, currentChainHeaderIDCount), tSettings, nil)

	// Then
	requireNonMerkleDuplicateRejectedAsInvalid(t, valid, err)
}
