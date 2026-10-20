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

// TestGapBV051_NonMerkleDuplicateInBoundBodyIsInvalid asserts BV-BODY-004.
//
// GAP-BV-051: every duplicate transaction ID is classified ERR_BLOCK_CORRUPT. The spec reserves
// CORRUPT for the CVE-2012-2459 odd-node duplication pattern (the root is unchanged, so an honest
// body exists for the same header). Any other duplicate in a BOUND body means the header commits
// to an invalid block and MUST be ERR_BLOCK_INVALID.
//
// Construction: two full 4-leaf subtrees [cb, a, b, X] and [c, d, e, X]. X sits at flat positions
// 3 and 7, in different subtrees, so it is not a trailing odd-node duplicate (both subtrees have
// an even leaf count; no level of the tree has an odd number of nodes). The header root is the
// root computed over exactly this list (dedupLoadFixtureN with headerRoot nil), so the body is
// bound and cannot be repaired by re-downloading. Fails today because model/Block.go
// getAndValidateSubtreesWithDedup / checkDuplicateTransactions return NewBlockCorruptError for
// any duplicate regardless of binding or pattern.
func TestGapBV051_NonMerkleDuplicateInBoundBodyIsInvalid(t *testing.T) {
	dup := randCorruptTestHash(t)

	b, subtrees, store := dedupLoadFixtureN(t, &dup, 2, nil)

	// Independently prove the body is bound and that the duplicate is not the CVE pattern.
	hashes := []*chainhash.Hash{subtrees[0].RootHash(), subtrees[1].RootHash()}
	require.Equal(t, testBodyMerkleRoot(t, b.CoinbaseTx, hashes, subtrees[0]).String(), b.Header.HashMerkleRoot.String(),
		"fixture precondition: header root commits to exactly this body")
	require.Len(t, subtrees[0].Nodes, 4)
	require.Len(t, subtrees[1].Nodes, 4)
	require.Equal(t, dup, subtrees[0].Nodes[3].Hash)
	require.Equal(t, dup, subtrees[1].Nodes[3].Hash)

	tSettings := test.CreateBaseTestSettings(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	valid, err := b.Valid(ctx, ulogger.TestLogger{}, store, nil, txmap.NewSyncedMap[chainhash.Hash, []uint32](),
		regtestGenesisParentChain(t, b.Header), make([]uint32, 11), tSettings, nil)
	require.False(t, valid)
	require.Error(t, err)
	require.ErrorContains(t, err, "duplicate transaction", "the rejection must come from the duplicate rule")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid),
		"BV-BODY-004: a non-merkle-pattern duplicate in a bound body must be ERR_BLOCK_INVALID, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err), "must not be ERR_BLOCK_CORRUPT")
}
