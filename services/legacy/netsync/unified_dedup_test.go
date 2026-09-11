package netsync

// Tests for the CVE-2012-2459 duplicate-transaction check on the unified
// below-checkpoint route.
//
// CheckMerkleRoot cannot detect duplicate-last-when-odd mutations. These
// tests exercise the separate subtree dedup predicate; the real delivery path
// is covered by TestHandleBlockMsg_VerifiedHeaderBodyCommitment.

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// makeTestSubtree builds a *subtreepkg.Subtree whose Nodes contain exactly the
// provided hashes, in order. Callers in this file use it to construct both
// clean and CVE-mutated slice sets.
func makeTestSubtree(hashes ...chainhash.Hash) *subtreepkg.Subtree {
	st := &subtreepkg.Subtree{}
	for _, h := range hashes {
		h := h
		st.Nodes = append(st.Nodes, subtreepkg.Node{Hash: h})
	}
	return st
}

// makeUniqueHash returns a deterministic non-placeholder hash seeded by n.
func makeUniqueHash(n byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = n
	h[31] = 0xFF ^ n
	return h
}

// TestUnifiedRouteDedup_CleanBlock verifies that the dedup guard passes a block
// whose subtree slices contain no duplicate hashes. This is the expected
// steady-state: every real block should pass.
func TestUnifiedRouteDedup_CleanBlock(t *testing.T) {
	// Slot [0][0] = coinbase placeholder (all-0xFF), remaining slots = unique hashes.
	slices := []*subtreepkg.Subtree{
		makeTestSubtree(
			subtreepkg.CoinbasePlaceholderHashValue,
			makeUniqueHash(0x01),
			makeUniqueHash(0x02),
		),
		makeTestSubtree(
			makeUniqueHash(0x03),
			makeUniqueHash(0x04),
		),
	}

	err := model.CheckSubtreeSlicesForDuplicateTxs(slices)
	require.NoError(t, err, "clean subtree slices must pass the CVE-2012-2459 dedup guard")
}

// TestUnifiedRouteDedup_DuplicateTxRejected is the RED-then-GREEN discriminator
// for the CVE-2012-2459 fix: it constructs a slice set where one transaction
// hash appears in two different subtrees (the CVE mutation pattern), then
// asserts that the guard returns a BlockInvalidError. Before the fix was
// applied, HandleBlockDirect had NO dedup check, so this scenario would have
// been silently accepted, potentially corrupting the UTXO set when
// createAndSpendUTXOsForBatch double-processed the duplicated tx.
func TestUnifiedRouteDedup_DuplicateTxRejected(t *testing.T) {
	// tx_dup appears in both subtrees — the CVE-2012-2459 mutation pattern.
	dupHash := makeUniqueHash(0xDE)

	slices := []*subtreepkg.Subtree{
		// First subtree: coinbase placeholder + tx_dup + another unique tx.
		makeTestSubtree(
			subtreepkg.CoinbasePlaceholderHashValue,
			dupHash,
			makeUniqueHash(0x01),
		),
		// Second subtree: tx_dup AGAIN — the duplicate.
		makeTestSubtree(
			dupHash,
			makeUniqueHash(0x02),
		),
	}

	err := model.CheckSubtreeSlicesForDuplicateTxs(slices)
	require.Error(t, err, "duplicate transaction hash must be rejected by the CVE-2012-2459 guard")

	var terr *errors.Error
	require.ErrorAs(t, err, &terr, "error must be a teranode *Error")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "error kind must be BlockInvalid")
}

// TestUnifiedRouteDedup_CVE2012_DuplicateLastWhenOdd simulates the specific
// CVE-2012-2459 scenario: a block where the last transaction is duplicated
// (the "duplicate-last-when-odd" construction). This is the exact mutation
// that preserves the merkle root while inserting a repeated hash. The guard
// must catch it even though CheckMerkleRoot would pass for such a block.
func TestUnifiedRouteDedup_CVE2012_DuplicateLastWhenOdd(t *testing.T) {
	lastTx := makeUniqueHash(0xAD)

	// Odd-count subtree where the last tx hash is then appended again,
	// simulating what the CVE mutation does to an odd-leaf subtree.
	slices := []*subtreepkg.Subtree{
		makeTestSubtree(
			subtreepkg.CoinbasePlaceholderHashValue,
			makeUniqueHash(0x01),
			lastTx,
			lastTx, // duplicated last tx — CVE-2012-2459 pattern
		),
	}

	err := model.CheckSubtreeSlicesForDuplicateTxs(slices)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid))
}
