package model

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// A released block now leaves nil entries in SubtreeSlices rather than gutted
// husks, and the readers below index that slice WITHOUT holding
// subtreeSlicesMu. The mutex serialises writers only, so an eviction that fires
// while a validation goroutine is mid-flight can nil an entry underneath it.
//
// Two properties matter, and each test asserts both:
//
//  1. No panic. Before these guards, a nil entry reached
//     Subtree.RootHash().String() (a value receiver on the returned nil
//     *chainhash.Hash), which panics and kills the process — there is no
//     recover in the errgroup goroutines these run in.
//
//  2. The error is TRANSIENT, not consensus. A released-mid-validation block is
//     a memory-management artifact, never a statement about the block's
//     validity. Returning ErrBlockInvalid here would re-create the 2026-08-11
//     incident this branch exists to fix: a perfectly valid block permanently
//     invalidated on-chain. Every guard must yield a processing error so the
//     block is requeued and revalidated from the store.
//
// The guards capture the pointer into a local, test the local, then use the
// local. Re-reading b.SubtreeSlices[i] after the check would leave the same
// nil-deref window open.

// newTestBlockWithSubtreeSlices builds a minimal block carrying the given
// subtree slice entries. Entries may be nil — that is the point.
func newTestBlockWithSubtreeSlices(t *testing.T, slices []*subtreepkg.Subtree) *Block {
	t.Helper()

	hashes := make([]*chainhash.Hash, 0, len(slices))

	for _, st := range slices {
		if st != nil {
			hashes = append(hashes, st.RootHash())
			continue
		}

		// A nil entry still needs a hash in b.Subtrees: the readers size their
		// work from len(b.Subtrees), and the real block always has one. Nothing
		// reads the value, so a fixed placeholder is better than a random hash
		// — it cannot pass on a lucky draw.
		hashes = append(hashes, &chainhash.Hash{})
	}

	b, err := NewBlock(newTestBlockHeader(t), newTestCoinbaseTx(t), hashes, 2, 123, 0, 0)
	require.NoError(t, err)

	b.SubtreeSlices = slices

	return b
}

// newCompleteTestSubtree returns a complete 2-leaf subtree: coinbase
// placeholder plus one real transaction.
func newCompleteTestSubtree(t *testing.T) *subtreepkg.Subtree {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	raw := make([]byte, 32)
	_, _ = rand.Read(raw)

	txHash, err := chainhash.NewHash(raw)
	require.NoError(t, err)
	require.NoError(t, st.AddNode(*txHash, 1, 0))

	return st
}

// requireTransientError asserts the guard returned a processing (retryable)
// error rather than a consensus verdict.
func requireTransientError(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err, "a nil subtree entry must be reported, not ignored")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid),
		"a subtree released mid-validation is a transient condition, never grounds for invalidating the block on-chain")
	require.True(t, errors.Is(err, errors.ErrProcessing),
		"expected a processing error so the block is requeued, got: %v", err)
}

func TestCheckDuplicateTransactionsInSubtree_NilSubtreeIsTransient(t *testing.T) {
	b := newTestBlockWithSubtreeSlices(t, []*subtreepkg.Subtree{newCompleteTestSubtree(t)})

	require.NotPanics(t, func() {
		requireTransientError(t, b.checkDuplicateTransactionsInSubtree(nil, 0, 2))
	})
}

func TestValidateSubtree_NilSubtreeIsTransient(t *testing.T) {
	b := newTestBlockWithSubtreeSlices(t, []*subtreepkg.Subtree{newCompleteTestSubtree(t)})

	// deps and validationCtx are deliberately nil: the guard must fire before
	// anything touches them, and before the tracing log line dereferences the
	// subtree's root hash.
	require.NotPanics(t, func() {
		requireTransientError(t, b.validateSubtree(context.Background(), ulogger.TestLogger{}, nil, nil, nil, 0))
	})
}

func TestCheckBlockRewardAndFees_NilSubtreeEntryIsTransient(t *testing.T) {
	b := newTestBlockWithSubtreeSlices(t, []*subtreepkg.Subtree{newCompleteTestSubtree(t), nil})
	b.Height = 100

	require.NotPanics(t, func() {
		requireTransientError(t, b.checkBlockRewardAndFees(nil, false, false))
	})
}

func TestCheckDuplicateTransactions_NilSubtreeEntryIsTransient(t *testing.T) {
	t.Run("nil first entry", func(t *testing.T) {
		b := newTestBlockWithSubtreeSlices(t, []*subtreepkg.Subtree{nil})

		require.NotPanics(t, func() {
			requireTransientError(t, b.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 1, nil))
		})
	})

	t.Run("nil later entry", func(t *testing.T) {
		b := newTestBlockWithSubtreeSlices(t, []*subtreepkg.Subtree{newCompleteTestSubtree(t), nil})

		require.NotPanics(t, func() {
			requireTransientError(t, b.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 1, nil))
		})
	})
}

// TestCheckMerkleRoot_NilSubtreeEntryIsTransient pins the OUTCOME for a nil
// entry in CheckMerkleRoot — a processing error, never a consensus verdict.
//
// It deliberately does not claim to pin CheckMerkleRoot's own entry-nil guard,
// and cannot: that guard is defence in depth rather than the load-bearing
// check. Removing it keeps this test green from either subtree position,
// because go-subtree degrades gracefully on a nil receiver — RootHash returns
// a nil pointer, which the rootHash == nil check below catches, and
// RootHashWithReplaceRootNode returns an error rather than panicking. Both
// were confirmed by removing the guard and re-running. Keep the guard anyway:
// it fails fast with the index in the message, and it does not depend on a
// third-party package's nil-receiver behaviour staying charitable.
func TestCheckMerkleRoot_NilSubtreeEntryIsTransient(t *testing.T) {
	b := newTestBlockWithSubtreeSlices(t, []*subtreepkg.Subtree{newCompleteTestSubtree(t), nil})

	require.NotPanics(t, func() {
		requireTransientError(t, b.CheckMerkleRoot(context.Background()))
	})
}
