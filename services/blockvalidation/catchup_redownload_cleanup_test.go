package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/stretchr/testify/require"
)

// TestRemoveCatchupSubtreeFiles is the freemans13 item 9 fix (bitcoin-sv/teranode#4692): on a failed
// quick validation the block's .subtree blobs must be removed from the store so a fresh re-download
// is not shadowed by stale peer-supplied data left under readSubtree's "already validated" marker.
// This helper is now called on BOTH the plain quick-validation failure AND the corrupt-body path
// (which previously returned WITHOUT deleting, leaving attacker-supplied content in the store to be
// re-applied on every retry).
func TestRemoveCatchupSubtreeFiles(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	ctx := context.Background()

	h1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	h2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")

	require.NoError(t, suite.Server.subtreeStore.Set(ctx, h1[:], fileformat.FileTypeSubtree, []byte("attacker-supplied-1")))
	require.NoError(t, suite.Server.subtreeStore.Set(ctx, h2[:], fileformat.FileTypeSubtree, []byte("attacker-supplied-2")))

	block := &model.Block{Subtrees: []*chainhash.Hash{h1, h2}}

	require.NoError(t, suite.Server.removeCatchupSubtreeFiles(ctx, block),
		"deleting stored subtree blobs must succeed")

	_, err := suite.Server.subtreeStore.Get(ctx, h1[:], fileformat.FileTypeSubtree)
	require.True(t, errors.Is(err, errors.ErrNotFound), "subtree 1 blob must be gone after a failed/corrupt quick validation")
	_, err = suite.Server.subtreeStore.Get(ctx, h2[:], fileformat.FileTypeSubtree)
	require.True(t, errors.Is(err, errors.ErrNotFound), "subtree 2 blob must be gone after a failed/corrupt quick validation")
}

// TestRemoveCatchupSubtreeFiles_MissingIsNotAnError proves the corrupt path is safe when the blobs
// were never written (or already evicted): a missing file is tolerated, not surfaced as an error
// (bitcoin-sv/teranode#4692).
func TestRemoveCatchupSubtreeFiles_MissingIsNotAnError(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	missing, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000009")
	block := &model.Block{Subtrees: []*chainhash.Hash{missing}}

	require.NoError(t, suite.Server.removeCatchupSubtreeFiles(context.Background(), block),
		"a missing subtree blob must not be an error")
}
