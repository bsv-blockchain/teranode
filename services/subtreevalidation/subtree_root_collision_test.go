package subtreevalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSubtreeRootIsNotUniqueUnderDuplicateLastMutation pins the fact that licenses the corrupt-body
// blob cleanup on the subtree-validation branch (bitcoin-sv/teranode#4692): a subtree's merkle root
// does NOT uniquely determine its node list.
//
// The merkle tree's duplicate-last-when-odd rule means [a,b,c] and [a,b,c,c] hash to the SAME root:
// both compute H(H(a,b), H(c,c)). So the fetch-side root check — the only content check performed
// before FileTypeSubtreeToCheck is stored (check_block_subtrees.go) — cannot tell an honest node list
// from a CVE-2012-2459 duplicate-last mutation of it. This is the subtree-level instance of what
// model.CheckSubtreeSlicesForDuplicateTxs documents at the block level.
//
// Two consequences follow, and both are load-bearing elsewhere:
//
//   - A stored FileTypeSubtreeToCheck can hold a mutated list under an honest hash. The duplicate is
//     only caught later, by ValidateSubtreeInternal's own scan, which returns BEFORE storeSubtreeFiles
//     writes the scanned FileTypeSubtree — so on that branch the mutated fallback is the only local
//     copy, and deleting it is what lets an honest re-delivery recover.
//   - "Bound to its key" therefore does NOT imply "a re-fetch returns identical bytes". Any argument
//     that a delete can only force a re-fetch of byte-identical data is false for this class.
func TestSubtreeRootIsNotUniqueUnderDuplicateLastMutation(t *testing.T) {
	a := chainhash.HashH([]byte("tx-a"))
	b := chainhash.HashH([]byte("tx-b"))
	c := chainhash.HashH([]byte("tx-c"))

	honest, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, honest.AddNode(a, 1, 1))
	require.NoError(t, honest.AddNode(b, 2, 2))
	require.NoError(t, honest.AddNode(c, 3, 3))

	// The mutation: duplicate the trailing node. This is exactly what the merkle tree does internally
	// to pad an odd level, which is why the root is preserved.
	mutated, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, mutated.AddNode(a, 1, 1))
	require.NoError(t, mutated.AddNode(b, 2, 2))
	require.NoError(t, mutated.AddNode(c, 3, 3))
	require.NoError(t, mutated.AddNode(c, 3, 3))

	require.Equal(t, 3, honest.Length())
	require.Equal(t, 4, mutated.Length(), "the mutated list must genuinely carry an extra node")

	honestRoot := honest.RootHash()
	mutatedRoot := mutated.RootHash()
	require.NotNil(t, honestRoot)
	require.NotNil(t, mutatedRoot)

	// THE POINT: same root, different content. A root check cannot separate these two.
	require.True(t, honestRoot.IsEqual(mutatedRoot),
		"a duplicate-last mutation must preserve the subtree root, or the fetch-side root check would already catch it")

	honestBytes, err := honest.Serialize()
	require.NoError(t, err)
	mutatedBytes, err := mutated.Serialize()
	require.NoError(t, err)
	require.NotEqual(t, honestBytes, mutatedBytes,
		"the two lists must serialize differently, or there would be nothing to re-fetch")

	// And the scan that DOES separate them is the duplicate scan, which is why a FileTypeSubtree
	// written by storeSubtreeFiles (scanned first) is trustworthy where the fetch-side marker is not.
	require.Error(t, model.CheckSubtreeSlicesForDuplicateTxs([]*subtreepkg.Subtree{mutated}),
		"the duplicate scan is what catches the mutation the root check cannot")
	require.NoError(t, model.CheckSubtreeSlicesForDuplicateTxs([]*subtreepkg.Subtree{honest}))
}

// shortCircuitTestBlock builds a one-subtree block request for the gate test below.
func shortCircuitTestBlock(t *testing.T, subtreeHash *chainhash.Hash) *subtreevalidation_api.CheckBlockSubtreesRequest {
	t.Helper()

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
		Nonce:          0,
	}

	block, err := model.NewBlock(header, &bt.Tx{Version: 1}, []*chainhash.Hash{subtreeHash}, 2, 500, 0, 0)
	require.NoError(t, err)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	return &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: testPeerURL,
	}
}

// TestCheckBlockSubtrees_ShortCircuitRequiresFileTypeSubtree pins the REACHABILITY INVARIANT that
// makes the block.Valid merkle branch's missing cleanup inert (bitcoin-sv/teranode#4692): a block can
// only short-circuit past subtree validation — and so reach block.Valid without any subtree work —
// when FileTypeSubtree is present for every subtree it names.
//
// It has to live here, against the REAL gate. services/blockvalidation only ever holds the
// subtreeValidationClient interface, so no fixture there can execute this code.
//
// The observable is u.blockchainClient.GetFSMCurrentState, which sits AFTER the "all subtrees already
// exist" early return and is unconditional. Nothing touches blockchainClient before that return, so
// whether it was called separates the two paths exactly, with no CSVHeight fixture needed (the MTP
// call would also work but only above CSVHeight). httpmock is active with NO responders registered,
// so any peer fetch would fail loudly rather than pass silently.
func TestCheckBlockSubtrees_ShortCircuitRequiresFileTypeSubtree(t *testing.T) {
	// A real, serializable one-node subtree, so the blob on disk is well-formed whichever file type
	// it is stored under.
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(chainhash.HashH([]byte("tx-1")), 1, 1))

	subtreeHash := subtree.RootHash()
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	t.Run("FileTypeSubtree present: short-circuits with no subtree work at all", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		require.NoError(t, server.subtreeStore.Set(context.Background(), subtreeHash[:],
			fileformat.FileTypeSubtree, subtreeBytes))

		resp, err := server.CheckBlockSubtrees(context.Background(), shortCircuitTestBlock(t, subtreeHash))
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.Blessed)

		// The early return fired: execution never reached the FSM-state read below it.
		server.blockchainClient.(*blockchain.Mock).AssertNotCalled(t, "GetFSMCurrentState", mock.Anything)
		require.Zero(t, httpmock.GetTotalCallCount(), "no peer fetch may happen on the short-circuit path")
	})

	t.Run("only FileTypeSubtreeToCheck present: does NOT short-circuit", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Reached only on the non-short-circuit path, so it needs an expectation here and deliberately
		// has none in the sibling sub-test above.
		server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
			mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1, 2, 3}, nil).Maybe()

		// The fallback marker alone. The missing-subtree gate keys on FileTypeSubtree ONLY, so this
		// hash must still be treated as missing — which is precisely why a block whose only local copy
		// is this marker cannot reach block.Valid without going through subtree validation first.
		require.NoError(t, server.subtreeStore.Set(context.Background(), subtreeHash[:],
			fileformat.FileTypeSubtreeToCheck, subtreeBytes))

		// The outcome of the full validation run is deliberately NOT asserted: this fixture has no real
		// UTXO/tx-meta stack, so it may fail downstream. The claim under test is only that execution
		// proceeded PAST the early return, which the observable below establishes on its own. The
		// write-side half of the invariant — that a gate which proceeds leaves FileTypeSubtree behind,
		// or returns an error to the subtree-validation corrupt branch — is a property of
		// storeSubtreeFiles and is covered where that runs.
		_, _ = server.CheckBlockSubtrees(context.Background(), shortCircuitTestBlock(t, subtreeHash))

		server.blockchainClient.(*blockchain.Mock).AssertCalled(t, "GetFSMCurrentState", mock.Anything)
	})
}
