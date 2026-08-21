package subtreevalidation

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// emptySubtreeBlob builds a serialized zero-node subtree exactly as go-subtree's reader expects it:
// rootHash[32] | fees[8] | sizeInBytes[8] | numNodes[8]=0 | numConflictingNodes[8]=0. A well-behaved
// producer never writes one (Subtree.Serialize errors with zero nodes), so this stands in for the
// hand-crafted junk a peer could plant on disk.
func emptySubtreeBlob(root chainhash.Hash) []byte {
	blob := make([]byte, 64)
	copy(blob[:32], root[:])
	binary.LittleEndian.PutUint64(blob[48:56], 0) // numNodes
	binary.LittleEndian.PutUint64(blob[56:64], 0) // numConflictingNodes
	return blob
}

func zeroNodeTestBlock(t *testing.T, subtreeHash *chainhash.Hash) *subtreevalidation_api.CheckBlockSubtreesRequest {
	t.Helper()

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
		Nonce:          0,
	}
	coinbaseTx := &bt.Tx{Version: 1}

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{subtreeHash}, 2, 500, 0, 0)
	require.NoError(t, err)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	return &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: testPeerURL,
	}
}

// TestCheckBlockSubtrees_RejectsZeroNodeLocalRead pins the post-deserialisation guard for the
// LOCAL-READ route (bitcoin-sv/teranode#4692): a zero-node subtree blob already sitting on disk as
// FileTypeSubtreeToCheck is read locally by loadSubtreeBatch (no fetch, no leaf-count check) and must
// be rejected before it can reach block.Valid — otherwise a peer that planted junk on an earlier
// attempt would surface as a corrupt verdict that strikes an innocent peer. This is the route the
// model-side reclassification depends on being closed.
//
// Mutation proof: remove the `subtreeToCheck.Length() == 0` guard in loadSubtreeBatch and the
// zero-node subtree is accepted for first use instead of rejected, so the "subtree has zero nodes"
// assertion reddens.
func TestCheckBlockSubtrees_RejectsZeroNodeLocalRead(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
		mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil).Maybe()

	// A subtree hash the block references. Seed ONLY FileTypeSubtreeToCheck (not FileTypeSubtree), so
	// the existence check marks it missing and loadSubtreeBatch reads the local pending blob.
	subtreeHash := chainhash.Hash{0x01}
	require.NoError(t, server.subtreeStore.Set(context.Background(), subtreeHash[:],
		fileformat.FileTypeSubtreeToCheck, emptySubtreeBlob(subtreeHash)))

	request := zeroNodeTestBlock(t, &subtreeHash)

	_, err := server.CheckBlockSubtrees(context.Background(), request)
	require.Error(t, err)
	require.Contains(t, err.Error(), "subtree has zero nodes",
		"a zero-node subtree read locally must be rejected in CheckBlockSubtrees, not passed to block.Valid")
}

// TestCheckBlockSubtrees_RejectsZeroNodeFetch pins the explicit leafCount==0 check in
// validateSubtreeLeafCount for the FRESH-FETCH route (bitcoin-sv/teranode#4692): a peer serving a
// zero-length subtree body is rejected at the explicit check, BEFORE the
// NewIncompleteTreeByLeafCount constructor. The fetch branch already rejects a zero-node subtree
// today, but only incidentally (log2(0) drives a negative tree height); this test pins that the
// rejection comes from the explicit check by asserting on its message.
//
// Mutation proof: remove the explicit `leafCount == 0` from validateSubtreeLeafCount and the
// rejection still happens — but from the constructor ("failed to create subtree structure"), not the
// explicit check, so the "subtree has zero nodes" assertion reddens. (Asserting merely that an error
// occurs would NOT pin the explicit check, since the incidental constructor error also fails.)
func TestCheckBlockSubtrees_RejectsZeroNodeFetch(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
		mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil).Maybe()

	// Seed nothing, so the subtree is missing locally and loadSubtreeBatch fetches it from the peer.
	subtreeHash := chainhash.Hash{0x02}

	// /subtree/<root> serves an EMPTY body → zero leaf count.
	httpmock.RegisterResponder("GET",
		fmt.Sprintf("%s/subtree/%s", testPeerURL, subtreeHash.String()),
		httpmock.NewBytesResponder(http.StatusOK, []byte{}))

	request := zeroNodeTestBlock(t, &subtreeHash)

	_, err := server.CheckBlockSubtrees(context.Background(), request)
	require.Error(t, err)
	require.Contains(t, err.Error(), "subtree has zero nodes",
		"a zero-node subtree fetch must be rejected at the explicit leaf-count check")
}
