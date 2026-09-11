package blockvalidation

import (
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/require"
)

// emptySubtreeBlob builds a serialized zero-node subtree exactly as go-subtree's reader expects it:
// rootHash[32] | fees[8] | sizeInBytes[8] | numNodes[8]=0 | numConflictingNodes[8]=0. A well-behaved
// producer never writes one, so this stands in for the hand-crafted junk a peer could plant on disk.
func emptySubtreeBlob(root chainhash.Hash) []byte {
	blob := make([]byte, 64)
	copy(blob[:32], root[:])
	binary.LittleEndian.PutUint64(blob[48:56], 0)
	binary.LittleEndian.PutUint64(blob[56:64], 0)
	return blob
}

// TestReadSubtree_RejectsZeroNodeBlob pins the third and last route of the zero-node class
// (bitcoin-sv/teranode#4692). The fetch route is guarded by validateSubtreeLeafCount and the
// subtree-validation local read by loadSubtreeBatch; quick validation has its own local read that
// neither covers, because CheckBlockSubtrees never runs on this route. Without the guard a planted
// zero-node blob is deserialised and returned, and the only thing rejecting it is out of repo and
// only incidental.
//
// Mutation proof: remove the Length() == 0 check and readSubtree returns no error here, reddening
// both assertions.
func TestReadSubtree_RejectsZeroNodeBlob(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	root := chainhash.Hash{0x11}

	require.NoError(t, suite.Server.subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtree, emptySubtreeBlob(root)))

	res := suite.Server.blockValidation.readSubtree(suite.Ctx, block, 0, &root)
	require.Error(t, res.err)
	require.Contains(t, res.err.Error(), "has zero nodes")
}
