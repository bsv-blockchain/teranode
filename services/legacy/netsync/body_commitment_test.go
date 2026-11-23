package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	legacychain "github.com/bsv-blockchain/teranode/services/legacy/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/stretchr/testify/require"
)

// bodyCommitment mirrors the small model passed by HandleBlockDirect. The
// constructor is safe for these small fixtures; production reuses its decode.
func bodyCommitment(t *testing.T, block *bsvutil.Block) *model.Block {
	t.Helper()
	commitment, err := model.NewBlockFromMsgBlock(block.MsgBlock(), nil)
	require.NoError(t, err)
	commitment.Subtrees = nil
	commitment.SubtreeSlices = nil
	return commitment
}

func setBodyMerkleRoot(block *wire.MsgBlock) {
	roots := legacychain.BuildMerkleTreeStore(bsvutil.NewBlock(block).Transactions())
	block.Header.MerkleRoot = *roots[len(roots)-1]
}
