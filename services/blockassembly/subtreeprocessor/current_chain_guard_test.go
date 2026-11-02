package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// A header without meta cannot give a block ID. The resolver must return an error, so the rollback
// takes no exemption from it, instead of dereferencing nil.
func TestCurrentChainGuard_NilMetaIsAnError(t *testing.T) {
	client := &blockchain.Mock{}
	hash := chainhash.HashH([]byte("no-meta"))
	client.On("GetBlockHeader", mock.Anything, &hash).Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), nil)

	guard := CurrentChainGuard(client)
	require.NotNil(t, guard)

	_, err := guard.BlockID(t.Context(), hash)
	require.ErrorContains(t, err, "has no header meta")
}
