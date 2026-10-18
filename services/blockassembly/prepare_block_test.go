package blockassembly

import (
	"bytes"
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func prepareTestBlock(t *testing.T) *model.Block {
	t.Helper()

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	subtree := chainhash.HashH([]byte("prepare-block-subtree"))

	block, err := model.NewBlock(&model.BlockHeader{
		Version:        1,
		HashPrevBlock:  model.GenesisBlockHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Bits:           model.GenesisBlockHeader.Bits,
	}, coinbase, []*chainhash.Hash{&subtree}, 2, 300, 1, 0)
	require.NoError(t, err)

	return block
}

func TestClient_PrepareBlock_SendsTheBlock(t *testing.T) {
	block := prepareTestBlock(t)
	want, err := block.Bytes()
	require.NoError(t, err)

	api := &mockBlockAssemblyAPIClient{}
	api.On("PrepareBlock", mock.Anything, mock.MatchedBy(func(req *blockassembly_api.PrepareBlockRequest) bool {
		return bytes.Equal(req.Block, want)
	}), mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

	require.NoError(t, createTestClient(api, 0).PrepareBlock(context.Background(), block))
	api.AssertExpectations(t)
}

func TestServer_PrepareBlock_HandsTheBlockToTheSubtreeProcessor(t *testing.T) {
	server, _ := setupServer(t)
	block := prepareTestBlock(t)

	stp := &subtreeprocessor.MockSubtreeProcessor{}
	stp.On("PrepareBlock", mock.MatchedBy(func(b *model.Block) bool {
		return b.Hash().IsEqual(block.Hash()) && len(b.Subtrees) == 1 && b.Subtrees[0].IsEqual(block.Subtrees[0])
	})).Return()
	original := server.blockAssembler.subtreeProcessor
	server.blockAssembler.subtreeProcessor = stp
	t.Cleanup(func() { server.blockAssembler.subtreeProcessor = original })

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, err = server.PrepareBlock(context.Background(), &blockassembly_api.PrepareBlockRequest{Block: blockBytes})
	require.NoError(t, err)
	stp.AssertExpectations(t)
}

func TestServer_PrepareBlock_RejectsMalformedBlock(t *testing.T) {
	server, _ := setupServer(t)

	_, err := server.PrepareBlock(context.Background(), &blockassembly_api.PrepareBlockRequest{Block: []byte{1, 2, 3}})
	require.Error(t, err)
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrInvalidArgument), "got %v", err)
}

// Mock.PrepareBlock is a hint the caller does not wait on, so a mock without an expectation
// for it accepts the call instead of failing the test.
func TestMock_PrepareBlockWithoutExpectation(t *testing.T) {
	m := NewMock()
	require.NoError(t, m.PrepareBlock(context.Background(), prepareTestBlock(t)))

	m.On("PrepareBlock", mock.Anything, mock.Anything).Return(errors.NewProcessingError("boom")).Once()
	require.Error(t, m.PrepareBlock(context.Background(), prepareTestBlock(t)))
	m.AssertExpectations(t)
}
