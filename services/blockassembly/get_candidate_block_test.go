package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetCandidateBlock(t *testing.T) {
	t.Run("candidate not found returns error", func(t *testing.T) {
		server, _ := setupServer(t)

		req := &blockassembly_api.GetCandidateBlockRequest{
			Id: make([]byte, 32),
		}

		resp, err := server.GetCandidateBlock(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "candidate not found")
	})

	t.Run("invalid candidate ID returns error", func(t *testing.T) {
		server, _ := setupServer(t)

		req := &blockassembly_api.GetCandidateBlockRequest{
			Id: []byte{0x01, 0x02}, // too short for chainhash
		}

		resp, err := server.GetCandidateBlock(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("returns block data for valid candidate", func(t *testing.T) {
		server, _ := setupServer(t)
		err := server.blockAssembler.Start(t.Context())
		require.NoError(t, err)

		// Add transactions to create subtrees
		for i := 0; i < 5; i++ {
			txHash := chainhash.HashH([]byte{byte(i), 0xca, 0xfe})
			server.blockAssembler.AddTxBatch([]subtreepkg.Node{{
				Hash:        txHash,
				Fee:         uint64(100),
				SizeInBytes: uint64(250),
			}}, []*subtreepkg.TxInpoints{{}})
		}

		time.Sleep(50 * time.Millisecond)

		// Get a mining candidate to populate the job store
		mc, err := server.GetMiningCandidate(context.Background(), &blockassembly_api.GetMiningCandidateRequest{})
		require.NoError(t, err)
		require.NotNil(t, mc)

		// Now call GetCandidateBlock with that ID
		resp, err := server.GetCandidateBlock(context.Background(), &blockassembly_api.GetCandidateBlockRequest{
			Id: mc.Id,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify the response
		require.Len(t, resp.Header, model.BlockHeaderSize, "header should be 80 bytes")
		require.NotEmpty(t, resp.CoinbaseTx, "coinbase tx should not be empty")
		require.Greater(t, resp.TransactionCount, uint64(0), "should have at least 1 transaction")
	})

	t.Run("FSM not running still works for cached candidate", func(t *testing.T) {
		server, _ := setupServer(t)
		err := server.blockAssembler.Start(t.Context())
		require.NoError(t, err)

		// Get a mining candidate while FSM is running
		mc, err := server.GetMiningCandidate(context.Background(), &blockassembly_api.GetMiningCandidateRequest{})
		require.NoError(t, err)

		// Now mock FSM as not running — GetCandidateBlock should still work
		// because it only reads from the job cache
		mockClient := &blockchain.Mock{}
		mockClient.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(false, nil)
		server.blockchainClient = mockClient

		resp, err := server.GetCandidateBlock(context.Background(), &blockassembly_api.GetCandidateBlockRequest{
			Id: mc.Id,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Header, model.BlockHeaderSize)
	})
}
