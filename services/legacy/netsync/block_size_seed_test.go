package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// After a restart the size tracker held only the blocks that completed since the start. On
// 2026-10-08 at 09:28 its largest block was 193 MB at heights where blocks are up to 4 GB, so a
// 4 MB/s peer looked in time for far blocks and took 16 of them. The tracker starts with the sizes
// of the last blocks of the chain.
func TestTheBlockSizesStartFromTheChain(t *testing.T) {
	sm := schedulerManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)

	tip := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	metas := make([]*model.BlockHeaderMeta, 0, largestSizeSamples)
	headers := make([]*model.BlockHeader, 0, largestSizeSamples)

	// Newest first, as GetBlockHeaders returns them: one 4 GB block among 100 MB blocks.
	for i := range largestSizeSamples {
		size := uint64(100_000_000)
		if i == 50 {
			size = 4_000_000_000
		}

		metas = append(metas, &model.BlockHeaderMeta{Height: uint32(760_000 - i), SizeInBytes: size})
		headers = append(headers, tip)
	}

	client := &blockchain2.Mock{}
	client.On("GetBestBlockHeader", mock.Anything).Return(tip, &model.BlockHeaderMeta{Height: 760_000}, nil)
	client.On("GetBlockHeaders", mock.Anything, mock.Anything, uint64(largestSizeSamples)).Return(headers, metas, nil)
	sm.blockchainClient = client

	sm.seedBlockSizes(context.Background())

	require.Equal(t, int64(4_000_000_000), sm.blockSizeTracker.largestRecentSize())
	require.Equal(t, int64(100_000_000), sm.blockSizeTracker.getAverageSize(), "the mean of the last 10 blocks, all 100 MB")
}
