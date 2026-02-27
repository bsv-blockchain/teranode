package blockchain

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
)

// MedianTimeBlocks is the number of previous blocks used to calculate MTP (BIP113).
// MTP is the median of the timestamps of the last 11 blocks.
const MedianTimeBlocks = 11

// GetMedianTimePastForHeights returns the MTP for one or more block heights.
// MTP values are read from pre-stored block metadata rather than recomputed on demand.
//
// Parameters:
//   - ctx: Context for the operation
//   - heights: Array of block heights to get MTP for
//
// Returns:
//   - []uint32: Array of MTP values corresponding to input heights (0 for height < CSVHeight or height < 11)
//   - error: Error if block metadata cannot be retrieved
//
// Note: MTP of block N is the median of timestamps from blocks [N-11, N-1] (previous 11 blocks),
// pre-calculated and stored at block persistence time.
func (b *Blockchain) GetMedianTimePastForHeights(ctx context.Context, heights []uint32) ([]uint32, error) {
	if len(heights) == 0 {
		return []uint32{}, nil
	}

	minHeight, maxHeight := heights[0], heights[0]
	for _, h := range heights[1:] {
		if h < minHeight {
			minHeight = h
		}
		if h > maxHeight {
			maxHeight = h
		}
	}

	_, metas, err := b.store.GetBlockHeadersByHeight(ctx, minHeight, maxHeight)
	if err != nil {
		return nil, errors.NewProcessingError("[Blockchain][GetMedianTimePastForHeights] failed to get block headers from %d to %d", minHeight, maxHeight, err)
	}

	mtpByHeight := make(map[uint32]uint32, len(metas))
	for _, meta := range metas {
		mtpByHeight[meta.Height] = meta.MedianTimePast
	}

	mtps := make([]uint32, len(heights))
	for i, height := range heights {
		mtps[i] = mtpByHeight[height]
	}

	return mtps, nil
}
