package blockchain

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// MedianTimeBlocks is the number of previous blocks used to calculate MTP (BIP113).
// MTP is the median of the timestamps of the last 11 blocks.
const MedianTimeBlocks = 11

// computeMTPForMissingHeight computes stored_mtp(targetHeight) = median([targetHeight-11, targetHeight-1])
// from the block_time values already present in metas, for a block that is not yet persisted to the DB.
// This resolves the off-by-one in BIP68 block-time MTP: stored_mtp(N) is only written when block N is
// inserted, but all predecessor blocks [N-11, N-1] are already in the database and in metas.
// Returns 0 without error when there is insufficient data (e.g. targetHeight < 11).
func computeMTPForMissingHeight(metas []*model.BlockHeaderMeta, targetHeight uint32) (uint32, error) {
	if targetHeight < uint32(MedianTimeBlocks) {
		return 0, nil
	}

	startHeight := targetHeight - uint32(MedianTimeBlocks)
	times := make([]time.Time, 0, MedianTimeBlocks)
	for _, meta := range metas {
		if meta.Height >= startHeight && meta.Height < targetHeight {
			times = append(times, time.Unix(int64(meta.BlockTime), 0))
		}
	}

	if len(times) != MedianTimeBlocks {
		return 0, nil
	}

	medianTime, err := model.CalculateMedianTimestamp(times)
	if err != nil {
		return 0, errors.NewProcessingError("failed to compute MTP for missing height %d", targetHeight, err)
	}

	return uint32(medianTime.Unix()), nil
}

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

	// If the highest requested height is not in the database (block not yet persisted),
	// compute its MTP on the fly from the block_time values already fetched.
	if _, ok := mtpByHeight[maxHeight]; !ok && maxHeight >= uint32(MedianTimeBlocks) {
		computed, err := computeMTPForMissingHeight(metas, maxHeight)
		if err != nil {
			return nil, err
		}
		mtpByHeight[maxHeight] = computed
	}

	mtps := make([]uint32, len(heights))
	for i, height := range heights {
		mtps[i] = mtpByHeight[height]
	}

	return mtps, nil
}

// GetMedianTimePastRange returns the MTP values for all blocks in [fromHeight, toHeight].
// Returns a dense slice where result[i] = MTP for height (fromHeight + i).
// Blocks missing from the canonical chain (e.g. heights below 11) are left as zero.
func (b *Blockchain) GetMedianTimePastRange(ctx context.Context, fromHeight, toHeight uint32) ([]uint32, error) {
	if toHeight < fromHeight {
		return []uint32{}, nil
	}

	_, metas, err := b.store.GetBlockHeadersByHeight(ctx, fromHeight, toHeight)
	if err != nil {
		return nil, errors.NewProcessingError("[Blockchain][GetMedianTimePastRange] failed to get block headers from %d to %d", fromHeight, toHeight, err)
	}

	result := make([]uint32, toHeight-fromHeight+1)
	for _, meta := range metas {
		result[meta.Height-fromHeight] = meta.MedianTimePast
	}

	// If the top height is not in the database (block not yet persisted), compute its MTP
	// on the fly from the block_time values of the preceding 11 blocks already in metas.
	topMissing := len(metas) == 0 || metas[len(metas)-1].Height < toHeight
	if topMissing && toHeight >= uint32(MedianTimeBlocks) {
		computed, err := computeMTPForMissingHeight(metas, toHeight)
		if err != nil {
			return nil, err
		}
		result[toHeight-fromHeight] = computed
	}

	return result, nil
}
