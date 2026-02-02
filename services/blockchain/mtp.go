package blockchain

import (
	"context"
	"time"

	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// MedianTimeBlocks is the number of previous blocks used to calculate MTP (BIP113).
// MTP is the median of the timestamps of the last 11 blocks.
const MedianTimeBlocks = 11

// CalculateMedianTimePastForHeight calculates the Median Time Past (MTP) for a given block height.
// MTP is defined as the median of the timestamps of the previous 11 blocks (BIP113).
//
// BIP113 (Median Time Past) was activated as part of the CSV softfork at a specific block height
// on each network (mainnet: 419328, testnet3: 770112, etc.). Before this activation height,
// MTP was not used and this function returns 0.
//
// Parameters:
//   - ctx: Context for the operation
//   - height: The block height to calculate MTP for
//
// Returns:
//   - uint32: The MTP value as Unix timestamp, or 0 if height < CSVHeight or height < 11
//   - error: Error if block headers cannot be retrieved or MTP cannot be calculated
//
// Note: MTP of block N is the median of timestamps from blocks [N-11, N-1] (previous 11 blocks).
func (b *Blockchain) CalculateMedianTimePastForHeight(ctx context.Context, height uint32) (uint32, error) {
	// BIP113 is only active from CSVHeight onwards
	// Before CSVHeight, MTP was not used
	if height < b.settings.ChainCfgParams.CSVHeight {
		return 0, nil
	}

	// MTP requires at least 11 previous blocks
	// For early blocks (height < 11), return 0
	if height < MedianTimeBlocks {
		return 0, nil
	}

	// Calculate the range: [height-11, height-1] (previous 11 blocks)
	startHeight := height - MedianTimeBlocks
	endHeight := height - 1

	// Fetch block headers for the range
	headers, _, err := b.store.GetBlockHeadersByHeight(ctx, startHeight, endHeight)
	if err != nil {
		return 0, errors.NewProcessingError("[Blockchain][CalculateMedianTimePastForHeight] failed to get block headers from %d to %d", startHeight, endHeight, err)
	}

	// Verify we got exactly 11 headers
	if len(headers) != MedianTimeBlocks {
		return 0, errors.NewProcessingError("[Blockchain][CalculateMedianTimePastForHeight] expected %d headers, got %d", MedianTimeBlocks, len(headers))
	}

	// Extract timestamps from headers
	timestamps := make([]time.Time, MedianTimeBlocks)
	for i, header := range headers {
		timestamps[i] = time.Unix(int64(header.Timestamp), 0)
	}

	// Calculate median timestamp using existing function
	medianTime, err := model.CalculateMedianTimestamp(timestamps)
	if err != nil {
		return 0, errors.NewProcessingError("[Blockchain][CalculateMedianTimePastForHeight] failed to calculate median timestamp", err)
	}

	// Convert to uint32 (Unix timestamp)
	mtpUint32, err := safeconversion.TimeToUint32(*medianTime)
	if err != nil {
		return 0, errors.NewProcessingError("[Blockchain][CalculateMedianTimePastForHeight] failed to convert median time to uint32", err)
	}

	return mtpUint32, nil
}

// CalculateMedianTimePastForHeights calculates MTP for multiple block heights in batch.
// This is more efficient than calling CalculateMedianTimePastForHeight multiple times.
//
// Parameters:
//   - ctx: Context for the operation
//   - heights: Array of block heights to calculate MTP for
//
// Returns:
//   - []uint32: Array of MTP values corresponding to input heights (0 for height < 11)
//   - error: Error if any MTP calculation fails
func (b *Blockchain) CalculateMedianTimePastForHeights(ctx context.Context, heights []uint32) ([]uint32, error) {
	if len(heights) == 0 {
		return []uint32{}, nil
	}

	mtps := make([]uint32, len(heights))

	// Calculate MTP for each height
	// TODO: Optimize by batching header fetches for overlapping ranges
	for i, height := range heights {
		mtp, err := b.CalculateMedianTimePastForHeight(ctx, height)
		if err != nil {
			return nil, err
		}
		mtps[i] = mtp
	}

	return mtps, nil
}
