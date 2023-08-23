package sql

import (
	"context"
	"errors"
	"fmt"

	"github.com/TAAL-GmbH/arc/blocktx/store"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (s *SQL) GetBlockHeaders(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []uint32, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blockchain").NewStat("GetBlock").AddTime(start)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	blockHeaders := make([]*model.BlockHeader, 0, numberOfHeaders)
	heights := make([]uint32, 0, numberOfHeaders)

	blockHeader, height, err := s.GetBlockHeader(ctx, blockHashFrom)
	if err != nil {
		if errors.Is(err, store.ErrBlockNotFound) {
			// could not find header, return empty list
			return blockHeaders, []uint32{}, nil
		}
		return nil, nil, fmt.Errorf("failed to get header: %w", err)
	}

	blockHeaders = append(blockHeaders, blockHeader)
	heights = append(heights, height)

	for i := uint64(1); i < numberOfHeaders; i++ {
		blockHeader, height, err = s.GetBlockHeader(ctx, blockHeaders[i-1].HashPrevBlock)
		if err != nil {
			if errors.Is(err, store.ErrBlockNotFound) {
				break
			} else {
				return nil, nil, fmt.Errorf("failed to get header: %w", err)
			}
		}
		blockHeaders = append(blockHeaders, blockHeader)
		heights = append(heights, height)
	}

	return blockHeaders, heights, nil
}
