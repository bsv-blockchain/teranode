package sql

import (
	"context"
	"errors"
	"fmt"

	"github.com/TAAL-GmbH/arc/blocktx/store"
	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (s *SQL) GetBlockHeaders(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blockchain").NewStat("GetBlock").AddTime(start)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	blockHeaders := make([]*model.BlockHeader, 0, numberOfHeaders)

	blockHeader, err := s.GetHeader(ctx, blockHashFrom)
	if err != nil {
		if errors.Is(err, store.ErrBlockNotFound) {
			// could not find header, return empty list
			return blockHeaders, nil
		}
		return nil, fmt.Errorf("failed to get header: %w", err)
	}

	blockHeaders = append(blockHeaders, blockHeader)

	for i := uint64(1); i < numberOfHeaders; i++ {
		blockHeader, err = s.GetHeader(ctx, blockHeaders[i-1].HashPrevBlock)
		if err != nil {
			if errors.Is(err, store.ErrBlockNotFound) {
				break
			} else {
				return nil, fmt.Errorf("failed to get header: %w", err)
			}
		}
		blockHeaders = append(blockHeaders, blockHeader)
	}

	return blockHeaders, nil
}
