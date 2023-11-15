package model

import (
	"context"
	"fmt"

	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

func UpdateTxMinedStatus(ctx context.Context, txMetaStore txmeta_store.Store, subtrees []*util.Subtree, blockHeader *BlockHeader) error {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockAssembly:UpdateTxMinedStatus")
	defer func() {
		span.Finish()
	}()

	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(1024)

	maxBatchSize, _ := gocore.Config().GetInt("txmeta_store_maxMinedBatchSize", 1024)

	blockHeaderHash := blockHeader.Hash()
	for _, subtree := range subtrees {
		subtree := subtree
		g.Go(func() error {
			hashes := make([]*chainhash.Hash, 0, maxBatchSize)
			for idx, node := range subtree.Nodes {
				hashes = append(hashes, &node.Hash)
				if idx > 0 && idx%maxBatchSize == 0 {
					if err := txMetaStore.SetMinedMulti(gCtx, hashes, blockHeaderHash); err != nil {
						return fmt.Errorf("[BlockAssembly] error setting mined tx: %v", err)
					}
					hashes = make([]*chainhash.Hash, 0, maxBatchSize)
				}
			}

			if len(hashes) > 0 {
				if err := txMetaStore.SetMinedMulti(gCtx, hashes, blockHeaderHash); err != nil {
					return fmt.Errorf("[BlockAssembly] error setting mined tx: %v", err)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("[BlockAssembly] error updating tx mined status: %w", err)
	}

	return nil
}
