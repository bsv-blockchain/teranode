package model

import (
	"context"
	"fmt"

	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/sync/errgroup"
)

func UpdateTxMinedStatus(ctx context.Context, txMetaStore txmeta_store.Store, subtrees []*util.Subtree, blockHeader *BlockHeader) error {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockAssembly:UpdateTxMinedStatus")
	defer func() {
		span.Finish()
	}()

	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(1024)

	blockHeaderHash := blockHeader.Hash()
	for _, subtree := range subtrees {
		for _, node := range subtree.Nodes {
			hash := node.Hash
			g.Go(func() error {
				if err := txMetaStore.SetMined(gCtx, &hash, blockHeaderHash); err != nil {
					return fmt.Errorf("[BlockAssembly] error setting mined tx: %v", err)
				}

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("[BlockAssembly] error updating tx mined status: %w", err)
	}

	return nil
}
