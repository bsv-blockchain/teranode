package model

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

//func UpdateTxMinedStatus(ctx context.Context, logger ulogger.Logger, txMetaStore txmeta_store.Store, subtrees []*util.Subtree, blockHeader *BlockHeader) error {
//	return nil
//}

type txMinedStatus interface {
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
}

func UpdateTxMinedStatus(ctx context.Context, logger ulogger.Logger, txMetaStore txMinedStatus, subtrees []*util.Subtree,
	blockHash *chainhash.Hash, blockID uint32) error {

	span, spanCtx := opentracing.StartSpanFromContext(ctx, "UpdateTxMinedStatus")
	defer func() {
		span.Finish()
	}()

	logger.Infof("[UpdateTxMinedStatus][%s] blockID %d for %d subtrees", blockHash.String(), blockID, len(subtrees))

	updateTxMinedStatus := gocore.Config().GetBool("txmeta_store_updateTxMinedStatus", true)
	if !updateTxMinedStatus {
		return nil
	}

	maxMinedRoutines, _ := gocore.Config().GetInt("txmeta_store_maxMinedRoutines", 128)
	maxMinedBatchSize, _ := gocore.Config().GetInt("txmeta_store_maxMinedBatchSize", 1024)

	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(maxMinedRoutines)

	for subtreeIdx, subtree := range subtrees {
		subtreeIdx := subtreeIdx
		subtree := subtree
		g.Go(func() error {
			hashes := make([]*chainhash.Hash, 0, maxMinedBatchSize)
			for idx, node := range subtree.Nodes {
				idx := idx
				node := node

				// Skip the first node in the first subtree, as it is the coinbase tx
				if subtreeIdx == 0 && idx == 0 {
					continue
				}

				hashes = append(hashes, &node.Hash)
				if idx > 0 && idx%maxMinedBatchSize == 0 {
					logger.Debugf("[UpdateTxMinedStatus][%s] SetMinedMulti for %d hashes, batch %d, for subtree %s in block %d", blockHash.String(), len(hashes), idx/maxMinedBatchSize, subtree.RootHash().String(), blockID)
					for retries := 0; retries < 3; retries++ {
						if err := txMetaStore.SetMinedMulti(gCtx, hashes, blockID); err != nil {
							if retries >= 2 {
								return fmt.Errorf("[UpdateTxMinedStatus][%s] error setting mined tx: %v", blockHash.String(), err)
							} else {
								backoff := time.Duration(2^retries) * time.Second
								logger.Warnf("[UpdateTxMinedStatus][%s] error setting mined tx, retrying in %s: %v", blockHash.String(), backoff.String(), err)
								time.Sleep(backoff)
							}
						} else {
							break
						}
					}

					hashes = make([]*chainhash.Hash, 0, maxMinedBatchSize)
				}
			}

			if len(hashes) > 0 {
				logger.Debugf("[UpdateTxMinedStatus][%s] SetMinedMulti for %d hashes, remainder batch, for subtree %s in block %d", blockHash.String(), len(hashes), subtree.RootHash().String(), blockID)
				if err := txMetaStore.SetMinedMulti(gCtx, hashes, blockID); err != nil {
					return fmt.Errorf("[UpdateTxMinedStatus][%s] error setting mined tx: %v", blockHash.String(), err)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("[UpdateTxMinedStatus][%s] error updating tx mined status: %w", blockHash.String(), err)
	}

	logger.Infof("[UpdateTxMinedStatus][%s] blockID %d DONE", blockHash.String(), blockID)

	return nil
}
