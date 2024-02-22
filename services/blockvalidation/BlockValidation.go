package blockvalidation

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ordishs/go-utils/expiringmap"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/deduplicator"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type BlockValidation struct {
	logger              ulogger.Logger
	blockchainClient    blockchain.ClientI
	subtreeStore        blob.Store
	subtreeTTL          time.Duration
	txStore             blob.Store
	txMetaStore         txmeta.Store
	minedBlockStore     *txmetacache.ImprovedCache
	validatorClient     validator.Interface
	subtreeDeDuplicator *deduplicator.DeDuplicator
	optimisticMining    bool
	localSetMined       bool
	lastValidatedBlocks *expiringmap.ExpiringMap[chainhash.Hash, *model.Block] // map of block hashes that have been validated
}

type missingTx struct {
	tx  *bt.Tx
	idx int
}
type missingTxHash struct {
	hash *chainhash.Hash
	idx  int
}

func NewBlockValidation(logger ulogger.Logger, blockchainClient blockchain.ClientI, subtreeStore blob.Store,
	txStore blob.Store, txMetaStore txmeta.Store, validatorClient validator.Interface) *BlockValidation {

	subtreeTTLMinutes, _ := gocore.Config().GetInt("blockvalidation_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	optimisticMining := gocore.Config().GetBool("optimisticMining", true)

	blockMinedCacheMaxMB, _ := gocore.Config().GetInt("blockMinedCacheMaxMB", 128)

	bv := &BlockValidation{
		logger:              logger,
		blockchainClient:    blockchainClient,
		subtreeStore:        subtreeStore,
		subtreeTTL:          subtreeTTL,
		txStore:             txStore,
		txMetaStore:         txMetaStore,
		minedBlockStore:     txmetacache.NewImprovedCache(blockMinedCacheMaxMB * 1024 * 1024),
		validatorClient:     validatorClient,
		subtreeDeDuplicator: deduplicator.New(subtreeTTL),
		optimisticMining:    optimisticMining,
		localSetMined:       gocore.Config().GetBool("blockvalidation_localSetMined", false),
		lastValidatedBlocks: expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
	}

	if bv.localSetMined {
		// start a blockchain listener and process all the blocks that are mined into the txmetastore
		// this should only be done when shared storage is available, and the block validation has access to all subtrees locally
		ctx := context.Background()
		go func() {
			for {
				blockchainSubscription, err := bv.blockchainClient.Subscribe(ctx, "blockvalidation")
				if err != nil {
					logger.Errorf("[BlockValidation:localSetMined] failed to subscribe to blockchain: %s", err)

					// backoff for 5 seconds and try again
					time.Sleep(5 * time.Second)
					continue
				}

				for notification := range blockchainSubscription {
					if notification == nil {
						continue
					}

					if notification.Type == model.NotificationType_Block {
						startTime := time.Now()
						logger.Infof("[BlockValidation:localSetMined][%s] block localSetTxMined", notification.Hash.String())
						err = bv.localSetTxMined(ctx, notification.Hash)
						if err != nil {
							logger.Errorf("[BlockValidation][%s] failed localSetTxMined: %s", notification.Hash.String(), err)
						}
						logger.Infof("[BlockValidation:localSetMined][%s] block localSetTxMined DONE in %s", notification.Hash.String(), time.Since(startTime))
					}
				}
			}
		}()
	}

	return bv
}

func (u *BlockValidation) localSetTxMined(ctx context.Context, blockHash *chainhash.Hash) (err error) {
	var block *model.Block
	var ids []uint32

	cachedBlock, blockWasAlreadyCached := u.lastValidatedBlocks.Get(*blockHash)
	if blockWasAlreadyCached && cachedBlock != nil {
		// we have just validated this block, so we can use the cached block
		// this should have all the subtrees already loaded
		block = cachedBlock
	} else {
		// get the block from the blockchain
		if block, err = u.blockchainClient.GetBlock(ctx, blockHash); err != nil {
			return fmt.Errorf("[localSetMined][%s] failed to get block from blockchain: %v", blockHash.String(), err)
		}
	}

	if ids, err = u.blockchainClient.GetBlockHeaderIDs(ctx, blockHash, 1); err != nil || len(ids) != 1 {
		return fmt.Errorf("[localSetMined][%s] failed to get block header ids: %v", blockHash.String(), err)
	}

	// add the transactions in this block to the txMeta block hashes
	startTime := time.Now()
	u.logger.Infof("[localSetMined][%s] update tx mined for block", blockHash.String())

	localSetTxMinedConcurrency, _ := gocore.Config().GetInt("blockvalidation_localSetTxMinedConcurrency", util.Max(4, runtime.NumCPU()/2))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(localSetTxMinedConcurrency) // keep 32 cores free for other tasks

	for subtreeIdx, subtreeHash := range block.Subtrees {
		subtreeIdx := subtreeIdx
		subtreeHash := subtreeHash
		g.Go(func() error {
			var subtreeTxIDBytes []byte
			var reader io.ReadCloser

			// check whether the subtree has already been loaded in the block
			if len(block.SubtreeSlices) > 0 && block.SubtreeSlices[subtreeIdx] != nil {
				subtreeTxIDBytes, err = block.SubtreeSlices[subtreeIdx].SerializeNodes()
				if err != nil {
					// we don't want to return here, since we can get the subtree from the store if needed
					u.logger.Errorf("[localSetMined][%s] failed to serialize subtree from slice: %v, will request from the subtree store", blockHash.String(), err)
				}
			}

			if len(subtreeTxIDBytes) == 0 {
				// get the subtree, it was not loaded in the block
				if reader, err = u.subtreeStore.GetIoReader(gCtx, subtreeHash[:]); err != nil {
					return fmt.Errorf("[localSetMined][%s] failed to get subtree from store: %v", blockHash.String(), err)
				}
				defer reader.Close()

				if subtreeTxIDBytes, err = util.DeserializeNodesFromReader(reader); err != nil {
					return fmt.Errorf("[localSetMined][%s] failed to deserialize subtree from reader: %v", blockHash.String(), err)
				}
			}

			blockIDBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(blockIDBytes, ids[0])
			return u.minedBlockStore.SetMultiKeysSingleValue(subtreeTxIDBytes, blockIDBytes, chainhash.HashSize)
		})
	}

	if err = g.Wait(); err != nil {
		return fmt.Errorf("[localSetMined][%s] failed to update tx mined for block: %v", blockHash.String(), err)
	}

	// delete the block from the cache, if it was there
	if blockWasAlreadyCached {
		u.lastValidatedBlocks.Delete(*blockHash)
	}

	u.logger.Infof("[localSetMined][%s] update tx mined for block DONE in %s", blockHash.String(), time.Since(startTime))

	return nil
}

func (u *BlockValidation) SetTxMetaCache(ctx context.Context, hash *chainhash.Hash, txMeta *txmeta.Data) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:SetTxMetaCache")
		defer func() {
			span.Finish()
		}()

		return cache.SetCache(hash, txMeta)
	}

	return nil
}

func (u *BlockValidation) SetTxMetaCacheMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:SetTxMetaCacheMinedMulti")
		defer func() {
			span.Finish()
		}()

		return cache.SetMinedMulti(ctx, hashes, blockID)
	}

	return nil
}

func (u *BlockValidation) SetTxMetaCacheMulti(ctx context.Context, keys [][]byte, values [][]byte) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:SetTxMetaCacheMulti")
		defer func() {
			span.Finish()
		}()

		return cache.SetCacheMulti(keys, values)
	}

	return nil
}

func (u *BlockValidation) DelTxMetaCacheMulti(ctx context.Context, hash *chainhash.Hash) error {
	if cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:DelTxMetaCacheMulti")
		defer func() {
			span.Finish()
		}()

		return cache.Delete(ctx, hash)
	}

	return nil
}

func (u *BlockValidation) ValidateBlock(ctx context.Context, block *model.Block, baseUrl string) error {
	timeStart, stat, ctx := util.NewStatFromContext(ctx, "ValidateBlock", stats)
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:ValidateBlock")
	span.LogKV("block", block.Hash().String())
	defer func() {
		span.Finish()
		stat.AddTime(timeStart)
		prometheusBlockValidationValidateBlock.Inc()
	}()

	u.logger.Infof("[ValidateBlock][%s] called", block.Header.Hash().String())

	// validate all the subtrees in the block
	u.logger.Infof("[ValidateBlock][%s] validating %d subtrees", block.Hash().String(), len(block.Subtrees))
	if err := u.validateBlockSubtrees(spanCtx, block, baseUrl); err != nil {
		return err
	}
	u.logger.Infof("[ValidateBlock][%s] validating %d subtrees DONE", block.Hash().String(), len(block.Subtrees))

	// get all 100 previous block headers on the main chain
	u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders", block.Header.Hash().String())
	blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(spanCtx, block.Header.HashPrevBlock, 100)
	if err != nil {
		return err
	}

	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(spanCtx, block.Header.HashPrevBlock, 100)
	if err != nil {
		return err
	}
	u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders DONE", block.Header.Hash().String())

	// Add the coinbase transaction to the metaTxStore
	// TODO why is this needed?
	//u.logger.Infof("[ValidateBlock][%s] storeCoinbaseTx", block.Header.Hash().String())
	//err = u.storeCoinbaseTx(spanCtx, block)
	//if err != nil {
	//	return err
	//}
	//u.logger.Infof("[ValidateBlock][%s] storeCoinbaseTx DONE", block.Header.Hash().String())

	var optimisticMiningWg sync.WaitGroup
	if u.optimisticMining {
		// make sure the proof of work is enough
		headerValid, _, err := block.Header.HasMetTargetDifficulty()
		if !headerValid {
			return fmt.Errorf("invalid block header: %s - %v", block.Header.Hash().String(), err)
		}

		// set the block in the temporary block cache for 2 minutes, could then be used for SetMined
		// must be set before AddBlock is called
		u.lastValidatedBlocks.Set(*block.Hash(), block)

		u.logger.Infof("[ValidateBlock][%s] adding block optimistically to blockchain", block.Hash().String())
		if err = u.blockchainClient.AddBlock(spanCtx, block, baseUrl); err != nil {
			return fmt.Errorf("[ValidateBlock][%s] failed to store block [%w]", block.Hash().String(), err)
		}
		u.logger.Infof("[ValidateBlock][%s] adding block optimistically to blockchain DONE", block.Hash().String())

		// decouple the tracing context to not cancel the context when finalize the block processing in the background
		callerSpan := opentracing.SpanFromContext(spanCtx)
		validateCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

		optimisticMiningWg.Add(1)
		go func() {
			defer optimisticMiningWg.Done()

			u.logger.Infof("[ValidateBlock][%s] validating block in background", block.Hash().String())
			if ok, err := block.Valid(validateCtx, u.subtreeStore, u.txMetaStore, u.minedBlockStore, blockHeaders, blockHeaderIDs); !ok {
				u.logger.Warnf("[ValidateBlock][%s] block is not valid in background: %v", block.String(), err)

				if err = u.blockchainClient.InvalidateBlock(validateCtx, block.Header.Hash()); err != nil {
					u.logger.Errorf("[ValidateBlock] failed to invalidate block in background: %s", err)
				}
			}
		}()
	} else {
		// validate the block
		u.logger.Infof("[ValidateBlock][%s] validating block", block.Hash().String())
		if ok, err := block.Valid(spanCtx, u.subtreeStore, u.txMetaStore, u.minedBlockStore, blockHeaders, blockHeaderIDs); !ok {
			return fmt.Errorf("[ValidateBlock][%s] block is not valid: %v", block.String(), err)
		}
		u.logger.Infof("[ValidateBlock][%s] validating block DONE", block.Hash().String())

		// set the block in the temporary block cache for 2 minutes, could then be used for SetMined
		// must be set before AddBlock is called
		u.lastValidatedBlocks.Set(*block.Hash(), block)

		// if valid, store the block
		u.logger.Infof("[ValidateBlock][%s] adding block to blockchain", block.Hash().String())
		if err = u.blockchainClient.AddBlock(spanCtx, block, baseUrl); err != nil {
			return fmt.Errorf("[ValidateBlock][%s] failed to store block [%w]", block.Hash().String(), err)
		}
		u.logger.Infof("[ValidateBlock][%s] adding block to blockchain DONE", block.Hash().String())
	}

	u.logger.Infof("[ValidateBlock][%s] storing coinbase tx: %s", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())
	if err = u.txStore.Set(spanCtx, block.CoinbaseTx.TxIDChainHash()[:], block.CoinbaseTx.Bytes()); err != nil {
		u.logger.Errorf("[ValidateBlock][%s] failed to store coinbase transaction [%s]", block.Hash().String(), err)
	}
	u.logger.Infof("[ValidateBlock][%s] storing coinbase tx: %s DONE", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())

	// decouple the tracing context to not cancel the context when finalize the block processing in the background
	callerSpan := opentracing.SpanFromContext(spanCtx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	go func() {
		optimisticMiningWg.Wait()

		// this happens in the background, since we have already added the block to the blockchain
		// TODO should we recover this somehow if it fails?
		// what are the consequences of this failing?
		err = u.finalizeBlockValidation(setCtx, block)
		if err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to finalize block validation [%v]", block.Hash().String(), err)
		}
		u.logger.Infof("[ValidateBlock][%s] finalizeBlockValidation DONE", block.Hash().String())
	}()

	prometheusBlockValidationValidateBlockDuration.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	u.logger.Infof("[ValidateBlock][%s] DONE but finalizeBlockValidation will continue in the background", block.Hash().String())

	return nil
}

// storeCoinbaseTx
func (u *BlockValidation) _(spanCtx context.Context, block *model.Block) (err error) {
	childSpan, childSpanCtx := opentracing.StartSpanFromContext(spanCtx, "BlockValidation:storeCoinbaseTx")
	defer func() {
		childSpan.Finish()
	}()

	// TODO - we need to consider if we can do this differently
	if _, err = u.txMetaStore.Create(childSpanCtx, block.CoinbaseTx); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("[ValidateBlock][%s] failed to create coinbase transaction in txMetaStore [%s]", block.Hash().String(), err.Error())
		}
	}

	return nil
}

func (u *BlockValidation) finalizeBlockValidation(ctx context.Context, block *model.Block) error {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:finalizeBlockValidation")
	defer func() {
		span.Finish()
	}()

	// get all the subtrees from the block. This should have been loaded during validation, so should be instant
	u.logger.Infof("[ValidateBlock][%s] get subtrees", block.Hash().String())
	blockSubtrees, err := block.GetSubtrees(u.subtreeStore)
	if err != nil {
		return fmt.Errorf("[ValidateBlock][%s] failed to get subtrees from block [%w]", block.Hash().String(), err)
	}

	// decouple the tracing context to not cancel the context when the subtree TTL is being saved in the background
	callerSpan := opentracing.SpanFromContext(spanCtx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	finalizeBlockValidationConcurrency, _ := gocore.Config().GetInt("blockvalidation_finalizeBlockValidationConcurrency", util.Max(4, runtime.NumCPU()/2))

	g, gCtx := errgroup.WithContext(setCtx)
	g.SetLimit(finalizeBlockValidationConcurrency) // keep 32 cores free for other tasks

	ids, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.Hash(), 1)
	if err != nil {
		return fmt.Errorf("failed to get block header ids: %w", err)
	}
	blockID := ids[0]

	g.Go(func() error {
		u.logger.Infof("[ValidateBlock][%s] updating subtrees TTL", block.Hash().String())
		err := u.updateSubtreesTTL(gCtx, block)
		if err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to update subtrees TTL [%s]", block.Hash().String(), err)
		}
		u.logger.Infof("[ValidateBlock][%s] update subtrees TTL DONE", block.Hash().String())

		return nil
	})

	if !u.localSetMined {
		// update the txMeta block hashes, if we are not doing it locally in the background
		g.Go(func() error {
			// add the transactions in this block to the txMeta block hashes
			u.logger.Infof("[ValidateBlock][%s] update tx mined", block.Hash().String())
			if err = model.UpdateTxMinedStatus(gCtx, u.logger, u.txMetaStore, blockSubtrees, blockID); err != nil {
				// TODO this should be a fatal error, but for now we just log it
				//return nil, fmt.Errorf("[ValidateBlock] error updating tx mined status: %w", err)
				u.logger.Errorf("[ValidateBlock][%s] error updating tx mined status: %w", block.Hash().String(), err)
			}
			u.logger.Infof("[ValidateBlock][%s] update tx mined DONE", block.Hash().String())
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return fmt.Errorf("[ValidateBlock][%s] failed to finalize block validation [%w]", block.Hash().String(), err)
	}

	return nil
}

func (u *BlockValidation) updateSubtreesTTL(ctx context.Context, block *model.Block) (err error) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:updateSubtreesTTL")
	defer func() {
		span.Finish()
	}()

	subtreeTTLConcurrency, _ := gocore.Config().GetInt("subtreeTTLConcurrency", 32)

	// update the subtree TTLs
	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(subtreeTTLConcurrency)

	for _, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash
		g.Go(func() error {
			err = u.subtreeStore.SetTTL(gCtx, subtreeHash[:], 0)
			if err != nil {
				return errors.Join(errors.New("failed to update subtree TTL"), err)
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.Join(fmt.Errorf("[ValidateBlock][%s] failed to update subtree TTLs", block.Hash().String()), err)
	}

	return nil
}

func (u *BlockValidation) validateBlockSubtrees(ctx context.Context, block *model.Block, baseUrl string) error {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:validateBlockSubtrees")
	start, stat, _ := util.StartStatFromContext(spanCtx, "ValidateBlockSubtrees")
	defer func() {
		span.Finish()
		stat.AddTime(start)
	}()

	// TODO This can be very slow, but mostly isn't :-S, example:
	// 14:05:29 | INFO  | BlockValidation.go:94 | bval  | [ValidateBlock][007a6ff44d6d48df201887f5d9725cb131fb837f774278413720292a6b705e41] validating 154 subtrees
	// 14:10:01 | INFO  | BlockValidation.go:99 | bval  | [ValidateBlock][007a6ff44d6d48df201887f5d9725cb131fb837f774278413720292a6b705e41] validating 154 subtrees DONE

	validateBlockSubtreesConcurrency, _ := gocore.Config().GetInt("blockvalidation_validateBlockSubtreesConcurrency", util.Max(4, runtime.NumCPU()/2))

	start1 := gocore.CurrentTime()
	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(validateBlockSubtreesConcurrency) // keep 32 cores free for other tasks

	missingSubtrees := make([]*chainhash.Hash, len(block.Subtrees))
	missingSubtreesMu := sync.Mutex{}
	for idx, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash
		idx := idx
		// first check all the subtrees exist or not in our store, in parallel, and gather what is missing
		g.Go(func() error {
			// get subtree from store
			subtreeExists, err := u.subtreeStore.Exists(gCtx, subtreeHash[:])
			if err != nil {
				return errors.Join(fmt.Errorf("[validateBlockSubtrees][%s] failed to check if subtree exists in store", subtreeHash.String()), err)
			}
			if !subtreeExists {
				// subtree already exists in store, which means it's valid
				missingSubtreesMu.Lock()
				missingSubtrees[idx] = subtreeHash
				missingSubtreesMu.Unlock()
			}

			return nil
		})
	}
	err := g.Wait()
	stat.NewStat("1. missingSubtrees").AddTime(start1)
	if err != nil {
		return err
	}

	startGet := gocore.CurrentTime()
	statGet := stat.NewStat("1b. GetSubtrees")

	subtreeBytesMap := make(map[chainhash.Hash][]chainhash.Hash, len(missingSubtrees))
	subtreeBytesMapMu := sync.Mutex{}
	g, gCtx = errgroup.WithContext(spanCtx)
	g.SetLimit(validateBlockSubtreesConcurrency) // mostly IO bound, so double the limit

	for _, subtreeHash := range missingSubtrees {
		// since the missingSubtrees is a full slice with only the missing subtrees set, we need to check if it's nil
		if subtreeHash != nil {
			subtreeHash := subtreeHash
			g.Go(func() error {
				// get subtree from network over http using the baseUrl
				txHashes, err := u.getSubtreeTxHashes(spanCtx, statGet, subtreeHash, baseUrl)
				if err != nil {
					return fmt.Errorf("[validateBlockSubtrees][%s] failed to get subtree from network: %v", subtreeHash.String(), err)
				}

				subtreeBytesMapMu.Lock()
				subtreeBytesMap[*subtreeHash] = txHashes
				subtreeBytesMapMu.Unlock()

				return nil
			})
		}
	}
	statGet.AddTime(startGet)

	if err = g.Wait(); err != nil {
		return fmt.Errorf("[validateBlockSubtrees][%s] failed to get subtrees for block: %v", block.Hash().String(), err)
	}

	start2 := gocore.CurrentTime()
	stat2 := stat.NewStat("2. validateBlockSubtrees")
	// validate the missing subtrees in series, transactions might rely on each other
	for _, subtreeHash := range missingSubtrees {
		// since the missingSubtrees is a full slice with only the missing subtrees set, we need to check if it's nil
		if subtreeHash != nil {
			ctx1 := util.ContextWithStat(spanCtx, stat2)
			if err = u.validateSubtree(ctx1, subtreeHash, baseUrl, &subtreeBytesMap); err != nil {
				return errors.Join(fmt.Errorf("[validateBlockSubtrees][%s] invalid subtree found [%s]", block.Hash().String(), subtreeHash.String()), err)
			}
		}
	}
	stat2.AddTime(start2)

	return nil
}

// getMissingTransactionsBatch gets a batch of transactions from the network
// NOTE: it does not return the transactions in the same order as the txHashes
func (u *BlockValidation) getMissingTransactionsBatch(ctx context.Context, txHashes []missingTxHash, baseUrl string) ([]*bt.Tx, error) {
	txIDBytes := make([]byte, 32*len(txHashes))
	for idx, txHash := range txHashes {
		copy(txIDBytes[idx*32:(idx+1)*32], txHash.hash[:])
	}

	// do http request to baseUrl + txHash.String()
	u.logger.Debugf("[getMissingTransactionsBatch] getting %d txs from other miner %s", len(txHashes), baseUrl)
	url := fmt.Sprintf("%s/txs", baseUrl)
	body, err := util.DoHTTPRequestBodyReader(ctx, url, txIDBytes)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("[getMissingTransactionsBatch] failed to do http request"), err)
	}
	defer body.Close()

	// read the body into transactions using go-bt
	missingTxs := make([]*bt.Tx, 0, len(txHashes))
	var tx *bt.Tx
	for {
		tx, err = u.readTxFromReader(body)
		if err != nil || tx == nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.Join(fmt.Errorf("[getMissingTransactionsBatch] failed to read transaction from body"), err)
		}

		missingTxs = append(missingTxs, tx)
	}

	return missingTxs, nil
}

func (u *BlockValidation) readTxFromReader(body io.ReadCloser) (tx *bt.Tx, err error) {
	defer func() {
		// there is a bug in go-bt, that does not check input and throws a runtime error in
		// github.com/libsv/go-bt/v2@v2.2.2/input.go:76 +0x16b
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = fmt.Errorf("unknown panic: %v", r)
			}
		}
	}()

	tx = &bt.Tx{}
	_, err = tx.ReadFrom(body)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (u *BlockValidation) getMissingTransaction(ctx context.Context, txHash *chainhash.Hash, baseUrl string) (*bt.Tx, error) {
	//startTotal, stat, ctx := util.StartStatFromContext(ctx, "getMissingTransaction")
	defer func() {
		//stat.AddTime(startTotal)
	}()

	// get transaction from network over http using the baseUrl
	if baseUrl == "" {
		return nil, fmt.Errorf("[getMissingTransaction][%s] baseUrl for transaction is empty", txHash.String())
	}

	//start := gocore.CurrentTime()
	alreadyHaveTransaction := true
	txBytes, err := u.txStore.Get(ctx, txHash[:])
	//stat.NewStat("getTxFromStore").AddTime(start)
	if txBytes == nil || err != nil {
		alreadyHaveTransaction = false

		// do http request to baseUrl + txHash.String()
		u.logger.Infof("[getMissingTransaction][%s] getting tx from other miner", txHash.String(), baseUrl)
		url := fmt.Sprintf("%s/tx/%s", baseUrl, txHash.String())
		//startM := gocore.CurrentTime()
		//statM := stat.NewStat("http fetch missing tx")
		txBytes, err = util.DoHTTPRequest(ctx, url)
		//statM.AddTime(startM)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("[getMissingTransaction][%s] failed to do http request", txHash.String()), err)
		}
	}

	// validate the transaction by creating a transaction object
	tx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		return nil, fmt.Errorf("[getMissingTransaction][%s] failed to create transaction from bytes [%s]", txHash.String(), err.Error())
	}

	if !alreadyHaveTransaction {
		//start = gocore.CurrentTime()
		// store the transaction, we did not get it via propagation
		err = u.txStore.Set(ctx, txHash[:], txBytes)
		//stat.NewStat("storeTx").AddTime(start)
		if err != nil {
			return nil, fmt.Errorf("[getMissingTransaction][%s] failed to store transaction [%s]", txHash.String(), err.Error())
		}
	}

	return tx, nil
}

func (u *BlockValidation) blessMissingTransaction(ctx context.Context, tx *bt.Tx) (txMeta *txmeta.Data, err error) {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "getMissingTransaction")
	defer func() {
		stat.AddTime(startTotal)
		prometheusBlockValidationBlessMissingTransaction.Inc()
		prometheusBlockValidationBlessMissingTransactionDuration.Observe(float64(time.Since(startTotal).Microseconds()) / 1_000_000)
	}()

	if tx == nil {
		return nil, fmt.Errorf("[blessMissingTransaction] tx is nil")
	}
	u.logger.Debugf("[blessMissingTransaction][%s] called", tx.TxID())

	if tx.IsCoinbase() {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] transaction is coinbase", tx.TxID())
	}

	// validate the transaction in the validation service
	// this should spend utxos, create the tx meta and create new utxos
	// todo return tx meta data
	err = u.validatorClient.Validate(ctx, tx)
	if err != nil {
		// TODO what to do here? This could be a double spend and the transaction needs to be marked as conflicting
		return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to validate transaction [%s]", tx.TxID(), err.Error())
	}

	start := gocore.CurrentTime()
	txMeta, err = u.txMetaStore.GetMeta(ctx, tx.TxIDChainHash())
	stat.NewStat("getTxMeta").AddTime(start)
	if err != nil {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to get tx meta [%s]", tx.TxID(), err.Error())
	}

	if txMeta == nil {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] tx meta is nil", tx.TxID())
	}

	return txMeta, nil
}

func (u *BlockValidation) validateSubtree(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string, subtreeHashesMap ...*map[chainhash.Hash][]chainhash.Hash) error {
	// validateSubtreeInternal does the actual work, but it can be expensive.  We need to make sure that we only call it once
	// for each subtreeHash, so we use a map to keep track of which ones we have already called it for
	// and using a sync.Cond to broadcast the signal to all the other goroutines that are waiting for the result

	return u.subtreeDeDuplicator.DeDuplicate(ctx, *subtreeHash, func() error {
		return u.validateSubtreeInternal(ctx, subtreeHash, baseUrl, subtreeHashesMap...)
	})
}

func (u *BlockValidation) validateSubtreeInternal(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string, subtreeHashesMap ...*map[chainhash.Hash][]chainhash.Hash) error {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "validateSubtreeBlobInternal")
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:validateSubtree")
	span.LogKV("subtree", subtreeHash.String())
	defer func() {
		span.Finish()
		stat.AddTime(startTotal)
		prometheusBlockValidationValidateSubtree.Inc()
	}()

	u.logger.Infof("[validateSubtree][%s] called", subtreeHash.String())

	start := gocore.CurrentTime()
	var txHashes []chainhash.Hash

	// check whether we have the subtree in the subtreeHashesMap and can skip getting it here
	if len(subtreeHashesMap) > 0 && len(*subtreeHashesMap[0]) > 0 && len((*subtreeHashesMap[0])[*subtreeHash]) > 0 {
		txHashes = (*subtreeHashesMap[0])[*subtreeHash]
	} else {
		// get subtree from store
		subtreeExists, err := u.subtreeStore.Exists(spanCtx, subtreeHash[:])
		stat.NewStat("1. subtreeExists").AddTime(start)
		if err != nil {
			return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to check if subtree exists in store", subtreeHash.String()), err)
		}
		if subtreeExists {
			// subtree already exists in store, which means it's valid
			// TODO is this true?
			return nil
		}

		// get subtree from network over http using the baseUrl
		txHashes, err = u.getSubtreeTxHashes(spanCtx, stat, subtreeHash, baseUrl)
		if err != nil {
			return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to get subtree from network", subtreeHash.String()), err)
		}
	}

	// create the empty subtree
	height := math.Ceil(math.Log2(float64(len(txHashes))))
	subtree, err := util.NewTree(int(height))
	if err != nil {
		return err
	}

	validateSubtreeInternalConcurrency, _ := gocore.Config().GetInt("blockvalidation_validateSubtreeInternal", util.Max(4, runtime.NumCPU()/2))

	start = gocore.CurrentTime()
	// validate the subtree
	txMetaSlice := make([]*txmeta.Data, len(txHashes))
	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(validateSubtreeInternalConcurrency) // keep 32 cores free for other tasks

	u.logger.Infof("[validateSubtree][%s] processing %d txs from subtree", subtreeHash.String(), len(txHashes))
	// unlike many other lists, this needs to be a pointer list, because a lot of values could be empty = nil
	missingTxHashes := make([]*chainhash.Hash, len(txHashes))
	missingTxHashesFromCache := make([]*chainhash.Hash, len(txHashes))
	nrOfMissingTransactions := atomic.Int32{}

	cache, _ := u.txMetaStore.(*txmetacache.TxMetaCache)

	// cycle through batches of 1024 txHashes at a time
	batchSize, _ := gocore.Config().GetInt("blockvalidation_validateSubtreeBatchSize", 1024)
	for i := 0; i < len(txHashes); i += batchSize {
		i := i
		g.Go(func() error {
			var txMeta *txmeta.Data
			var err error
			// cycle through the batch size, making sure not to go over the length of the txHashes
			for j := 0; j < util.Min(batchSize, len(txHashes)-i); j++ {
				txHash := txHashes[i+j]
				if cache != nil {
					txMeta = cache.GetMetaCached(gCtx, &txHash)
					missingTxHashesFromCache[i+j] = &txHash
				} else {
					txMeta, err = u.txMetaStore.GetMeta(gCtx, &txHash)
					if err != nil {
						if errors.Is(err, txmeta.NewErrTxmetaNotFound(&txHash)) || strings.Contains(err.Error(), "failed to get tx meta") {
							// collect all missing transactions for processing in order
							// that is why we use an indexed slice instead of just a slice append
							// don't add the coinbase placeholder to the missing transactions
							if !txHash.Equal(*model.CoinbasePlaceholderHash) {
								missingTxHashes[i+j] = &txHash
								nrOfMissingTransactions.Add(1)
							}
							continue
						} else {
							return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to get tx meta", subtreeHash.String()), err)
						}
					}

					// this might be an old check, where we did not get an error, but also txMeta was nil
					// TODO test and see if this is still needed
					if txMeta == nil {
						return fmt.Errorf("[validateSubtree][%s] tx meta is nil [%s]", subtreeHash.String(), txHash.String())
					}
				}

				if txMeta != nil {
					txMetaSlice[i+j] = txMeta
				}
			}

			return nil
		})
	}

	err = g.Wait()
	stat.NewStat("4. checkTxs").AddTime(start)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to bless all transactions in subtree", subtreeHash.String()), err)
	}

	getMissingSubtreeTxMetaFromCacheConcurrency, _ := gocore.Config().GetInt("blockvalidation_getMissingSubtreeTxMetaFromCacheConcurrency", 1024)
	missingFromCacheG, missingFromCacheGCtx := errgroup.WithContext(gCtx)
	missingFromCacheG.SetLimit(getMissingSubtreeTxMetaFromCacheConcurrency)

	if len(missingTxHashesFromCache) > 0 {
		var txMeta *txmeta.Data
		// process missingTxHashesFromCache
		for idx, txHash := range missingTxHashesFromCache {
			if txHash != nil {
				idx := idx
				txHash := txHash
				g.Go(func() error {
					txMeta, err = u.txMetaStore.GetMeta(missingFromCacheGCtx, txHash)
					if err != nil {
						if errors.Is(err, txmeta.NewErrTxmetaNotFound(txHash)) || strings.Contains(err.Error(), "failed to get tx meta") {
							// collect all missing transactions for processing in order
							// that is why we use an indexed slice instead of just a slice append
							// don't add the coinbase placeholder to the missing transactions
							if !txHash.Equal(*model.CoinbasePlaceholderHash) {
								missingTxHashes[idx] = txHash
								nrOfMissingTransactions.Add(1)
							}
							return nil
						} else {
							return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to get tx meta", subtreeHash.String()), err)
						}
					}
					if txMeta == nil {
						return fmt.Errorf("[validateSubtree][%s] tx meta is nil [%s]", subtreeHash.String(), txHash.String())
					}

					txMetaSlice[idx] = txMeta

					return nil
				})
			}
		}
	}

	if err = missingFromCacheG.Wait(); err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to bless all transactions in subtree", subtreeHash.String()), err)
	}

	if nrOfMissingTransactions.Load() > 0 {
		start, stat5, ctx5 := util.StartStatFromContext(spanCtx, "5. processMissingTransactions")
		// missingTxHashes is a slice if all txHashes in the subtree, but only the missing ones are not nil
		// this is done to make sure the order is preserved when getting them in parallel
		// compact the missingTxHashes to only a list of the missing ones
		missingTxHashesCompacted := make([]missingTxHash, 0, nrOfMissingTransactions.Load())
		for idx, txHash := range missingTxHashes {
			if txHash != nil {
				missingTxHashesCompacted = append(missingTxHashesCompacted, missingTxHash{
					hash: txHash,
					idx:  idx,
				})
			}
		}

		if len(missingTxHashes) == 1 {
			u.logger.Infof("[validateSubtree][%s] processing missing tx %s for subtree instance", subtreeHash.String(), missingTxHashes[0].String())
		} else {
			u.logger.Infof("[validateSubtree][%s] processing %d missing tx for subtree instance", subtreeHash.String(), len(missingTxHashesCompacted))
		}

		err = u.processMissingTransactions(ctx5, subtreeHash, missingTxHashesCompacted, baseUrl, txMetaSlice)
		if err != nil {
			return err
		}
		stat5.AddTime(start)
	}

	start = gocore.CurrentTime()
	var txMeta *txmeta.Data
	u.logger.Infof("[validateSubtree][%s] adding %d nodes to subtree instance", subtreeHash.String(), len(txHashes))
	for idx, txHash := range txHashes {
		// if placeholder just add it and continue
		if idx == 0 && txHash.Equal(*model.CoinbasePlaceholderHash) {
			err = subtree.AddNode(txHash, 0, 0)
			if err != nil {
				return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to add coinbase placeholder node to subtree", subtreeHash.String()), err)
			}
			continue
		}
		// finally add the transaction hash and fee to the subtree
		txMeta = txMetaSlice[idx]
		if txMeta == nil {
			found := false
			index := -1
			for i, h := range txHashes {
				if h.IsEqual(&txHash) {
					found = true
					index = i
					break
				}
			}
			if found {
				u.logger.Warnf("[validateSubtree][%s] tx meta exists in txHashes @ %d of %d [%s]", subtreeHash.String(), index, len(txHashes), txHash.String())
			} else {
				u.logger.Warnf("[validateSubtree][%s] tx meta not found in txHashes. Not possible? [%s]", subtreeHash.String(), txHash.String())
			}

			found = false
			index = -1
			for i, missingTxHash := range missingTxHashes {
				if txHash.IsEqual(missingTxHash) {
					found = true
					index = i
					break
				}
			}
			if found {
				u.logger.Warnf("[validateSubtree][%s] tx meta exists in missingTxHashes but wasn't processed? @ %d of %d [%s]", subtreeHash.String(), index, len(missingTxHashes), txHash.String())
			} else {
				u.logger.Warnf("[validateSubtree][%s] tx meta not found in missingTxHashes [%s]", subtreeHash.String(), txHash.String())
			}

			return fmt.Errorf("[validateSubtree][%s] tx meta not found in txMetaSlice [%s]", subtreeHash.String(), txHash.String())
		}

		err = subtree.AddNode(txHash, txMeta.Fee, txMeta.SizeInBytes)
		if err != nil {
			return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to add node to subtree", subtreeHash.String()), err)
		}
	}
	stat.NewStat("6. addAllTxHashFeeSizesToSubtree").AddTime(start)

	// does the merkle tree give the correct root?
	merkleRoot := subtree.RootHash()
	if !merkleRoot.IsEqual(subtreeHash) {
		return fmt.Errorf("[validateSubtree][%s] subtree root hash does not match [%s]", subtreeHash.String(), merkleRoot.String())
	}

	u.logger.Infof("[validateSubtree][%s] serialize subtree", subtreeHash.String())
	completeSubtreeBytes, err := subtree.Serialize()
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to serialize subtree", subtreeHash.String()), err)
	}

	start = gocore.CurrentTime()
	// store subtree in store
	u.logger.Infof("[validateSubtree][%s] store subtree", subtreeHash.String())
	err = u.subtreeStore.Set(spanCtx, merkleRoot[:], completeSubtreeBytes, options.WithTTL(u.subtreeTTL))
	stat.NewStat("7. storeSubtree").AddTime(start)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to store subtree", subtreeHash.String()), err)
	}

	// only set this on no errors
	prometheusBlockValidationValidateSubtreeDuration.Observe(float64(time.Since(startTotal).Microseconds()) / 1_000_000)

	return nil
}

func (u *BlockValidation) getSubtreeTxHashes(spanCtx context.Context, stat *gocore.Stat, subtreeHash *chainhash.Hash, baseUrl string) ([]chainhash.Hash, error) {
	if baseUrl == "" {
		return nil, fmt.Errorf("[validateSubtree][%s] baseUrl for subtree is empty", subtreeHash.String())
	}

	start := gocore.CurrentTime()
	// do http request to baseUrl + subtreeHash.String()
	u.logger.Infof("[validateSubtree][%s] getting subtree from %s", subtreeHash.String(), baseUrl)
	url := fmt.Sprintf("%s/subtree/%s", baseUrl, subtreeHash.String())
	body, err := util.DoHTTPRequestBodyReader(spanCtx, url)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("[validateSubtree][%s] failed to do http request", subtreeHash.String()), err)
	}
	defer body.Close()

	stat.NewStat("2. http fetch subtree").AddTime(start)

	start = gocore.CurrentTime()
	txHashes := make([]chainhash.Hash, 0, 1024*1024)
	buffer := make([]byte, chainhash.HashSize)
	bufferedReader := bufio.NewReaderSize(body, 1024*1024*4)

	u.logger.Infof("[validateSubtree][%s] processing subtree response into tx hashes", subtreeHash.String())
	for {
		n, err := io.ReadFull(bufferedReader, buffer)
		if n > 0 {
			txHashes = append(txHashes, chainhash.Hash(buffer))
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("[validateSubtree][%s] unexpected EOF: partial hash read", subtreeHash.String())
			}
			return nil, fmt.Errorf("[validateSubtree][%s] error reading stream: %v", subtreeHash.String(), err)
		}
	}

	// // the subtree bytes we got from our competing miner only contain the transaction hashes
	// // it's basically just a list of 32 byte transaction hashes
	// txHashes := make([]chainhash.Hash, len(subtreeBytes)/chainhash.HashSize)
	// for i := 0; i < len(subtreeBytes); i += chainhash.HashSize {
	// 	txHashes[i/chainhash.HashSize] = chainhash.Hash(subtreeBytes[i : i+chainhash.HashSize])
	// }
	stat.NewStat("3. createTxHashes").AddTime(start)

	u.logger.Infof("[validateSubtree][%s] done with subtree response", subtreeHash.String())

	return txHashes, nil
}

func (u *BlockValidation) processMissingTransactions(ctx context.Context, subtreeHash *chainhash.Hash,
	missingTxHashes []missingTxHash, baseUrl string, txMetaSlice []*txmeta.Data) error {

	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:processMissingTransactions")
	defer func() {
		span.Finish()
	}()

	u.logger.Infof("[validateSubtree][%s] fetching %d missing txs", subtreeHash.String(), len(missingTxHashes))
	missingTxs, err := u.getMissingTransactions(spanCtx, missingTxHashes, baseUrl)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to get missing transactions", subtreeHash.String()), err)
	}

	u.logger.Infof("[validateSubtree][%s] blessing %d missing txs", subtreeHash.String(), len(missingTxs))
	var txMeta *txmeta.Data
	var mTx missingTx
	for _, mTx = range missingTxs {
		if mTx.tx == nil {
			return fmt.Errorf("[validateSubtree][%s] missing transaction is nil", subtreeHash.String())
		}
		txMeta, err = u.blessMissingTransaction(spanCtx, mTx.tx)
		if err != nil {
			return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to bless missing transaction: %s", subtreeHash.String(), mTx.tx.TxIDChainHash().String()), err)
		}
		if txMeta == nil {
			u.logger.Infof("[validateSubtree][%s] tx meta is nil [%s]", subtreeHash.String(), mTx.tx.TxIDChainHash().String())
		}

		u.logger.Debugf("[validateSubtree][%s] adding missing tx to txMetaSlice: %s", subtreeHash.String(), mTx.tx.TxIDChainHash().String())
		txMetaSlice[mTx.idx] = txMeta
	}

	// check if all missing transactions have been blessed
	count := 0
	for _, txMeta := range txMetaSlice {
		if txMeta == nil {
			count++
		}
	}
	if count > 0 {
		u.logger.Errorf("[validateSubtree][%s] %d missing entries in txMetaSlice", subtreeHash.String(), count)
	}

	return nil
}

func (u *BlockValidation) getMissingTransactions(ctx context.Context, missingTxHashes []missingTxHash, baseUrl string) (missingTxs []missingTx, err error) {
	// transactions have to be returned in the same order as they were requested
	missingTxsMap := make(map[chainhash.Hash]*bt.Tx, len(missingTxHashes))
	missingTxsMu := sync.Mutex{}

	getMissingTransactionsConcurrency, _ := gocore.Config().GetInt("blockvalidation_getMissingTransactions", util.Max(4, runtime.NumCPU()/2))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(getMissingTransactionsConcurrency) // keep 32 cores free for other tasks

	// get the transactions in batches of 500
	batchSize, _ := gocore.Config().GetInt("blockvalidation_missingTransactionsBatchSize", 100_000)
	for i := 0; i < len(missingTxHashes); i += batchSize {
		missingTxHashesBatch := missingTxHashes[i:util.Min(i+batchSize, len(missingTxHashes))]
		g.Go(func() error {
			missingTxsBatch, err := u.getMissingTransactionsBatch(gCtx, missingTxHashesBatch, baseUrl)
			if err != nil {
				return errors.Join(fmt.Errorf("[getMissingTransactions] failed to get missing transactions batch"), err)
			}

			missingTxsMu.Lock()
			for _, tx := range missingTxsBatch {
				if tx == nil {
					missingTxsMu.Unlock()
					return fmt.Errorf("[getMissingTransactions] #1 missing transaction is nil")
				}
				missingTxsMap[*tx.TxIDChainHash()] = tx
			}
			missingTxsMu.Unlock()

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, errors.Join(fmt.Errorf("[blessMissingTransaction] failed to get all transactions"), err)
	}

	// populate the missingTx slice with the tx data
	missingTxs = make([]missingTx, 0, len(missingTxHashes))
	for _, mTx := range missingTxHashes {
		if mTx.hash == nil {
			return nil, fmt.Errorf("[blessMissingTransaction] #2 missing transaction hash is nil [%s]", mTx.hash.String())
		}
		tx, ok := missingTxsMap[*mTx.hash]
		if !ok {
			return nil, fmt.Errorf("[blessMissingTransaction] missing transaction [%s]", mTx.hash.String())
		}
		if tx == nil {
			return nil, fmt.Errorf("[blessMissingTransaction] #3 missing transaction is nil [%s]", mTx.hash.String())
		}
		missingTxs = append(missingTxs, missingTx{tx: tx, idx: mTx.idx})
	}

	return missingTxs, nil
}
