package blockvalidation

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/deduplicator"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type revalidateBlockData struct {
	block          *model.Block
	blockHeaders   []*model.BlockHeader
	blockHeaderIDs []uint32
	baseUrl        string
	retries        int
}

type BlockValidation struct {
	logger                             ulogger.Logger
	blockchainClient                   blockchain.ClientI
	subtreeStore                       blob.Store
	subtreeTTL                         time.Duration
	txStore                            blob.Store
	utxoStore                          utxo.Store
	recentBlocksBloomFilters           []*model.BlockBloomFilter
	recentBlocksBloomFiltersMu         sync.Mutex
	recentBlocksBloomFiltersExpiration time.Duration
	validatorClient                    validator.Interface
	subtreeValidationClient            subtreevalidation.Interface
	subtreeDeDuplicator                *deduplicator.DeDuplicator
	optimisticMining                   bool
	lastValidatedBlocks                *expiringmap.ExpiringMap[chainhash.Hash, *model.Block] // map of full blocks that have been validated
	blockExists                        *expiringmap.ExpiringMap[chainhash.Hash, bool]         // map of block hashes that have been validated and exist
	subtreeExists                      *expiringmap.ExpiringMap[chainhash.Hash, bool]         // map of block hashes that have been validated and exist
	subtreeCount                       atomic.Int32
	blockHashesCurrentlyValidated      *util.SwissMap
	blockBloomFiltersBeingCreated      *util.SwissMap
	bloomFilterStats                   *model.BloomStats
	setMinedChan                       chan *chainhash.Hash
	revalidateBlockChan                chan revalidateBlockData
	stats                              *gocore.Stat
	excessiveblocksize                 int
}

func NewBlockValidation(ctx context.Context, logger ulogger.Logger, blockchainClient blockchain.ClientI, subtreeStore blob.Store,
	txStore blob.Store, txMetaStore utxo.Store, validatorClient validator.Interface, subtreeValidationClient subtreevalidation.Interface, bloomExpiration time.Duration) *BlockValidation {

	subtreeTTLMinutes, _ := gocore.Config().GetInt("blockvalidation_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	optimisticMining := gocore.Config().GetBool("optimisticMining", true)
	logger.Infof("optimisticMining = %v", optimisticMining)

	excessiveblocksize, _ := gocore.Config().GetInt("excessiveblocksize", 0)

	bv := &BlockValidation{
		logger:                             logger,
		blockchainClient:                   blockchainClient,
		subtreeStore:                       subtreeStore,
		subtreeTTL:                         subtreeTTL,
		txStore:                            txStore,
		utxoStore:                          txMetaStore,
		recentBlocksBloomFilters:           make([]*model.BlockBloomFilter, 0),
		recentBlocksBloomFiltersExpiration: bloomExpiration,
		validatorClient:                    validatorClient,
		subtreeValidationClient:            subtreeValidationClient,
		subtreeDeDuplicator:                deduplicator.New(subtreeTTL),
		optimisticMining:                   optimisticMining,
		lastValidatedBlocks:                expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExists:                        expiringmap.New[chainhash.Hash, bool](120 * time.Minute), // we keep this for 2 hours
		subtreeExists:                      expiringmap.New[chainhash.Hash, bool](10 * time.Minute),  // we keep this for 10 minutes
		subtreeCount:                       atomic.Int32{},
		blockHashesCurrentlyValidated:      util.NewSwissMap(0),
		blockBloomFiltersBeingCreated:      util.NewSwissMap(0),
		bloomFilterStats:                   model.NewBloomStats(),
		setMinedChan:                       make(chan *chainhash.Hash, 1000),
		revalidateBlockChan:                make(chan revalidateBlockData, 2),
		stats:                              gocore.NewStat("blockvalidation"),
		excessiveblocksize:                 excessiveblocksize,
	}

	go func() {
		// update stats for the expiring maps every 5 seconds
		for {
			time.Sleep(5 * time.Second)
			prometheusBlockValidationLastValidatedBlocksCache.Set(float64(bv.lastValidatedBlocks.Len()))
			prometheusBlockValidationBlockExistsCache.Set(float64(bv.blockExists.Len()))
			prometheusBlockValidationSubtreeExistsCache.Set(float64(bv.subtreeExists.Len()))
		}
	}()

	if bv.blockchainClient != nil {
		go func() {
			for {
				blockchainSubscription, err := bv.blockchainClient.Subscribe(ctx, "blockvalidation")
				if err != nil {
					bv.logger.Errorf("[BlockValidation:setMined] failed to subscribe to blockchain: %s", err)

					// backoff for 5 seconds and try again
					time.Sleep(5 * time.Second)
					continue
				}

				for notification := range blockchainSubscription {
					if notification == nil {
						continue
					}

					if notification.Type == model.NotificationType_BlockSubtreesSet {

						bv.logger.Infof("[BlockValidation:setMined] received BlockSubtreesSet notification. STU: %s", notification.Hash.String())

						// if blocks, err := bv.blockchainClient.GetBlocksSubtreesNotSet(ctx); err != nil {
						// 	bv.logger.Errorf("[BlockValidation:setMined] failed to getBlocksSubtreesNotSet: %s", err)
						// } else {
						// 	for _, block := range blocks {
						// 		if block.Header.Hash().IsEqual(notification.Hash) {
						// 			bv.logger.Warnf("[BlockValidation:setMined] block's subtrees aren't processed yet STU: %s", notification.Hash.String())
						// 			break
						// 		}
						// 	}
						// }

						// push block hash to the setMinedChan
						bv.setMinedChan <- notification.Hash
					}
				}
			}
		}()
	}

	go func() {
		bv.start(ctx)
	}()

	return bv
}

func (u *BlockValidation) start(ctx context.Context) {
	go u.bloomFilterStats.BloomFilterStatsProcessor(ctx)

	if u.blockchainClient != nil {
		// first check whether all old blocks have been processed properly
		blocksMinedNotSet, err := u.blockchainClient.GetBlocksMinedNotSet(ctx)
		if err != nil {
			u.logger.Errorf("[BlockValidation:start] failed to get blocks mined not set: %s", err)
		}

		g, gCtx := errgroup.WithContext(ctx)

		if len(blocksMinedNotSet) > 0 {
			u.logger.Infof("[BlockValidation:start] found %d blocks mined not set", len(blocksMinedNotSet))
			for _, block := range blocksMinedNotSet {
				blockHash := block.Hash()

				_ = u.blockHashesCurrentlyValidated.Put(*blockHash)
				g.Go(func() error {
					u.logger.Debugf("[BlockValidation:start] processing block mined not set: %s", blockHash.String())
					if err := u.setTxMined(gCtx, blockHash); err != nil {
						u.logger.Errorf("[BlockValidation:start] failed to set block mined: %s", err)
						u.setMinedChan <- blockHash
					} else {
						_ = u.blockHashesCurrentlyValidated.Delete(*blockHash)
					}

					return nil
				})
			}
		}

		// get all blocks that have subtrees not set
		blocksSubtreesNotSet, err := u.blockchainClient.GetBlocksSubtreesNotSet(ctx)
		if err != nil {
			u.logger.Errorf("[BlockValidation:start] failed to get blocks subtrees not set: %s", err)
		}

		if len(blocksSubtreesNotSet) > 0 {
			u.logger.Infof("[BlockValidation:start] found %d blocks subtrees not set", len(blocksSubtreesNotSet))
			for _, block := range blocksSubtreesNotSet {
				block := block
				g.Go(func() error {
					u.logger.Infof("[BlockValidation:start] processing block subtrees not set: %s", block.Hash().String())
					if err := u.updateSubtreesTTL(gCtx, block); err != nil {
						u.logger.Errorf("[BlockValidation:start] failed to update subtrees TTL: %s", err)
					}

					return nil
				})
			}
		}

		// wait for all blocks to be processed
		if err = g.Wait(); err != nil {
			// we cannot start the block validation, we are in a bad state
			u.logger.Fatalf("[BlockValidation:start] failed to start, process old block mined/subtrees sets: %s", err)
		}
	}

	// start a worker to process the setMinedChan
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case blockHash := <-u.setMinedChan:
				startTime := time.Now()
				u.logger.Infof("[BlockValidation:start][%s] block setTxMined", blockHash.String())

				_ = u.blockHashesCurrentlyValidated.Put(*blockHash)

				if err := u.setTxMined(ctx, blockHash); err != nil {
					u.logger.Errorf("[BlockValidation:start][%s] failed setTxMined: %s", blockHash.String(), err)
					// put the block back in the setMinedChan
					u.setMinedChan <- blockHash
				} else {
					_ = u.blockHashesCurrentlyValidated.Delete(*blockHash)
				}

				u.logger.Infof("[BlockValidation:start][%s] block setTxMined DONE in %s", blockHash.String(), time.Since(startTime))
			}
		}
	}()

	// start a worker to revalidate blocks
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case blockData := <-u.revalidateBlockChan:
				startTime := time.Now()
				u.logger.Infof("[BlockValidation:start][%s] block revalidation Chan", blockData.block.String())

				err := u.reValidateBlock(blockData)
				if err != nil {
					prometheusBlockValidationReValidateBlockErr.Observe(float64(time.Since(startTime).Microseconds() / 1_000_000))
					u.logger.Errorf("[BlockValidation:start][%s] failed block revalidation, retrying: %s", blockData.block.String(), err)
					// put the block back in the revalidateBlockChan
					if blockData.retries < 3 {
						blockData.retries++
						go func() {
							u.revalidateBlockChan <- blockData
						}()
					} else {
						u.logger.Errorf("[BlockValidation:start][%s] failed block revalidation, retries exhausted: %s", blockData.block.String(), err)
					}
				} else {
					prometheusBlockValidationReValidateBlock.Observe(float64(time.Since(startTime).Microseconds() / 1_000_000))
				}

				u.logger.Infof("[BlockValidation:start][%s] block revalidation Chan DONE in %s", blockData.block.String(), time.Since(startTime))
			}
		}
	}()
}

// validateBlock()
func (u *BlockValidation) _(ctx context.Context, blockHash *chainhash.Hash) error {
	startTime := time.Now()
	u.logger.Infof("[BlockValidation:start][%s] validate block", blockHash.String())
	defer func() {
		u.logger.Infof("[BlockValidation:start][%s] validate block DONE in %s", blockHash.String(), time.Since(startTime))
	}()

	block, err := u.blockchainClient.GetBlock(ctx, blockHash)
	if err != nil {
		return fmt.Errorf("[BlockValidation:start][%s] failed to get block: %s", blockHash.String(), err)
	}

	// get all 100 previous block headers on the main chain
	blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(ctx, block.Header.HashPrevBlock, 100)
	if err != nil {
		return fmt.Errorf("[BlockValidation:start][%s] failed to get block headers: %s", block.String(), err)
	}

	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, 100)
	if err != nil {
		return fmt.Errorf("[BlockValidation:start][%s] failed to get block header ids: %s", block.String(), err)
	}

	// make a copy of the recent bloom filters, so we don't get race conditions if the bloom filters are updated
	u.recentBlocksBloomFiltersMu.Lock()
	bloomFilters := make([]*model.BlockBloomFilter, 0)
	bloomFilters = append(bloomFilters, u.recentBlocksBloomFilters...)
	u.recentBlocksBloomFiltersMu.Unlock()

	if ok, err := block.Valid(ctx, u.logger, u.subtreeStore, u.utxoStore, bloomFilters, blockHeaders, blockHeaderIDs, u.bloomFilterStats); !ok {
		if iErr := u.blockchainClient.InvalidateBlock(ctx, block.Header.Hash()); err != nil {
			u.logger.Errorf("[BlockValidation:start][%s][InvalidateBlock] failed to invalidate block: %s", block.String(), iErr)
		}
		return fmt.Errorf("[BlockValidation:start][%s] InvalidateBlock block is not valid: %v", block.String(), err)
	}

	return nil
}

func (u *BlockValidation) SetBlockExists(hash *chainhash.Hash) error {
	u.blockExists.Set(*hash, true)
	return nil
}

func (u *BlockValidation) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	start := time.Now()
	stat := gocore.NewStat("GetBlockExists")
	defer func() {
		stat.AddTime(start)
	}()

	_, ok := u.blockExists.Get(*hash)
	if ok {
		return true, nil
	}

	exists, err := u.blockchainClient.GetBlockExists(ctx, hash)
	if err != nil {
		return false, err
	}

	if exists {
		u.blockExists.Set(*hash, true)
	}

	return exists, nil
}

func (u *BlockValidation) SetSubtreeExists(hash *chainhash.Hash) error {
	u.subtreeExists.Set(*hash, true)
	return nil
}

func (u *BlockValidation) GetSubtreeExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	start, stat, ctx := tracing.StartStatFromContext(ctx, "GetSubtreeExists")
	defer func() {
		stat.AddTime(start)
	}()

	_, ok := u.subtreeExists.Get(*hash)
	if ok {
		return true, nil
	}

	exists, err := u.subtreeStore.Exists(ctx, hash[:])
	if err != nil {
		return false, err
	}

	if exists {
		u.subtreeExists.Set(*hash, true)
	}

	return exists, nil
}

func (u *BlockValidation) setTxMined(ctx context.Context, blockHash *chainhash.Hash) (err error) {
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
			return fmt.Errorf("[setTxMined][%s] failed to get block from blockchain: %v", blockHash.String(), err)
		}
	}

	// make sure all the subtrees are loaded in the block
	_, err = block.GetSubtrees(ctx, u.logger, u.subtreeStore)
	if err != nil {
		return fmt.Errorf("[setTxMined][%s] failed to get subtrees from block [%w]", block.Hash().String(), err)
	}

	if ids, err = u.blockchainClient.GetBlockHeaderIDs(ctx, blockHash, 1); err != nil || len(ids) != 1 {
		return fmt.Errorf("[setTxMined][%s] failed to get block header ids: %v", blockHash.String(), err)
	}

	blockID := ids[0]

	// add the transactions in this block to the txMeta block IDs in the txMeta store
	if err = model.UpdateTxMinedStatus(
		ctx,
		u.logger,
		u.utxoStore,
		block,
		blockID,
	); err != nil {
		return fmt.Errorf("[setTxMined][%s] error updating tx mined status: %w", block.Hash().String(), err)
	}

	// delete the block from the cache, if it was there
	if blockWasAlreadyCached {
		u.lastValidatedBlocks.Delete(*blockHash)
	}

	// update block mined_set to true
	if err = u.blockchainClient.SetBlockMinedSet(ctx, blockHash); err != nil {
		return fmt.Errorf("[setTxMined][%s] failed to set block mined: %s", block.Hash().String(), err)
	}

	return nil
}

// isParentMined: check whether the parent block has been set to mined in the db
// keep on trying until the parent block has been set to mined
func (u *BlockValidation) isParentMined(ctx context.Context, blockHeader *model.BlockHeader) (bool, error) {
	blockNotMined, err := u.blockchainClient.GetBlocksMinedNotSet(ctx)
	if err != nil {
		return false, fmt.Errorf("[setTxMined][%s] failed to get blocks mined not set: %s", blockHeader.Hash().String(), err)
	}

	// check whether our parent block is in the list of not mined blocks
	parentBlockMined := true
	for _, b := range blockNotMined {
		if b.Header.Hash().IsEqual(blockHeader.HashPrevBlock) {
			parentBlockMined = false
			break
		}
	}

	return parentBlockMined, nil
}

func (u *BlockValidation) SetTxMetaCache(ctx context.Context, hash *chainhash.Hash, txMeta *meta.Data) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:SetTxMetaCache")
		defer func() {
			span.Finish()
		}()

		return cache.SetCache(hash, txMeta)
	}

	return nil
}

func (u *BlockValidation) SetTxMetaCacheFromBytes(_ context.Context, key, txMetaBytes []byte) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		return cache.SetCacheFromBytes(key, txMetaBytes)
	}

	return nil
}

func (u *BlockValidation) SetTxMetaCacheMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:SetTxMetaCacheMinedMulti")
		defer func() {
			span.Finish()
		}()

		return cache.SetMinedMulti(ctx, hashes, blockID)
	}

	return nil
}

func (u *BlockValidation) SetTxMetaCacheMulti(ctx context.Context, keys [][]byte, values [][]byte) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:SetTxMetaCacheMulti")
		defer func() {
			span.Finish()
		}()

		return cache.SetCacheMulti(keys, values)
	}

	return nil
}

func (u *BlockValidation) DelTxMetaCacheMulti(ctx context.Context, hash *chainhash.Hash) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		span, _ := opentracing.StartSpanFromContext(ctx, "BlockValidation:DelTxMetaCacheMulti")
		defer func() {
			span.Finish()
		}()

		return cache.Delete(ctx, hash)
	}

	return nil
}

func (u *BlockValidation) ValidateBlock(ctx context.Context, block *model.Block, baseUrl string, bloomStats *model.BloomStats, disableOptimisticMining ...bool) error {
	timeStart, stat, ctx := tracing.NewStatFromContext(ctx, "ValidateBlock", u.stats)
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:ValidateBlock")
	span.LogKV("block", block.Hash().String())
	defer func() {
		span.Finish()
		stat.AddTime(timeStart)
		prometheusBlockValidationValidateBlock.Inc()
	}()

	// first check if the block already exists in the blockchain
	blockExists, err := u.GetBlockExists(spanCtx, block.Header.Hash())
	if err == nil && blockExists {
		u.logger.Warnf("[ValidateBlock][%s] tried to validate existing block", block.Header.Hash().String())
		return nil
	}

	u.logger.Infof("[ValidateBlock][%s] called", block.Header.Hash().String())

	initialDelay := 10 * time.Millisecond // Initial delay of 10ms
	maxDelay := 5 * time.Second           // Maximum delay
	delay := initialDelay

	// check the size of the block

	// 0 is unlimited so don't check the size
	if u.excessiveblocksize > 0 {
		if block.SizeInBytes > uint64(u.excessiveblocksize) {
			return errors.New(errors.ERR_BLOCK_INVALID, fmt.Sprintf("[ValidateBlock][%s] block size %d exceeds excessiveblocksize %d", block.Header.Hash().String(), block.SizeInBytes, u.excessiveblocksize))
		}
	}

	for {
		parentBlockMined, err := u.isParentMined(ctx, block.Header)
		if err != nil {
			u.logger.Errorf("[BlockValidation:start][%s] failed isParentMined: %s", block.Hash().String(), err)
			time.Sleep(1 * time.Second)
			continue
		}

		if !parentBlockMined {
			u.logger.Warnf("[BlockValidation:start][%s] parent block not mined yet, retrying", block.Hash().String())
			time.Sleep(delay)
			delay = min(2*delay, maxDelay) // Increase delay, ensuring it does not exceed maxDelay
		} else {
			break
		}
	}

	// validate all the subtrees in the block
	u.logger.Infof("[ValidateBlock][%s] validating %d subtrees", block.Hash().String(), len(block.Subtrees))
	if err := u.validateBlockSubtrees(spanCtx, block, baseUrl); err != nil {
		return err
	}
	u.logger.Infof("[ValidateBlock][%s] validating %d subtrees DONE", block.Hash().String(), len(block.Subtrees))

	// Add the coinbase transaction to the metaTxStore
	// don't be tempted to rely on BlockAssembly to do this.
	// We need to be sure that the coinbase transaction is stored before we try and do setMinedMulti().
	u.logger.Infof("[ValidateBlock][%s] height %d storeCoinbaseTx %s", block.Header.Hash().String(), block.Height, block.CoinbaseTx.TxIDChainHash().String())
	if _, err = u.utxoStore.Create(ctx, block.CoinbaseTx, block.Height+100); err != nil {
		if errors.Is(err, errors.ErrTxAlreadyExists) {
			u.logger.Warnf("[ValidateBlock][%s] coinbase tx already exists: %s", block.Header.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())
		} else {
			return errors.New(errors.ERR_TX_ERROR, fmt.Sprintf("[ValidateBlock][%s] error storing utxos: %v", block.Header.Hash().String(), err))
		}
	}
	u.logger.Infof("[ValidateBlock][%s] storeCoinbaseTx DONE", block.Header.Hash().String())

	useOptimisticMining := u.optimisticMining
	if len(disableOptimisticMining) > 0 {
		// if the disableOptimisticMining is set to true, then we don't use optimistic mining, even if it is enabled
		useOptimisticMining = useOptimisticMining && !disableOptimisticMining[0]
	}

	var optimisticMiningWg sync.WaitGroup
	if useOptimisticMining {
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

		if err = u.SetBlockExists(block.Header.Hash()); err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Header.Hash().String(), err)
		}

		// decouple the tracing context to not cancel the context when finalize the block processing in the background
		callerSpan := opentracing.SpanFromContext(spanCtx)
		validateCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

		optimisticMiningWg.Add(1)
		go func() {
			defer optimisticMiningWg.Done()

			// get all 100 previous block headers on the main chain
			u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders", block.Header.Hash().String())
			blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(validateCtx, block.Header.HashPrevBlock, 100)
			if err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to get block headers: %s", block.String(), err)
				u.ReValidateBlock(block, baseUrl)
				return
			}

			blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(validateCtx, block.Header.HashPrevBlock, 100)
			if err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to get block header ids: %s", block.String(), err)
				u.ReValidateBlock(block, baseUrl)
				return
			}
			u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders DONE", block.Header.Hash().String())

			u.logger.Infof("[ValidateBlock][%s] validating block in background", block.Hash().String())

			u.recentBlocksBloomFiltersMu.Lock()
			bloomFilters := make([]*model.BlockBloomFilter, 0)
			bloomFilters = append(bloomFilters, u.recentBlocksBloomFilters...)
			u.recentBlocksBloomFiltersMu.Unlock()

			if ok, err := block.Valid(validateCtx, u.logger, u.subtreeStore, u.utxoStore, bloomFilters, blockHeaders, blockHeaderIDs, bloomStats); !ok {
				u.logger.Errorf("[ValidateBlock][%s] InvalidateBlock block is not valid in background: %v", block.String(), err)

				if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrProcessing) {
					// storage or processing error, block is not really invalid, but we need to re-validate
					u.ReValidateBlock(block, baseUrl)
				} else {
					u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] block is invalid: %v", block.String(), err)
					// TODO TEMP disable invalidation in the scaling test
					//if err = u.blockchainClient.InvalidateBlock(validateCtx, block.Header.Hash()); err != nil {
					//	u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] failed to invalidate block: %v", block.String(), err)
					//}
				}
			}
		}()
	} else {
		// get all 100 previous block headers on the main chain
		u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders", block.Header.Hash().String())
		blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(spanCtx, block.Header.HashPrevBlock, 100)
		if err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to get block headers: %s", block.String(), err)
			u.ReValidateBlock(block, baseUrl)
			return fmt.Errorf("[ValidateBlock][%s] failed to get block headers: %s", block.String(), err)
		}

		blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(spanCtx, block.Header.HashPrevBlock, 100)
		if err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to get block header ids: %s", block.String(), err)
			u.ReValidateBlock(block, baseUrl)
			return fmt.Errorf("[ValidateBlock][%s] failed to get block header ids: %s", block.String(), err)
		}
		u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders DONE", block.Header.Hash().String())

		// validate the block
		u.logger.Infof("[ValidateBlock][%s] validating block", block.Hash().String())

		u.recentBlocksBloomFiltersMu.Lock()
		bloomFilters := make([]*model.BlockBloomFilter, 0)
		bloomFilters = append(bloomFilters, u.recentBlocksBloomFilters...)
		u.recentBlocksBloomFiltersMu.Unlock()

		if ok, err := block.Valid(spanCtx, u.logger, u.subtreeStore, u.utxoStore, bloomFilters, blockHeaders, blockHeaderIDs, bloomStats); !ok {
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

		if err = u.SetBlockExists(block.Header.Hash()); err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Header.Hash().String(), err)
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
		u.logger.Infof("[ValidateBlock][%s] updating subtrees TTL", block.Hash().String())
		err := u.updateSubtreesTTL(setCtx, block)
		if err != nil {
			// TODO: what to do here? We have already added the block to the blockchain
			u.logger.Errorf("[ValidateBlock][%s] failed to update subtrees TTL [%s]", block.Hash().String(), err)
		}
		u.logger.Infof("[ValidateBlock][%s] update subtrees TTL DONE", block.Hash().String())
	}()

	go func() {
		/// create bloom filter for the block and store
		u.logger.Infof("[ValidateBlock][%s] creating bloom filter for the validated block", block.Hash().String())
		// TODO: for the next phase, consider re-building the bloom filter in the background when node restarts.
		// currently, if the node restarts, the bloom filter is not re-built and the node will not be able to validate transactions properly,
		// since there are no bloom filters. This is a temporary solution to get the node up and running.
		// Opitons are:
		// 1. Re-build the bloom filter in the background when the node restarts
		// 2. after creating the bloom filter, record it in the storage, and delete it after it expires.
		u.createAppendBloomFilter(setCtx, block)
		u.logger.Infof("[ValidateBlock][%s] creating bloom filter is DONE", block.Hash().String())
	}()

	prometheusBlockValidationValidateBlockDuration.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	u.logger.Infof("[ValidateBlock][%s] DONE but updateSubtreesTTL will continue in the background", block.Hash().String())

	return nil
}

func (u *BlockValidation) ReValidateBlock(block *model.Block, baseUrl string) {
	u.logger.Errorf("[ValidateBlock][%s] re-validating block", block.String())
	u.revalidateBlockChan <- revalidateBlockData{
		block:   block,
		baseUrl: baseUrl,
	}
}

func (u *BlockValidation) reValidateBlock(blockData revalidateBlockData) error {
	ctx := context.Background()

	timeStart, stat, ctx := tracing.NewStatFromContext(ctx, "reValidateBlock", u.stats)
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:reValidateBlock")
	span.LogKV("block", blockData.block.Hash().String())
	defer func() {
		span.Finish()
		stat.AddTime(timeStart)
		prometheusBlockValidationValidateBlock.Inc()
	}()

	// make a copy of the recent bloom filters, so we don't get race conditions if the bloom filters are updated
	u.recentBlocksBloomFiltersMu.Lock()
	bloomFilters := make([]*model.BlockBloomFilter, 0)
	bloomFilters = append(bloomFilters, u.recentBlocksBloomFilters...)
	u.recentBlocksBloomFiltersMu.Unlock()

	// validate all the subtrees in the block
	u.logger.Infof("[ReValidateBlock][%s] validating %d subtrees", blockData.block.Hash().String(), len(blockData.block.Subtrees))
	if err := u.validateBlockSubtrees(spanCtx, blockData.block, blockData.baseUrl); err != nil {
		return err
	}
	u.logger.Infof("[ReValidateBlock][%s] validating %d subtrees DONE", blockData.block.Hash().String(), len(blockData.block.Subtrees))

	if ok, err := blockData.block.Valid(spanCtx, u.logger, u.subtreeStore, u.utxoStore, bloomFilters, blockData.blockHeaders, blockData.blockHeaderIDs, u.bloomFilterStats); !ok {
		u.logger.Errorf("[ReValidateBlock][%s] InvalidateBlock block is not valid in background: %v", blockData.block.String(), err)

		if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrProcessing) {
			// storage error, block is not really invalid, but we need to re-validate
			return err
		} else {
			u.logger.Errorf("[ReValidateBlock][%s][InvalidateBlock] block is invalid: %v", blockData.block.String(), err)
			// TODO TEMP disable invalidation in the scaling test
			//if err = u.blockchainClient.InvalidateBlock(spanCtx, blockData.block.Header.Hash()); err != nil {
			//	u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] failed to invalidate block: %s", blockData.block.String(), err)
			//}
		}
	}

	return nil
}

func (u *BlockValidation) createAppendBloomFilter(ctx context.Context, block *model.Block) {
	if u.blockBloomFiltersBeingCreated.Exists(*block.Hash()) {
		return
	}

	// check whether the bloom filter for this block already exists
	for _, bf := range u.recentBlocksBloomFilters {
		if bf.BlockHash.IsEqual(block.Hash()) {
			return
		}
	}

	_ = u.blockBloomFiltersBeingCreated.Put(*block.Hash())
	defer func() {
		_ = u.blockBloomFiltersBeingCreated.Delete(*block.Hash())
	}()

	startTime := time.Now()
	u.logger.Infof("[createAppendBloomFilter][%s] creating bloom filter", block.Hash().String())

	var err error

	// create a bloom filter for the block
	bbf := &model.BlockBloomFilter{}
	bbf.Filter, err = block.NewOptimizedBloomFilter(ctx, u.logger, u.subtreeStore)
	if err != nil {
		u.logger.Errorf("[createAppendBloomFilter][%s] failed to create bloom filter: %s", block.Hash().String(), err)
		return
	}

	bbf.CreationTime = time.Now()
	bbf.BlockHash = block.Hash()

	// prune older bloom filters
	u.recentBlocksBloomFiltersMu.Lock()
	newBloomFilters := make([]*model.BlockBloomFilter, 0, len(u.recentBlocksBloomFilters))
	for _, bf := range u.recentBlocksBloomFilters {
		// only add recent bloom filters back to the list
		if bf.CreationTime.After(time.Now().Add(-u.recentBlocksBloomFiltersExpiration)) {
			newBloomFilters = append(newBloomFilters, bf)
		}
	}
	u.recentBlocksBloomFilters = newBloomFilters

	// append the most recently validated bloom filter
	u.recentBlocksBloomFilters = append(u.recentBlocksBloomFilters, bbf)

	u.logger.Infof("[createAppendBloomFilter][%s] creating bloom filter DONE in %s (%d slices)", block.Hash().String(), time.Since(startTime), len(u.recentBlocksBloomFilters))

	u.recentBlocksBloomFiltersMu.Unlock()
}

// storeCoinbaseTx
func (u *BlockValidation) _(spanCtx context.Context, block *model.Block) (err error) {
	childSpan, childSpanCtx := opentracing.StartSpanFromContext(spanCtx, "BlockValidation:storeCoinbaseTx")
	defer func() {
		childSpan.Finish()
	}()

	// TODO - we need to consider if we can do this differently
	if _, err = u.utxoStore.Create(childSpanCtx, block.CoinbaseTx); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("[ValidateBlock][%s] failed to create coinbase transaction in txMetaStore [%s]", block.Hash().String(), err.Error())
		}
	}

	return nil
}

func (u *BlockValidation) updateSubtreesTTL(ctx context.Context, block *model.Block) (err error) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:updateSubtreesTTL")
	defer func() {
		span.Finish()
	}()

	subtreeTTLConcurrency, _ := gocore.Config().GetInt("blockvalidation_subtreeTTLConcurrency", 32)

	// update the subtree TTLs
	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(subtreeTTLConcurrency)

	for _, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash
		g.Go(func() error {
			if err := u.subtreeStore.SetTTL(gCtx, subtreeHash[:], 0); err != nil {
				return errors.Join(errors.New(errors.ERR_STORAGE_ERROR, "failed to update subtree TTL"), err)
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.Join(fmt.Errorf("[ValidateBlock][%s] failed to update subtree TTLs", block.Hash().String()), err)
	}

	// update block subtrees_set to true
	if err = u.blockchainClient.SetBlockSubtreesSet(ctx, block.Hash()); err != nil {
		return fmt.Errorf("[ValidateBlock][%s] failed to set block subtrees_set: %s", block.Hash().String(), err)
	}

	return nil
}

func (u *BlockValidation) validateBlockSubtrees(ctx context.Context, block *model.Block, baseUrl string) error {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:validateBlockSubtrees")
	start, stat, spanCtx := tracing.StartStatFromContext(spanCtx, "ValidateBlockSubtrees")
	defer func() {
		span.Finish()
		stat.AddTime(start)
	}()

	validateBlockSubtreesConcurrency, _ := gocore.Config().GetInt("blockvalidation_validateBlockSubtreesConcurrency", util.Max(4, runtime.NumCPU()/2))

	start1 := gocore.CurrentTime()
	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(validateBlockSubtreesConcurrency) // keep 32 cores free for other tasks

	blockHeight := block.Height
	if block.Header.Version > 1 {
		var err error
		blockHeight, err = block.ExtractCoinbaseHeight()
		if err != nil {
			return fmt.Errorf("[validateBlockSubtrees][%s] failed to extract coinbase height: %v", block.Hash().String(), err)
		}
	}

	//missingSubtrees := make([]*chainhash.Hash, len(block.Subtrees))
	for _, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash
		//idx := idx
		// first check all the subtrees exist or not in our store, in parallel, and gather what is missing
		g.Go(func() error {
			// get subtree from store
			subtreeExists, err := u.GetSubtreeExists(gCtx, subtreeHash)
			if err != nil {
				return fmt.Errorf("[validateBlockSubtrees][%s] failed to check if subtree exists in store: %w", subtreeHash.String(), err)
			}
			if !subtreeExists {
				u.logger.Infof("[validateBlockSubtrees][%s] instructing stv to check missing subtree [%s]", block.Hash().String(), subtreeHash.String())
				// we don't have the subtree, so we need to process it in the subtree validation service
				// this will also store the subtree in the store and block while the subtree is being processed
				// we do this with a timeout of max 2 minutes
				checkCtx, cancel := context.WithTimeout(spanCtx, 2*time.Minute)
				defer func() {
					cancel()
				}()
				err = u.subtreeValidationClient.CheckSubtree(checkCtx, *subtreeHash, baseUrl, blockHeight)
				if err != nil {
					return fmt.Errorf("[validateBlockSubtrees][%s] failed to get subtree from subtree validation service: %v", subtreeHash.String(), err)
				}
			}

			return nil
		})
	}
	err := g.Wait()
	stat.NewStat("1. missingSubtrees").AddTime(start1)
	if err != nil {
		return err
	}

	//count := 0
	//for _, subtreeHash := range missingSubtrees {
	//	if subtreeHash != nil {
	//		count++
	//	}
	//}
	//
	//if count > 0 {
	//	u.logger.Infof("[validateBlockSubtrees][%s] missing %d of %d subtrees", block.Hash().String(), count, len(block.Subtrees))
	//}
	//
	//startGet := gocore.CurrentTime()
	//statGet := stat.NewStat("1b. GetSubtrees")
	//
	//subtreeBytesMap := make(map[chainhash.Hash][]chainhash.Hash, len(missingSubtrees))
	//subtreeBytesMapMu := sync.Mutex{}
	//g, gCtx = errgroup.WithContext(spanCtx)
	//g.SetLimit(validateBlockSubtreesConcurrency) // mostly IO bound, so double the limit
	//
	//for _, subtreeHash := range missingSubtrees {
	//	// since the missingSubtrees is a full slice with only the missing subtrees set, we need to check if it's nil
	//	if subtreeHash != nil {
	//		subtreeHash := subtreeHash
	//		g.Go(func() error {
	//			// get subtree from network over http using the baseUrl
	//			txHashes, err := u.getSubtreeTxHashes(spanCtx, statGet, subtreeHash, baseUrl)
	//			if err != nil {
	//				return fmt.Errorf("[validateBlockSubtrees][%s] failed to get subtree from network: %v", subtreeHash.String(), err)
	//			}
	//
	//			subtreeBytesMapMu.Lock()
	//			subtreeBytesMap[*subtreeHash] = txHashes
	//			subtreeBytesMapMu.Unlock()
	//
	//			return nil
	//		})
	//	}
	//}
	//statGet.AddTime(startGet)
	//
	//if err = g.Wait(); err != nil {
	//	return fmt.Errorf("[validateBlockSubtrees][%s] failed to get subtrees for block: %v", block.Hash().String(), err)
	//}

	//start2 := gocore.CurrentTime()
	//stat2 := stat.NewStat("2. validateBlockSubtrees")
	//// validate the missing subtrees in series, transactions might rely on each other
	//for _, subtreeHash := range missingSubtrees {
	//	// since the missingSubtrees is a full slice with only the missing subtrees set, we need to check if it's nil
	//	if subtreeHash != nil {
	//		ctx1 := util.ContextWithStat(spanCtx, stat2)
	//		v := ValidateSubtree{
	//			SubtreeHash:   *subtreeHash,
	//			BaseUrl:       baseUrl,
	//			SubtreeHashes: subtreeBytesMap[*subtreeHash],
	//			AllowFailFast: false,
	//		}
	//		wasDeduplicated, err := u.validateSubtree(ctx1, v)
	//		if wasDeduplicated && err != nil && errors.Is(err, ubsverrors.ErrThresholdExceeded) {
	//			// do it again, this time it will not be fail-fast mode
	//			err = u.validateSubtreeInternal(ctx1, v)
	//		}
	//		if err != nil {
	//			return errors.Join(fmt.Errorf("[validateBlockSubtrees][%s] invalid subtree found [%s]", block.Hash().String(), subtreeHash.String()), err)
	//		}
	//	}
	//}
	//stat2.AddTime(start2)

	return nil
}
