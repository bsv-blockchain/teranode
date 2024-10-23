package blockvalidation

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/deduplicator"
	"github.com/bitcoin-sv/ubsv/util/retry"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type revalidateBlockData struct {
	block          *model.Block
	blockHeaders   []*model.BlockHeader
	blockHeaderIDs []uint32
	baseURL        string
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
	excessiveBlockSize                 int
	lastUsedBaseURL                    string
	maxPreviousBlockHeadersToCheck     uint64
}

func NewBlockValidation(ctx context.Context, logger ulogger.Logger, blockchainClient blockchain.ClientI, subtreeStore blob.Store,
	txStore blob.Store, txMetaStore utxo.Store, validatorClient validator.Interface, subtreeValidationClient subtreevalidation.Interface, bloomExpiration time.Duration) *BlockValidation {

	subtreeTTLMinutes, _ := gocore.Config().GetInt("blockvalidation_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	optimisticMining := gocore.Config().GetBool("optimisticMining", true)
	logger.Infof("optimisticMining = %v", optimisticMining)

	excessiveblocksize, _ := gocore.Config().GetInt("excessiveblocksize", 0)
	maxPreviousBlockHeadersToCheck, _ := gocore.Config().GetInt("blockvalidation_maxPreviousBlockHeadersToCheck", 100)

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
		excessiveBlockSize:                 excessiveblocksize,
		lastUsedBaseURL:                    "",
		maxPreviousBlockHeadersToCheck:     uint64(maxPreviousBlockHeadersToCheck),
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

					if notification.Type == model.NotificationType_Block {
						cHash := chainhash.Hash(notification.Hash)
						bv.logger.Infof("[BlockValidation:setMined] received BlockSubtreesSet notification. STU: %s", cHash.String())

						// convert hash to chainhash
						hash, err := chainhash.NewHash(notification.Hash)
						if err != nil {
							bv.logger.Errorf("[BlockValidation:setMined] failed to convert hash to chainhash: %s", err)
							continue
						}
						// push block hash to the setMinedChan
						bv.setMinedChan <- hash
					}
				}
			}
		}()
	}

	go func() {
		if err := bv.start(ctx); err != nil {
			logger.Errorf("[BlockValidation:start] failed to start: %s", err)
		}
	}()

	return bv
}

func (u *BlockValidation) start(ctx context.Context) error {
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
			return errors.NewServiceError("[BlockValidation:start] failed to start, process old block mined/subtrees sets", err)
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

				u.logger.Debugf("[BlockValidation:start][%s] block setTxMined", blockHash.String())

				_ = u.blockHashesCurrentlyValidated.Put(*blockHash)

				if err := u.setTxMined(ctx, blockHash); err != nil {
					u.logger.Errorf("[BlockValidation:start][%s] failed setTxMined: %s", blockHash.String(), err)
					// put the block back in the setMinedChan
					u.setMinedChan <- blockHash
				} else {
					_ = u.blockHashesCurrentlyValidated.Delete(*blockHash)
				}

				u.logger.Debugf("[BlockValidation:start][%s] block setTxMined DONE in %s", blockHash.String(), time.Since(startTime))
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

	return nil
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
		return errors.NewServiceError("[BlockValidation:start][%s] failed to get block", blockHash.String(), err)
	}

	// get all 100 previous block headers on the main chain
	blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(ctx, block.Header.HashPrevBlock, u.maxPreviousBlockHeadersToCheck)
	if err != nil {
		return errors.NewServiceError("[BlockValidation:start][%s] failed to get block headers", block.String(), err)
	}

	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, u.maxPreviousBlockHeadersToCheck)
	if err != nil {
		return errors.NewServiceError("[BlockValidation:start][%s] failed to get block header ids", block.String(), err)
	}

	// make a copy of the recent bloom filters, so we don't get race conditions if the bloom filters are updated
	u.recentBlocksBloomFiltersMu.Lock()
	bloomFilters := make([]*model.BlockBloomFilter, 0)
	bloomFilters = append(bloomFilters, u.recentBlocksBloomFilters...)
	u.recentBlocksBloomFiltersMu.Unlock()

	oldBlockIDs := &sync.Map{}
	if ok, err := block.Valid(ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDs, bloomFilters, blockHeaders, blockHeaderIDs, u.bloomFilterStats); !ok {
		if iErr := u.blockchainClient.InvalidateBlock(ctx, block.Header.Hash()); err != nil {
			u.logger.Errorf("[BlockValidation:start][%s][InvalidateBlock] failed to invalidate block: %s", block.String(), iErr)
		}

		return errors.NewServiceError("[BlockValidation:start][%s] InvalidateBlock block is not valid", block.String(), err)
	}

	referencedOldBlockIDs, hasTransactionsReferencingOldBlocks := util.ConvertSyncMapToUint32Slice(oldBlockIDs)

	// Check if the old blocks are part of the current chain
	if hasTransactionsReferencingOldBlocks {
		// Flag to check if the old blocks are part of the current chain
		var blocksPartOfCurrentChain bool

		blocksPartOfCurrentChain, err = u.blockchainClient.CheckBlockIsInCurrentChain(ctx, referencedOldBlockIDs)
		if err != nil {
			return errors.NewServiceError("[BlockValidation][%s] failed to check if old blocks are part of the current chain", block.String(), err)
		}

		if !blocksPartOfCurrentChain {
			return errors.NewBlockInvalidError("[BlockValidation][%s] block is not valid, transactions refer old blocks (%v) that are not part of our current chain", block.String(), referencedOldBlockIDs)
		}
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
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetSubtreeExists")
	defer deferFn()

	_, ok := u.subtreeExists.Get(*hash)
	if ok {
		return true, nil
	}

	exists, err := u.subtreeStore.Exists(ctx, hash[:], options.WithFileExtension("subtree"))
	if err != nil {
		return false, err
	}

	if exists {
		u.subtreeExists.Set(*hash, true)
	}

	return exists, nil
}

func (u *BlockValidation) setTxMined(ctx context.Context, blockHash *chainhash.Hash) (err error) {
	var (
		block *model.Block
		ids   []uint32
	)

	cachedBlock, blockWasAlreadyCached := u.lastValidatedBlocks.Get(*blockHash)
	if blockWasAlreadyCached && cachedBlock != nil {
		// we have just validated this block, so we can use the cached block
		// this should have all the subtrees already loaded
		block = cachedBlock
	} else {
		// get the block from the blockchain
		if block, err = u.blockchainClient.GetBlock(ctx, blockHash); err != nil {
			return errors.NewServiceError("[setTxMined][%s] failed to get block from blockchain", blockHash.String(), err)
		}
	}

	// make sure all the subtrees are loaded in the block
	fallbackGetFunc := func(subtreeHash chainhash.Hash) error {
		return u.subtreeValidationClient.CheckSubtree(ctx, subtreeHash, u.lastUsedBaseURL, block.Height, block.Hash())
	}
	_, err = block.GetSubtrees(ctx, u.logger, u.subtreeStore, fallbackGetFunc)
	if err != nil {
		return errors.NewProcessingError("[setTxMined][%s] failed to get subtrees from block", block.Hash().String(), err)
	}

	if ids, err = u.blockchainClient.GetBlockHeaderIDs(ctx, blockHash, 1); err != nil || len(ids) != 1 {
		return errors.NewServiceError("[setTxMined][%s] failed to get block header ids", blockHash.String(), err)
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
		return errors.NewProcessingError("[setTxMined][%s] error updating tx mined status", block.Hash().String(), err)
	}

	// delete the block from the cache, if it was there
	if blockWasAlreadyCached {
		u.lastValidatedBlocks.Delete(*blockHash)
	}

	// update block mined_set to true
	if err = u.blockchainClient.SetBlockMinedSet(ctx, blockHash); err != nil {
		return errors.NewServiceError("[setTxMined][%s] failed to set block mined", block.Hash().String(), err)
	}

	return nil
}

// isParentMined: check whether the parent block has been set to mined in the db
// keep on trying until the parent block has been set to mined
func (u *BlockValidation) isParentMined(ctx context.Context, blockHeader *model.BlockHeader) (bool, error) {
	blockNotMined, err := u.blockchainClient.GetBlocksMinedNotSet(ctx)
	if err != nil {
		return false, errors.NewServiceError("[setTxMined][%s] failed to get blocks mined not set", blockHeader.Hash().String(), err)
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
		_, _, deferFn := tracing.StartTracing(ctx, "SetTxMetaCache")
		defer deferFn()

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
		_, _, deferFn := tracing.StartTracing(ctx, "BlockValidation:SetTxMetaCacheMinedMulti")
		defer deferFn()

		return cache.SetMinedMulti(ctx, hashes, blockID)
	}

	return nil
}

func (u *BlockValidation) SetTxMetaCacheMulti(ctx context.Context, keys [][]byte, values [][]byte) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		_, _, deferFn := tracing.StartTracing(ctx, "BlockValidation:SetTxMetaCacheMulti")
		defer deferFn()

		return cache.SetCacheMulti(keys, values)
	}

	return nil
}

func (u *BlockValidation) DelTxMetaCacheMulti(ctx context.Context, hash *chainhash.Hash) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		_, _, deferFn := tracing.StartTracing(ctx, "BlockValidation:DelTxMetaCacheMulti")
		defer deferFn()

		return cache.Delete(ctx, hash)
	}

	return nil
}

func (u *BlockValidation) ValidateBlock(ctx context.Context, block *model.Block, baseURL string, bloomStats *model.BloomStats, disableOptimisticMining ...bool) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "ValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationValidateBlock),
		tracing.WithLogMessage(u.logger, "[ValidateBlock][%s] validating block from %s", block.Hash().String(), baseURL),
	)
	defer deferFn()

	u.lastUsedBaseURL = baseURL

	// first check if the block already exists in the blockchain
	blockExists, err := u.GetBlockExists(ctx, block.Header.Hash())
	if err == nil && blockExists {
		u.logger.Warnf("[ValidateBlock][%s] tried to validate existing block", block.Header.Hash().String())
		return nil
	}

	u.logger.Infof("[ValidateBlock][%s] called", block.Header.Hash().String())

	// check the size of the block
	// 0 is unlimited so don't check the size
	if u.excessiveBlockSize > 0 {
		if block.SizeInBytes > uint64(u.excessiveBlockSize) {
			return errors.NewBlockInvalidError("[ValidateBlock][%s] block size %d exceeds excessiveblocksize %d", block.Header.Hash().String(), block.SizeInBytes, u.excessiveBlockSize)
		}
	}

	if err = u.waitForParentToBeMined(ctx, block); err != nil {
		// re-trigger the setMinedChan for the parent block
		u.setMinedChan <- block.Header.HashPrevBlock

		// Wait for reValidationBlock to do its thing
		if err = u.waitForParentToBeMined(ctx, block); err != nil {
			// Give up, the parent block isn't being fully validated
			return errors.NewBlockError("[ValidateBlock][%s] given up waiting on parent %s", block.Hash().String(), block.Header.HashPrevBlock.String())
		}
	}

	// validate all the subtrees in the block
	u.logger.Infof("[ValidateBlock][%s] validating %d subtrees", block.Hash().String(), len(block.Subtrees))

	if err = u.validateBlockSubtrees(ctx, block, baseURL); err != nil {
		return err
	}

	u.logger.Infof("[ValidateBlock][%s] validating %d subtrees DONE", block.Hash().String(), len(block.Subtrees))

	useOptimisticMining := u.optimisticMining
	if len(disableOptimisticMining) > 0 {
		// if the disableOptimisticMining is set to true, then we don't use optimistic mining, even if it is enabled
		useOptimisticMining = useOptimisticMining && !disableOptimisticMining[0]
		u.logger.Infof("[ValidateBlock][%s] useOptimisticMining override: %v", block.Header.Hash().String(), useOptimisticMining)
	}

	var optimisticMiningWg sync.WaitGroup

	oldBlockIDs := &sync.Map{}

	if useOptimisticMining {
		// make sure the proof of work is enough
		headerValid, _, err := block.Header.HasMetTargetDifficulty()
		if !headerValid {
			return errors.NewBlockInvalidError("invalid block header: %s", block.Header.Hash().String(), err)
		}

		// set the block in the temporary block cache for 2 minutes, could then be used for SetMined
		// must be set before AddBlock is called
		u.lastValidatedBlocks.Set(*block.Hash(), block)

		u.logger.Infof("[ValidateBlock][%s] adding block optimistically to blockchain", block.Hash().String())

		if err = u.blockchainClient.AddBlock(ctx, block, baseURL); err != nil {
			return errors.NewServiceError("[ValidateBlock][%s] failed to store block", block.Hash().String(), err)
		}

		u.logger.Infof("[ValidateBlock][%s] adding block optimistically to blockchain DONE", block.Hash().String())

		if err = u.SetBlockExists(block.Header.Hash()); err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Header.Hash().String(), err)
		}

		// decouple the tracing context to not cancel the context when finalize the block processing in the background
		callerSpan := tracing.DecoupleTracingSpan(ctx, "decoupleValidateBlock")
		defer callerSpan.Finish()

		optimisticMiningWg.Add(1)

		go func() {
			defer optimisticMiningWg.Done()

			// get all 100 previous block headers on the main chain
			u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders", block.Header.Hash().String())

			blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(callerSpan.Ctx, block.Header.HashPrevBlock, u.maxPreviousBlockHeadersToCheck)
			if err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to get block headers: %s", block.String(), err)
				u.ReValidateBlock(block, baseURL)

				return
			}

			blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(callerSpan.Ctx, block.Header.HashPrevBlock, u.maxPreviousBlockHeadersToCheck)
			if err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to get block header ids: %s", block.String(), err)

				u.ReValidateBlock(block, baseURL)

				return
			}

			u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders DONE", block.Header.Hash().String())

			u.logger.Infof("[ValidateBlock][%s] validating block in background", block.Hash().String())

			u.recentBlocksBloomFiltersMu.Lock()
			bloomFilters := make([]*model.BlockBloomFilter, 0)
			bloomFilters = append(bloomFilters, u.recentBlocksBloomFilters...)
			u.recentBlocksBloomFiltersMu.Unlock()

			if ok, err := block.Valid(callerSpan.Ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDs, bloomFilters, blockHeaders, blockHeaderIDs, bloomStats); !ok {
				u.logger.Errorf("[ValidateBlock][%s] InvalidateBlock block is not valid in background: %v", block.String(), err)

				if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrProcessing) {
					// storage or processing error, block is not really invalid, but we need to re-validate
					u.ReValidateBlock(block, baseURL)
				} else {
					// TODO TEMP disable invalidation in the scaling test
					//      Since the invalidation is disabled, here we are not invalidating the block
					// 		Consider enabling the invalidation in the future
					// if err = u.blockchainClient.InvalidateBlock(validateCtx, block.Header.Hash()); err != nil {
					//	u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] failed to invalidate block: %v", block.String(), err)
					//}
					u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] block is invalid: %v", block.String(), err)
				}
			}

			referencedOldBlockIDs, hasTransactionsReferencingOldBlocks := util.ConvertSyncMapToUint32Slice(oldBlockIDs)

			// Check if the old blocks are part of the current chain
			if hasTransactionsReferencingOldBlocks {
				// Flag to check if the old blocks are part of the current chain
				var blocksPartOfCurrentChain bool
				// TODO: what to do with the error here other than logging?
				blocksPartOfCurrentChain, err = u.blockchainClient.CheckBlockIsInCurrentChain(ctx, referencedOldBlockIDs)
				if err != nil {
					u.logger.Errorf("[ValidateBlock][%s] failed to check if old blocks are part of the current chain: %v", block.String(), err)
				}

				if !blocksPartOfCurrentChain {
					// TODO TEMP disable invalidation in the scaling test. Re-enable in the future
					u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] block is invalid, transactions refer old blocks (%v) that are not part of our current chain", block.String())
				}
			}
		}()
	} else {
		// get all 100 previous block headers on the main chain
		u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders", block.Header.Hash().String())

		blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(ctx, block.Header.HashPrevBlock, 100)
		if err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to get block headers: %s", block.String(), err)
			u.ReValidateBlock(block, baseURL)

			return errors.NewServiceError("[ValidateBlock][%s] failed to get block headers", block.String(), err)
		}

		blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, 100)
		if err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to get block header ids: %s", block.String(), err)
			u.ReValidateBlock(block, baseURL)

			return errors.NewServiceError("[ValidateBlock][%s] failed to get block header ids", block.String(), err)
		}

		u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders DONE", block.Header.Hash().String())

		// validate the block
		u.logger.Infof("[ValidateBlock][%s] validating block", block.Hash().String())

		u.recentBlocksBloomFiltersMu.Lock()
		bloomFilters := make([]*model.BlockBloomFilter, 0)
		bloomFilters = append(bloomFilters, u.recentBlocksBloomFilters...)
		u.recentBlocksBloomFiltersMu.Unlock()

		if ok, err := block.Valid(ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDs, bloomFilters, blockHeaders, blockHeaderIDs, bloomStats); !ok {
			return errors.NewBlockInvalidError("[ValidateBlock][%s] block is not valid", block.String(), err)
		}

		referencedOldBlockIDs, hasTransactionsReferencingOldBlocks := util.ConvertSyncMapToUint32Slice(oldBlockIDs)

		// Check if the old blocks are part of the current chain
		if hasTransactionsReferencingOldBlocks {
			// Flag to check if the old blocks are part of the current chain
			var blocksPartOfCurrentChain bool

			blocksPartOfCurrentChain, err = u.blockchainClient.CheckBlockIsInCurrentChain(ctx, referencedOldBlockIDs)
			if err != nil {
				return errors.NewServiceError("[ValidateBlock][%s] failed to check if old blocks are part of the current chain", block.String(), err)
			}

			if !blocksPartOfCurrentChain {
				return errors.NewBlockInvalidError("[ValidateBlock][%s] block is not valid, transactions refer old blocks (%v) that are not part of our current chain", block.String(), referencedOldBlockIDs)
			}
		}

		u.logger.Infof("[ValidateBlock][%s] validating block DONE", block.Hash().String())

		// set the block in the temporary block cache for 2 minutes, could then be used for SetMined
		// must be set before AddBlock is called
		u.lastValidatedBlocks.Set(*block.Hash(), block)

		// if valid, store the block
		u.logger.Infof("[ValidateBlock][%s] adding block to blockchain", block.Hash().String())

		if err = u.blockchainClient.AddBlock(ctx, block, baseURL); err != nil {
			return errors.NewServiceError("[ValidateBlock][%s] failed to store block", block.Hash().String(), err)
		}

		if err = u.SetBlockExists(block.Header.Hash()); err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Header.Hash().String(), err)
		}

		u.logger.Infof("[ValidateBlock][%s] adding block to blockchain DONE", block.Hash().String())
	}

	u.logger.Infof("[ValidateBlock][%s] storing coinbase in tx store: %s", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())

	if err = u.txStore.Set(ctx, block.CoinbaseTx.TxIDChainHash()[:], block.CoinbaseTx.Bytes()); err != nil {
		u.logger.Errorf("[ValidateBlock][%s] failed to store coinbase transaction [%s]", block.Hash().String(), err)
	}

	u.logger.Infof("[ValidateBlock][%s] storing coinbase in tx store: %s DONE", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())

	// decouple the tracing context to not cancel the context when finalize the block processing in the background
	callerSpan := tracing.DecoupleTracingSpan(ctx, "decoupleValidateBlock")

	go func() {
		u.logger.Infof("[ValidateBlock][%s] updating subtrees TTL", block.Hash().String())

		err := u.updateSubtreesTTL(callerSpan.Ctx, block)
		if err != nil {
			// TODO: what to do here? We have already added the block to the blockchain
			u.logger.Errorf("[ValidateBlock][%s] failed to update subtrees TTL [%s]", block.Hash().String(), err)
		}

		u.logger.Infof("[ValidateBlock][%s] update subtrees TTL DONE", block.Hash().String())
	}()

	if useOptimisticMining {
		// run the bloom filter creation in the background
		go func() {
			/// create bloom filter for the block and store
			u.logger.Infof("[ValidateBlock][%s] creating bloom filter for the validated block in background", block.Hash().String())
			// TODO: for the next phase, consider re-building the bloom filter in the background when node restarts.
			// currently, if the node restarts, the bloom filter is not re-built and the node will not be able to validate transactions properly,
			// since there are no bloom filters. This is a temporary solution to get the node up and running.
			//
			// Options are:
			//   1. Re-build the bloom filter in the background when the node restarts
			//   2. after creating the bloom filter, record it in the storage, and delete it after it expires.
			u.createAppendBloomFilter(callerSpan.Ctx, block)
			u.logger.Infof("[ValidateBlock][%s] creating bloom filter is DONE", block.Hash().String())
		}()
	} else {
		// create bloom filter for the block and wait for it
		u.logger.Infof("[ValidateBlock][%s] creating bloom filter for the validated block", block.Hash().String())
		u.createAppendBloomFilter(callerSpan.Ctx, block)
		u.logger.Infof("[ValidateBlock][%s] creating bloom filter is DONE", block.Hash().String())
	}

	return nil
}

func (u *BlockValidation) waitForParentToBeMined(ctx context.Context, block *model.Block) error {
	// Caution, in regtest, when mining initial blocks, this logic wants to retry over and over as fast as possible to ensure it keeps up
	backOffMultiplier, _ := gocore.Config().GetInt("blockvalidation_isParentMined_retry_backoff_multiplier", 30)
	retryCount, _ := gocore.Config().GetInt("blockvalidation_isParentMined_retry_max_retry", 60)

	checkParentBlock := func() (bool, error) {
		parentBlockMined, err := u.isParentMined(ctx, block.Header)
		if err != nil {
			return false, err
		}

		if !parentBlockMined {
			return false, errors.NewBlockParentNotMinedError("[BlockValidation:isParentMined][%s] parent block %s not mined yet", block.Hash().String(), block.Header.HashPrevBlock.String())
		}

		return true, nil
	}

	_, err := retry.Retry(
		ctx,
		u.logger,
		checkParentBlock,
		retry.WithBackoffDurationType(time.Millisecond),
		retry.WithBackoffMultiplier(backOffMultiplier),
		retry.WithRetryCount(retryCount),
	)

	return err
}

func (u *BlockValidation) ReValidateBlock(block *model.Block, baseURL string) {
	u.logger.Errorf("[ValidateBlock][%s] re-validating block", block.String())
	u.revalidateBlockChan <- revalidateBlockData{
		block:   block,
		baseURL: baseURL,
	}
}

func (u *BlockValidation) reValidateBlock(blockData revalidateBlockData) error {
	ctx, _, deferFn := tracing.StartTracing(context.Background(), "reValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[reValidateBlock][%s] validating block from %s", blockData.block.Hash().String(), blockData.baseURL),
	)
	defer deferFn()

	u.lastUsedBaseURL = blockData.baseURL

	// make a copy of the recent bloom filters, so we don't get race conditions if the bloom filters are updated
	u.recentBlocksBloomFiltersMu.Lock()
	bloomFilters := make([]*model.BlockBloomFilter, 0)
	bloomFilters = append(bloomFilters, u.recentBlocksBloomFilters...)
	u.recentBlocksBloomFiltersMu.Unlock()

	// validate all the subtrees in the block
	u.logger.Infof("[ReValidateBlock][%s] validating %d subtrees", blockData.block.Hash().String(), len(blockData.block.Subtrees))

	if err := u.validateBlockSubtrees(ctx, blockData.block, blockData.baseURL); err != nil {
		return err
	}

	u.logger.Infof("[ReValidateBlock][%s] validating %d subtrees DONE", blockData.block.Hash().String(), len(blockData.block.Subtrees))

	oldBlockIDs := &sync.Map{}

	if ok, err := blockData.block.Valid(ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDs, bloomFilters, blockData.blockHeaders, blockData.blockHeaderIDs, u.bloomFilterStats); !ok {
		u.logger.Errorf("[ReValidateBlock][%s] InvalidateBlock block is not valid in background: %v", blockData.block.String(), err)

		if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrServiceError) {
			// storage or service error, block is not really invalid, but we need to re-validate
			return err
		} else {
			if err = u.blockchainClient.InvalidateBlock(ctx, blockData.block.Header.Hash()); err != nil {
				u.logger.Errorf("[ReValidateBlock][%s][InvalidateBlock] failed to invalidate block: %s", blockData.block.String(), err)
			}
		}
	}

	referencedOldBlockIDs, hasTransactionsReferencingOldBlocks := util.ConvertSyncMapToUint32Slice(oldBlockIDs)

	// Check if the old blocks are part of the current chain
	if hasTransactionsReferencingOldBlocks {
		// Flag to check if the old blocks are part of the current chain
		var blocksPartOfCurrentChain bool

		blocksPartOfCurrentChain, err := u.blockchainClient.CheckBlockIsInCurrentChain(ctx, referencedOldBlockIDs)
		if err != nil {
			return errors.NewServiceError("[ReValidateBlock][%s] failed to check if old blocks are part of the current chain", blockData.block.String(), err)
		}

		if !blocksPartOfCurrentChain {
			return errors.NewBlockInvalidError("[ReValidateBlock][%s] block is not valid, transactions refer old blocks (%v) that are not part of our current chain", blockData.block.String(), referencedOldBlockIDs)
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
	bbf := &model.BlockBloomFilter{
		CreationTime: time.Now(),
		BlockHash:    block.Hash(),
	}

	bbf.Filter, err = block.NewOptimizedBloomFilter(ctx, u.logger, u.subtreeStore)
	if err != nil {
		u.logger.Errorf("[createAppendBloomFilter][%s] failed to create bloom filter: %s", block.Hash().String(), err)
		return
	}

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

func (u *BlockValidation) updateSubtreesTTL(ctx context.Context, block *model.Block) (err error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "BlockValidation:updateSubtreesTTL")
	defer deferFn()

	subtreeTTLConcurrency, _ := gocore.Config().GetInt("blockvalidation_subtreeTTLConcurrency", 32)

	// update the subtree TTLs
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(subtreeTTLConcurrency)

	for _, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash

		g.Go(func() error {
			if err := u.subtreeStore.SetTTL(gCtx, subtreeHash[:], 0, options.WithFileExtension("subtree")); err != nil {
				return errors.NewStorageError("failed to update subtree TTL", err)
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.NewServiceError("[ValidateBlock][%s] failed to update subtree TTLs", block.Hash().String(), err)
	}

	// update block subtrees_set to true
	if err = u.blockchainClient.SetBlockSubtreesSet(ctx, block.Hash()); err != nil {
		return errors.NewServiceError("[ValidateBlock][%s] failed to set block subtrees_set", block.Hash().String(), err)
	}

	return nil
}

func (u *BlockValidation) validateBlockSubtrees(ctx context.Context, block *model.Block, baseURL string) error {
	ctx, stat, deferFn := tracing.StartTracing(ctx, "ValidateBlockSubtrees")
	defer deferFn()

	u.lastUsedBaseURL = baseURL

	validateBlockSubtreesConcurrency, _ := gocore.Config().GetInt("blockvalidation_validateBlockSubtreesConcurrency", util.Max(4, runtime.NumCPU()/2))

	start1 := gocore.CurrentTime()
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(validateBlockSubtreesConcurrency) // keep 32 cores free for other tasks

	blockHeight := block.Height
	if blockHeight == 0 && block.Header.Version > 1 {
		var err error

		blockHeight, err = block.ExtractCoinbaseHeight()
		if err != nil {
			return errors.NewProcessingError("[validateBlockSubtrees][%s] failed to extract coinbase height", block.Hash().String(), err)
		}
	}

	for _, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash // Create a new variable for each iteration to avoid data race

		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
				subtreeExists, err := u.GetSubtreeExists(gCtx, subtreeHash)
				if err != nil {
					return errors.NewStorageError("[validateBlockSubtrees][%s] failed to check if subtree exists in store", subtreeHash.String(), err)
				}

				if !subtreeExists {
					u.logger.Debugf("[validateBlockSubtrees][%s] instructing stv to check missing subtree [%s]", block.Hash().String(), subtreeHash.String())

					checkCtx, cancel := context.WithTimeout(gCtx, 2*time.Minute)
					defer cancel()

					err = u.subtreeValidationClient.CheckSubtree(checkCtx, *subtreeHash, baseURL, blockHeight, block.Hash())
					if err != nil {
						return errors.NewServiceError("[validateBlockSubtrees][%s] failed to get subtree from subtree validation service", subtreeHash.String(), err)
					}
				}

				return nil
			}
		})
	}

	err := g.Wait()

	stat.NewStat("1. validateBlockSubtrees").AddTime(start1)

	if err != nil {
		return errors.NewError("[validateBlockSubtrees][%s] failed to validate subtrees", block.Hash().String(), err)
	}

	return nil
}
