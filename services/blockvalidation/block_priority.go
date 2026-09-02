package blockvalidation

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
)

type BlockProcessor interface {
	CanProcessBlock(ctx context.Context, hash *chainhash.Hash) (bool, error)
}

// BlockPriority represents the priority level of a block for processing
type GetStatus int

const (
	GetOK GetStatus = iota
	GetEmpty
	GetAllBlocked

	DefaultSkipCount = 10
)

type BlockPriority int

const (
	// PriorityChainExtending blocks that extend the current best chain
	PriorityChainExtending BlockPriority = iota
	// PriorityNearFork blocks that are within a few heights of the best block
	PriorityNearFork
	// PriorityDeepFork blocks that are significantly behind the best block
	PriorityDeepFork
)

// PrioritizedBlock wraps a processBlockFound with priority information
type PrioritizedBlock struct {
	blockFound        processBlockFound
	priority          BlockPriority
	height            uint32
	timestamp         time.Time
	additionalSources []processBlockFound
	retryCount        int
	lastRetryTime     time.Time
	skipCount         int
}

// BlockPriorityQueue implements a priority queue for blocks
type BlockPriorityQueue struct {
	items     []*PrioritizedBlock
	mu        sync.Mutex
	hashIndex map[chainhash.Hash]*PrioritizedBlock
	needsSort bool
	cond      *sync.Cond
	logger    ulogger.Logger
}

// NewBlockPriorityQueue creates a new priority queue for blocks
func NewBlockPriorityQueue(logger ulogger.Logger) *BlockPriorityQueue {
	pq := &BlockPriorityQueue{
		items:     make([]*PrioritizedBlock, 0),
		hashIndex: make(map[chainhash.Hash]*PrioritizedBlock),
		needsSort: false,
		logger:    logger,
	}
	pq.cond = sync.NewCond(&pq.mu)
	return pq
}

// Len returns the number of items in the queue
func (item *PrioritizedBlock) getEffectivePriority(now time.Time) BlockPriority {
	priority := item.priority

	if item.skipCount > DefaultSkipCount {
		return PriorityChainExtending
	}

	ageMinutes := int(now.Sub(item.timestamp).Minutes())
	if ageMinutes > 1 && priority > PriorityChainExtending {
		boost := ageMinutes - 1
		if boost > int(priority-PriorityChainExtending) {
			boost = int(priority - PriorityChainExtending)
		}
		priority -= BlockPriority(boost)
	}

	return priority
}

func (pq *BlockPriorityQueue) sortItems() {
	now := time.Now()

	sort.Slice(pq.items, func(i, j int) bool {
		iPriority := pq.items[i].getEffectivePriority(now)
		jPriority := pq.items[j].getEffectivePriority(now)

		if iPriority != jPriority {
			return iPriority < jPriority
		}

		if pq.items[i].height != pq.items[j].height {
			return pq.items[i].height < pq.items[j].height
		}

		return pq.items[i].timestamp.Before(pq.items[j].timestamp)
	})

	pq.needsSort = false
}

func (pq *BlockPriorityQueue) removeItem(index int) {
	item := pq.items[index]
	pq.items = append(pq.items[:index], pq.items[index+1:]...)
	delete(pq.hashIndex, *item.blockFound.hash)
	updateQueueSizeMetrics(pq.items)
}

func (pq *BlockPriorityQueue) Len() int {
	return len(pq.items)
}

// Add adds a block to the priority queue
func (pq *BlockPriorityQueue) Add(blockFound processBlockFound, priority BlockPriority, height uint32) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if existingItem, exists := pq.hashIndex[*blockFound.hash]; exists {
		existingItem.additionalSources = append(existingItem.additionalSources, blockFound)
		return
	}

	item := &PrioritizedBlock{
		blockFound:        blockFound,
		priority:          priority,
		height:            height,
		timestamp:         time.Now(),
		additionalSources: make([]processBlockFound, 0),
		retryCount:        0,
		lastRetryTime:     time.Time{},
		skipCount:         0,
	}

	pq.items = append(pq.items, item)
	pq.hashIndex[*blockFound.hash] = item
	pq.needsSort = true

	// Always use Signal() to wake exactly one worker per block added
	// This avoids thundering herd where all workers wake and race for one block
	pq.cond.Signal()

	if prometheusBlockPriorityQueueAdded != nil {
		prometheusBlockPriorityQueueAdded.WithLabelValues(priorityToString(priority)).Inc()
	}
	updateQueueSizeMetrics(pq.items)
}

// takeIfPresent removes and returns the block matching hash if it is still queued,
// reporting whether it was found. Used by Get's fast path to claim the front block
// without copying the whole queue.
func (pq *BlockPriorityQueue) takeIfPresent(hash *chainhash.Hash) (processBlockFound, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for i, currentItem := range pq.items {
		if currentItem.blockFound.hash.IsEqual(hash) {
			if blockQueueWaitTime != nil {
				blockQueueWaitTime.Observe(time.Since(currentItem.timestamp).Seconds())
			}

			pq.removeItem(i)

			return currentItem.blockFound, true
		}
	}

	return processBlockFound{}, false
}

func (pq *BlockPriorityQueue) Get(ctx context.Context, bp BlockProcessor) (block processBlockFound, status GetStatus) {
	defer func() {
		if r := recover(); r != nil {
			pq.logger.Errorf("Panic in Get(): %v", r)
			status = GetEmpty
		}
	}()

	pq.mu.Lock()

	if len(pq.items) == 0 {
		pq.mu.Unlock()
		return processBlockFound{}, GetEmpty
	}

	if pq.needsSort {
		pq.sortItems()
	}

	// Fast path: in steady state the highest-priority (front) block is immediately
	// processable, so probe it first without copying the whole queue. Only fall back
	// to the full scan below when the front block is blocked or already taken.
	front := pq.items[0]
	pq.mu.Unlock()

	if ctx.Err() != nil {
		return processBlockFound{}, GetEmpty
	}

	if canProcess, err := bp.CanProcessBlock(ctx, front.blockFound.hash); err == nil && canProcess {
		if takenBlock, ok := pq.takeIfPresent(front.blockFound.hash); ok {
			return takenBlock, GetOK
		}
	}

	// Slow path: the front block was blocked or taken by another worker. Copy the
	// queue once so the CanProcessBlock round-trips below run without holding the lock.
	pq.mu.Lock()

	if len(pq.items) == 0 {
		pq.mu.Unlock()
		return processBlockFound{}, GetEmpty
	}

	itemsCopy := make([]*PrioritizedBlock, len(pq.items))
	copy(itemsCopy, pq.items)
	pq.mu.Unlock()

	// Collect skip count updates to apply in batch
	skipUpdates := make(map[chainhash.Hash]bool)

	for _, item := range itemsCopy {
		select {
		case <-ctx.Done():
			return processBlockFound{}, GetEmpty
		default:
		}

		canProcess, err := bp.CanProcessBlock(ctx, item.blockFound.hash)
		if err != nil {
			pq.logger.Warnf("Error checking if block %s can be processed: %v",
				item.blockFound.hash, err)
			continue
		}

		if canProcess {
			pq.mu.Lock()

			for i, currentItem := range pq.items {
				if currentItem.blockFound.hash.IsEqual(item.blockFound.hash) {
					if blockQueueWaitTime != nil {
						waitTime := time.Since(currentItem.timestamp).Seconds()
						blockQueueWaitTime.Observe(waitTime)
					}

					pq.removeItem(i)
					pq.mu.Unlock()
					return currentItem.blockFound, GetOK
				}
			}

			// Item not found - verify it wasn't already processed
			if _, exists := pq.hashIndex[*item.blockFound.hash]; !exists {
				pq.mu.Unlock()
				continue // Item was processed by another worker, try next
			}
			pq.mu.Unlock()
			return pq.Get(ctx, bp)
		}

		// Mark this item for skip count update (apply later in batch)
		skipUpdates[*item.blockFound.hash] = true
	}

	// Batch apply all skip count updates with a single lock
	if len(skipUpdates) > 0 {
		pq.mu.Lock()
		for _, currentItem := range pq.items {
			if skipUpdates[*currentItem.blockFound.hash] {
				currentItem.skipCount++
				if blockQueueSkipCount != nil {
					blockQueueSkipCount.Observe(float64(currentItem.skipCount))
				}
			}
		}
		pq.mu.Unlock()
	}

	return processBlockFound{}, GetAllBlocked
}

func (pq *BlockPriorityQueue) Broadcast() {
	pq.cond.Broadcast()
}

func (pq *BlockPriorityQueue) Signal() {
	pq.cond.Signal()
}

func (pq *BlockPriorityQueue) WaitForBlock(ctx context.Context, bp BlockProcessor) (processBlockFound, GetStatus) {
	for {
		block, status := pq.Get(ctx, bp)

		switch status {
		case GetOK:
			return block, status
		case GetEmpty, GetAllBlocked:
			// Both empty queue and all-blocked should wait on condition variable
			// to avoid busy loop burning CPU
			pq.mu.Lock()

			// Double-check queue status after acquiring lock.
			// Only GetEmpty can be resolved by items appearing: for GetEmpty the
			// queue was empty, so if items are now present we retry Get() instead
			// of parking. For GetAllBlocked the queue is by definition non-empty,
			// so len(pq.items) > 0 is always true; we must still cond.Wait() and let
			// a Signal from Add (new block), FinishProcessingBlock, or the fork
			// BlockProcessingGuard.Release (a processing slot freeing up) wake us when
			// a block becomes processable, otherwise this spins at 100% CPU re-running Get().
			if status == GetEmpty && len(pq.items) > 0 {
				pq.mu.Unlock()
				continue // Items appeared, try Get() again
			}

			waitCh := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					pq.cond.Broadcast()
				case <-waitCh:
				}
			}()

			pq.cond.Wait()
			pq.mu.Unlock()

			close(waitCh)

			if ctx.Err() != nil {
				return processBlockFound{}, GetEmpty
			}
			continue
		}
	}
}

func (pq *BlockPriorityQueue) Peek() (processBlockFound, BlockPriority, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.Len() == 0 {
		return processBlockFound{}, 0, false
	}

	if pq.needsSort {
		pq.sortItems()
	}

	item := pq.items[0]
	return item.blockFound, item.priority, true
}

func (pq *BlockPriorityQueue) Contains(hash chainhash.Hash) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	_, exists := pq.hashIndex[hash]
	return exists
}

func (pq *BlockPriorityQueue) Size() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.Len()
}

// GetAlternativeSource returns an alternative source for a block if available
func (pq *BlockPriorityQueue) GetAlternativeSource(blockHash *chainhash.Hash) (processBlockFound, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	item, exists := pq.hashIndex[*blockHash]
	if !exists || len(item.additionalSources) == 0 {
		return processBlockFound{}, false
	}

	// Get the first alternative source
	alternative := item.additionalSources[0]
	// Remove it from the list
	item.additionalSources = item.additionalSources[1:]

	return alternative, true
}

// getQueueStats returns statistics about the queue without locking
func getQueueStats(items []*PrioritizedBlock) (chainExtending, nearFork, deepFork int) {
	for _, item := range items {
		switch item.priority {
		case PriorityChainExtending:
			chainExtending++
		case PriorityNearFork:
			nearFork++
		case PriorityDeepFork:
			deepFork++
		}
	}
	return
}

func (pq *BlockPriorityQueue) GetQueueStats() (chainExtending, nearFork, deepFork int) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return getQueueStats(pq.items)
}

func (pq *BlockPriorityQueue) Clear() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.items = pq.items[:0]
	pq.hashIndex = make(map[chainhash.Hash]*PrioritizedBlock)
	pq.needsSort = false
}

// BlockClassifier determines the priority of blocks based on chain state
type BlockClassifier struct {
	logger            ulogger.Logger
	nearForkThreshold uint32 // Heights within this range are considered "near"
	blockchainClient  blockchain.ClientI
}

// NewBlockClassifier creates a new block classifier
func NewBlockClassifier(logger ulogger.Logger, nearForkThreshold uint32, blockchainClient blockchain.ClientI) *BlockClassifier {
	return &BlockClassifier{
		logger:            logger,
		nearForkThreshold: nearForkThreshold,
		blockchainClient:  blockchainClient,
	}
}

// ClassifyBlock determines the priority of a block based on current chain state
func (bc *BlockClassifier) ClassifyBlock(ctx context.Context, block *model.Block) (BlockPriority, error) {
	bestHeader, bestMeta, err := bc.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return PriorityDeepFork, err
	}

	// Check if this block extends the best chain
	if block.Header.HashPrevBlock.IsEqual(bestHeader.Hash()) {
		bc.logger.Debugf("Block %s extends best chain at height %d", block.Hash(), bestMeta.Height+1)
		return PriorityChainExtending, nil
	}

	// Check if the parent exists and get its height
	_, parentMeta, err := bc.blockchainClient.GetBlockHeader(ctx, block.Header.HashPrevBlock)
	if err != nil {
		// Parent doesn't exist, treat as deep fork
		bc.logger.Debugf("Block %s has unknown parent, treating as deep fork", block.Hash())
		return PriorityDeepFork, nil
	}

	// Calculate expected height
	expectedHeight := parentMeta.Height + 1

	// Check if it's within near fork threshold
	if expectedHeight >= bestMeta.Height-bc.nearForkThreshold {
		bc.logger.Debugf("Block %s at height %d is near fork (best height: %d)", block.Hash(), expectedHeight, bestMeta.Height)
		return PriorityNearFork, nil
	}

	bc.logger.Debugf("Block %s at height %d is deep fork (best height: %d)", block.Hash(), expectedHeight, bestMeta.Height)
	return PriorityDeepFork, nil
}

// priorityToString converts BlockPriority to string for metrics labels
func priorityToString(priority BlockPriority) string {
	switch priority {
	case PriorityChainExtending:
		return "chain_extending"
	case PriorityNearFork:
		return "near_fork"
	case PriorityDeepFork:
		return "deep_fork"
	default:
		return "unknown"
	}
}

func (pq *BlockPriorityQueue) RequeueForRetry(blockFound processBlockFound, priority BlockPriority, height uint32) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if existingItem, exists := pq.hashIndex[*blockFound.hash]; exists {
		existingItem.retryCount++
		existingItem.lastRetryTime = time.Now()
		pq.needsSort = true
		// Signal one worker that the queue has been updated
		pq.cond.Signal()
		return
	}

	item := &PrioritizedBlock{
		blockFound:        blockFound,
		priority:          priority,
		height:            height,
		timestamp:         time.Now(),
		additionalSources: make([]processBlockFound, 0),
		retryCount:        1,
		lastRetryTime:     time.Now(),
		skipCount:         0,
	}

	pq.items = append(pq.items, item)
	pq.hashIndex[*item.blockFound.hash] = item
	pq.needsSort = true

	// Always use Signal() to wake exactly one worker
	pq.cond.Signal()

	updateQueueSizeMetrics(pq.items)
}

// updateQueueSizeMetrics updates the queue size metrics
func updateQueueSizeMetrics(items []*PrioritizedBlock) {
	if prometheusBlockPriorityQueueSize == nil {
		return
	}

	chainExtending, nearFork, deepFork := getQueueStats(items)
	prometheusBlockPriorityQueueSize.WithLabelValues("chain_extending").Set(float64(chainExtending))
	prometheusBlockPriorityQueueSize.WithLabelValues("near_fork").Set(float64(nearFork))
	prometheusBlockPriorityQueueSize.WithLabelValues("deep_fork").Set(float64(deepFork))
}
