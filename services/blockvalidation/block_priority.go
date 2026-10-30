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

// BlockProcessor gates which queued blocks may start. Get claims a block's processing slot
// with StartProcessingBlock before removing it from the queue, so a block never leaves the
// queue without a slot; the caller releases the slot with FinishProcessingBlock. priority is
// the class the block was queued with. IsBlockInFlight reports whether the block is processing
// or, by pending, queued or being fetched, reading both with slot changes held off.
type BlockProcessor interface {
	CanProcessBlock(ctx context.Context, hash *chainhash.Hash, priority BlockPriority) (bool, error)
	StartProcessingBlock(hash *chainhash.Hash, priority BlockPriority) bool
	FinishProcessingBlock(hash *chainhash.Hash)
	IsBlockInFlight(hash *chainhash.Hash, pending func(chainhash.Hash) bool) bool
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

	// notBefore holds a requeued block back until its retry delay has passed. The block
	// stays queued meanwhile, so its children still see their parent in flight.
	notBefore time.Time
}

// BlockPriorityQueue implements a priority queue for blocks
type BlockPriorityQueue struct {
	items     []*PrioritizedBlock
	mu        sync.Mutex
	hashIndex map[chainhash.Hash]*PrioritizedBlock
	needsSort bool
	cond      *sync.Cond
	logger    ulogger.Logger

	// fetching holds announced blocks an intake worker is still fetching. A block being
	// fetched is not queued yet, but its children must wait for it all the same.
	fetching map[chainhash.Hash]struct{}

	// wakeups is bumped under mu by every Signal and Broadcast. WaitForBlock compares it with
	// the value it saw before Get, so a wake-up sent while it was not parked is not lost.
	wakeups uint64
}

// NewBlockPriorityQueue creates a new priority queue for blocks
func NewBlockPriorityQueue(logger ulogger.Logger) *BlockPriorityQueue {
	pq := &BlockPriorityQueue{
		items:     make([]*PrioritizedBlock, 0),
		hashIndex: make(map[chainhash.Hash]*PrioritizedBlock),
		needsSort: false,
		logger:    logger,
		fetching:  make(map[chainhash.Hash]struct{}),
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
	pq.wakeups++
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

			taken := currentItem.blockFound
			taken.priority = currentItem.priority
			taken.height = currentItem.height

			return taken, true
		}
	}

	return processBlockFound{}, false
}

// claimAndTake claims the block's processing slot first and only then removes it from the
// queue. Taking first would hand out a block whose slot another worker grabbed after
// CanProcessBlock said yes, and the worker would drop it. If the block is gone by the time
// the slot is claimed, the slot is released again.
func (pq *BlockPriorityQueue) claimAndTake(bp BlockProcessor, hash *chainhash.Hash, priority BlockPriority) (processBlockFound, bool) {
	if !bp.StartProcessingBlock(hash, priority) {
		return processBlockFound{}, false
	}

	takenBlock, ok := pq.takeIfPresent(hash)
	if !ok {
		bp.FinishProcessingBlock(hash)
	}

	return takenBlock, ok
}

// isWaiting reports whether a queued block must not start yet: it is in its retry delay, or
// its parent is still being fetched, queued or processing. Validating it before the parent is stored would
// find no parent and send it to catchup. This gate holds regardless of the block's effective
// priority, so the skip and age promotions cannot bypass it. notBefore and parentHash are
// set when the item is created and never change, so they are read without the lock.
func (pq *BlockPriorityQueue) isWaiting(bp BlockProcessor, item *PrioritizedBlock) bool {
	if time.Now().Before(item.notBefore) {
		return true
	}

	parent := item.blockFound.parentHash
	if parent == nil {
		return false
	}

	return bp.IsBlockInFlight(parent, pq.IsPending)
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

	if !pq.isWaiting(bp, front) {
		if canProcess, err := bp.CanProcessBlock(ctx, front.blockFound.hash, front.priority); err == nil && canProcess {
			if takenBlock, ok := pq.claimAndTake(bp, front.blockFound.hash, front.priority); ok {
				return takenBlock, GetOK
			}
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

		if pq.isWaiting(bp, item) {
			continue
		}

		canProcess, err := bp.CanProcessBlock(ctx, item.blockFound.hash, item.priority)
		if err != nil {
			pq.logger.Warnf("Error checking if block %s can be processed: %v",
				item.blockFound.hash, err)
			continue
		}

		if canProcess {
			if takenBlock, ok := pq.claimAndTake(bp, item.blockFound.hash, item.priority); ok {
				return takenBlock, GetOK
			}

			// The slot was lost to another worker, or the block was taken: leave it queued.
			continue
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

// Broadcast and Signal take mu to record the wake-up. The fork manager calls them while
// holding its own lock, so mu must never be held while calling into the fork manager.
func (pq *BlockPriorityQueue) Broadcast() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.wakeups++
	pq.cond.Broadcast()
}

func (pq *BlockPriorityQueue) Signal() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.wakeups++
	pq.cond.Signal()
}

func (pq *BlockPriorityQueue) WaitForBlock(ctx context.Context, bp BlockProcessor) (processBlockFound, GetStatus) {
	for {
		// Get answers GetEmpty on a cancelled context even with blocks queued, which the
		// GetEmpty re-check below would read as "items appeared" and loop on.
		if ctx.Err() != nil {
			return processBlockFound{}, GetEmpty
		}

		pq.mu.Lock()
		wakeupsBefore := pq.wakeups
		pq.mu.Unlock()

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

			// A wake-up sent while Get was running, such as a retry delay ending or a slot
			// being released, found no parked worker. Look again instead of waiting for the
			// next one.
			if pq.wakeups != wakeupsBefore {
				pq.mu.Unlock()
				continue
			}

			waitCh := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					pq.Broadcast()
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

// IsPending reports whether a block is queued or still being fetched by an intake worker.
func (pq *BlockPriorityQueue) IsPending(hash chainhash.Hash) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	_, queued := pq.hashIndex[hash]
	_, fetching := pq.fetching[hash]

	return queued || fetching
}

// MarkFetching records that an intake worker has started fetching an announced block, and
// reports whether this worker holds the mark. Only the first fetcher does: a duplicate
// announcement still fetching after the first one queued the block must not hold children
// behind it. The holder calls UnmarkFetching once the block is queued or dropped.
func (pq *BlockPriorityQueue) MarkFetching(hash *chainhash.Hash) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if _, fetching := pq.fetching[*hash]; fetching {
		return false
	}

	pq.fetching[*hash] = struct{}{}

	return true
}

// UnmarkFetching ends a fetch started with MarkFetching. A child waiting on a block whose
// fetch failed can start now, so a worker is woken.
func (pq *BlockPriorityQueue) UnmarkFetching(hash *chainhash.Hash) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	delete(pq.fetching, *hash)

	pq.wakeups++
	pq.cond.Signal()
}

// ClearFetching drops a block's fetching mark, whoever holds it. It is called once the block is
// stored, when nothing is left for its children to wait on.
func (pq *BlockPriorityQueue) ClearFetching(hash *chainhash.Hash) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	delete(pq.fetching, *hash)
}

// PromoteChildren raises every queued child of parent to priority, if that is a higher class than
// it has. A child queued while its parent was still being fetched or validated outside the queue
// was classified without it, and is held by isWaiting until the parent finishes, which is when the
// callers promote it. Two callers can promote the same child, so a class is never lowered.
func (pq *BlockPriorityQueue) PromoteChildren(parent *chainhash.Hash, priority BlockPriority) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for _, item := range pq.items {
		if item.blockFound.parentHash != nil && item.blockFound.parentHash.IsEqual(parent) && priority < item.priority {
			item.priority = priority
			pq.needsSort = true
		}
	}

	updateQueueSizeMetrics(pq.items)
}

func (pq *BlockPriorityQueue) Size() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.Len()
}

// PriorityOf returns the class a queued block was added with.
func (pq *BlockPriorityQueue) PriorityOf(hash *chainhash.Hash) (BlockPriority, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	item, ok := pq.hashIndex[*hash]
	if !ok {
		return PriorityDeepFork, false
	}

	return item.priority, true
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
	pq.RequeueAfter(blockFound, priority, height, 0)
}

// RequeueAfter puts a block back on the queue, held back for delay. A worker parked in
// WaitForBlock is woken when the delay ends.
func (pq *BlockPriorityQueue) RequeueAfter(blockFound processBlockFound, priority BlockPriority, height uint32, delay time.Duration) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if existingItem, exists := pq.hashIndex[*blockFound.hash]; exists {
		existingItem.retryCount++
		existingItem.lastRetryTime = time.Now()
		pq.needsSort = true
		// Signal one worker that the queue has been updated
		pq.wakeups++
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
		notBefore:         time.Now().Add(delay),
	}

	if delay > 0 {
		time.AfterFunc(delay, pq.Signal)
	}

	pq.items = append(pq.items, item)
	pq.hashIndex[*item.blockFound.hash] = item
	pq.needsSort = true

	// Always use Signal() to wake exactly one worker
	pq.wakeups++
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
