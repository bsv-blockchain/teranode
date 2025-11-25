package blockvalidation

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// ForkState represents the state of a fork
type ForkState int

const (
	ForkStateActive ForkState = iota
	ForkStateStale
	ForkStateResolved
	ForkStateOrphaned
)

// ForkBranch represents a fork branch being processed
type ForkBranch struct {
	ID              string            // Unique identifier for the fork
	BaseHash        *chainhash.Hash   // Common ancestor with main chain
	BaseHeight      uint32            // Height of the common ancestor
	TipHash         *chainhash.Hash   // Current tip of this fork
	TipHeight       uint32            // Height of the tip
	ProcessingBlock *chainhash.Hash   // Currently processing block on this fork
	Blocks          []*chainhash.Hash // Ordered list of blocks in this fork
	CreatedAt       time.Time         // Time fork was created
	LastActivity    time.Time         // Last time fork was updated
	State           ForkState         // Current state of the fork
	Workers         int32             // Number of workers processing this fork (atomic)
	mu              sync.RWMutex
}

// ForkStats contains statistics about a fork
type ForkStats struct {
	ID              string
	BaseHeight      uint32
	TipHeight       uint32
	BlockCount      int
	ProcessingBlock bool
}

// BlockProcessingGuard ensures cleanup even on panic
type BlockProcessingGuard struct {
	fm        *ForkManager
	blockHash *chainhash.Hash
	released  bool
	mu        sync.Mutex
}

func (g *BlockProcessingGuard) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.released {
		g.fm.mu.Lock()

		// Only decrement if the block is still in the map
		// This prevents double-decrement if CleanupFork already removed it
		if g.fm.processingBlocks[*g.blockHash] {
			delete(g.fm.processingBlocks, *g.blockHash)
			g.fm.processingCount.Add(-1) // Decrement only if we removed it
		}

		if forkID, exists := g.fm.blockToFork[*g.blockHash]; exists {
			if fork, forkExists := g.fm.forks[forkID]; forkExists {
				fork.mu.Lock()
				if fork.ProcessingBlock != nil && fork.ProcessingBlock.IsEqual(g.blockHash) {
					fork.ProcessingBlock = nil
				}
				if fork.Workers > 0 {
					atomic.AddInt32(&fork.Workers, -1)
				}
				fork.mu.Unlock()
			}
		}

		if g.fm.priorityQueue != nil {
			g.fm.priorityQueue.Signal()
		}

		g.fm.mu.Unlock()
		g.released = true

		g.fm.logger.Debugf("Released processing lock for block %s", g.blockHash)
	}
}

type condBroadcaster interface {
	Broadcast()
	Signal()
}

// ForkCleanupConfig holds configuration for fork cleanup behavior
type ForkCleanupConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	MaxInactiveForks int
	CheckInterval    time.Duration
}

// ForkResolution represents a resolved fork
type ForkResolution struct {
	ForkID       string
	ResolvedAt   time.Time
	ResolvedTo   string
	FinalBlock   *chainhash.Hash
	FinalHeight  uint32
	BlocksInFork int
}

// ForkResolutionCallback is called when a fork is resolved
type ForkResolutionCallback func(resolution *ForkResolution)

// DefaultForkCleanupConfig returns default cleanup settings
func DefaultForkCleanupConfig() ForkCleanupConfig {
	return ForkCleanupConfig{
		Enabled:          true,
		MaxAge:           1 * time.Hour,
		MaxInactiveForks: 100,
		CheckInterval:    5 * time.Minute,
	}
}

type ForkManager struct {
	logger              ulogger.Logger
	settings            *settings.Settings
	forks               map[string]*ForkBranch
	blockToFork         map[chainhash.Hash]string
	processingBlocks    map[chainhash.Hash]bool
	mu                  sync.RWMutex
	maxParallelForks    int
	maxTrackedForks     int
	priorityQueue       condBroadcaster
	cleanupConfig       ForkCleanupConfig
	mainChainTip        *chainhash.Hash
	mainChainHeight     uint32
	resolutionCallbacks []ForkResolutionCallback
	processingCount     atomic.Int32 // Atomic counter for fast-path checking
}

func NewForkManager(logger ulogger.Logger, tSettings *settings.Settings) *ForkManager {
	return NewForkManagerWithConfig(logger, tSettings, DefaultForkCleanupConfig())
}

func NewForkManagerWithConfig(logger ulogger.Logger, tSettings *settings.Settings, cleanupConfig ForkCleanupConfig) *ForkManager {
	// Determine max parallel forks (default to 4)
	maxParallelForks := 4
	if tSettings.BlockValidation.MaxParallelForks > 0 {
		maxParallelForks = tSettings.BlockValidation.MaxParallelForks
	}

	// Determine max tracked forks (default to 1000)
	maxTrackedForks := 1000
	if tSettings.BlockValidation.MaxTrackedForks > 0 {
		maxTrackedForks = tSettings.BlockValidation.MaxTrackedForks
	}

	return &ForkManager{
		logger:              logger,
		settings:            tSettings,
		forks:               make(map[string]*ForkBranch),
		blockToFork:         make(map[chainhash.Hash]string),
		processingBlocks:    make(map[chainhash.Hash]bool),
		maxParallelForks:    maxParallelForks,
		maxTrackedForks:     maxTrackedForks,
		cleanupConfig:       cleanupConfig,
		resolutionCallbacks: make([]ForkResolutionCallback, 0),
	}
}

func (fm *ForkManager) SetPriorityQueue(pq condBroadcaster) {
	fm.priorityQueue = pq
}

// RegisterFork registers a new fork branch
func (fm *ForkManager) RegisterFork(forkID string, baseHash *chainhash.Hash, baseHeight uint32) *ForkBranch {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fork, exists := fm.forks[forkID]; exists {
		return fork
	}

	now := time.Now()
	fork := &ForkBranch{
		ID:           forkID,
		BaseHash:     baseHash,
		BaseHeight:   baseHeight,
		TipHash:      baseHash,
		TipHeight:    baseHeight,
		Blocks:       make([]*chainhash.Hash, 0),
		CreatedAt:    now,
		LastActivity: now,
		State:        ForkStateActive,
		Workers:      0,
	}

	fm.forks[forkID] = fork
	fm.logger.Infof("Registered new fork %s with base %s at height %d", forkID, baseHash, baseHeight)
	prometheusForkCount.Set(float64(len(fm.forks)))
	if prometheusForkCreated != nil {
		prometheusForkCreated.WithLabelValues("new_fork").Inc()
	}
	return fork
}

// AddBlockToFork adds a block to a fork branch
func (fm *ForkManager) AddBlockToFork(block *model.Block, forkID string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fork, exists := fm.forks[forkID]
	if !exists {
		return errors.NewInvalidArgumentError("fork %s not found when adding block %s", forkID, block.Hash())
	}

	blockHash := block.Hash()
	fm.blockToFork[*blockHash] = forkID

	fork.mu.Lock()
	defer fork.mu.Unlock()

	fork.Blocks = append(fork.Blocks, blockHash)
	if block.Height > fork.TipHeight {
		fork.TipHash = blockHash
		fork.TipHeight = block.Height
	}
	fork.LastActivity = time.Now()

	fm.logger.Debugf("Added block %s to fork %s (total blocks: %d)", blockHash, forkID, len(fork.Blocks))
	return nil
}

// GetForkForBlock returns the fork ID for a given block
func (fm *ForkManager) GetForkForBlock(blockHash *chainhash.Hash) (string, bool) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	forkID, exists := fm.blockToFork[*blockHash]
	return forkID, exists
}

func (fm *ForkManager) CanProcessBlock(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	// Fast-path: Check if we've reached max parallel forks without acquiring any locks
	if int(fm.processingCount.Load()) >= fm.maxParallelForks {
		return false, nil
	}

	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if fm.processingBlocks[*blockHash] {
		return false, nil
	}

	if len(fm.processingBlocks) >= fm.maxParallelForks {
		return false, nil
	}

	if forkID, exists := fm.blockToFork[*blockHash]; exists {
		if fork, forkExists := fm.forks[forkID]; forkExists {
			fork.mu.RLock()
			isProcessing := fork.ProcessingBlock != nil
			fork.mu.RUnlock()
			if isProcessing {
				return false, nil
			}
		}
	}

	return true, nil
}

// MarkBlockProcessing marks a block as being processed with cleanup guarantee
func (fm *ForkManager) MarkBlockProcessing(blockHash *chainhash.Hash) (*BlockProcessingGuard, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.processingBlocks[*blockHash] {
		return nil, errors.NewProcessingError("block %s already being processed", blockHash)
	}

	if len(fm.processingBlocks) >= fm.maxParallelForks {
		return nil, errors.NewProcessingError("max parallel forks reached (%d)", fm.maxParallelForks)
	}

	fm.processingBlocks[*blockHash] = true
	fm.processingCount.Add(1) // Increment atomic counter

	if forkID, exists := fm.blockToFork[*blockHash]; exists {
		if fork, forkExists := fm.forks[forkID]; forkExists {
			fork.mu.Lock()
			fork.ProcessingBlock = blockHash
			atomic.AddInt32(&fork.Workers, 1)
			fork.mu.Unlock()
		}
	}

	guard := &BlockProcessingGuard{
		fm:        fm,
		blockHash: blockHash,
		released:  false,
	}

	runtime.SetFinalizer(guard, func(g *BlockProcessingGuard) {
		if !g.released {
			fm.logger.Errorf("Guard for block %s was not released properly", g.blockHash)
			g.Release()
		}
	})

	fm.logger.Debugf("Started processing block %s", blockHash)
	return guard, nil
}

// StartProcessingBlock marks a block as being processed
func (fm *ForkManager) StartProcessingBlock(blockHash *chainhash.Hash) bool {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.processingBlocks[*blockHash] {
		return false
	}

	if len(fm.processingBlocks) >= fm.maxParallelForks {
		return false
	}

	fm.processingBlocks[*blockHash] = true
	fm.processingCount.Add(1) // Increment atomic counter

	if forkID, exists := fm.blockToFork[*blockHash]; exists {
		if fork, forkExists := fm.forks[forkID]; forkExists {
			fork.mu.Lock()
			fork.ProcessingBlock = blockHash
			fork.mu.Unlock()
		}
	}

	fm.logger.Debugf("Started processing block %s", blockHash)
	return true
}

// FinishProcessingBlock marks a block as finished processing
func (fm *ForkManager) FinishProcessingBlock(blockHash *chainhash.Hash) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.processingBlocks[*blockHash] {
		delete(fm.processingBlocks, *blockHash)
		fm.processingCount.Add(-1) // Decrement atomic counter
	}

	if forkID, exists := fm.blockToFork[*blockHash]; exists {
		if fork, forkExists := fm.forks[forkID]; forkExists {
			fork.mu.Lock()
			if fork.ProcessingBlock != nil && fork.ProcessingBlock.IsEqual(blockHash) {
				fork.ProcessingBlock = nil
			}
			if fork.Workers > 0 {
				atomic.AddInt32(&fork.Workers, -1)
			}
			fork.mu.Unlock()
		}
	}

	if fm.priorityQueue != nil {
		// Use Signal() instead of Broadcast() to wake only one worker
		// This prevents thundering herd where all workers wake up and race for locks
		fm.priorityQueue.Signal()
	}

	fm.logger.Debugf("Finished processing block %s", blockHash)
}

// GetParallelProcessingCount returns the number of blocks currently being processed
func (fm *ForkManager) GetParallelProcessingCount() int {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return len(fm.processingBlocks)
}

// GetForkCount returns the number of active forks
func (fm *ForkManager) GetForkCount() int {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return len(fm.forks)
}

// CleanupFork removes a fork and its associated data
func (fm *ForkManager) CleanupFork(forkID string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fork, exists := fm.forks[forkID]
	if !exists {
		return
	}

	// Remove all blocks associated with this fork
	// Must decrement counter for any blocks that were being processed
	for _, blockHash := range fork.Blocks {
		delete(fm.blockToFork, *blockHash)
		if fm.processingBlocks[*blockHash] {
			delete(fm.processingBlocks, *blockHash)
			fm.processingCount.Add(-1) // Decrement counter to prevent drift
		}
	}

	delete(fm.forks, forkID)
	fm.logger.Infof("Cleaned up fork %s", forkID)
}

// GetFork returns a specific fork by ID
func (fm *ForkManager) GetFork(forkID string) *ForkBranch {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.forks[forkID]
}

// SetProcessingBlockForFork sets the processing block for a fork
func (fm *ForkManager) SetProcessingBlockForFork(forkID string, blockHash *chainhash.Hash) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fork, exists := fm.forks[forkID]
	if !exists {
		return errors.NewInvalidArgumentError("fork %s not found", forkID)
	}

	fork.mu.Lock()
	fork.ProcessingBlock = blockHash
	fork.mu.Unlock()

	return nil
}

// CleanupOldForks removes forks older than the specified height
func (fm *ForkManager) CleanupOldForks(heightThreshold uint32) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	forksToRemove := []string{}
	for forkID, fork := range fm.forks {
		if fork.TipHeight < heightThreshold {
			forksToRemove = append(forksToRemove, forkID)
		}
	}

	for _, forkID := range forksToRemove {
		fork := fm.forks[forkID]

		// Remove all blocks associated with this fork
		// Must decrement counter for any blocks that were being processed
		for _, blockHash := range fork.Blocks {
			delete(fm.blockToFork, *blockHash)
			if fm.processingBlocks[*blockHash] {
				delete(fm.processingBlocks, *blockHash)
				fm.processingCount.Add(-1) // Decrement counter to prevent drift
			}
		}

		delete(fm.forks, forkID)
		fm.logger.Infof("Cleaned up old fork %s (tip height: %d)", forkID, fork.TipHeight)
	}
}

// GetAllForks returns all currently tracked forks
func (fm *ForkManager) GetAllForks() []*ForkBranch {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	forks := make([]*ForkBranch, 0, len(fm.forks))
	for _, fork := range fm.forks {
		forks = append(forks, fork)
	}
	return forks
}

// GetForkStats returns statistics about all forks
func (fm *ForkManager) GetForkStats() map[string]ForkStats {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	stats := make(map[string]ForkStats)
	for forkID, fork := range fm.forks {
		fork.mu.RLock()
		stats[forkID] = ForkStats{
			ID:              forkID,
			BaseHeight:      fork.BaseHeight,
			TipHeight:       fork.TipHeight,
			BlockCount:      len(fork.Blocks),
			ProcessingBlock: fork.ProcessingBlock != nil,
		}
		fork.mu.RUnlock()
	}
	return stats
}

// generateForkID creates a unique fork ID using full hash, height, and timestamp
func (fm *ForkManager) generateForkID(blockHash *chainhash.Hash, height uint32) string {
	return fmt.Sprintf("%s-%d-%d", blockHash.String(), height, time.Now().UnixNano())
}

// DetermineForkID determines the fork ID for a block based on its parent with thread-safe fork creation
func (fm *ForkManager) DetermineForkID(ctx context.Context, block *model.Block, blockchainClient blockchain.ClientI) (string, error) {
	parentHash := block.Header.HashPrevBlock

	fm.mu.Lock()
	if existingForkID, exists := fm.blockToFork[*parentHash]; exists {
		fm.mu.Unlock()
		fm.logger.Debugf("Block %s belongs to existing fork %s", block.Hash(), existingForkID)
		return existingForkID, nil
	}
	fm.mu.Unlock()

	bestHeader, _, err := blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return "", err
	}

	if parentHash.IsEqual(bestHeader.Hash()) {
		return "main", nil
	}

	fm.mu.Lock()
	if existingForkID, exists := fm.blockToFork[*parentHash]; exists {
		fm.mu.Unlock()
		return existingForkID, nil
	}
	fm.mu.Unlock()

	_, parentMeta, err := blockchainClient.GetBlockHeader(ctx, parentHash)

	fm.mu.Lock()
	defer fm.mu.Unlock()

	if existingForkID, exists := fm.blockToFork[*parentHash]; exists {
		return existingForkID, nil
	}

	if len(fm.forks) >= fm.maxTrackedForks {
		return "", errors.NewProcessingError("maximum tracked forks limit reached (%d), rejecting new fork for block %s", fm.maxTrackedForks, block.Hash())
	}

	if err != nil {
		blockHashStr := block.Hash().String()
		forkID := "orphan-" + blockHashStr[len(blockHashStr)-8:]

		if _, exists := fm.forks[forkID]; !exists {
			now := time.Now()
			fork := &ForkBranch{
				ID:           forkID,
				BaseHash:     parentHash,
				BaseHeight:   0,
				TipHash:      parentHash,
				TipHeight:    0,
				Blocks:       make([]*chainhash.Hash, 0),
				CreatedAt:    now,
				LastActivity: now,
				State:        ForkStateActive,
				Workers:      0,
			}
			fm.forks[forkID] = fork
			fm.blockToFork[*parentHash] = forkID
			fm.blockToFork[*block.Hash()] = forkID
			prometheusForkCount.Set(float64(len(fm.forks)))
			if prometheusForkCreated != nil {
				prometheusForkCreated.WithLabelValues("orphan").Inc()
			}
		}
		return forkID, nil
	}

	forkID := fm.generateForkID(parentHash, parentMeta.Height)

	fm.logger.Infof("Creating new fork %s for block %s at height %d", forkID, block.Hash(), block.Height)

	now := time.Now()
	fork := &ForkBranch{
		ID:           forkID,
		BaseHash:     parentHash,
		BaseHeight:   parentMeta.Height,
		TipHash:      parentHash,
		TipHeight:    parentMeta.Height,
		Blocks:       make([]*chainhash.Hash, 0),
		CreatedAt:    now,
		LastActivity: now,
		State:        ForkStateActive,
		Workers:      0,
	}

	fm.forks[forkID] = fork
	fm.blockToFork[*parentHash] = forkID
	fm.blockToFork[*block.Hash()] = forkID

	prometheusForkCount.Set(float64(len(fm.forks)))
	if prometheusForkCreated != nil {
		prometheusForkCreated.WithLabelValues("fork").Inc()
	}

	return forkID, nil
}

// StartCleanupRoutine starts the automatic fork cleanup goroutine
func (fm *ForkManager) StartCleanupRoutine(ctx context.Context) {
	if !fm.cleanupConfig.Enabled {
		fm.logger.Infof("Fork cleanup disabled")
		return
	}

	ticker := time.NewTicker(fm.cleanupConfig.CheckInterval)
	defer ticker.Stop()

	fm.logger.Infof("Started fork cleanup routine (interval: %s, max age: %s)", fm.cleanupConfig.CheckInterval, fm.cleanupConfig.MaxAge)

	for {
		select {
		case <-ctx.Done():
			fm.logger.Infof("Fork cleanup routine stopped")
			return
		case <-ticker.C:
			fm.performCleanup()
		}
	}
}

// performCleanup executes the cleanup logic
func (fm *ForkManager) performCleanup() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	now := time.Now()
	cleanupCandidates := make([]*ForkBranch, 0)

	for _, fork := range fm.forks {
		switch fork.State {
		case ForkStateResolved:
			if now.Sub(fork.LastActivity) > 5*time.Minute {
				cleanupCandidates = append(cleanupCandidates, fork)
			}

		case ForkStateStale, ForkStateOrphaned:
			if now.Sub(fork.LastActivity) > fm.cleanupConfig.MaxAge {
				cleanupCandidates = append(cleanupCandidates, fork)
			}

		case ForkStateActive:
			if now.Sub(fork.LastActivity) > fm.cleanupConfig.MaxAge && atomic.LoadInt32(&fork.Workers) == 0 {
				fork.State = ForkStateStale
				cleanupCandidates = append(cleanupCandidates, fork)
			}
		}
	}

	cleanupCandidates = fm.applyRetentionPolicy(cleanupCandidates)

	for _, fork := range cleanupCandidates {
		fm.logger.Infof("Cleaning up fork %s (state: %v, age: %s, blocks: %d)", fork.ID, fork.State, now.Sub(fork.CreatedAt), len(fork.Blocks))

		prometheusForkLifetime.Observe(now.Sub(fork.CreatedAt).Seconds())
		prometheusForkDepth.Observe(float64(len(fork.Blocks)))

		fm.removeFork(fork)
	}

	if len(cleanupCandidates) > 0 {
		fm.logger.Infof("Cleaned up %d forks, remaining: %d", len(cleanupCandidates), len(fm.forks))
		prometheusForkCount.Set(float64(len(fm.forks)))
	}

	fm.updateAggregateMetrics()
	fm.updateProcessingBlocksMetrics()
}

// removeFork removes a fork and all associated data
// NOTE: Caller must hold fm.mu lock
func (fm *ForkManager) removeFork(fork *ForkBranch) {
	delete(fm.forks, fork.ID)

	for blockHash, forkID := range fm.blockToFork {
		if forkID == fork.ID {
			delete(fm.blockToFork, blockHash)
			// Also check if this block was being processed and decrement counter
			if fm.processingBlocks[blockHash] {
				delete(fm.processingBlocks, blockHash)
				fm.processingCount.Add(-1) // Decrement counter to prevent drift
			}
		}
	}
}

// applyRetentionPolicy ensures we don't remove too many forks
func (fm *ForkManager) applyRetentionPolicy(candidates []*ForkBranch) []*ForkBranch {
	if len(fm.forks) <= fm.cleanupConfig.MaxInactiveForks {
		return candidates
	}

	type forkWithActivity struct {
		fork         *ForkBranch
		lastActivity time.Time
	}

	sorted := make([]forkWithActivity, len(candidates))
	for i, fork := range candidates {
		sorted[i] = forkWithActivity{fork: fork, lastActivity: fork.LastActivity}
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].lastActivity.After(sorted[j].lastActivity)
	})

	canRemove := len(fm.forks) - fm.cleanupConfig.MaxInactiveForks
	if canRemove <= 0 {
		return nil
	}

	if canRemove > len(candidates) {
		canRemove = len(candidates)
	}

	result := make([]*ForkBranch, canRemove)
	for i := 0; i < canRemove; i++ {
		result[i] = sorted[len(sorted)-1-i].fork
	}

	return result
}

// OnForkResolution registers a callback for fork resolution events
func (fm *ForkManager) OnForkResolution(callback ForkResolutionCallback) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fm.resolutionCallbacks = append(fm.resolutionCallbacks, callback)
}

// CheckForkResolution checks if a fork has been resolved after block processing
func (fm *ForkManager) CheckForkResolution(ctx context.Context, processedBlock *model.Block, newMainChainTip *chainhash.Hash, newMainChainHeight uint32) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	previousTip := fm.mainChainTip
	fm.mainChainTip = newMainChainTip
	fm.mainChainHeight = newMainChainHeight

	forkID, exists := fm.blockToFork[*processedBlock.Hash()]
	if !exists {
		return
	}

	fork, exists := fm.forks[forkID]
	if !exists {
		return
	}

	if processedBlock.Hash().IsEqual(newMainChainTip) {
		fm.handleForkResolution(fork, "main", processedBlock)
		fm.checkOrphanedForks(newMainChainHeight)
	}

	_ = previousTip
}

// handleForkResolution marks a fork as resolved and notifies callbacks
func (fm *ForkManager) handleForkResolution(fork *ForkBranch, resolvedTo string, finalBlock *model.Block) {
	fm.logger.Infof("Fork %s resolved to %s at height %d with %d blocks", fork.ID, resolvedTo, finalBlock.Height, len(fork.Blocks))

	fork.State = ForkStateResolved
	fork.LastActivity = time.Now()

	resolution := &ForkResolution{
		ForkID:       fork.ID,
		ResolvedAt:   time.Now(),
		ResolvedTo:   resolvedTo,
		FinalBlock:   finalBlock.Hash(),
		FinalHeight:  finalBlock.Height,
		BlocksInFork: len(fork.Blocks),
	}

	for _, callback := range fm.resolutionCallbacks {
		go callback(resolution)
	}

	if prometheusForkResolved != nil {
		prometheusForkResolved.WithLabelValues(resolvedTo).Inc()
	}
	if prometheusForkResolutionDepth != nil {
		prometheusForkResolutionDepth.Observe(float64(len(fork.Blocks)))
	}
}

// updateProcessingBlocksMetrics tracks blocks that may be stuck in processing state
func (fm *ForkManager) updateProcessingBlocksMetrics() {
	if prometheusProcessingBlocksStuck == nil {
		return
	}

	stuckCount := 0
	for _, fork := range fm.forks {
		if fork.ProcessingBlock != nil && atomic.LoadInt32(&fork.Workers) == 0 {
			stuckCount++
		}
	}

	prometheusProcessingBlocksStuck.Set(float64(stuckCount))
}

// updateAggregateMetrics calculates and updates aggregate fork metrics
func (fm *ForkManager) updateAggregateMetrics() {
	if prometheusForkAverageDepth == nil || prometheusForkLongestDepth == nil || prometheusAverageForkLifetime == nil {
		return
	}

	var totalDepth int
	var totalLifetime time.Duration
	var longestDepth int
	var activeForks int
	now := time.Now()

	for _, fork := range fm.forks {
		if fork.State == ForkStateActive {
			activeForks++
			forkDepth := len(fork.Blocks)
			totalDepth += forkDepth
			totalLifetime += now.Sub(fork.CreatedAt)

			if forkDepth > longestDepth {
				longestDepth = forkDepth
			}
		}
	}

	if activeForks > 0 {
		prometheusForkAverageDepth.Set(float64(totalDepth) / float64(activeForks))
		prometheusAverageForkLifetime.Set(totalLifetime.Seconds() / float64(activeForks))
	} else {
		prometheusForkAverageDepth.Set(0)
		prometheusAverageForkLifetime.Set(0)
	}

	prometheusForkLongestDepth.Set(float64(longestDepth))
}

// checkOrphanedForks marks forks as orphaned if they're too far behind the main chain
func (fm *ForkManager) checkOrphanedForks(newMainHeight uint32) {
	orphanThreshold := uint32(fm.settings.ChainCfgParams.CoinbaseMaturity)

	for _, fork := range fm.forks {
		if fork.State != ForkStateActive {
			continue
		}

		if newMainHeight > fork.BaseHeight+orphanThreshold {
			fm.logger.Warnf("Fork %s potentially orphaned (base: %d, main: %d)", fork.ID, fork.BaseHeight, newMainHeight)

			fork.State = ForkStateOrphaned
			fork.LastActivity = time.Now()

			if prometheusOrphanedForks != nil {
				prometheusOrphanedForks.Inc()
			}
		}
	}
}
