package rewindblockchain

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// Stats captures counters for the summary log.
type Stats struct {
	BlocksDeleted          int
	TxsDeleted             int
	TxsBlockIDsTrimmed     int
	SubtreesDeleted        int
	SubtreesSkippedShared  int
	UnminedPurged          int
	ConflictingPurged      int
	ParentConflictsCleaned int
	Duration               time.Duration
}

// Rewind executes all four phases.
func Rewind(ctx context.Context, logger ulogger.Logger, s *settings.Settings, opts Options) error {
	start := time.Now()
	stats := &Stats{}

	// Open the stores directly (no server startup).
	blockchainStore, err := blockchain.NewStore(logger, s.BlockChain.StoreURL, s)
	if err != nil {
		return errors.NewConfigurationError("failed to open blockchain store: %w", err)
	}

	utxoStore, err := utxofactory.NewStore(ctx, logger, s, "rewindblockchain", false)
	if err != nil {
		return errors.NewConfigurationError("failed to open utxo store: %w", err)
	}

	subtreeStore, err := blob.NewStore(logger, s.SubtreeValidation.SubtreeStore)
	if err != nil {
		return errors.NewConfigurationError("failed to open subtree blob store: %w", err)
	}

	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = s.BlockAssembly.MoveBackBlockConcurrency
		if concurrency <= 0 {
			concurrency = 4
		}
	}

	env := &env{
		logger:          logger,
		settings:        s,
		blockchainStore: blockchainStore,
		utxoStore:       utxoStore,
		subtreeStore:    subtreeStore,
		opts:            opts,
		stats:           stats,
		concurrency:     concurrency,
	}

	// Preflight: gates, target resolution, enumeration.
	preflightResult, err := env.preflight(ctx)
	if err != nil {
		return err
	}

	if opts.DryRun {
		logger.Infof("--dry-run: would delete %d blocks above target height %d; stopping before mutation",
			len(preflightResult.deleteList), preflightResult.target)
		env.logStats(time.Since(start))
		return nil
	}

	// Phase 0 — UTXO store internal height reset.
	if err = utxoStore.SetBlockHeight(preflightResult.target); err != nil {
		return errors.NewStorageError("failed to reset UTXO store blockHeight: %w", err)
	}

	logger.Infof("Phase 0 complete: UTXO store blockHeight set to %d", preflightResult.target)

	// Phase 1 — unmined + conflicting cleanup.
	if err = env.phase1Unmined(ctx, preflightResult); err != nil {
		return errors.NewProcessingError("Phase 1 failed: %w", err)
	}

	logger.Infof("Phase 1 complete: unmined_purged=%d conflicting_purged=%d",
		stats.UnminedPurged, stats.ConflictingPurged)

	// Phase 2 — block rewind.
	if err = env.phase2Blocks(ctx, preflightResult); err != nil {
		return errors.NewProcessingError("Phase 2 failed: %w", err)
	}

	logger.Infof("Phase 2 complete: blocks_deleted=%d txs_deleted=%d blockids_trimmed=%d subtrees_deleted=%d subtrees_skipped_shared=%d",
		stats.BlocksDeleted, stats.TxsDeleted, stats.TxsBlockIDsTrimmed,
		stats.SubtreesDeleted, stats.SubtreesSkippedShared)

	// Phase 3 — finalize.
	if err = env.phase3Finalize(ctx, preflightResult); err != nil {
		return errors.NewProcessingError("Phase 3 failed: %w", err)
	}

	if opts.Verify {
		if err = env.phase4Verify(ctx, preflightResult); err != nil {
			return errors.NewProcessingError("Phase 4 verify failed: %w", err)
		}
		logger.Infof("Phase 4 verify complete")
	}

	env.logStats(time.Since(start))
	return nil
}

// env bundles the resolved stores and counters shared across phases.
type env struct {
	logger          ulogger.Logger
	settings        *settings.Settings
	blockchainStore blockchain.Store
	utxoStore       utxo.Store
	subtreeStore    blob.Store
	opts            Options
	stats           *Stats
	concurrency     int
}

// logStats prints the summary at the end of a run.
func (e *env) logStats(d time.Duration) {
	e.stats.Duration = d
	e.logger.Infof("rewind summary: blocks_deleted=%d txs_deleted=%d blockids_trimmed=%d subtrees_deleted=%d subtrees_skipped_shared=%d unmined_purged=%d conflicting_purged=%d parent_conflicts_cleaned=%d duration=%s",
		e.stats.BlocksDeleted,
		e.stats.TxsDeleted,
		e.stats.TxsBlockIDsTrimmed,
		e.stats.SubtreesDeleted,
		e.stats.SubtreesSkippedShared,
		e.stats.UnminedPurged,
		e.stats.ConflictingPurged,
		e.stats.ParentConflictsCleaned,
		d,
	)
}

// fmt is pulled in only so gofmt keeps the import group above tidy when
// we later add more imports. Safe to remove when unneeded.
var _ = fmt.Sprintf
