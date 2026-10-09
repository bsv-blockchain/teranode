package rewindblockchain

import (
	"context"
	"net/url"
	"strconv"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
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

// Stores bundles the backend stores used by Rewind. Tests can pass
// pre-constructed stores via Options.Stores; production callers leave it nil
// and Rewind opens stores from settings.
type Stores struct {
	Blockchain blockchain.Store
	UTXO       utxo.Store
	Subtree    blob.Store
	// Block is where the UTXO persister keeps its lastProcessed marker, which
	// Phase 3 deletes. Optional when Stores is supplied: nil skips the delete.
	Block blob.Store
}

// Rewind executes all four phases.
func Rewind(ctx context.Context, logger ulogger.Logger, s *settings.Settings, opts Options) (*Stats, error) {
	start := time.Now()
	stats := &Stats{}

	stores, ownedByUs, err := resolveStores(ctx, logger, s, opts)
	if err != nil {
		return stats, err
	}
	_ = ownedByUs // currently unused; would drive Close() calls once the store interfaces expose them.

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
		blockchainStore: stores.Blockchain,
		utxoStore:       stores.UTXO,
		subtreeStore:    stores.Subtree,
		blockStore:      stores.Block,
		opts:            opts,
		stats:           stats,
		concurrency:     concurrency,
	}

	// Preflight: gates, target resolution, enumeration.
	preflightResult, err := env.preflight(ctx)
	if err != nil {
		return stats, err
	}

	if opts.DryRun {
		logger.Infof("--dry-run: would delete %d blocks above target height %d; stopping before mutation",
			len(preflightResult.deleteList), preflightResult.target)
		env.logStats(time.Since(start))
		return stats, nil
	}

	// Phase 0 — UTXO store internal height reset.
	if err = stores.UTXO.SetBlockHeight(preflightResult.target); err != nil {
		return stats, errors.NewStorageError("failed to reset UTXO store blockHeight", err)
	}

	logger.Infof("Phase 0 complete: UTXO store blockHeight set to %d", preflightResult.target)

	// Phase 1 — unmined + conflicting cleanup.
	if err = env.phase1Unmined(ctx, preflightResult); err != nil {
		return stats, errors.NewProcessingError("Phase 1 failed", err)
	}

	logger.Infof("Phase 1 complete: unmined_purged=%d conflicting_purged=%d",
		stats.UnminedPurged, stats.ConflictingPurged)

	// Phase 2 — block rewind.
	if err = env.phase2Blocks(ctx, preflightResult); err != nil {
		return stats, errors.NewProcessingError("Phase 2 failed", err)
	}

	logger.Infof("Phase 2 complete: blocks_deleted=%d txs_deleted=%d blockids_trimmed=%d subtrees_deleted=%d subtrees_skipped_shared=%d",
		stats.BlocksDeleted, stats.TxsDeleted, stats.TxsBlockIDsTrimmed,
		stats.SubtreesDeleted, stats.SubtreesSkippedShared)

	// Phase 3 — finalize.
	if err = env.phase3Finalize(ctx, preflightResult); err != nil {
		return stats, errors.NewProcessingError("Phase 3 failed", err)
	}

	if opts.Verify {
		if err = env.phase4Verify(ctx, preflightResult); err != nil {
			return stats, errors.NewProcessingError("Phase 4 verify failed", err)
		}
		logger.Infof("Phase 4 verify complete")
	}

	env.logStats(time.Since(start))
	return stats, nil
}

// resolveStores returns either the caller-supplied stores or freshly opened
// ones. When it opens stores, ownedByUs is true so callers know to close them
// (kept as a hint for future use).
func resolveStores(ctx context.Context, logger ulogger.Logger, s *settings.Settings, opts Options) (*Stores, bool, error) {
	if opts.Stores != nil {
		if opts.Stores.Blockchain == nil || opts.Stores.UTXO == nil || opts.Stores.Subtree == nil {
			return nil, false, errors.NewConfigurationError("Options.Stores must have all three stores set when supplied")
		}
		return opts.Stores, false, nil
	}

	blockchainStore, err := blockchain.NewStore(logger, s.BlockChain.StoreURL, s)
	if err != nil {
		return nil, false, errors.NewConfigurationError("failed to open blockchain store", err)
	}

	utxoStore, err := utxofactory.NewStore(ctx, logger, s, "rewindblockchain", false)
	if err != nil {
		return nil, false, errors.NewConfigurationError("failed to open utxo store", err)
	}

	subtreeStore, err := newSubtreeStore(logger, s)
	if err != nil {
		return nil, false, err
	}

	// Best-effort, like the delete it serves: a block store the tool cannot
	// open must not stop a rewind that does not otherwise need it.
	blockStore, err := newBlockStore(logger, s)
	if err != nil {
		logger.Warnf("could not open the block store, so the utxo-persister lastProcessed marker will not be deleted (delete <blockstore>/lastProcessed.dat by hand): %v", err)
		blockStore = nil
	}

	return &Stores{
		Blockchain: blockchainStore,
		UTXO:       utxoStore,
		Subtree:    subtreeStore,
		Block:      blockStore,
	}, true, nil
}

// newBlockStore opens the block blob store at the node's configured URL. The
// tool only uses it for the UTXO persister's lastProcessed marker, which is
// read and written with options.WithNoHashPrefix(), so the hashPrefix the node
// applies to block files does not affect it; the daemon's block-height tracker
// and deletion scheduler are not needed while the node is stopped.
func newBlockStore(logger ulogger.Logger, s *settings.Settings) (blob.Store, error) {
	if s.Block.BlockStore == nil {
		return nil, errors.NewConfigurationError("blockstore config not found")
	}

	blockStore, err := blob.NewStore(logger, s.Block.BlockStore)
	if err != nil {
		return nil, errors.NewConfigurationError("failed to open block store", err)
	}

	return blockStore, nil
}

// defaultSubtreeHashPrefix matches daemon.GetSubtreeStore's default
// (daemon/daemon_stores.go:454): a subtree store URL with no ?hashPrefix shards
// two characters deep. Every shipped subtreestore setting is exactly that — a
// bare file:// URL with no query — so this default is what real deployments run.
const defaultSubtreeHashPrefix = 2

// subtreeHashPrefix resolves the hash prefix for the subtree store, mirroring
// daemon.GetSubtreeStore: default 2, overridden by ?hashPrefix= on the URL.
func subtreeHashPrefix(subtreeStoreURL *url.URL) (int, error) {
	v := subtreeStoreURL.Query().Get("hashPrefix")
	if v == "" {
		return defaultSubtreeHashPrefix, nil
	}

	parsed, err := strconv.Atoi(v)
	if err != nil {
		return 0, errors.NewConfigurationError("subtreestore hashPrefix config error", err)
	}

	return parsed, nil
}

// newSubtreeStore opens the subtree blob store the same way the node does.
//
// The prefix has to be passed as a store option rather than left to the URL.
// stores/blob/factory.go parses only batch, logger, sizeInBytes and writeKeys.
// The file backend does additionally read ?hashPrefix / ?hashSuffix for itself
// (stores/blob/file/file.go:446-462) and applies them after the options, so on
// file:// a query parameter would win — but the s3 backend does not read the
// query at all (s3.go derives its path via CalculatePrefix from the store
// options alone), and no shipped subtreestore URL carries a query anyway. So
// with the default configuration the option is the only source of the prefix,
// and it is the only route that works across backends.
//
// Without it Options.HashPrefix stays 0, CalculatePrefix returns "", and every
// key resolves to a flat path while the node writes hash-sharded ones — so every
// read misses with NOT_FOUND and Phase 2 cannot rewind the deployment at all.
//
// Only WithHashPrefix is mirrored from the daemon: its other options drive
// DAH-managed lifecycle, and this tool wants raw Dels.
//
// Known parity gap, inherited from daemon.GetSubtreeStore: neither reads
// ?hashSuffix, which the file backend turns into a negative HashPrefix and
// which would therefore override what is passed here.
func newSubtreeStore(logger ulogger.Logger, s *settings.Settings) (blob.Store, error) {
	subtreeStoreURL := s.SubtreeValidation.SubtreeStore
	if subtreeStoreURL == nil {
		return nil, errors.NewConfigurationError("subtreestore URL is not configured")
	}

	hashPrefix, err := subtreeHashPrefix(subtreeStoreURL)
	if err != nil {
		return nil, err
	}

	subtreeStore, err := blob.NewStore(logger, subtreeStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewConfigurationError("failed to open subtree blob store", err)
	}

	return subtreeStore, nil
}

// env bundles the resolved stores and counters shared across phases.
type env struct {
	logger          ulogger.Logger
	settings        *settings.Settings
	blockchainStore blockchain.Store
	utxoStore       utxo.Store
	subtreeStore    blob.Store
	blockStore      blob.Store
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
