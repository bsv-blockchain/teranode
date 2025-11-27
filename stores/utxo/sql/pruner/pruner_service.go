package pruner

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/ordishs/gocore"
)

// Ensure Store implements the Pruner Service interface
var _ pruner.Service = (*Service)(nil)

const (
	// DefaultWorkerCount is the default number of worker goroutines
	DefaultWorkerCount = 2

	// DefaultMaxJobsHistory is the default number of jobs to keep in history
	DefaultMaxJobsHistory = 1000
)

// Service implements the utxo.CleanupService interface for SQL-based UTXO stores
type Service struct {
	logger             ulogger.Logger
	settings           *settings.Settings
	db                 *usql.DB
	jobManager         *pruner.JobManager
	ctx                context.Context
	getPersistedHeight func() uint32
}

// Options contains configuration options for the cleanup service
type Options struct {
	// Logger is the logger to use
	Logger ulogger.Logger

	// DB is the SQL database connection
	DB *usql.DB

	// WorkerCount is the number of worker goroutines to use
	WorkerCount int

	// MaxJobsHistory is the maximum number of jobs to keep in history
	MaxJobsHistory int

	// Ctx is the context to use to signal shutdown
	Ctx context.Context
}

// NewService creates a new cleanup service for the SQL store
func NewService(tSettings *settings.Settings, opts Options) (*Service, error) {
	if opts.Logger == nil {
		return nil, errors.NewProcessingError("logger is required")
	}

	if tSettings == nil {
		return nil, errors.NewProcessingError("settings is required")
	}

	if opts.DB == nil {
		return nil, errors.NewProcessingError("db is required")
	}

	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount, _ = gocore.Config().GetInt("sql_cleanup_worker_count", DefaultWorkerCount)
	}

	maxJobsHistory := opts.MaxJobsHistory
	if maxJobsHistory <= 0 {
		maxJobsHistory = DefaultMaxJobsHistory
	}

	service := &Service{
		logger:   opts.Logger,
		settings: tSettings,
		db:       opts.DB,
		ctx:      opts.Ctx,
	}

	// Create the job processor function
	jobProcessor := func(job *pruner.Job, workerID int) {
		service.processCleanupJob(job, workerID)
	}

	// Create the job manager
	jobManager, err := pruner.NewJobManager(pruner.JobManagerOptions{
		Logger:         opts.Logger,
		WorkerCount:    workerCount,
		MaxJobsHistory: maxJobsHistory,
		JobProcessor:   jobProcessor,
	})
	if err != nil {
		return nil, err
	}

	service.jobManager = jobManager

	return service, nil
}

// Start starts the cleanup service
func (s *Service) Start(ctx context.Context) {
	s.logger.Infof("[SQLCleanupService] starting cleanup service")
	s.jobManager.Start(ctx)
}

// UpdateBlockHeight updates the current block height and triggers cleanup if needed
func (s *Service) UpdateBlockHeight(blockHeight uint32, doneCh ...chan string) error {
	if blockHeight == 0 {
		return errors.NewProcessingError("Cannot update block height to 0")
	}

	return s.jobManager.TriggerPruner(blockHeight, doneCh...)
}

// SetPersistedHeightGetter sets the function used to get block persister progress.
// This allows cleanup to coordinate with block persister to avoid premature deletion.
func (s *Service) SetPersistedHeightGetter(getter func() uint32) {
	s.getPersistedHeight = getter
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (s *Service) GetJobs() []*pruner.Job {
	return s.jobManager.GetJobs()
}

// processCleanupJob executes the cleanup for a specific job
func (s *Service) processCleanupJob(job *pruner.Job, workerID int) {
	s.logger.Debugf("[SQLCleanupService %d] running cleanup job for block height %d", workerID, job.BlockHeight)

	job.Started = time.Now()

	// BLOCK PERSISTER COORDINATION: Calculate safe cleanup height
	//
	// PROBLEM: Block persister creates .subtree_data files after a delay (BlockPersisterPersistAge blocks).
	// If we delete transactions before block persister creates these files, catchup will fail with
	// "subtree length does not match tx data length" (actually missing transactions).
	//
	// SOLUTION: Limit cleanup to transactions that block persister has already processed:
	//   safe_height = min(requested_cleanup_height, persisted_height + retention)
	//
	// EXAMPLE with retention=288, persisted=100, requested=200:
	//   - Block persister has processed blocks up to height 100
	//   - Those blocks' transactions are in .subtree_data files (safe to delete after retention)
	//   - Safe deletion height = 100 + 288 = 388... but wait, we want to clean height 200
	//   - Since 200 < 388, we can safely proceed with cleaning up to 200
	//
	// EXAMPLE where cleanup would be limited (persisted=50, requested=200, retention=100):
	//   - Block persister only processed up to height 50
	//   - Safe deletion = 50 + 100 = 150
	//   - Requested cleanup of 200 is LIMITED to 150 to protect unpersisted blocks 51-200
	//
	// HEIGHT=0 SPECIAL CASE: If persistedHeight=0, block persister isn't running or hasn't
	// processed any blocks yet. Proceed with normal cleanup without coordination.
	safeCleanupHeight := job.BlockHeight

	if s.getPersistedHeight != nil {
		persistedHeight := s.getPersistedHeight()

		// Only apply limitation if block persister has actually processed blocks (height > 0)
		if persistedHeight > 0 {
			retention := s.settings.GetUtxoStoreBlockHeightRetention()

			// Calculate max safe height: persisted_height + retention
			// Block persister at height N means blocks 0 to N are persisted in .subtree_data files.
			// Those transactions can be safely deleted after retention blocks.
			maxSafeHeight := persistedHeight + retention
			if maxSafeHeight < safeCleanupHeight {
				s.logger.Infof("[SQLCleanupService %d] Limiting cleanup from height %d to %d (persisted: %d, retention: %d)",
					workerID, job.BlockHeight, maxSafeHeight, persistedHeight, retention)
				safeCleanupHeight = maxSafeHeight
			}
		}
	}

	// Log start of cleanup
	s.logger.Infof("[SQLCleanupService %d] starting cleanup scan for height %d (delete_at_height <= %d)",
		workerID, job.BlockHeight, safeCleanupHeight)

	// Execute the cleanup with safe height
	deletedCount, err := deleteTombstonedWithCount(s.db, safeCleanupHeight)

	if err != nil {
		job.SetStatus(pruner.JobStatusFailed)
		job.Error = err
		job.Ended = time.Now()

		s.logger.Errorf("[SQLCleanupService %d] cleanup job failed for block height %d: %v", workerID, job.BlockHeight, err)
	} else {
		job.SetStatus(pruner.JobStatusCompleted)
		job.Ended = time.Now()

		s.logger.Infof("[SQLCleanupService %d] cleanup job completed for block height %d in %v - deleted %d records",
			workerID, job.BlockHeight, time.Since(job.Started), deletedCount)
	}
}

// deleteTombstonedWithCount removes transactions that have passed their expiration time and returns the count.
func deleteTombstonedWithCount(db *usql.DB, blockHeight uint32) (int64, error) {
	// Delete transactions that have passed their expiration time
	// this will cascade to inputs, outputs, block_ids and conflicting_children
	result, err := db.Exec("DELETE FROM transactions WHERE delete_at_height <= $1", blockHeight)
	if err != nil {
		return 0, errors.NewStorageError("failed to delete transactions", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, errors.NewStorageError("failed to get rows affected", err)
	}

	return count, nil
}

// deleteTombstoned removes transactions that have passed their expiration time.
func deleteTombstoned(db *usql.DB, blockHeight uint32) error {
	// Delete transactions that have passed their expiration time
	// this will cascade to inputs, outputs, block_ids and conflicting_children
	if _, err := db.Exec("DELETE FROM transactions WHERE delete_at_height <= $1", blockHeight); err != nil {
		return errors.NewStorageError("failed to delete transactions", err)
	}

	return nil
}
