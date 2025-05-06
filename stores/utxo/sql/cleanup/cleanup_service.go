package cleanup

import (
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/cleanup"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/usql"
	"github.com/ordishs/gocore"
)

// Ensure Store implements the Cleanup Service interface
var _ cleanup.Service = (*Service)(nil)

const (
	// DefaultWorkerCount is the default number of worker goroutines
	DefaultWorkerCount = 2

	// DefaultMaxJobsHistory is the default number of jobs to keep in history
	DefaultMaxJobsHistory = 1000
)

// Service implements the utxo.CleanupService interface for SQL-based UTXO stores
type Service struct {
	logger     ulogger.Logger
	db         *usql.DB
	jobManager *cleanup.JobManager
	ctx        context.Context
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
func NewService(opts Options) (*Service, error) {
	if opts.Logger == nil {
		return nil, errors.NewProcessingError("logger is required")
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
		logger: opts.Logger,
		db:     opts.DB,
		ctx:    opts.Ctx,
	}

	// Create the job processor function
	jobProcessor := func(job *cleanup.Job, workerID int) {
		service.processCleanupJob(job, workerID)
	}

	// Create the job manager
	jobManager, err := cleanup.NewJobManager(cleanup.JobManagerOptions{
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

	return s.jobManager.TriggerCleanup(blockHeight, doneCh...)
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (s *Service) GetJobs() []*cleanup.Job {
	return s.jobManager.GetJobs()
}

// processCleanupJob executes the cleanup for a specific job
func (s *Service) processCleanupJob(job *cleanup.Job, workerID int) {
	s.logger.Debugf("[SQLCleanupService %d] running cleanup job for block height %d", workerID, job.BlockHeight)

	job.Started = time.Now()

	// Execute the cleanup
	err := deleteTombstoned(s.db, job.BlockHeight)

	if err != nil {
		job.SetStatus(cleanup.JobStatusFailed)
		job.Error = err
		job.Ended = time.Now()

		s.logger.Errorf("[SQLCleanupService %d] cleanup job failed for block height %d: %v", workerID, job.BlockHeight, err)

		if job.DoneCh != nil {
			job.DoneCh <- cleanup.JobStatusFailed.String()
			close(job.DoneCh)
		}
	} else {
		job.SetStatus(cleanup.JobStatusCompleted)
		job.Ended = time.Now()

		s.logger.Debugf("[SQLCleanupService %d] cleanup job completed for block height %d in %v",
			workerID, job.BlockHeight, time.Since(job.Started))

		if job.DoneCh != nil {
			job.DoneCh <- cleanup.JobStatusCompleted.String()
			close(job.DoneCh)
		}
	}
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
