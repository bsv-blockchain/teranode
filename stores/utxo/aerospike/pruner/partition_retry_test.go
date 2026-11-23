package pruner

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ensurePrometheusMetrics initializes the package-level prometheus metrics
// so tests that bypass NewService() don't hit nil pointer dereferences.
func ensurePrometheusMetrics() {
	prometheusMetricsInitOnce.Do(func() {
		prometheusUtxoCleanupBatch = prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "test_utxo_cleanup_batch_duration_seconds",
		})
		prometheusUtxoRecordErrors = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_utxo_pruner_record_errors_total",
		})
		prometheusUtxoBatchQueryError = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_utxo_pruner_batch_query_errors_total",
		})
		prometheusUtxoRecordsDeleted = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_utxo_pruner_records_deleted_total",
		})
		prometheusUtxoRecordsDeletedSkipped = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_utxo_pruner_records_deleted_skipped_total",
		})
		prometheusUtxoParentsUpdated = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_utxo_pruner_parents_updated_total",
		})
		prometheusUtxoParentsUpdatedSkipped = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_utxo_pruner_parents_updated_skipped_total",
		})
		prometheusUtxoExternalFilesDeleted = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_utxo_pruner_external_files_deleted_total",
		})
		prometheusUtxoExternalFilesDeletedSkipped = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_utxo_pruner_external_files_deleted_skipped_total",
		})
		prometheusUtxoRetryAttempts = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_utxo_pruner_retry_attempts_total",
		})
		prometheusUtxoTimeoutEvents = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_utxo_pruner_timeout_events_total",
		})
	})
}

// mockPartitionWorkerResult holds a configured result for a partition range
type mockPartitionWorkerResult struct {
	processed int64
	skipped   int64
	err       error
}

// mockPartitionWorkerCall records a single call to the mock partition worker
type mockPartitionWorkerCall struct {
	partitionStart int
	partitionCount int
}

// mockPartitionWorker tracks calls and returns configured results per partition range
type mockPartitionWorker struct {
	mu      sync.Mutex
	calls   []mockPartitionWorkerCall
	results map[int][]mockPartitionWorkerResult // keyed by partitionStart, pops from front
}

func newMockPartitionWorker() *mockPartitionWorker {
	return &mockPartitionWorker{
		results: make(map[int][]mockPartitionWorkerResult),
	}
}

func (m *mockPartitionWorker) addResult(partitionStart int, r mockPartitionWorkerResult) {
	m.results[partitionStart] = append(m.results[partitionStart], r)
}

func (m *mockPartitionWorker) worker(_ context.Context, _ uint32, start, count int) (int64, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls = append(m.calls, mockPartitionWorkerCall{partitionStart: start, partitionCount: count})

	results, ok := m.results[start]
	if !ok || len(results) == 0 {
		panic(fmt.Sprintf("no mock result configured for partition start %d", start))
	}

	r := results[0]
	m.results[start] = results[1:]
	return r.processed, r.skipped, r.err
}

func (m *mockPartitionWorker) callCountForPartition(partitionStart int) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for _, c := range m.calls {
		if c.partitionStart == partitionStart {
			count++
		}
	}
	return count
}

func (m *mockPartitionWorker) totalCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockPartitionWorker) calledPartitions() []int {
	m.mu.Lock()
	defer m.mu.Unlock()

	seen := map[int]bool{}
	for _, c := range m.calls {
		seen[c.partitionStart] = true
	}

	result := make([]int, 0, len(seen))
	for p := range seen {
		result = append(result, p)
	}
	sort.Ints(result)
	return result
}

func createTestServiceWithMockWorker(t *testing.T, mock *mockPartitionWorker) *Service {
	t.Helper()
	ensurePrometheusMetrics()
	svc := &Service{
		logger:            ulogger.NewVerboseTestLogger(t),
		notifier:          NewPrunerEventNotifier(),
		partitionWorkerFn: mock.worker,
	}
	return svc
}

func TestPruneWithPartitions_AllSucceed(t *testing.T) {
	mock := newMockPartitionWorker()

	// 4 workers, each succeeds with 100 processed
	mock.addResult(0, mockPartitionWorkerResult{processed: 100})
	mock.addResult(1024, mockPartitionWorkerResult{processed: 100})
	mock.addResult(2048, mockPartitionWorkerResult{processed: 100})
	mock.addResult(3072, mockPartitionWorkerResult{processed: 100})

	svc := createTestServiceWithMockWorker(t, mock)
	total, err := svc.PruneWithPartitions(context.Background(), 1000, "abc123", 4)

	require.NoError(t, err)
	assert.Equal(t, int64(400), total)
	assert.Equal(t, 4, mock.totalCalls())
}

func TestPruneWithPartitions_OneWorkerTimeout_RetryOnlyFailed(t *testing.T) {
	mock := newMockPartitionWorker()

	// First attempt: 3 succeed, 1 times out with partial progress
	mock.addResult(0, mockPartitionWorkerResult{processed: 100})
	mock.addResult(1024, mockPartitionWorkerResult{processed: 100})
	mock.addResult(2048, mockPartitionWorkerResult{processed: 50, err: &TimeoutError{cause: errors.NewProcessingError("timeout")}})
	mock.addResult(3072, mockPartitionWorkerResult{processed: 100})

	// Second attempt: only the failed range retries and succeeds
	mock.addResult(2048, mockPartitionWorkerResult{processed: 80})

	svc := createTestServiceWithMockWorker(t, mock)
	total, err := svc.PruneWithPartitions(context.Background(), 1000, "abc123", 4)

	require.NoError(t, err)
	// 100 + 100 + 50 (partial from timeout) + 100 + 80 (retry success)
	assert.Equal(t, int64(430), total)
	// Workers at 0, 1024, 3072 called once; worker at 2048 called twice
	assert.Equal(t, 1, mock.callCountForPartition(0))
	assert.Equal(t, 1, mock.callCountForPartition(1024))
	assert.Equal(t, 2, mock.callCountForPartition(2048))
	assert.Equal(t, 1, mock.callCountForPartition(3072))
	assert.Equal(t, 5, mock.totalCalls())
}

func TestPruneWithPartitions_MultipleWorkersTimeout(t *testing.T) {
	mock := newMockPartitionWorker()

	// First attempt: 2 succeed, 2 timeout
	mock.addResult(0, mockPartitionWorkerResult{processed: 100})
	mock.addResult(1024, mockPartitionWorkerResult{processed: 30, err: &TimeoutError{cause: errors.NewProcessingError("timeout")}})
	mock.addResult(2048, mockPartitionWorkerResult{processed: 100})
	mock.addResult(3072, mockPartitionWorkerResult{processed: 20, err: &TimeoutError{cause: errors.NewProcessingError("timeout")}})

	// Second attempt: both failed ranges succeed
	mock.addResult(1024, mockPartitionWorkerResult{processed: 70})
	mock.addResult(3072, mockPartitionWorkerResult{processed: 80})

	svc := createTestServiceWithMockWorker(t, mock)
	total, err := svc.PruneWithPartitions(context.Background(), 1000, "abc123", 4)

	require.NoError(t, err)
	assert.Equal(t, int64(400), total) // 100+30+100+20+70+80
	assert.Equal(t, 1, mock.callCountForPartition(0))
	assert.Equal(t, 2, mock.callCountForPartition(1024))
	assert.Equal(t, 1, mock.callCountForPartition(2048))
	assert.Equal(t, 2, mock.callCountForPartition(3072))
}

func TestPruneWithPartitions_NonTimeoutError_ImmediateReturn(t *testing.T) {
	mock := newMockPartitionWorker()

	mock.addResult(0, mockPartitionWorkerResult{processed: 100})
	mock.addResult(1024, mockPartitionWorkerResult{processed: 50, err: errors.NewProcessingError("permanent failure")})
	mock.addResult(2048, mockPartitionWorkerResult{processed: 100})
	mock.addResult(3072, mockPartitionWorkerResult{processed: 100})

	svc := createTestServiceWithMockWorker(t, mock)
	_, err := svc.PruneWithPartitions(context.Background(), 1000, "abc123", 4)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "permanent failure")
	// No retries — only 4 total calls
	assert.Equal(t, 4, mock.totalCalls())
}

func TestPruneWithPartitions_MixedTimeoutAndNonTimeout(t *testing.T) {
	mock := newMockPartitionWorker()

	// One worker times out, another has a permanent error
	mock.addResult(0, mockPartitionWorkerResult{processed: 100})
	mock.addResult(1024, mockPartitionWorkerResult{err: &TimeoutError{cause: errors.NewProcessingError("timeout")}})
	mock.addResult(2048, mockPartitionWorkerResult{err: errors.NewProcessingError("disk failure")})
	mock.addResult(3072, mockPartitionWorkerResult{processed: 100})

	svc := createTestServiceWithMockWorker(t, mock)
	_, err := svc.PruneWithPartitions(context.Background(), 1000, "abc123", 4)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk failure")
	// Non-timeout error means no retry
	assert.Equal(t, 4, mock.totalCalls())
}

func TestPruneWithPartitions_MaxRetriesExceeded(t *testing.T) {
	mock := newMockPartitionWorker()

	// Worker at partition 0 always times out
	for i := 0; i < 10; i++ {
		mock.addResult(0, mockPartitionWorkerResult{processed: 10, err: &TimeoutError{cause: errors.NewProcessingError("timeout")}})
	}
	// Other workers succeed on first attempt
	mock.addResult(1024, mockPartitionWorkerResult{processed: 100})
	mock.addResult(2048, mockPartitionWorkerResult{processed: 100})
	mock.addResult(3072, mockPartitionWorkerResult{processed: 100})

	svc := createTestServiceWithMockWorker(t, mock)
	_, err := svc.PruneWithPartitions(context.Background(), 1000, "abc123", 4)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "max retries")
	// Partition 0 called 10 times, others called once
	assert.Equal(t, 10, mock.callCountForPartition(0))
	assert.Equal(t, 1, mock.callCountForPartition(1024))
	assert.Equal(t, 1, mock.callCountForPartition(2048))
	assert.Equal(t, 1, mock.callCountForPartition(3072))
}

func TestPruneWithPartitions_ProgressiveRecovery(t *testing.T) {
	mock := newMockPartitionWorker()

	// Attempt 1: 1 succeeds, 3 timeout
	mock.addResult(0, mockPartitionWorkerResult{processed: 100})
	mock.addResult(1024, mockPartitionWorkerResult{processed: 10, err: &TimeoutError{cause: errors.NewProcessingError("timeout")}})
	mock.addResult(2048, mockPartitionWorkerResult{processed: 20, err: &TimeoutError{cause: errors.NewProcessingError("timeout")}})
	mock.addResult(3072, mockPartitionWorkerResult{processed: 30, err: &TimeoutError{cause: errors.NewProcessingError("timeout")}})

	// Attempt 2: 2 of 3 succeed, 1 still times out
	mock.addResult(1024, mockPartitionWorkerResult{processed: 90})
	mock.addResult(2048, mockPartitionWorkerResult{processed: 80})
	mock.addResult(3072, mockPartitionWorkerResult{processed: 15, err: &TimeoutError{cause: errors.NewProcessingError("timeout")}})

	// Attempt 3: last worker succeeds
	mock.addResult(3072, mockPartitionWorkerResult{processed: 55})

	svc := createTestServiceWithMockWorker(t, mock)
	total, err := svc.PruneWithPartitions(context.Background(), 1000, "abc123", 4)

	require.NoError(t, err)
	// 100 + 10 + 20 + 30 + 90 + 80 + 15 + 55 = 400
	assert.Equal(t, int64(400), total)
	assert.Equal(t, 1, mock.callCountForPartition(0))
	assert.Equal(t, 2, mock.callCountForPartition(1024))
	assert.Equal(t, 2, mock.callCountForPartition(2048))
	assert.Equal(t, 3, mock.callCountForPartition(3072))
}

func TestPruneWithPartitions_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mock := newMockPartitionWorker()

	// First worker cancels the context, simulating external cancellation
	mock.addResult(0, mockPartitionWorkerResult{err: &TimeoutError{cause: errors.NewProcessingError("timeout")}})
	mock.addResult(1024, mockPartitionWorkerResult{processed: 100})
	mock.addResult(2048, mockPartitionWorkerResult{processed: 100})
	mock.addResult(3072, mockPartitionWorkerResult{processed: 100})

	// On retry, the context is cancelled so the worker returns ctx.Err()
	cancel()
	mock.addResult(0, mockPartitionWorkerResult{err: context.Canceled})

	svc := createTestServiceWithMockWorker(t, mock)
	_, err := svc.PruneWithPartitions(ctx, 1000, "abc123", 4)

	require.Error(t, err)
	// The context.Canceled error is not a TimeoutError, so it returns immediately
	assert.Equal(t, 5, mock.totalCalls()) // 4 initial + 1 retry that fails with non-timeout
}
