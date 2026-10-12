package subtreevalidation

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// flakyBatchDecorateStore fails the first `failures` BatchDecorate calls with
// `err`, then succeeds and populates every item. Only BatchDecorate is
// exercised by processTxMetaUsingStore's batched path, so the embedded nil
// Store is never dereferenced.
type flakyBatchDecorateStore struct {
	utxo.Store

	mu       sync.Mutex
	calls    int
	failures int
	err      error
	onCall   func(attempt int)
}

func (s *flakyBatchDecorateStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	s.mu.Lock()
	s.calls++
	attempt := s.calls
	onCall := s.onCall
	s.mu.Unlock()

	if onCall != nil {
		onCall(attempt)
	}

	if attempt <= s.failures {
		return s.err
	}

	for _, item := range items {
		item.Data = &meta.Data{Fee: 1, SizeInBytes: 191}
		item.Err = nil
	}

	return nil
}

func (s *flakyBatchDecorateStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

// newRetryTestServer builds the minimal Server needed to drive
// processTxMetaUsingStore's batched path against a single batch, so
// BatchDecorate call counts are deterministic.
func newRetryTestServer(t *testing.T, store utxo.Store, logger ulogger.Logger, retries int) *Server {
	t.Helper()

	tSettings := settings.NewSettings()
	tSettings.BlockValidation.ProcessTxMetaUsingStoreBatchSize = 1024
	tSettings.BlockValidation.ProcessTxMetaUsingStoreConcurrency = 1
	tSettings.BlockValidation.ProcessTxMetaUsingStoreMissingTxThreshold = 0
	tSettings.BlockValidation.ProcessTxMetaUsingStoreRetries = retries

	return &Server{
		logger:    logger,
		utxoStore: store,
		settings:  tSettings,
	}
}

func retryTestHashes(n int) ([]chainhash.Hash, []metaSliceItem) {
	hashes := make([]chainhash.Hash, n)
	for i := range hashes {
		hashes[i][0] = byte(i + 1)
	}

	return hashes, make([]metaSliceItem, n)
}

// A transient store failure must not be fatal. This is the scale-2 block-307
// wedge: one Aerospike client timeout inside a single batch rejected an
// otherwise sound block, with no retry anywhere in the chain.
func TestProcessTxMetaUsingStore_RetriesTransientStorageError(t *testing.T) {
	store := &flakyBatchDecorateStore{
		failures: 2,
		err:      errors.NewStorageError("error in aerospike map store batch records", errors.NewError("ResultCode: TIMEOUT")),
	}
	logger := newCapturingLogger()
	server := newRetryTestServer(t, store, logger, 3)

	hashes, metaSlice := retryTestHashes(4)

	missed, err := server.processTxMetaUsingStore(context.Background(), hashes, metaSlice, map[uint32]bool{}, true, false, false)
	require.NoError(t, err, "a transient storage error must be retried, not turned into a block-validation verdict")
	require.Equal(t, 0, missed)
	require.Equal(t, 3, store.callCount(), "expected 2 failed attempts followed by a successful one")

	for i := range metaSlice {
		require.True(t, metaSlice[i].isSet, "metadata at index %d should be populated after the successful retry", i)
	}

	// The operator has to be able to see that the store is misbehaving.
	warnings := logger.warnings()
	require.NotEmpty(t, warnings, "retried storage failures must be logged")
	require.Contains(t, warnings, "batch decorate", "expected a warning naming the failing operation, got: %s", warnings)
	require.Contains(t, warnings, "TIMEOUT", "expected the underlying store error in the warning, got: %s", warnings)
}

// Retries are bounded: a store that never recovers still fails the call, and
// the returned error must still carry the underlying storage cause.
func TestProcessTxMetaUsingStore_GivesUpAfterRetryLimit(t *testing.T) {
	store := &flakyBatchDecorateStore{
		failures: 1 << 30,
		err:      errors.NewStorageError("error in aerospike map store batch records", errors.NewError("ResultCode: TIMEOUT")),
	}
	server := newRetryTestServer(t, store, newCapturingLogger(), 2)

	hashes, metaSlice := retryTestHashes(4)

	_, err := server.processTxMetaUsingStore(context.Background(), hashes, metaSlice, map[uint32]bool{}, true, false, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "the storage cause must survive wrapping, got: %v", err)
	require.Equal(t, 3, store.callCount(), "expected the initial attempt plus 2 retries")
}

// A non-retryable error must fail fast — retrying it only burns the block
// validation deadline.
func TestProcessTxMetaUsingStore_DoesNotRetryNonRetryableError(t *testing.T) {
	store := &flakyBatchDecorateStore{
		failures: 1 << 30,
		err:      errors.NewInvalidArgumentError("malformed request"),
	}
	server := newRetryTestServer(t, store, newCapturingLogger(), 3)

	hashes, metaSlice := retryTestHashes(4)

	_, err := server.processTxMetaUsingStore(context.Background(), hashes, metaSlice, map[uint32]bool{}, true, false, false)
	require.Error(t, err)
	require.Equal(t, 1, store.callCount(), "a non-retryable error must not be retried")
}

// A cancelled context must stop the retry loop immediately rather than sitting
// through the full backoff schedule.
func TestProcessTxMetaUsingStore_StopsRetryingOnContextCancel(t *testing.T) {
	store := &flakyBatchDecorateStore{
		failures: 1 << 30,
		err:      errors.NewStorageError("error in aerospike map store batch records", errors.NewError("ResultCode: TIMEOUT")),
	}
	server := newRetryTestServer(t, store, newCapturingLogger(), 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel from inside the second attempt, so the retry loop is guaranteed to
	// be mid-flight rather than racing a wall-clock sleep.
	store.onCall = func(attempt int) {
		if attempt == 2 {
			cancel()
		}
	}

	hashes, metaSlice := retryTestHashes(4)

	_, err := server.processTxMetaUsingStore(ctx, hashes, metaSlice, map[uint32]bool{}, true, false, false)
	require.Error(t, err)
	require.Less(t, store.callCount(), 100, "retries must stop once the context is cancelled")
}

// A misconfigured retry count must stay bounded. retry.Retry uses -1 as its
// "retry forever" sentinel, so passing the setting through unclamped would turn
// an operator typo into block validation hammering a dead store until the
// context is cancelled — with every batch of the block doing it at once.
func TestProcessTxMetaUsingStore_NegativeRetryCountIsBounded(t *testing.T) {
	for _, retries := range []int{-1, -5} {
		store := &flakyBatchDecorateStore{
			failures: 1 << 30,
			err:      errors.NewStorageError("error in aerospike map store batch records", errors.NewError("ResultCode: TIMEOUT")),
		}
		server := newRetryTestServer(t, store, newCapturingLogger(), retries)

		hashes, metaSlice := retryTestHashes(4)

		_, err := server.processTxMetaUsingStore(context.Background(), hashes, metaSlice, map[uint32]bool{}, true, false, false)
		require.Error(t, err)
		require.Equal(t, 1, store.callCount(), "a negative retry count (%d) must clamp to no retries, not infinite ones", retries)
	}
}

// Zero is documented as "previous behaviour": one attempt, no retry.
func TestProcessTxMetaUsingStore_ZeroRetriesMakesOneAttempt(t *testing.T) {
	store := &flakyBatchDecorateStore{
		failures: 1 << 30,
		err:      errors.NewStorageError("error in aerospike map store batch records", errors.NewError("ResultCode: TIMEOUT")),
	}
	server := newRetryTestServer(t, store, newCapturingLogger(), 0)

	hashes, metaSlice := retryTestHashes(4)

	_, err := server.processTxMetaUsingStore(context.Background(), hashes, metaSlice, map[uint32]bool{}, true, false, false)
	require.Error(t, err)
	require.Equal(t, 1, store.callCount(), "retries=0 must make exactly one attempt")
}

// A cancelled context is a shutdown, not a storage fault. Reporting the stale
// store error from the abandoned attempt would have blockvalidation classify the
// Kafka message as a recoverable storage failure and redeliver the block.
func TestProcessTxMetaUsingStore_CancelledContextIsNotAStorageError(t *testing.T) {
	store := &flakyBatchDecorateStore{
		failures: 1 << 30,
		err:      errors.NewStorageError("error in aerospike map store batch records", errors.NewError("ResultCode: TIMEOUT")),
	}
	server := newRetryTestServer(t, store, newCapturingLogger(), 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store.onCall = func(attempt int) {
		if attempt == 2 {
			cancel()
		}
	}

	hashes, metaSlice := retryTestHashes(4)

	_, err := server.processTxMetaUsingStore(ctx, hashes, metaSlice, map[uint32]bool{}, true, false, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrContextCanceled), "a cancelled context must surface as a context error, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrStorageError), "a cancelled context must not be reported as a storage fault, got: %v", err)
}

// flakyGetMetaStore fails the first `failures` GetMeta calls, then succeeds.
// The unbatched path is only reached when
// subtreevalidation_batch_missing_transactions is turned off.
type flakyGetMetaStore struct {
	utxo.Store

	mu       sync.Mutex
	calls    int
	failures int
	err      error
}

func (s *flakyGetMetaStore) GetMeta(_ context.Context, _ *chainhash.Hash, data *meta.Data) error {
	s.mu.Lock()
	s.calls++
	attempt := s.calls
	s.mu.Unlock()

	if attempt <= s.failures {
		return s.err
	}

	*data = meta.Data{Fee: 1, SizeInBytes: 191}

	return nil
}

func (s *flakyGetMetaStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

// The unbatched path must not turn a transient store failure into a
// block-validation verdict either: it is reachable by configuration, so leaving
// it unretried leaves the block-307 wedge one setting away.
func TestProcessTxMetaUsingStore_UnbatchedRetriesTransientStorageError(t *testing.T) {
	store := &flakyGetMetaStore{
		failures: 2,
		err:      errors.NewStorageError("error in aerospike map store batch records", errors.NewError("ResultCode: TIMEOUT")),
	}
	server := newRetryTestServer(t, store, newCapturingLogger(), 3)

	hashes, metaSlice := retryTestHashes(1)

	missed, err := server.processTxMetaUsingStore(context.Background(), hashes, metaSlice, map[uint32]bool{}, false, false, false)
	require.NoError(t, err, "a transient storage error must be retried on the unbatched path too")
	require.Equal(t, 0, missed)
	require.True(t, metaSlice[0].isSet)
	require.Equal(t, 3, store.callCount(), "expected 2 failed attempts followed by a successful one")
}
