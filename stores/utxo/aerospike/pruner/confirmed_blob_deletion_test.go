package pruner

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// blobDeletionFixture builds a Service whose batch is answered by answer, and three records that
// each have an external blob in a memory store.
type blobDeletionFixture struct {
	service *Service
	store   *memory.Memory
	keys    []*aerospike.Key
	files   []*externalFileInfo
}

func newBlobDeletionFixture(t *testing.T, skipDeletions bool, answer func(records []aerospike.BatchRecordIfc)) *blobDeletionFixture {
	t.Helper()
	ensurePrometheusMetrics()

	store := memory.New()
	fixture := &blobDeletionFixture{store: store}

	for i := byte(1); i <= 3; i++ {
		hash := chainhash.Hash{i}

		key, err := aerospike.NewKey("test", "test", hash[:])
		require.NoError(t, err)

		require.NoError(t, store.Set(context.Background(), hash[:], fileformat.FileTypeTx, []byte{i}))

		fixture.keys = append(fixture.keys, key)
		fixture.files = append(fixture.files, &externalFileInfo{txHash: &hash, fileType: fileformat.FileTypeTx, masterKey: key})
	}

	fixture.service = &Service{
		logger:   ulogger.NewErrorTestLogger(t),
		settings: &settings.Settings{Pruner: settings.PrunerSettings{SkipDeletions: skipDeletions}},
		external: store,
		batchOperateFn: func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
			answer(records)

			return nil
		},
	}

	return fixture
}

func (f *blobDeletionFixture) blobExists(t *testing.T, i int) bool {
	t.Helper()

	exists, err := f.store.Exists(context.Background(), f.files[i].txHash[:], fileformat.FileTypeTx)
	require.NoError(t, err)

	return exists
}

// TestFlushCleanupBatches_DeletesTheBlobOfEveryConfirmedRecordOnly: a blob goes only after its own
// record's delete is confirmed, and the confirmed records' blobs go even when another delete in the
// batch failed. A record that is gone is never found again by the deleteAtHeight scan, so a blob
// kept back because a neighbour failed would stay for good.
func TestFlushCleanupBatches_DeletesTheBlobOfEveryConfirmedRecordOnly(t *testing.T) {
	fixture := newBlobDeletionFixture(t, false, func(records []aerospike.BatchRecordIfc) {
		// record 0: deleted. record 1: already gone. record 2: the delete failed.
		records[1].BatchRec().Err = aerospike.ErrKeyNotFound
		records[2].BatchRec().Err = aerospike.ErrTimeout
	})

	err := fixture.service.flushCleanupBatches(context.Background(), fixture.keys, fixture.files)
	require.Error(t, err, "a failed record delete is still reported")

	require.False(t, fixture.blobExists(t, 0), "a deleted record's blob goes")
	require.False(t, fixture.blobExists(t, 1), "an already-gone record's blob goes")
	require.True(t, fixture.blobExists(t, 2), "the blob of a record whose delete failed stays")
}

func TestFlushCleanupBatches_KeepsEveryBlobWhenTheBatchFails(t *testing.T) {
	fixture := newBlobDeletionFixture(t, false, func([]aerospike.BatchRecordIfc) {})
	fixture.service.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) error {
		return aerospike.ErrTimeout
	}

	require.Error(t, fixture.service.flushCleanupBatches(context.Background(), fixture.keys, fixture.files))

	for i := range fixture.files {
		require.True(t, fixture.blobExists(t, i), "no delete was confirmed, so blob %d stays", i)
	}
}

// TestFlushCleanupBatches_SkipDeletionsKeepsTheBlobs: with pruner_skipDeletions on the records stay,
// and a blob deleted under a record that stays would leave a transaction that can never be read back.
func TestFlushCleanupBatches_SkipDeletionsKeepsTheBlobs(t *testing.T) {
	called := false

	fixture := newBlobDeletionFixture(t, true, func([]aerospike.BatchRecordIfc) { called = true })

	require.NoError(t, fixture.service.flushCleanupBatches(context.Background(), fixture.keys, fixture.files))
	require.False(t, called, "no record delete is sent")

	for i := range fixture.files {
		require.True(t, fixture.blobExists(t, i), "blob %d stays", i)
	}
}
