package batcher

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// getBatchFiles gets the most recent batch files from the store
func getBatchFiles(t *testing.T, store *memory.Memory) ([]byte, []byte) {
	t.Helper()

	// Get all keys
	keys := store.ListKeys()
	if len(keys) == 0 {
		t.Fatal("no keys found in store")
	}

	// Get the data
	data, err := store.Get(context.Background(), keys[0], fileformat.FileTypeBatchData)
	if err != nil {
		t.Fatalf("failed to get batch data: %v", err)
	}

	// Get the keys
	keysData, err := store.Get(context.Background(), keys[0], fileformat.FileTypeBatchKeys)
	if err != nil {
		t.Fatalf("failed to get batch keys: %v", err)
	}

	return data, keysData
}

// waitForBatchProcessing waits for batch processing to complete by checking the store
func waitForBatchProcessing(t *testing.T, store *memory.Memory, maxWait time.Duration) {
	t.Helper()

	deadline := time.Now().Add(maxWait)

	for time.Now().Before(deadline) {
		if len(store.ListKeys()) > 0 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("timeout waiting for batch processing")
}

func TestBatcher_New(t *testing.T) {
	store := memory.New()
	batcher := New(ulogger.TestLogger{}, store, 1024, true)

	if batcher == nil {
		t.Fatal("expected non-nil batcher")
	}
}

func TestBatcher_Set(t *testing.T) {
	store := memory.New()
	batcher := New(ulogger.TestLogger{}, store, 10, true)

	// Create test data
	hash := chainhash.Hash{}
	value := []byte("test data")

	err := batcher.Set(context.Background(), hash[:], fileformat.FileTypeUtxoSet, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batcher.Close(context.Background())
	waitForBatchProcessing(t, store, 2*time.Second)

	batchData, keysData := getBatchFiles(t, store)

	// Verify batch contains the value
	if !bytes.Contains(batchData, value) {
		t.Error("batch data doesn't contain the value")
	}

	// Verify keys file format
	keysLines := strings.Split(string(keysData), "\n")
	if len(keysLines) < 1 {
		t.Fatal("expected at least 1 key entry")
	}

	// Parse and verify key entry
	keyData, err := hex.DecodeString(keysLines[0])
	if err != nil {
		t.Fatalf("failed to decode key entry: %v", err)
	}

	if !bytes.Equal(keyData[:32], hash[:]) {
		t.Error("key entry doesn't match hash")
	}
}

func TestBatcher_SetFromReader(t *testing.T) {
	store := memory.New()
	batcher := New(ulogger.TestLogger{}, store, 10, true)

	hash := chainhash.Hash{}
	testData := []byte("test data")
	reader := io.NopCloser(bytes.NewReader(testData))

	err := batcher.SetFromReader(context.Background(), hash[:], fileformat.FileTypeUtxoSet, reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batcher.Close(context.Background())
	waitForBatchProcessing(t, store, 2*time.Second)

	batchData, keysData := getBatchFiles(t, store)

	// Verify batch contains the test data
	if !bytes.Contains(batchData, testData) {
		t.Error("batch data doesn't contain the test data")
	}

	// Verify keys file format
	keysLines := strings.Split(string(keysData), "\n")
	if len(keysLines) < 1 {
		t.Fatal("expected at least 1 key entry")
	}

	// Parse and verify key entry
	keyData, err := hex.DecodeString(keysLines[0])
	if err != nil {
		t.Fatalf("failed to decode key entry: %v", err)
	}

	if !bytes.Equal(keyData[:32], hash[:]) {
		t.Error("key entry doesn't match hash")
	}
}

func TestBatcher_Health(t *testing.T) {
	store := memory.New()
	batcher := New(ulogger.TestLogger{}, store, 1024, true)

	// Test health check
	status, _, err := batcher.Health(context.Background(), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if status != 200 {
		t.Errorf("expected status 200, got %d", status)
	}
}

func TestBatcher_BatchSizeLimit(t *testing.T) {
	store := memory.New()
	batchSize := 10
	batcher := New(ulogger.TestLogger{}, store, batchSize, true)

	hash := chainhash.Hash{}
	value := make([]byte, batchSize+5)
	binary.BigEndian.PutUint32(value, uint32(1234))

	err := batcher.Set(context.Background(), hash[:], fileformat.FileTypeUtxoSet, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batcher.Close(context.Background())
	waitForBatchProcessing(t, store, 2*time.Second)

	batchData, keysData := getBatchFiles(t, store)

	// Verify batch contains the large value
	if !bytes.Equal(batchData, value) {
		t.Error("batch data doesn't match large value")
	}

	// Verify keys file format
	keysLines := strings.Split(string(keysData), "\n")
	if len(keysLines) < 1 {
		t.Fatal("expected at least 1 key entry")
	}

	// Parse and verify key entry
	keyData, err := hex.DecodeString(keysLines[0])
	if err != nil {
		t.Fatalf("failed to decode key entry: %v", err)
	}

	if !bytes.Equal(keyData[:32], hash[:]) {
		t.Error("key entry doesn't match hash")
	}
}

func TestBatcher_BatchingBehavior(t *testing.T) {
	store := memory.New()
	batchSize := 100
	batcher := New(ulogger.TestLogger{}, store, batchSize, true)

	// Create test data smaller than batch size
	hash1 := chainhash.Hash{}
	value1 := []byte("test data 1")
	hash2 := chainhash.Hash{}
	hash2[0] = 1
	value2 := []byte("test data 2")

	// Add data to batcher
	err := batcher.Set(context.Background(), hash1[:], fileformat.FileTypeUtxoSet, value1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = batcher.Set(context.Background(), hash2[:], fileformat.FileTypeUtxoSet, value2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batcher.Close(context.Background())
	waitForBatchProcessing(t, store, 2*time.Second)

	batchData, keysData := getBatchFiles(t, store)

	// Verify batch contains both values
	if !bytes.Contains(batchData, value1) {
		t.Error("batch data doesn't contain value1")
	}

	if !bytes.Contains(batchData, value2) {
		t.Error("batch data doesn't contain value2")
	}

	// Verify keys file format
	keysLines := strings.Split(string(keysData), "\n")
	if len(keysLines) < 2 {
		t.Fatal("expected at least 2 key entries")
	}

	// Parse and verify first key entry
	key1Data, err := hex.DecodeString(keysLines[0])
	if err != nil {
		t.Fatalf("failed to decode key entry: %v", err)
	}

	if !bytes.Equal(key1Data[:32], hash1[:]) {
		t.Error("first key entry doesn't match hash1")
	}
}

func TestBatcher_UnsupportedOperations(t *testing.T) {
	store := memory.New()
	batcher := New(ulogger.TestLogger{}, store, 1024, true)

	t.Run("Get", func(t *testing.T) {
		_, err := batcher.Get(context.Background(), []byte("key"), fileformat.FileTypeUtxoSet)
		if err == nil {
			t.Error("expected error for unsupported Get operation")
		}
	})

	t.Run("Exists", func(t *testing.T) {
		_, err := batcher.Exists(context.Background(), []byte("key"), fileformat.FileTypeUtxoSet)
		if err == nil {
			t.Error("expected error for unsupported Exists operation")
		}
	})

	t.Run("Del", func(t *testing.T) {
		err := batcher.Del(context.Background(), []byte("key"), fileformat.FileTypeUtxoSet)
		if err == nil {
			t.Error("expected error for unsupported Del operation")
		}
	})

	t.Run("SetDAH", func(t *testing.T) {
		err := batcher.SetDAH(context.Background(), []byte("key"), fileformat.FileTypeUtxoSet, 1)
		if err == nil {
			t.Error("expected error for unsupported SetDAH operation")
		}
	})

	t.Run("GetDAH removed", func(t *testing.T) {
		// GetDAH has been removed from the blob.Store interface
		// DAH functionality is now centralized in the pruner service
		t.Skip("GetDAH removed from interface - see e2e pruner tests")
	})

	t.Run("GetIoReader", func(t *testing.T) {
		_, err := batcher.GetIoReader(context.Background(), []byte("key"), fileformat.FileTypeUtxoSet)
		if err == nil {
			t.Error("expected error for unsupported GetIoReader operation")
		}
	})
}

// failFirstThenSucceedStore is a blobStoreSetter fake whose Set call fails exactly
// once (the first call) and succeeds on every call after that. It records every
// value written on a successful batch-data Set so tests can assert nothing was
// silently dropped by a failed overflow flush.
type failFirstThenSucceedStore struct {
	mu      sync.Mutex
	calls   int
	written [][]byte
}

func (f *failFirstThenSucceedStore) Health(_ context.Context, _ bool) (int, string, error) {
	return 200, "ok", nil
}

func (f *failFirstThenSucceedStore) Set(_ context.Context, _ []byte, fileType fileformat.FileType, value []byte, _ ...options.FileOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	if f.calls == 1 {
		return errors.NewStorageError("simulated transient backend failure")
	}

	if fileType == fileformat.FileTypeBatchData {
		cp := make([]byte, len(value))
		copy(cp, value)
		f.written = append(f.written, cp)
	}

	return nil
}

func (f *failFirstThenSucceedStore) concatWritten() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	var all []byte
	for _, w := range f.written {
		all = append(all, w...)
	}

	return all
}

// TestBatcher_OverflowFlushFailure_ItemNotDropped pins defect 1: processBatchItem
// used to return before appending the item that triggered a failed overflow
// flush, silently discarding it forever (the queue has no re-enqueue). value2
// below is that triggering item. writeKeys is disabled so writeBatch only makes
// one Set call per flush, keeping the fake's call-count-based failure
// deterministic.
func TestBatcher_OverflowFlushFailure_ItemNotDropped(t *testing.T) {
	store := &failFirstThenSucceedStore{}
	batcher := New(ulogger.TestLogger{}, store, 10, false)

	value1 := []byte("AAAAAAAA") // 8 bytes
	value2 := []byte("BBBBBBBB") // 8 bytes: triggers the (failing) first overflow flush
	value3 := []byte("CCCCCCCC") // 8 bytes: triggers the (succeeding) second overflow flush

	var hash1, hash2, hash3 chainhash.Hash
	hash1[0] = 1
	hash2[0] = 2
	hash3[0] = 3

	require.NoError(t, batcher.Set(context.Background(), hash1[:], fileformat.FileTypeUtxoSet, value1))
	require.NoError(t, batcher.Set(context.Background(), hash2[:], fileformat.FileTypeUtxoSet, value2))
	require.NoError(t, batcher.Set(context.Background(), hash3[:], fileformat.FileTypeUtxoSet, value3))
	require.NoError(t, batcher.Close(context.Background()))

	all := store.concatWritten()

	require.Contains(t, string(all), string(value1), "value1 should have been flushed")
	require.Contains(t, string(all), string(value2), "value2 was the item that triggered the failed flush and must not be dropped")
	require.Contains(t, string(all), string(value3), "value3 should have been flushed on close")
}

// TestBatcher_KeyOffset_MatchesActualPosition pins defect 2: currentPos used to be
// captured before the overflow flush reset the batch, so on a successful flush the
// triggering item actually lands at offset 0 of the freshly reset batch but its key
// record kept the stale pre-reset offset. value2 below is that triggering item; its
// key must describe where it actually sits in the batch that gets written to the
// store, not where it would have sat in the batch that was just flushed away.
func TestBatcher_KeyOffset_MatchesActualPosition(t *testing.T) {
	store := memory.New()
	batcher := New(ulogger.TestLogger{}, store, 10, true)

	value1 := []byte("AAAAAAAA") // 8 bytes
	value2 := []byte("BBBBBBBB") // 8 bytes: triggers the overflow flush of value1's batch

	var hash1, hash2 chainhash.Hash
	hash1[0] = 1
	hash2[0] = 2

	require.NoError(t, batcher.Set(context.Background(), hash1[:], fileformat.FileTypeUtxoSet, value1))
	require.NoError(t, batcher.Set(context.Background(), hash2[:], fileformat.FileTypeUtxoSet, value2))
	require.NoError(t, batcher.Close(context.Background()))

	keys := store.ListKeys()
	require.NotEmpty(t, keys)

	expected := map[chainhash.Hash][]byte{
		hash1: value1,
		hash2: value2,
	}
	found := map[chainhash.Hash]bool{}

	for _, k := range keys {
		batchData, err := store.Get(context.Background(), k, fileformat.FileTypeBatchData)
		require.NoError(t, err)

		batchKeys, err := store.Get(context.Background(), k, fileformat.FileTypeBatchKeys)
		require.NoError(t, err)

		for _, line := range strings.Split(strings.TrimSpace(string(batchKeys)), "\n") {
			if line == "" {
				continue
			}

			raw, err := hex.DecodeString(line)
			require.NoError(t, err)
			require.Len(t, raw, chainhash.HashSize+8)

			var h chainhash.Hash
			copy(h[:], raw[:chainhash.HashSize])

			pos := binary.BigEndian.Uint32(raw[chainhash.HashSize : chainhash.HashSize+4])
			size := binary.BigEndian.Uint32(raw[chainhash.HashSize+4 : chainhash.HashSize+8])

			want, ok := expected[h]
			if !ok {
				continue
			}

			require.LessOrEqualf(t, uint64(pos)+uint64(size), uint64(len(batchData)),
				"key record for %x claims offset %d size %d but its batch data is only %d bytes", h[:], pos, size, len(batchData))
			require.Equal(t, want, batchData[pos:pos+size], "batch data at the key's recorded offset does not match the value written for %x", h[:])

			found[h] = true
		}
	}

	require.True(t, found[hash1], "hash1's key record was never located")
	require.True(t, found[hash2], "hash2's key record was never located")
}

// blockingStore is a blobStoreSetter fake whose Set call never returns, simulating
// a hung backend. Used to verify Close abandons the shutdown flush wait rather than
// blocking forever.
type blockingStore struct{}

func (blockingStore) Health(_ context.Context, _ bool) (int, string, error) {
	return 200, "ok", nil
}

func (blockingStore) Set(_ context.Context, _ []byte, _ fileformat.FileType, _ []byte, _ ...options.FileOption) error {
	select {} // block forever
}

// TestBatcher_Close_AbandonsHungFlush pins defect 3: Close used to discard its
// context and do a bare wg.Wait(), so a hung backend meant Close never returned.
// Here the shutdown flush calls into blockingStore.Set, which never returns; Close
// must still return (with an error) once its own context expires, rather than
// hanging indefinitely.
func TestBatcher_Close_AbandonsHungFlush(t *testing.T) {
	batcher := New(ulogger.TestLogger{}, blockingStore{}, 1024, false)

	var hash chainhash.Hash
	require.NoError(t, batcher.Set(context.Background(), hash[:], fileformat.FileTypeUtxoSet, []byte("data")))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := batcher.Close(ctx)
	elapsed := time.Since(start)

	require.Error(t, err, "Close should report the abandoned shutdown flush")
	require.Less(t, elapsed, 5*time.Second, "Close should abandon the wait close to its deadline, not hang")
}

// alwaysFailingStore is a blobStoreSetter fake whose Set call always returns an error,
// simulating a backend that is reachable but rejecting writes — as opposed to
// blockingStore, which hangs. Used to verify Close reports a shutdown flush that ran
// and failed, rather than one that never completed.
type alwaysFailingStore struct{}

func (alwaysFailingStore) Health(_ context.Context, _ bool) (int, string, error) {
	return 200, "ok", nil
}

func (alwaysFailingStore) Set(_ context.Context, _ []byte, _ fileformat.FileType, _ []byte, _ ...options.FileOption) error {
	return errors.NewStorageError("simulated permanent backend failure")
}

// TestBatcher_Close_ReportsFailedShutdownFlush pins the case the hung-backend fix left
// open: the worker's final writeBatch error was only logged, so Close returned nil and
// the caller was told the store shut down cleanly while the batch it was holding had
// been discarded. That is the same "told success, dropped the bytes" defect this type
// fixes on the overflow path, and it must not survive on the shutdown path.
func TestBatcher_Close_ReportsFailedShutdownFlush(t *testing.T) {
	batcher := New(ulogger.TestLogger{}, alwaysFailingStore{}, 1024, false)

	var hash chainhash.Hash
	require.NoError(t, batcher.Set(context.Background(), hash[:], fileformat.FileTypeUtxoSet, []byte("data")))

	// A generous deadline: this must fail because the flush failed, not because it timed out.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := batcher.Close(ctx)
	require.Error(t, err, "Close must report a shutdown flush that ran and failed")
	require.NotErrorIs(t, err, context.DeadlineExceeded, "the error should describe the failed flush, not a timeout")
}

// TestBatcher_Close_IsIdempotent pins the panic that making Close fallible invited: a
// caller that sees an error from Close is exactly the caller who retries it, and the
// second close of the done channel used to panic during shutdown.
func TestBatcher_Close_IsIdempotent(t *testing.T) {
	batcher := New(ulogger.TestLogger{}, memory.New(), 1024, false)

	var hash chainhash.Hash
	require.NoError(t, batcher.Set(context.Background(), hash[:], fileformat.FileTypeUtxoSet, []byte("data")))

	first := batcher.Close(context.Background())
	require.NoError(t, first)

	require.NotPanics(t, func() {
		second := batcher.Close(context.Background())
		require.Equal(t, first, second, "a repeat Close should return the first call's result")
	})
}

// TestBatcher_Set_AfterClose_ReturnsError pins the third "told success, dropped the
// bytes" path: the worker has returned by then, so an item accepted after Close would
// sit in the queue forever with its caller told the write was queued fine.
func TestBatcher_Set_AfterClose_ReturnsError(t *testing.T) {
	batcher := New(ulogger.TestLogger{}, memory.New(), 1024, false)

	require.NoError(t, batcher.Close(context.Background()))

	var hash chainhash.Hash
	err := batcher.Set(context.Background(), hash[:], fileformat.FileTypeUtxoSet, []byte("data"))
	require.Error(t, err, "Set after Close must not silently accept a write nobody will process")
}

// TestBatcher_Set_ShortKey_ReturnsError pins defect 4: Set used to do
// chainhash.Hash(hash) unconditionally, which panics for any key that isn't
// exactly 32 bytes (e.g. the blockchain peer-registry store's 13-byte
// "peer-registry" key). It must return an error instead.
func TestBatcher_Set_ShortKey_ReturnsError(t *testing.T) {
	store := memory.New()
	batcher := New(ulogger.TestLogger{}, store, 1024, false)
	defer batcher.Close(context.Background())

	shortKey := []byte("peer-registry") // 13 bytes, not a 32-byte hash

	err := batcher.Set(context.Background(), shortKey, fileformat.FileTypeUtxoSet, []byte("value"))
	require.Error(t, err)
}

// TestBatcher_shouldFlushBefore pins the last "told success, dropped the bytes" path.
// Key records address each item with a uint32 offset, so a batch retained across flush
// failures could grow past that ceiling; the conversion in processBatchItem then failed
// and returned before appending the item, discarding bytes whose caller had long since
// been told the write was queued. The batch has to be flushed before it can reach the
// ceiling, which resets the offset to zero and lets the item be appended normally.
//
// The arithmetic is exercised directly rather than through Set, because reaching the
// ceiling for real would need a 4 GiB batch.
// TestBatcher_exceedsKeyRecordSize covers the guard Set applies to an oversize value.
// The arithmetic is tested directly because reaching the limit through Set would need a
// 4 GiB allocation. The boundary matters: a value of exactly MaxUint32 bytes still fits a
// key record and must be accepted, so the comparison has to be strictly greater than.
func TestBatcher_exceedsKeyRecordSize(t *testing.T) {
	tests := []struct {
		name      string
		valueLen  int
		writeKeys bool
		want      bool
	}{
		{name: "an ordinary value is fine", valueLen: 4096, writeKeys: true, want: false},
		{name: "exactly the ceiling still fits a key record", valueLen: math.MaxUint32, writeKeys: true, want: false},
		{name: "one byte past the ceiling cannot be indexed", valueLen: math.MaxUint32 + 1, writeKeys: true, want: true},
		{name: "without key records there is no ceiling", valueLen: math.MaxUint32 + 1, writeKeys: false, want: false},
		{name: "an empty value is fine", valueLen: 0, writeKeys: true, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exceedsKeyRecordSize(tt.valueLen, tt.writeKeys)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBatcher_shouldFlushBefore(t *testing.T) {
	// One item short of the ceiling, so a modest item tips it over.
	nearCeiling := int(math.MaxUint32) - 8

	tests := []struct {
		name        string
		currentPos  int
		dataSize    int
		sizeInBytes int
		writeKeys   bool
		want        bool
	}{
		{
			name:       "an empty batch is never flushed, even for an oversize item",
			currentPos: 0, dataSize: 4096, sizeInBytes: 1024, want: false,
		},
		{
			name:       "an item that exactly fills the batch does not flush first",
			currentPos: 512, dataSize: 512, sizeInBytes: 1024, want: false,
		},
		{
			name:       "one byte of overflow flushes first",
			currentPos: 512, dataSize: 513, sizeInBytes: 1024, want: true,
		},
		{
			name:       "the uint32 offset ceiling flushes first when keys are written",
			currentPos: nearCeiling, dataSize: 16, sizeInBytes: math.MaxInt, writeKeys: true, want: true,
		},
		{
			name:       "the uint32 offset ceiling is irrelevant when no keys are written",
			currentPos: nearCeiling, dataSize: 16, sizeInBytes: math.MaxInt, writeKeys: false, want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldFlushBefore(tt.currentPos, tt.dataSize, tt.sizeInBytes, tt.writeKeys)
			require.Equal(t, tt.want, got)
		})
	}
}
