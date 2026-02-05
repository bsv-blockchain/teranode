package batcher

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
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
