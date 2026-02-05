package memory

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
)

func TestCleanExpiredFiles(t *testing.T) {
	// Create test data
	testCases := []struct {
		name     string
		key      []byte
		dah      uint32
		expected bool // whether the blob should still exist after cleaning
	}{
		{
			name:     "expired blob",
			key:      []byte("expired"),
			dah:      1,
			expected: false,
		},
		{
			name:     "non-expired blob",
			key:      []byte("fresh"),
			dah:      2,
			expected: true,
		},
		{
			name:     "blob without DAH",
			key:      []byte("no-dah"),
			dah:      0,
			expected: true,
		},
	}

	// Create a new memory store
	store := New()

	currentHeight := uint32(1)
	store.SetBlockHeight(currentHeight)

	// Set up test data
	for _, tc := range testCases {
		storeKey := hashKey(tc.key, fileformat.FileTypeTesting, options.NewStoreOptions())
		store.blobs[storeKey] = &blobData{
			data: []byte("test data"),
			dah:  tc.dah,
		}
	}

	// Run the cleaner to clean at block height 1
	cleanExpiredFiles(store, currentHeight)

	// Verify results
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storeKey := hashKey(tc.key, fileformat.FileTypeTesting, options.NewStoreOptions())

			_, exists := store.blobs[storeKey]
			if exists != tc.expected {
				t.Errorf("case %s: expected blob existence to be %v, got %v",
					tc.name, tc.expected, exists)
			}

			// Verify that associated metadata is also cleaned up
			dahExists := false
			if bd, ok := store.blobs[storeKey]; ok && bd.dah > 0 {
				dahExists = true
			}

			headerExists := false // headers map removed
			footerExists := false // footers map removed

			if exists {
				// If blob should exist, its metadata should also exist (if it had DAH)
				// For DAH == 0, we do not expect a DAH/timestamp to exist
				if tc.dah > 0 && !dahExists {
					t.Errorf("case %s: DAH was cleaned up but blob exists", tc.name)
				}
			} else {
				// If blob shouldn't exist, its metadata should be cleaned up
				if dahExists {
					t.Errorf("case %s: timestamp exists but blob was cleaned", tc.name)
				}

				if dahExists {
					t.Errorf("case %s: DAH exists but blob was cleaned", tc.name)
				}

				if headerExists {
					t.Errorf("case %s: header exists but blob was cleaned", tc.name)
				}

				if footerExists {
					t.Errorf("case %s: footer exists but blob was cleaned", tc.name)
				}
			}
		})
	}
}

func TestMemory_Health(t *testing.T) {
	store := New()

	status, msg, err := store.Health(context.Background(), true)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if status != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, status)
	}

	if msg != "Memory Store" {
		t.Errorf("expected message 'Memory Store', got '%s'", msg)
	}

	if store.Counters["health"] != 1 {
		t.Errorf("expected health counter to be 1, got %d", store.Counters["health"])
	}
}

func TestMemory_SetAndGet(t *testing.T) {
	store := New()
	key := []byte("test-key")
	value := []byte("test-value")

	// Test Set
	err := store.Set(context.Background(), key, fileformat.FileTypeTesting, value)
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	// Test Get
	retrieved, err := store.Get(context.Background(), key, fileformat.FileTypeTesting)
	if err != nil {
		t.Errorf("unexpected error on Get: %v", err)
	}

	if !bytes.Equal(retrieved, value) {
		t.Errorf("expected value %s, got %s", value, retrieved)
	}

	// Test Get with non-existent key
	_, err = store.Get(context.Background(), []byte("non-existent"), fileformat.FileTypeTesting)
	if err == nil || !errors.Is(err, errors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_SetFromReader(t *testing.T) {
	store := New()
	key := []byte("test-key")
	value := []byte("test-value")
	reader := io.NopCloser(bytes.NewReader(value))

	err := store.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, reader)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify the value was stored correctly
	retrieved, err := store.Get(context.Background(), key, fileformat.FileTypeTesting)
	if err != nil {
		t.Errorf("unexpected error on Get: %v", err)
	}

	if !bytes.Equal(retrieved, value) {
		t.Errorf("expected value %s, got %s", value, retrieved)
	}
}

func TestMemory_TTLOperations(t *testing.T) {
	store := New()
	key := []byte("test-key")
	value := []byte("test-value")
	dah := uint32(5)

	// Set with DAH
	err := store.Set(context.Background(), key, fileformat.FileTypeTesting, value, options.WithDeleteAt(dah))
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	// DAH is now managed centrally by pruner service, not by blob stores
	// GetDAH removed from interface - use pruner service API to query DAH values

	// Update DAH
	newDAH := uint32(10)

	err = store.SetDAH(context.Background(), key, fileformat.FileTypeTesting, newDAH)
	if err != nil {
		t.Errorf("unexpected error on SetDAH: %v", err)
	}

	// DAH verification now done via pruner service in e2e tests
}

func TestMemory_Exists(t *testing.T) {
	store := New()
	key := []byte("test-key")
	value := []byte("test-value")

	// Test non-existent key
	exists, err := store.Exists(context.Background(), key, fileformat.FileTypeTesting)
	if err != nil {
		t.Errorf("unexpected error on Exists: %v", err)
	}

	if exists {
		t.Error("key should not exist")
	}

	// Add key and test again
	err = store.Set(context.Background(), key, fileformat.FileTypeTesting, value)
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	exists, err = store.Exists(context.Background(), key, fileformat.FileTypeTesting)
	if err != nil {
		t.Errorf("unexpected error on Exists: %v", err)
	}

	if !exists {
		t.Error("key should exist")
	}
}

func TestMemory_Del(t *testing.T) {
	store := New()
	key := []byte("test-key")
	value := []byte("test-value")

	// Set up initial data
	err := store.Set(context.Background(), key, fileformat.FileTypeTesting, value)
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	// Delete the key
	err = store.Del(context.Background(), key, fileformat.FileTypeTesting)
	if err != nil {
		t.Errorf("unexpected error on Del: %v", err)
	}

	// Verify key no longer exists
	exists, err := store.Exists(context.Background(), key, fileformat.FileTypeTesting)
	if err != nil {
		t.Errorf("unexpected error on Exists: %v", err)
	}

	if exists {
		t.Error("key should not exist after deletion")
	}
}

func TestMemory_GetIoReader(t *testing.T) {
	store := New()
	key := []byte("test-key")
	value := []byte("test-value")

	err := store.Set(context.Background(), key, fileformat.FileTypeTesting, value)
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	reader, err := store.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
	if err != nil {
		t.Errorf("unexpected error on GetIoReader: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected error reading from reader: %v", err)
	}

	if !bytes.Equal(data, value) {
		t.Errorf("expected value %s, got %s", value, data)
	}
}

func TestMemory_Close(t *testing.T) {
	store := New()

	err := store.Close(context.Background())
	if err != nil {
		t.Errorf("unexpected error on Close: %v", err)
	}

	if store.Counters["close"] != 1 {
		t.Errorf("expected close counter to be 1, got %d", store.Counters["close"])
	}
}
