package memory

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
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
		storeKey := hashKey(tc.key, options.NewStoreOptions())
		store.blobs[storeKey] = []byte("test data")
		store.dahs[storeKey] = tc.dah
	}

	// Run the cleaner to clean at block height 1
	cleanExpiredFiles(store, currentHeight)

	// Verify results
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storeKey := hashKey(tc.key, options.NewStoreOptions())

			_, exists := store.blobs[storeKey]
			if exists != tc.expected {
				t.Errorf("case %s: expected blob existence to be %v, got %v",
					tc.name, tc.expected, exists)
			}

			// Verify that associated metadata is also cleaned up
			_, dahExists := store.dahs[storeKey]
			_, headerExists := store.headers[storeKey]
			_, footerExists := store.footers[storeKey]

			if exists {
				// If blob should exist, its metadata should also exist (if it had DAH)
				if tc.dah > 0 && !dahExists {
					t.Errorf("case %s: DAH was cleaned up but blob exists", tc.name)
				}

				if !dahExists {
					t.Errorf("case %s: timestamp was cleaned up but blob exists", tc.name)
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
	err := store.Set(context.Background(), key, value)
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	// Test Get
	retrieved, err := store.Get(context.Background(), key)
	if err != nil {
		t.Errorf("unexpected error on Get: %v", err)
	}

	if !bytes.Equal(retrieved, value) {
		t.Errorf("expected value %s, got %s", value, retrieved)
	}

	// Test Get with non-existent key
	_, err = store.Get(context.Background(), []byte("non-existent"))
	if err == nil || !errors.Is(err, errors.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_SetFromReader(t *testing.T) {
	store := New()
	key := []byte("test-key")
	value := []byte("test-value")
	reader := io.NopCloser(bytes.NewReader(value))

	err := store.SetFromReader(context.Background(), key, reader)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify the value was stored correctly
	retrieved, err := store.Get(context.Background(), key)
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
	err := store.Set(context.Background(), key, value, options.WithDeleteAt(dah))
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	// Get DAH
	retrievedDAH, err := store.GetDAH(context.Background(), key)
	if err != nil {
		t.Errorf("unexpected error on GetDAH: %v", err)
	}

	if retrievedDAH != dah {
		t.Errorf("expected DAH %v, got %v", dah, retrievedDAH)
	}

	// Update DAH
	newDAH := uint32(10)

	err = store.SetDAH(context.Background(), key, newDAH)
	if err != nil {
		t.Errorf("unexpected error on SetDAH: %v", err)
	}

	// Verify updated DAH
	retrievedDAH, err = store.GetDAH(context.Background(), key)
	if err != nil {
		t.Errorf("unexpected error on GetDAH: %v", err)
	}

	if retrievedDAH != newDAH {
		t.Errorf("expected DAH %v, got %v", newDAH, retrievedDAH)
	}
}

func TestMemory_GetHead(t *testing.T) {
	store := New()
	key := []byte("test-key")
	value := []byte("test-value-longer-than-needed")

	err := store.Set(context.Background(), key, value)
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	// Test getting partial content
	head, err := store.GetHead(context.Background(), key, 4)
	if err != nil {
		t.Errorf("unexpected error on GetHead: %v", err)
	}

	if !bytes.Equal(head, []byte("test")) {
		t.Errorf("expected head 'test', got '%s'", head)
	}

	// Test getting more bytes than available
	head, err = store.GetHead(context.Background(), key, 100)
	if err != nil {
		t.Errorf("unexpected error on GetHead: %v", err)
	}

	if !bytes.Equal(head, value) {
		t.Errorf("expected full value when requesting more bytes than available")
	}
}

func TestMemory_Exists(t *testing.T) {
	store := New()
	key := []byte("test-key")
	value := []byte("test-value")

	// Test non-existent key
	exists, err := store.Exists(context.Background(), key)
	if err != nil {
		t.Errorf("unexpected error on Exists: %v", err)
	}

	if exists {
		t.Error("key should not exist")
	}

	// Add key and test again
	err = store.Set(context.Background(), key, value)
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	exists, err = store.Exists(context.Background(), key)
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
	err := store.Set(context.Background(), key, value)
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	// Delete the key
	err = store.Del(context.Background(), key)
	if err != nil {
		t.Errorf("unexpected error on Del: %v", err)
	}

	// Verify key no longer exists
	exists, err := store.Exists(context.Background(), key)
	if err != nil {
		t.Errorf("unexpected error on Exists: %v", err)
	}

	if exists {
		t.Error("key should not exist after deletion")
	}
}

func TestMemory_HeaderAndFooter(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")
	header := []byte("test-header")
	footer := []byte("test-footer")
	metadata := []byte("test-metadata")

	// Create a proper test footer implementation
	tf := options.NewFooter(len(footer)+len(metadata), footer, func() []byte {
		return metadata
	})

	store := New(
		options.WithHeader(header),
		options.WithFooter(tf),
	)

	// Set with header and footer
	err := store.Set(context.Background(), key, value)
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	// Get header
	retrievedHeader, err := store.GetHeader(context.Background(), key)
	if err != nil {
		t.Errorf("unexpected error on GetHeader: %v", err)
	}

	if !bytes.Equal(retrievedHeader, header) {
		t.Errorf("expected header %s, got %s", header, retrievedHeader)
	}

	// Get footer metadata
	retrievedMetadata, err := store.GetFooterMetaData(context.Background(), key)
	if err != nil {
		t.Errorf("unexpected error on GetFooterMetaData: %v", err)
	}

	if !bytes.Equal(retrievedMetadata, metadata) {
		t.Errorf("expected footer metadata %s, got %s", metadata, retrievedMetadata)
	}
}

func TestMemory_GetIoReader(t *testing.T) {
	store := New()
	key := []byte("test-key")
	value := []byte("test-value")

	err := store.Set(context.Background(), key, value)
	if err != nil {
		t.Errorf("unexpected error on Set: %v", err)
	}

	reader, err := store.GetIoReader(context.Background(), key)
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
