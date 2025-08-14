package uaerospike

import (
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func TestClient_Put(t *testing.T) {
	// Create a test client with mocked semaphore behavior
	client := &Client{
		Client:        nil,                    // We'll test semaphore behavior without actual client
		connSemaphore: make(chan struct{}, 2), // Small buffer for testing
	}

	t.Run("semaphore acquire and release", func(t *testing.T) {
		// Fill the semaphore
		client.connSemaphore <- struct{}{}
		client.connSemaphore <- struct{}{}

		// Start a goroutine that will block trying to acquire
		blocked := make(chan bool)
		go func() {
			select {
			case client.connSemaphore <- struct{}{}:
				blocked <- false
			case <-time.After(10 * time.Millisecond):
				blocked <- true
			}
		}()

		// Should be blocked
		assert.True(t, <-blocked)

		// Release one slot
		<-client.connSemaphore

		// Now it should succeed
		go func() {
			select {
			case client.connSemaphore <- struct{}{}:
				blocked <- false
			case <-time.After(10 * time.Millisecond):
				blocked <- true
			}
		}()

		assert.False(t, <-blocked)
	})
}

func TestCalculateKeySource(t *testing.T) {
	tests := []struct {
		name     string
		hash     *chainhash.Hash
		num      uint32
		expected func([]byte) bool
	}{
		{
			name: "zero offset returns hash bytes",
			hash: &chainhash.Hash{0x01, 0x02, 0x03},
			num:  0,
			expected: func(result []byte) bool {
				return len(result) == chainhash.HashSize && result[0] == 0x01 && result[1] == 0x02 && result[2] == 0x03
			},
		},
		{
			name: "non-zero offset appends to hash",
			hash: &chainhash.Hash{0x01, 0x02, 0x03},
			num:  1,
			expected: func(result []byte) bool {
				return len(result) == chainhash.HashSize+4 && result[0] == 0x01 && result[chainhash.HashSize] == 0x01
			},
		},
		{
			name: "large offset",
			hash: &chainhash.Hash{0xFF},
			num:  0xFFFFFFFF,
			expected: func(result []byte) bool {
				return len(result) == chainhash.HashSize+4 &&
					result[chainhash.HashSize] == 0xFF &&
					result[chainhash.HashSize+1] == 0xFF &&
					result[chainhash.HashSize+2] == 0xFF &&
					result[chainhash.HashSize+3] == 0xFF
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateKeySource(tt.hash, tt.num)
			assert.True(t, tt.expected(result), "Unexpected result for %s", tt.name)
		})
	}
}

func TestGetConnectionQueueSize(t *testing.T) {
	tests := []struct {
		name     string
		policy   *aerospike.ClientPolicy
		expected int
	}{
		{
			name:     "nil policy returns default",
			policy:   nil,
			expected: DefaultConnectionQueueSize,
		},
		{
			name: "policy with zero queue size returns default",
			policy: &aerospike.ClientPolicy{
				ConnectionQueueSize: 0,
			},
			expected: DefaultConnectionQueueSize,
		},
		{
			name: "policy with custom queue size",
			policy: &aerospike.ClientPolicy{
				ConnectionQueueSize: 256,
			},
			expected: 256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getConnectionQueueSize(tt.policy)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClient_ConcurrentOperations(t *testing.T) {
	client := &Client{
		Client:        nil,
		connSemaphore: make(chan struct{}, 2), // Allow 2 concurrent operations
	}

	t.Run("concurrent semaphore usage", func(t *testing.T) {
		// Test that multiple goroutines can acquire and release semaphore correctly
		const numGoroutines = 10
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				// Simulate acquiring semaphore
				client.connSemaphore <- struct{}{}
				time.Sleep(1 * time.Millisecond) // Simulate work
				<-client.connSemaphore           // Release
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			select {
			case <-done:
				// Success
			case <-time.After(1 * time.Second):
				t.Fatal("Timeout waiting for goroutines to complete")
			}
		}
	})
}

// BenchmarkCalculateKeySource benchmarks the key source calculation
func BenchmarkCalculateKeySource(b *testing.B) {
	hash := &chainhash.Hash{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	b.Run("WithZeroOffset", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = CalculateKeySource(hash, 0)
		}
	})

	b.Run("WithNonZeroOffset", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = CalculateKeySource(hash, uint32(i))
		}
	})
}

// Helper function to test semaphore behavior
func testSemaphoreBlocking(t *testing.T, client *Client, expectedBlocked bool) {
	blocked := make(chan bool)
	go func() {
		select {
		case client.connSemaphore <- struct{}{}:
			blocked <- false
			<-client.connSemaphore // Clean up
		case <-time.After(10 * time.Millisecond):
			blocked <- true
		}
	}()

	assert.Equal(t, expectedBlocked, <-blocked)
}

func TestClientStats(t *testing.T) {
	t.Run("NewClientStats creates valid stats", func(t *testing.T) {
		stats := NewClientStats()
		assert.NotNil(t, stats)
		assert.NotNil(t, stats.stat)
		assert.NotNil(t, stats.operateStat)
		assert.NotNil(t, stats.batchOperateStat)
	})

	t.Run("client always has stats", func(t *testing.T) {
		client := &Client{
			Client:        nil,
			connSemaphore: make(chan struct{}, 1),
			stats:         NewClientStats(),
		}
		assert.NotNil(t, client.stats)
	})
}
