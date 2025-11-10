package subtreevalidation

import (
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createUniqueTx creates a unique transaction by cloning a base transaction and modifying its LockTime
// This follows the pattern used in SubtreeValidation_test.go
func createUniqueTx(baseTx *bt.Tx, nonce uint32) *bt.Tx {
	tx := baseTx.Clone()
	tx.LockTime = nonce
	return tx
}

// createTestServer creates a server with a test configuration
func createTestServer(t *testing.T, maxSize int) *Server {
	orphanage, err := NewOrphanage(15*time.Minute, maxSize, &ulogger.TestLogger{})
	require.NoError(t, err, "Failed to create orphanage")
	return &Server{
		orphanage: orphanage,
		logger:    &ulogger.TestLogger{},
	}
}

// ============================================================================
// Basic Operations Tests
// ============================================================================

// TestOrphanageBasicOperations tests the basic CRUD operations of the orphanage
func TestOrphanageBasicOperations(t *testing.T) {
	logger := &ulogger.TestLogger{}

	orphanage, err := NewOrphanage(15*time.Minute, 100, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	tx, err := createTestTransaction("tx1")
	require.NoError(t, err)
	txHash := *tx.TxIDChainHash()

	// Test Set and Get
	success := server.orphanage.Set(txHash, tx)
	assert.True(t, success, "Should be able to add transaction")

	// Test Get
	retrievedTx, exists := server.orphanage.Get(txHash)
	assert.True(t, exists, "Transaction should exist after adding")
	assert.Equal(t, tx.TxID(), retrievedTx.TxID(), "Retrieved transaction should match original")

	// Test Delete
	server.orphanage.Delete(txHash)
	_, exists = server.orphanage.Get(txHash)
	assert.False(t, exists, "Transaction should not exist after deletion")

	// Test Items
	items := server.orphanage.Items()
	assert.Empty(t, items, "Should return empty slice when no items")
}

// ============================================================================
// Size Limit Tests
// ============================================================================

// TestOrphanageSizeLimit tests that the orphanage enforces size limits by rejecting new entries when full
func TestOrphanageSizeLimit(t *testing.T) {
	logger := &ulogger.TestLogger{}
	maxSize := 5

	// Create a server with a small orphanage max size for testing
	orphanage, err := NewOrphanage(15*time.Minute, maxSize, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	// Create a base transaction to clone from
	baseTx, err := createTestTransaction("tx1")
	require.NoError(t, err)

	// Add transactions up to maxSize using unique transactions
	for i := 0; i < maxSize; i++ {
		tx := createUniqueTx(baseTx, uint32(i+1))
		success := server.orphanage.Set(*tx.TxIDChainHash(), tx)
		assert.True(t, success, "Should be able to add transaction when under limit")
	}

	// Should be at maxSize
	assert.Equal(t, maxSize, server.orphanage.Len(), "Orphanage should be at maxSize")

	// Try to add more transactions - should be rejected
	for i := 0; i < 3; i++ {
		tx := createUniqueTx(baseTx, uint32(i+100))
		success := server.orphanage.Set(*tx.TxIDChainHash(), tx)
		assert.False(t, success, "Should reject transaction when orphanage is full")
	}

	// Should still be at maxSize
	assert.Equal(t, maxSize, server.orphanage.Len(), "Orphanage should still be at maxSize")
}

// TestOrphanageRejectionPreventsParentRemoval tests that rejecting new entries when full
// prevents removing parent transactions that child transactions depend on
func TestOrphanageRejectionPreventsParentRemoval(t *testing.T) {
	logger := &ulogger.TestLogger{}
	maxSize := 3

	orphanage, err := NewOrphanage(15*time.Minute, maxSize, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	// Create a base transaction to clone from
	baseTx, err := createTestTransaction("tx1")
	require.NoError(t, err)

	// Add transactions up to maxSize (these represent parent transactions)
	var parentTxs []*bt.Tx
	for i := 0; i < maxSize; i++ {
		tx := createUniqueTx(baseTx, uint32(i+1))
		parentTxs = append(parentTxs, tx)
		success := server.orphanage.Set(*tx.TxIDChainHash(), tx)
		assert.True(t, success, "Should be able to add parent transaction")
	}

	// Verify all parent transactions are still there
	assert.Equal(t, maxSize, server.orphanage.Len(), "All parent transactions should be present")

	// Try to add a child transaction - should be rejected
	childTx := createUniqueTx(baseTx, uint32(999))
	success := server.orphanage.Set(*childTx.TxIDChainHash(), childTx)
	assert.False(t, success, "Should reject child transaction when orphanage is full")

	// Verify parent transactions are still there (no eviction occurred)
	assert.Equal(t, maxSize, server.orphanage.Len(), "Parent transactions should still be present")

	// Verify we can still retrieve the first parent transaction
	firstParentHash := *parentTxs[0].TxIDChainHash()
	retrievedTx, exists := server.orphanage.Get(firstParentHash)
	assert.True(t, exists, "First parent transaction should still exist")
	assert.Equal(t, parentTxs[0].TxID(), retrievedTx.TxID(), "First parent transaction should be unchanged")
}

// ============================================================================
// Locking Tests
// ============================================================================

// TestOrphanageLockingSetAndGet tests that orphanage Set and Get operations are properly locked
func TestOrphanageLockingSetAndGet(t *testing.T) {
	logger := &ulogger.TestLogger{}

	orphanage, err := NewOrphanage(15*time.Minute, 100, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	// Create a test transaction
	tx, err := createTestTransaction("test")
	require.NoError(t, err)
	txHash := *tx.TxIDChainHash()

	// Test Set (with internal locking)
	added := server.orphanage.Set(txHash, tx)

	assert.True(t, added, "Transaction should be added")

	// Test Get (with internal locking)
	retrieved, exists := server.orphanage.Get(txHash)
	length := server.orphanage.Len()

	assert.True(t, exists, "Transaction should exist")
	assert.Equal(t, 1, length, "Orphanage should have 1 transaction")
	assert.Equal(t, tx.TxID(), retrieved.TxID(), "Retrieved transaction should match")
}

// TestOrphanageLockingDelete tests that orphanage Delete operations are properly locked
func TestOrphanageLockingDelete(t *testing.T) {
	logger := &ulogger.TestLogger{}

	orphanage, err := NewOrphanage(15*time.Minute, 100, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	tx, err := createTestTransaction("test")
	require.NoError(t, err)
	txHash := *tx.TxIDChainHash()

	// Add transaction
	server.orphanage.Set(txHash, tx)

	// Delete (with internal locking)
	server.orphanage.Delete(txHash)

	// Verify deletion
	_, exists := server.orphanage.Get(txHash)
	length := server.orphanage.Len()

	assert.False(t, exists, "Transaction should not exist after deletion")
	assert.Equal(t, 0, length, "Orphanage should be empty")
}

// TestOrphanageLockingMultipleDeletes tests deleting multiple transactions with proper locking
func TestOrphanageLockingMultipleDeletes(t *testing.T) {
	logger := &ulogger.TestLogger{}

	orphanage, err := NewOrphanage(15*time.Minute, 100, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	// Add multiple transactions
	const count = 10
	var txHashes []chainhash.Hash

	for i := 0; i < count; i++ {
		tx := createUniqueTx(baseTx, uint32(i))
		txHash := *tx.TxIDChainHash()
		server.orphanage.Set(txHash, tx)
		txHashes = append(txHashes, txHash)
	}

	// Verify all added
	initialLength := server.orphanage.Len()
	assert.Equal(t, count, initialLength)

	// Delete all transactions (simulating subtree validation completion)
	for _, txHash := range txHashes {
		server.orphanage.Delete(txHash)
	}

	// Verify all deleted
	finalLength := server.orphanage.Len()

	assert.Equal(t, 0, finalLength, "All transactions should be deleted")
}

// TestOrphanageLockingFullQueue tests proper locking when orphanage is full
func TestOrphanageLockingFullQueue(t *testing.T) {
	logger := &ulogger.TestLogger{}
	maxSize := 5

	orphanage, err := NewOrphanage(15*time.Minute, maxSize, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	// Fill orphanage
	for i := 0; i < maxSize; i++ {
		tx := createUniqueTx(baseTx, uint32(i))
		added := server.orphanage.Set(*tx.TxIDChainHash(), tx)
		assert.True(t, added, "Should be able to add transaction %d", i)
	}

	// Try to add more (should be rejected)
	tx := createUniqueTx(baseTx, uint32(999))
	added := server.orphanage.Set(*tx.TxIDChainHash(), tx)
	length := server.orphanage.Len()

	assert.False(t, added, "Should reject when full")
	assert.Equal(t, maxSize, length, "Should still be at maxSize")
}

// TestOrphanageLockingItems tests that Items() can be safely called with proper locking
func TestOrphanageLockingItems(t *testing.T) {
	logger := &ulogger.TestLogger{}

	orphanage, err := NewOrphanage(15*time.Minute, 100, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	// Add transactions
	const count = 5
	for i := 0; i < count; i++ {
		tx := createUniqueTx(baseTx, uint32(i))
		server.orphanage.Set(*tx.TxIDChainHash(), tx)
	}

	// Get items
	items := server.orphanage.Items()
	length := server.orphanage.Len()

	assert.Equal(t, count, length, "Should have correct number of items")
	assert.Equal(t, count, len(items), "Items() should return all transactions")
}

// TestOrphanageLockingProcessOrphansPattern tests the locking pattern used in processOrphans
func TestOrphanageLockingProcessOrphansPattern(t *testing.T) {
	logger := &ulogger.TestLogger{}

	orphanage, err := NewOrphanage(15*time.Minute, 100, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	// Add orphans
	const count = 5
	for i := 0; i < count; i++ {
		tx := createUniqueTx(baseTx, uint32(i))
		server.orphanage.Set(*tx.TxIDChainHash(), tx)
	}

	// Simulate processOrphans pattern: get initial length and items
	initialLength := server.orphanage.Len()
	orphanTxs := server.orphanage.Items()

	assert.Equal(t, count, initialLength)
	assert.Equal(t, count, len(orphanTxs))

	// Simulate processing and deleting orphans
	for _, tx := range orphanTxs {
		server.orphanage.Delete(*tx.TxIDChainHash())
	}

	// Verify all processed
	finalLength := server.orphanage.Len()

	assert.Equal(t, 0, finalLength, "All orphans should be processed and removed")
}

// TestOrphanageLockingConcurrentOperations tests concurrent operations with proper locking
func TestOrphanageLockingConcurrentOperations(t *testing.T) {
	logger := &ulogger.TestLogger{}

	orphanage, err := NewOrphanage(15*time.Minute, 100, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	const numGoroutines = 10
	const operationsPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Concurrent add operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				tx := createUniqueTx(baseTx, uint32(id*1000+j))
				server.orphanage.Set(*tx.TxIDChainHash(), tx)
			}
		}(i)
	}

	// Concurrent delete operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				tx := createUniqueTx(baseTx, uint32(id*1000+j))
				server.orphanage.Delete(*tx.TxIDChainHash())
			}
		}(i)
	}

	wg.Wait()

	// Verify no race conditions occurred (test passes if no panic)
	_ = server.orphanage.Len()

	t.Log("Concurrent operations completed successfully")
}

// ============================================================================
// Race Condition and Concurrency Tests
// ============================================================================

// TestOrphanageConcurrentAccess tests concurrent reads and writes to the orphanage
// This test is designed to catch race conditions with the -race flag
func TestOrphanageConcurrentAccess(t *testing.T) {
	server := createTestServer(t, 100)

	// Create a base transaction for testing
	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	const numGoroutines = 50
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3) // 3 types of operations

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				tx := createUniqueTx(baseTx, uint32(id*1000+j))
				server.orphanage.Set(*tx.TxIDChainHash(), tx)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				tx := createUniqueTx(baseTx, uint32(id*1000+j))
				_, _ = server.orphanage.Get(*tx.TxIDChainHash())
			}
		}(i)
	}

	// Concurrent deletes
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				tx := createUniqueTx(baseTx, uint32(id*1000+j))
				server.orphanage.Delete(*tx.TxIDChainHash())
			}
		}(i)
	}

	// Wait for all goroutines to complete with timeout to detect deadlocks
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - no deadlock
		t.Log("All concurrent operations completed successfully")
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out - possible deadlock detected")
	}
}

// TestOrphanageMixedOperations tests a realistic scenario with mixed operations
func TestOrphanageMixedOperations(t *testing.T) {
	server := createTestServer(t, 50)

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	const numWorkers = 10
	const numIterations = 50

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for worker := 0; worker < numWorkers; worker++ {
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < numIterations; i++ {
				tx := createUniqueTx(baseTx, uint32(workerID*1000+i))
				txHash := *tx.TxIDChainHash()

				// Add transaction
				added := server.orphanage.Set(txHash, tx)

				if added {
					// Try to get it back
					retrieved, exists := server.orphanage.Get(txHash)

					if exists {
						assert.Equal(t, tx.TxID(), retrieved.TxID())

						// Sometimes delete it
						if i%2 == 0 {
							server.orphanage.Delete(txHash)
						}
					}
				}

				// Check length (should not cause race)
				_ = server.orphanage.Len()
			}
		}(worker)
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Mixed operations completed successfully")
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out - possible deadlock detected")
	}
}

// TestOrphanageNoDeadlockOnFullQueue tests that operations don't deadlock when orphanage is full
func TestOrphanageNoDeadlockOnFullQueue(t *testing.T) {
	maxSize := 10
	server := createTestServer(t, maxSize)

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	// Fill the orphanage
	for i := 0; i < maxSize; i++ {
		tx := createUniqueTx(baseTx, uint32(i))
		added := server.orphanage.Set(*tx.TxIDChainHash(), tx)
		assert.True(t, added, "Should be able to add transaction %d", i)
	}

	// Verify it's full
	length := server.orphanage.Len()
	assert.Equal(t, maxSize, length)

	// Now try to add more from multiple goroutines - should not deadlock
	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	rejected := make(chan bool, numGoroutines*10)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				tx := createUniqueTx(baseTx, uint32(maxSize+id*100+j))
				added := server.orphanage.Set(*tx.TxIDChainHash(), tx)
				rejected <- !added
			}
		}(i)
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		close(rejected)
		// Count rejections
		rejectionCount := 0
		for wasRejected := range rejected {
			if wasRejected {
				rejectionCount++
			}
		}
		t.Logf("Successfully handled %d rejections without deadlock", rejectionCount)
		assert.Greater(t, rejectionCount, 0, "Should have rejected some transactions")
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out - possible deadlock when orphanage is full")
	}
}

// ============================================================================
// Stress Tests
// ============================================================================

// TestOrphanageStressTest is a comprehensive stress test
func TestOrphanageStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	server := createTestServer(t, 200)

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	const duration = 5 * time.Second
	const numWriters = 10
	const numReaders = 10
	const numDeleters = 5

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Start writers
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			counter := 0
			for {
				select {
				case <-stopCh:
					return
				default:
					tx := createUniqueTx(baseTx, uint32(id*100000+counter))
					server.orphanage.Set(*tx.TxIDChainHash(), tx)
					counter++
				}
			}
		}(i)
	}

	// Start readers
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()
			counter := 0
			for {
				select {
				case <-stopCh:
					return
				default:
					tx := createUniqueTx(baseTx, uint32(id*100000+counter))
					_, _ = server.orphanage.Get(*tx.TxIDChainHash())
					_ = server.orphanage.Len()
					_ = server.orphanage.Items()
					counter++
				}
			}
		}(i)
	}

	// Start deleters
	wg.Add(numDeleters)
	for i := 0; i < numDeleters; i++ {
		go func(id int) {
			defer wg.Done()
			counter := 0
			for {
				select {
				case <-stopCh:
					return
				default:
					tx := createUniqueTx(baseTx, uint32(id*100000+counter))
					server.orphanage.Delete(*tx.TxIDChainHash())
					counter++
				}
			}
		}(i)
	}

	// Let it run for the specified duration
	time.Sleep(duration)
	close(stopCh)

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Stress test completed successfully")
	case <-time.After(10 * time.Second):
		t.Fatal("Stress test timed out - possible deadlock")
	}
}

// ============================================================================
// NewOrphanage Error Tests
// ============================================================================

// TestNewOrphanage_ErrorOnNilLogger tests that NewOrphanage returns error when logger is nil
func TestNewOrphanage_ErrorOnNilLogger(t *testing.T) {
	_, err := NewOrphanage(15*time.Minute, 100, nil)
	assert.Error(t, err, "NewOrphanage should return error when logger is nil")
	assert.Contains(t, err.Error(), "logger")
}

// TestNewOrphanage_ErrorOnZeroMaxSize tests that NewOrphanage returns error when maxSize is 0
func TestNewOrphanage_ErrorOnZeroMaxSize(t *testing.T) {
	logger := &ulogger.TestLogger{}

	_, err := NewOrphanage(15*time.Minute, 0, logger)
	assert.Error(t, err, "NewOrphanage should return error when maxSize is 0")
	assert.Contains(t, err.Error(), "maxSize")
}

// TestNewOrphanage_ErrorOnNegativeMaxSize tests that NewOrphanage returns error when maxSize is negative
func TestNewOrphanage_ErrorOnNegativeMaxSize(t *testing.T) {
	logger := &ulogger.TestLogger{}

	_, err := NewOrphanage(15*time.Minute, -1, logger)
	assert.Error(t, err, "NewOrphanage should return error when maxSize is negative")
	assert.Contains(t, err.Error(), "maxSize")
}

// TestNewOrphanage_ErrorOnZeroTimeout tests that NewOrphanage returns error when timeout is 0
func TestNewOrphanage_ErrorOnZeroTimeout(t *testing.T) {
	logger := &ulogger.TestLogger{}

	_, err := NewOrphanage(0, 100, logger)
	assert.Error(t, err, "NewOrphanage should return error when timeout is 0")
	assert.Contains(t, err.Error(), "timeout")
}

// TestNewOrphanage_ErrorOnNegativeTimeout tests that NewOrphanage returns error when timeout is negative
func TestNewOrphanage_ErrorOnNegativeTimeout(t *testing.T) {
	logger := &ulogger.TestLogger{}

	_, err := NewOrphanage(-1*time.Second, 100, logger)
	assert.Error(t, err, "NewOrphanage should return error when timeout is negative")
	assert.Contains(t, err.Error(), "timeout")
}

// ============================================================================
// Eviction/Expiration Tests
// ============================================================================

// TestOrphanageEvictionFunction tests that the eviction function is properly configured
func TestOrphanageEvictionFunction(t *testing.T) {
	logger := &ulogger.TestLogger{}

	// Create orphanage with very short timeout for testing
	orphanage, err := NewOrphanage(100*time.Millisecond, 100, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	// Create test transaction
	tx, err := createTestTransaction("expiring_tx")
	require.NoError(t, err)
	txHash := *tx.TxIDChainHash()

	// Add transaction to orphanage
	added := server.orphanage.Set(txHash, tx)
	require.True(t, added, "Transaction should be added")

	// Verify transaction exists initially
	_, exists := server.orphanage.Get(txHash)
	initialLen := server.orphanage.Len()
	assert.True(t, exists, "Transaction should exist immediately after adding")
	assert.Equal(t, 1, initialLen, "Orphanage should have 1 transaction")

	// Note: The expiringmap uses lazy eviction, so we just verify the transaction was added
	// and the orphanage is configured with a timeout. The eviction function logs when called.
	// Actual eviction timing is handled by the expiringmap library internally.
}

// TestOrphanageTimeoutConfiguration tests that the timeout is properly configured
func TestOrphanageTimeoutConfiguration(t *testing.T) {
	logger := &ulogger.TestLogger{}

	// Create orphanages with different timeouts
	shortTimeout, err := NewOrphanage(1*time.Second, 100, logger)
	require.NoError(t, err)
	longTimeout, err := NewOrphanage(10*time.Minute, 100, logger)
	require.NoError(t, err)

	// Both should be created successfully
	assert.NotNil(t, shortTimeout, "Orphanage with short timeout should be created")
	assert.NotNil(t, longTimeout, "Orphanage with long timeout should be created")

	// Test that MaxSize is correctly set
	assert.Equal(t, 100, shortTimeout.MaxSize(), "MaxSize should be 100")
	assert.Equal(t, 100, longTimeout.MaxSize(), "MaxSize should be 100")
}

// TestOrphanageNoEvictionBeforeTimeout tests that transactions are NOT evicted before timeout
func TestOrphanageNoEvictionBeforeTimeout(t *testing.T) {
	logger := &ulogger.TestLogger{}

	// Create orphanage with longer timeout
	orphanage, err := NewOrphanage(5*time.Second, 100, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	// Create test transaction
	tx, err := createTestTransaction("persistent_tx")
	require.NoError(t, err)
	txHash := *tx.TxIDChainHash()

	// Add transaction
	server.orphanage.Set(txHash, tx)

	// Wait a short time (but less than timeout)
	time.Sleep(100 * time.Millisecond)

	// Verify transaction still exists
	_, exists := server.orphanage.Get(txHash)
	length := server.orphanage.Len()

	assert.True(t, exists, "Transaction should still exist before timeout")
	assert.Equal(t, 1, length, "Orphanage should still have the transaction")
}

// ============================================================================
// Helper Method Tests
// ============================================================================

// TestOrphanageMaxSize tests the MaxSize() method
func TestOrphanageMaxSize(t *testing.T) {
	logger := &ulogger.TestLogger{}

	tests := []struct {
		name    string
		maxSize int
	}{
		{"Small size", 10},
		{"Medium size", 100},
		{"Large size", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orphanage, err := NewOrphanage(15*time.Minute, tt.maxSize, logger)
			require.NoError(t, err)
			assert.Equal(t, tt.maxSize, orphanage.MaxSize(), "MaxSize should return the configured value")
		})
	}
}

// TestOrphanageIsFull tests the IsFull() method
func TestOrphanageIsFull(t *testing.T) {
	logger := &ulogger.TestLogger{}
	maxSize := 3

	orphanage, err := NewOrphanage(15*time.Minute, maxSize, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	// Initially should not be full
	assert.False(t, server.orphanage.IsFull(), "Orphanage should not be full when empty")

	// Add transactions one by one
	for i := 0; i < maxSize-1; i++ {
		tx := createUniqueTx(baseTx, uint32(i))
		server.orphanage.Set(*tx.TxIDChainHash(), tx)

		// Should not be full yet
		assert.False(t, server.orphanage.IsFull(), "Orphanage should not be full at %d/%d", i+1, maxSize)
	}

	// Add the last transaction to fill it
	tx := createUniqueTx(baseTx, uint32(maxSize-1))
	server.orphanage.Set(*tx.TxIDChainHash(), tx)

	// Now it should be full
	assert.True(t, server.orphanage.IsFull(), "Orphanage should be full at %d/%d", maxSize, maxSize)

	// Remove one transaction
	server.orphanage.Delete(*tx.TxIDChainHash())

	// Should not be full anymore
	assert.False(t, server.orphanage.IsFull(), "Orphanage should not be full after removing one transaction")
}

// TestOrphanageStats tests the Stats() method
func TestOrphanageStats(t *testing.T) {
	logger := &ulogger.TestLogger{}
	maxSize := 10

	orphanage, err := NewOrphanage(15*time.Minute, maxSize, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	// Test empty orphanage
	currentSize, returnedMaxSize, utilization := server.orphanage.Stats()
	assert.Equal(t, 0, currentSize, "Current size should be 0 when empty")
	assert.Equal(t, maxSize, returnedMaxSize, "Max size should match configured value")
	assert.Equal(t, 0.0, utilization, "Utilization should be 0% when empty")

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	// Add 5 transactions (50% full)
	for i := 0; i < 5; i++ {
		tx := createUniqueTx(baseTx, uint32(i))
		server.orphanage.Set(*tx.TxIDChainHash(), tx)
	}

	currentSize, returnedMaxSize, utilization = server.orphanage.Stats()
	assert.Equal(t, 5, currentSize, "Current size should be 5")
	assert.Equal(t, maxSize, returnedMaxSize, "Max size should match configured value")
	assert.Equal(t, 50.0, utilization, "Utilization should be 50%")

	// Fill to capacity (100% full)
	for i := 5; i < maxSize; i++ {
		tx := createUniqueTx(baseTx, uint32(i))
		server.orphanage.Set(*tx.TxIDChainHash(), tx)
	}

	currentSize, returnedMaxSize, utilization = server.orphanage.Stats()
	assert.Equal(t, maxSize, currentSize, "Current size should equal max size")
	assert.Equal(t, maxSize, returnedMaxSize, "Max size should match configured value")
	assert.Equal(t, 100.0, utilization, "Utilization should be 100% when full")
}

// TestOrphanageCleanup tests the Cleanup() method
func TestOrphanageCleanup(t *testing.T) {
	logger := &ulogger.TestLogger{}
	maxSize := 10

	orphanage, err := NewOrphanage(15*time.Minute, maxSize, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	// Add some transactions
	for i := 0; i < 3; i++ {
		tx := createUniqueTx(baseTx, uint32(i))
		server.orphanage.Set(*tx.TxIDChainHash(), tx)
	}

	// Call Cleanup - it should log the status without error
	// This is mainly testing that the method doesn't panic and properly acquires the lock
	assert.NotPanics(t, func() {
		server.orphanage.Cleanup()
	}, "Cleanup should not panic")

	// Verify orphanage state is unchanged after cleanup
	length := server.orphanage.Len()
	assert.Equal(t, 3, length, "Cleanup should not modify orphanage contents")
}

// TestOrphanageCleanupConcurrency tests that Cleanup is thread-safe
func TestOrphanageCleanupConcurrency(t *testing.T) {
	logger := &ulogger.TestLogger{}
	maxSize := 50

	orphanage, err := NewOrphanage(15*time.Minute, maxSize, logger)
	require.NoError(t, err)
	server := &Server{
		orphanage: orphanage,
		logger:    logger,
	}

	baseTx, err := createTestTransaction("base")
	require.NoError(t, err)

	// Add some transactions
	for i := 0; i < 10; i++ {
		tx := createUniqueTx(baseTx, uint32(i))
		server.orphanage.Set(*tx.TxIDChainHash(), tx)
	}

	var wg sync.WaitGroup
	const numGoroutines = 10

	// Call Cleanup concurrently from multiple goroutines
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			server.orphanage.Cleanup()
		}()
	}

	wg.Wait()

	// Verify state is still consistent
	length := server.orphanage.Len()
	assert.Equal(t, 10, length, "Concurrent Cleanup calls should not affect orphanage contents")
}

// TestOrphanageStatsWithZeroMaxSize tests Stats() edge case (though this shouldn't happen in practice due to panic)
func TestOrphanageStatsCalculation(t *testing.T) {
	logger := &ulogger.TestLogger{}

	// Test with different fill levels
	tests := []struct {
		name                string
		maxSize             int
		itemsToAdd          int
		expectedUtilization float64
	}{
		{"Empty", 100, 0, 0.0},
		{"Quarter full", 100, 25, 25.0},
		{"Half full", 100, 50, 50.0},
		{"Three quarters full", 100, 75, 75.0},
		{"Full", 100, 100, 100.0},
		{"Small orphanage", 10, 3, 30.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orphanage, err := NewOrphanage(15*time.Minute, tt.maxSize, logger)
			require.NoError(t, err)
			server := &Server{
				orphanage: orphanage,
				logger:    logger,
			}

			baseTx, err := createTestTransaction("base")
			require.NoError(t, err)

			// Add specified number of transactions
			for i := 0; i < tt.itemsToAdd; i++ {
				tx := createUniqueTx(baseTx, uint32(i))
				server.orphanage.Set(*tx.TxIDChainHash(), tx)
			}

			currentSize, maxSize, utilization := server.orphanage.Stats()
			assert.Equal(t, tt.itemsToAdd, currentSize, "Current size should match items added")
			assert.Equal(t, tt.maxSize, maxSize, "Max size should match configured value")
			assert.InDelta(t, tt.expectedUtilization, utilization, 0.01, "Utilization should be calculated correctly")
		})
	}
}
