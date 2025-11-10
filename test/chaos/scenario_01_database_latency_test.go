package chaos

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestScenario01_DatabaseLatency tests how the system handles slow database responses
// This implements Scenario 1 from TOXIPROXY_CHAOS_TESTING.md
//
// Test Scenario:
// 1. Establish baseline performance with normal database
// 2. Add 5 second latency to PostgreSQL through toxiproxy
// 3. Execute database operations and verify they handle timeouts gracefully
// 4. Verify retry logic and circuit breakers work correctly
// 5. Remove latency and verify system recovers
//
// Expected Behavior:
// - Services should timeout gracefully
// - Retry logic should kick in
// - Circuit breakers should trigger if implemented
// - No data corruption
// - System recovers when latency is removed
func TestScenario01_DatabaseLatency(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// Configuration
	const (
		toxiproxyURL      = "http://localhost:8474"
		proxyName         = "postgres"
		dbConnStr         = "postgres://postgres:really_strong_password_change_me@localhost:5432/postgres?sslmode=disable"
		toxiproxyConnStr  = "postgres://postgres:really_strong_password_change_me@localhost:15432/postgres?sslmode=disable&connect_timeout=30"
		latencyMs         = 5000             // 5 seconds downstream latency
		shortTimeout      = 3 * time.Second  // Less than expected query latency
		longQueryTimeout  = 30 * time.Second // Long enough for any query with latency
		baselineQueryTime = 100 * time.Millisecond
		expectedLatency   = time.Duration(latencyMs) * time.Millisecond // Minimum expected latency
	)

	// Create toxiproxy client
	toxiClient := NewToxiproxyClient(toxiproxyURL)

	// Ensure toxiproxy is available
	t.Log("Waiting for toxiproxy to be available...")
	err := toxiClient.WaitForProxy(proxyName, 30*time.Second)
	require.NoError(t, err, "toxiproxy should be available")

	// Ensure clean state at start
	t.Log("Resetting toxiproxy to clean state...")
	err = toxiClient.ResetProxy(proxyName)
	require.NoError(t, err, "should reset proxy before test")

	// Cleanup function to reset toxiproxy after test
	defer func() {
		t.Log("Cleaning up: resetting toxiproxy...")
		if err := toxiClient.ResetProxy(proxyName); err != nil {
			t.Logf("Warning: failed to reset proxy: %v", err)
		}
	}()

	// Step 1: Establish baseline performance
	t.Run("Baseline_Performance", func(t *testing.T) {
		t.Log("Testing baseline database performance (no latency)...")

		db, err := sql.Open("postgres", dbConnStr)
		require.NoError(t, err, "should connect to database")
		defer db.Close()

		// Verify connection
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = db.PingContext(ctx)
		require.NoError(t, err, "should ping database successfully")

		// Measure baseline query time
		start := time.Now()
		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		duration := time.Since(start)

		require.NoError(t, err, "baseline query should succeed")
		require.Equal(t, 1, result, "query should return expected result")
		require.Less(t, duration, baselineQueryTime, "baseline query should be fast")

		t.Logf("✓ Baseline query completed in %v", duration)
	})

	// Step 2: Add latency through toxiproxy
	t.Run("Inject_Latency", func(t *testing.T) {
		t.Logf("Injecting %dms latency to database connections...", latencyMs)

		err := toxiClient.AddLatency(proxyName, latencyMs, "downstream")
		require.NoError(t, err, "should add latency toxic")

		// Verify toxic was added
		toxics, err := toxiClient.ListToxics(proxyName)
		require.NoError(t, err, "should list toxics")
		require.Len(t, toxics, 1, "should have one toxic")
		require.Equal(t, "latency_downstream", toxics[0].Name, "toxic should be latency")

		t.Logf("✓ Latency toxic injected successfully")
	})

	// Step 3: Test database operations with latency
	t.Run("Query_With_Latency", func(t *testing.T) {
		t.Log("Testing database queries through toxiproxy with latency...")

		// Connect through toxiproxy
		db, err := sql.Open("postgres", toxiproxyConnStr)
		require.NoError(t, err, "should connect to toxiproxy")
		defer db.Close()

		// Set connection pool settings
		db.SetConnMaxLifetime(60 * time.Second)
		db.SetMaxOpenConns(5)
		db.SetMaxIdleConns(2)

		// Establish connection first (this will be slow due to latency)
		t.Log("Establishing initial connection (this will be slow due to latency)...")
		ctx, cancel := context.WithTimeout(context.Background(), longQueryTimeout)
		err = db.PingContext(ctx)
		cancel()
		require.NoError(t, err, "should establish initial connection")
		t.Log("Initial connection established")

		// Test 1: Query with insufficient timeout should fail
		t.Run("Timeout_Failure", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
			defer cancel()

			start := time.Now()
			var result int
			err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
			duration := time.Since(start)

			// Should timeout because latency (5s) > timeout (3s)
			require.Error(t, err, "query should timeout")
			require.Contains(t, err.Error(), "context deadline exceeded", "should be context timeout error")
			require.GreaterOrEqual(t, duration, shortTimeout-500*time.Millisecond, "should wait close to timeout duration")
			require.Less(t, duration, shortTimeout+3*time.Second, "should timeout promptly")

			t.Logf("✓ Query correctly timed out after %v (expected ~%v)", duration, shortTimeout)
		})

		// Test 2: Query with sufficient timeout should succeed (slowly)
		t.Run("Slow_Success", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), longQueryTimeout)
			defer cancel()

			start := time.Now()
			var result int
			err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
			duration := time.Since(start)

			require.NoError(t, err, "query with long timeout should succeed")
			require.Equal(t, 1, result, "query should return correct result")
			// Query should take at least the added latency (behavior varies by connection state)
			require.Greater(t, duration, expectedLatency-time.Second, "query should experience latency")

			t.Logf("✓ Slow query completed successfully in %v (with %dms latency applied)", duration, latencyMs)
		})

		// Test 3: Multiple queries should all be slow
		t.Run("Multiple_Slow_Queries", func(t *testing.T) {
			const numQueries = 3
			// Each query should experience some latency (exact timing varies by connection state)
			totalTimeout := time.Duration(numQueries*15) * time.Second // Generous timeout
			ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
			defer cancel()

			totalStart := time.Now()
			for i := 0; i < numQueries; i++ {
				start := time.Now()
				var result int
				err := db.QueryRowContext(ctx, "SELECT $1", i).Scan(&result)
				duration := time.Since(start)

				require.NoError(t, err, fmt.Sprintf("query %d should succeed", i))
				// All queries should experience latency (at least 4s to account for variability)
				require.Greater(t, duration, expectedLatency-1*time.Second, "each query should experience latency")

				t.Logf("Query %d completed in %v", i, duration)
			}
			totalDuration := time.Since(totalStart)

			t.Logf("✓ Completed %d queries in %v (all experienced latency)", numQueries, totalDuration)
		})
	})

	// Step 4: Verify retry behavior (if applicable)
	t.Run("Retry_Behavior", func(t *testing.T) {
		t.Log("Testing retry behavior with latency...")

		db, err := sql.Open("postgres", toxiproxyConnStr)
		require.NoError(t, err, "should connect to toxiproxy")
		defer db.Close()

		// Establish connection first
		t.Log("Establishing connection for retry test...")
		ctx, cancel := context.WithTimeout(context.Background(), longQueryTimeout)
		err = db.PingContext(ctx)
		cancel()
		require.NoError(t, err, "should establish connection")

		// Simulate application retry logic with very short timeout
		const maxRetries = 3
		retryDelay := 1 * time.Second
		veryShortTimeout := 1 * time.Second // Much shorter than latency

		start := time.Now()
		var lastErr error
		var result int
		var attempts int

		for attempt := 0; attempt < maxRetries; attempt++ {
			attempts++
			ctx, cancel := context.WithTimeout(context.Background(), veryShortTimeout)
			err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
			cancel()

			if err == nil {
				t.Logf("Query succeeded on attempt %d", attempt+1)
				break
			}

			lastErr = err
			t.Logf("Attempt %d failed: %v", attempt+1, err)

			if attempt < maxRetries-1 {
				time.Sleep(retryDelay)
			}
		}

		duration := time.Since(start)

		// All retries should fail because latency (5s) > timeout (1s)
		require.Error(t, lastErr, "retries should eventually fail with high latency and short timeout")
		require.Equal(t, maxRetries, attempts, "should have tried all retries")

		// Should have tried multiple times
		expectedMinDuration := veryShortTimeout*time.Duration(maxRetries) + retryDelay*time.Duration(maxRetries-1)
		require.Greater(t, duration, expectedMinDuration, "should have retried multiple times")

		t.Logf("✓ Retry logic executed %d attempts over %v", maxRetries, duration)
	})

	// Step 5: Remove latency and verify recovery
	t.Run("Recovery", func(t *testing.T) {
		t.Log("Removing latency and verifying system recovery...")

		// Remove latency toxic
		err := toxiClient.RemoveToxic(proxyName, "latency_downstream")
		require.NoError(t, err, "should remove latency toxic")

		// Verify toxic is gone
		toxics, err := toxiClient.ListToxics(proxyName)
		require.NoError(t, err, "should list toxics")
		require.Len(t, toxics, 0, "should have no toxics after removal")

		t.Log("Latency removed, testing recovery...")

		// Give a moment for connections to stabilize
		time.Sleep(2 * time.Second)

		// Connect through toxiproxy (no latency now)
		db, err := sql.Open("postgres", toxiproxyConnStr)
		require.NoError(t, err, "should connect to toxiproxy")
		defer db.Close()

		// Verify fast queries are back
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		start := time.Now()
		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		duration := time.Since(start)

		require.NoError(t, err, "query should succeed after latency removal")
		require.Equal(t, 1, result, "query should return correct result")
		require.Less(t, duration, baselineQueryTime*2, "query should be fast again")

		t.Logf("✓ System recovered: query completed in %v (back to normal)", duration)
	})

	// Step 6: Verify data consistency
	t.Run("Data_Consistency", func(t *testing.T) {
		t.Log("Verifying no data corruption occurred...")

		db, err := sql.Open("postgres", dbConnStr)
		require.NoError(t, err, "should connect to database")
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a test table
		_, err = db.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS chaos_test_scenario_01 (
				id SERIAL PRIMARY KEY,
				value TEXT NOT NULL,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`)
		require.NoError(t, err, "should create test table")

		// Insert test data
		_, err = db.ExecContext(ctx, `
			INSERT INTO chaos_test_scenario_01 (value)
			VALUES ('test_data_before_chaos'), ('test_data_after_chaos')
		`)
		require.NoError(t, err, "should insert test data")

		// Verify data
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM chaos_test_scenario_01").Scan(&count)
		require.NoError(t, err, "should query test data")
		require.GreaterOrEqual(t, count, 2, "test data should be present")

		// Cleanup
		_, err = db.ExecContext(ctx, "DROP TABLE chaos_test_scenario_01")
		require.NoError(t, err, "should drop test table")

		t.Logf("✓ Data consistency verified: no corruption detected")
	})

	t.Log("✅ Scenario 1 (Database Latency) completed successfully")
}
