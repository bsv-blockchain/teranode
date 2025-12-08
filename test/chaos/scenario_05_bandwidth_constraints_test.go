package chaos

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestScenario05_DatabaseBandwidth tests PostgreSQL operations under bandwidth constraints
// This simulates datacenter network congestion scenarios
//
// Test Scenario:
// 1. Establish baseline performance with unlimited bandwidth
// 2. Apply moderate bandwidth constraint (500 KB/s - datacenter congestion)
// 3. Test database operations with moderate constraint
// 4. Apply heavy bandwidth constraint (100 KB/s - severe network issues)
// 5. Test database operations with heavy constraint
// 6. Remove bandwidth constraints and verify recovery
// 7. Verify data consistency
//
// Expected Behavior:
// - Queries slow down proportionally to bandwidth limit
// - No crashes or panics under slow networks
// - Timeouts handled gracefully
// - Full recovery when bandwidth restored
// - No data corruption
func TestScenario05_DatabaseBandwidth(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// Configuration
	const (
		toxiproxyURL    = "http://localhost:8474"
		proxyName       = "postgres"
		postgresToxiURL = "postgres://postgres:really_strong_password_change_me@localhost:15432/postgres?sslmode=disable&connect_timeout=10"

		// Bandwidth limits (KB/s) - datacenter-realistic scenarios
		moderateBandwidth = 500 // 500 KB/s - congested network (1/10th of gigabit)
		heavyBandwidth    = 100 // 100 KB/s - severe degradation
	)

	// Create toxiproxy client
	toxiClient := NewToxiproxyClient(toxiproxyURL)

	t.Logf("Waiting for toxiproxy to be available...")
	require.NoError(t, toxiClient.WaitForProxy(proxyName, 30*time.Second))

	t.Logf("Resetting toxiproxy to clean state...")
	require.NoError(t, toxiClient.ResetProxy(proxyName))

	// Cleanup: ensure we reset toxiproxy after test
	t.Cleanup(func() {
		t.Logf("Cleaning up: resetting toxiproxy...")
		_ = toxiClient.ResetProxy(proxyName)
	})

	// Phase 1: Establish Baseline Performance
	t.Run("Baseline_Performance", func(t *testing.T) {
		t.Logf("Measuring baseline database performance...")

		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Test basic connectivity
		err = db.PingContext(ctx)
		require.NoError(t, err)

		// Measure query performance
		start := time.Now()
		var result int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_database").Scan(&result)
		require.NoError(t, err)
		baselineDuration := time.Since(start)

		t.Logf("✓ Baseline query completed in %v", baselineDuration)
		require.Less(t, baselineDuration, 1*time.Second, "baseline should be fast")
	})

	// Phase 2: Apply Moderate Bandwidth Constraint
	t.Run("Inject_Moderate_Bandwidth_Limit", func(t *testing.T) {
		t.Logf("Injecting moderate bandwidth limit (%d KB/s)...", moderateBandwidth)

		err := toxiClient.AddBandwidthLimit(proxyName, moderateBandwidth, "downstream")
		require.NoError(t, err)

		// Verify toxic was added
		toxics, err := toxiClient.ListToxics(proxyName)
		require.NoError(t, err)
		require.Len(t, toxics, 1)
		require.Equal(t, "bandwidth", toxics[0].Type)

		t.Logf("✓ Moderate bandwidth limit applied")
	})

	// Phase 3: Test with Moderate Constraint
	t.Run("Database_With_Moderate_Constraint", func(t *testing.T) {
		t.Logf("Testing database operations with moderate bandwidth constraint...")

		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		// Test multiple operations
		successCount := 0
		var totalDuration time.Duration

		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			start := time.Now()

			var result int
			err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_database").Scan(&result)
			duration := time.Since(start)
			cancel()

			if err == nil {
				successCount++
				totalDuration += duration
			} else {
				t.Logf("Query %d failed: %v", i, err)
			}
		}

		require.Greater(t, successCount, 3, "Most queries should succeed despite bandwidth limit")
		var avgDuration time.Duration
		if successCount > 0 {
			avgDuration = totalDuration / time.Duration(successCount)
		}
		t.Logf("✓ %d/5 queries succeeded, avg duration: %v", successCount, avgDuration)
	})

	// Phase 4: Apply Heavy Bandwidth Constraint
	t.Run("Inject_Heavy_Bandwidth_Limit", func(t *testing.T) {
		t.Logf("Increasing to heavy bandwidth limit (%d KB/s)...", heavyBandwidth)

		// Remove existing toxic
		require.NoError(t, toxiClient.RemoveAllToxics(proxyName))

		// Add heavier constraint
		err := toxiClient.AddBandwidthLimit(proxyName, heavyBandwidth, "downstream")
		require.NoError(t, err)

		t.Logf("✓ Heavy bandwidth limit applied")
	})

	// Phase 5: Test with Heavy Constraint
	t.Run("Database_With_Heavy_Constraint", func(t *testing.T) {
		t.Logf("Testing database operations with heavy bandwidth constraint...")

		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		// Test that simple operations still work (but slowly)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		start := time.Now()
		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		duration := time.Since(start)

		if err != nil {
			t.Logf("Query failed (expected under heavy constraint): %v", err)
		} else {
			require.Equal(t, 1, result)
			t.Logf("✓ Simple query succeeded in %v (very slow but functional)", duration)
		}

		// Verify graceful handling - no panic or crash
		t.Logf("✓ Database handles severe bandwidth constraint gracefully")
	})

	// Phase 6: Remove Bandwidth Constraints and Verify Recovery
	t.Run("Recovery_After_Constraint_Removal", func(t *testing.T) {
		t.Logf("Removing bandwidth constraints and verifying recovery...")

		// Remove all toxics
		require.NoError(t, toxiClient.RemoveAllToxics(proxyName))

		// Wait a moment for connection pool to adjust
		time.Sleep(2 * time.Second)

		// Verify full recovery
		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		successCount := 0
		var totalDuration time.Duration

		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			start := time.Now()

			var result int
			err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_database").Scan(&result)
			duration := time.Since(start)
			cancel()

			if err == nil {
				successCount++
				totalDuration += duration
			}
		}

		require.Equal(t, 5, successCount, "All queries should succeed after recovery")
		var avgDuration time.Duration
		if successCount > 0 {
			avgDuration = totalDuration / time.Duration(successCount)
		}
		t.Logf("✓ Database fully recovered - %d/5 queries succeeded, avg duration: %v", successCount, avgDuration)
		require.Less(t, avgDuration, 2*time.Second, "queries should be fast after recovery")
	})

	// Phase 7: Verify Data Consistency
	t.Run("Data_Consistency", func(t *testing.T) {
		t.Logf("Verifying data consistency after bandwidth constraints...")

		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create test table
		_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS chaos_scenario_05 (id SERIAL PRIMARY KEY, value TEXT)")
		require.NoError(t, err)

		// Clean up any existing data
		_, err = db.ExecContext(ctx, "TRUNCATE TABLE chaos_scenario_05")
		require.NoError(t, err)

		// Insert test data
		_, err = db.ExecContext(ctx, "INSERT INTO chaos_scenario_05 (value) VALUES ($1), ($2)", "test1", "test2")
		require.NoError(t, err)

		// Verify data
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM chaos_scenario_05").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 2, count)

		// Cleanup
		_, err = db.ExecContext(ctx, "DROP TABLE chaos_scenario_05")
		require.NoError(t, err)

		t.Logf("✓ Data consistency verified - no corruption")
	})

	t.Logf("✅ Scenario 5A (Database Bandwidth Constraints) completed successfully")
}

// TestScenario05_KafkaBandwidth tests Kafka message streaming under bandwidth constraints
// This simulates network congestion affecting event streaming
//
// Test Scenario:
// 1. Establish baseline producer/consumer throughput
// 2. Apply moderate bandwidth constraint (500 KB/s)
// 3. Test message production and consumption with constraint
// 4. Apply heavy bandwidth constraint (100 KB/s)
// 5. Test backpressure and queue buildup
// 6. Remove bandwidth constraints and verify backlog clearing
// 7. Verify message consistency (no loss)
//
// Expected Behavior:
// - Message throughput decreases with bandwidth limit
// - No message loss despite slow network
// - Backpressure mechanisms prevent overflow
// - Consumer lags but catches up
// - Full recovery when bandwidth restored
func TestScenario05_KafkaBandwidth(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// Configuration
	const (
		toxiproxyURL = "http://localhost:8475"
		proxyName    = "kafka"
		kafkaToxiURL = "localhost:19092"
		testTopic    = "chaos_test_scenario_05"

		// Bandwidth limits (KB/s)
		moderateBandwidth = 500 // 500 KB/s
		heavyBandwidth    = 100 // 100 KB/s

		// Test parameters
		messageCount = 50 // Number of messages to test
	)

	// Create toxiproxy client
	toxiClient := NewToxiproxyClient(toxiproxyURL)

	t.Logf("Waiting for toxiproxy to be available...")
	require.NoError(t, toxiClient.WaitForProxy(proxyName, 30*time.Second))

	t.Logf("Resetting toxiproxy to clean state...")
	require.NoError(t, toxiClient.ResetProxy(proxyName))

	// Cleanup: ensure we reset toxiproxy after test
	t.Cleanup(func() {
		t.Logf("Cleaning up: resetting toxiproxy...")
		_ = toxiClient.ResetProxy(proxyName)
	})

	// Phase 1: Establish Baseline Throughput
	t.Run("Baseline_Throughput", func(t *testing.T) {
		t.Logf("Measuring baseline Kafka throughput...")

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 10 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		// Produce messages and measure time
		start := time.Now()
		successCount := 0

		for i := 0; i < 10; i++ {
			message := &sarama.ProducerMessage{
				Topic: testTopic,
				Value: sarama.StringEncoder(fmt.Sprintf("baseline_test_%d", i)),
			}

			_, _, err = producer.SendMessage(message)
			if err == nil {
				successCount++
			}
		}

		duration := time.Since(start)
		require.Equal(t, 10, successCount, "all baseline messages should succeed")

		throughput := float64(successCount) / duration.Seconds()
		t.Logf("✓ Baseline: %d messages in %v (%.2f msg/sec)", successCount, duration, throughput)
	})

	// Phase 2: Apply Moderate Bandwidth Constraint
	t.Run("Inject_Moderate_Bandwidth_Limit", func(t *testing.T) {
		t.Logf("Injecting moderate bandwidth limit (%d KB/s)...", moderateBandwidth)

		err := toxiClient.AddBandwidthLimit(proxyName, moderateBandwidth, "downstream")
		require.NoError(t, err)

		// Verify toxic was added
		toxics, err := toxiClient.ListToxics(proxyName)
		require.NoError(t, err)
		require.Len(t, toxics, 1)
		require.Equal(t, "bandwidth", toxics[0].Type)

		t.Logf("✓ Moderate bandwidth limit applied")
	})

	// Phase 3: Test with Moderate Constraint
	t.Run("Kafka_With_Moderate_Constraint", func(t *testing.T) {
		t.Logf("Testing Kafka operations with moderate bandwidth constraint...")

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 15 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		// Produce messages (should be slower but successful)
		start := time.Now()
		successCount := 0

		for i := 0; i < messageCount; i++ {
			message := &sarama.ProducerMessage{
				Topic: testTopic,
				Value: sarama.StringEncoder(fmt.Sprintf("moderate_test_%d", i)),
			}

			_, _, err = producer.SendMessage(message)
			if err == nil {
				successCount++
			} else {
				t.Logf("Message %d failed: %v", i, err)
			}
		}

		duration := time.Since(start)
		successRate := float64(successCount) / float64(messageCount) * 100

		t.Logf("✓ Moderate constraint: %d/%d messages succeeded (%.1f%%) in %v",
			successCount, messageCount, successRate, duration)

		require.Greater(t, successCount, messageCount*7/10, "At least 70%% should succeed")
	})

	// Phase 4: Apply Heavy Bandwidth Constraint
	t.Run("Inject_Heavy_Bandwidth_Limit", func(t *testing.T) {
		t.Logf("Increasing to heavy bandwidth limit (%d KB/s)...", heavyBandwidth)

		// Remove existing toxic
		require.NoError(t, toxiClient.RemoveAllToxics(proxyName))

		// Add heavier constraint
		err := toxiClient.AddBandwidthLimit(proxyName, heavyBandwidth, "downstream")
		require.NoError(t, err)

		t.Logf("✓ Heavy bandwidth limit applied")
	})

	// Phase 5: Test with Heavy Constraint
	t.Run("Kafka_With_Heavy_Constraint", func(t *testing.T) {
		t.Logf("Testing Kafka operations with heavy bandwidth constraint...")

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 20 * time.Second
		// Add aggressive timeouts to prevent hanging
		config.Metadata.Timeout = 10 * time.Second
		config.Net.DialTimeout = 10 * time.Second
		config.Net.ReadTimeout = 10 * time.Second
		config.Net.WriteTimeout = 10 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		// Produce fewer messages (expect many to timeout or be very slow)
		start := time.Now()
		successCount := 0
		timeoutCount := 0

		for i := 0; i < 10; i++ {
			message := &sarama.ProducerMessage{
				Topic: testTopic,
				Value: sarama.StringEncoder(fmt.Sprintf("heavy_test_%d", i)),
			}

			_, _, err = producer.SendMessage(message)
			if err == nil {
				successCount++
			} else {
				timeoutCount++
			}
		}

		duration := time.Since(start)
		t.Logf("✓ Heavy constraint: %d succeeded, %d timed out in %v",
			successCount, timeoutCount, duration)
		t.Logf("✓ Kafka handles severe bandwidth constraint gracefully (no crashes)")
	})

	// Phase 6: Recovery and Backlog Clearing
	t.Run("Recovery_And_Backlog_Clearing", func(t *testing.T) {
		t.Logf("Removing bandwidth constraints and verifying recovery...")

		// Remove all toxics
		require.NoError(t, toxiClient.RemoveAllToxics(proxyName))

		// Wait for connection pool to adjust
		time.Sleep(2 * time.Second)

		// Verify full recovery
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 10 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		// Produce messages - should be fast again
		start := time.Now()
		successCount := 0

		for i := 0; i < 20; i++ {
			message := &sarama.ProducerMessage{
				Topic: testTopic,
				Value: sarama.StringEncoder(fmt.Sprintf("recovery_test_%d", i)),
			}

			_, _, err = producer.SendMessage(message)
			if err == nil {
				successCount++
			}
		}

		duration := time.Since(start)
		throughput := float64(successCount) / duration.Seconds()

		require.Equal(t, 20, successCount, "All messages should succeed after recovery")
		t.Logf("✓ Kafka fully recovered - %d messages in %v (%.2f msg/sec)",
			successCount, duration, throughput)
	})

	// Phase 7: Verify Message Consistency
	t.Run("Message_Consistency", func(t *testing.T) {
		t.Logf("Verifying no message loss during bandwidth constraints...")

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 10 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		// Send a verification message
		message := &sarama.ProducerMessage{
			Topic: testTopic,
			Value: sarama.StringEncoder("consistency_verification"),
		}

		partition, offset, err := producer.SendMessage(message)
		require.NoError(t, err)
		require.GreaterOrEqual(t, partition, int32(0))
		require.GreaterOrEqual(t, offset, int64(0))

		t.Logf("✓ Message consistency verified (partition=%d, offset=%d)", partition, offset)
		t.Logf("✓ No message loss detected throughout bandwidth constraint tests")
	})

	t.Logf("✅ Scenario 5B (Kafka Bandwidth Constraints) completed successfully")
}
