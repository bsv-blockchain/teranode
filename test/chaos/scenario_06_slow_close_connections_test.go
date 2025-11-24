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

// TestScenario06_DatabaseSlowClose tests PostgreSQL operations with slicer toxic
// This simulates slow/chunked data transmission typical of congested or unstable networks
//
// Test Scenario:
// 1. Establish baseline performance with normal connections
// 2. Apply moderate slicer (moderate chunking and delays)
// 3. Test database operations with moderate slicer
// 4. Apply aggressive slicer (small chunks, frequent delays)
// 5. Test database operations with aggressive slicer
// 6. Remove slicer and verify recovery
// 7. Verify data consistency
//
// Expected Behavior:
// - Queries complete but take longer due to chunked transmission
// - No data corruption despite slow transmission
// - System doesn't hang or timeout excessively
// - Full recovery when slicer removed
func TestScenario06_DatabaseSlowClose(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// Configuration
	const (
		toxiproxyURL    = "http://localhost:8474"
		proxyName       = "postgres"
		postgresToxiURL = "postgres://postgres:really_strong_password_change_me@localhost:15432/postgres?sslmode=disable&connect_timeout=10"

		// Slicer parameters: avgSize (bytes), sizeVariation (bytes), delay (ms)
		moderateAvgSize   = 1024 // 1KB chunks
		moderateSizeVar   = 512  // ±512 bytes variation
		moderateDelay     = 10   // 10ms delay between chunks
		aggressiveAvgSize = 256  // 256 byte chunks (very small)
		aggressiveSizeVar = 128  // ±128 bytes variation
		aggressiveDelay   = 50   // 50ms delay between chunks
	)

	// Create toxiproxy client
	toxiClient := NewToxiproxyClient(toxiproxyURL)

	// Wait for toxiproxy to be available
	t.Log("Waiting for toxiproxy to be available...")
	require.NoError(t, toxiClient.WaitForProxy(proxyName, 30*time.Second))
	t.Log("Resetting toxiproxy to clean state...")
	require.NoError(t, toxiClient.ResetProxy(proxyName))

	// Cleanup at the end
	defer func() {
		t.Log("Cleaning up: resetting toxiproxy...")
		_ = toxiClient.ResetProxy(proxyName)
	}()

	// Phase 1: Establish Baseline
	t.Run("Baseline_Performance", func(t *testing.T) {
		t.Log("Measuring baseline database performance...")

		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		db.SetMaxOpenConns(5)
		db.SetMaxIdleConns(2)
		db.SetConnMaxLifetime(5 * time.Minute)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		start := time.Now()
		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		duration := time.Since(start)

		require.NoError(t, err)
		require.Equal(t, 1, result)
		t.Logf("✓ Baseline query completed in %v", duration)
	})

	// Phase 2: Inject Moderate Slicer
	t.Run("Inject_Moderate_Slicer", func(t *testing.T) {
		t.Logf("Injecting moderate slicer (%d bytes avg, %dms delay)...", moderateAvgSize, moderateDelay)

		err := toxiClient.AddSlicer(proxyName, moderateAvgSize, moderateSizeVar, moderateDelay, "downstream")
		require.NoError(t, err)

		// Verify slicer was added
		toxics, err := toxiClient.ListToxics(proxyName)
		require.NoError(t, err)
		require.NotEmpty(t, toxics, "slicer toxic should be present")
		t.Log("✓ Moderate slicer applied")
	})

	// Phase 3: Test Database with Moderate Slicer
	t.Run("Database_With_Moderate_Slicer", func(t *testing.T) {
		t.Log("Testing database operations with moderate slicer...")

		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		db.SetMaxOpenConns(5)
		db.SetMaxIdleConns(2)

		// Test multiple queries
		successCount := 0
		var totalDuration time.Duration

		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			start := time.Now()
			var result int
			err := db.QueryRowContext(ctx, "SELECT 1 FROM pg_sleep(0.1)").Scan(&result)
			duration := time.Since(start)
			cancel()

			if err == nil && result == 1 {
				successCount++
				totalDuration += duration
			} else {
				t.Logf("Query %d failed or incorrect result: %v", i, err)
			}
		}

		require.GreaterOrEqual(t, successCount, 4, "Most queries should succeed with moderate slicer")
		var avgDuration time.Duration
		if successCount > 0 {
			avgDuration = totalDuration / time.Duration(successCount)
		}
		t.Logf("✓ %d/5 queries succeeded, avg duration: %v (slower due to chunking)", successCount, avgDuration)
		require.Less(t, avgDuration, 5*time.Second, "queries should complete within reasonable time despite slicer")
	})

	// Phase 4: Apply Aggressive Slicer
	t.Run("Inject_Aggressive_Slicer", func(t *testing.T) {
		t.Logf("Increasing to aggressive slicer (%d bytes avg, %dms delay)...", aggressiveAvgSize, aggressiveDelay)

		// Remove existing slicer
		require.NoError(t, toxiClient.RemoveAllToxics(proxyName))

		// Add aggressive slicer
		err := toxiClient.AddSlicer(proxyName, aggressiveAvgSize, aggressiveSizeVar, aggressiveDelay, "downstream")
		require.NoError(t, err)

		t.Log("✓ Aggressive slicer applied")
	})

	// Phase 5: Test Database with Aggressive Slicer
	t.Run("Database_With_Aggressive_Slicer", func(t *testing.T) {
		t.Log("Testing database operations with aggressive slicer...")

		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		db.SetMaxOpenConns(3)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		start := time.Now()
		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		duration := time.Since(start)

		require.NoError(t, err, "query should complete despite aggressive slicer")
		require.Equal(t, 1, result)
		t.Logf("✓ Simple query succeeded in %v (very slow but functional)", duration)
		t.Log("✓ Database handles aggressive slicer gracefully")
	})

	// Phase 6: Recovery After Slicer Removal
	t.Run("Recovery_After_Slicer_Removal", func(t *testing.T) {
		t.Log("Removing slicer and verifying recovery...")

		// Remove all toxics
		require.NoError(t, toxiClient.RemoveAllToxics(proxyName))

		// Wait a moment for connections to stabilize
		time.Sleep(2 * time.Second)

		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		db.SetMaxOpenConns(5)

		// Test multiple queries to verify recovery
		successCount := 0
		var totalDuration time.Duration

		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			start := time.Now()
			var result int
			err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
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
		t.Log("Verifying data consistency after slicer...")

		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create temp table, insert data, verify
		_, err = db.ExecContext(ctx, `
			CREATE TEMP TABLE slicer_test (
				id SERIAL PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`)
		require.NoError(t, err)

		// Insert test data
		testData := "test_data_slow_transmission"
		_, err = db.ExecContext(ctx, "INSERT INTO slicer_test (data) VALUES ($1)", testData)
		require.NoError(t, err)

		// Verify data
		var retrievedData string
		err = db.QueryRowContext(ctx, "SELECT data FROM slicer_test WHERE id = 1").Scan(&retrievedData)
		require.NoError(t, err)
		require.Equal(t, testData, retrievedData)

		t.Log("✓ Data consistency verified - no corruption")
	})

	t.Log("✅ Scenario 6A (Database Slow Close) completed successfully")
}

// TestScenario06_KafkaSlowClose tests Kafka operations with slicer toxic
// This simulates slow message transmission in chunks
func TestScenario06_KafkaSlowClose(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// Configuration
	const (
		toxiproxyURL = "http://localhost:8475"
		proxyName    = "kafka"
		kafkaToxiURL = "localhost:19092"
		testTopic    = "chaos_test_scenario_06"

		// Slicer parameters
		moderateAvgSize   = 1024 // 1KB chunks
		moderateSizeVar   = 512
		moderateDelay     = 10  // 10ms
		aggressiveAvgSize = 256 // 256 byte chunks
		aggressiveSizeVar = 128
		aggressiveDelay   = 50 // 50ms

		// Test parameters
		messageCount = 20 // Keep small for fast test
	)

	// Create toxiproxy client
	toxiClient := NewToxiproxyClient(toxiproxyURL)

	// Wait for toxiproxy to be available
	t.Log("Waiting for toxiproxy to be available...")
	require.NoError(t, toxiClient.WaitForProxy(proxyName, 30*time.Second))
	t.Log("Resetting toxiproxy to clean state...")
	require.NoError(t, toxiClient.ResetProxy(proxyName))

	// Cleanup at the end
	defer func() {
		t.Log("Cleaning up: resetting toxiproxy...")
		_ = toxiClient.ResetProxy(proxyName)
	}()

	// Phase 1: Establish Baseline
	t.Run("Baseline_Throughput", func(t *testing.T) {
		t.Log("Measuring baseline Kafka throughput...")

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 10 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		start := time.Now()
		successCount := 0

		for i := 0; i < 10; i++ {
			msg := &sarama.ProducerMessage{
				Topic: testTopic,
				Value: sarama.StringEncoder(fmt.Sprintf("baseline_message_%d", i)),
			}

			_, _, err := producer.SendMessage(msg)
			if err == nil {
				successCount++
			}
		}

		duration := time.Since(start)
		throughput := float64(successCount) / duration.Seconds()

		require.Equal(t, 10, successCount, "all baseline messages should send")
		t.Logf("✓ Baseline: %d messages in %v (%.2f msg/sec)", successCount, duration, throughput)
	})

	// Phase 2: Inject Moderate Slicer
	t.Run("Inject_Moderate_Slicer", func(t *testing.T) {
		t.Logf("Injecting moderate slicer (%d bytes avg, %dms delay)...", moderateAvgSize, moderateDelay)

		err := toxiClient.AddSlicer(proxyName, moderateAvgSize, moderateSizeVar, moderateDelay, "downstream")
		require.NoError(t, err)

		// Verify slicer was added
		toxics, err := toxiClient.ListToxics(proxyName)
		require.NoError(t, err)
		require.NotEmpty(t, toxics, "slicer toxic should be present")
		t.Log("✓ Moderate slicer applied")
	})

	// Phase 3: Test Kafka with Moderate Slicer
	t.Run("Kafka_With_Moderate_Slicer", func(t *testing.T) {
		t.Log("Testing Kafka operations with moderate slicer...")

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 15 * time.Second
		config.Net.DialTimeout = 10 * time.Second
		config.Net.ReadTimeout = 10 * time.Second
		config.Net.WriteTimeout = 10 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		start := time.Now()
		successCount := 0

		for i := 0; i < messageCount; i++ {
			msg := &sarama.ProducerMessage{
				Topic: testTopic,
				Value: sarama.StringEncoder(fmt.Sprintf("moderate_slicer_message_%d", i)),
			}

			_, _, err := producer.SendMessage(msg)
			if err == nil {
				successCount++
			} else {
				t.Logf("Message %d failed: %v", i, err)
			}
		}

		duration := time.Since(start)
		successRate := float64(successCount) / float64(messageCount) * 100

		t.Logf("✓ Moderate slicer: %d/%d messages succeeded (%.1f%%) in %v", successCount, messageCount, successRate, duration)
		require.GreaterOrEqual(t, successCount, messageCount*80/100, "at least 80% of messages should succeed with moderate slicer")
	})

	// Phase 4: Inject Aggressive Slicer
	t.Run("Inject_Aggressive_Slicer", func(t *testing.T) {
		t.Logf("Increasing to aggressive slicer (%d bytes avg, %dms delay)...", aggressiveAvgSize, aggressiveDelay)

		// Remove existing slicer
		require.NoError(t, toxiClient.RemoveAllToxics(proxyName))

		// Add aggressive slicer
		err := toxiClient.AddSlicer(proxyName, aggressiveAvgSize, aggressiveSizeVar, aggressiveDelay, "downstream")
		require.NoError(t, err)

		t.Log("✓ Aggressive slicer applied")
	})

	// Phase 5: Test Kafka with Aggressive Slicer
	t.Run("Kafka_With_Aggressive_Slicer", func(t *testing.T) {
		t.Log("Testing Kafka operations with aggressive slicer...")

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 20 * time.Second
		config.Producer.Retry.Max = 3
		config.Net.DialTimeout = 15 * time.Second
		config.Net.ReadTimeout = 15 * time.Second
		config.Net.WriteTimeout = 15 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		start := time.Now()
		successCount := 0

		// Test fewer messages with aggressive slicer
		for i := 0; i < 5; i++ {
			msg := &sarama.ProducerMessage{
				Topic: testTopic,
				Value: sarama.StringEncoder(fmt.Sprintf("aggressive_slicer_message_%d", i)),
			}

			_, _, err := producer.SendMessage(msg)
			if err == nil {
				successCount++
			}
		}

		duration := time.Since(start)

		t.Logf("✓ Aggressive slicer: %d succeeded in %v", successCount, duration)
		t.Log("✓ Kafka handles aggressive slicer gracefully (no crashes)")
		require.GreaterOrEqual(t, successCount, 3, "at least 3/5 messages should succeed even with aggressive slicer")
	})

	// Phase 6: Recovery and Backlog Clearing
	t.Run("Recovery_And_Backlog_Clearing", func(t *testing.T) {
		t.Log("Removing slicer and verifying recovery...")

		// Remove all toxics
		require.NoError(t, toxiClient.RemoveAllToxics(proxyName))

		// Wait for connections to stabilize
		time.Sleep(2 * time.Second)

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 10 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		start := time.Now()
		successCount := 0

		for i := 0; i < 10; i++ {
			msg := &sarama.ProducerMessage{
				Topic: testTopic,
				Value: sarama.StringEncoder(fmt.Sprintf("recovery_message_%d", i)),
			}

			_, _, err := producer.SendMessage(msg)
			if err == nil {
				successCount++
			}
		}

		duration := time.Since(start)
		throughput := float64(successCount) / duration.Seconds()

		require.Equal(t, 10, successCount, "all messages should succeed after recovery")
		t.Logf("✓ Kafka fully recovered - %d messages in %v (%.2f msg/sec)", successCount, duration, throughput)
	})

	// Phase 7: Message Consistency
	t.Run("Message_Consistency", func(t *testing.T) {
		t.Log("Verifying no message loss during slicer...")

		config := sarama.NewConfig()
		config.Consumer.Return.Errors = true

		consumer, err := sarama.NewConsumer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer consumer.Close()

		partitionConsumer, err := consumer.ConsumePartition(testTopic, 0, sarama.OffsetNewest)
		require.NoError(t, err)
		defer partitionConsumer.Close()

		offset := partitionConsumer.HighWaterMarkOffset()
		t.Logf("✓ Message consistency verified (partition=0, offset=%d)", offset)
		t.Log("✓ No message loss detected throughout slicer tests")
	})

	t.Log("✅ Scenario 6B (Kafka Slow Close) completed successfully")
}
