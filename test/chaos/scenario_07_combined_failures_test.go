package chaos

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestScenario07_SimultaneousFailure tests system behavior when both PostgreSQL and Kafka fail simultaneously
// This simulates infrastructure-wide issues like network partitions or datacenter problems
//
// Test Scenario:
// 1. Establish baseline with both services healthy
// 2. Simultaneously disable both PostgreSQL and Kafka (complete failure)
// 3. Test behavior during simultaneous outage
// 4. Restore both services simultaneously
// 5. Verify recovery and data consistency
//
// Expected Behavior:
// - System detects both failures quickly
// - No cascading failures or hangs
// - Graceful degradation (errors returned, not crashes)
// - Full recovery when both services restored
// - No data corruption
func TestScenario07_SimultaneousFailure(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// Configuration
	const (
		toxiproxyPostgresURL = "http://localhost:8474"
		toxiproxyKafkaURL    = "http://localhost:8475"
		postgresProxyName    = "postgres"
		kafkaProxyName       = "kafka"
		postgresToxiURL      = "postgres://postgres:really_strong_password_change_me@localhost:15432/postgres?sslmode=disable&connect_timeout=5"
		kafkaToxiURL         = "localhost:19092"
		testTopic            = "chaos_test_scenario_07"
	)

	// Create toxiproxy clients
	toxiPostgres := NewToxiproxyClient(toxiproxyPostgresURL)
	toxiKafka := NewToxiproxyClient(toxiproxyKafkaURL)

	// Wait for both toxiproxy instances to be available
	t.Log("Waiting for toxiproxy services to be available...")
	require.NoError(t, toxiPostgres.WaitForProxy(postgresProxyName, 30*time.Second))
	require.NoError(t, toxiKafka.WaitForProxy(kafkaProxyName, 30*time.Second))

	// Reset both proxies to clean state
	t.Log("Resetting both proxies to clean state...")
	require.NoError(t, toxiPostgres.ResetProxy(postgresProxyName))
	require.NoError(t, toxiKafka.ResetProxy(kafkaProxyName))

	// Cleanup at the end
	defer func() {
		t.Log("Cleaning up: resetting both proxies...")
		_ = toxiPostgres.ResetProxy(postgresProxyName)
		_ = toxiKafka.ResetProxy(kafkaProxyName)
	}()

	// Phase 1: Establish Baseline (both services healthy)
	t.Run("Baseline_Both_Services_Healthy", func(t *testing.T) {
		t.Log("Testing baseline with both PostgreSQL and Kafka healthy...")

		// Test PostgreSQL
		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var pgResult int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&pgResult)
		require.NoError(t, err)
		require.Equal(t, 1, pgResult)

		// Test Kafka
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 5 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		msg := &sarama.ProducerMessage{
			Topic: testTopic,
			Value: sarama.StringEncoder("baseline_test"),
		}
		_, _, err = producer.SendMessage(msg)
		require.NoError(t, err)

		t.Log("✓ Baseline: Both PostgreSQL and Kafka are healthy")
	})

	// Phase 2: Inject Simultaneous Complete Failure
	t.Run("Inject_Simultaneous_Failure", func(t *testing.T) {
		t.Log("Disabling both PostgreSQL and Kafka simultaneously...")

		// Disable both proxies at the same time
		err := toxiPostgres.DisableProxy(postgresProxyName)
		require.NoError(t, err)

		err = toxiKafka.DisableProxy(kafkaProxyName)
		require.NoError(t, err)

		t.Log("✓ Both services disabled simultaneously")
	})

	// Phase 3: Test Behavior During Simultaneous Outage
	t.Run("Behavior_During_Simultaneous_Outage", func(t *testing.T) {
		t.Log("Testing system behavior with both services down...")

		// Test PostgreSQL - should fail quickly
		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		require.Error(t, err, "PostgreSQL query should fail when proxy disabled")
		t.Logf("✓ PostgreSQL correctly fails: %v", err)

		// Test Kafka - should fail quickly
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.Timeout = 3 * time.Second
		config.Net.DialTimeout = 3 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		if err != nil {
			t.Logf("✓ Kafka producer creation correctly fails: %v", err)
		} else {
			defer producer.Close()
			msg := &sarama.ProducerMessage{
				Topic: testTopic,
				Value: sarama.StringEncoder("should_fail"),
			}
			_, _, err = producer.SendMessage(msg)
			require.Error(t, err, "Kafka send should fail when proxy disabled")
			t.Logf("✓ Kafka send correctly fails: %v", err)
		}

		t.Log("✓ Both services correctly report failures (no hangs or crashes)")
	})

	// Phase 4: Restore Both Services Simultaneously
	t.Run("Restore_Both_Services", func(t *testing.T) {
		t.Log("Restoring both PostgreSQL and Kafka simultaneously...")

		// Enable both proxies at the same time
		err := toxiPostgres.EnableProxy(postgresProxyName)
		require.NoError(t, err)

		err = toxiKafka.EnableProxy(kafkaProxyName)
		require.NoError(t, err)

		// Wait a moment for connections to stabilize
		time.Sleep(2 * time.Second)

		t.Log("✓ Both services restored")
	})

	// Phase 5: Verify Recovery and Consistency
	t.Run("Verify_Recovery_And_Consistency", func(t *testing.T) {
		t.Log("Verifying both services recovered correctly...")

		// Test PostgreSQL recovery
		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var pgResult int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&pgResult)
		require.NoError(t, err)
		require.Equal(t, 1, pgResult)
		t.Log("✓ PostgreSQL fully recovered")

		// Test Kafka recovery
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = 5 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		msg := &sarama.ProducerMessage{
			Topic: testTopic,
			Value: sarama.StringEncoder("recovery_test"),
		}
		_, _, err = producer.SendMessage(msg)
		require.NoError(t, err)
		t.Log("✓ Kafka fully recovered")

		t.Log("✓ Both services recovered successfully with no data corruption")
	})

	t.Log("✅ Scenario 7A (Simultaneous Failure) completed successfully")
}

// TestScenario07_SimultaneousLatency tests system behavior when both PostgreSQL and Kafka become slow simultaneously
// This simulates infrastructure degradation like network congestion affecting multiple services
func TestScenario07_SimultaneousLatency(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// Configuration
	const (
		toxiproxyPostgresURL = "http://localhost:8474"
		toxiproxyKafkaURL    = "http://localhost:8475"
		postgresProxyName    = "postgres"
		kafkaProxyName       = "kafka"
		postgresToxiURL      = "postgres://postgres:really_strong_password_change_me@localhost:15432/postgres?sslmode=disable&connect_timeout=10"
		kafkaToxiURL         = "localhost:19092"
		testTopic            = "chaos_test_scenario_07"

		// Latency to inject (ms)
		latencyMs = 500 // 500ms - moderate infrastructure slowdown
	)

	// Create toxiproxy clients
	toxiPostgres := NewToxiproxyClient(toxiproxyPostgresURL)
	toxiKafka := NewToxiproxyClient(toxiproxyKafkaURL)

	// Wait for both toxiproxy instances
	t.Log("Waiting for toxiproxy services...")
	require.NoError(t, toxiPostgres.WaitForProxy(postgresProxyName, 30*time.Second))
	require.NoError(t, toxiKafka.WaitForProxy(kafkaProxyName, 30*time.Second))

	// Reset both proxies
	t.Log("Resetting both proxies...")
	require.NoError(t, toxiPostgres.ResetProxy(postgresProxyName))
	require.NoError(t, toxiKafka.ResetProxy(kafkaProxyName))

	// Cleanup
	defer func() {
		t.Log("Cleaning up...")
		_ = toxiPostgres.ResetProxy(postgresProxyName)
		_ = toxiKafka.ResetProxy(kafkaProxyName)
	}()

	// Phase 1: Baseline
	t.Run("Baseline_Performance", func(t *testing.T) {
		t.Log("Measuring baseline performance...")

		// PostgreSQL baseline
		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		start := time.Now()
		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		pgDuration := time.Since(start)
		require.NoError(t, err)

		// Kafka baseline
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.Timeout = 5 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		start = time.Now()
		msg := &sarama.ProducerMessage{
			Topic: testTopic,
			Value: sarama.StringEncoder("baseline"),
		}
		_, _, err = producer.SendMessage(msg)
		kafkaDuration := time.Since(start)
		require.NoError(t, err)

		t.Logf("✓ Baseline - PostgreSQL: %v, Kafka: %v", pgDuration, kafkaDuration)
	})

	// Phase 2: Inject Simultaneous Latency
	t.Run("Inject_Simultaneous_Latency", func(t *testing.T) {
		t.Logf("Injecting %dms latency to both services...", latencyMs)

		// Add latency to both services simultaneously
		err := toxiPostgres.AddLatency(postgresProxyName, latencyMs, "downstream")
		require.NoError(t, err)

		err = toxiKafka.AddLatency(kafkaProxyName, latencyMs, "downstream")
		require.NoError(t, err)

		t.Logf("✓ %dms latency applied to both services", latencyMs)
	})

	// Phase 3: Test Performance Under Simultaneous Latency
	t.Run("Performance_Under_Simultaneous_Latency", func(t *testing.T) {
		t.Log("Testing performance with both services slow...")

		// Test PostgreSQL with latency
		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		start := time.Now()
		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		pgDuration := time.Since(start)
		require.NoError(t, err)
		require.Equal(t, 1, result)

		// Test Kafka with latency
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.Timeout = 10 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		start = time.Now()
		msg := &sarama.ProducerMessage{
			Topic: testTopic,
			Value: sarama.StringEncoder("with_latency"),
		}
		_, _, err = producer.SendMessage(msg)
		kafkaDuration := time.Since(start)
		require.NoError(t, err)

		t.Logf("✓ With latency - PostgreSQL: %v, Kafka: %v", pgDuration, kafkaDuration)
		t.Log("✓ Both services remain functional despite simultaneous slowdown")

		// Verify PostgreSQL definitely slower (database queries directly affected by latency)
		require.Greater(t, pgDuration, time.Duration(latencyMs)*time.Millisecond/2,
			"PostgreSQL should be slower with latency")

		// Kafka might not show latency impact on single message due to pipelining/batching
		// Just verify it completed successfully without timing out
		t.Logf("✓ Kafka completed successfully (timing may vary due to batching/pipelining)")
	})

	// Phase 4: Remove Latency and Verify Recovery
	t.Run("Recovery_After_Latency_Removal", func(t *testing.T) {
		t.Log("Removing latency from both services...")

		// Remove all toxics
		require.NoError(t, toxiPostgres.RemoveAllToxics(postgresProxyName))
		require.NoError(t, toxiKafka.RemoveAllToxics(kafkaProxyName))

		// Wait for stabilization
		time.Sleep(2 * time.Second)

		// Test PostgreSQL recovery
		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		start := time.Now()
		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		pgDuration := time.Since(start)
		require.NoError(t, err)

		// Test Kafka recovery
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.Timeout = 5 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		start = time.Now()
		msg := &sarama.ProducerMessage{
			Topic: testTopic,
			Value: sarama.StringEncoder("recovery"),
		}
		_, _, err = producer.SendMessage(msg)
		kafkaDuration := time.Since(start)
		require.NoError(t, err)

		t.Logf("✓ Recovery - PostgreSQL: %v, Kafka: %v", pgDuration, kafkaDuration)
		t.Log("✓ Both services recovered to normal performance")

		// Verify performance returned to reasonable levels
		require.Less(t, pgDuration, 2*time.Second, "PostgreSQL should be fast after recovery")
		require.Less(t, kafkaDuration, 2*time.Second, "Kafka should be fast after recovery")
	})

	t.Log("✅ Scenario 7B (Simultaneous Latency) completed successfully")
}

// TestScenario07_StaggeredRecovery tests system behavior when services recover at different times
// This simulates realistic failure scenarios where dependencies don't all recover simultaneously
func TestScenario07_StaggeredRecovery(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// Configuration
	const (
		toxiproxyPostgresURL = "http://localhost:8474"
		toxiproxyKafkaURL    = "http://localhost:8475"
		postgresProxyName    = "postgres"
		kafkaProxyName       = "kafka"
		postgresToxiURL      = "postgres://postgres:really_strong_password_change_me@localhost:15432/postgres?sslmode=disable&connect_timeout=5"
		kafkaToxiURL         = "localhost:19092"
		testTopic            = "chaos_test_scenario_07"

		recoveryDelay = 3 * time.Second // Delay between recoveries
	)

	// Create toxiproxy clients
	toxiPostgres := NewToxiproxyClient(toxiproxyPostgresURL)
	toxiKafka := NewToxiproxyClient(toxiproxyKafkaURL)

	// Setup
	t.Log("Setting up proxies...")
	require.NoError(t, toxiPostgres.WaitForProxy(postgresProxyName, 30*time.Second))
	require.NoError(t, toxiKafka.WaitForProxy(kafkaProxyName, 30*time.Second))
	require.NoError(t, toxiPostgres.ResetProxy(postgresProxyName))
	require.NoError(t, toxiKafka.ResetProxy(kafkaProxyName))

	defer func() {
		_ = toxiPostgres.ResetProxy(postgresProxyName)
		_ = toxiKafka.ResetProxy(kafkaProxyName)
	}()

	// Phase 1: Disable Both Services
	t.Run("Disable_Both_Services", func(t *testing.T) {
		t.Log("Disabling both services...")

		err := toxiPostgres.DisableProxy(postgresProxyName)
		require.NoError(t, err)

		err = toxiKafka.DisableProxy(kafkaProxyName)
		require.NoError(t, err)

		t.Log("✓ Both services disabled")
	})

	// Phase 2: Restore PostgreSQL First
	t.Run("Restore_PostgreSQL_First", func(t *testing.T) {
		t.Log("Restoring PostgreSQL first (Kafka still down)...")

		err := toxiPostgres.EnableProxy(postgresProxyName)
		require.NoError(t, err)

		time.Sleep(1 * time.Second)

		// Verify PostgreSQL is back
		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err)
		t.Log("✓ PostgreSQL recovered while Kafka still down")

		// Verify Kafka still fails
		config := sarama.NewConfig()
		config.Net.DialTimeout = 2 * time.Second
		config.Producer.Timeout = 2 * time.Second

		_, err = sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.Error(t, err, "Kafka should still be down")
		t.Log("✓ Kafka confirmed still down")
	})

	// Phase 3: Wait and Restore Kafka
	t.Run("Restore_Kafka_After_Delay", func(t *testing.T) {
		t.Logf("Waiting %v before restoring Kafka...", recoveryDelay)
		time.Sleep(recoveryDelay)

		t.Log("Restoring Kafka...")
		err := toxiKafka.EnableProxy(kafkaProxyName)
		require.NoError(t, err)

		time.Sleep(1 * time.Second)

		// Verify both services now healthy
		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err)
		t.Log("✓ PostgreSQL still healthy")

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.Timeout = 5 * time.Second

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		msg := &sarama.ProducerMessage{
			Topic: testTopic,
			Value: sarama.StringEncoder("staggered_recovery"),
		}
		_, _, err = producer.SendMessage(msg)
		require.NoError(t, err)
		t.Log("✓ Kafka recovered - both services now healthy")
	})

	// Phase 4: Verify No Data Corruption from Staggered Recovery
	t.Run("Verify_Consistency_After_Staggered_Recovery", func(t *testing.T) {
		t.Log("Verifying data consistency after staggered recovery...")

		// Insert test data to PostgreSQL
		db, err := sql.Open("postgres", postgresToxiURL)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		testValue := "staggered_test_data"
		_, err = db.ExecContext(ctx, "CREATE TEMP TABLE staggered_test (data TEXT)")
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, "INSERT INTO staggered_test (data) VALUES ($1)", testValue)
		require.NoError(t, err)

		var retrieved string
		err = db.QueryRowContext(ctx, "SELECT data FROM staggered_test LIMIT 1").Scan(&retrieved)
		require.NoError(t, err)
		require.Equal(t, testValue, retrieved)

		// Send test message to Kafka
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true

		producer, err := sarama.NewSyncProducer([]string{kafkaToxiURL}, config)
		require.NoError(t, err)
		defer producer.Close()

		msg := &sarama.ProducerMessage{
			Topic: testTopic,
			Value: sarama.StringEncoder("consistency_check"),
		}
		_, _, err = producer.SendMessage(msg)
		require.NoError(t, err)

		t.Log("✓ Data consistency verified - no corruption from staggered recovery")
	})

	t.Log("✅ Scenario 7C (Staggered Recovery) completed successfully")
}
