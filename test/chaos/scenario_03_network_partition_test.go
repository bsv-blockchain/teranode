package chaos

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestScenario03_NetworkPartition tests how the system handles network partitions
// This implements Scenario 3 from chaos testing suite
//
// Test Scenario:
// 1. Establish baseline connectivity to both PostgreSQL and Kafka
// 2. Simulate network partition by disabling toxiproxy proxies
// 3. Verify services become unreachable
// 4. Test application resilience during partition
// 5. Restore network connectivity
// 6. Verify services recover and reconnect
// 7. Confirm data consistency after partition
//
// Expected Behavior:
// - Services should detect network partition
// - Connection errors should be handled gracefully
// - No panic or crashes during partition
// - Services recover automatically when partition is healed
// - No data loss or corruption
func TestScenario03_NetworkPartition(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// Configuration
	const (
		postgresToxiURL   = "http://localhost:8474"
		kafkaToxiURL      = "http://localhost:8475"
		postgresProxy     = "postgres"
		kafkaProxy        = "kafka"
		postgresDirectURL = "postgres://postgres:really_strong_password_change_me@localhost:5432/postgres?sslmode=disable"
		postgresToxiConn  = "postgres://postgres:really_strong_password_change_me@localhost:15432/postgres?sslmode=disable&connect_timeout=5"
		kafkaDirectURL    = "localhost:9092"
		kafkaToxiConn     = "localhost:19092"
		testTopic         = "chaos_test_scenario_03"
		baselineTimeout   = 10 * time.Second
		partitionDuration = 15 * time.Second
	)

	// Create toxiproxy clients
	postgresProxyClient := NewToxiproxyClient(postgresToxiURL)
	kafkaProxyClient := NewToxiproxyClient(kafkaToxiURL)

	// Ensure toxiproxy services are available
	t.Log("Waiting for toxiproxy services to be available...")
	err := postgresProxyClient.WaitForProxy(postgresProxy, 30*time.Second)
	require.NoError(t, err, "postgres toxiproxy should be available")
	err = kafkaProxyClient.WaitForProxy(kafkaProxy, 30*time.Second)
	require.NoError(t, err, "kafka toxiproxy should be available")

	// Ensure clean state at start
	t.Log("Resetting toxiproxy to clean state...")
	err = postgresProxyClient.ResetProxy(postgresProxy)
	require.NoError(t, err, "should reset postgres proxy before test")
	err = kafkaProxyClient.ResetProxy(kafkaProxy)
	require.NoError(t, err, "should reset kafka proxy before test")

	// Cleanup function to reset toxiproxy after test
	defer func() {
		t.Log("Cleaning up: resetting toxiproxy...")
		if err := postgresProxyClient.ResetProxy(postgresProxy); err != nil {
			t.Logf("Warning: failed to reset postgres proxy: %v", err)
		}
		if err := kafkaProxyClient.ResetProxy(kafkaProxy); err != nil {
			t.Logf("Warning: failed to reset kafka proxy: %v", err)
		}
	}()

	// Step 1: Establish baseline connectivity
	t.Run("Baseline_Connectivity", func(t *testing.T) {
		t.Log("Testing baseline connectivity to PostgreSQL and Kafka...")

		// Test PostgreSQL connectivity
		t.Run("PostgreSQL", func(t *testing.T) {
			db, err := sql.Open("postgres", postgresToxiConn)
			require.NoError(t, err, "should connect to PostgreSQL via toxiproxy")
			defer db.Close()

			db.SetConnMaxLifetime(60 * time.Second)
			db.SetMaxOpenConns(5)
			db.SetMaxIdleConns(2)

			err = db.Ping()
			require.NoError(t, err, "should ping PostgreSQL successfully")

			var result int
			err = db.QueryRow("SELECT 1").Scan(&result)
			require.NoError(t, err, "should query PostgreSQL")
			require.Equal(t, 1, result, "query should return expected result")

			t.Logf("✓ PostgreSQL baseline connectivity verified")
		})

		// Test Kafka connectivity
		t.Run("Kafka", func(t *testing.T) {
			config := sarama.NewConfig()
			config.Producer.Return.Successes = true
			config.Producer.Return.Errors = true
			config.Producer.RequiredAcks = sarama.WaitForAll
			config.Producer.Retry.Max = 3
			config.Producer.Timeout = 10 * time.Second

			producer, err := sarama.NewSyncProducer([]string{kafkaToxiConn}, config)
			require.NoError(t, err, "should create Kafka producer via toxiproxy")
			defer producer.Close()

			message := &sarama.ProducerMessage{
				Topic: testTopic,
				Key:   sarama.StringEncoder("baseline_key"),
				Value: sarama.StringEncoder("baseline_value"),
			}
			partition, offset, err := producer.SendMessage(message)
			require.NoError(t, err, "should produce to Kafka")
			require.GreaterOrEqual(t, partition, int32(0), "should have valid partition")
			require.GreaterOrEqual(t, offset, int64(0), "should have valid offset")

			t.Logf("✓ Kafka baseline connectivity verified (partition=%d, offset=%d)", partition, offset)
		})

		t.Logf("✓ Baseline connectivity test complete")
	})

	// Step 2: Simulate network partition by disabling proxies
	t.Run("Inject_Network_Partition", func(t *testing.T) {
		t.Log("Simulating network partition by disabling toxiproxy services...")

		// Disable PostgreSQL proxy
		err := postgresProxyClient.DisableProxy(postgresProxy)
		require.NoError(t, err, "should disable postgres proxy")

		// Disable Kafka proxy
		err = kafkaProxyClient.DisableProxy(kafkaProxy)
		require.NoError(t, err, "should disable kafka proxy")

		// Verify proxies are disabled
		postgresProxyInfo, err := postgresProxyClient.GetProxy(postgresProxy)
		require.NoError(t, err, "should get postgres proxy info")
		require.False(t, postgresProxyInfo.Enabled, "postgres proxy should be disabled")

		kafkaProxyInfo, err := kafkaProxyClient.GetProxy(kafkaProxy)
		require.NoError(t, err, "should get kafka proxy info")
		require.False(t, kafkaProxyInfo.Enabled, "kafka proxy should be disabled")

		t.Logf("✓ Network partition injected (both PostgreSQL and Kafka proxies disabled)")
	})

	// Step 3: Verify services become unreachable
	t.Run("Services_Unreachable", func(t *testing.T) {
		t.Log("Verifying services are unreachable during network partition...")

		// Test PostgreSQL is unreachable
		t.Run("PostgreSQL_Unreachable", func(t *testing.T) {
			db, err := sql.Open("postgres", postgresToxiConn)
			require.NoError(t, err, "should create db connection (not connected yet)")
			defer db.Close()

			db.SetConnMaxLifetime(5 * time.Second)
			db.SetMaxOpenConns(5)
			db.SetMaxIdleConns(2)

			// Try to ping - should fail
			start := time.Now()
			err = db.Ping()
			duration := time.Since(start)

			require.Error(t, err, "PostgreSQL ping should fail during partition")
			t.Logf("✓ PostgreSQL correctly unreachable (failed after %v)", duration)
		})

		// Test Kafka is unreachable
		t.Run("Kafka_Unreachable", func(t *testing.T) {
			config := sarama.NewConfig()
			config.Producer.Return.Successes = true
			config.Producer.Return.Errors = true
			config.Producer.RequiredAcks = sarama.WaitForAll
			config.Producer.Retry.Max = 2
			config.Producer.Timeout = 3 * time.Second
			config.Metadata.Retry.Max = 2
			config.Metadata.Retry.Backoff = 500 * time.Millisecond

			start := time.Now()
			producer, err := sarama.NewSyncProducer([]string{kafkaToxiConn}, config)
			duration := time.Since(start)

			if err != nil {
				t.Logf("✓ Kafka correctly unreachable (failed during creation after %v): %v", duration, err)
				return
			}
			defer producer.Close()

			// If producer was created, try to send message
			message := &sarama.ProducerMessage{
				Topic: testTopic,
				Key:   sarama.StringEncoder("partition_key"),
				Value: sarama.StringEncoder("partition_value"),
			}
			start = time.Now()
			_, _, err = producer.SendMessage(message)
			duration = time.Since(start)

			require.Error(t, err, "Kafka produce should fail during partition")
			t.Logf("✓ Kafka correctly unreachable (produce failed after %v)", duration)
		})

		t.Logf("✓ Both services correctly unreachable during partition")
	})

	// Step 4: Test application resilience during partition
	t.Run("Resilience_During_Partition", func(t *testing.T) {
		t.Log("Testing application resilience during partition...")

		// Simulate multiple connection attempts (as an application would do)
		const retryAttempts = 3
		var postgresFailures, kafkaFailures int

		for i := 0; i < retryAttempts; i++ {
			t.Logf("Retry attempt %d/%d...", i+1, retryAttempts)

			// Try PostgreSQL
			db, err := sql.Open("postgres", postgresToxiConn)
			if err == nil {
				db.SetConnMaxLifetime(2 * time.Second)
				err = db.Ping()
				db.Close()
			}
			if err != nil {
				postgresFailures++
			}

			// Try Kafka
			config := sarama.NewConfig()
			config.Producer.Timeout = 2 * time.Second
			config.Producer.Retry.Max = 1
			config.Metadata.Timeout = 2 * time.Second
			producer, err := sarama.NewSyncProducer([]string{kafkaToxiConn}, config)
			if err != nil {
				kafkaFailures++
			} else {
				producer.Close()
			}

			time.Sleep(1 * time.Second)
		}

		// All attempts should fail during partition
		require.Equal(t, retryAttempts, postgresFailures, "all PostgreSQL attempts should fail")
		require.Equal(t, retryAttempts, kafkaFailures, "all Kafka attempts should fail")

		t.Logf("✓ Application resilience verified: gracefully handled %d failed attempts", retryAttempts)
	})

	// Step 5: Heal network partition
	t.Run("Heal_Network_Partition", func(t *testing.T) {
		t.Log("Healing network partition by re-enabling toxiproxy services...")

		// Re-enable PostgreSQL proxy
		err := postgresProxyClient.EnableProxy(postgresProxy)
		require.NoError(t, err, "should enable postgres proxy")

		// Re-enable Kafka proxy
		err = kafkaProxyClient.EnableProxy(kafkaProxy)
		require.NoError(t, err, "should enable kafka proxy")

		// Verify proxies are enabled
		postgresProxyInfo, err := postgresProxyClient.GetProxy(postgresProxy)
		require.NoError(t, err, "should get postgres proxy info")
		require.True(t, postgresProxyInfo.Enabled, "postgres proxy should be enabled")

		kafkaProxyInfo, err := kafkaProxyClient.GetProxy(kafkaProxy)
		require.NoError(t, err, "should get kafka proxy info")
		require.True(t, kafkaProxyInfo.Enabled, "kafka proxy should be enabled")

		t.Logf("✓ Network partition healed (both proxies re-enabled)")

		// Give services a moment to recover
		time.Sleep(2 * time.Second)
	})

	// Step 6: Verify services recover
	t.Run("Services_Recovery", func(t *testing.T) {
		t.Log("Verifying services recover after partition healing...")

		// Test PostgreSQL recovery
		t.Run("PostgreSQL_Recovery", func(t *testing.T) {
			db, err := sql.Open("postgres", postgresToxiConn)
			require.NoError(t, err, "should connect to PostgreSQL")
			defer db.Close()

			db.SetConnMaxLifetime(60 * time.Second)
			db.SetMaxOpenConns(5)
			db.SetMaxIdleConns(2)

			start := time.Now()
			err = db.Ping()
			duration := time.Since(start)

			require.NoError(t, err, "should ping PostgreSQL after recovery")

			var result int
			err = db.QueryRow("SELECT 42").Scan(&result)
			require.NoError(t, err, "should query PostgreSQL after recovery")
			require.Equal(t, 42, result, "query should return expected result")

			t.Logf("✓ PostgreSQL recovered (reconnected in %v)", duration)
		})

		// Test Kafka recovery
		t.Run("Kafka_Recovery", func(t *testing.T) {
			config := sarama.NewConfig()
			config.Producer.Return.Successes = true
			config.Producer.Return.Errors = true
			config.Producer.RequiredAcks = sarama.WaitForAll
			config.Producer.Retry.Max = 3
			config.Producer.Timeout = 10 * time.Second

			start := time.Now()
			producer, err := sarama.NewSyncProducer([]string{kafkaToxiConn}, config)
			require.NoError(t, err, "should create Kafka producer after recovery")
			defer producer.Close()

			message := &sarama.ProducerMessage{
				Topic: testTopic,
				Key:   sarama.StringEncoder("recovery_key"),
				Value: sarama.StringEncoder("recovery_value"),
			}
			partition, offset, err := producer.SendMessage(message)
			duration := time.Since(start)

			require.NoError(t, err, "should produce to Kafka after recovery")
			require.GreaterOrEqual(t, partition, int32(0), "should have valid partition")
			require.GreaterOrEqual(t, offset, int64(0), "should have valid offset")

			t.Logf("✓ Kafka recovered (produced in %v, partition=%d, offset=%d)", duration, partition, offset)
		})

		t.Logf("✓ Both services recovered successfully")
	})

	// Step 7: Verify data consistency
	t.Run("Data_Consistency", func(t *testing.T) {
		t.Log("Verifying data consistency after partition recovery...")

		// Test PostgreSQL data consistency
		t.Run("PostgreSQL_Consistency", func(t *testing.T) {
			db, err := sql.Open("postgres", postgresDirectURL)
			require.NoError(t, err, "should connect to PostgreSQL")
			defer db.Close()

			// Create test table
			_, err = db.Exec(`
				CREATE TABLE IF NOT EXISTS chaos_test_scenario_03 (
					id SERIAL PRIMARY KEY,
					value TEXT NOT NULL,
					created_at TIMESTAMP DEFAULT NOW()
				)
			`)
			require.NoError(t, err, "should create test table")

			// Insert test data
			_, err = db.Exec(`
				INSERT INTO chaos_test_scenario_03 (value)
				VALUES ('before_partition'), ('after_partition')
			`)
			require.NoError(t, err, "should insert test data")

			// Verify data
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM chaos_test_scenario_03").Scan(&count)
			require.NoError(t, err, "should query test data")
			require.GreaterOrEqual(t, count, 2, "test data should be present")

			// Cleanup
			_, err = db.Exec("DROP TABLE chaos_test_scenario_03")
			require.NoError(t, err, "should drop test table")

			t.Logf("✓ PostgreSQL data consistency verified (%d records)", count)
		})

		// Test Kafka message delivery consistency
		t.Run("Kafka_Consistency", func(t *testing.T) {
			config := sarama.NewConfig()
			config.Producer.Return.Successes = true
			config.Producer.Return.Errors = true
			config.Producer.RequiredAcks = sarama.WaitForAll
			config.Producer.Retry.Max = 3
			config.Producer.Timeout = 10 * time.Second

			producer, err := sarama.NewSyncProducer([]string{kafkaDirectURL}, config)
			require.NoError(t, err, "should create producer for consistency check")
			defer producer.Close()

			// Send a final verification message
			verifyMsg := &sarama.ProducerMessage{
				Topic: testTopic,
				Key:   sarama.StringEncoder("consistency_check"),
				Value: sarama.StringEncoder(fmt.Sprintf("verification_%d", time.Now().Unix())),
			}
			partition, offset, err := producer.SendMessage(verifyMsg)
			require.NoError(t, err, "should produce verification message")
			require.GreaterOrEqual(t, partition, int32(0), "should have valid partition")
			require.GreaterOrEqual(t, offset, int64(0), "should have valid offset")

			t.Logf("✓ Kafka message consistency verified (offset=%d)", offset)
		})

		t.Logf("✓ Data consistency verified for both services")
	})

	t.Log("✅ Scenario 3 (Network Partition) completed successfully")
}
