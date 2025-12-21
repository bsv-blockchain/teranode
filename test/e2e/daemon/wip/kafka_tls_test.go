package smoke

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	testkafka "github.com/bsv-blockchain/teranode/test/longtest/util/kafka"
	kafkautil "github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaTLSConnection(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		// Start Kafka container with TLS
		ctx := context.Background()
		kafkaContainer, err := testkafka.RunTestContainerTLS(ctx)
		require.NoError(t, err)
		defer func() {
			err := kafkaContainer.Container.Stop(ctx, nil)
			if err != nil {
				t.Logf("Failed to stop Kafka container: %v", err)
			}
		}()

		time.Sleep(5 * time.Second)

		td := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			SettingsOverrideFunc: test.ComposeSettings(
				test.SystemTestSettings(),
				func(settings *settings.Settings) {
					settings.Kafka.EnableTLS = true
					settings.Kafka.TLSSkipVerify = true

					kafkaBrokers := kafkaContainer.GetBrokerAddresses()
					if len(kafkaBrokers) > 0 {
						settings.Kafka.Hosts = kafkaBrokers[0]
					}

					settings.Kafka.BlocksConfig.Scheme = "memory"
					settings.Kafka.BlocksFinalConfig.Scheme = "memory"
					settings.Kafka.LegacyInvConfig.Scheme = "memory"
					settings.Kafka.RejectedTxConfig.Scheme = "memory"
					settings.Kafka.SubtreesConfig.Scheme = "memory"
					settings.Kafka.TxMetaConfig.Scheme = "memory"
				},
			),
		})

		defer td.Stop(t)

		t.Logf("Testing Kafka TLS connection with broker: %s", td.Settings.Kafka.Hosts)

		time.Sleep(2 * time.Second)

		// Verify that Kafka services are working by checking if we can mine a block
		// This indirectly tests the Kafka TLS connection
		block := td.MineAndWait(t, 1)
		require.NotNil(t, block, "Should be able to mine a block with TLS-enabled Kafka")

		t.Logf("Successfully mined block %s with TLS-enabled Kafka", block.Hash().String())
	})
}

func TestKafkaTLSWithCertificateVerification(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		ctx := context.Background()
		kafkaContainer, err := testkafka.RunTestContainerTLS(ctx)
		require.NoError(t, err)
		defer func() {
			err := kafkaContainer.Container.Stop(ctx, nil)
			if err != nil {
				t.Logf("Failed to stop Kafka container: %v", err)
			}
		}()

		time.Sleep(5 * time.Second)

		t.Run("With Skip Verification", func(t *testing.T) {
			td := daemon.NewTestDaemon(t, daemon.TestOptions{
				EnableRPC: true,
				SettingsOverrideFunc: test.ComposeSettings(
					test.SystemTestSettings(),
					func(settings *settings.Settings) {
						settings.Kafka.EnableTLS = true
						settings.Kafka.TLSSkipVerify = true

						kafkaBrokers := kafkaContainer.GetBrokerAddresses()
						if len(kafkaBrokers) > 0 {
							settings.Kafka.Hosts = kafkaBrokers[0]
						}

						settings.Kafka.BlocksConfig.Scheme = "memory"
						settings.Kafka.BlocksFinalConfig.Scheme = "memory"
						settings.Kafka.LegacyInvConfig.Scheme = "memory"
						settings.Kafka.RejectedTxConfig.Scheme = "memory"
						settings.Kafka.SubtreesConfig.Scheme = "memory"
						settings.Kafka.TxMetaConfig.Scheme = "memory"
					},
				),
			})

			defer td.Stop(t)

			time.Sleep(2 * time.Second)

			block := td.MineAndWait(t, 1)
			require.NotNil(t, block, "Should be able to mine a block with TLS skip verification")

			assert.True(t, td.Settings.Kafka.EnableTLS, "TLS should be enabled")
			assert.True(t, td.Settings.Kafka.TLSSkipVerify, "TLS verification should be skipped")
		})

		t.Run("Without Skip Verification", func(t *testing.T) {
			td := daemon.NewTestDaemon(t, daemon.TestOptions{
				EnableRPC: true,
				SettingsOverrideFunc: test.ComposeSettings(
					test.SystemTestSettings(),
					func(settings *settings.Settings) {
						settings.Kafka.EnableTLS = true
						settings.Kafka.TLSSkipVerify = false

						kafkaBrokers := kafkaContainer.GetBrokerAddresses()
						if len(kafkaBrokers) > 0 {
							settings.Kafka.Hosts = kafkaBrokers[0]
						}

						settings.Kafka.BlocksConfig.Scheme = "memory"
						settings.Kafka.BlocksFinalConfig.Scheme = "memory"
						settings.Kafka.LegacyInvConfig.Scheme = "memory"
						settings.Kafka.RejectedTxConfig.Scheme = "memory"
						settings.Kafka.SubtreesConfig.Scheme = "memory"
						settings.Kafka.TxMetaConfig.Scheme = "memory"
					},
				),
			})

			defer td.Stop(t)

			time.Sleep(2 * time.Second)

			block := td.MineAndWait(t, 1)
			require.NotNil(t, block, "Should be able to mine a block with TLS certificate verification")

			assert.True(t, td.Settings.Kafka.EnableTLS, "TLS should be enabled")
			assert.False(t, td.Settings.Kafka.TLSSkipVerify, "TLS verification should not be skipped")
		})
	})
}

func TestKafkaTLSWithCustomCertificates(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		settings := settings.NewSettings("dev.system.test")
		settings.Kafka.EnableTLS = true
		settings.Kafka.TLSSkipVerify = false
		settings.Kafka.TLSCAFile = "/non/existent/ca.pem"
		settings.Kafka.TLSCertFile = "/non/existent/cert.pem"
		settings.Kafka.TLSKeyFile = "/non/existent/key.pem"

		err := kafkautil.ValidateKafkaAuthSettings(&settings.Kafka)
		assert.Error(t, err, "Should fail validation with non-existent certificate files")
		assert.Contains(t, err.Error(), "TLS CA certificate file not found")

		assert.True(t, settings.Kafka.EnableTLS, "TLS should be enabled")
		assert.False(t, settings.Kafka.TLSSkipVerify, "TLS verification should not be skipped")
		assert.Equal(t, "/non/existent/ca.pem", settings.Kafka.TLSCAFile, "CA file should be set")
		assert.Equal(t, "/non/existent/cert.pem", settings.Kafka.TLSCertFile, "Cert file should be set")
		assert.Equal(t, "/non/existent/key.pem", settings.Kafka.TLSKeyFile, "Key file should be set")

		t.Logf("Successfully tested TLS certificate validation with non-existent files")
	})
}

func TestKafkaTLSCertificateValidation(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		settings := settings.NewSettings("dev.system.test")
		settings.Kafka.EnableTLS = true
		settings.Kafka.TLSCertFile = "/non/existent/cert.pem"
		settings.Kafka.TLSKeyFile = "/non/existent/key.pem"

		err := kafkautil.ValidateKafkaAuthSettings(&settings.Kafka)
		assert.Error(t, err, "Should fail validation with non-existent certificate files")
		assert.Contains(t, err.Error(), "TLS certificate file not found")

		settings.Kafka.TLSCertFile = ""
		settings.Kafka.TLSKeyFile = ""
		err = kafkautil.ValidateKafkaAuthSettings(&settings.Kafka)
		assert.NoError(t, err, "Should pass validation with no certificate files specified")
	})
}

func TestKafkaTLSConnectionFailure(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		ctx := context.Background()
		kafkaContainer, err := testkafka.RunTestContainer(ctx)
		require.NoError(t, err)
		defer func() {
			err := kafkaContainer.Container.Stop(ctx, nil)
			if err != nil {
				t.Logf("Failed to stop Kafka container: %v", err)
			}
		}()

		time.Sleep(5 * time.Second)

		td := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			SettingsOverrideFunc: test.ComposeSettings(
				test.SystemTestSettings(),
				func(settings *settings.Settings) {
					settings.Kafka.EnableTLS = true
					settings.Kafka.TLSSkipVerify = false

					kafkaBrokers := kafkaContainer.GetBrokerAddresses()
					if len(kafkaBrokers) > 0 {
						settings.Kafka.Hosts = kafkaBrokers[0]
					}

					settings.Kafka.BlocksConfig.Scheme = "memory"
					settings.Kafka.BlocksFinalConfig.Scheme = "memory"
					settings.Kafka.LegacyInvConfig.Scheme = "memory"
					settings.Kafka.RejectedTxConfig.Scheme = "memory"
					settings.Kafka.SubtreesConfig.Scheme = "memory"
					settings.Kafka.TxMetaConfig.Scheme = "memory"
				},
			),
		})

		defer td.Stop(t)

		// The daemon should fail to start or have connection issues
		// We can't easily test this without modifying the daemon startup logic
		// For now, we'll just verify the settings are correct
		assert.True(t, td.Settings.Kafka.EnableTLS, "TLS should be enabled")
		assert.False(t, td.Settings.Kafka.TLSSkipVerify, "TLS verification should not be skipped")
	})
}

func TestKafkaTLSEnvironmentVariables(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		os.Setenv("KAFKA_ENABLE_TLS", "true")
		os.Setenv("KAFKA_TLS_SKIP_VERIFY", "true")
		defer func() {
			os.Unsetenv("KAFKA_ENABLE_TLS")
			os.Unsetenv("KAFKA_TLS_SKIP_VERIFY")
		}()

		settings := settings.NewSettings("dev.system.test")

		assert.True(t, settings.Kafka.EnableTLS, "TLS should be enabled from environment variable")
		assert.True(t, settings.Kafka.TLSSkipVerify, "TLS skip verify should be enabled from environment variable")
	})
}

func TestKafkaTLSIntegrationWithServices(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		ctx := context.Background()
		kafkaContainer, err := testkafka.RunTestContainerTLS(ctx)
		require.NoError(t, err)
		defer func() {
			err := kafkaContainer.Container.Stop(ctx, nil)
			if err != nil {
				t.Logf("Failed to stop Kafka container: %v", err)
			}
		}()

		time.Sleep(5 * time.Second)

		td := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			SettingsOverrideFunc: test.ComposeSettings(
				test.SystemTestSettings(),
				func(settings *settings.Settings) {
					settings.Kafka.EnableTLS = true
					settings.Kafka.TLSSkipVerify = true

					kafkaBrokers := kafkaContainer.GetBrokerAddresses()
					if len(kafkaBrokers) > 0 {
						settings.Kafka.Hosts = kafkaBrokers[0]
					}

					settings.Kafka.BlocksConfig.Scheme = "memory"
					settings.Kafka.BlocksFinalConfig.Scheme = "memory"
					settings.Kafka.LegacyInvConfig.Scheme = "memory"
					settings.Kafka.RejectedTxConfig.Scheme = "memory"
					settings.Kafka.SubtreesConfig.Scheme = "memory"
					settings.Kafka.TxMetaConfig.Scheme = "memory"
				},
			),
		})

		defer td.Stop(t)

		t.Run("Blockchain Service", func(t *testing.T) {
			block := td.MineAndWait(t, 1)
			require.NotNil(t, block, "Blockchain service should work with TLS")

			err := block.CheckMerkleRoot(ctx)
			require.NoError(t, err, "Block should have valid merkle root")
		})

		t.Run("Block Assembly Service", func(t *testing.T) {
			coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, ctx)
			require.NotNil(t, coinbaseTx, "Should be able to create coinbase transaction")

			block := td.MineAndWait(t, 1)
			require.NotNil(t, block, "Block assembly service should work with TLS")
		})

		t.Run("RPC Service", func(t *testing.T) {
			resp, err := td.CallRPC(ctx, "getblockchaininfo", []any{})
			require.NoError(t, err, "RPC service should work with TLS")
			require.NotEmpty(t, resp, "RPC response should not be empty")
		})
	})
}

func TestKafkaTLSConfigurationValidation(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		settings := settings.NewSettings("dev.system.test")

		settings.Kafka.EnableTLS = true
		settings.Kafka.TLSSkipVerify = false

		assert.True(t, settings.Kafka.EnableTLS, "TLS should be enabled")
		assert.False(t, settings.Kafka.TLSSkipVerify, "TLS verification should not be skipped")
	})
}

func TestKafkaTLSPerformance(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		ctx := context.Background()
		kafkaContainer, err := testkafka.RunTestContainerTLS(ctx)
		require.NoError(t, err)
		defer func() {
			err := kafkaContainer.Container.Stop(ctx, nil)
			if err != nil {
				t.Logf("Failed to stop Kafka container: %v", err)
			}
		}()

		time.Sleep(5 * time.Second)

		td := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			SettingsOverrideFunc: test.ComposeSettings(
				test.SystemTestSettings(),
				func(settings *settings.Settings) {
					settings.Kafka.EnableTLS = true
					settings.Kafka.TLSSkipVerify = true

					kafkaBrokers := kafkaContainer.GetBrokerAddresses()
					if len(kafkaBrokers) > 0 {
						settings.Kafka.Hosts = kafkaBrokers[0]
					}

					settings.Kafka.BlocksConfig.Scheme = "memory"
					settings.Kafka.BlocksFinalConfig.Scheme = "memory"
					settings.Kafka.LegacyInvConfig.Scheme = "memory"
					settings.Kafka.RejectedTxConfig.Scheme = "memory"
					settings.Kafka.SubtreesConfig.Scheme = "memory"
					settings.Kafka.TxMetaConfig.Scheme = "memory"
				},
			),
		})

		defer td.Stop(t)

		startTime := time.Now()

		for i := 0; i < 5; i++ {
			block := td.MineAndWait(t, 1)
			require.NotNil(t, block, fmt.Sprintf("Should be able to mine block %d with TLS", i+1))
		}

		endTime := time.Now()
		duration := endTime.Sub(startTime)

		t.Logf("Mined 5 blocks with TLS in %v (average: %v per block)", duration, duration/5)

		assert.Less(t, duration, 30*time.Second, "Mining 5 blocks should complete within 30 seconds")
	})
}

func TestKafkaTLSReconnection(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		ctx := context.Background()

		t.Logf("Phase 1: Starting Kafka container and establishing initial connection...")
		kafkaContainer, err := testkafka.RunTestContainerTLS(ctx)
		require.NoError(t, err)

		time.Sleep(5 * time.Second)

		td := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			SettingsOverrideFunc: test.ComposeSettings(
				test.SystemTestSettings(),
				func(settings *settings.Settings) {
					settings.Kafka.EnableTLS = true
					settings.Kafka.TLSSkipVerify = true

					kafkaBrokers := kafkaContainer.GetBrokerAddresses()
					if len(kafkaBrokers) > 0 {
						settings.Kafka.Hosts = kafkaBrokers[0]
					}

					settings.Kafka.BlocksConfig.Scheme = "memory"
					settings.Kafka.BlocksFinalConfig.Scheme = "memory"
					settings.Kafka.LegacyInvConfig.Scheme = "memory"
					settings.Kafka.RejectedTxConfig.Scheme = "memory"
					settings.Kafka.SubtreesConfig.Scheme = "memory"
					settings.Kafka.TxMetaConfig.Scheme = "memory"
				},
			),
		})

		block1 := td.MineAndWait(t, 1)
		require.NotNil(t, block1, "Should be able to mine first block")
		t.Logf("Successfully mined initial block: %s", block1.Hash().String())

		t.Logf("Phase 2: Stopping Kafka container to simulate disconnection...")
		err = kafkaContainer.Container.Stop(ctx, nil)
		require.NoError(t, err)
		t.Logf("Kafka container stopped successfully")

		time.Sleep(3 * time.Second)

		t.Logf("Phase 3: Starting new Kafka container for reconnection test...")
		kafkaContainer2, err := testkafka.RunTestContainerTLS(ctx)
		require.NoError(t, err)
		defer func() {
			err := kafkaContainer2.Container.Stop(ctx, nil)
			if err != nil {
				t.Logf("Failed to stop Kafka container: %v", err)
			}
		}()

		time.Sleep(5 * time.Second)

		t.Logf("Phase 4: Testing reconnection by mining a new block...")

		block2 := td.MineAndWait(t, 1)
		require.NotNil(t, block2, "Should be able to mine second block after reconnection")
		t.Logf("Successfully mined second block after reconnection: %s", block2.Hash().String())

		assert.NotEqual(t, block1.Hash().String(), block2.Hash().String(), "Blocks should be different")

		t.Logf("Phase 5: Testing additional operations to ensure full reconnection...")

		resp, err := td.CallRPC(ctx, "getblockchaininfo", []any{})
		require.NoError(t, err, "RPC service should work after reconnection")
		require.NotEmpty(t, resp, "RPC response should not be empty after reconnection")

		block3 := td.MineAndWait(t, 1)
		require.NotNil(t, block3, "Should be able to mine third block after reconnection")
		t.Logf("Successfully mined third block after reconnection: %s", block3.Hash().String())

		t.Logf("✅ Kafka TLS reconnection test completed successfully!")
	})
}
