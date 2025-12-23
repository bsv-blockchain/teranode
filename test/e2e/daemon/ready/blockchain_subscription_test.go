package smoke

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/stretchr/testify/require"
)

func TestBlockchainSubscriptionReconnection(t *testing.T) {
	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:     true,
		EnableP2P:     true,
		UTXOStoreType: "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})
	defer node.Stop(t, true)

	// Subscribe to blockchain notifications with a long-lived context
	subscribeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	subscriptionCh, err := node.BlockchainClient.Subscribe(subscribeCtx, "test-subscription")
	require.NoError(t, err)

	// Helper function to drain notifications and verify we got at least one
	drainAndVerifyNotifications := func(testName string, minExpected int) {
		notifications := make([]*blockchain_api.Notification, 0)
		deadline := time.After(3 * time.Second)

		// Collect notifications for a short period
		collecting := true
		for collecting {
			select {
			case notification := <-subscriptionCh:
				notifications = append(notifications, notification)
				t.Logf("%s: Received notification type=%v", testName, notification.Type)
			case <-time.After(100 * time.Millisecond):
				// No more notifications in last 100ms, assume we're done
				collecting = false
			case <-deadline:
				collecting = false
			}
		}

		require.GreaterOrEqual(t, len(notifications), minExpected,
			"%s: expected at least %d notifications, got %d", testName, minExpected, len(notifications))
	}

	// Get initial block height for verification
	blockCtx, blockCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer blockCancel()
	_, initialMeta, err := node.BlockchainClient.GetBestBlockHeader(blockCtx)
	require.NoError(t, err)
	initialHeight := initialMeta.Height

	// Generate a block to trigger a notification
	_, err = node.CallRPC(node.Ctx, "generate", []any{1})
	require.NoError(t, err)

	// Wait for block to be fully processed (poll until height increases)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return false
		}
		return meta.Height > initialHeight
	}, 3*time.Second, 100*time.Millisecond, "Block was not processed")

	// Now drain notifications - we should have at least one
	drainAndVerifyNotifications("First block", 1)

	t.Log("Testing subscription resilience - generating more blocks")

	// Generate more blocks and verify we continue to receive notifications
	for i := 0; i < 3; i++ {
		currentCtx, currentCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, currentMeta, err := node.BlockchainClient.GetBestBlockHeader(currentCtx)
		currentCancel()
		require.NoError(t, err)
		currentHeight := currentMeta.Height

		_, err = node.CallRPC(node.Ctx, "generate", []any{1})
		require.NoError(t, err)

		// Wait for block to be processed
		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
			if err != nil {
				return false
			}
			return meta.Height > currentHeight
		}, 3*time.Second, 100*time.Millisecond, "Block %d was not processed", i+1)

		// Drain and verify notifications
		drainAndVerifyNotifications(fmt.Sprintf("Block %d", i+1), 1)
	}

	t.Log("Subscription test completed successfully")
}
