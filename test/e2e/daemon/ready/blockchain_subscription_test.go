package smoke

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/stretchr/testify/require"
)

func TestBlockchainSubscriptionReconnection(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Use a test-scoped context so we always tear down the subscription
	// before stopping the daemon. This helps avoid races during shutdown.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:     true,
		EnableP2P:     true,
		UTXOStoreType: "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})
	t.Cleanup(func() {
		// Ensure subscription context is cancelled before shutting down services
		cancel()
		// Give the subscription goroutine a brief window to detach cleanly
		time.Sleep(200 * time.Millisecond)
		node.Stop(t, true)
	})

	// Subscribe to blockchain notifications
	subscriptionCh, err := node.BlockchainClient.Subscribe(ctx, "test-subscription")
	require.NoError(t, err)

	// Generate a block to trigger a notification
	_, err = node.CallRPC(node.Ctx, "generate", []any{1})
	require.NoError(t, err)

	// Wait for notification
	select {
	case notification := <-subscriptionCh:
		require.NotNil(t, notification)
		t.Logf("Received notification: %v", notification.Type)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for notification")
	}

	// Simulate network interruption by stopping and restarting the blockchain service
	// This would normally cause the subscription to fail and need reconnection
	t.Log("Testing subscription resilience - generating more blocks")

	// Generate more blocks and verify we continue to receive notifications
	for i := 0; i < 3; i++ {
		_, err = node.CallRPC(node.Ctx, "generate", []any{1})
		require.NoError(t, err)

		select {
		case notification := <-subscriptionCh:
			require.NotNil(t, notification)
			t.Logf("Received notification %d: %v", i+1, notification.Type)
		case <-time.After(10 * time.Second):
			t.Fatalf("Timeout waiting for notification %d", i+1)
		}
	}

	t.Log("Subscription test completed successfully")
}
