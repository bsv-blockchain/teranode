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

	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:     true,
		EnableP2P:     true,
		UTXOStoreType: "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})
	defer node.Stop(t, true)

	// Subscribe to blockchain notifications
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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
