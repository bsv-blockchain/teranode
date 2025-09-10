package propagation

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/propagation/propagation_api"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// getFreePort finds a free port to use for testing
func getFreePort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// TestPropagationServiceErrors tests error handling when the validator service fails.
// This test uses a null validator that always returns errors to validate proper
// error propagation and service behavior under validation failure conditions.
func TestPropagationServiceErrors(t *testing.T) {
	// Create test settings
	tSettings := test.CreateBaseTestSettings(t)
	// Use dynamic ports to avoid conflicts
	grpcPort := getFreePort(t)
	httpPort := getFreePort(t)
	tSettings.Propagation.GRPCListenAddress = fmt.Sprintf("localhost:%d", grpcPort)
	tSettings.Propagation.HTTPListenAddress = fmt.Sprintf("localhost:%d", httpPort)

	mockClient := &blockchain.Mock{}

	mockClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(nil)

	// Create a server with a null validator that always returns an error
	server := New(
		ulogger.TestLogger{},
		tSettings,
		nil,        // No tx store
		nil,        // Validator that always returns error
		mockClient, // Mock blockchain client
		nil,        // No kafka producer
	)

	// Start the server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readyCh := make(chan struct{})
	serverErrCh := make(chan error, 1)
	go func() {
		err := server.Start(ctx, readyCh)
		if err != nil {
			serverErrCh <- err
		}
	}()

	// Wait for server to be ready or error
	select {
	case <-readyCh:
		// Server started successfully
		time.Sleep(100 * time.Millisecond) // Give a bit more time for GRPC to be ready
	case err := <-serverErrCh:
		t.Skipf("Skipping test - server failed to start (port likely in use): %v", err)
		return
	case <-time.After(5 * time.Second):
		t.Fatal("Server failed to start within timeout")
	}

	// Since we're using port 0, we need to get the actual address the server is listening on
	// For now, we'll use the configured address and skip if connection fails
	conn, err := grpc.NewClient(
		tSettings.Propagation.GRPCListenAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Skipf("Skipping test - failed to connect to server: %v", err)
		return
	}
	defer conn.Close()

	client := propagation_api.NewPropagationAPIClient(conn)

	t.Run("ProcessTransactionBatch returns TError for each invalid tx", func(t *testing.T) {
		// Create two simple transactions
		tx1 := bt.NewTx()
		tx2 := bt.NewTx()

		// Try to process them in a batch
		resp, err := client.ProcessTransactionBatch(context.Background(), &propagation_api.ProcessTransactionBatchRequest{
			Items: []*propagation_api.BatchTransactionItem{
				{Tx: tx1.ExtendedBytes()},
				{Tx: tx2.ExtendedBytes()},
			},
		})

		// Verify we get a successful response but with TErrors for each tx
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Errors, 2)

		// Check each TError in detail
		for _, terr := range resp.Errors {
			require.NotNil(t, terr)
			assert.Equal(t, errors.ERR_TX_INVALID, terr.Code)
			assert.Contains(t, terr.Message, "transaction with no inputs")
		}
	})
}
