package propagation

import (
	"context"
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

// TestPropagationServiceErrors tests error handling when the validator service fails.
// This test uses a null validator that always returns errors to validate proper
// error propagation and service behavior under validation failure conditions.
func TestPropagationServiceErrors(t *testing.T) {
	// Create test settings
	tSettings := test.CreateBaseTestSettings(t)
	// tSettings.Propagation.GRPCListenAddress = "localhost:0" // Let OS choose port

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
	readyCh := make(chan struct{})
	go func() {
		err := server.Start(context.Background(), readyCh)
		require.NoError(t, err)
	}()

	// Wait for server to be ready and get the actual port
	<-readyCh
	time.Sleep(100 * time.Millisecond) // Give a bit more time for GRPC to be ready

	// Create GRPC client
	conn, err := grpc.NewClient(
		tSettings.Propagation.GRPCListenAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
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
