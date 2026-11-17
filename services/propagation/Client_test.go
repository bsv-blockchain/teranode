package propagation

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewClient tests the NewClient constructor
func TestNewClient(t *testing.T) {
	logger := ulogger.TestLogger{}
	ctx := context.Background()

	t.Run("with empty addresses", func(t *testing.T) {
		// Create mock settings with empty addresses
		s := &settings.Settings{
			Propagation: settings.PropagationSettings{
				GRPCAddresses: []string{},
				HTTPAddresses: []string{},
			},
		}

		client, err := NewClient(ctx, logger, s)
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "no gRPC addresses provided")
	})

	t.Run("with invalid HTTP address", func(t *testing.T) {
		// Create mock settings with invalid HTTP address
		s := &settings.Settings{
			Propagation: settings.PropagationSettings{
				GRPCAddresses: []string{"localhost:9090"},
				HTTPAddresses: []string{"://invalid-url"},
			},
		}

		client, err := NewClient(ctx, logger, s)
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "invalid")
	})
}

// TestHandleBatchError tests the handleBatchError method of the Client
func TestHandleBatchError(t *testing.T) {
	// Create a test logger
	logger := ulogger.TestLogger{}

	// Create a test client with the logger
	client := &Client{
		logger: logger,
	}

	t.Run("Single transaction batch", func(t *testing.T) {
		// Create a batch with a single item sharing a completion group.
		group := completion.NewGroup(1)
		batch := []*batchItem{
			{group: group},
		}

		// Create an error to pass
		originalErr := errors.NewUnknownError("test error")

		// Format string and args
		format := "Batch processing failed: %v"

		// Call handleBatchError
		returnedErr := client.handleBatchError(batch, originalErr, format)

		// Verify the returned error is a service error with the correct message
		require.Error(t, returnedErr)
		assert.Contains(t, returnedErr.Error(), "Batch processing failed")
		assert.Contains(t, returnedErr.Error(), "test error")

		// Verify the error was delivered to the transaction via the group.
		require.NoError(t, group.Wait(context.Background(), 0))
		require.Error(t, batch[0].result)
		assert.Contains(t, batch[0].result.Error(), "Batch processing failed")
		assert.Contains(t, batch[0].result.Error(), "test error")
	})

	t.Run("Multiple transaction batch", func(t *testing.T) {
		// Create a batch with multiple items sharing one completion group.
		group := completion.NewGroup(3)
		batch := []*batchItem{
			{group: group},
			{group: group},
			{group: group},
		}

		// Create an error to pass
		originalErr := errors.NewUnknownError("batch failure")

		// Format string with additional args
		format := "Failed to process batch with ID %s: %v"
		batchID := "test-batch-123"

		// Call handleBatchError
		returnedErr := client.handleBatchError(batch, originalErr, format, batchID)

		// Verify the returned error is a service error with the correct message
		require.Error(t, returnedErr)
		assert.Contains(t, returnedErr.Error(), "Failed to process batch with ID test-batch-123")
		assert.Contains(t, returnedErr.Error(), "batch failure")

		// Verify the same error was delivered to all transactions.
		require.NoError(t, group.Wait(context.Background(), 0))
		for i, item := range batch {
			require.Error(t, item.result)
			assert.Contains(t, item.result.Error(), "Failed to process batch with ID test-batch-123")
			assert.Contains(t, item.result.Error(), "batch failure")

			// Verify it's a service error
			_, ok := item.result.(*errors.Error)
			assert.True(t, ok, fmt.Sprintf("Error sent to transaction %d is not a service error", i))
		}
	})

	t.Run("Empty batch", func(t *testing.T) {
		// Create an empty batch
		batch := []*batchItem{}

		// Create an error to pass
		originalErr := errors.NewUnknownError("test error")

		// Format string and args
		format := "Empty batch error: %v"

		// Call handleBatchError
		returnedErr := client.handleBatchError(batch, originalErr, format)

		// Verify the returned error is a service error with the correct message
		require.Error(t, returnedErr)
		assert.Contains(t, returnedErr.Error(), "Empty batch error")
		assert.Contains(t, returnedErr.Error(), "test error")
	})
}
