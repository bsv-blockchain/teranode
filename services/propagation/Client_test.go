package propagation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
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
		// Create a batch with a single item
		done := make(chan error, 1)
		batch := []*batchItem{
			{
				done: done,
			},
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

		// Verify the error was sent to the transaction
		select {
		case err := <-done:
			require.Error(t, err)
			assert.Contains(t, err.Error(), "Batch processing failed")
			assert.Contains(t, err.Error(), "test error")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Timed out waiting for error to be sent to transaction")
		}
	})

	t.Run("Multiple transaction batch", func(t *testing.T) {
		// Create a batch with multiple items
		done1 := make(chan error, 1)
		done2 := make(chan error, 1)
		done3 := make(chan error, 1)

		batch := []*batchItem{
			{done: done1},
			{done: done2},
			{done: done3},
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

		// Verify the same error was sent to all transactions
		for i, doneChan := range []chan error{done1, done2, done3} {
			select {
			case err := <-doneChan:
				require.Error(t, err)
				assert.Contains(t, err.Error(), "Failed to process batch with ID test-batch-123")
				assert.Contains(t, err.Error(), "batch failure")

				// Verify it's a service error
				_, ok := err.(*errors.Error)
				assert.True(t, ok, fmt.Sprintf("Error sent to transaction %d is not a service error", i))
			case <-time.After(100 * time.Millisecond):
				t.Fatalf("Timed out waiting for error to be sent to transaction %d", i)
			}
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
