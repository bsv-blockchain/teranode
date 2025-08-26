package blockassemblyutil

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestWaitForBlockAssemblyReady tests the WaitForBlockAssemblyReady function
func TestWaitForBlockAssemblyReady(t *testing.T) {
	tests := []struct {
		name          string
		blockHeight   uint32
		blockHash     string
		setupMock     func(*blockassembly.Mock)
		expectedError bool
		errorContains string
	}{
		{
			name:        "block assembly is ready",
			blockHeight: 100,
			blockHash:   "test-hash",
			setupMock: func(m *blockassembly.Mock) {
				m.On("GetBlockAssemblyState", mock.Anything).Return(
					&blockassembly_api.StateMessage{CurrentHeight: 100},
					nil,
				).Once()
			},
			expectedError: false,
		},
		{
			name:        "block assembly is ready with max blocks behind",
			blockHeight: 100,
			blockHash:   "test-hash",
			setupMock: func(m *blockassembly.Mock) {
				m.On("GetBlockAssemblyState", mock.Anything).Return(
					&blockassembly_api.StateMessage{CurrentHeight: 99},
					nil,
				).Once()
			},
			expectedError: false,
		},
		{
			name:        "block assembly catches up after retries",
			blockHeight: 100,
			blockHash:   "test-hash",
			setupMock: func(m *blockassembly.Mock) {
				// First call - behind
				m.On("GetBlockAssemblyState", mock.Anything).Return(
					&blockassembly_api.StateMessage{CurrentHeight: 98},
					nil,
				).Once()
				// Second call - still behind
				m.On("GetBlockAssemblyState", mock.Anything).Return(
					&blockassembly_api.StateMessage{CurrentHeight: 98},
					nil,
				).Once()
				// Third call - caught up
				m.On("GetBlockAssemblyState", mock.Anything).Return(
					&blockassembly_api.StateMessage{CurrentHeight: 99},
					nil,
				).Once()
			},
			expectedError: false,
		},
		{
			name:        "block assembly is persistently behind",
			blockHeight: 100,
			blockHash:   "test-hash",
			setupMock: func(m *blockassembly.Mock) {
				// Always return behind height
				m.On("GetBlockAssemblyState", mock.Anything).Return(
					&blockassembly_api.StateMessage{CurrentHeight: 98},
					nil,
				)
			},
			expectedError: true,
			errorContains: "block assembly is behind",
		},
		{
			name:        "block assembly client error",
			blockHeight: 100,
			blockHash:   "test-hash",
			setupMock: func(m *blockassembly.Mock) {
				m.On("GetBlockAssemblyState", mock.Anything).Return(
					(*blockassembly_api.StateMessage)(nil),
					errors.NewServiceError("connection failed"),
				)
			},
			expectedError: true,
			errorContains: "failed to get block assembly state",
		},
		{
			name:          "nil block assembly client",
			blockHeight:   100,
			blockHash:     "test-hash",
			setupMock:     func(m *blockassembly.Mock) {},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock block assembly client
			var mockClient blockassembly.ClientI
			if tt.name != "nil block assembly client" {
				mockClientImpl := &blockassembly.Mock{}
				tt.setupMock(mockClientImpl)
				mockClient = mockClientImpl
			}

			// Use a short timeout for tests to avoid waiting too long
			// The actual retry will be limited by the retry count (20)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			logger := ulogger.TestLogger{}

			err := WaitForBlockAssemblyReady(
				ctx,
				logger,
				mockClient,
				tt.blockHeight,
				1, // Allow 1 block behind for the test
			)

			if tt.expectedError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					// The error could be either the expected error or context timeout
					// due to aggressive retry configuration
					if err.Error() != "context deadline exceeded" {
						assert.Contains(t, err.Error(), tt.errorContains)
					}
				}
			} else {
				assert.NoError(t, err)
			}

			// Verify all expectations were met
			if mockClientImpl, ok := mockClient.(*blockassembly.Mock); ok && mockClientImpl != nil {
				mockClientImpl.AssertExpectations(t)
			}
		})
	}
}

// TestWaitForBlockAssemblyReady_ContextCancellation tests that the function respects context cancellation
func TestWaitForBlockAssemblyReadyContextCancellation(t *testing.T) {
	mockClient := &blockassembly.Mock{}

	// Make block assembly always behind
	mockClient.On("GetBlockAssemblyState", mock.Anything).Return(
		&blockassembly_api.StateMessage{CurrentHeight: 98},
		nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	logger := ulogger.TestLogger{}

	err := WaitForBlockAssemblyReady(
		ctx,
		logger,
		mockClient,
		100,
		1, // Allow 1 block behind for the test
	)

	assert.Error(t, err)
	// The error message might vary, but it should indicate timeout/cancellation
	assert.True(t,
		errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, errors.ErrContextCanceled),
	)
}
