package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/util/test/mock_logger"
	"github.com/stretchr/testify/assert"
)

func TestRetryWithLogger(t *testing.T) {
	logger := mock_logger.NewTestLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Function that will succeed on the first attempt
	successFn := func() error {
		return nil
	}

	// Function that will fail once then succeed
	staticCallCount := 0
	retryOnceFn := func() error {
		if staticCallCount == 0 {
			staticCallCount++
			return errors.New("error")
		}
		return nil
	}

	// Function that will always fail
	alwaysFailFn := func() error {
		return errors.New("persistent error")
	}

	// Test case 1: Function succeeds on the first try
	err := RetryWithLogger(ctx, logger, successFn, 3, 2, 100*time.Millisecond, "Trying again")
	assert.NoError(t, err)
	logger.AssertNumberOfCalls(t, "Infof", 1)
	logger.Reset()

	// Test case 2: Function fails once then succeeds
	err = RetryWithLogger(ctx, logger, retryOnceFn, 3, 2, 100*time.Millisecond, "Trying again")
	assert.NoError(t, err)
	logger.AssertNumberOfCalls(t, "Infof", 2)
	logger.Reset()

	// Test case 3: Function fails all attempts
	err = RetryWithLogger(ctx, logger, alwaysFailFn, 3, 2, 100*time.Millisecond, "Trying again")
	assert.Error(t, err)
	logger.AssertNumberOfCalls(t, "Infof", 3)
	logger.Reset()

	// Test case 4: Context cancellation
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Immediately cancel the context
	err = RetryWithLogger(cancelCtx, logger, alwaysFailFn, 3, 2, 100*time.Millisecond, "Trying again")
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	logger.AssertNumberOfCalls(t, "Infof", 0)
}

func TestRetryWithLoggerTimer(t *testing.T) {
	// Save the original function
	originalSleepFunc := sleepFunc
	// Restore the original function after test
	defer func() { sleepFunc = originalSleepFunc }()

	// Record the sleep calls with the mock sleep function
	// This is used to test the backoff time
	var recordedSleeps []time.Duration
	// mock sleep function
	sleepFunc = func(duration time.Duration) {
		recordedSleeps = append(recordedSleeps, duration)
	}

	tests := []struct {
		name           string
		retryCount     int
		backoffMult    int
		backoffType    time.Duration
		expectedSleeps []time.Duration
		simulateErrors int // Number of times the function should return an error before succeeding
		expectedError  bool
	}{
		{
			name:           "Retry three times with increasing backoff, fail all retries",
			retryCount:     3,
			backoffMult:    1,
			backoffType:    time.Millisecond, // Use smaller units for testing
			expectedSleeps: []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond},
			simulateErrors: 3, // Fails three times
			expectedError:  true,
		},
		{
			name:           "Function succeeds on first try",
			retryCount:     3,
			backoffMult:    1,
			backoffType:    time.Millisecond,
			expectedSleeps: nil, // No sleep calls
			expectedError:  false,
		},
		{
			name:           "Error twice then succeed, success on last try",
			retryCount:     3,
			backoffMult:    1,
			backoffType:    time.Millisecond,
			expectedSleeps: []time.Duration{1 * time.Millisecond, 2 * time.Millisecond},
			simulateErrors: 2, // Fails twice, then succeeds
			expectedError:  false,
		},
		{
			name:           "Retry with increasing backoff, succeeds midway",
			retryCount:     5,
			backoffMult:    1,
			backoffType:    time.Millisecond,
			expectedSleeps: []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond},
			simulateErrors: 3, // Fails twice, then succeeds on the third try
			expectedError:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recordedSleeps = nil // Reset before each test case
			ctx := context.Background()
			logger := mock_logger.NewTestLogger()
			errorCount := 0
			f := func() error {
				if errorCount < tc.simulateErrors {
					errorCount++
					return errors.New("test error")
				}
				return nil
			}

			err := RetryWithLogger(ctx, logger, f, tc.retryCount, tc.backoffMult, tc.backoffType, "retrying...")

			if errorCount < tc.simulateErrors && err == nil {
				t.Errorf("Expected an error but got nil")
			}

			if errorCount >= tc.simulateErrors && tc.expectedError == false && err != nil {
				t.Errorf("Expected no error but got %v", err)
			}

			if len(recordedSleeps) != len(tc.expectedSleeps) {
				t.Errorf("Expected %d sleep calls but got %d", len(tc.expectedSleeps), len(recordedSleeps))
			}

			for i, expected := range tc.expectedSleeps {
				if recordedSleeps[i] != expected {
					t.Errorf("Expected sleep of %v but got %v on attempt %d", expected, recordedSleeps[i], i+1)
				}
			}
			// if len(recordedSleeps) != len(tc.expectedSleeps) {
			// 	t.Errorf("Expected %d sleep calls but got %d", len(tc.expectedSleeps), len(recordedSleeps))
			// } else {
			// 	for i, expected := range tc.expectedSleeps {
			// 		if recordedSleeps[i] != expected {
			// 			t.Errorf("Expected sleep of %v but got %v on attempt %d", expected, recordedSleeps[i], i+1)
			// 		}
			// 	}
			// }
		})
	}
}
