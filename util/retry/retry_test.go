package retry

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/util/test/mock_logger"
	"github.com/stretchr/testify/assert"
)

func TestRetry(t *testing.T) {
	logger := mock_logger.NewTestLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Function that will succeed on the first attempt
	successFn := func() (string, error) {
		return "success", nil
	}

	// Function that will fail once then succeed
	staticCallCount := 0
	retryOnceFn := func() (string, error) {
		if staticCallCount == 0 {
			staticCallCount++
			return "", errors.NewProcessingError("error")
		}
		return "success", nil
	}

	// Function that will always fail
	alwaysFailFn := func() (string, error) {
		return "", errors.NewProcessingError("persistent error")
	}

	retryOpts := WithRetryCount(3)
	backoffMultOpts := WithBackoffMultiplier(2)
	backoffDurOpts := WithBackoffDurationType(100 * time.Millisecond)
	messageOpts := WithMessage("Trying again")

	// Test case 1: Function succeeds on the first try
	result, err := Retry(ctx, logger, successFn, retryOpts, backoffMultOpts, backoffDurOpts, messageOpts)
	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	logger.AssertNumberOfCalls(t, "Infof", 1)
	logger.Reset()

	// Test case 2: Function fails once then succeeds
	result, err = Retry(ctx, logger, retryOnceFn, retryOpts, backoffMultOpts, backoffDurOpts, messageOpts)
	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	logger.AssertNumberOfCalls(t, "Infof", 2)
	logger.Reset()

	// Test case 3: Function fails all attempts
	result, err = Retry(ctx, logger, alwaysFailFn, retryOpts, backoffMultOpts, backoffDurOpts, messageOpts)
	assert.Error(t, err)
	assert.Empty(t, result)
	logger.AssertNumberOfCalls(t, "Infof", 3)
	logger.Reset()

	// Test case 4: Context cancellation
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Immediately cancel the context
	result, err = Retry(cancelCtx, logger, alwaysFailFn, retryOpts, backoffMultOpts, backoffDurOpts, messageOpts)
	assert.Empty(t, result)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	logger.AssertNumberOfCalls(t, "Infof", 0)
}

func TestRetryTimer(t *testing.T) {
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
		options        []Options
		expectedSleeps []time.Duration
		simulateErrors int // Number of times the function should return an error before succeeding
		expectedError  bool
	}{
		{
			name:           "Retry three times with increasing backoff, fail all retries",
			options:        []Options{WithRetryCount(3), WithBackoffMultiplier(1), WithBackoffDurationType(time.Millisecond), WithMessage("retrying...")},
			expectedSleeps: []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond},
			simulateErrors: 3, // Fails three times
			expectedError:  true,
		},
		{
			name:           "Function succeeds on first try",
			options:        []Options{WithRetryCount(3), WithBackoffMultiplier(1), WithBackoffDurationType(time.Millisecond), WithMessage("retrying...")},
			expectedSleeps: nil, // No sleep calls
			expectedError:  false,
		},
		{
			name:           "Error twice then succeed, success on last try",
			options:        []Options{WithRetryCount(3), WithBackoffMultiplier(1), WithBackoffDurationType(time.Millisecond), WithMessage("retrying...")},
			expectedSleeps: []time.Duration{1 * time.Millisecond, 2 * time.Millisecond},
			simulateErrors: 2, // Fails twice, then succeeds
			expectedError:  false,
		},
		{
			name:           "Retry with increasing backoff, succeeds midway",
			options:        []Options{WithRetryCount(5), WithBackoffMultiplier(1), WithBackoffDurationType(time.Millisecond), WithMessage("retrying...")},
			expectedSleeps: []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond},
			simulateErrors: 3, // Fails twice, then succeeds on the third try
			expectedError:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recordedSleeps = nil
			ctx := context.Background()
			logger := mock_logger.NewTestLogger()
			errorCount := 0
			f := func() (string, error) {
				if errorCount < tc.simulateErrors {
					errorCount++
					return "", errors.NewError("test error")
				}
				return "success", nil
			}

			_, err := Retry(ctx, logger, f, tc.options...)

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
		})
	}
}
