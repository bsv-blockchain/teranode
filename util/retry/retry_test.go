package retry

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/assert"
)

func TestRetry(t *testing.T) {
	logger := mocklogger.NewTestLogger()

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
	logger.AssertNumberOfCalls(t, "Warnf", 0)
	logger.Reset()

	// Test case for exponential backoff with cap
	result, err = Retry(ctx, logger, retryOnceFn,
		WithExponentialBackoff(),
		WithBackoffDurationType(50*time.Millisecond),
		WithBackoffFactor(2.0),
		WithMaxBackoff(200*time.Millisecond),
		WithRetryCount(3))
	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	logger.Reset()

	// Test case for infinite retry (will succeed after one failure)
	staticCallCount = 0 // Reset counter
	result, err = Retry(ctx, logger, retryOnceFn,
		WithInfiniteRetry(),
		WithExponentialBackoff(),
		WithBackoffDurationType(10*time.Millisecond),
		WithBackoffFactor(2.0),
		WithMaxBackoff(100*time.Millisecond))
	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	logger.Reset()

	// Test case for context cancellation with infinite retry
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = Retry(ctx, logger, alwaysFailFn,
		WithInfiniteRetry(),
		WithExponentialBackoff(),
		WithBackoffDurationType(10*time.Millisecond))
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
	logger.Reset()
}

func TestCappedExponentialBackoff(t *testing.T) {
	// Test exponential backoff without hitting the cap
	backoff := CappedExponentialBackoff(100*time.Millisecond, 2.0, 1*time.Second)
	assert.Equal(t, 200*time.Millisecond, backoff)

	// Test exponential backoff hitting the cap
	backoff = CappedExponentialBackoff(600*time.Millisecond, 2.0, 1*time.Second)
	assert.Equal(t, 1*time.Second, backoff)

	// Test with different factor
	backoff = CappedExponentialBackoff(100*time.Millisecond, 1.5, 1*time.Second)
	assert.Equal(t, 150*time.Millisecond, backoff)
}

func TestRetryWithExponentialBackoff(t *testing.T) {
	logger := mocklogger.NewTestLogger()
	ctx := context.Background()

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

	// Test exponential backoff with successful retry
	staticCallCount = 0
	result, err := Retry(ctx, logger, retryOnceFn,
		WithExponentialBackoff(),
		WithBackoffDurationType(10*time.Millisecond),
		WithBackoffFactor(2.0),
		WithMaxBackoff(100*time.Millisecond),
		WithRetryCount(3))
	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	logger.Reset()

	// Test infinite retry with context cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = Retry(ctx, logger, alwaysFailFn,
		WithInfiniteRetry(),
		WithExponentialBackoff(),
		WithBackoffDurationType(10*time.Millisecond))
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
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
			simulateErrors: 2, // Fails three times
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
			logger := mocklogger.NewTestLogger()
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
			//	if len(recordedSleeps) != len(tc.expectedSleeps) {
			//		t.Errorf("Expected %d sleep calls but got %d", len(tc.expectedSleeps), len(recordedSleeps))
			//	}
			//
			//	for i, expected := range tc.expectedSleeps {
			//		if recordedSleeps[i] != expected {
			//			t.Errorf("Expected sleep of %v but got %v on attempt %d", expected, recordedSleeps[i], i+1)
			//		}
			//	}
		})
	}
}
