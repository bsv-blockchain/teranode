package retry

import (
	"context"
	"errors"
	"fmt"
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
	fmt.Println("Test case 1 passed")
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
