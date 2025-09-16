package retry

import (
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
)

const (
	// maxLinearBackoffRetries is the maximum number of retries to consider for linear backoff
	// when using infinite retry to prevent excessive sleep delays
	maxLinearBackoffRetries = 10
)

// Retry will retry a function call a number of times, with a backoff time between each retry.
// Parameters:
// ctx: The context that will be used to control the retry operation
// logger: The logger that will be used to log messages
// f: The function that will be retried. It should return an error, which will be checked to determine if the function was successful
// opts: The options that will be used to control the retry operation. These can be set using the WithMessage, WithBackoffDurationType, WithBackoffMultiplier, and WithRetryCount functions
// Returns:
// T: The result of the function call, or the zero value of T if the function was not successful
// error: The error returned by the function, or nil if the function was successful
// func Retry[T any](ctx context.Context, logger ulogger.Logger, f func() (T, error), opts ...Options) (T, error) {
func Retry[T any](ctx context.Context, logger ulogger.Logger, f func() (T, error), opts ...Options) (T, error) {
	var (
		result T
		err    error
	)

	// duplicate the logger, showing the source as coming from the caller of this function
	logger = logger.Duplicate(ulogger.WithSkipFrame(1))

	// NewSetOptions creates a new SetOptions struct with the default values,
	// and then applies the options provided in the opts slice
	setOptions := NewSetOptions(opts...)

	// Call the function for the first time
	result, err = f()
	if err == nil {
		// This worked successfully first time, so return the result and nil
		return result, nil
	}

	// If we reach here, we have an error, so we need to retry
	// Initialize backoff parameters for exponential backoff
	var currentBackoff time.Duration
	if setOptions.ExponentialBackoff {
		currentBackoff = setOptions.BackoffDurationType
	}

	// Determine retry limit
	maxRetries := setOptions.RetryCount
	if setOptions.InfiniteRetry {
		maxRetries = -1 // Use -1 to indicate infinite retries
	}

	// Loop through the number of retries (or infinite if maxRetries is -1)
	for i := 0; maxRetries == -1 || i < maxRetries; i++ {
		select {
		case <-ctx.Done(): // Check if the context has been cancelled
			return result, ctx.Err()

		default:
			// Calculate wait time for logging
			var waitTime time.Duration
			if setOptions.ExponentialBackoff {
				waitTime = currentBackoff
			} else {
				// Calculate linear backoff wait time
				retryCountForBackoff := i
				if setOptions.InfiniteRetry && i > maxLinearBackoffRetries {
					retryCountForBackoff = maxLinearBackoffRetries
				}
				backoff := (setOptions.BackoffMultiplier * retryCountForBackoff) + 1
				waitTime = time.Duration(backoff) * setOptions.BackoffDurationType
			}

			// Log the retry message with wait time
			logger.Warnf(setOptions.Message+" (attempt %d): %v, trying again in %.1f seconds",
				i+1, err, waitTime.Seconds())

			// Wait before retrying
			if setOptions.ExponentialBackoff {
				// Use exponential backoff
				select {
				case <-ctx.Done():
					return result, ctx.Err()
				case <-time.After(currentBackoff):
					// Update backoff for next iteration
					currentBackoff = CappedExponentialBackoff(currentBackoff, setOptions.BackoffFactor, setOptions.MaxBackoff)
				}
			} else {
				// Use linear backoff
				// Cap the retry count for linear backoff to prevent infinite sleep times
				retryCountForBackoff := i
				if setOptions.InfiniteRetry && i > maxLinearBackoffRetries {
					retryCountForBackoff = maxLinearBackoffRetries // Cap for infinite retry to prevent excessive delays
				}
				if err := BackoffAndSleep(ctx, retryCountForBackoff, setOptions.BackoffMultiplier, setOptions.BackoffDurationType); err != nil {
					logger.Errorf("Context cancelled during backoff, stopping retries")
					return result, err
				}
			}

			// Call the function
			result, err = f()

			// If the function was successful, return result
			if err == nil {
				return result, nil
			}
		}
	}

	// Log the retry message for finite retries
	if !setOptions.InfiniteRetry {
		logger.Warnf(setOptions.Message+" (given up after %d attempts): %v", setOptions.RetryCount, err)
	}

	return result, err
}
