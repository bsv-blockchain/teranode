package retry

import (
	"context"
	"time"
)

// this global variable is used to store the sleep function, and is used for testing purposes
var sleepFunc = func(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// BackoffAndSleep is a context-aware utility function that will sleep according to given parameters
// It will sleep for (backoffMultiplier*retries) +1 seconds, and then return
// The function returns early if the context is cancelled
// Parameters:
// ctx: Context for cancellation
// retries: The number of times the function has been retried
// backoffMultiplier: The multiplier that will be used to calculate the backoff time
// durationType: The type of duration that will be used to calculate the backoff time
// Returns:
// error: nil if sleep completed, or context error if cancelled
func BackoffAndSleep(ctx context.Context, retries int, backoffMultiplier int, durationType time.Duration) error {
	backoff := (backoffMultiplier * retries) + 1
	backoffPeriod := time.Duration(backoff) * durationType
	return sleepFunc(ctx, backoffPeriod)
}

// CappedExponentialBackoff calculates the next backoff duration using exponential backoff
// with a maximum cap. The backoff starts at initialBackoff and is multiplied by backoffFactor
// each time, up to maxBackoff.
// Parameters:
// currentBackoff: The current backoff duration
// backoffFactor: The multiplier for exponential backoff (e.g., 2.0 for doubling)
// maxBackoff: The maximum backoff duration
// Returns:
// time.Duration: The next backoff duration to use
func CappedExponentialBackoff(currentBackoff time.Duration, backoffFactor float64, maxBackoff time.Duration) time.Duration {
	nextBackoff := time.Duration(float64(currentBackoff) * backoffFactor)
	if nextBackoff > maxBackoff {
		return maxBackoff
	}
	return nextBackoff
}
