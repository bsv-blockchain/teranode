package retry

import (
	"context"

	"github.com/bitcoin-sv/ubsv/ulogger"
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
	var result T
	var err error

	// NewSetOptions creates a new SetOptions struct with the default values,
	// and then applies the options provided in the opts slice
	setOptions := NewSetOptions(opts...)

	for i := 0; i < setOptions.RetryCount; i++ {
		select {
		case <-ctx.Done(): // Check if the context has been cancelled
			logger.Errorf("Context cancelled, stopping retries")
			return result, ctx.Err()
		default:
			// Log the retry message
			logger.Infof(setOptions.Message, " (attempt %d): ", i+1)

			// Call the function
			result, err = f()

			// If the function was successful, return nil
			if err == nil {
				return result, nil
			}

			// Backoff and sleep for the backoff time
			BackoffAndSleep(i, setOptions.BackoffMultiplier, setOptions.BackoffDurationType)
		}
	}
	return result, err
}
