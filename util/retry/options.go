package retry

import (
	"time"
)

type Options func(s *SetOptions)

// SetOptions is a struct that contains the options that can be set for the RetryWithLogger function
// Message: The message that will be logged when retrying
// BackoffDurationType: The time to wait between each retry
// BackoffMultiplier: The multiplier that will be used to calculate the backoff time
// RetryCount: The number of times the function will be retried
// InfiniteRetry: If true, retry indefinitely until context cancellation
// ExponentialBackoff: If true, use exponential backoff instead of linear
// BackoffFactor: The factor for exponential backoff (e.g., 2.0 for doubling)
// MaxBackoff: The maximum backoff duration for exponential backoff
// By default:
// Message: "In RetryWithLogger, "
// BackoffDurationType: time.Second
// BackoffMultiplier: 2
// RetryCount: 3
// InfiniteRetry: false
// ExponentialBackoff: false
// BackoffFactor: 2.0
// MaxBackoff: 30 * time.Second
type SetOptions struct {
	Message             string
	BackoffDurationType time.Duration
	BackoffMultiplier   int
	RetryCount          int
	InfiniteRetry       bool
	ExponentialBackoff  bool
	BackoffFactor       float64
	MaxBackoff          time.Duration
}

func NewSetOptions(opts ...Options) *SetOptions {
	options := &SetOptions{}
	options.setDefaults()

	for _, opt := range opts {
		opt(options)
	}

	return options
}

func (o *SetOptions) setDefaults() {
	o.Message = "In RetryWithLogger, "
	o.BackoffDurationType = time.Second
	o.BackoffMultiplier = 2
	o.RetryCount = 3
	o.InfiniteRetry = false
	o.ExponentialBackoff = false
	o.BackoffFactor = 2.0
	o.MaxBackoff = 30 * time.Second
}

func WithMessage(message string) Options {
	return func(s *SetOptions) {
		s.Message = message
	}
}

func WithBackoffDurationType(retryTime time.Duration) Options {
	return func(s *SetOptions) {
		s.BackoffDurationType = retryTime
	}
}

func WithBackoffMultiplier(backoffMultiplier int) Options {
	return func(s *SetOptions) {
		s.BackoffMultiplier = backoffMultiplier
	}
}

func WithRetryCount(retryCount int) Options {
	return func(s *SetOptions) {
		s.RetryCount = retryCount
	}
}

func WithInfiniteRetry() Options {
	return func(s *SetOptions) {
		s.InfiniteRetry = true
	}
}

func WithExponentialBackoff() Options {
	return func(s *SetOptions) {
		s.ExponentialBackoff = true
	}
}

func WithBackoffFactor(factor float64) Options {
	return func(s *SetOptions) {
		s.BackoffFactor = factor
	}
}

func WithMaxBackoff(maxBackoff time.Duration) Options {
	return func(s *SetOptions) {
		s.MaxBackoff = maxBackoff
	}
}
