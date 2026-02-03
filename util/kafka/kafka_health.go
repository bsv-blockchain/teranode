// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"context"
	"net/http"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// HealthChecker creates a function that checks the health of a Kafka cluster using franz-go.
// It returns a health check function that can be used to verify the Kafka cluster's status.
//
// Parameters:
//   - ctx: Context for the health check operation
//   - brokersURL: List of Kafka broker URLs to check
//
// Returns:
//   - A function that performs the actual health check with the following signature:
//     func(ctx context.Context, checkLiveness bool) (int, string, error)
//     where:
//   - int: HTTP status code (200 for healthy, 503 for unhealthy)
//   - string: Health check message
//   - error: Any error encountered during the health check
func HealthChecker(_ context.Context, brokersURL []string) func(ctx context.Context, checkLiveness bool) (int, string, error) {
	return func(ctx context.Context, checkLiveness bool) (int, string, error) {
		if brokersURL == nil {
			return http.StatusOK, "Kafka is not configured - skipping health check", nil
		}

		// Create a minimal franz-go client for health checking
		opts := []kgo.Opt{
			kgo.SeedBrokers(brokersURL...),
			kgo.ConnIdleTimeout(100 * time.Millisecond),
			kgo.MetadataMinAge(100 * time.Millisecond),
			kgo.RetryBackoffFn(func(int) time.Duration { return 0 }),
		}

		client, err := kgo.NewClient(opts...)
		if err != nil {
			return http.StatusServiceUnavailable, "Failed to connect to Kafka", err
		}
		defer client.Close()

		// Ping the cluster by fetching metadata
		pingCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()

		if err := client.Ping(pingCtx); err != nil {
			return http.StatusServiceUnavailable, "Failed to connect to Kafka", err
		}

		return http.StatusOK, "Kafka is healthy", nil
	}
}
