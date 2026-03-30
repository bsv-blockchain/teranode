// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"context"
	"net/http"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// HealthChecker returns a function that checks basic connectivity to the Kafka cluster.
// It does not verify individual consumer or producer functionality.
//
// Kafka brokers do not expose a dedicated health endpoint; the usual approach is to verify
// connectivity with a metadata (or equivalent) request. This check creates a short-lived
// client, pings the cluster, and returns 200 if connectable, 503 otherwise. In production,
// producers and consumers reconnect on their own; we assume that if we can connect to the
// brokers, the cluster is healthy.
//
// Parameters:
//   - ctx: Context for the health check operation (unused at construction time).
//   - brokersURL: List of Kafka broker URLs to check.
//
// Returns a function with signature:
//
//	func(ctx context.Context, checkLiveness bool) (int, string, error)
//
// where int is HTTP status (200 healthy, 503 unhealthy), string is a message, and error is non-nil on failure.
func HealthChecker(_ context.Context, brokersURL []string) func(ctx context.Context, checkLiveness bool) (int, string, error) {
	return func(ctx context.Context, checkLiveness bool) (int, string, error) {
		if brokersURL == nil || len(brokersURL) == 0 {
			return http.StatusOK, "Kafka is not configured - skipping health check", nil
		}

		if checkLiveness {
			return http.StatusOK, "Kafka liveness (skipped)", nil
		}

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

		pingCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		if err := client.Ping(pingCtx); err != nil {
			return http.StatusServiceUnavailable, "Failed to connect to Kafka", err
		}

		return http.StatusOK, "Kafka is healthy", nil
	}
}
