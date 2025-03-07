// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"context"
	"net/http"
	"time"

	"github.com/IBM/sarama"
)

// HealthChecker creates a function that checks the health of a Kafka cluster.
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
//
// The returned health check function attempts to establish a connection to the Kafka cluster.
// If successful, it indicates the cluster is healthy. This check doesn't verify individual
// consumer or producer functionality but rather tests basic connectivity to the cluster.
func HealthChecker(_ context.Context, brokersURL []string) func(ctx context.Context, checkLiveness bool) (int, string, error) {
	/*
		There isn't a built-in way to check the health of a Kafka cluster.
		So, we need to connect to the cluster and check if we can connect to it.
		If we can't connect to it, we return a 503.
		If we can connect to it, we return a 200.
		In reality this isn't testing the Kafka consumer or producer, it's testing
		that the Kafka cluster is healthy.
		However, in production every producer and consumer reconnects without fuss so we assume
		that if we can connect to the brokers then the cluster is healthy.
		Not perfect but it's something.
	*/
	return func(ctx context.Context, checkLiveness bool) (int, string, error) {
		if brokersURL == nil {
			return http.StatusOK, "Kafka is not configured - skipping health check", nil
		}

		config := sarama.NewConfig()
		config.Version = sarama.V2_1_0_0
		config.Admin.Retry.Max = 0
		config.Admin.Timeout = 100 * time.Millisecond
		config.Net.DialTimeout = 100 * time.Millisecond
		config.Net.ReadTimeout = 100 * time.Millisecond
		config.Net.WriteTimeout = 100 * time.Millisecond
		config.Metadata.Retry.Max = 0
		config.Metadata.Full = true
		config.Metadata.AllowAutoTopicCreation = false

		kafkaClusterAdmin, err := sarama.NewClusterAdmin(brokersURL, config)
		if err != nil {
			return http.StatusServiceUnavailable, "Failed to connect to Kafka", err
		}

		if err = kafkaClusterAdmin.Close(); err != nil {
			return http.StatusServiceUnavailable, "Failed to close Kafka connection", err
		}

		return http.StatusOK, "Kafka is healthy", nil
	}
}
