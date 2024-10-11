package kafka

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

/*
HealthChecker is a function that checks the health of a Kafka cluster.
It returns a function that can be used to check the health of a Kafka cluster.
*/
func HealthChecker(_ context.Context, kafkaURL *url.URL) func(ctx context.Context, checkLiveness bool) (int, string, error) {
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
		if kafkaURL == nil {
			return http.StatusOK, "Kafka is not configured - skipping health check", nil
		}

		brokersURL := strings.Split(kafkaURL.Host, ",")

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
