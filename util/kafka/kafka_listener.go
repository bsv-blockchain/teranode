// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"context"
	"net/url"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// StartKafkaControlledListener starts a Kafka listener that can be controlled via a channel.
// It manages the lifecycle of a Kafka listener based on control signals received through kafkaControlChan.
//
// Parameters:
//   - ctx: Context for controlling the listener's lifecycle
//   - logger: Logger instance for logging operations
//   - kafkaControlChan: Channel for receiving control signals (true to start, false to stop)
//   - kafkaConfigURL: Kafka configuration URL
//   - listener: Function that implements the actual listening logic
func StartKafkaControlledListener(ctx context.Context, logger ulogger.Logger, groupID string, kafkaControlChan chan bool, kafkaConfigURL *url.URL,
	listener func(ctx context.Context, kafkaURL *url.URL, groupID string)) {
	var (
		kafkaCtx    context.Context
		kafkaCancel context.CancelFunc
	)

	for {
		select {
		case <-ctx.Done():
			if kafkaCancel != nil {
				kafkaCancel()
			}
			return
		case control := <-kafkaControlChan:
			if control { // Start signal
				if kafkaCancel != nil {
					// Listener is already running, no need to start
					continue
				}

				kafkaCtx, kafkaCancel = context.WithCancel(ctx)

				logger.Infof("[Legacy Manager] starting Kafka listener for %s", kafkaConfigURL.String())

				go listener(kafkaCtx, kafkaConfigURL, groupID)
			} else if kafkaCancel != nil {
				logger.Infof("[Legacy Manager] stopping Kafka listener for %s", kafkaConfigURL.String())
				kafkaCancel() // Stop the listener
				kafkaCancel = nil
			}
		}
	}
}

// StartKafkaListener starts a new Kafka listener for the given URL and group ID
// this function is a utility function to cleanly stop the listener when the context is done
// the consumerFn is called for each message received
// NOTE: this functionality could be moved into the client.Start() function
//
// Parameters:
//   - ctx: Context for controlling the listener's lifecycle
//   - logger: Logger instance for logging operations
//   - kafkaURL: Kafka configuration URL
//   - groupID: Consumer group identifier
//   - autoCommit: Whether to enable auto-commit of offsets
//   - consumerFn: Function to process received messages
//   - kafkaSettings: Kafka settings for TLS and debug logging (can be nil for defaults)
func StartKafkaListener(ctx context.Context, logger ulogger.Logger, kafkaURL *url.URL, groupID string, autoCommit bool, consumerFn func(msg *KafkaMessage) error, kafkaSettings *settings.KafkaSettings) {
	client, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, groupID, autoCommit, kafkaSettings)
	if err != nil {
		logger.Errorf("failed to start Kafka listener for %s: %v", kafkaURL.String(), err)
	}

	// create a new context for the consumer, so we are able to stop the consumer gracefully
	kCtx, kCancel := context.WithCancel(context.Background())

	// start a go routine to close the client when the context is done
	go func() {
		<-ctx.Done()

		// close the client gracefully
		if err = client.Close(); err != nil {
			logger.Errorf("failed to close Kafka client: %v", err)
		}

		// cancel the context for the consumer
		kCancel()
	}()

	// start the consumer
	client.Start(kCtx, consumerFn)
}
