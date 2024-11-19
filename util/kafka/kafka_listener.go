package kafka

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/ubsv/ulogger"
)

func StartKafkaControlledListener(ctx context.Context, logger ulogger.Logger, kafkaControlChan chan bool, kafkaConfigURL *url.URL,
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
		case control := <-kafkaControlChan:
			if control { // Start signal
				if kafkaCancel != nil {
					// Listener is already running, no need to start
					continue
				}

				kafkaCtx, kafkaCancel = context.WithCancel(ctx)

				logger.Infof("[Legacy Manager] starting Kafka listener for %s", kafkaConfigURL.String())

				go listener(kafkaCtx, kafkaConfigURL, "legacy")
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
func StartKafkaListener(ctx context.Context, logger ulogger.Logger, kafkaURL *url.URL, groupID string, autoCommit bool, consumerFn func(msg *KafkaMessage) error) {
	client, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, groupID, autoCommit)
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
