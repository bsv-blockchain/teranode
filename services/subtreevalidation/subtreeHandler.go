// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util/kafka"
	"github.com/libsv/go-bt/v2/chainhash"
)

// consumerMessageHandler returns a function that processes Kafka messages for subtree validation.
// It handles both recoverable and unrecoverable errors appropriately.
func (u *Server) consumerMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		errCh := make(chan error, 1)
		go func() {
			errCh <- u.subtreesHandler(msg)
		}()

		select {
		case err := <-errCh:
			// if err is nil, it means function is successfully executed, return nil.
			if err == nil {
				return nil
			}

			if errors.Is(err, errors.ErrSubtreeExists) {
				// if the error is subtree exists, then return nil, so that the kafka message is marked as committed.
				// So the message will not be consumed again.
				u.logger.Infof("Subtree already exists, marking Kafka message as completed.\n")
				return nil
			}

			// currently, the following cases are considered recoverable:
			// ERR_SERVICE_ERROR, ERR_STORAGE_ERROR, ERR_CONTEXT_ERROR, ERR_THRESHOLD_EXCEEDED, ERR_EXTERNAL_ERROR
			// all other cases, including but not limited to, are considered as unrecoverable:
			// ERR_PROCESSING, ERR_SUBTREE_INVALID, ERR_SUBTREE_INVALID_FORMAT, ERR_INVALID_ARGUMENT, ERR_SUBTREE_EXISTS, ERR_TX_INVALID

			// if error is not nil, check if the error is a recoverable error.
			// If the error is a recoverable error, then return the error, so that it kafka message is not marked as committed.
			// So the message will be consumed again.
			if errors.Is(err, errors.ErrServiceError) || errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrThresholdExceeded) || errors.Is(err, errors.ErrContextCanceled) || errors.Is(err, errors.ErrExternal) {
				u.logger.Errorf("Recoverable error (%v) processing kafka message %v for handling subtree, returning error, thus not marking Kafka message as complete.\n", msg, err)
				return err
			}

			// error is not nil and not recoverable, so it is unrecoverable error, and it should not be tried again
			// kafka message should be committed, so return nil to mark message.
			u.logger.Errorf("Unrecoverable error (%v) processing kafka message %v for handling subtree, marking Kafka message as completed.\n", msg, err)

			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (u *Server) subtreesHandler(msg *kafka.KafkaMessage) error {
	if msg != nil {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		startTime := time.Now()
		defer func() {
			prometheusSubtreeValidationValidateSubtreeHandler.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		}()

		if len(msg.Value) < 32 {
			u.logger.Errorf("Received subtree message of %d bytes", len(msg.Value))
			return errors.New(errors.ERR_INVALID_ARGUMENT, "Received subtree message of %d bytes", len(msg.Value))
		}

		hash, err := chainhash.NewHash(msg.Value[:32])
		if err != nil {
			u.logger.Errorf("Failed to parse subtree hash from message: %v", err)
			return errors.New(errors.ERR_INVALID_ARGUMENT, "Failed to parse subtree hash from message", err)
		}

		var baseURL string
		if len(msg.Value) > 32 {
			baseURL = string(msg.Value[32:])
		}

		u.logger.Infof("Received subtree message for %s from %s", hash.String(), baseURL)
		defer u.logger.Infof("Finished processing subtree message for %s", hash.String())

		gotLock, _, releaseLockFunc, err := q.TryLockIfNotExists(ctx, hash)
		if err != nil {
			u.logger.Infof("error getting lock for Subtree %s", hash.String())
			return errors.NewProcessingError("error getting lock for Subtree %s", hash.String(), err)
		}
		defer releaseLockFunc()

		if !gotLock {
			u.logger.Infof("Subtree %s already exists", hash.String())
			return errors.New(errors.ERR_SUBTREE_EXISTS, "Subtree %s already exists", hash.String())
		}

		v := ValidateSubtree{
			SubtreeHash:   *hash,
			BaseURL:       baseURL,
			TxHashes:      nil,
			AllowFailFast: true, // allow subtrees to fail fast, when getting from the network, will be retried if in a block
		}

		// Call the validateSubtreeInternal method
		if err = u.ValidateSubtreeInternal(ctx, v, 0); err != nil {
			u.logger.Errorf("Failed to validate subtree %s: %v", hash.String(), err)
			// Here we return the error directly without further wrapping, as ValidateSubtreeInternal categorizes the error
			return err
		}
	}

	return nil
}
