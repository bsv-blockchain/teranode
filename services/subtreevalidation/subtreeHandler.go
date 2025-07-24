// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"net/url"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"google.golang.org/protobuf/proto"
)

// consumerMessageHandler returns a function that processes Kafka messages for subtree validation.
// It handles both recoverable and unrecoverable errors appropriately.
func (u *Server) consumerMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		for {
			if !u.pauseSubtreeProcessing.Load() {
				break
			}

			u.logger.Warnf("[consumerMessageHandler] Subtree processing is paused, waiting to resume...")
			time.Sleep(100 * time.Millisecond)
		}

		state, err := u.blockchainClient.GetFSMCurrentState(ctx)
		if err != nil {
			return errors.NewProcessingError("[consumerMessageHandler] failed to get FSM current state", err)
		}

		if *state == blockchain.FSMStateCATCHINGBLOCKS {
			return nil
		}

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
				u.logger.Infof("[consumerMessageHandler] Subtree already exists, marking Kafka message as completed.\n")
				return nil
			}

			// currently, the following cases are considered recoverable:
			// ERR_SERVICE_ERROR, ERR_STORAGE_ERROR, ERR_CONTEXT_ERROR, ERR_THRESHOLD_EXCEEDED, ERR_EXTERNAL_ERROR
			// all other cases, including but not limited to, are considered as unrecoverable:
			// ERR_PROCESSING, ERR_SUBTREE_INVALID, ERR_SUBTREE_INVALID_FORMAT, ERR_INVALID_ARGUMENT, ERR_SUBTREE_EXISTS, ERR_TX_INVALID

			// if error is not nil, check if the error is a recoverable error.
			// If the error is a recoverable error, then return the error, so that it kafka message is not marked as committed.
			// So the message will be consumed again.
			notFoundError := errors.Is(err, errors.ErrSubtreeNotFound)
			recoverableError := errors.Is(err, errors.ErrServiceError) || errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrThresholdExceeded) || errors.Is(err, errors.ErrContextCanceled) || errors.Is(err, errors.ErrExternal)

			if recoverableError && !notFoundError {
				u.logger.Errorf("[consumerMessageHandler] Recoverable error processing kafka message, returning error, not marking Kafka message as complete: %v", err)
				return err
			}

			// error is not nil and not recoverable, so it is unrecoverable error, and it should not be tried again
			// kafka message should be committed, so return nil to mark message.
			u.logger.Errorf("[consumerMessageHandler] Unrecoverable error processing kafka message, marking Kafka message as completed: %v", err)

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

		var (
			kafkaMsg kafkamessage.KafkaSubtreeTopicMessage
			subtree  *subtreepkg.Subtree
		)

		if err := proto.Unmarshal(msg.Value, &kafkaMsg); err != nil {
			u.logger.Errorf("Failed to unmarshal kafka message: %v", err)
			return err
		}

		hash, err := chainhash.NewHashFromStr(kafkaMsg.Hash)
		if err != nil {
			u.logger.Errorf("Failed to parse block hash from message: %v", err)
			return err
		}

		baseURL, err := url.Parse(kafkaMsg.URL)
		if err != nil {
			u.logger.Errorf("Failed to parse block base url from message: %v", err)
			return err
		}

		if len(msg.Value) < 32 {
			u.logger.Errorf("Received subtree message of %d bytes", len(msg.Value))
			return errors.New(errors.ERR_INVALID_ARGUMENT, "Received subtree message of %d bytes", len(msg.Value))
		}

		u.logger.Infof("Received subtree message for %s from %s", hash.String(), baseURL.String())
		defer u.logger.Infof("Finished processing subtree message for %s", hash.String())

		gotLock, _, releaseLockFunc, err := q.TryLockIfFileNotExists(ctx, hash, fileformat.FileTypeSubtree)
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
			BaseURL:       baseURL.String(),
			TxHashes:      nil,
			AllowFailFast: true,
		}

		if subtree, err = u.ValidateSubtreeInternal(ctx, v, 0, nil); err != nil {
			return err
		}

		// if no error was thrown, remove all the transactions from this subtree from the orphanage
		for _, node := range subtree.Nodes {
			u.orphanage.Delete(node.Hash)
		}
	}

	return nil
}
