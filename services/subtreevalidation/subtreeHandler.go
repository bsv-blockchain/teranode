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

// consumerMessageHandler returns a Kafka message handler for subtree validation.
//
// The handler pauses all subtree processing during execution, skips processing when
// blockchain FSM is in CATCHINGBLOCKS state, and classifies errors to prevent
// infinite retry loops on unrecoverable failures.
func (u *Server) consumerMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
	WAITFORPAUSE:
		for {
			select {
			case <-ctx.Done():
				u.logger.Warnf("[consumerMessageHandler] Context done, stopping processing: %v", ctx.Err())
				return ctx.Err()
			default:
				// continue processing
				if !u.pauseSubtreeProcessing.Load() {
					break WAITFORPAUSE
				}

				u.logger.Warnf("[consumerMessageHandler] Subtree processing is paused, waiting to resume...")
				time.Sleep(1 * time.Second)
			}
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
				u.logger.Infof("[consumerMessageHandler] Subtree already exists - skipping")
				return nil
			}

			if errors.Is(err, errors.ErrContextCanceled) {
				// if the error is context canceled, then return nil, so that the kafka message is marked as committed.
				// So the message will not be consumed again.
				u.logger.Infof("[consumerMessageHandler] Context canceled, skipping: %v", err)
				return nil
			}

			u.logger.Errorf("[consumerMessageHandler] error processing kafka message, %v", err)
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (u *Server) subtreesHandler(msg *kafka.KafkaMessage) error {
	if msg != nil {
		blockIDsMap := u.currentBlockIDsMap.Load()
		if blockIDsMap == nil {
			return errors.NewProcessingError("failed to get block IDs map during subtree validation")
		}

		bestBlockHeaderMeta := u.bestBlockHeaderMeta.Load()
		if bestBlockHeaderMeta == nil {
			return errors.NewProcessingError("failed to get best block header meta during subtree validation")
		}

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

		// validate the subtree as if it is for the next block height
		// this is because subtrees are always validated ahead of time before they are needed for a block
		if subtree, err = u.ValidateSubtreeInternal(ctx, v, bestBlockHeaderMeta.Height+1, *blockIDsMap); err != nil {
			return err
		}

		// if no error was thrown, remove all the transactions from this subtree from the orphanage
		for _, node := range subtree.Nodes {
			u.orphanage.Delete(node.Hash)
		}
	}

	return nil
}
