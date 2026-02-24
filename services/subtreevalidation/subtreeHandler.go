// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"math"
	"net/url"
	"runtime"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// subtreeMessageHandler returns a Kafka message handler for subtree validation.
//
// The handler skips processing when blockchain FSM is in CATCHINGBLOCKS state and classifies
// errors to prevent infinite retry loops on unrecoverable failures.
//
// rather than blocking in this handler. This prevents session timeouts and improves resource usage.
func (u *Server) subtreeMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(int(math.Max(4, float64(runtime.NumCPU()))))

	return func(msg *kafka.KafkaMessage) error {
		if msg == nil {
			u.logger.Errorf("[subtreeMessageHandler] received nil message")
			return nil
		}

		if len(msg.Value) < 32 {
			u.logger.Errorf("[subtreeMessageHandler] received subtree message of only %d bytes", len(msg.Value))
			return nil
		}

		// Check if context is already cancelled
		select {
		case <-gCtx.Done():
			u.logger.Warnf("[subtreeMessageHandler] Context done, stopping processing: %v", gCtx.Err())
			return gCtx.Err()
		default:
		}

		state, err := u.blockchainClient.GetFSMCurrentState(gCtx)
		if err != nil {
			return errors.NewProcessingError("[subtreeMessageHandler] failed to get FSM current state", err)
		}

		if *state == blockchain.FSMStateCATCHINGBLOCKS {
			return nil
		}

		var kafkaMsg kafkamessage.KafkaSubtreeTopicMessage
		if err := proto.Unmarshal(msg.Value, &kafkaMsg); err != nil {
			u.logger.Errorf("[subtreeMessageHandler] failed to unmarshal kafka message: %v", err)
			return nil
		}

		hash, err := chainhash.NewHashFromStr(kafkaMsg.Hash)
		if err != nil {
			u.logger.Errorf("[subtreeMessageHandler] failed to parse block hash from message: %v", err)
			return nil
		}

		baseURL, err := url.Parse(kafkaMsg.URL)
		if err != nil {
			u.logger.Errorf("[subtreeMessageHandler] failed to parse block base url from message: %v", err)
			return nil
		}

		// Run the subtree handler in a goroutine managed by errgroup.
		// We validate subtrees on best effort basis - no retries needed.
		g.Go(func() error {
			err := u.subtreesHandler(gCtx, hash, baseURL, kafkaMsg.PeerId)
			if err == nil {
				return nil
			}

			if errors.Is(err, errors.ErrSubtreeExists) {
				u.logger.Warnf("[subtreeMessageHandler] Subtree already exists - skipping")
				return nil
			}

			if errors.Is(err, errors.ErrContextCanceled) {
				u.logger.Warnf("[subtreeMessageHandler] Context canceled, skipping: %v", err)
				return nil
			}

			u.logger.Errorf("[subtreeMessageHandler] error processing kafka message, %v", err)
			return nil
		})

		return nil
	}
}

func (u *Server) subtreesHandler(ctx context.Context, hash *chainhash.Hash, baseURL *url.URL, peerID string, validationOptions ...validator.Option) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "subtreesHandler",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusSubtreeValidationValidateSubtreeHandler),
		tracing.WithLogMessage(u.logger, "[subtreesHandler] Received subtree message for %s from %s", hash.String(), baseURL.String()),
	)
	defer deferFn()

	blockIDsMap := u.currentBlockIDsMap.Load()
	if blockIDsMap == nil {
		return errors.NewProcessingError("[subtreesHandler] failed to get block IDs map during subtree validation")
	}

	bestBlockHeaderMeta := u.bestBlockHeaderMeta.Load()
	if bestBlockHeaderMeta == nil {
		return errors.NewProcessingError("[subtreesHandler] failed to get best block header meta during subtree validation")
	}

	gotLock, _, releaseLockFunc, err := u.quorum.TryLockIfFileNotExists(ctx, hash, fileformat.FileTypeSubtree)
	if err != nil {
		return errors.NewProcessingError("[subtreesHandler] error getting lock for Subtree %s", hash.String(), err)
	}
	defer releaseLockFunc()

	if !gotLock {
		return errors.NewSubtreeExistsError("[subtreesHandler] Subtree lock %s already exists", hash.String())
	}

	v := ValidateSubtree{
		SubtreeHash:   *hash,
		BaseURL:       baseURL.String(),
		PeerID:        peerID,
		TxHashes:      nil,
		AllowFailFast: true,
	}

	// validate the subtree as if it is for the next block height
	// this is because subtrees are always validated ahead of time before they are needed for a block
	subtree, err := u.ValidateSubtreeInternal(ctx, v, bestBlockHeaderMeta.Height+1, *blockIDsMap, validationOptions...)
	if err != nil {
		return err
	}

	// if no error was thrown, remove all the transactions from this subtree from the orphanage
	for _, node := range subtree.Nodes {
		u.orphanage.Delete(node.Hash)
	}

	return nil
}
