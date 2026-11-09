// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"math"
	"net/url"
	"runtime"
	"time"

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
// While the blockchain FSM is not RUNNING the handler does not validate peer subtrees; those
// announced while catching up are kept (see skippedSubtrees) and validated once the node is
// RUNNING again. It classifies
// errors to prevent infinite retry loops on unrecoverable failures.
//
// rather than blocking in this handler. This prevents session timeouts and improves resource usage.
func (u *Server) subtreeMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(int(math.Max(4, float64(runtime.NumCPU()))))

	return func(msg *kafka.KafkaMessage) error {
		// Taken before any wait, including for a slot in g: a subtree kept for replay is aged
		// from its receipt.
		receivedAt := time.Now()

		if msg == nil {
			u.logger.Errorf("[subtreeMessageHandler] received nil message")
			prometheusSubtreeKafkaMalformed.WithLabelValues("nil_message").Inc()
			return nil
		}

		if len(msg.Value) < 32 {
			u.logger.Errorf("[subtreeMessageHandler] received subtree message of only %d bytes", len(msg.Value))
			prometheusSubtreeKafkaMalformed.WithLabelValues("too_short").Inc()
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

		// Peer subtrees are only validated when caught up. IDLE can follow an
		// operator STOP mid-catchup, with a UTXO set far behind the tip. Those
		// announced while catching up are kept and validated once the node is
		// RUNNING again.
		if *state != blockchain.FSMStateRUNNING {
			u.recordSkippedSubtreeMessage(gCtx, *state, msg.Value)
			return nil
		}

		u.replaySkippedSubtrees(gCtx)

		// In BlocksOnly mode, skip processing peer-announced subtrees (only process subtrees from blocks)
		if u.settings.SubtreeValidation.BlocksOnly {
			return nil
		}

		var kafkaMsg kafkamessage.KafkaSubtreeTopicMessage
		if err := proto.Unmarshal(msg.Value, &kafkaMsg); err != nil {
			u.logger.Errorf("[subtreeMessageHandler] failed to unmarshal kafka message: %v", err)
			prometheusSubtreeKafkaMalformed.WithLabelValues("unmarshal_failure").Inc()
			return nil
		}

		hash, err := chainhash.NewHashFromStr(kafkaMsg.Hash)
		if err != nil {
			u.logger.Errorf("[subtreeMessageHandler] failed to parse block hash from message: %v", err)
			prometheusSubtreeKafkaMalformed.WithLabelValues("bad_hash").Inc()
			return nil
		}

		baseURL, err := url.Parse(kafkaMsg.URL)
		if err != nil {
			u.logger.Errorf("[subtreeMessageHandler] failed to parse block base url from message: %v", err)
			prometheusSubtreeKafkaMalformed.WithLabelValues("bad_url").Inc()
			return nil
		}

		// Run the subtree handler in a goroutine managed by errgroup.
		// We validate subtrees on best effort basis - no retries needed.
		g.Go(func() error {
			err := u.subtreesHandler(gCtx, receivedAt, hash, baseURL, kafkaMsg.PeerId)
			if err == nil {
				return nil
			}

			if errors.Is(err, errors.ErrSubtreeExists) {
				prometheusSubtreeAlreadyExistsSkipped.Inc()
				u.logger.Debugf("[subtreeMessageHandler] Subtree already exists - skipping")
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

// subtreesHandler validates a peer subtree received at receivedAt. A subtree kept for replay
// because the node is catching up is aged from receivedAt.
func (u *Server) subtreesHandler(ctx context.Context, receivedAt time.Time, hash *chainhash.Hash, baseURL *url.URL, peerID string, validationOptions ...validator.Option) error {
	// Wait for a validation slot before taking the subtree's quorum lock:
	// block validation (CheckBlockSubtrees) waits on that lock, so a subtree
	// queued here while holding it would make a block wait for the queue. A
	// subtree validated while this one was queued is then skipped by the lock
	// check below.
	releaseAdmission, err := u.admitPeerSubtree(ctx)
	if err != nil {
		return err
	}
	defer releaseAdmission()

	// The FSM was checked when the message arrived; the node may have left
	// RUNNING while this subtree was queued, and peer subtrees are only
	// validated when caught up.
	state, err := u.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		return errors.NewProcessingError("[subtreesHandler] failed to get FSM current state", err)
	}

	if *state != blockchain.FSMStateRUNNING {
		if u.recordSkippedSubtree(ctx, *state, skippedSubtree{hash: *hash, baseURL: baseURL, peerID: peerID, recordedAt: receivedAt}) {
			prometheusSkippedSubtrees.WithLabelValues("recorded").Inc()
		}

		return nil
	}

	result, err := u.validateAdmittedPeerSubtree(ctx, hash, baseURL, peerID, validationOptions...)
	if err != nil {
		return err
	}

	if result != peerSubtreeStored {
		return errors.NewSubtreeExistsError("[subtreesHandler] Subtree lock %s already exists", hash.String())
	}

	return nil
}

// admitPeerSubtree waits for a peer-subtree validation slot (subtreevalidation_peerSubtreeConcurrency)
// and returns the function that frees it.
func (u *Server) admitPeerSubtree(ctx context.Context) (func(), error) {
	waitStart := time.Now()

	release, err := u.peerSubtreeAdmission.acquire(ctx, u.settings.SubtreeValidation.PeerSubtreeConcurrency)
	if err != nil {
		return nil, err
	}

	prometheusPeerSubtreeAdmissionWait.Observe(time.Since(waitStart).Seconds())

	return release, nil
}

// peerSubtreeResult is how validateAdmittedPeerSubtree ended without an error.
type peerSubtreeResult int

const (
	// peerSubtreeStored: the subtree was validated and stored, or was stored meanwhile.
	peerSubtreeStored peerSubtreeResult = iota

	// peerSubtreeExists: the subtree was already stored, so nothing was done.
	peerSubtreeExists

	// peerSubtreeLocked: another validation holds the subtree's quorum lock and nothing is stored
	// yet, so nothing was done. That validation may still fail.
	peerSubtreeLocked
)

// validateAdmittedPeerSubtree validates a peer subtree that holds an admission slot, on a node
// that is RUNNING.
func (u *Server) validateAdmittedPeerSubtree(ctx context.Context, hash *chainhash.Hash, baseURL *url.URL, peerID string, validationOptions ...validator.Option) (peerSubtreeResult, error) {
	// The span starts after the wait, so the handler histogram measures
	// validation only; the wait has its own metric.
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "subtreesHandler",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusSubtreeValidationValidateSubtreeHandler),
		tracing.WithDebugLogMessage(u.logger, "[subtreesHandler] Received subtree message for %s from %s", hash.String(), baseURL.String()),
	)
	defer deferFn()

	// Read the chain view after the wait: a block accepted while this subtree
	// was queued must be in blockIds and the height it is validated for.
	blockIDsMap := u.currentBlockIDsMap.Load()
	if blockIDsMap == nil {
		return 0, errors.NewProcessingError("[subtreesHandler] failed to get block IDs map during subtree validation")
	}

	bestBlockHeaderMeta := u.bestBlockHeaderMeta.Load()
	if bestBlockHeaderMeta == nil {
		return 0, errors.NewProcessingError("[subtreesHandler] failed to get best block header meta during subtree validation")
	}

	gotLock, exists, releaseLockFunc, err := u.quorum.TryLockIfFileNotExists(ctx, hash, fileformat.FileTypeSubtree)
	if err != nil {
		return 0, errors.NewProcessingError("[subtreesHandler] error getting lock for Subtree %s", hash.String(), err)
	}
	defer releaseLockFunc()

	if !gotLock {
		if exists {
			return peerSubtreeExists, nil
		}

		return peerSubtreeLocked, nil
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
	// A nil subtree with no error means the subtree already existed in the store - another
	// goroutine completed the write between TryLockIfFileNotExists and the in-store existence
	// check. Nothing to clean up, and it is stored either way.
	if _, err = u.ValidateSubtreeInternal(ctx, v, bestBlockHeaderMeta.Height+1, *blockIDsMap, validationOptions...); err != nil {
		return 0, err
	}

	return peerSubtreeStored, nil
}
