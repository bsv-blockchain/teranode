// Package blockvalidation implements block validation for Bitcoin SV nodes in Teranode.
//
// This package provides the core functionality for validating Bitcoin blocks, managing block subtrees,
// and processing transaction metadata. It is designed for high-performance operation at scale,
// supporting features like:
//
// - Concurrent block validation with optimistic mining support
// - Subtree-based block organization and validation
// - Transaction metadata caching and management
// - Automatic chain catchup when falling behind
// - Integration with Kafka for distributed operation
//
// The package exposes gRPC interfaces for block validation operations,
// making it suitable for use in distributed Teranode deployments.
package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// processBlockFound encapsulates information about a newly discovered block
// that requires validation. It includes both the block identifier and communication
// channels for handling validation results.
type processBlockFound struct {
	// hash uniquely identifies the discovered block using a double SHA256 hash
	hash *chainhash.Hash

	// baseURL specifies the peer URL from which additional block data can be retrieved
	// if needed during validation
	baseURL string

	// errCh receives any errors encountered during block validation and allows
	// synchronous waiting for validation completion
	errCh chan error
}

// processBlockCatchup contains information needed to process a block during chain catchup
// operations when the node has fallen behind the current chain tip.
type processBlockCatchup struct {
	// block contains the full block data to be validated, including header,
	// transactions, and subtrees
	block *model.Block

	// baseURL indicates the peer URL from which additional block data can be
	// retrieved if needed during catchup
	baseURL string
}

// Server implements a high-performance block validation service for Bitcoin SV.
// It coordinates block validation, subtree management, and transaction metadata processing
// across multiple subsystems while maintaining chain consistency. The server supports
// both synchronous and asynchronous validation modes, with automatic catchup capabilities
// when falling behind the chain tip.
type Server struct {
	// UnimplementedBlockValidationAPIServer provides default implementations of gRPC methods
	blockvalidation_api.UnimplementedBlockValidationAPIServer

	// logger provides structured logging with contextual information
	logger ulogger.Logger

	// settings contains operational parameters for block validation,
	// including timeouts, concurrency limits, and feature flags
	settings *settings.Settings

	// blockchainClient provides access to the blockchain state and operations
	// like block storage, header retrieval, and chain reorganization
	blockchainClient blockchain.ClientI

	// subtreeStore provides persistent storage for block subtrees,
	// enabling efficient block organization and validation
	subtreeStore blob.Store

	// txStore handles permanent storage of individual transactions,
	// allowing retrieval of historical transaction data
	txStore blob.Store

	// utxoStore manages the Unspent Transaction Output (UTXO) set,
	// tracking available outputs for transaction validation
	utxoStore utxo.Store

	// blockFoundCh receives notifications of newly discovered blocks
	// that need validation. This channel buffers requests when high load occurs.
	blockFoundCh chan processBlockFound

	// catchupCh handles blocks that need processing during chain catchup operations.
	// This channel is used when the node falls behind the chain tip.
	catchupCh chan processBlockCatchup

	// blockValidation contains the core validation logic and state
	blockValidation *BlockValidation

	// SetTxMetaQ provides a lock-free queue for handling transaction metadata
	// operations asynchronously
	SetTxMetaQ *util.LockFreeQ[[][]byte]

	// kafkaConsumerClient handles subscription to and consumption of
	// Kafka messages for distributed coordination
	kafkaConsumerClient kafka.KafkaConsumerGroupI

	// processSubtreeNotify caches subtree processing state to prevent duplicate
	// processing of the same subtree from multiple miners
	processSubtreeNotify *ttlcache.Cache[chainhash.Hash, bool]

	// stats tracks operational metrics for monitoring and troubleshooting
	stats *gocore.Stat
}

// New creates a new block validation server with the provided dependencies.
// It initializes internal channels, caches and metrics collection.
// Parameters:
//   - logger: provides structured logging
//   - tSettings: configuration parameters
//   - subtreeStore: storage for block subtrees
//   - txStore: storage for transactions
//   - utxoStore: manages UTXO set
//   - validatorClient: provides transaction validation
//   - blockchainClient: interfaces with the blockchain
//   - kafkaConsumerClient: handles Kafka message consumption
func New(
	logger ulogger.Logger,
	tSettings *settings.Settings,
	subtreeStore blob.Store,
	txStore blob.Store,
	utxoStore utxo.Store,
	validatorClient validator.Interface,
	blockchainClient blockchain.ClientI,
	kafkaConsumerClient kafka.KafkaConsumerGroupI,
) *Server {
	initPrometheusMetrics()

	// TEMP limit to 1, to prevent multiple subtrees processing at the same time
	subtreeGroup := errgroup.Group{}
	subtreeGroup.SetLimit(tSettings.BlockValidation.SubtreeGroupConcurrency)

	bVal := &Server{
		logger:               logger,
		settings:             tSettings,
		subtreeStore:         subtreeStore,
		blockchainClient:     blockchainClient,
		txStore:              txStore,
		utxoStore:            utxoStore,
		blockFoundCh:         make(chan processBlockFound, tSettings.BlockValidation.BlockFoundChBufferSize),
		catchupCh:            make(chan processBlockCatchup, tSettings.BlockValidation.CatchupChBufferSize),
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
		SetTxMetaQ:           util.NewLockFreeQ[[][]byte](),
		stats:                gocore.NewStat("blockvalidation"),
		kafkaConsumerClient:  kafkaConsumerClient,
	}

	return bVal
}

// Health performs comprehensive health and readiness checks on the block validation service.
// It verifies both the core service and its dependencies are functioning correctly.
//
// When checkLiveness is true, it performs only basic service health checks without checking
// dependencies. This is useful for determining if the service needs to be restarted.
// When false, it performs complete dependency checking to ensure the service can function fully.
//
// The method returns:
//   - An HTTP status code indicating the overall health state
//   - A descriptive message about the health status
//   - Any error encountered during health checking
//
// A return of http.StatusOK indicates the service is healthy and ready to handle requests.
// http.StatusServiceUnavailable indicates the service or a dependency is not ready.
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	var brokersURL []string
	if u.kafkaConsumerClient != nil { // tests may not set this
		brokersURL = u.kafkaConsumerClient.BrokersURL()
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 6)
	checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})

	if u.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: u.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(u.blockchainClient)})
	}

	if u.subtreeStore != nil {
		checks = append(checks, health.Check{Name: "SubtreeStore", Check: u.subtreeStore.Health})
	}

	if u.txStore != nil {
		checks = append(checks, health.Check{Name: "TxStore", Check: u.txStore.Health})
	}

	if u.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: u.utxoStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC provides a gRPC interface to the health checking functionality.
// It wraps the Health method to provide standardized health information via gRPC,
// including service status, detailed health information, and timestamps.
//
// The method performs a full dependency check and returns a structured response
// suitable for gRPC health monitoring systems. Any errors are properly wrapped
// to maintain gRPC error semantics.
func (u *Server) HealthGRPC(ctx context.Context, _ *blockvalidation_api.EmptyMessage) (*blockvalidation_api.HealthResponse, error) {
	prometheusBlockValidationHealth.Add(1)

	status, details, err := u.Health(ctx, false)

	return &blockvalidation_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.Now(),
	}, errors.WrapGRPC(err)
}

func (u *Server) Init(ctx context.Context) (err error) {
	subtreeValidationClient, err := subtreevalidation.NewClient(ctx, u.logger, u.settings, "blockvalidation")
	if err != nil {
		return errors.NewServiceError("[Init] failed to create subtree validation client", err)
	}

	storeURL := u.settings.UtxoStore.UtxoStore
	if storeURL == nil {
		return errors.NewConfigurationError("could not get utxostore URL", err)
	}

	u.blockValidation = NewBlockValidation(ctx, u.logger, u.settings, u.blockchainClient, u.subtreeStore, u.txStore, u.utxoStore, subtreeValidationClient)

	go u.processSubtreeNotify.Start()

	go func() {
		for {
			select {
			case <-ctx.Done():
				u.logger.Infof("[Init] closing block found channel")
				return
			default:
				data := u.SetTxMetaQ.Dequeue()
				if data == nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				go func(data *[][]byte) {
					prometheusBlockValidationSetTxMetaQueueCh.Dec()

					keys := make([][]byte, 0)
					values := make([][]byte, 0)

					for _, meta := range *data {
						if len(meta) < 32 {
							u.logger.Errorf("meta data is too short: %v", meta)
							return
						}

						// first 32 bytes is hash
						keys = append(keys, meta[:32])
						values = append(values, meta[32:])
					}

					if err := u.blockValidation.SetTxMetaCacheMulti(ctx, keys, values); err != nil {
						u.logger.Errorf("failed to set tx meta data: %v", err)
					}
				}(data)
			}
		}
	}()

	// process blocks found from channel
	go func() {
		for {
			_, _, ctx1 := tracing.NewStatFromContext(ctx, "catchupCh", u.stats, false)
			select {
			case <-ctx.Done():
				u.logger.Infof("[Init] closing block found channel")
				return
			case c := <-u.catchupCh:
				{
					// stop mining
					err = u.blockchainClient.CatchUpBlocks(ctx)
					if err != nil {
						u.logger.Errorf("[BlockValidation Init] failed to send CATCHUPBLOCKS event [%v]", err)
					}

					u.logger.Infof("[BlockValidation Init] processing catchup on channel [%s]", c.block.Hash().String())

					for {
						if err := u.catchup(ctx1, c.block, c.baseURL); err != nil {
							u.logger.Errorf("[BlockValidation Init] failed to catchup from [%s] - will retry [%v]", c.block.Hash().String(), err)

							continue
						}

						break
					}

					u.logger.Infof("[BlockValidation Init] processing catchup on channel DONE [%s]", c.block.Hash().String())
					prometheusBlockValidationCatchupCh.Set(float64(len(u.catchupCh)))

					// catched up, ready to mine, send RUN event
					err = u.blockchainClient.Run(ctx1, "blockvalidation/Server")
					if err != nil {
						u.logger.Errorf("[BlockValidation Init] failed to send RUN event [%v]", err)
					}
				}
			case blockFound := <-u.blockFoundCh:
				{
					if err := u.processBlockFoundChannel(ctx, blockFound); err != nil {
						u.logger.Errorf("[Init] failed to process block found [%s] [%v]", blockFound.hash.String(), err)
					}
				}
			}
		}
	}()

	return nil
}

func (u *Server) consumerMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		errCh := make(chan error, 1)
		go func() {
			errCh <- u.blockHandler(msg)
		}()

		select {
		case err := <-errCh:
			// if err is nil, it means function is successfully executed, return nil.
			if err == nil {
				return nil
			}

			// if error is not nil, check if the error is a recoverable error.
			// If the error is a recoverable error, then return the error, so that it kafka message is not marked as committed.
			// So the message will be consumed again.
			if errors.Is(err, errors.ErrServiceError) || errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrThresholdExceeded) || errors.Is(err, errors.ErrContextCanceled) || errors.Is(err, errors.ErrExternal) {
				u.logger.Errorf("Recoverable error (%v) processing kafka message %v for handling block, returning error, thus not marking Kafka message as complete.\n", msg, err)
				return err
			}

			// error is not nil and not recoverable, so it is unrecoverable error, and it should not be tried again
			// kafka message should be committed, so return nil to mark message.
			u.logger.Errorf("Unrecoverable error (%v) processing kafka message %v for handling block, marking Kafka message as completed.\n", msg, err)

			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (u *Server) blockHandler(msg *kafka.KafkaMessage) error {
	if msg == nil {
		return nil
	}

	var kafkaMsg kafkamessage.KafkaBlockTopicMessage
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, _, deferFn := tracing.StartTracing(ctx, "BlockFound",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationBlockFound),
		tracing.WithDebugLogMessage(u.logger, "[BlockFound][%s] called from %s", hash.String(), baseURL.String()),
	)
	defer deferFn()

	// first check if the block exists, it is very expensive to do all the checks below
	exists, err := u.blockValidation.GetBlockExists(ctx, hash)
	if err != nil {
		return errors.NewServiceError("[BlockFound][%s] failed to check if block exists", hash.String(), err)
	}

	if exists {
		u.logger.Infof("[BlockFound][%s] already validated, skipping", hash.String())
		return nil
	}

	errCh := make(chan error, 1)

	u.logger.Infof("[BlockFound][%s] add on channel", hash.String())
	u.blockFoundCh <- processBlockFound{
		hash:    hash,
		baseURL: baseURL.String(),
		errCh:   errCh,
	}
	prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))

	err = <-errCh
	if err != nil {
		return errors.NewProcessingError("[BlockFound][%s] failed to process block", hash.String(), err)
	}

	return nil
}

func (u *Server) processBlockFoundChannel(ctx context.Context, blockFound processBlockFound) error {
	// TODO GOKHAN: parameterize this
	if u.settings.BlockValidation.UseCatchupWhenBehind && len(u.blockFoundCh) > 3 {
		// we are multiple blocks behind, process all the blocks per peer on the catchup channel
		u.logger.Infof("[Init] processing block found on channel [%s] - too many blocks behind", blockFound.hash.String())

		peerBlocks := make(map[string]processBlockFound)
		peerBlocks[blockFound.baseURL] = blockFound
		// get the newest block per peer, emptying the block found channel
		for len(u.blockFoundCh) > 0 {
			pb := <-u.blockFoundCh
			peerBlocks[pb.baseURL] = pb
		}

		u.logger.Infof("[Init] peerBlocks: %v", peerBlocks)
		// add that latest block of each peer to the catchup channel
		for _, pb := range peerBlocks {
			block, err := u.getBlock(ctx, pb.hash, pb.baseURL)
			if err != nil {
				return errors.NewProcessingError("[Init] failed to get block [%s]", pb.hash.String(), err)
			}

			u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: pb.baseURL,
			}
		}

		if blockFound.errCh != nil {
			blockFound.errCh <- nil
		}

		return nil
	}

	_, _, ctx1 := tracing.NewStatFromContext(ctx, "blockFoundCh", u.stats, false)

	// TODO optimize this for the valid chain, not processing everything ???
	u.logger.Infof("[Init] processing block found on channel [%s]", blockFound.hash.String())

	err := u.processBlockFound(ctx1, blockFound.hash, blockFound.baseURL)
	if err != nil {
		if blockFound.errCh != nil {
			blockFound.errCh <- err
		}

		return errors.NewProcessingError("[Init] failed to process block [%s]", blockFound.hash.String(), err)
	}

	if blockFound.errCh != nil {
		blockFound.errCh <- nil
	}

	u.logger.Infof("[Init] processing block found on channel DONE [%s]", blockFound.hash.String())
	prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))

	return nil
}

// Start initiates the block validation service, starting Kafka consumers
// and HTTP/gRPC servers. It begins processing blocks and handling validation
// requests.
func (u *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Blocks until the FSM transitions from the IDLE state
	err := u.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		u.logger.Errorf("[Block Validation Service] Failed to wait for FSM transition from IDLE state: %s", err)

		return err
	}

	// start blocks kafka consumer
	u.kafkaConsumerClient.Start(ctx, u.consumerMessageHandler(ctx), kafka.WithRetryAndMoveOn(0, 1, time.Second))

	// this will block
	if err := util.StartGRPCServer(ctx, u.logger, u.settings, "blockvalidation", u.settings.BlockValidation.GRPCListenAddress, func(server *grpc.Server) {
		blockvalidation_api.RegisterBlockValidationAPIServer(server, u)
		closeOnce.Do(func() { close(readyCh) })
	}); err != nil {
		return err
	}

	return nil
}

func (u *Server) Stop(_ context.Context) error {
	u.processSubtreeNotify.Stop()

	// close the kafka consumer gracefully
	if err := u.kafkaConsumerClient.Close(); err != nil {
		u.logger.Errorf("[BlockValidation] failed to close kafka consumer gracefully: %v", err)
	}

	return nil
}

// BlockFound notifies the service about a newly discovered block that needs validation.
// It initiates the block validation process, optionally waiting for completion based
// on the request parameters.
//
// The method first checks if the block already exists to avoid duplicate processing.
// If the block is new, it queues it for validation using the block processing channels.
// The validation itself happens asynchronously to avoid blocking the gRPC handler.
//
// Parameters:
//   - ctx: The context for handling timeouts and cancellation
//   - req: Contains the block hash and source URL
//
// Returns an EmptyMessage on success or an error if validation cannot be initiated.
// If WaitToComplete is set in the request, waits for validation to finish before returning.
func (u *Server) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*blockvalidation_api.EmptyMessage, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "BlockFound",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationBlockFound),
		tracing.WithDebugLogMessage(u.logger, "[BlockFound][%s] called from %s", utils.ReverseAndHexEncodeSlice(req.Hash), req.GetBaseUrl()),
	)
	defer deferFn()

	hash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(
			errors.NewProcessingError("[BlockFound][%s] failed to create hash from bytes", utils.ReverseAndHexEncodeSlice(req.Hash), err))
	}

	// first check if the block exists, it is very expensive to do all the checks below
	exists, err := u.blockValidation.GetBlockExists(ctx, hash)
	if err != nil {
		return nil, errors.WrapGRPC(
			errors.NewServiceError("[BlockFound][%s] failed to check if block exists", hash.String(), err))
	}

	if exists {
		u.logger.Infof("[BlockFound][%s] already validated, skipping", utils.ReverseAndHexEncodeSlice(req.Hash))
		return &blockvalidation_api.EmptyMessage{}, nil
	}

	var errCh chan error

	if req.WaitToComplete {
		errCh = make(chan error)
	}

	// process the block in the background, in the order we receive them, but without blocking the grpc call
	go func() {
		u.logger.Infof("[BlockFound][%s] add on channel", hash.String())
		u.blockFoundCh <- processBlockFound{
			hash:    hash,
			baseURL: req.GetBaseUrl(),
			errCh:   errCh,
		}
		prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))
	}()

	if req.WaitToComplete {
		err := <-errCh
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}
	}

	return &blockvalidation_api.EmptyMessage{}, nil
}

// ProcessBlock handles validation of a complete block at a specified height.
// This method is typically used during initial block sync or when receiving blocks
// through legacy interfaces.
//
// The method performs several key operations:
// - Validates the block height is correct, fetching from parent if needed
// - Ensures the block can be properly deserialized
// - Processes the block through the validation pipeline
// - Updates chain state if validation succeeds
//
// Parameters:
//   - ctx: Context for the operation
//   - request: Contains the raw block data and target height
//
// Returns an EmptyMessage on successful validation or an error if validation fails.
func (u *Server) ProcessBlock(ctx context.Context, request *blockvalidation_api.ProcessBlockRequest) (*blockvalidation_api.EmptyMessage, error) {
	block, err := model.NewBlockFromBytes(request.Block, u.settings)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("failed to create block from bytes", err))
	}

	ctx, _, deferFn := tracing.StartTracing(ctx, "ProcessBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[ProcessBlock][%s] process block called for height %d (%d txns)", block.Hash(), request.Height, block.TransactionCount),
	)
	defer deferFn()

	// we need the height for the subsidy calculation
	height := request.Height

	if height <= 0 {
		// try to get the height from the previous block
		_, previousBlockMeta, err := u.blockchainClient.GetBlockHeader(ctx, block.Header.HashPrevBlock)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewServiceError("failed to get previous block header", err))
		}

		if previousBlockMeta != nil {
			height = previousBlockMeta.Height + 1
		}
	}

	if height <= 0 {
		return nil, errors.WrapGRPC(errors.NewProcessingError("invalid height: %d", height))
	}

	block.Height = height

	if err = u.processBlockFound(ctx, block.Header.Hash(), "legacy", block); err != nil {
		// error from processBlockFound is already wrapped
		return nil, errors.WrapGRPC(err)
	}

	return &blockvalidation_api.EmptyMessage{}, nil
}

func (u *Server) processBlockFound(ctx context.Context, hash *chainhash.Hash, baseURL string, useBlock ...*model.Block) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "processBlockFound",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationProcessBlockFound),
		tracing.WithDebugLogMessage(u.logger, "[processBlockFound][%s] processing block found from %s", hash.String(), baseURL),
	)
	defer deferFn()

	// first check if the block exists, it might have already been processed
	exists, err := u.blockValidation.GetBlockExists(ctx, hash)
	if err != nil {
		return errors.NewServiceError("[processBlockFound][%s] failed to check if block exists", hash.String(), err)
	}

	if exists {
		u.logger.Warnf("[processBlockFound][%s] not processing block that already was found", hash.String())
		return nil
	}

	var block *model.Block
	if len(useBlock) > 0 {
		block = useBlock[0]
	} else {
		block, err = u.getBlock(ctx, hash, baseURL)
		if err != nil {
			return err
		}
	}

	u.checkParentProcessingComplete(ctx, block, baseURL)

	// catchup if we are missing the parent block.
	parentExists, err := u.blockValidation.GetBlockExists(ctx, block.Header.HashPrevBlock)
	if err != nil {
		return errors.NewServiceError("[processBlockFound][%s] failed to check if parent block %s exists", hash.String(), block.Header.HashPrevBlock.String(), err)
	}

	if !parentExists {
		// add to catchup channel, which will block processing any new blocks until we have caught up
		go func() {
			u.logger.Infof("[processBlockFound][%s] processBlockFound add to catchup channel", hash.String())
			u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: baseURL,
			}
			prometheusBlockValidationCatchupCh.Set(float64(len(u.catchupCh)))
		}()

		return nil
	}

	// validate the block
	u.logger.Infof("[processBlockFound][%s] validate block from %s", hash.String(), baseURL)

	// this is a bit of a hack, but we need to turn off optimistic mining when in legacy mode
	if baseURL == "legacy" {
		err = u.blockValidation.ValidateBlock(ctx, block, baseURL, u.blockValidation.bloomFilterStats, true)
	} else {
		err = u.blockValidation.ValidateBlock(ctx, block, baseURL, u.blockValidation.bloomFilterStats)
	}

	if err != nil {
		return errors.NewServiceError("failed block validation BlockFound [%s]", block.String(), err)
	}

	return nil
}

// checkParentProcessingComplete ensures that a block's parent has completed validation
// before allowing the current block's validation to proceed. This method implements
// a backoff strategy while waiting for parent block processing to complete.
//
// The method:
// - Verifies parent block validation status
// - Implements exponential backoff for retry attempts
// - Monitors both validation and bloom filter creation status
// - Provides detailed logging of the waiting process
//
// Parameters:
//   - ctx: Context for operation management
//   - block: The block whose parent requires verification
//   - baseURL: Source URL for additional data retrieval if needed
func (u *Server) checkParentProcessingComplete(ctx context.Context, block *model.Block, baseURL string) {
	_, _, deferFn := tracing.StartTracing(ctx, "checkParentProcessingComplete",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[checkParentProcessingComplete][%s] called from %s", block.Hash().String(), baseURL),
	)
	defer deferFn()

	delay := 10 * time.Millisecond
	maxDelay := 10 * time.Second

	// check if the parent block is being validated, then wait for it to finish.
	blockBeingFinalized := u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock) ||
		u.blockValidation.blockBloomFiltersBeingCreated.Exists(*block.Header.HashPrevBlock)

	if blockBeingFinalized {
		u.logger.Infof("[processBlockFound][%s] parent block is being validated (hash: %s), waiting for it to finish: validated %v - bloom filters %v",
			block.Hash().String(),
			block.Header.HashPrevBlock.String(),
			u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock),
			u.blockValidation.blockBloomFiltersBeingCreated.Exists(*block.Header.HashPrevBlock),
		)

		retries := 0

		for {
			blockBeingFinalized = u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock) ||
				u.blockValidation.blockBloomFiltersBeingCreated.Exists(*block.Header.HashPrevBlock)

			if !blockBeingFinalized {
				break
			}

			if (retries % 10) == 0 {
				u.logger.Infof("[processBlockFound][%s] parent block is still (%d) being validated (hash: %s), waiting for it to finish: validated %v - bloom filters %v",
					block.Hash().String(),
					retries,
					block.Header.HashPrevBlock.String(),
					u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock),
					u.blockValidation.blockBloomFiltersBeingCreated.Exists(*block.Header.HashPrevBlock),
				)
			}

			time.Sleep(delay)

			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}

			time.Sleep(delay)

			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}

			retries++
		}
	}
}

func (u *Server) getBlock(ctx context.Context, hash *chainhash.Hash, baseURL string) (*model.Block, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "getBlock",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	blockBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", baseURL, hash.String()))
	if err != nil {
		return nil, errors.NewProcessingError("[getBlock][%s] failed to get block from peer", hash.String(), err)
	}

	block, err := model.NewBlockFromBytes(blockBytes, u.settings)
	if err != nil {
		return nil, errors.NewProcessingError("[getBlock][%s] failed to create block from bytes", hash.String(), err)
	}

	if block == nil {
		return nil, errors.NewProcessingError("[getBlock][%s] block could not be created from bytes: %v", hash.String(), blockBytes)
	}

	return block, nil
}

func (u *Server) getBlocks(ctx context.Context, hash *chainhash.Hash, n uint32, baseURL string) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "getBlocks",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	blockBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/blocks/%s?n=%d", baseURL, hash.String(), n))
	if err != nil {
		return nil, errors.NewProcessingError("[getBlocks][%s] failed to get blocks from peer", hash.String(), err)
	}

	blockReader := bytes.NewReader(blockBytes)

	blocks := make([]*model.Block, 0)

	for {
		block, err := model.NewBlockFromReader(blockReader, u.settings)
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				// if strings.Contains(err.Error(), "EOF") || errors.Is(err, io.ErrUnexpectedEOF) { // doesn't catch the EOF!!!! //TODO
				break
			}

			return nil, errors.NewProcessingError("[getBlocks][%s] failed to create block from bytes", hash.String(), err)
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

func (u *Server) getBlockHeaders(ctx context.Context, hash *chainhash.Hash, height uint32, baseURL string) ([]*model.BlockHeader, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "getBlockHeaders",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	bestBlockHeader, bestBlockMeta, err := u.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("[getBlockHeaders][%s] failed to get best block", hash.String(), err)
	}

	locatorHashes, err := u.blockchainClient.GetBlockLocator(ctx, bestBlockHeader.Hash(), bestBlockMeta.Height)
	if err != nil {
		return nil, errors.NewProcessingError("[getBlockHeaders][%s] failed to get block locator", hash.String(), err)
	}

	blockLocatorStr := ""
	for _, locatorHash := range locatorHashes {
		blockLocatorStr += locatorHash.String()
	}

	blockHeadersBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/headers_to_common_ancestor/%s?block_locator_hashes=%s", baseURL, hash.String(), blockLocatorStr))
	if err != nil {
		return nil, errors.NewProcessingError("[getBlockHeaders][%s] failed to get block headers from peer", hash.String(), err)
	}

	blockHeaders := make([]*model.BlockHeader, 0, len(blockHeadersBytes)/model.BlockHeaderSize)

	var blockHeader *model.BlockHeader
	for i := 0; i < len(blockHeadersBytes); i += model.BlockHeaderSize {
		blockHeader, err = model.NewBlockHeaderFromBytes(blockHeadersBytes[i : i+model.BlockHeaderSize])
		if err != nil {
			return nil, errors.NewProcessingError("[getBlockHeaders][%s] failed to create block header from bytes", hash.String(), err)
		}

		blockHeaders = append(blockHeaders, blockHeader)
	}

	return blockHeaders, nil
}

func (u *Server) catchup(ctx context.Context, blockUpTo *model.Block, baseURL string) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "catchup",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationCatchup),
		tracing.WithLogMessage(u.logger, "[catchup][%s] catching up on server %s", blockUpTo.Hash().String(), baseURL),
	)
	defer deferFn()

	catchupBlockHeaders, err := u.catchupGetBlocks(ctx, blockUpTo, baseURL)
	if err != nil {
		return err
	}

	if catchupBlockHeaders == nil {
		return nil
	}

	lastCommonAncestorBlockHash := catchupBlockHeaders[len(catchupBlockHeaders)-1].HashPrevBlock
	if secretMining, err := u.checkSecretMining(ctx, lastCommonAncestorBlockHash); err != nil {
		return err
	} else if secretMining {
		u.logger.Infof("[catchup][%s] ignoring catchup, last common ancestor block %s is too far behind current head", blockUpTo.Hash().String(), lastCommonAncestorBlockHash.String())
		return nil
	}

	u.logger.Infof("[catchup][%s] catching up (%d blocks) from [%s] to [%s]", blockUpTo.Hash().String(), len(catchupBlockHeaders), catchupBlockHeaders[len(catchupBlockHeaders)-1].String(), catchupBlockHeaders[0].String())

	validateBlocksChan := make(chan *model.Block, len(catchupBlockHeaders))

	size := atomic.Uint32{}

	// process the catchup block headers in reverse order and put them on the channel
	// this will allow the blocks to be validated while getting them from the other node
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(u.settings.BlockValidation.CatchupConcurrency)
	g.Go(func() error {
		slices.Reverse(catchupBlockHeaders)
		batches := getBlockBatchGets(catchupBlockHeaders, 100)

		u.logger.Debugf("[catchup][%s] getting %d batches", blockUpTo.Hash().String(), len(batches))

		blockCount := 0
		i := 0

		var (
			blocks []*model.Block
			err    error
		)

		for _, batch := range batches {
			batch := batch
			i++
			u.logger.Debugf("[catchup][%s] [batch %d] getting %d blocks from %s", blockUpTo.Hash().String(), i, batch.size, batch.hash.String())

			size.Add(batch.size)

			blocks, err = u.getBlocks(gCtx, &batch.hash, batch.size, baseURL)
			if err != nil {
				// TODO
				// we aren't waiting for the func to finish so we never catch this error and log it
				u.logger.Errorf("[catchup][%s] failed to get %d blocks [%s]:%v", blockUpTo.Hash().String(), batch.size, batch.hash.String(), err)
				return errors.NewProcessingError("[catchup][%s] failed to get %d blocks [%s]", blockUpTo.Hash().String(), batch.size, batch.hash.String(), err)
			}

			u.logger.Debugf("[catchup][%s] got %d blocks from %s", blockUpTo.Hash().String(), len(blocks), batch.hash.String())

			// reverse the blocks, so they are in the correct order, we get them newest to oldest from the other node
			slices.Reverse(blocks)

			for _, block := range blocks {
				blockCount++
				validateBlocksChan <- block
			}
		}

		u.logger.Infof("[catchup][%s] added %d blocks for validating", blockUpTo.Hash().String(), blockCount)

		// close the channel to signal that all blocks have been processed
		close(validateBlocksChan)

		return nil
	})

	i := 0
	// validate the blocks while getting them from the other node
	// this will block until all blocks are validated
	for block := range validateBlocksChan {
		i++
		u.logger.Infof("[catchup][%s] validating block %d/%d", block.Hash().String(), i, size.Load())

		// error is returned from validate block:

		if err = u.blockValidation.ValidateBlock(ctx, block, baseURL, u.blockValidation.bloomFilterStats); err != nil {
			return errors.NewServiceError("[catchup][%s]grep  [%s]", blockUpTo.Hash().String(), block.String(), err)
		}

		u.logger.Debugf("[catchup][%s] validated block %d/%d", block.Hash().String(), i, size.Load())
	}

	u.logger.Infof("[catchup][%s] done validating catchup blocks", blockUpTo.Hash().String())

	return nil
}

func (u *Server) checkSecretMining(ctx context.Context, lastCommonAncestorBlockHash *chainhash.Hash) (bool, error) {
	// check whether we are catching up to very old blocks, while we are ahead
	// this could mean we are catching up on a secretly mined chain, which we should ignore
	_, blockMeta, err := u.blockchainClient.GetBlockHeader(ctx, lastCommonAncestorBlockHash)
	if err != nil {
		return false, errors.NewProcessingError("[checkSecretMining] failed to get last common ancestor block from db %s", lastCommonAncestorBlockHash.String(), err)
	}

	if blockMeta == nil {
		return false, errors.NewProcessingError("[checkSecretMining] failed to get last common ancestor block %s", lastCommonAncestorBlockHash.String())
	}

	// the last common ancestor block should not be more than X blocks behind the current block height in the utxo store
	if blockMeta.Height < u.utxoStore.GetBlockHeight()-u.settings.BlockValidation.SecretMiningThreshold {
		return true, nil
	}

	return false, nil
}

func (u *Server) catchupGetBlocks(ctx context.Context, blockUpTo *model.Block, baseURL string) ([]*model.BlockHeader, error) {
	// first check whether this block already exists, which would mean we caught up from another peer
	exists, err := u.blockValidation.GetBlockExists(ctx, blockUpTo.Hash())
	if err != nil {
		return nil, errors.NewServiceError("[catchup][%s] failed to check if block exists", blockUpTo.Hash().String(), err)
	}

	if exists {
		return nil, nil
	}

	catchupBlockHeaders := []*model.BlockHeader{blockUpTo.Header}

	blockHeaderHashUpTo := blockUpTo.Header.HashPrevBlock
	blockHeaderHeightUpTo := blockUpTo.Height

	var blockHeaders []*model.BlockHeader
LOOP:
	for {
		u.logger.Debugf("[catchup][%s] getting block headers for catchup up to [%s]", blockUpTo.Hash().String(), blockHeaderHashUpTo.String())
		blockHeaders, err = u.getBlockHeaders(ctx, blockHeaderHashUpTo, blockHeaderHeightUpTo, baseURL)
		if err != nil {
			return nil, err
		}

		if len(blockHeaders) == 0 {
			return nil, errors.NewServiceError("[catchup][%s] failed to get block headers up to [%s]", blockUpTo.Hash().String(), blockHeaderHashUpTo.String())
		}

		for _, blockHeader := range blockHeaders {
			// check if parent block is currently being validated, then wait for it to finish. If the parent block was being validated, when the for loop is done, GetBlockExists will return true.
			exists := u.parentExistsAndIsValidated(ctx, blockHeader, blockUpTo)
			if exists {
				break LOOP
			}

			u.logger.Warnf("[catchup][%s] parent block does not exist [%s]", blockUpTo.Hash().String(), blockHeader.String())

			catchupBlockHeaders = append(catchupBlockHeaders, blockHeader)

			blockHeaderHashUpTo = blockHeader.HashPrevBlock
			// TODO: check if its only useful for a chain with different genesis block?
			if blockHeaderHashUpTo.IsEqual(&chainhash.Hash{}) {
				return nil, errors.NewProcessingError("[catchup][%s] failed to find parent block header, last was: %s", blockUpTo.Hash().String(), blockHeader.String())
			}
		}
	}

	return catchupBlockHeaders, nil
}

func (u *Server) parentExistsAndIsValidated(ctx context.Context, blockHeader *model.BlockHeader, blockUpTo *model.Block) bool {
	blockBeingFinalized := u.blockValidation.blockHashesCurrentlyValidated.Exists(*blockHeader.HashPrevBlock) ||
		u.blockValidation.blockBloomFiltersBeingCreated.Exists(*blockHeader.HashPrevBlock)

	if blockBeingFinalized {
		u.logger.Infof("[catchup][%s] parent block is being validated (hash: %s), waiting for it to finish: %v - %v", blockUpTo.Hash().String(), blockHeader.HashPrevBlock.String(), u.blockValidation.blockHashesCurrentlyValidated.Exists(*blockHeader.HashPrevBlock), u.blockValidation.blockBloomFiltersBeingCreated.Exists(*blockHeader.HashPrevBlock))

		retries := 0

		for {
			blockBeingFinalized = u.blockValidation.blockHashesCurrentlyValidated.Exists(*blockHeader.HashPrevBlock) ||
				u.blockValidation.blockBloomFiltersBeingCreated.Exists(*blockHeader.HashPrevBlock)

			if !blockBeingFinalized {
				break
			}

			if (retries % 10) == 0 {
				u.logger.Infof("[catchup][%s] parent block is still (%d) being validated (hash: %s), waiting for it to finish: validated %v - bloom filters %v", blockUpTo.Hash().String(), retries, blockHeader.HashPrevBlock.String(), u.blockValidation.blockHashesCurrentlyValidated.Exists(*blockHeader.HashPrevBlock), u.blockValidation.blockBloomFiltersBeingCreated.Exists(*blockHeader.HashPrevBlock))
			}

			time.Sleep(1 * time.Second)

			retries++
		}

		u.logger.Infof("[catchup][%s] parent block is done being validated", blockUpTo.Hash().String())
	}

	exists, err := u.blockValidation.GetBlockExists(ctx, blockHeader.Hash())
	if err != nil {
		u.logger.Errorf("[catchup][%s] failed to check if block exists", blockUpTo.Hash().String(), err)
	}

	return exists
}

type blockBatchGet struct {
	hash chainhash.Hash
	size uint32
}

func getBlockBatchGets(catchupBlockHeaders []*model.BlockHeader, batchSize int) []blockBatchGet {
	batches := make([]blockBatchGet, 0)

	var useBlockHeaders []*model.BlockHeader

	for i := 0; i < len(catchupBlockHeaders); i += batchSize {
		start := i

		end := i + batchSize
		if end > len(catchupBlockHeaders)-1 {
			useBlockHeaders = catchupBlockHeaders[start:]
		} else {
			useBlockHeaders = catchupBlockHeaders[start:end]
		}

		lastHash := useBlockHeaders[len(useBlockHeaders)-1].Hash()

		useBlockHeadersSizeUint32, err := util.SafeIntToUint32(len(useBlockHeaders))
		if err != nil {
			return nil
		}

		batches = append(batches, blockBatchGet{
			hash: *lastHash,
			size: useBlockHeadersSizeUint32,
		})
	}

	return batches
}

// Get retrieves a subtree from storage based on its hash identifier.
// This method provides direct access to the underlying subtree storage system,
// allowing retrieval of block organization structures for validation and
// chain synchronization purposes.
//
// The method manages performance tracking and error handling for subtree
// retrieval operations. It ensures proper extension management and converts
// any storage-level errors into appropriate gRPC responses.
//
// Parameters:
//   - ctx: Context for managing operation timeouts and cancellation
//   - request: Contains the hash identifying the requested subtree
//
// Returns a GetSubtreeResponse containing the serialized subtree data or
// an error if retrieval fails. Storage errors are properly wrapped to
// maintain consistent gRPC error semantics.
func (u *Server) Get(ctx context.Context, request *blockvalidation_api.GetSubtreeRequest) (*blockvalidation_api.GetSubtreeResponse, error) {
	start, stat, ctx := tracing.NewStatFromContext(ctx, "Get", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	subtree, err := u.subtreeStore.Get(ctx, request.Hash, options.WithFileExtension("subtree"))
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewStorageError("failed to get subtree: %s", utils.ReverseAndHexEncodeSlice(request.Hash), err))
	}

	return &blockvalidation_api.GetSubtreeResponse{
		Subtree: subtree,
	}, nil
}

// Exists verifies whether a specific subtree exists in storage.
// This method provides efficient existence checking for subtrees without
// requiring full data retrieval. It is particularly useful during block
// validation to verify the availability of required subtree structures.
//
// The implementation includes performance monitoring and maintains
// consistency with the service's caching and storage layers.
//
// Parameters:
//   - ctx: Context for the verification operation
//   - request: Contains the hash of the subtree to verify
//
// Returns an ExistsSubtreeResponse indicating whether the subtree exists.
// Any errors during verification are wrapped appropriately for gRPC
// error handling.
func (u *Server) Exists(ctx context.Context, request *blockvalidation_api.ExistsSubtreeRequest) (*blockvalidation_api.ExistsSubtreeResponse, error) {
	start, stat, ctx := tracing.NewStatFromContext(ctx, "Exists", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	hash := chainhash.Hash(request.Hash)

	exists, err := u.blockValidation.GetSubtreeExists(ctx, &hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewServiceError("failed to check if subtree exists: %s", hash.String(), err))
	}

	return &blockvalidation_api.ExistsSubtreeResponse{
		Exists: exists,
	}, nil
}

// SetTxMeta queues transaction metadata updates for processing.
// This method handles batched updates to transaction metadata, managing them
// through an asynchronous queue to prevent overwhelming the storage system.
//
// The method:
// - Updates prometheus metrics for monitoring
// - Enqueues the metadata for processing
// - Returns quickly to allow high throughput
//
// The actual processing happens asynchronously through the SetTxMetaQ queue.
func (u *Server) SetTxMeta(ctx context.Context, request *blockvalidation_api.SetTxMetaRequest) (*blockvalidation_api.SetTxMetaResponse, error) {
	start, stat, _ := tracing.NewStatFromContext(ctx, "SetTxMeta", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	// number of items added
	prometheusBlockValidationSetTXMetaCache.Add(float64(len(request.Data)))

	// queue size
	prometheusBlockValidationSetTxMetaQueueCh.Inc()

	u.SetTxMetaQ.Enqueue(request.Data)

	return &blockvalidation_api.SetTxMetaResponse{
		Ok: true,
	}, nil
}

// DelTxMeta removes transaction metadata from the system.
// This method handles the deletion of transaction metadata, typically used
// when transactions are no longer needed or during chain reorganization.
//
// The method:
// - Validates the provided transaction hash
// - Updates monitoring metrics
// - Executes the deletion through the caching layer
// - Handles any errors during deletion
func (u *Server) DelTxMeta(ctx context.Context, request *blockvalidation_api.DelTxMetaRequest) (*blockvalidation_api.DelTxMetaResponse, error) {
	start, stat, ctx := tracing.NewStatFromContext(ctx, "SetTxMeta", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockValidationSetTXMetaCacheDel.Inc()

	hash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("failed to create hash from bytes", err))
	}

	if err = u.blockValidation.DelTxMetaCacheMulti(ctx, hash); err != nil {
		u.logger.Errorf("failed to delete tx meta data: %v", err)
	}

	return &blockvalidation_api.DelTxMetaResponse{
		Ok: true,
	}, nil
}

// SetMinedMulti marks multiple transactions as mined in a specific block.
// This method efficiently handles batch updates for transaction mining status,
// typically called when a block has been successfully validated and added to the chain.
//
// Parameters:
//   - ctx: Context for the operation
//   - request: Contains block ID and list of transaction hashes
//
// The method ensures atomic updates and maintains consistency between the transaction
// metadata cache and permanent storage. It includes metric tracking for monitoring
// system performance.
func (u *Server) SetMinedMulti(ctx context.Context, request *blockvalidation_api.SetMinedMultiRequest) (*blockvalidation_api.SetMinedMultiResponse, error) {
	start, stat, ctx := tracing.NewStatFromContext(ctx, "SetMinedMulti", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	u.logger.Warnf("GRPC SetMinedMulti %d: %d", request.BlockId, len(request.Hashes))

	hashes := make([]*chainhash.Hash, 0, len(request.Hashes))

	for _, hash := range request.Hashes {
		hash32 := chainhash.Hash(hash)
		hashes = append(hashes, &hash32)
	}

	prometheusBlockValidationSetMinedMulti.Inc()

	// TODO add the height and subtree index to the request
	err := u.blockValidation.SetTxMetaCacheMinedMulti(ctx, hashes, utxo.MinedBlockInfo{
		BlockID:     request.BlockId,
		BlockHeight: 0,
		SubtreeIdx:  0,
	})
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("failed to set tx meta data", err))
	}

	return &blockvalidation_api.SetMinedMultiResponse{
		Ok: true,
	}, nil
}
