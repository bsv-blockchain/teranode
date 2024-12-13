// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation/subtreevalidation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/bitcoin-sv/ubsv/util/quorum"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server represents the main subtree validation service.
type Server struct {
	subtreevalidation_api.UnimplementedSubtreeValidationAPIServer
	// logger handles all logging operations
	logger ulogger.Logger
	// settings contains the configuration for the service
	settings *settings.Settings
	// subtreeStore manages persistent storage of subtrees
	subtreeStore blob.Store
	// txStore manages transaction storage
	txStore blob.Store
	// utxoStore manages UTXO state
	utxoStore utxo.Store
	// validatorClient provides transaction validation services
	validatorClient validator.Interface
	// subtreeCount tracks the number of subtrees processed
	subtreeCount atomic.Int32
	// stats tracks operational statistics
	stats *gocore.Stat
	// prioritySubtreeCheckActiveMap tracks active priority subtree checks
	prioritySubtreeCheckActiveMap map[string]bool
	// prioritySubtreeCheckActiveMapLock protects the priority map
	prioritySubtreeCheckActiveMapLock sync.Mutex
	// blockchainClient interfaces with the blockchain
	blockchainClient blockchain.ClientI
	// subtreeConsumerClient consumes subtree-related Kafka messages
	subtreeConsumerClient kafka.KafkaConsumerGroupI
	// txmetaConsumerClient consumes transaction metadata Kafka messages
	txmetaConsumerClient kafka.KafkaConsumerGroupI
}

var (
	// once ensures the quorum is initialized only once
	once sync.Once
	// q is a singleton instance of the quorum manager used for subtree validation
	q *quorum.Quorum
)

// New creates a new Server instance with the provided dependencies.
// It initializes the service with the given configuration and stores.
func New(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	subtreeStore blob.Store,
	txStore blob.Store,
	utxoStore utxo.Store,
	validatorClient validator.Interface,
	blockchainClient blockchain.ClientI,
	subtreeConsumerClient kafka.KafkaConsumerGroupI,
	txmetaConsumerClient kafka.KafkaConsumerGroupI,
) (*Server, error) {
	u := &Server{
		logger:                            logger,
		settings:                          tSettings,
		subtreeStore:                      subtreeStore,
		txStore:                           txStore,
		utxoStore:                         utxoStore,
		validatorClient:                   validatorClient,
		subtreeCount:                      atomic.Int32{},
		stats:                             gocore.NewStat("subtreevalidation"),
		prioritySubtreeCheckActiveMap:     map[string]bool{},
		prioritySubtreeCheckActiveMapLock: sync.Mutex{},
		blockchainClient:                  blockchainClient,
		subtreeConsumerClient:             subtreeConsumerClient,
		txmetaConsumerClient:              txmetaConsumerClient,
	}

	var err error

	once.Do(func() {
		quorumPath := tSettings.SubtreeValidation.QuorumPath
		if quorumPath == "" {
			err = errors.NewConfigurationError("No subtree_quorum_path specified")
			return
		}

		var absoluteQuorumTimeout time.Duration

		absoluteQuorumTimeout = tSettings.SubtreeValidation.QuorumAbsoluteTimeout

		q, err = quorum.New(
			u.logger,
			u.subtreeStore,
			quorumPath,
			quorum.WithAbsoluteTimeout(absoluteQuorumTimeout),
		)
	})

	if err != nil {
		return nil, err
	}

	// create a caching tx meta store
	if tSettings.SubtreeValidation.TxMetaCacheEnabled {
		logger.Infof("Using cached version of tx meta store")

		var err error

		u.utxoStore, err = txmetacache.NewTxMetaCache(ctx, logger, utxoStore)
		if err != nil {
			logger.Errorf("Failed to create tx meta cache: %v", err)
		}
	} else {
		u.utxoStore = utxoStore
	}

	return u, nil
}

// Health checks the health status of the service and its dependencies.
// It returns an HTTP status code, status message, and any error encountered.
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	var brokersURL []string
	if u.txmetaConsumerClient != nil { // tests may not set this
		brokersURL = u.txmetaConsumerClient.BrokersURL()
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 5)
	checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})

	if u.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: u.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(u.blockchainClient)})
	}

	if u.subtreeStore != nil {
		checks = append(checks, health.Check{Name: "SubtreeStore", Check: u.subtreeStore.Health})
	}

	if u.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: u.utxoStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC implements the gRPC health check endpoint.
func (u *Server) HealthGRPC(ctx context.Context, _ *subtreevalidation_api.EmptyMessage) (*subtreevalidation_api.HealthResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "HealthGRPC",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusHealth),
		tracing.WithDebugLogMessage(u.logger, "[HealthGRPC] called"),
	)
	defer deferFn()

	status, details, err := u.Health(ctx, false)

	return &subtreevalidation_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.Now(),
	}, errors.WrapGRPC(err)
}

// Init initializes the server metrics and performs any necessary setup.
func (u *Server) Init(ctx context.Context) (err error) {
	InitPrometheusMetrics()

	return nil
}

func (u *Server) GetUutxoStore() utxo.Store {
	return u.utxoStore
}

func (u *Server) SetUutxoStore(s utxo.Store) {
	u.utxoStore = s
}

// Start initializes and starts the server components including Kafka consumers
// and gRPC server. It blocks until the context is canceled or an error occurs.
func (u *Server) Start(ctx context.Context) error {
	// start kafka consumers
	u.subtreeConsumerClient.Start(ctx, u.consumerMessageHandler(ctx), kafka.WithRetryAndMoveOn(3, 2, time.Second))
	u.txmetaConsumerClient.Start(ctx, u.txmetaHandler, kafka.WithRetryAndMoveOn(0, 1, time.Second))

	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.

	if u.settings.BlockChain.FSMStateRestore {
		// Send Restore event to FSM
		err := u.blockchainClient.Restore(ctx)
		if err != nil {
			u.logger.Errorf("[Subtreevalidation] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		u.logger.Infof("[Subtreevalidation] Node is restoring, waiting for FSM to transition to Running state")
		_ = u.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain.FSMStateRUNNING)
		u.logger.Infof("[Subtreevalidation] Node finished restoring and has transitioned to Running state, continuing to start Subtreevalidation service")
	}

	// this will block
	if err := util.StartGRPCServer(ctx, u.logger, "subtreevalidation", func(server *grpc.Server) {
		subtreevalidation_api.RegisterSubtreeValidationAPIServer(server, u)
	}); err != nil {
		return err
	}

	return nil
}

// Stop gracefully shuts down the server components including Kafka consumers.
func (u *Server) Stop(_ context.Context) error {
	// close the kafka consumers gracefully
	if err := u.subtreeConsumerClient.Close(); err != nil {
		u.logger.Errorf("[BlockValidation] failed to close kafka consumer gracefully: %v", err)
	}

	if err := u.txmetaConsumerClient.Close(); err != nil {
		u.logger.Errorf("[BlockValidation] failed to close kafka consumer gracefully: %v", err)
	}

	return nil
}

// checkSubtree validates a subtree and its transactions based on the provided request.
// It handles both legacy and current validation paths, managing locks to prevent
// duplicate processing.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - request: Contains subtree hash, base URL, and block information
//
// Returns:
//   - bool: True if the subtree is valid
//   - error: Any error encountered during validation
//
// The function implements a retry mechanism for lock acquisition and supports
// both legacy and current validation paths. It will retry for up to 20 seconds
// when attempting to acquire a lock.
func (u *Server) CheckSubtree(ctx context.Context, request *subtreevalidation_api.CheckSubtreeRequest) (*subtreevalidation_api.CheckSubtreeResponse, error) {
	subtreeBlessed, err := u.checkSubtree(ctx, request)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &subtreevalidation_api.CheckSubtreeResponse{
		Blessed: subtreeBlessed,
	}, nil
}

// checkSubtree is the internal function used to check a subtree
func (u *Server) checkSubtree(ctx context.Context, request *subtreevalidation_api.CheckSubtreeRequest) (ok bool, err error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "checkSubtree",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusSubtreeValidationCheckSubtree),
		tracing.WithLogMessage(u.logger, "[checkSubtree] called for subtree %s (block %s / height %d)", utils.ReverseAndHexEncodeSlice(request.Hash), utils.ReverseAndHexEncodeSlice(request.BlockHash), request.BlockHeight),
	)
	defer func() {
		deferFn(err)
	}()

	var hash *chainhash.Hash

	hash, err = chainhash.NewHash(request.Hash)
	if err != nil {
		return false, errors.NewProcessingError("[CheckSubtree] Failed to parse subtree hash from request", err)
	}

	if request.BaseUrl == "" {
		return false, errors.NewInvalidArgumentError("[CheckSubtree] Missing base URL in request")
	}

	u.logger.Debugf("[CheckSubtree] Received priority subtree message for %s from %s", hash.String(), request.BaseUrl)
	defer u.logger.Debugf("[CheckSubtree] Finished processing priority subtree message for %s from %s", hash.String(), request.BaseUrl)

	u.prioritySubtreeCheckActiveMapLock.Lock()
	u.prioritySubtreeCheckActiveMap[hash.String()] = true
	u.prioritySubtreeCheckActiveMapLock.Unlock()

	defer func() {
		u.prioritySubtreeCheckActiveMapLock.Lock()
		delete(u.prioritySubtreeCheckActiveMap, hash.String())
		u.prioritySubtreeCheckActiveMapLock.Unlock()
	}()

	retryCount := 0

	for {
		gotLock, exists, releaseLockFunc, err := q.TryLockIfNotExists(ctx, hash)
		if err != nil {
			return false, errors.NewError("[CheckSubtree] error getting lock for Subtree %s", hash.String(), err)
		}
		defer releaseLockFunc()

		if exists {
			u.logger.Infof("[CheckSubtree] Priority subtree request no longer needed as subtree now exists for %s from %s", hash.String(), request.BaseUrl)

			return true, nil
		}

		if gotLock {
			u.logger.Infof("[CheckSubtree] Processing priority subtree message for %s from %s", hash.String(), request.BaseUrl)

			var subtree *util.Subtree

			if request.BaseUrl == "legacy" {
				// read from legacy store
				subtreeBytes, err := u.subtreeStore.Get(
					ctx,
					hash[:],
					options.WithFileExtension("subtree"),
				)
				if err != nil {
					return false, errors.NewStorageError("[getSubtreeTxHashes][%s] failed to get subtree from store", hash.String(), err)
				}

				subtree, err = util.NewSubtreeFromBytes(subtreeBytes)
				if err != nil {
					return false, errors.NewProcessingError("[CheckSubtree] Failed to create subtree from bytes", err)
				}

				txHashes := make([]chainhash.Hash, subtree.Length())

				for i := 0; i < subtree.Length(); i++ {
					txHashes[i] = subtree.Nodes[i].Hash
				}

				v := ValidateSubtree{
					SubtreeHash:   *hash,
					BaseURL:       request.BaseUrl,
					TxHashes:      txHashes,
					AllowFailFast: false,
				}

				// Call the ValidateSubtreeInternal method
				if err = u.ValidateSubtreeInternal(ctx, v, request.BlockHeight); err != nil {
					// u.logger.Errorf("SAO %s", err)
					return false, errors.NewProcessingError("[CheckSubtree] Failed to validate legacy subtree %s", hash.String(), err)
				}

				u.logger.Debugf("[CheckSubtree] Finished processing priority legacy subtree message for %s from %s", hash.String(), request.BaseUrl)

				return true, nil
			}

			// This line is only reached when the base URL is not "legacy"
			v := ValidateSubtree{
				SubtreeHash:   *hash,
				BaseURL:       request.BaseUrl,
				AllowFailFast: false,
			}

			// Call the ValidateSubtreeInternal method
			if err = u.ValidateSubtreeInternal(ctx, v, request.BlockHeight); err != nil {
				return false, errors.NewProcessingError("[CheckSubtree] Failed to validate subtree %s", hash.String(), err)
			}

			u.logger.Debugf("[CheckSubtree] Finished processing priority subtree message for %s from %s", hash.String(), request.BaseUrl)

			return true, nil
		} else {
			u.logger.Infof("[CheckSubtree] Failed to get lock for subtree %s, retry #%d", hash.String(), retryCount)

			// Wait for a bit before retrying.
			select {
			case <-ctx.Done():
				return false, errors.NewContextCanceledError("[CheckSubtree] context cancelled")
			case <-time.After(1 * time.Second):
				retryCount++

				// will retry for 20 seconds
				if retryCount > 20 {
					return false, errors.NewError("[CheckSubtree] failed to get lock for subtree %s after 20 retries", hash.String())
				}

				// Automatically retries the loop.
				continue
			}
		}
	}
}
