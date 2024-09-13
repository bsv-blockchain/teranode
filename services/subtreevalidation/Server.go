package subtreevalidation

import (
	"context"
	"net/url"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/util/quorum"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ordishs/go-utils"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation/subtreevalidation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/google/uuid"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Server type carries the logger within it
type Server struct {
	subtreevalidation_api.UnimplementedSubtreeValidationAPIServer
	logger                            ulogger.Logger
	subtreeStore                      blob.Store
	subtreeTTL                        time.Duration
	txStore                           blob.Store
	utxoStore                         utxo.Store
	validatorClient                   validator.Interface
	subtreeCount                      atomic.Int32
	maxMerkleItemsPerSubtree          int
	stats                             *gocore.Stat
	prioritySubtreeCheckActiveMap     map[string]bool
	prioritySubtreeCheckActiveMapLock sync.Mutex
	blockchainClient                  blockchain.ClientI
}

var (
	once sync.Once
	q    *quorum.Quorum
)

func New(
	ctx context.Context,
	logger ulogger.Logger,
	subtreeStore blob.Store,
	txStore blob.Store,
	utxoStore utxo.Store,
	validatorClient validator.Interface,
	blockchainClient blockchain.ClientI,
) (*Server, error) {
	maxMerkleItemsPerSubtree, _ := gocore.Config().GetInt("initial_merkle_items_per_subtree", 1024)
	subtreeTTLMinutes, _ := gocore.Config().GetInt("subtreevalidation_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	u := &Server{
		logger:                            logger,
		subtreeStore:                      subtreeStore,
		subtreeTTL:                        subtreeTTL,
		txStore:                           txStore,
		utxoStore:                         utxoStore,
		validatorClient:                   validatorClient,
		subtreeCount:                      atomic.Int32{},
		maxMerkleItemsPerSubtree:          maxMerkleItemsPerSubtree,
		stats:                             gocore.NewStat("subtreevalidation"),
		prioritySubtreeCheckActiveMap:     map[string]bool{},
		prioritySubtreeCheckActiveMapLock: sync.Mutex{},
		blockchainClient:                  blockchainClient,
	}

	var err error

	once.Do(func() {
		quorumPath, _ := gocore.Config().Get("subtree_quorum_path", "")
		if quorumPath == "" {
			err = errors.NewConfigurationError("No subtree_quorum_path specified")
			return
		}

		var absoluteQuorumTimeout time.Duration

		absoluteQuorumTimeout, err, _ = gocore.Config().GetDuration("subtree_quorum_absolute_timeout")
		if err != nil {
			err = errors.NewConfigurationError("Bad subtree_quorum_absolute_timeout specified", err)
			return
		}

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
	if gocore.Config().GetBool("subtreevalidation_txMetaCacheEnabled", true) {
		logger.Infof("Using cached version of tx meta store")

		u.utxoStore, _ = txmetacache.NewTxMetaCache(ctx, ulogger.TestLogger{}, utxoStore)
	} else {
		u.utxoStore = utxoStore
	}

	return u, nil
}

func (u *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (u *Server) Init(ctx context.Context) (err error) {
	initPrometheusMetrics()

	return nil
}

// Start function
func (u *Server) Start(ctx context.Context) error {
	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		_, err := u.blockchainClient.Restore(ctx, &emptypb.Empty{})
		if err != nil {
			u.logger.Errorf("[Subtreevalidation] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		u.logger.Infof("[Subtreevalidation] Node is restoring, waiting for FSM to transition to Running state")
		_ = u.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain_api.FSMStateType_RUNNING)
		u.logger.Infof("[Subtreevalidation] Node finished restoring and has transitioned to Running state, continuing to start Subtreevalidation service")
	}

	subtreesKafkaURL, err, ok := gocore.Config().GetURL("kafka_subtreesConfig")
	if err == nil && ok {
		// Start a number of Kafka consumers equal to the number of CPU cores, minus 16 to leave processing for the tx meta cache.
		// subtreeConcurrency, _ := gocore.Config().GetInt("subtreevalidation_kafkaSubtreeConcurrency", util.Max(4, runtime.NumCPU()-16))
		// g.SetLimit(subtreeConcurrency)
		var partitions int

		if partitions, err = strconv.Atoi(subtreesKafkaURL.Query().Get("partitions")); err != nil {
			return errors.NewInvalidArgumentError("[Subtreevalidation] unable to parse Kafka partitions from %s", subtreesKafkaURL, err)
		}

		consumerRatio := util.GetQueryParamInt(subtreesKafkaURL, "consumer_ratio", 4)
		if consumerRatio < 1 {
			consumerRatio = 1
		}

		consumerCount := partitions / consumerRatio

		if consumerCount < 0 {
			consumerCount = 1
		}

		// set the concurrency limit by default to leave 16 cpus for doing tx meta processing
		subtreeConcurrency, _ := gocore.Config().GetInt("subtreevalidation_kafkaSubtreeConcurrency", util.Max(4, runtime.NumCPU()-16))
		g := errgroup.Group{}
		g.SetLimit(subtreeConcurrency)

		// By using the fixed "subtreevalidation" group ID, we ensure that only one instance of this service will process the subtree messages.
		u.logger.Infof("Starting %d Kafka consumers for subtree messages", consumerCount)
		// Autocommit is disabled for subtree messages, so that we can commit the message only after the subtree has been processed.
		go u.startKafkaListener(ctx, subtreesKafkaURL, "subtreevalidation", consumerCount, false, func(msg util.KafkaMessage) error {
			errCh := make(chan error, 1)
			go func() {
				errCh <- u.subtreeHandler(msg)
			}()

			select {
			// error handling
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
				if errors.Is(err, errors.ErrServiceError) || errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrThresholdExceeded) || errors.Is(err, errors.ErrContext) || errors.Is(err, errors.ErrExternal) {
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
		})
	}

	txmetaKafkaURL, err, ok := gocore.Config().GetURL("kafka_txmetaConfig")
	if err == nil && ok {
		var partitions int

		if partitions, err = strconv.Atoi(txmetaKafkaURL.Query().Get("partitions")); err != nil {
			return errors.NewInvalidArgumentError("[Subtreevalidation] unable to parse Kafka partitions from %s", txmetaKafkaURL, err)
		}

		consumerRatio := util.GetQueryParamInt(txmetaKafkaURL, "consumer_ratio", 8)
		if consumerRatio < 1 {
			consumerRatio = 1
		}

		consumerCount := partitions / consumerRatio
		if consumerCount < 0 {
			consumerCount = 1
		}

		// Generate a unique group ID for the txmeta Kafka listener, to ensure that each instance of this service will process all txmeta messages.
		// This is necessary because the txmeta messages are used to populate the txmeta cache, which is shared across all instances of this service.
		groupID := "subtreevalidation-" + uuid.New().String()

		u.logger.Infof("Starting %d Kafka consumers for tx meta messages", consumerCount)

		// For TxMeta, we are using autocommit, as we want to consume every message as fast as possible, and it is okay if some of the messages are not properly processed.
		// We don't need manual kafka commit and error handling here, as it is not necessary to retry the message, we have the message in stores.
		// Therefore, autocommit is set to true.
		go u.startKafkaListener(ctx, txmetaKafkaURL, groupID, consumerCount, true, u.txmetaHandler)
	}

	// this will block
	if err = util.StartGRPCServer(ctx, u.logger, "subtreevalidation", func(server *grpc.Server) {
		subtreevalidation_api.RegisterSubtreeValidationAPIServer(server, u)
	}); err != nil {
		return err
	}

	return nil
}

func (u *Server) startKafkaListener(ctx context.Context, kafkaURL *url.URL, groupID string, consumerCount int, autoCommit bool, fn func(msg util.KafkaMessage) error) {
	u.logger.Infof("starting Kafka on address: %s", kafkaURL.String())

	if err := util.StartKafkaGroupListener(ctx, u.logger, kafkaURL, groupID, nil, consumerCount, autoCommit, fn); err != nil {
		u.logger.Errorf("Failed to start Kafka listener for %s: %v", kafkaURL.String(), err)
	}
}

func (u *Server) Stop(_ context.Context) error {
	return nil
}

func (u *Server) HealthGRPC(ctx context.Context, _ *subtreevalidation_api.EmptyMessage) (*subtreevalidation_api.HealthResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "HealthGRPC",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusHealth),
		tracing.WithLogMessage(u.logger, "[HealthGRPC] called"),
	)
	defer deferFn()

	return &subtreevalidation_api.HealthResponse{
		Ok: true,
		// nolint: gosec
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

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
		tracing.WithDebugLogMessage(u.logger, "[checkSubtree] called for subtree %s (height %d)", utils.ReverseAndHexEncodeSlice(request.Hash), request.BlockHeight),
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

				// Call the validateSubtreeInternal method
				if err = u.validateSubtreeInternal(ctx, v, request.BlockHeight); err != nil {
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

			// Call the validateSubtreeInternal method
			if err = u.validateSubtreeInternal(ctx, v, request.BlockHeight); err != nil {
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
