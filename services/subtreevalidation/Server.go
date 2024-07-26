package subtreevalidation

import (
	"context"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
}

func New(
	ctx context.Context,
	logger ulogger.Logger,
	subtreeStore blob.Store,
	txStore blob.Store,
	utxoStore utxo.Store,
	validatorClient validator.Interface,
) *Server {

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
	}

	// create a caching tx meta store
	if gocore.Config().GetBool("subtreevalidation_txMetaCacheEnabled", true) {
		logger.Infof("Using cached version of tx meta store")
		u.utxoStore = txmetacache.NewTxMetaCache(ctx, ulogger.TestLogger{}, utxoStore)
	} else {
		u.utxoStore = utxoStore
	}

	return u
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
	subtreesKafkaURL, err, ok := gocore.Config().GetURL("kafka_subtreesConfig")
	if err == nil && ok {
		// Start a number of Kafka consumers equal to the number of CPU cores, minus 16 to leave processing for the tx meta cache.
		// subtreeConcurrency, _ := gocore.Config().GetInt("subtreevalidation_kafkaSubtreeConcurrency", util.Max(4, runtime.NumCPU()-16))
		// g.SetLimit(subtreeConcurrency)
		var partitions int
		if partitions, err = strconv.Atoi(subtreesKafkaURL.Query().Get("partitions")); err != nil {
			u.logger.Fatalf("[Subtreevalidation] unable to parse Kafka partitions from %s: %s", subtreesKafkaURL, err)
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
		go u.startKafkaListener(ctx, subtreesKafkaURL, "subtreevalidation", consumerCount, func(msg util.KafkaMessage) error {
			// TODO is there a way to return an error here and have Kafka mark the message as not done?
			errCh := make(chan error, 1)
			go func() {
				errCh <- u.subtreeHandler(msg)
			}()

			// TODO GOKHAN: UPDATE ERROR HANDLING, RECOVERABLE ERRORS
			select {
			// error handling
			case err := <-errCh:
				// unrecoverable error, should not be tried again, and kafka message should be committed.
				// return nil
				if errors.Is(err, errors.ErrSubtreeInvalidFormat) || errors.Is(err, errors.ErrSubtreeError) {
					return nil
				}

				// recoveable error, return nil.
				// currently, the following cases are considered recoverable:
				// errors.ErrProcessing, errors.ErrServiceError, errors.StorageError
				u.logger.Errorf("Unrecoverable error (%v) processing kafka message %v for handling subtree, marking Kafka message as complete.\n", msg, err)
				return err
			case <-ctx.Done():
				return ctx.Err()
			}

		})
	}

	txmetaKafkaURL, err, ok := gocore.Config().GetURL("kafka_txmetaConfig")
	if err == nil && ok {
		var partitions int
		if partitions, err = strconv.Atoi(txmetaKafkaURL.Query().Get("partitions")); err != nil {
			u.logger.Fatalf("[Subtreevalidation] unable to parse Kafka partitions from %s: %s", txmetaKafkaURL, err)
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

		// TODO GOKHAN: ADD ERROR HANDLING
		go u.startKafkaListener(ctx, txmetaKafkaURL, groupID, consumerCount, u.txmetaHandler)
	}

	// this will block
	if err = util.StartGRPCServer(ctx, u.logger, "subtreevalidation", func(server *grpc.Server) {
		subtreevalidation_api.RegisterSubtreeValidationAPIServer(server, u)
	}); err != nil {
		return err
	}

	return nil
}

func (u *Server) startKafkaListener(ctx context.Context, kafkaURL *url.URL, groupID string, consumerCount int, fn func(msg util.KafkaMessage) error) {
	u.logger.Infof("starting Kafka on address: %s", kafkaURL.String())

	// Autocommit is disabled for subtree messages, as we want to ensure that the message is processed before committing the offset.
	if err := util.StartKafkaGroupListener(ctx, u.logger, kafkaURL, groupID, nil, consumerCount, false, fn); err != nil {
		u.logger.Errorf("Failed to start Kafka listener: %v", err)
	}
}

func (u *Server) Stop(_ context.Context) error {
	return nil
}

func (u *Server) HealthGRPC(_ context.Context, _ *subtreevalidation_api.EmptyMessage) (*subtreevalidation_api.HealthResponse, error) {
	start, stat, _ := tracing.NewStatFromContext(context.Background(), "Health", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	// prometheusSubtreeValidationHealth.Inc()

	return &subtreevalidation_api.HealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (u *Server) CheckSubtree(ctx context.Context, request *subtreevalidation_api.CheckSubtreeRequest) (*subtreevalidation_api.CheckSubtreeResponse, error) {
	start, stat, ctx, cancel := tracing.NewStatFromContextWithCancel(ctx, "CheckSubtree", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	defer cancel()

	hash, err := chainhash.NewHash(request.Hash[:])
	if err != nil {
		return nil, fmt.Errorf("[CheckSubtree] Failed to parse subtree hash from request: %w", err)
	}

	if request.BaseUrl == "" {
		return nil, fmt.Errorf("[CheckSubtree] Missing base URL in request")
	}

	u.logger.Infof("[CheckSubtree] Received priority subtree message for %s from %s", hash.String(), request.BaseUrl)
	defer u.logger.Infof("[CheckSubtree] Finished processing priority subtree message for %s from %s", hash.String(), request.BaseUrl)

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
		gotLock, exists, releaseLockFunc, err := tryLockIfNotExists(ctx, u.logger, u.subtreeStore, hash)
		if err != nil {
			return nil, fmt.Errorf("[CheckSubtree] error getting lock for Subtree %s: %w", hash.String(), err)
		}
		defer releaseLockFunc()

		if exists {
			u.logger.Infof("[CheckSubtree] Priority subtree request no longer needed as subtree now exists for %s from %s", hash.String(), request.BaseUrl)

			return &subtreevalidation_api.CheckSubtreeResponse{
				Blessed: true,
			}, nil
		}

		if gotLock {
			u.logger.Infof("[CheckSubtree] Processing priority subtree message for %s from %s", hash.String(), request.BaseUrl)

			var subtree *util.Subtree
			if request.BaseUrl == "legacy" {
				// read from legacy store
				subtreeBytes, err := u.subtreeStore.Get(
					ctx,
					hash[:],
					options.WithSubDirectory("legacy"),
					options.WithFileExtension("subtree"),
				)
				if err != nil {
					return nil, errors.Join(fmt.Errorf("[getSubtreeTxHashes][%s] failed to get subtree from store", hash.String()), err)
				}

				subtree, err = util.NewSubtreeFromBytes(subtreeBytes)
				if err != nil {
					return nil, fmt.Errorf("[CheckSubtree] Failed to create subtree from bytes: %w", err)
				}
				txHashes := make([]chainhash.Hash, subtree.Length())
				for i := 0; i < subtree.Length(); i++ {
					txHashes[i] = subtree.Nodes[i].Hash
				}

				v := ValidateSubtree{
					SubtreeHash:   *hash,
					BaseUrl:       request.BaseUrl,
					TxHashes:      txHashes,
					AllowFailFast: false,
				}

				// Call the validateSubtreeInternal method
				if err = u.validateSubtreeInternal(ctx, v, request.BlockHeight); err != nil {
					return nil, fmt.Errorf("[CheckSubtree] Failed to validate subtree %s: %w", hash.String(), err)
				}

				u.logger.Infof("[CheckSubtree] Finished processing priority subtree message for %s from %s", hash.String(), request.BaseUrl)

				return &subtreevalidation_api.CheckSubtreeResponse{
					Blessed: true,
				}, nil
			}

			v := ValidateSubtree{
				SubtreeHash:   *hash,
				BaseUrl:       request.BaseUrl,
				AllowFailFast: false,
			}

			// Call the validateSubtreeInternal method
			if err = u.validateSubtreeInternal(ctx, v, request.BlockHeight); err != nil {
				return nil, fmt.Errorf("[CheckSubtree] Failed to validate subtree %s: %w", hash.String(), err)
			}

			u.logger.Infof("[CheckSubtree] Finished processing priority subtree message for %s from %s", hash.String(), request.BaseUrl)

			return &subtreevalidation_api.CheckSubtreeResponse{
				Blessed: true,
			}, nil

		} else {
			u.logger.Infof("[CheckSubtree] Failed to get lock for subtree %s, retry #%d", hash.String(), retryCount)
			// Wait for a bit before retrying.
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("[CheckSubtree] context cancelled")
			case <-time.After(1 * time.Second):
				retryCount++

				// will retry for 20 seconds
				if retryCount > 20 {
					return nil, fmt.Errorf("[CheckSubtree] failed to get lock for subtree %s after 20 retries", hash.String())
				}

				// Automatically retries the loop.
				continue
			}
		}
	}
}
