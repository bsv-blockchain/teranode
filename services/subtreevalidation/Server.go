package subtreevalidation

import (
	"context"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/google/uuid"
	"github.com/libsv/go-bt/v2/chainhash"

	"github.com/bitcoin-sv/ubsv/services/subtreevalidation/subtreevalidation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
)

var stats = gocore.NewStat("subtreevalidation")

// Server type carries the logger within it
type Server struct {
	subtreevalidation_api.UnimplementedSubtreeValidationAPIServer
	logger                   ulogger.Logger
	subtreeStore             blob.Store
	subtreeTTL               time.Duration
	txStore                  blob.Store
	txMetaStore              txmeta_store.Store
	validatorClient          validator.Interface
	subtreeCount             atomic.Int32
	maxMerkleItemsPerSubtree int
}

func Enabled() bool {
	_, found := gocore.Config().Get("subtreevalidation_grpcListenAddress")
	return found
}

func New(
	logger ulogger.Logger,
	subtreeStore blob.Store,
	txStore blob.Store,
	txMetaStore txmeta.Store,
	validatorClient validator.Interface,
) *Server {

	maxMerkleItemsPerSubtree, _ := gocore.Config().GetInt("initial_merkle_items_per_subtree", 1024)
	subtreeTTLMinutes, _ := gocore.Config().GetInt("subtreevalidation_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	u := &Server{
		logger:                   logger,
		subtreeStore:             subtreeStore,
		subtreeTTL:               subtreeTTL,
		txStore:                  txStore,
		txMetaStore:              txMetaStore,
		validatorClient:          validatorClient,
		subtreeCount:             atomic.Int32{},
		maxMerkleItemsPerSubtree: maxMerkleItemsPerSubtree,
	}

	// create a caching tx meta store
	if gocore.Config().GetBool("subtreevalidation_txMetaCacheEnabled", true) {
		logger.Infof("Using cached version of tx meta store")
		u.txMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore)
	} else {
		u.txMetaStore = txMetaStore
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
		go u.startKafkaListener(ctx, subtreesKafkaURL, "subtreevalidation", consumerCount, func(msg util.KafkaMessage) {
			g.Go(func() error {
				// TODO is there a way to return an error here and have Kafka mark the message as not done?
				u.subtreeHandler(msg)
				return nil
			})
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

func (u *Server) startKafkaListener(ctx context.Context, kafkaURL *url.URL, groupID string, consumerCount int, fn func(msg util.KafkaMessage)) {
	u.logger.Infof("starting Kafka on address: %s", kafkaURL.String())

	if err := util.StartKafkaGroupListener(ctx, u.logger, kafkaURL, groupID, nil, consumerCount, fn); err != nil {
		u.logger.Errorf("Failed to start Kafka listener: %v", err)
	}
}

func (u *Server) Stop(_ context.Context) error {
	return nil
}

func (u *Server) HealthGRPC(_ context.Context, _ *subtreevalidation_api.EmptyMessage) (*subtreevalidation_api.HealthResponse, error) {
	start, stat, _ := util.NewStatFromContext(context.Background(), "Health", stats)
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
	start, stat, ctx, cancel := util.NewStatFromContextWithCancel(ctx, "CheckSubtree", stats)
	defer func() {
		stat.AddTime(start)
	}()

	defer cancel()

	hash, err := chainhash.NewHash(request.Hash[:])
	if err != nil {
		return nil, fmt.Errorf("Failed to parse subtree hash from request: %w", err)
	}

	if request.BaseUrl == "" {
		return nil, fmt.Errorf("Missing base URL in request")
	}

	u.logger.Infof("Received priority subtree message for %s from %s", hash.String(), request.BaseUrl)

	retryCount := 0

	for {
		gotLock, exists, err := tryLockIfNotExists(ctx, u.subtreeStore, hash)
		if err != nil {
			return nil, fmt.Errorf("error getting lock for Subtree %s: %w", hash.String(), err)
		}

		if exists {
			return &subtreevalidation_api.CheckSubtreeResponse{
				Blessed: true,
			}, nil
		}

		if gotLock {
			v := ValidateSubtree{
				SubtreeHash:   *hash,
				BaseUrl:       request.BaseUrl,
				SubtreeHashes: nil,
				AllowFailFast: false,
			}

			// Call the validateSubtreeInternal method
			if err = u.validateSubtreeInternal(ctx, v); err != nil {
				return nil, fmt.Errorf("Failed to validate subtree %s: %w", hash.String(), err)
			}

			return &subtreevalidation_api.CheckSubtreeResponse{
				Blessed: true,
			}, nil

		} else {
			// Wait for a bit before retrying.
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("Context cancelled")
			case <-time.After(500 * time.Millisecond):
				retryCount++

				if retryCount > 10 {
					return nil, fmt.Errorf("Failed to get lock for subtree %s after 10 retries", hash.String())
				}

				// Automatically retries the loop.
				continue
			}
		}
	}
}
