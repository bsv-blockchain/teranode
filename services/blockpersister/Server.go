package blockpersister

import (
	"context"
	"net/url"
	"runtime"
	"strconv"

	"golang.org/x/sync/errgroup"

	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/google/uuid"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
)

// Server type carries the logger within it
type Server struct {
	logger       ulogger.Logger
	subtreeStore blob.Store
	txMetaStore  txmeta_store.Store
	stats        *gocore.Stat
}

func New(
	ctx context.Context,
	logger ulogger.Logger,
	subtreeStore blob.Store,
	txMetaStore txmeta.Store,
) *Server {

	u := &Server{
		logger:       logger,
		subtreeStore: subtreeStore,
		txMetaStore:  txMetaStore,
		stats:        gocore.NewStat("blockpersister"),
	}

	// create a caching tx meta store
	if gocore.Config().GetBool("blockpersister_txMetaCacheEnabled", true) {
		logger.Infof("Using cached version of tx meta store")
		u.txMetaStore = txmetacache.NewTxMetaCache(ctx, ulogger.TestLogger{}, txMetaStore)
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
	blocksFinalKafkaURL, err, ok := gocore.Config().GetURL("kafka_blocksFinalConfig")
	if err == nil && ok {
		// Start a number of Kafka consumers equal to the number of CPU cores, minus 16 to leave processing for the tx meta cache.
		// subtreeConcurrency, _ := gocore.Config().GetInt("blockpersister_kafkaSubtreeConcurrency", util.Max(4, runtime.NumCPU()-16))
		// g.SetLimit(subtreeConcurrency)
		var partitions int
		if partitions, err = strconv.Atoi(blocksFinalKafkaURL.Query().Get("partitions")); err != nil {
			u.logger.Fatalf("[BlockPersister] unable to parse Kafka partitions from %s: %s", blocksFinalKafkaURL, err)
		}

		consumerRatio := util.GetQueryParamInt(blocksFinalKafkaURL, "consumer_ratio", 4)
		if consumerRatio < 1 {
			consumerRatio = 1
		}

		consumerCount := partitions / consumerRatio

		if consumerCount <= 0 {
			consumerCount = 1
		}

		// set the concurrency limit by default to leave 16 cpus for doing tx meta processing
		blocksFinalConcurrency, _ := gocore.Config().GetInt("blockpersister_kafkaBlocksFinalConcurrency", util.Max(4, runtime.NumCPU()-16))
		g := errgroup.Group{}
		g.SetLimit(blocksFinalConcurrency)

		// By using the fixed "blockpersister" group ID, we ensure that only one instance of this service will process the subtree messages.
		u.logger.Infof("Starting %d Kafka consumers for blocksFinal messages", consumerCount)
		go u.startKafkaListener(ctx, blocksFinalKafkaURL, "blockpersister", consumerCount, func(msg util.KafkaMessage) {
			g.Go(func() error {
				// TODO is there a way to return an error here and have Kafka mark the message as not done?
				u.blocksFinalHandler(msg)
				return nil
			})
		})
	}

	txmetaKafkaURL, err, ok := gocore.Config().GetURL("kafka_txmetaConfig")
	if err == nil && ok {
		var partitions int
		if partitions, err = strconv.Atoi(txmetaKafkaURL.Query().Get("partitions")); err != nil {
			u.logger.Fatalf("[BlockPersister] unable to parse Kafka partitions from %s: %s", txmetaKafkaURL, err)
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
		groupID := "blockpersister-" + uuid.New().String()

		u.logger.Infof("Starting %d Kafka consumers for tx meta messages", consumerCount)
		go u.startKafkaListener(ctx, txmetaKafkaURL, groupID, consumerCount, u.txmetaHandler)
	}

	<-ctx.Done()

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
