package subtreevalidation

import (
	"context"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/google/uuid"

	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
)

// listen to kafka topic for subtree hashes
// get subtree from asset server
// validate the subtree
func NewSubtreeValidation(
	logger ulogger.Logger,
	subtreeStore blob.Store,
	txStore blob.Store,
	txMetaStore txmeta.Store,
	validatorClient validator.Interface,
) *SubtreeValidation {

	subtreeTTLMinutes, _ := gocore.Config().GetInt("subtreevalidation_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	bv := &SubtreeValidation{
		logger:          logger,
		subtreeStore:    subtreeStore,
		subtreeTTL:      subtreeTTL,
		txStore:         txStore,
		txMetaStore:     txMetaStore,
		validatorClient: validatorClient,
		subtreeCount:    atomic.Int32{},
	}

	// create a caching tx meta store
	if gocore.Config().GetBool("subtreevalidation_txMetaCacheEnabled", true) {
		logger.Infof("Using cached version of tx meta store")
		bv.txMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore)
	} else {
		bv.txMetaStore = txMetaStore
	}

	return bv
}

func (u *SubtreeValidation) Init(ctx context.Context) (err error) {
	initPrometheusMetrics()
	return nil
}

func (u *SubtreeValidation) Start(ctx context.Context) error {
	subtreesKafkaURL, err, ok := gocore.Config().GetURL("kafka_subtreesConfig")
	if err == nil && ok {
		// By using the fixed "subtreevalidation" group ID, we ensure that only one instance of this service will process the subtree messages.
		go u.startKafkaListener(ctx, subtreesKafkaURL, "subtreevalidation", u.subtreeHandler)
	}

	txmetaKafkaURL, err, ok := gocore.Config().GetURL("kafka_txmetaConfig")
	if err == nil && ok {
		// Generate a unique group ID for the txmeta Kafka listener, to ensure that each instance of this service will process all txmeta messages.
		// This is necessary because the txmeta messages are used to populate the txmeta cache, which is shared across all instances of this service.
		groupID := "subtreevalidation-" + uuid.New().String()

		go u.startKafkaListener(ctx, txmetaKafkaURL, groupID, u.txmetaHandler)
	}

	<-ctx.Done()

	return nil
}

func (u *SubtreeValidation) Stop(_ context.Context) error {
	return nil
}

func (u *SubtreeValidation) startKafkaListener(ctx context.Context, kafkaURL *url.URL, groupID string, fn func(msg util.KafkaMessage)) {
	u.logger.Infof("starting Kafka on address: %s", kafkaURL.String())

	if err := util.StartKafkaGroupListener(ctx, u.logger, kafkaURL, groupID, nil, 1, fn); err != nil {
		u.logger.Errorf("Failed to start Kafka listener: %v", err)
	}
}
