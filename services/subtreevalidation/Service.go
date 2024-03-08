package subtreevalidation

import (
	"context"
	"net/url"
	"sync/atomic"
	"time"

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

	return bv
}

func (u *SubtreeValidation) Init(ctx context.Context) (err error) {
	initPrometheusMetrics()
	return nil
}

func (u *SubtreeValidation) Start(ctx context.Context) error {
	subtreesKafkaURL, err, ok := gocore.Config().GetURL("kafka_subtreesConfig")
	if err == nil && ok {
		go u.startKafkaListener(ctx, subtreesKafkaURL, u.subtreeHandler)
	}

	txmetaKafkaURL, err, ok := gocore.Config().GetURL("kafka_txmetaConfig")
	if err == nil && ok {
		go u.startKafkaListener(ctx, txmetaKafkaURL, u.txmetaHandler)
	}

	<-ctx.Done()

	return nil
}

func (u *SubtreeValidation) Stop(_ context.Context) error {
	return nil
}

func (u *SubtreeValidation) startKafkaListener(ctx context.Context, kafkaURL *url.URL, fn func(msg util.KafkaMessage)) {
	u.logger.Infof("starting Kafka on address: %s", kafkaURL.String())

	if err := util.StartKafkaGroupListener(ctx, u.logger, kafkaURL, "subtreevalidation", nil, 1, fn); err != nil {
		u.logger.Errorf("Failed to start Kafka listener: %v", err)
	}
}
