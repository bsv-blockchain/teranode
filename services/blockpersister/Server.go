package blockpersister

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
)

// Server type carries the logger within it
type Server struct {
	logger       ulogger.Logger
	subtreeStore blob.Store
	txMetaStore  txmeta.Store
	bp           *blockPersister
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, subtreeStore blob.Store, txMetaStore txmeta.Store) *Server {
	initPrometheusMetrics()

	persistURL, err, ok := gocore.Config().GetURL("blockPersister_persistURL")
	if err != nil || !ok {
		logger.Fatalf("Error getting blockpersister_store URL: %v", err)
	}

	bp := newBlockPersister(logger, persistURL, subtreeStore, txMetaStore)

	return &Server{
		logger:       logger,
		subtreeStore: subtreeStore,
		txMetaStore:  txMetaStore,
		bp:           bp,
	}
}

func (ps *Server) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (ps *Server) Init(_ context.Context) (err error) {
	return nil
}

func (ps *Server) Start(ctx context.Context) (err error) {
	kafkaURL, err, ok := gocore.Config().GetURL("kafka_blocksFinalConfig")
	if err == nil && ok {
		ps.logger.Infof("[BlockPersister] Starting subtree Kafka on address: %s, with %d workers", kafkaURL.String(), 1)

		util.StartKafkaListener(ctx, ps.logger, kafkaURL, 1, "BlockPersister", "blockpersister", ps.bp.blockFinalHandler)
	}

	<-ctx.Done()

	return nil
}

// Stop function
func (ps *Server) Stop(_ context.Context) (err error) {
	return nil
}
