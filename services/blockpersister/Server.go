package blockpersister

import (
	"context"
	"net/http"
	"net/url"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/google/uuid"
	"github.com/ordishs/gocore"
)

// Server type carries the logger within it
type Server struct {
	ctx                            context.Context
	logger                         ulogger.Logger
	blockStore                     blob.Store
	subtreeStore                   blob.Store
	utxoStore                      utxo.Store
	stats                          *gocore.Stat
	blockchainClient               blockchain.ClientI
	blocksFinalKafkaConsumerClient *kafka.KafkaConsumerGroup
	kafkaHealthURL                 *url.URL
}

func New(
	ctx context.Context,
	logger ulogger.Logger,
	blockStore blob.Store,
	subtreeStore blob.Store,
	utxoStore utxo.Store,
	blockchainClient blockchain.ClientI,
) *Server {
	u := &Server{
		ctx:              ctx,
		logger:           logger,
		blockStore:       blockStore,
		subtreeStore:     subtreeStore,
		utxoStore:        utxoStore,
		stats:            gocore.NewStat("blockpersister"),
		blockchainClient: blockchainClient,
	}

	// clean old files from working dir
	// dir, ok := gocore.Config().Get("blockPersister_workingDir")
	// if ok {
	// 	logger.Infof("[BlockPersister] Cleaning old files from working dir: %s", dir)
	// 	files, err := os.ReadDir(dir)
	// 	if err != nil {
	// 		logger.Fatalf("error reading working dir: %v", err)
	// 	}
	// 	for _, file := range files {
	// 		fileInfo, err := file.Info()
	// 		if err != nil {
	// 			logger.Errorf("error reading file info: %v", err)
	// 		}
	// 		if time.Since(fileInfo.ModTime()) > 30*time.Minute {
	// 			logger.Infof("removing old file: %s", file.Name())
	// 			if err = os.Remove(path.Join(dir, file.Name())); err != nil {
	// 				logger.Errorf("error removing old file: %v", err)
	// 			}
	// 		}
	// 	}
	// }

	return u
}

func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := []health.Check{
		{Name: "BlockchainClient", Check: u.blockchainClient.Health},
		{Name: "BlockStore", Check: u.blockStore.Health},
		{Name: "SubtreeStore", Check: u.subtreeStore.Health},
		{Name: "UTXOStore", Check: u.utxoStore.Health},
		{Name: "FSM", Check: blockchain.CheckFSM(u.blockchainClient)},
		{Name: "Kafka", Check: kafka.HealthChecker(ctx, u.kafkaHealthURL)},
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (u *Server) Init(ctx context.Context) (err error) {
	initPrometheusMetrics()

	blocksFinalKafkaURL, err, ok := gocore.Config().GetURL("kafka_blocksFinalConfig")
	if err == nil && ok {
		u.kafkaHealthURL = blocksFinalKafkaURL
		u.logger.Infof("Starting Kafka consumer for blocksFinal messages")

		// Generate a unique group ID for the txmeta Kafka listener, to ensure that each instance of this service will process all txmeta messages.
		// This is necessary because the txmeta messages are used to populate the txmeta cache, which is shared across all instances of this service.
		groupID := "blockpersister-" + uuid.New().String()

		// By using the fixed "blockpersister" group ID, we ensure that only one instance of this service will process the blocksFinal messages.
		kafkaMessageHandler := func(msg kafka.KafkaMessage) error {
			// this does manual commit so we need to implement error handling and differentiate between errors
			errCh := make(chan error, 1)
			go func() {
				errCh <- u.blocksFinalHandler(msg)
			}()

			err := <-errCh
			// if err is nil, it means function is successfully executed, return nil.
			if err == nil {
				return nil
			}

			if errors.Is(err, errors.ErrBlockExists) {
				// if block exists, it is not an error, so return nil to mark message as committed.
				return nil
			}

			// currently, the following cases are considered recoverable:
			// ERR_STORAGE_ERROR, ERR_SERVICE_ERROR
			// all other cases, including but not limited to, are considered as unrecoverable:
			// ERR_PROCESSING, ERR_BLOCK_EXISTS, ERR_INVALID_ARGUMENT

			// If error is not nil, check if the error is a recoverable error.
			// If it is a recoverable error, then return the error, so that it kafka message is not marked as committed.
			// So the message will be consumed again.
			if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrServiceError) {
				u.logger.Errorf("blocksFinalHandler failed: %v", err)
				return err
			}

			// error is not nil and not recoverable, so it is unrecoverable error, and it should not be tried again
			// kafka message should be committed, so return nil to mark message.
			u.logger.Errorf("Unrecoverable error (%v) processing kafka message %v for block persister block handler, marking Kafka message as completed.\n", msg, err)
			return nil
		}
		u.blocksFinalKafkaConsumerClient, err = kafka.NewKafkaConsumeGroup(ctx, kafka.KafkaListenerConfig{
			Logger:            u.logger,
			URL:               blocksFinalKafkaURL,
			GroupID:           groupID,
			ConsumerCount:     1,
			AutoCommitEnabled: false,
			ConsumerFn:        kafkaMessageHandler,
		})
		if err != nil {
			return errors.NewConfigurationError("failed to create new Kafka listener for %s: %v", blocksFinalKafkaURL.String(), err)
		}
	}

	return nil
}

// Start function
func (u *Server) Start(ctx context.Context) error {
	blockPersisterHTTPListenAddress, addressFound := gocore.Config().Get("blockPersister_httpListenAddress")

	if addressFound {
		blockStoreURL, err, found := gocore.Config().GetURL("blockstore")
		if err != nil {
			return errors.NewConfigurationError("blockstore setting error", err)
		}

		if !found {
			return errors.NewConfigurationError("blockstore config not found")
		}

		blobStoreServer, err := blob.NewHTTPBlobServer(u.logger, blockStoreURL)
		if err != nil {
			return errors.NewServiceError("failed to create blob store server", err)
		}

		go func() {
			u.logger.Warnf("blockStoreServer ended: %v", blobStoreServer.Start(ctx, blockPersisterHTTPListenAddress))
		}()
	}

	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		err := u.blockchainClient.Restore(ctx)
		if err != nil {
			u.logger.Errorf("[Block Persister] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		u.logger.Infof("[Block Persister] Node is restoring, waiting for FSM to transition to Running state")
		_ = u.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain.FSMStateRUNNING)
		u.logger.Infof("[Block Persister] Node finished restoring and has transitioned to Running state, continuing to start Block Persister service")
	}

	go u.blocksFinalKafkaConsumerClient.Start(ctx)

	// http.HandleFunc("GET /block/", func(w http.ResponseWriter, req *http.Request) {
	// 	hashStr := req.PathValue("hash")

	// 	if hashStr == "" {
	// 		http.Error(w, "missing hash", http.StatusBadRequest)
	// 		return
	// 	}

	// 	hash, err := chainhash.NewHashFromStr(hashStr)
	// 	if err != nil {
	// 		http.Error(w, "invalid hash", http.StatusBadRequest)
	// 		return
	// 	}

	// 	client, err := blockchain.NewClient(req.Context(), u.logger)
	// 	if err != nil {
	// 		http.Error(w, "failed to create blockchain client", http.StatusInternalServerError)
	// 		return
	// 	}

	// 	block, err := client.GetBlock(req.Context(), hash)
	// 	if err != nil {
	// 		http.Error(w, "failed to get block", http.StatusInternalServerError)
	// 		return
	// 	}

	// 	blockBytes, err := block.Bytes()
	// 	if err != nil {
	// 		http.Error(w, "failed to get block bytes", http.StatusInternalServerError)
	// 		return
	// 	}

	// 	u.persistBlock(req.Context(), hash, blockBytes)

	// 	w.WriteHeader(http.StatusOK)
	// })

	<-ctx.Done()

	return nil
}

func (u *Server) Stop(_ context.Context) error {
	return nil
}
