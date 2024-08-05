package blockpersister

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/google/uuid"
	"github.com/ordishs/gocore"
)

// Server type carries the logger within it
type Server struct {
	ctx          context.Context
	logger       ulogger.Logger
	blockStore   blob.Store
	subtreeStore blob.Store
	utxoStore    utxo.Store
	stats        *gocore.Stat
}

func New(
	ctx context.Context,
	logger ulogger.Logger,
	blockStore blob.Store,
	subtreeStore blob.Store,
	utxoStore utxo.Store,
) *Server {

	u := &Server{
		ctx:          ctx,
		logger:       logger,
		blockStore:   blockStore,
		subtreeStore: subtreeStore,
		utxoStore:    utxoStore,
		stats:        gocore.NewStat("blockpersister"),
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
		u.logger.Infof("Starting Kafka consumer for blocksFinal messages")

		// Generate a unique group ID for the txmeta Kafka listener, to ensure that each instance of this service will process all txmeta messages.
		// This is necessary because the txmeta messages are used to populate the txmeta cache, which is shared across all instances of this service.
		groupID := "blockpersister-" + uuid.New().String()

		// By using the fixed "blockpersister" group ID, we ensure that only one instance of this service will process the blocksFinal messages.
		go u.startKafkaListener(ctx, blocksFinalKafkaURL, groupID, 1, func(msg util.KafkaMessage) {
			u.blocksFinalHandler(msg)
		})
	}

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

func (u *Server) startKafkaListener(ctx context.Context, kafkaURL *url.URL, groupID string, consumerCount int, fn func(msg util.KafkaMessage)) {
	u.logger.Infof("starting Kafka on address: %s", kafkaURL.String())

	if err := util.StartKafkaGroupListener(ctx, u.logger, kafkaURL, groupID, nil, consumerCount, fn); err != nil {
		u.logger.Errorf("Failed to start Kafka listener: %v", err)
	}
}

func (u *Server) Stop(_ context.Context) error {
	return nil
}
