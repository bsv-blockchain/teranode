package subtreeassembly

import (
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"net/url"
	"runtime"
	"time"
)

// Server type carries the logger within it
type Server struct {
	logger               ulogger.Logger
	blockchainClient     blockchain.ClientI
	subtreeStore         blob.Store
	txMetaStore          txmeta.Store
	subtreeKafkaProducer util.KafkaProducerI
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, blockchainClient blockchain.ClientI, subtreeStore blob.Store, txMetaStore txmeta.Store) *Server {
	initPrometheusMetrics()

	return &Server{
		logger:           logger,
		blockchainClient: blockchainClient,
		subtreeStore:     subtreeStore,
		txMetaStore:      txMetaStore,
	}
}

func (ps *Server) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (ps *Server) Init(_ context.Context) (err error) {
	return nil
}

// Start function
func (ps *Server) Start(ctx context.Context) (err error) {

	blockKafkaBrokersURL, err, ok := gocore.Config().GetURL("block_kafkaBrokers")
	if err == nil && ok {
		ps.startKafkaBlocksListener(ctx, blockKafkaBrokersURL)
	}

	subtreeKafkaBrokersURL, err, ok := gocore.Config().GetURL("subtree_kafkaBrokers")
	if err == nil && ok {
		_, ps.subtreeKafkaProducer, err = util.ConnectToKafka(subtreeKafkaBrokersURL)
		ps.startKafkaSubtreesListener(ctx, subtreeKafkaBrokersURL)
	}

	return nil
}

// Stop function
func (ps *Server) Stop(_ context.Context) (err error) {
	return nil
}

func (ps *Server) ProcessSubtree(ctx context.Context, subtreeHash chainhash.Hash) error {
	ps.logger.Infof("[SubtreeAssembly][%s] processing subtree data into subtree store", subtreeHash.String())

	// get the subtree from the subtree store
	subtreeReader, err := ps.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes())
	if err != nil {
		return fmt.Errorf("[SubtreeAssembly] error getting subtree from store: %s", err)
	}

	subtree := util.Subtree{}
	err = subtree.DeserializeFromReader(subtreeReader)
	if err != nil {
		return fmt.Errorf("[SubtreeAssembly] error deserializing subtree: %s", err)
	}

	subtreeBlob := make([]byte, 0, 250*1024*1024)
	for _, txHash := range subtree.Nodes {
		// get the tx meta from the tx meta store
		// TODO get the data in batches
		txMeta, err := ps.txMetaStore.Get(ctx, &txHash.Hash)
		if err != nil {
			return fmt.Errorf("[SubtreeAssembly] error getting tx meta from store: %s", err)
		}

		subtreeBlob = append(subtreeBlob, txMeta.Tx.Bytes()...)
	}

	// store the subtree blob to the blob store
	if err = ps.subtreeStore.Set(ctx, subtreeHash.CloneBytes(), subtreeBlob, options.WithFileExtension("data")); err != nil {
		return fmt.Errorf("[SubtreeAssembly] error storing subtree to store: %s", err)
	}

	return nil
}

// startKafkaBlocksListener listens to all new blocks on the blocks topic and processes the subtree hashes into the subtrees topic
func (ps *Server) startKafkaBlocksListener(ctx context.Context, kafkaBrokersURL *url.URL) {
	workers, _ := gocore.Config().GetInt("block_kafkaWorkers", runtime.NumCPU()*2)
	if workers < 1 {
		// no workers, nothing to do
		return
	}

	ps.logger.Infof("[SubtreeAssembly] Starting block Kafka on address: %s, with %d workers", kafkaBrokersURL.String(), workers)

	util.StartKafkaListener(ctx, ps.logger, kafkaBrokersURL, workers, "SubtreeAssembly", "subtreeassembly", func(ctx context.Context, key []byte, dataBytes []byte) error {
		startTime := time.Now()
		defer func() {
			prometheusSubtreeAssemblyBlocks.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		}()

		keyStr := utils.ReverseAndHexEncodeSlice(key)

		block, err := model.NewBlockFromBytes(dataBytes)
		if err != nil {
			return fmt.Errorf("[SubtreeAssembly][%s] error deserializing block: %s", keyStr, err)
		}

		ps.logger.Warnf("[SubtreeAssembly][%s] sending block subtrees to kafka: %s", keyStr, block.String())

		// store all the subtree hashes of the block in the subtrees topic
		for _, subtreeHash := range block.Subtrees {
			ps.logger.Warnf("[SubtreeAssembly][%s] sending subtree to kafka: %s", keyStr, subtreeHash.String())
			if err = ps.subtreeKafkaProducer.Send(subtreeHash.CloneBytes(), subtreeHash.CloneBytes()); err != nil {
				return fmt.Errorf("[SubtreeAssembly][%s] error sending subtree to kafka: %s", keyStr, err)
			}
		}

		return nil
	})
}

// startKafkaSubtreesListener listens to all new subtrees on the subtrees topic and processes the subtree data into the subtree store
func (ps *Server) startKafkaSubtreesListener(ctx context.Context, kafkaBrokersURL *url.URL) {
	workers, _ := gocore.Config().GetInt("subtree_kafkaWorkers", runtime.NumCPU()*2)
	if workers < 1 {
		// no workers, nothing to do
		return
	}

	ps.logger.Infof("[SubtreeAssembly] Starting subtree Kafka on address: %s, with %d workers", kafkaBrokersURL.String(), workers)

	util.StartKafkaListener(ctx, ps.logger, kafkaBrokersURL, workers, "SubtreeAssembly", "subtreeassembly", func(ctx context.Context, key []byte, dataBytes []byte) error {
		startTime := time.Now()
		defer func() {
			prometheusSubtreeAssemblySubtrees.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		}()

		keyStr := utils.ReverseAndHexEncodeSlice(key)

		ps.logger.Warnf("[SubtreeAssembly][%s] propcessing subtree data", keyStr)

		if err := ps.ProcessSubtree(ctx, chainhash.Hash(dataBytes)); err != nil {
			return fmt.Errorf("[SubtreeAssembly][%s] error processing subtree: %s", keyStr, err)
		}

		return nil
	})
}
