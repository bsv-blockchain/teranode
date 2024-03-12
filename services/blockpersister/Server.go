package blockpersister

import (
	"context"
	"fmt"
	"net/url"
	"runtime"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// Server type carries the logger within it
type Server struct {
	logger               ulogger.Logger
	subtreeStore         blob.Store
	txMetaStore          txmeta.Store
	subtreeKafkaProducer util.KafkaProducerI
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, subtreeStore blob.Store, txMetaStore txmeta.Store) *Server {
	initPrometheusMetrics()

	return &Server{
		logger:       logger,
		subtreeStore: subtreeStore,
		txMetaStore:  txMetaStore,
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

	blocksKafkaURL, err, ok := gocore.Config().GetURL("block_kafkaConfig")
	if err == nil && ok {
		ps.startKafkaBlocksListener(ctx, blocksKafkaURL)
	}

	subtreesKafkaURL, err, ok := gocore.Config().GetURL("kafka_subtreesConfig")
	if err == nil && ok {
		if _, ps.subtreeKafkaProducer, err = util.ConnectToKafka(subtreesKafkaURL); err != nil {
			return fmt.Errorf("[BlockPersister] error connecting to kafka: %s", err)
		}
		ps.startKafkaSubtreesListener(ctx, subtreesKafkaURL)
	}

	return nil
}

// Stop function
func (ps *Server) Stop(_ context.Context) (err error) {
	return nil
}

func (ps *Server) ProcessSubtree(ctx context.Context, subtreeHash chainhash.Hash) error {
	ps.logger.Infof("[BlockPersister][%s] processing subtree data into subtree store", subtreeHash.String())

	// get the subtree from the subtree store
	subtreeReader, err := ps.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes())
	if err != nil {
		return fmt.Errorf("[BlockPersister] error getting subtree from store: %s", err)
	}
	defer func() {
		_ = subtreeReader.Close()
	}()

	subtree := util.Subtree{}
	err = subtree.DeserializeFromReader(subtreeReader)
	if err != nil {
		return fmt.Errorf("[BlockPersister] error deserializing subtree: %s", err)
	}

	subtreeBlob := make([]byte, 0, 250*1024*1024)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(util.Max(4, runtime.NumCPU()-2))

	batchSize := 1024
	results := make([][]byte, len(subtree.Nodes))
	// get the tx metas from the tx meta store in batches of N
	for i := 0; i < len(subtree.Nodes); i += batchSize {
		i := i // capture range variable for goroutine

		end := util.Min(i+batchSize, len(subtree.Nodes))

		missingTxHashesCompacted := make([]*txmeta.MissingTxHash, 0, end-i)

		g.Go(func() error {

			for j := 0; j < util.Min(batchSize, len(subtree.Nodes)-i); j++ {

				if !subtree.Nodes[i+j].Hash.Equal(model.CoinbasePlaceholder) {
					missingTxHashesCompacted = append(missingTxHashesCompacted, &txmeta.MissingTxHash{
						Hash: &subtree.Nodes[i+j].Hash,
						Idx:  i + j,
					})
				}
			}

			if err := ps.txMetaStore.MetaBatchDecorate(gCtx, missingTxHashesCompacted, "tx"); err != nil {
				return fmt.Errorf("[BlockPersister] error getting tx metas from store: %s", err)
			}

			for _, data := range missingTxHashesCompacted {
				if data.Data != nil {
					results[data.Idx] = data.Data.Tx.Bytes()
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("[BlockPersister] error getting tx metas from store: %s", err)
	}

	// get the tx bytes in order
	for _, txBytes := range results {
		// get the tx meta from the tx meta store
		subtreeBlob = append(subtreeBlob, txBytes...)
	}

	// store the subtree blob to the blob store
	if err = ps.subtreeStore.Set(ctx, subtreeHash.CloneBytes(), subtreeBlob, options.WithFileExtension("data")); err != nil {
		return fmt.Errorf("[BlockPersister] error storing subtree to store: %s", err)
	}

	return nil
}

// startKafkaBlocksListener listens to all new blocks on the blocks topic and processes the subtree hashes into the subtrees topic
func (ps *Server) startKafkaBlocksListener(ctx context.Context, kafkaURL *url.URL) {
	workers, _ := gocore.Config().GetInt("block_kafkaWorkers", runtime.NumCPU()*2)
	if workers < 1 {
		// no workers, nothing to do
		return
	}

	ps.logger.Infof("[BlockPersister] Starting block Kafka on address: %s, with %d workers", kafkaURL.String(), workers)

	util.StartKafkaListener(ctx, ps.logger, kafkaURL, workers, "BlockPersister", "blockpersister", func(ctx context.Context, key []byte, dataBytes []byte) error {
		startTime := time.Now()
		defer func() {
			prometheusBlockPersisterBlocks.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		}()

		keyStr := utils.ReverseAndHexEncodeSlice(key)

		block, err := model.NewBlockFromBytes(dataBytes)
		if err != nil {
			return fmt.Errorf("[BlockPersister][%s] error deserializing block: %s", keyStr, err)
		}

		ps.logger.Warnf("[BlockPersister][%s] sending block subtrees to kafka: %s:%d", keyStr, block.String(), len(block.Subtrees))

		// store all the subtree hashes of the block in the subtrees topic
		for _, subtreeHash := range block.Subtrees {
			ps.logger.Warnf("[BlockPersister][%s] sending subtree to kafka: %s", keyStr, subtreeHash.String())
			if err = ps.subtreeKafkaProducer.Send(subtreeHash.CloneBytes(), subtreeHash.CloneBytes()); err != nil {
				return fmt.Errorf("[BlockPersister][%s] error sending subtree to kafka: %s", keyStr, err)
			}
		}

		return nil
	})
}

// startKafkaSubtreesListener listens to all new subtrees on the subtrees topic and processes the subtree data into the subtree store
func (ps *Server) startKafkaSubtreesListener(ctx context.Context, kafkaURL *url.URL) {
	workers, _ := gocore.Config().GetInt("subtree_kafkaWorkers", runtime.NumCPU()*2)
	if workers < 1 {
		// no workers, nothing to do
		return
	}

	ps.logger.Infof("[BlockPersister] Starting subtree Kafka on address: %s, with %d workers", kafkaURL.String(), workers)

	util.StartKafkaListener(ctx, ps.logger, kafkaURL, workers, "BlockPersister", "blockpersister", func(ctx context.Context, key []byte, dataBytes []byte) error {
		startTime := time.Now()
		defer func() {
			prometheusBlockPersisterSubtrees.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		}()

		keyStr := utils.ReverseAndHexEncodeSlice(key)

		ps.logger.Warnf("[BlockPersister][%s] propcessing subtree data", keyStr)

		if err := ps.ProcessSubtree(ctx, chainhash.Hash(dataBytes)); err != nil {
			return fmt.Errorf("[BlockPersister][%s] error processing subtree: %s", keyStr, err)
		}

		return nil
	})
}
