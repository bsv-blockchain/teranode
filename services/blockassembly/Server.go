package blockassembly

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/blockassembly_api"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/TAAL-GmbH/ubsv/services/txmeta"
	"github.com/TAAL-GmbH/ubsv/services/txmeta/store"
	"github.com/TAAL-GmbH/ubsv/services/validator/utxo"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	"github.com/TAAL-GmbH/ubsv/stores/blob/options"
	txmeta_store "github.com/TAAL-GmbH/ubsv/stores/txmeta"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	prometheusBlockAssemblyAddTx          prometheus.Counter
	prometheusTxMetaGetDuration           prometheus.Histogram
	prometheusUtxoStoreDuration           prometheus.Histogram
	prometheusSubtreeAddToChannelDuration prometheus.Histogram

	jobTTL = 10 * time.Minute
)

func init() {
	prometheusBlockAssemblyAddTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockassembly_add_tx",
			Help: "Number of txs added to the blockassembly service",
		},
	)

	prometheusTxMetaGetDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "blockassembly_tx_meta_get_duration",
			Help: "Duration of reading tx meta data from txmeta store",
		},
	)

	prometheusUtxoStoreDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "blockassembly_utxo_store_duration",
			Help: "Duration of storing new utxos by BlockAssembly",
		},
	)

	prometheusSubtreeAddToChannelDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "blockassembly_add_tx_to_channel_duration",
			Help: "Duration of writing tx to subtree processor channel",
		},
	)
}

// BlockAssembly type carries the logger within it
type BlockAssembly struct {
	blockassembly_api.UnimplementedBlockAssemblyAPIServer
	blockAssembler *BlockAssembler
	logger         utils.Logger

	grpcServer       *grpc.Server
	blockchainClient blockchain.ClientI
	txStore          blob.Store
	subtreeStore     blob.Store
	jobStore         *ttlcache.Cache[chainhash.Hash, *subtreeprocessor.Job] // has built in locking
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockassembly_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, txStore blob.Store, subtreeStore blob.Store) *BlockAssembly {
	blockchainClient, err := blockchain.NewClient()
	if err != nil {
		panic(err)
	}

	ba := &BlockAssembly{
		logger:           logger,
		blockchainClient: blockchainClient,
		txStore:          txStore,
		subtreeStore:     subtreeStore,
		jobStore:         ttlcache.New[chainhash.Hash, *subtreeprocessor.Job](),
	}

	return ba
}

func (ba *BlockAssembly) Init(ctx context.Context) error {
	utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		return fmt.Errorf("failed to get utxostore setting [%w]", err)
	}
	if !found {
		return fmt.Errorf("no utxostore setting found")
	}

	utxoStore, err := utxo.NewStore(ba.logger, utxostoreURL)
	if err != nil {
		return fmt.Errorf("failed to create utxo store [%w]", err)
	}

	txMetaStoreURL, err, found := gocore.Config().GetURL("txmeta_store")
	if err != nil {
		return fmt.Errorf("failed to get txmeta_store setting [%w]", err)
	}
	if !found {
		return fmt.Errorf("no txmeta_store setting found")
	}

	// TODO abstract into a factory
	var txMetaStore txmeta_store.Store
	if txMetaStoreURL.Scheme == "memory" {
		// the memory store is reached through a grpc client
		txMetaStore, err = txmeta.NewClient(ctx, ba.logger)
		if err != nil {
			return fmt.Errorf("failed to create txmeta store [%w]", err)
		}
	} else {
		txMetaStore, err = store.New(ba.logger, txMetaStoreURL)
		if err != nil {
			return fmt.Errorf("failed to create txmeta store [%w]", err)
		}
	}

	newSubtreeChan := make(chan *util.Subtree)

	// init the block assembler for this server
	ba.blockAssembler = NewBlockAssembler(ctx, ba.logger, txMetaStore, utxoStore, ba.txStore, ba.subtreeStore, ba.blockchainClient, newSubtreeChan)

	// start the new subtree listener in the background
	go func() {
		var subtreeBytes []byte
		for {
			subtree := <-newSubtreeChan
			// merkleRoot := stp.currentSubtree.ReplaceRootNode(*coinbaseHash)
			// assert.Equal(t, expectedMerkleRoot, utils.ReverseAndHexEncodeHash(merkleRoot))

			if subtreeBytes, err = subtree.Serialize(); err != nil {
				ba.logger.Errorf("Failed to serialize subtree [%s]", err)
				continue
			}

			if err = ba.subtreeStore.Set(ctx,
				subtree.RootHash()[:],
				subtreeBytes,
				options.WithTTL(120*time.Minute), // this sets the TTL for the subtree, it must be updated when a block is mined
			); err != nil {
				ba.logger.Errorf("Failed to store subtree [%s]", err)
				continue
			}

			if err = ba.blockchainClient.SendNotification(ctx, &model.Notification{
				Type: model.NotificationType_Subtree,
				Hash: subtree.RootHash(),
			}); err != nil {
				ba.logger.Errorf("Failed to send subtree notification [%s]", err)
			}

			ba.logger.Infof("Received new subtree notification for: %s (len %d)", subtree.RootHash().String(), subtree.Length())
		}
	}()

	return nil
}

// Start function
func (ba *BlockAssembly) Start(ctx context.Context) error {

	kafkaBrokers, ok := gocore.Config().Get("blockassembly_kafkaBrokers")
	if ok {
		ba.logger.Infof("[BlockAssembly] Starting Kafka on address: %s", kafkaBrokers)
		kafkaURL, err := url.Parse(kafkaBrokers)
		if err != nil {
			ba.logger.Errorf("[BlockAssembly] Kafka failed to start: %s", err)
		} else {
			workers, _ := gocore.Config().GetInt("blockassembly_kafkaWorkers", 100)
			ba.logger.Infof("[BlockAssembly] Kafka consumer started with %d workers", workers)

			var clusterAdmin sarama.ClusterAdmin

			// create the workers to process all messages
			n := atomic.Uint64{}
			workerCh := make(chan []byte)
			for i := 0; i < workers; i++ {
				go func() {
					for txIDBytes := range workerCh {
						if _, err = ba.AddTx(ctx, &blockassembly_api.AddTxRequest{
							Txid: txIDBytes,
						}); err != nil {
							ba.logger.Errorf("[BlockAssembly] Failed to add tx to block assembly: %s", err)
						} else {
							n.Add(1)
						}
					}
				}()
			}

			go func() {
				clusterAdmin, _, err = util.ConnectToKafka(kafkaURL)
				if err != nil {
					log.Fatal("[BlockAssembly] unable to connect to kafka: ", err)
				}
				defer func() { _ = clusterAdmin.Close() }()

				topic := kafkaURL.Path[1:]
				var partitions int
				if partitions, err = strconv.Atoi(kafkaURL.Query().Get("partitions")); err != nil {
					log.Fatal("[BlockAssembly] unable to parse Kafka partitions: ", err)
				}

				var replicationFactor int
				if replicationFactor, err = strconv.Atoi(kafkaURL.Query().Get("replication")); err != nil {
					log.Fatal("[BlockAssembly] unable to parse Kafka replication factor: ", err)
				}

				_ = clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
					NumPartitions:     int32(partitions),
					ReplicationFactor: int16(replicationFactor),
				}, false)

				err = util.StartKafkaGroupListener(ctx, ba.logger, kafkaURL, "validators", workerCh)
				if err != nil {
					ba.logger.Errorf("[BlockAssembly] Kafka listener failed to start: %s", err)
				}
			}()
		}
	}

	address, ok := gocore.Config().Get("blockassembly_grpcAddress")
	if !ok {
		return errors.New("no blockassembly_grpcAddress setting found")
	}

	var err error
	ba.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
	})
	if err != nil {
		return fmt.Errorf("could not create GRPC server [%w]", err)
	}

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	blockassembly_api.RegisterBlockAssemblyAPIServer(ba.grpcServer, ba)

	// Register reflection service on gRPC server.
	reflection.Register(ba.grpcServer)

	ba.logger.Infof("[BlockAssembly] GRPC service listening on %s", address)

	go func() {
		<-ctx.Done()
		ba.grpcServer.GracefulStop()
	}()

	if err = ba.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (ba *BlockAssembly) Stop(ctx context.Context) error {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	ba.grpcServer.GracefulStop()

	return nil
}

func (ba *BlockAssembly) Health(_ context.Context, _ *emptypb.Empty) (*blockassembly_api.HealthResponse, error) {
	return &blockassembly_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (*blockassembly_api.AddTxResponse, error) {
	// Look up the new utxos for this txHash, add them to the utxostore, and add the tx to the subtree builder...
	txHash, err := chainhash.NewHash(req.Txid)
	if err != nil {
		return nil, err
	}

	if err = ba.blockAssembler.AddTx(ctx, txHash); err != nil {
		return nil, err
	}

	return &blockassembly_api.AddTxResponse{
		Ok: true,
	}, nil
}

func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, _ *emptypb.Empty) (*model.MiningCandidate, error) {
	miningCandidate, subtrees, err := ba.blockAssembler.GetMiningCandidate(ctx)
	if err != nil {
		return nil, err
	}

	id, _ := chainhash.NewHash(miningCandidate.Id)
	ba.jobStore.Set(*id, &subtreeprocessor.Job{
		ID:              id,
		Subtrees:        subtrees,
		MiningCandidate: miningCandidate,
	}, jobTTL) // create a new job with a TTL, will be cleaned up automatically

	return miningCandidate, nil
}

func (ba *BlockAssembly) SubmitMiningSolution(ctx context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error) {
	ba.logger.Infof("[BlockAssembly] SubmitMiningSolution: %x", req.Id)

	storeId, err := chainhash.NewHash(req.Id[:])
	if err != nil {
		return nil, err
	}

	jobItem := ba.jobStore.Get(*storeId)
	if jobItem == nil {
		return nil, fmt.Errorf("[BlockAssembly] job not found")
	}
	job := jobItem.Value()

	hashPrevBlock, err := chainhash.NewHash(job.MiningCandidate.PreviousHash)
	if err != nil {
		return nil, fmt.Errorf("[BlockAssembly] failed to convert hashPrevBlock: %w", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(req.CoinbaseTx)
	if err != nil {
		return nil, fmt.Errorf("[BlockAssembly] failed to convert coinbaseTx: %w", err)
	}
	coinbaseTxIDHash := coinbaseTx.TxIDChainHash()

	subtreesInJob := make([]*util.Subtree, len(job.Subtrees))
	subtreeHashes := make([]*chainhash.Hash, len(job.Subtrees))
	jobSubtreeHashes := make([]*chainhash.Hash, len(job.Subtrees))
	transactionCount := uint64(0)
	for i, subtree := range job.Subtrees {
		jobSubtreeHashes[i] = subtree.RootHash()

		subtreesInJob[i] = subtree
		if i == 0 {
			subtreesInJob[i].ReplaceRootNode(coinbaseTxIDHash, 0)
		}
		rootHash := subtree.RootHash()
		subtreeHashes[i], _ = chainhash.NewHash(rootHash[:])
		transactionCount += uint64(subtree.Length())
	}

	// Create a new subtree with the subtreeHashes of the subtrees
	topTree := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(subtreesInJob)))
	for _, hash := range subtreeHashes {
		err = topTree.AddNode(hash, 1)
		if err != nil {
			return nil, err
		}
	}

	var hashMerkleRoot *chainhash.Hash
	var coinbaseMerkleProof []*chainhash.Hash

	if len(subtreesInJob) == 0 {
		hashMerkleRoot = coinbaseTxIDHash
	} else {
		coinbaseMerkleProof, err = util.GetMerkleProofForCoinbase(subtreesInJob)
		if err != nil {
			return nil, fmt.Errorf("[BlockAssembly] error getting merkle proof for coinbase: %w", err)
		}

		cmp := make([]string, len(coinbaseMerkleProof))
		cmpB := make([][]byte, len(coinbaseMerkleProof))
		for idx, hash := range coinbaseMerkleProof {
			cmp[idx] = hash.String()
			cmpB[idx] = hash.CloneBytes()
		}

		calculatedMerkleRoot := topTree.RootHash()
		hashMerkleRoot, err = chainhash.NewHash(calculatedMerkleRoot[:])
		if err != nil {
			return nil, err
		}
	}

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        req.Version,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Timestamp:      req.Time,
			Bits:           model.NewNBitFromSlice(job.MiningCandidate.NBits),
			Nonce:          req.Nonce,
		},
		CoinbaseTx:       coinbaseTx,
		TransactionCount: transactionCount,
		Subtrees:         jobSubtreeHashes, // we need to store the hashes of the subtrees in the block, without the coinbase
	}

	ba.logger.Infof("[BlockAssembly] validating block: %s", block.Header.Hash())
	// check fully valid, including whether difficulty in header is low enough
	if ok, err := block.Valid(ctx, nil, nil, nil); !ok {
		ba.logger.Errorf("[BlockAssembly] invalid block: %s - %v - %v", utils.ReverseAndHexEncodeHash(*block.Header.Hash()), block.Header, err)
		return nil, fmt.Errorf("[BlockAssembly] invalid block: %v", err)
	}

	ba.logger.Infof("[BlockAssembly] add block to blockchain: %s", block.Header.Hash())
	// add block to the blockchain
	if err = ba.blockchainClient.AddBlock(ctx, block); err != nil {
		return nil, fmt.Errorf("failed to add block: %w", err)
	}

	// update the subtree TTLs
	for _, subtree := range block.Subtrees {
		err = ba.subtreeStore.SetTTL(ctx, subtree[:], 0)
		if err != nil {
			ba.logger.Errorf("failed to update subtree TTL: %w", err)
		}
	}

	// remove job, we have already mined a block with it
	ba.jobStore.Delete(*storeId)

	return &blockassembly_api.SubmitMiningSolutionResponse{
		Ok: true,
	}, nil
}
