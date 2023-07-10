package blockassembly

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/url"
	"strconv"
	"sync"
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
	txmeta_store "github.com/TAAL-GmbH/ubsv/stores/txmeta"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
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
			Help: "Duration of storing new utxos by BlockAssembler",
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
	logger utils.Logger

	utxoStore        utxostore.Interface
	txMetaClient     txmeta_store.Store
	subtreeProcessor *subtreeprocessor.SubtreeProcessor
	grpcServer       *grpc.Server
	blockchainClient blockchain.ClientI
	subtreeStore     blob.Store
	jobStoreMutex    sync.RWMutex
	jobStore         map[chainhash.Hash]*subtreeprocessor.Job
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockassembly_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, subtreeStore blob.Store) *BlockAssembly {
	utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no utxostore setting found")
	}

	s, err := utxo.NewStore(logger, utxostoreURL)
	if err != nil {
		panic(err)
	}

	txMetaStoreURL, err, found := gocore.Config().GetURL("txmeta_store")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no txmeta_store setting found")
	}

	// TODO abstract into a factory
	var txMetaStore txmeta_store.Store
	if txMetaStoreURL.Scheme == "memory" {
		// the memory store is reached through a grpc client
		txMetaStore, err = txmeta.NewClient(context.Background(), logger)
		if err != nil {
			panic(err)
		}
	} else {
		txMetaStore, err = store.New(logger, txMetaStoreURL)
		if err != nil {
			panic(err)
		}
	}

	blockchainClient, err := blockchain.NewClient()
	if err != nil {
		panic(err)
	}

	newSubtreeChan := make(chan *util.Subtree)

	ba := &BlockAssembly{
		logger:           logger,
		utxoStore:        s,
		txMetaClient:     txMetaStore,
		subtreeProcessor: subtreeprocessor.NewSubtreeProcessor(logger, newSubtreeChan),
		blockchainClient: blockchainClient,
		subtreeStore:     subtreeStore,
		jobStore:         make(map[chainhash.Hash]*subtreeprocessor.Job),
	}

	go func() {
		var subtreeBytes []byte
		for {
			subtree := <-newSubtreeChan
			// merkleRoot := stp.currentSubtree.ReplaceRootNode(*coinbaseHash)
			// assert.Equal(t, expectedMerkleRoot, utils.ReverseAndHexEncodeHash(merkleRoot))

			if subtreeBytes, err = subtree.Serialize(); err != nil {
				logger.Errorf("Failed to serialize subtree [%s]", err)
				continue
			}

			if err = ba.subtreeStore.Set(context.Background(),
				subtree.RootHash()[:],
				subtreeBytes,
				blob.WithTTL(120*time.Minute), // this sets the TTL for the subtree, it must be updated when a block is mined
			); err != nil {
				logger.Errorf("Failed to store subtree [%s]", err)
				continue
			}

			logger.Infof("Received new subtree notification for: %s (len %d)", subtree.RootHash().String(), subtree.Size())
		}
	}()

	return ba
}

// Start function
func (ba *BlockAssembly) Start() error {

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
						if _, err = ba.AddTx(context.Background(), &blockassembly_api.AddTxRequest{
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
				partitions, err := strconv.Atoi(kafkaURL.Query().Get("partitions"))
				if err != nil {
					log.Fatal("[BlockAssembly] unable to parse Kafka partitions: ", err)
				}
				replicationFactor, err := strconv.Atoi(kafkaURL.Query().Get("replication"))
				if err != nil {
					log.Fatal("[BlockAssembly] unable to parse Kafka replication factor: ", err)
				}

				_ = clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
					NumPartitions:     int32(partitions),
					ReplicationFactor: int16(replicationFactor),
				}, false)

				err = util.StartKafkaGroupListener(ba.logger, kafkaURL, "validators", workerCh)
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

	ba.logger.Infof("BlockAssembler GRPC service listening on %s", address)

	if err = ba.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (ba *BlockAssembly) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	ba.grpcServer.GracefulStop()
}

func (ba *BlockAssembly) Health(_ context.Context, _ *emptypb.Empty) (*blockassembly_api.HealthResponse, error) {
	return &blockassembly_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

// func (ba *BlockAssembly) NewChaintipAndHeight(ctx context.Context, req *blockassembly_api.NewChaintipAndHeightRequest) (*emptypb.Empty, error) {
// 	return &emptypb.Empty{}, nil
// }

func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (*blockassembly_api.AddTxResponse, error) {
	// Look up the new utxos for this txid, add them to the utxostore, and add the tx to the subtree builder...
	txid, err := chainhash.NewHash(req.Txid)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()

	txMetadata, err := ba.txMetaClient.Get(ctx, txid)
	if err != nil {
		return nil, err
	}

	prometheusTxMetaGetDuration.Observe(float64(time.Since(startTime).Microseconds()))

	startTime = time.Now()

	// Add all the utxo hashes to the utxostore
	for _, hash := range txMetadata.UtxoHashes {
		if resp, err := ba.utxoStore.Store(context.Background(), hash); err != nil {
			return nil, fmt.Errorf("error storing utxo (%v): %w", resp, err)
		}
	}

	prometheusUtxoStoreDuration.Observe(float64(time.Since(startTime).Microseconds()))

	startTime = time.Now()

	ba.subtreeProcessor.Add(*txid, txMetadata.Fee)

	prometheusSubtreeAddToChannelDuration.Observe(float64(time.Since(startTime).Microseconds()))

	prometheusBlockAssemblyAddTx.Inc()

	return &blockassembly_api.AddTxResponse{
		Ok: true,
	}, nil
}

func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, _ *emptypb.Empty) (*model.MiningCandidate, error) {

	bestBlockHeader, bestBlockHeight, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting best block header: %w", err)
	}

	// Get the list of completed containers for the current chaintip and height...
	subtrees := ba.subtreeProcessor.GetCompletedSubtreesForMiningCandidate()

	var coinbaseValue uint64
	for _, subtree := range subtrees {
		coinbaseValue += subtree.Fees
	}
	coinbaseValue += util.GetBlockSubsidyForHeight(bestBlockHeight + 1)

	// Get the hash of the last subtree in the list...
	id := &chainhash.Hash{}
	if len(subtrees) > 0 {
		height := int(math.Ceil(math.Log2(float64(len(subtrees)))))
		topTree := util.NewTree(height)
		for _, subtree := range subtrees {
			_ = topTree.AddNode(subtree.RootHash(), subtree.Fees)
		}
		id = topTree.RootHash()
	}

	// TODO this will need to be calculated but for now we will keep the same difficulty for all blocks
	// nBits := bestBlockHeader.Bits
	// TEMP for testing only - moved from blockchain sql store
	nBits := model.NewNBitFromString("2000ffff") // TEMP We want hashes with 2 leading zeros

	coinbaseMerkleProof, err := util.GetMerkleProofForCoinbase(subtrees)
	if err != nil {
		return nil, fmt.Errorf("error getting merkle proof for coinbase: %w", err)
	}

	var coinbaseMerkleProofBytes [][]byte
	for _, hash := range coinbaseMerkleProof {
		coinbaseMerkleProofBytes = append(coinbaseMerkleProofBytes, hash.CloneBytes())
	}

	miningCandidate := &model.MiningCandidate{
		Id:            id[:],
		PreviousHash:  bestBlockHeader.Hash().CloneBytes(),
		CoinbaseValue: coinbaseValue,
		Version:       1,
		NBits:         nBits.CloneBytes(),
		Height:        bestBlockHeight + 1,
		Time:          uint32(time.Now().Unix()),
		MerkleProof:   coinbaseMerkleProofBytes,
	}

	ba.jobStoreMutex.Lock()
	ba.jobStore[*id] = &subtreeprocessor.Job{
		ID:              id,
		Subtrees:        subtrees,
		MiningCandidate: miningCandidate,
	}
	ba.jobStoreMutex.Unlock()

	return miningCandidate, nil
}

func (ba *BlockAssembly) SubmitMiningSolution(ctx context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error) {
	// TODO Should this all happen in the subtreeProcessor? There could be timing issues between adding block and
	// resetting for the next mining job etc. - also we are continually adding new subtrees and transactions etc.

	storeId, err := chainhash.NewHash(req.Id[:])
	if err != nil {
		return nil, err
	}

	ba.jobStoreMutex.RLock()
	job, ok := ba.jobStore[*storeId]
	ba.jobStoreMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("job not found")
	}

	hashPrevBlock, err := chainhash.NewHash(job.MiningCandidate.PreviousHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(req.CoinbaseTx)
	if err != nil {
		return nil, fmt.Errorf("failed to convert coinbaseTx: %w", err)
	}
	coinbaseTxIDHash, err := chainhash.NewHashFromStr(coinbaseTx.TxID())
	if err != nil {
		return nil, fmt.Errorf("failed to convert coinbaseTxHash: %w", err)
	}

	//subtreesInJob := ba.subtreeProcessor.GetCompleteSubtreesForJob(job.ID[:])
	subtreesInJob := job.Subtrees
	//ba.logger.Debugf("SERVER replacing coinbase, current hash: %s", subtreesInJob[0].RootHash().String())
	subtreesInJob[0].ReplaceRootNode(coinbaseTxIDHash)
	//ba.logger.Debugf("SERVER replacing coinbase, new hash: %s", subtreesInJob[0].RootHash().String())

	subtreeHashes := make([]*chainhash.Hash, len(subtreesInJob))
	transactionCount := uint64(0)
	for i, subtree := range subtreesInJob {
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

	//merkleProofs, _ := topTree.BuildMerkleTreeStoreFromBytes()
	//ba.logger.Debugf("SERVER SUBTREE HASHES: %v", subtreeHashes)
	coinbaseMerkleProof, err := util.GetMerkleProofForCoinbase(subtreesInJob)
	if err != nil {
		return nil, fmt.Errorf("error getting merkle proof for coinbase: %w", err)
	}

	cmp := make([]string, len(coinbaseMerkleProof))
	cmpB := make([][]byte, len(coinbaseMerkleProof))
	for idx, hash := range coinbaseMerkleProof {
		cmp[idx] = hash.String()
		cmpB[idx] = hash.CloneBytes()
	}
	//fmt.Printf("SERVER merkle proof: %v", cmp)
	//bMerkleRoot := util.BuildMerkleRootFromCoinbase(bt.ReverseBytes(coinbaseTx.TxIDBytes()), cmpB)
	//ba.logger.Debugf("SERVER Merkle root from proofs: %s", utils.ReverseAndHexEncodeSlice(bMerkleRoot))

	//ba.logger.Debugf("SERVER Coinbase: %s", coinbaseTx.TxID())
	//ba.logger.Debugf("SERVER MERKLE PROOfS: %v", merkleProofs)

	calculatedMerkleRoot := topTree.RootHash()
	hashMerkleRoot, err := chainhash.NewHash(calculatedMerkleRoot[:])
	if err != nil {
		return nil, err
	}

	//ba.logger.Debugf("SERVER MERKLE ROOT: %s", hashMerkleRoot.String())

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
		Subtrees:         subtreeHashes,
	}

	// check fully valid, including whether difficulty in header is low enough
	if ok, err = block.Valid(); !ok {
		ba.logger.Errorf("Invalid block: %s - %v - %v", utils.ReverseAndHexEncodeHash(*block.Header.Hash()), block.Header, err)
		return nil, fmt.Errorf("invalid block: %v", err)
	}

	// reset the subtrees
	err = ba.subtreeProcessor.Reset(job)
	if err != nil {
		// TODO if this happens, why might actually start mining the next block with the same subtrees and transactions
		// this would be VERY bad
		return nil, fmt.Errorf("failed to reset subtree processor: %w", err)
	}

	// only add block to blockchain if the reset was successful - otherwise we risk mining the same transactions
	if err = ba.blockchainClient.AddBlock(ctx, block); err != nil {
		return nil, fmt.Errorf("failed to add block: %w", err)
	}

	// remove job, we have already mined a block with it
	// TODO do we need to remove the rest of the jobs as well? Our subtree processor is completely different now
	ba.jobStoreMutex.Lock()
	delete(ba.jobStore, *storeId)
	ba.jobStoreMutex.Unlock()

	return &blockassembly_api.SubmitMiningSolutionResponse{
		Ok: true,
	}, nil
}
