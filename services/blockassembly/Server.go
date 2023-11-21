package blockassembly

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

var (
	// blockAssemblyStat = gocore.NewStat("blockassembly")
	// addTxBatchGrpc = blockAssemblyStat.NewStat("AddTxBatch_grpc", true)

	// channelStats = blockAssemblyStat.NewStat("channels", false)
	jobTTL = 10 * time.Minute
)

// BlockAssembly type carries the logger within it
type BlockAssembly struct {
	blockassembly_api.UnimplementedBlockAssemblyAPIServer
	blockAssembler *BlockAssembler
	logger         utils.Logger

	blockchainClient      blockchain.ClientI
	txStore               blob.Store
	utxoStore             utxostore.Interface
	txMetaStore           txmeta_store.Store
	subtreeStore          blob.Store
	subtreeTTL            time.Duration
	AssetClient           WrapperInterface
	blockValidationClient WrapperInterface
	jobStore              *ttlcache.Cache[chainhash.Hash, *subtreeprocessor.Job] // has built in locking
	blockSubmissionChan   chan *blockassembly_api.SubmitMiningSolutionRequest
	blockAssemblyDisabled bool
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockassembly_grpcListenAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, txStore blob.Store, utxoStore utxostore.Interface, txMetaStore txmeta_store.Store, subtreeStore blob.Store,
	blockchainClient blockchain.ClientI, AssetClient, blockValidationClient WrapperInterface) *BlockAssembly {

	// initialize Prometheus metrics, singleton, will only happen once
	initPrometheusMetrics()

	subtreeTTLMinutes, _ := gocore.Config().GetInt("blockassembly_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	ba := &BlockAssembly{
		logger:                logger,
		blockchainClient:      blockchainClient,
		txStore:               txStore,
		utxoStore:             utxoStore,
		txMetaStore:           txMetaStore,
		subtreeStore:          subtreeStore,
		subtreeTTL:            subtreeTTL,
		AssetClient:           AssetClient,
		blockValidationClient: blockValidationClient,
		jobStore:              ttlcache.New[chainhash.Hash, *subtreeprocessor.Job](),
		blockSubmissionChan:   make(chan *blockassembly_api.SubmitMiningSolutionRequest),
		blockAssemblyDisabled: gocore.Config().GetBool("blockassembly_disabled", false),
	}

	go ba.jobStore.Start()

	return ba
}

func (ba *BlockAssembly) Init(ctx context.Context) (err error) {
	newSubtreeChan := make(chan *util.Subtree)

	// init the block assembler for this server
	ba.blockAssembler = NewBlockAssembler(ctx, ba.logger, ba.utxoStore, ba.subtreeStore, ba.blockchainClient, newSubtreeChan)

	// start the new subtree listener in the background
	go func() {
		var subtreeBytes []byte
		for {
			select {
			case <-ctx.Done():
				ba.logger.Infof("Stopping subtree listener")
				return
			case subtree := <-newSubtreeChan:
				// start1, stat1, _ := util.NewStatFromContext(ctx, "newSubtreeChan", channelStats)
				prometheusBlockAssemblerSubtreeCreated.Inc()

				ba.logger.Infof("[BlockAssembly:Init][%s] new subtree notification from assembly: len %d", subtree.RootHash().String(), subtree.Length())

				if subtreeBytes, err = subtree.Serialize(); err != nil {
					ba.logger.Errorf("[BlockAssembly:Init][%s] failed to serialize subtree: %s", subtree.RootHash().String(), err)
					continue
				}

				// TODO context was being canceled, is this hiding a different problem?
				// start2, stat2, ctx2 := util.NewStatFromContext(ctx, "subtreeStore.Set", stat1)
				if err = ba.subtreeStore.Set(ctx,
					subtree.RootHash()[:],
					subtreeBytes,
					options.WithTTL(ba.subtreeTTL), // this sets the TTL for the subtree, it must be updated when a block is mined
				); err != nil {
					ba.logger.Errorf("[BlockAssembly:Init][%s] failed to store subtree: %s", subtree.RootHash().String(), err)
					continue
				}
				// stat2.AddTime(start2)

				// TODO #145
				// the repository in the blob server sometimes cannot find subtrees that were just stored
				// this is the dumbest way we can think of to fix it, at least temporarily
				time.Sleep(20 * time.Millisecond)

				// start3, stat3, ctx3 := util.NewStatFromContext(ctx, "SendNotification", stat1)
				if err = ba.blockchainClient.SendNotification(ctx, &model.Notification{
					Type: model.NotificationType_Subtree,
					Hash: subtree.RootHash(),
				}); err != nil {
					ba.logger.Errorf("[BlockAssembly:Init][%s] failed to send subtree notification: %s", subtree.RootHash().String(), err)
				}
				// stat3.AddTime(start3)

				// stat1.AddTime(start1)
			}
		}
	}()

	// start the block submission listener in the background
	go func() {
		for {
			select {
			case <-ctx.Done():
				ba.logger.Infof("Stopping block submission listener")
				return
			case blockSubmission := <-ba.blockSubmissionChan:
				// _, _, c := util.NewStatFromContext(ctx, "blockSubmissionChan", channelStats, false)
				if _, err = ba.submitMiningSolution(ctx, blockSubmission); err != nil {
					ba.logger.Warnf("Failed to submit block [%s]", err)
				}
				prometheusBlockAssemblySubmitMiningSolutionCh.Set(float64(len(ba.blockSubmissionChan)))
			}
		}
	}()

	return nil
}

// Start function
func (ba *BlockAssembly) Start(ctx context.Context) (err error) {

	remoteTTLStores := gocore.Config().GetBool("blockassembly_remoteTTLStores", false)
	if remoteTTLStores {
		ba.subtreeStore, err = NewRemoteTTLWrapper(ba.subtreeStore, ba.AssetClient, ba.blockValidationClient)
		if err != nil {
			return fmt.Errorf("failed to create remote TTL wrapper: %s", err)
		}
	}

	if err = ba.blockAssembler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start block assembler [%w]", err)
	}

	kafkaBrokersURL, err, ok := gocore.Config().GetURL("blockassembly_kafkaBrokers")
	if err == nil && ok {
		ba.startKafkaListener(ctx, kafkaBrokersURL)
	}

	// Experimental DRPC server - to test throughput at scale
	drpcAddress, ok := gocore.Config().Get("blockassembly_drpcListenAddress")
	if ok {
		err = ba.drpcServer(ctx, drpcAddress)
		if err != nil {
			ba.logger.Errorf("failed to start DRPC server: %v", err)
		}
	}

	// Experimental fRPC server - to test throughput at scale
	frpcAddress, ok := gocore.Config().Get("blockassembly_frpcListenAddress")
	if ok {
		err = ba.frpcServer(ctx, frpcAddress)
		if err != nil {
			ba.logger.Errorf("failed to start fRPC server: %v", err)
		}
	}

	// this will block
	if err = util.StartGRPCServer(ctx, ba.logger, "blockassembly", func(server *grpc.Server) {
		blockassembly_api.RegisterBlockAssemblyAPIServer(server, ba)
	}); err != nil {
		return err
	}

	return nil
}

func (ba *BlockAssembly) drpcServer(ctx context.Context, drpcAddress string) error {
	ba.logger.Infof("Starting DRPC server on %s", drpcAddress)
	m := drpcmux.New()
	// register the proto-specific methods on the mux
	err := blockassembly_api.DRPCRegisterBlockAssemblyAPI(m, ba)
	if err != nil {
		return fmt.Errorf("failed to register DRPC service: %v", err)
	}
	// create the drpc server
	s := drpcserver.New(m)

	// listen on a tcp socket
	var lis net.Listener
	lis, err = net.Listen("tcp", drpcAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on drpc server: %v", err)
	}

	// run the server
	// N.B.: if you want TLS, you need to wrap the net.Listener with
	// TLS before passing to Serve here.
	go func() {
		err = s.Serve(ctx, lis)
		if err != nil {
			ba.logger.Errorf("failed to serve drpc: %v", err)
		}
	}()

	return nil
}

func (ba *BlockAssembly) frpcServer(ctx context.Context, frpcAddress string) error {
	ba.logger.Infof("Starting fRPC server on %s", frpcAddress)

	frpcBa := &fRPC_BlockAssembly{
		ba: ba,
	}

	s, err := blockassembly_api.NewServer(frpcBa, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create fRPC server: %v", err)
	}

	concurrency, ok := gocore.Config().GetInt("blockassembly_frpcConcurrency")
	if ok {
		ba.logger.Infof("Setting fRPC server concurrency to %d", concurrency)
		s.SetConcurrency(uint64(concurrency))
	}

	// run the server
	go func() {
		err = s.Start(frpcAddress)
		if err != nil {
			ba.logger.Errorf("failed to serve frpc: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		err = s.Shutdown()
		if err != nil {
			ba.logger.Errorf("failed to shutdown frpc server: %v", err)
		}
	}()

	return nil
}

func (ba *BlockAssembly) startKafkaListener(ctx context.Context, kafkaBrokersURL *url.URL) {
	ba.logger.Infof("[BlockAssembly] Starting Kafka on address: %s", kafkaBrokersURL.String())

	workers, _ := gocore.Config().GetInt("blockassembly_kafkaWorkers", 100)
	ba.logger.Infof("[BlockAssembly] Kafka consumer starting with %d workers", workers)

	// create the workers to process all messages
	n := atomic.Uint64{}
	workerCh := make(chan []byte)
	for i := 0; i < workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					ba.logger.Infof("[BlockAssembly] Stopping Kafka worker")
					return
				case txIDBytes := <-workerCh:
					if _, err := ba.AddTx(ctx, &blockassembly_api.AddTxRequest{
						Txid: txIDBytes,
					}); err != nil {
						ba.logger.Errorf("[BlockAssembly] Failed to add tx to block assembly: %s", err)
					} else {
						n.Add(1)
					}
				}
			}
		}()
	}

	go func() {
		clusterAdmin, _, err := util.ConnectToKafka(kafkaBrokersURL)
		if err != nil {
			ba.logger.Fatalf("[BlockAssembly] unable to connect to kafka: ", err)
		}
		defer func() { _ = clusterAdmin.Close() }()

		topic := kafkaBrokersURL.Path[1:]
		var partitions int
		if partitions, err = strconv.Atoi(kafkaBrokersURL.Query().Get("partitions")); err != nil {
			ba.logger.Fatalf("[BlockAssembly] unable to parse Kafka partitions: ", err)
		}

		var replicationFactor int
		if replicationFactor, err = strconv.Atoi(kafkaBrokersURL.Query().Get("replication")); err != nil {
			ba.logger.Fatalf("[BlockAssembly] unable to parse Kafka replication factor: ", err)
		}

		_ = clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     int32(partitions),
			ReplicationFactor: int16(replicationFactor),
		}, false)

		if err = util.StartKafkaGroupListener(ctx, ba.logger, kafkaBrokersURL, "validators", workerCh); err != nil {
			ba.logger.Errorf("[BlockAssembly] Kafka listener failed to start: %s", err)
		}
	}()
}

func (ba *BlockAssembly) Stop(_ context.Context) error {
	ba.jobStore.Stop()
	return nil
}

func (ba *BlockAssembly) Health(_ context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.HealthResponse, error) {
	// start := gocore.CurrentTime()
	// defer func() {
	// 	blockAssemblyStat.NewStat("Health_grpc", true).AddTime(start)
	// }()

	prometheusBlockAssemblyHealth.Inc()

	return &blockassembly_api.HealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (resp *blockassembly_api.AddTxResponse, err error) {
	// startTime, stat, _ := util.NewStatFromContext(ctx, "AddTx_grpc", blockAssemblyStat)
	startTime := time.Now()

	//traceSpan := tracing.Start(ctx, "BlockAssembly:AddTx")

	prometheusBlockAssemblyAddTx.Inc()
	defer func() {
		//traceSpan.Finish()
		// stat.AddTime(startTime)
		prometheusBlockAssemblerTransactions.Set(float64(ba.blockAssembler.TxCount()))
		prometheusBlockAssemblerQueuedTransactions.Set(float64(ba.blockAssembler.QueueLength()))
		prometheusBlockAssemblerSubtrees.Set(float64(ba.blockAssembler.SubtreeCount()))
		prometheusBlockAssemblyAddTxDuration.Observe(util.TimeSince(startTime))
	}()

	if len(req.Txid) != 32 {
		return nil, fmt.Errorf("invalid txid length: %d for %s", len(req.Txid), utils.ReverseAndHexEncodeSlice(req.Txid))
	}

	if !ba.blockAssemblyDisabled {
		if err = ba.blockAssembler.AddTx(util.SubtreeNode{
			Hash:        chainhash.Hash(req.Txid),
			Fee:         req.Fee,
			SizeInBytes: req.Size,
		}); err != nil {
			return nil, err
		}
	}

	return &blockassembly_api.AddTxResponse{
		Ok: true,
	}, nil
}

func (ba *BlockAssembly) AddTxBatch(ctx context.Context, batch *blockassembly_api.AddTxBatchRequest) (*blockassembly_api.AddTxBatchResponse, error) {
	// start := gocore.CurrentTime()
	// defer func() {
	// 	addTxBatchGrpc.AddTime(start)
	// }()

	requests := batch.GetTxRequests()
	if len(requests) == 0 {
		return nil, fmt.Errorf("no tx requests in batch")
	}

	var batchError error = nil
	var err error
	txIdErrors := make([][]byte, 0, len(requests))
	for _, req := range requests {
		// create the subtree node
		if !ba.blockAssemblyDisabled {
			if err = ba.blockAssembler.AddTx(util.SubtreeNode{
				Hash:        chainhash.Hash(req.Txid),
				Fee:         req.Fee,
				SizeInBytes: req.Size,
			}); err != nil {
				batchError = err
				txIdErrors = append(txIdErrors, req.Txid)
			}
		}
	}
	return &blockassembly_api.AddTxBatchResponse{
		Ok:         true,
		TxIdErrors: txIdErrors,
	}, batchError
}

func (ba *BlockAssembly) GetTxMeta(ctx context.Context, txHash *chainhash.Hash) (*txmeta_store.Data, error) {
	startMetaTime := time.Now()
	txMetaSpan, txMetaSpanCtx := opentracing.StartSpanFromContext(ctx, "BlockAssembly:AddTx:txMeta")
	defer func() {
		txMetaSpan.Finish()
		// blockAssemblyStat.NewStat("GetTxMeta_grpc", true).AddTime(startMetaTime)
		prometheusBlockAssemblerTxMetaGetDuration.Observe(time.Since(startMetaTime).Seconds())
	}()

	txMetadata, err := ba.txMetaStore.Get(txMetaSpanCtx, txHash)
	if err != nil {
		return nil, err
	}

	currentChainMap := ba.blockAssembler.GetCurrentChainMap()

	// looking this up here and adding to the subtree processor, might create a situation where a transaction
	// that was in a block from a competing miner, is added to the subtree processor when it shouldn't
	if len(txMetadata.BlockHashes) > 0 {
		for _, hash := range txMetadata.BlockHashes {
			if _, ok := currentChainMap[*hash]; ok {
				// the tx is already in a block on our chain, nothing to do
				return nil, fmt.Errorf("tx already in a block on the active chain: %s", hash)
			}
		}
	}

	return txMetadata, nil
}

func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*model.MiningCandidate, error) {
	startTime := time.Now()
	prometheusBlockAssemblyGetMiningCandidate.Inc()
	defer func() {
		// blockAssemblyStat.NewStat("GetMiningCandidate_grpc", true).AddTime(startTime)
		prometheusBlockAssemblyGetMiningCandidateDuration.Observe(time.Since(startTime).Seconds())
	}()

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

func (ba *BlockAssembly) SubmitMiningSolution(_ context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error) {
	// start := gocore.CurrentTime()
	// defer func() {
	// 	blockAssemblyStat.NewStat("SubmitMiningSolution_grpc", true).AddTime(start)
	// }()

	// we don't have the processing to handle multiple huge blocks at the same time, so we limit it to 1
	// at a time, this is a temporary solution for now
	ba.blockSubmissionChan <- req
	prometheusBlockAssemblySubmitMiningSolutionCh.Set(float64(len(ba.blockSubmissionChan)))

	return &blockassembly_api.SubmitMiningSolutionResponse{
		Ok: true,
	}, nil
}

func (ba *BlockAssembly) submitMiningSolution(cntxt context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error) {
	// start, stat, ctx := util.NewStatFromContext(cntxt, "submitMiningSolution", blockAssemblyStat)
	start := time.Now()
	defer func() {
		// stat.AddTime(start)
		prometheusBlockAssemblySubmitMiningSolutionDuration.Observe(util.TimeSince(start))
		prometheusBlockAssemblySubmitMiningSolutionDuration.Observe(time.Since(start).Seconds())
	}()

	jobID := utils.ReverseAndHexEncodeSlice(req.Id)

	prometheusBlockAssemblySubmitMiningSolution.Inc()
	ba.logger.Infof("[BlockAssembly] SubmitMiningSolution: %s", jobID)

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

	var sizeInBytes uint64

	subtreesInJob := make([]*util.Subtree, len(job.Subtrees))
	subtreeHashes := make([]chainhash.Hash, len(job.Subtrees))
	jobSubtreeHashes := make([]*chainhash.Hash, len(job.Subtrees))
	transactionCount := uint64(0)
	if len(job.Subtrees) > 0 {
		ba.logger.Infof("[BlockAssembly] submit job %s has subtrees: %d", jobID, len(job.Subtrees))
		for i, subtree := range job.Subtrees {
			// the job subtree hash needs to be stored for the block, before the coinbase is replaced in the first
			// subtree, which changes the id of the subtree
			jobSubtreeHashes[i] = subtree.RootHash()

			if i == 0 {
				subtreesInJob[i] = subtree.Duplicate()
				subtreesInJob[i].ReplaceRootNode(coinbaseTxIDHash, 0, uint64(coinbaseTx.Size()))
			} else {
				subtreesInJob[i] = subtree
			}

			rootHash := subtreesInJob[i].RootHash()
			subtreeHashes[i] = chainhash.Hash(rootHash[:])

			transactionCount += uint64(subtree.Length())
			sizeInBytes += subtree.SizeInBytes
		}
	} else {
		transactionCount = 1 // Coinbase
		sizeInBytes = uint64(coinbaseTx.Size())
	}

	// Create a new subtree with the subtreeHashes of the subtrees
	topTree := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(subtreesInJob)))
	for _, hash := range subtreeHashes {
		err = topTree.AddNode(hash, 1, 0)
		if err != nil {
			return nil, err
		}
	}

	var hashMerkleRoot *chainhash.Hash
	var coinbaseMerkleProof []*chainhash.Hash

	if len(subtreesInJob) == 0 {
		hashMerkleRoot = coinbaseTxIDHash
	} else {
		ba.logger.Infof("[BlockAssembly] calculating merkle proof for job %s", jobID)
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
		SizeInBytes:      sizeInBytes + 80 + util.VarintSize(transactionCount), // 80 byte header and bytes for txcount, // TODO calculate varint of transaction count
		Subtrees:         jobSubtreeHashes,                                     // we need to store the hashes of the subtrees in the block, without the coinbase
		SubtreeSlices:    job.Subtrees,
	}

	ba.logger.Infof("[BlockAssembly] validating block: %s", block.Header.Hash())
	// check fully valid, including whether difficulty in header is low enough
	if ok, err := block.Valid(cntxt, nil, nil, nil); !ok {
		ba.logger.Errorf("[BlockAssembly] invalid block: %s - %v - %v", utils.ReverseAndHexEncodeHash(*block.Header.Hash()), block.Header, err)
		return nil, fmt.Errorf("[BlockAssembly] invalid block: %v", err)
	}

	// TODO context was being canceled, is this hiding a different problem?
	err = ba.txStore.Set(context.Background(), block.CoinbaseTx.TxIDChainHash().CloneBytes(), block.CoinbaseTx.ExtendedBytes())
	if err != nil {
		ba.logger.Errorf("[BlockAssembly] error storing coinbase tx in tx store: %v", err)
	}

	_, err = ba.txMetaStore.Create(cntxt, block.CoinbaseTx)
	if err != nil {
		ba.logger.Errorf("[BlockAssembly] error storing coinbase tx in tx meta store: %v", err)
	}

	ba.logger.Infof("[BlockAssembly] add block to blockchain: %s", block.Header.Hash())
	// add block to the blockchain
	if err = ba.blockchainClient.AddBlock(cntxt, block, false); err != nil {
		return nil, fmt.Errorf("failed to add block: %w", err)
	}

	// decouple the tracing context to not cancel the context when the subtree TTL is being saved in the background
	callerSpan := opentracing.SpanFromContext(cntxt)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	go func() {
		// TODO what do we do if this fails, the subtrees TTL and tx meta status still needs to be updated
		g, gCtx := errgroup.WithContext(setCtx)

		g.Go(func() error {
			ba.logger.Infof("[BlockAssembly] remove subtrees TTL: %s", block.Header.Hash())
			if err = ba.removeSubtreesTTL(gCtx, block); err != nil {
				ba.logger.Errorf("failed to remove subtrees TTL: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			// add the transactions in this block to the txMeta block hashes
			ba.logger.Infof("[BlockAssembly] update tx mined status: %s", block.Header.Hash())
			if err = model.UpdateTxMinedStatus(gCtx, ba.logger, ba.txMetaStore, subtreesInJob, block.Header); err != nil {
				ba.logger.Errorf("[BlockAssembly] error updating tx mined status: %w", err)
			}
			return nil
		})

		if err = g.Wait(); err != nil {
			ba.logger.Errorf("[BlockAssembly] error updating status: %w", err)
		}
	}()

	// remove jobs, we have already mined a block
	// if we don't do this, all the subtrees will never be removed from memory
	ba.jobStore.DeleteAll()

	return &blockassembly_api.SubmitMiningSolutionResponse{
		Ok: true,
	}, nil
}

func (ba *BlockAssembly) removeSubtreesTTL(ctx context.Context, block *model.Block) (err error) {
	// start, stat, ctx := util.NewStatFromContext(cntxt, "removeSubtreesTTL", blockAssemblyStat)
	start := time.Now()
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockAssembly:removeSubtreesTTL")
	defer func() {
		span.Finish()
		// stat.AddTime(start)
		prometheusBlockAssemblyUpdateSubtreesTTL.Observe(util.TimeSince(start))
	}()

	// decouple the tracing context to not cancel the context when the subtree TTL is being saved in the background
	callerSpan := opentracing.SpanFromContext(spanCtx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	// update the subtree TTLs
	for _, subtreeHash := range block.Subtrees {
		go func(subtreeHashBytes []byte) {
			// TODO this would be better as a batch operation
			err = ba.subtreeStore.SetTTL(setCtx, subtreeHashBytes, 0)
			if err != nil {
				ba.logger.Warnf("failed to update subtree TTL: %v", err)
			}
		}(subtreeHash[:])
	}

	return nil
}
