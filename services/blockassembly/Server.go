package blockassembly

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"time"

	"go.uber.org/atomic"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/file"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var (
	blockAssemblyStat = gocore.NewStat("blockassembly")
	// addTxBatchGrpc = blockAssemblyStat.NewStat("AddTxBatch_grpc", true)

	// channelStats = blockAssemblyStat.NewStat("channels", false)
	jobTTL = 10 * time.Minute
)

type BlockSubmissionRequest struct {
	*blockassembly_api.SubmitMiningSolutionRequest
	responseChan chan bool
}

// BlockAssembly type carries the logger within it
type BlockAssembly struct {
	blockassembly_api.UnimplementedBlockAssemblyAPIServer
	blockAssembler *BlockAssembler
	logger         ulogger.Logger

	blockchainClient          blockchain.ClientI
	txStore                   blob.Store
	utxoStore                 utxostore.Interface
	txMetaStore               txmeta_store.Store
	subtreeStore              blob.Store
	subtreeTTL                time.Duration
	assetClient               WrapperInterface
	blockValidationClient     WrapperInterface
	jobStore                  *ttlcache.Cache[chainhash.Hash, *subtreeprocessor.Job] // has built in locking
	blockSubmissionChan       chan *BlockSubmissionRequest
	blockAssemblyDisabled     bool
	blockAssemblyCreatesUTXOs bool
	localSetMined             bool
}

type subtreeRetrySend struct {
	subtreeHash  chainhash.Hash
	subtreeBytes []byte
	retries      int
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockassembly_grpcListenAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, txStore blob.Store, utxoStore utxostore.Interface, txMetaStore txmeta_store.Store, subtreeStore blob.Store,
	blockchainClient blockchain.ClientI, AssetClient, blockValidationClient WrapperInterface) *BlockAssembly {

	// initialize Prometheus metrics, singleton, will only happen once
	initPrometheusMetrics()

	subtreeTTLMinutes, _ := gocore.Config().GetInt("blockassembly_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	ba := &BlockAssembly{
		logger:                    logger,
		blockchainClient:          blockchainClient,
		txStore:                   txStore,
		utxoStore:                 utxoStore,
		txMetaStore:               txMetaStore,
		subtreeStore:              subtreeStore,
		subtreeTTL:                subtreeTTL,
		assetClient:               AssetClient,
		blockValidationClient:     blockValidationClient,
		jobStore:                  ttlcache.New[chainhash.Hash, *subtreeprocessor.Job](),
		blockSubmissionChan:       make(chan *BlockSubmissionRequest),
		blockAssemblyDisabled:     gocore.Config().GetBool("blockassembly_disabled", false),
		blockAssemblyCreatesUTXOs: gocore.Config().GetBool("blockassembly_creates_utxos", false),
		localSetMined:             gocore.Config().GetBool("blockvalidation_localSetMined", false),
	}

	go ba.jobStore.Start()

	return ba
}

func (ba *BlockAssembly) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (ba *BlockAssembly) Init(ctx context.Context) (err error) {
	// this is passed into the block assembler and subtree processor where new subtrees are created
	newSubtreeChanBuffer, _ := gocore.Config().GetInt("blockassembly_newSubtreeChanBuffer", 1_000)
	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, newSubtreeChanBuffer)

	// retry channel for subtrees that failed to be stored
	subtreeRetryChanBuffer, _ := gocore.Config().GetInt("blockassembly_subtreeRetryChanBuffer", 1_000)
	subtreeRetryChan := make(chan *subtreeRetrySend, subtreeRetryChanBuffer)

	remoteTTLStores := gocore.Config().GetBool("blockassembly_remoteTTLStores", false)
	if remoteTTLStores {
		ba.subtreeStore, err = NewRemoteTTLWrapper(ba.logger, ba.subtreeStore, ba.assetClient, ba.blockValidationClient)
		if err != nil {
			return fmt.Errorf("failed to create remote TTL wrapper: %s", err)
		}
	}

	auxiliarySubtreeStoreDir, ok := gocore.Config().Get("blockassembly_auxiliarySubtreeStore", "")
	if ok && auxiliarySubtreeStoreDir != "" {
		auxiliarySubtreeStore, err := file.New(ba.logger, auxiliarySubtreeStoreDir)
		if err != nil {
			return fmt.Errorf("failed to init auxiliary subtree store: %s", err)
		}

		// wrap the subtree store with the auxiliary subtree store
		ba.subtreeStore, err = NewAuxiliaryStore(ba.logger, ba.subtreeStore, auxiliarySubtreeStore)
		if err != nil {
			return fmt.Errorf("failed to create auxiliary subtree store: %s", err)
		}
	}

	// init the block assembler for this server
	ba.blockAssembler = NewBlockAssembler(ctx, ba.logger, ba.utxoStore, ba.subtreeStore, ba.blockchainClient, newSubtreeChan)

	// start the new subtree retry processor in the background
	go func() {
		for {
			select {
			case <-ctx.Done():
				ba.logger.Infof("Stopping subtree retry processor")
				return
			case subtreeRetry := <-subtreeRetryChan:
				if err = ba.subtreeStore.Set(ctx,
					subtreeRetry.subtreeHash[:],
					subtreeRetry.subtreeBytes,
					options.WithTTL(ba.subtreeTTL), // this sets the TTL for the subtree, it must be updated when a block is mined
				); err != nil {
					ba.logger.Errorf("[BlockAssembly:Init][%s] failed to retry store subtree: %s", subtreeRetry.subtreeHash.String(), err)

					if subtreeRetry.retries > 10 {
						ba.logger.Errorf("[BlockAssembly:Init][%s] failed to retry store subtree, retries exhausted", subtreeRetry.subtreeHash.String())
						continue
					}

					subtreeRetry.retries++
					go func() {
						// backoff and wait before re-adding to retry queue
						backoff := time.Duration(math.Pow(2, float64(subtreeRetry.retries))) * time.Second
						time.Sleep(backoff)

						// re-add the subtree to the retry queue
						subtreeRetryChan <- subtreeRetry
					}()

					continue
				}

				// TODO #145
				// the repository in the blob server sometimes cannot find subtrees that were just stored
				// this is the dumbest way we can think of to fix it, at least temporarily
				time.Sleep(20 * time.Millisecond)

				if err = ba.blockchainClient.SendNotification(ctx, &model.Notification{
					Type: model.NotificationType_Subtree,
					Hash: &subtreeRetry.subtreeHash,
				}); err != nil {
					ba.logger.Errorf("[BlockAssembly:Init][%s] failed to send subtree notification: %s", subtreeRetry.subtreeHash.String(), err)
				}
			}
		}
	}()

	// start the new subtree listener in the background
	go func() {
		for {
			select {
			case <-ctx.Done():
				ba.logger.Infof("Stopping subtree listener")
				return

			case newSubtreeRequest := <-newSubtreeChan:

				err = ba.storeSubtree(ctx, newSubtreeRequest.Subtree, subtreeRetryChan)
				if err != nil {
					ba.logger.Errorf(err.Error())
				}
				if newSubtreeRequest.ErrChan != nil {
					newSubtreeRequest.ErrChan <- err
				}
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
				ok := true
				if _, err := ba.submitMiningSolution(ctx, blockSubmission); err != nil {
					ba.logger.Warnf("Failed to submit block [%s]", err)
					ok = false
				}
				if blockSubmission.responseChan != nil {
					blockSubmission.responseChan <- ok
				}
				prometheusBlockAssemblySubmitMiningSolutionCh.Set(float64(len(ba.blockSubmissionChan)))
			}
		}
	}()

	return nil
}

func (ba *BlockAssembly) storeSubtree(ctx context.Context, subtree *util.Subtree, subtreeRetryChan chan *subtreeRetrySend) (err error) {
	// start1, stat1, _ := util.NewStatFromContext(ctx, "newSubtreeChan", channelStats)

	// check whether this subtree already exists in the store, which would mean it has already been announced
	if ok, _ := ba.subtreeStore.Exists(ctx, subtree.RootHash()[:]); ok {

		// subtree already exists, nothing to do
		ba.logger.Debugf("[BlockAssembly:Init][%s] subtree already exists", subtree.RootHash().String())
		return
	}

	prometheusBlockAssemblerSubtreeCreated.Inc()
	ba.logger.Infof("[BlockAssembly:Init][%s] new subtree notification from assembly: len %d", subtree.RootHash().String(), subtree.Length())

	var subtreeBytes []byte
	if subtreeBytes, err = subtree.Serialize(); err != nil {
		return fmt.Errorf("[BlockAssembly:Init][%s] failed to serialize subtree: %s", subtree.RootHash().String(), err)

	}

	if err = ba.subtreeStore.Set(ctx,
		subtree.RootHash()[:],
		subtreeBytes,
		options.WithTTL(ba.subtreeTTL), // this sets the TTL for the subtree, it must be updated when a block is mined
	); err != nil {
		ba.logger.Errorf("[BlockAssembly:Init][%s] failed to store subtree: %s", subtree.RootHash().String(), err)

		// add to retry saving the subtree
		subtreeRetryChan <- &subtreeRetrySend{
			subtreeHash:  *subtree.RootHash(),
			subtreeBytes: subtreeBytes,
			retries:      0,
		}

		return nil
	}

	// TODO #145
	// the repository in the blob server sometimes cannot find subtrees that were just stored
	// this is the dumbest way we can think of to fix it, at least temporarily
	time.Sleep(20 * time.Millisecond)

	if err = ba.blockchainClient.SendNotification(ctx, &model.Notification{
		Type: model.NotificationType_Subtree,
		Hash: subtree.RootHash(),
	}); err != nil {
		return fmt.Errorf("[BlockAssembly:Init][%s] failed to send subtree notification: %s", subtree.RootHash().String(), err)
	}
	return nil
}

// Start function
func (ba *BlockAssembly) Start(ctx context.Context) (err error) {

	if err = ba.blockAssembler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start block assembler [%w]", err)
	}

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_txsConfig")
	if err == nil && ok {
		go ba.startKafkaListener(ctx, kafkaURL)
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
		err := s.Start(frpcAddress)
		if err != nil {
			ba.logger.Errorf("failed to serve frpc: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		err := s.Shutdown()
		if err != nil {
			ba.logger.Errorf("failed to shutdown frpc server: %v", err)
		}
	}()

	return nil
}

func (ba *BlockAssembly) startKafkaListener(ctx context.Context, kafkaURL *url.URL) {
	workers, _ := gocore.Config().GetInt("blockassembly_kafkaWorkers", 100)
	if workers < 1 {
		// no workers, nothing to do
		return
	}

	consumerRatio := util.GetQueryParamInt(kafkaURL, "consumer_ratio", 8)
	if consumerRatio < 1 {
		consumerRatio = 1
	}

	partitions := util.GetQueryParamInt(kafkaURL, "partitions", 1)

	consumerCount := partitions / consumerRatio
	if consumerCount < 0 {
		consumerCount = 1
	}

	ba.logger.Infof("[BlockAssembly] starting Kafka on address: %s, with %d consumers and %d workers\n", kafkaURL.String(), consumerCount, workers)

	// updates the stats every 5 seconds
	go func() {
		for {
			time.Sleep(5 * time.Second)
			prometheusBlockAssemblerTransactions.Set(float64(ba.blockAssembler.TxCount()))
			prometheusBlockAssemblerQueuedTransactions.Set(float64(ba.blockAssembler.QueueLength()))
			prometheusBlockAssemblerSubtrees.Set(float64(ba.blockAssembler.SubtreeCount()))
		}
	}()

	if err := util.StartKafkaGroupListener(ctx, ba.logger, kafkaURL, "blockassembly", nil, consumerCount, func(msg util.KafkaMessage) {
		startTime := time.Now()

		data, err := NewFromBytes(msg.Message.Value)
		if err != nil {
			ba.logger.Errorf("[BlockAssembly] Failed to decode kafka message: %s", err)
			return
		}

		utxoHashesBytes := make([][]byte, len(data.UtxoHashes))
		for i, hash := range data.UtxoHashes {
			utxoHashesBytes[i] = hash.CloneBytes()
		}

		if _, err = ba.AddTx(ctx, &blockassembly_api.AddTxRequest{
			Txid:     data.TxIDChainHash.CloneBytes(),
			Fee:      data.Fee,
			Size:     data.Size,
			Locktime: data.LockTime,
			Utxos:    utxoHashesBytes,
		}); err != nil {
			ba.logger.Errorf("[BlockAssembly] failed to add tx to block assembly: %s", err)
		}

		prometheusBlockAssemblerSetFromKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}); err != nil {
		ba.logger.Errorf("[BlockAssembly] failed to start Kafka listener: %s", err)
	}
}

func (ba *BlockAssembly) Stop(_ context.Context) error {
	ba.jobStore.Stop()
	return nil
}

func (ba *BlockAssembly) HealthGRPC(_ context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.HealthResponse, error) {
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

var txsProcessed = atomic.Uint64{}

func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (resp *blockassembly_api.AddTxResponse, err error) {
	startTime := time.Now()
	defer func() {
		if txsProcessed.Load()%1000 == 0 {
			// we should NOT be setting this on every call, it's a waste of resources
			prometheusBlockAssemblerTransactions.Set(float64(ba.blockAssembler.TxCount()))
			prometheusBlockAssemblerQueuedTransactions.Set(float64(ba.blockAssembler.QueueLength()))
			prometheusBlockAssemblerSubtrees.Set(float64(ba.blockAssembler.SubtreeCount()))
		}

		prometheusBlockAssemblyAddTxDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		txsProcessed.Inc()
	}()

	if len(req.Txid) != 32 {
		return nil, fmt.Errorf("invalid txid length: %d for %s", len(req.Txid), utils.ReverseAndHexEncodeSlice(req.Txid))
	}

	if !ba.blockAssemblyDisabled {
		if ba.blockAssemblyCreatesUTXOs {
			if err = ba.storeUtxos(ctx, req); err != nil {
				return nil, err
			}
		}

		ba.blockAssembler.AddTx(util.SubtreeNode{
			Hash:        chainhash.Hash(req.Txid),
			Fee:         req.Fee,
			SizeInBytes: req.Size,
		})
	}

	return &blockassembly_api.AddTxResponse{
		Ok: true,
	}, nil
}

func (ba *BlockAssembly) RemoveTx(_ context.Context, req *blockassembly_api.RemoveTxRequest) (*blockassembly_api.EmptyMessage, error) {
	startTime := time.Now()
	prometheusBlockAssemblyRemoveTx.Inc()
	defer func() {
		prometheusBlockAssemblyRemoveTxDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	if len(req.Txid) != 32 {
		return nil, fmt.Errorf("invalid txid length: %d for %s", len(req.Txid), utils.ReverseAndHexEncodeSlice(req.Txid))
	}

	hash := chainhash.Hash(req.Txid)

	if !ba.blockAssemblyDisabled {
		if err := ba.blockAssembler.RemoveTx(hash); err != nil {
			return nil, err
		}
	}

	return &blockassembly_api.EmptyMessage{}, nil
}

func (ba *BlockAssembly) AddTxBatch(ctx context.Context, batch *blockassembly_api.AddTxBatchRequest) (*blockassembly_api.AddTxBatchResponse, error) {
	// start := gocore.CurrentTime()
	// defer func() {
	// 	addTxBatchGrpc.AddTime(start)
	// }()

	defer func() {
		//traceSpan.Finish()
		// stat.AddTime(startTime)
		prometheusBlockAssemblerTransactions.Set(float64(ba.blockAssembler.TxCount()))
		prometheusBlockAssemblerQueuedTransactions.Set(float64(ba.blockAssembler.QueueLength()))
		prometheusBlockAssemblerSubtrees.Set(float64(ba.blockAssembler.SubtreeCount()))
	}()

	requests := batch.GetTxRequests()
	if len(requests) == 0 {
		return nil, fmt.Errorf("no tx requests in batch")
	}

	var batchError error = nil
	var err error
	txIdErrors := make([][]byte, 0, len(requests))
	for _, req := range requests {
		startTxTime := time.Now()
		// create the subtree node
		if !ba.blockAssemblyDisabled {
			if ba.blockAssemblyCreatesUTXOs {
				if err = ba.storeUtxos(ctx, req); err != nil {
					batchError = err
					txIdErrors = append(txIdErrors, req.Txid)
				}
			}

			ba.blockAssembler.AddTx(util.SubtreeNode{
				Hash:        chainhash.Hash(req.Txid),
				Fee:         req.Fee,
				SizeInBytes: req.Size,
			})

			prometheusBlockAssemblyAddTxDuration.Observe(float64(time.Since(startTxTime).Microseconds()) / 1_000_000)
		}
	}

	return &blockassembly_api.AddTxBatchResponse{
		Ok:         true,
		TxIdErrors: txIdErrors,
	}, batchError
}

func (ba *BlockAssembly) storeUtxos(ctx context.Context, req *blockassembly_api.AddTxRequest) error {
	utxoHashes := make([]chainhash.Hash, len(req.Utxos))
	for i, hash := range req.Utxos {
		utxoHashes[i] = chainhash.Hash(hash)
	}

	if err := ba.utxoStore.StoreFromHashes(ctx, chainhash.Hash(req.Txid), utxoHashes, req.Locktime); err != nil {
		return fmt.Errorf("failed to store utxos: %s", err)
	}

	return nil
}

func (ba *BlockAssembly) GetTxMeta(ctx context.Context, txHash *chainhash.Hash) (*txmeta_store.Data, error) {
	startMetaTime := time.Now()
	txMetaSpan, txMetaSpanCtx := opentracing.StartSpanFromContext(ctx, "BlockAssembly:AddTx:txMeta")
	defer func() {
		txMetaSpan.Finish()
		// blockAssemblyStat.NewStat("GetTxMeta_grpc", true).AddTime(startMetaTime)
		prometheusBlockAssemblerTxMetaGetDuration.Observe(float64(time.Since(startMetaTime).Microseconds()) / 1_000_000)
	}()

	txMetadata, err := ba.txMetaStore.Get(txMetaSpanCtx, txHash)
	if err != nil {
		return nil, err
	}

	currentChainMapIDs := ba.blockAssembler.GetCurrentChainMapIDs()

	// looking this up here and adding to the subtree processor, might create a situation where a transaction
	// that was in a block from a competing miner, is added to the subtree processor when it shouldn't
	if len(txMetadata.BlockIDs) > 0 {
		for _, id := range txMetadata.BlockIDs {
			if _, ok := currentChainMapIDs[id]; ok {
				// the tx is already in a block on our chain, nothing to do
				return nil, fmt.Errorf("tx already in a block on the active chain: %d", id)
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
		prometheusBlockAssemblyGetMiningCandidateDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
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

	// decouple the tracing context to not cancel the context when the subtree TTL is being saved in the background
	callerSpan := opentracing.SpanFromContext(ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	go func() {
		previousHash, _ := chainhash.NewHash(miningCandidate.PreviousHash)
		if err := ba.blockchainClient.SendNotification(setCtx, &model.Notification{
			Type: model.NotificationType_MiningOn,
			Hash: previousHash,
		}); err != nil {
			ba.logger.Errorf("failed to send mining on notification: %s", err)
		}
	}()

	return miningCandidate, nil
}

func (ba *BlockAssembly) SubmitMiningSolution(_ context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error) {
	start := gocore.CurrentTime()
	defer blockAssemblyStat.NewStat("SubmitMiningSolution_grpc", true).AddTime(start)

	waitForResponse := gocore.Config().GetBool("blockassembly_SubmitMiningSolution_waitForResponse", true)
	var responseChan chan bool
	if waitForResponse {
		responseChan = make(chan bool)
		defer close(responseChan)
	}

	// we don't have the processing to handle multiple huge blocks at the same time, so we limit it to 1
	// at a time, this is a temporary solution for now
	request := &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: req,
		responseChan:                responseChan,
	}
	ba.blockSubmissionChan <- request

	ok := true

	if waitForResponse {
		ok = <-request.responseChan
		ba.logger.Infof("block submission success=%v - finished in %s", ok, time.Since(start).String())
	}

	prometheusBlockAssemblySubmitMiningSolutionCh.Set(float64(len(ba.blockSubmissionChan)))

	return &blockassembly_api.SubmitMiningSolutionResponse{
		Ok: ok,
	}, nil
}

func (ba *BlockAssembly) submitMiningSolution(cntxt context.Context, req *BlockSubmissionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error) {
	start, stat, ctx := util.NewStatFromContext(cntxt, "submitMiningSolution", blockAssemblyStat)
	defer func() {
		stat.AddTime(start)
		prometheusBlockAssemblySubmitMiningSolutionDuration.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	jobID := utils.ReverseAndHexEncodeSlice(req.Id)

	prometheusBlockAssemblySubmitMiningSolution.Inc()
	ba.logger.Infof("[BlockAssembly][%s] SubmitMiningSolution", jobID)

	storeId, err := chainhash.NewHash(req.Id[:])
	if err != nil {
		return nil, err
	}

	jobItem := ba.jobStore.Get(*storeId)
	if jobItem == nil {
		return nil, fmt.Errorf("[BlockAssembly][%s] job not found", jobID)
	}
	job := jobItem.Value()

	hashPrevBlock, err := chainhash.NewHash(job.MiningCandidate.PreviousHash)
	if err != nil {
		return nil, fmt.Errorf("[BlockAssembly][%s] failed to convert hashPrevBlock: %w", jobID, err)
	}

	// TODO check whether we are already mining on a higher chain work, then just ignore this solution

	coinbaseTx, err := bt.NewTxFromBytes(req.CoinbaseTx)
	if err != nil {
		return nil, fmt.Errorf("[BlockAssembly][%s] failed to convert coinbaseTx: %w", jobID, err)
	}
	coinbaseTxIDHash := coinbaseTx.TxIDChainHash()

	var sizeInBytes uint64

	subtreesInJob := make([]*util.Subtree, len(job.Subtrees))
	subtreeHashes := make([]chainhash.Hash, len(job.Subtrees))
	jobSubtreeHashes := make([]*chainhash.Hash, len(job.Subtrees))
	transactionCount := uint64(0)
	if len(job.Subtrees) > 0 {
		ba.logger.Infof("[BlockAssembly][%s] submit job has subtrees: %d", jobID, len(job.Subtrees))
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
	topTree, err := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(subtreesInJob)))
	if err != nil {
		return nil, fmt.Errorf("[BlockAssembly][%s] failed to create topTree: %w", jobID, err)
	}
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
			return nil, fmt.Errorf("[BlockAssembly][%s] error getting merkle proof for coinbase: %w", jobID, err)
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

	// sizeInBytes from the subtrees, 80 byte header and varint bytes for txcount
	blockSize := sizeInBytes + 80 + util.VarintSize(transactionCount)
	// add the size of the coinbase tx to the blocksize
	blockSize += uint64(coinbaseTx.Size())

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
		SizeInBytes:      blockSize,
		Subtrees:         jobSubtreeHashes, // we need to store the hashes of the subtrees in the block, without the coinbase
		SubtreeSlices:    job.Subtrees,
	}

	startTime := time.Now()
	ba.logger.Infof("[BlockAssembly][%s][%s] validating block", jobID, block.Header.Hash())
	// check fully valid, including whether difficulty in header is low enough
	if ok, err := block.Valid(ctx, ba.logger, nil, nil, nil, nil, nil); !ok {
		ba.logger.Errorf("[BlockAssembly][%s][%s] invalid block: %v - %v", jobID, block.Hash().String(), block.Header, err)
		return nil, fmt.Errorf("[BlockAssembly][%s][%s] invalid block: %v", jobID, block.Hash().String(), err)
	}
	ba.logger.Infof("[BlockAssembly][%s][%s] validating block DONE in %s", jobID, block.Header.Hash(), time.Since(startTime).String())

	// TODO context was being canceled, is this hiding a different problem?
	err = ba.txStore.Set(context.Background(), block.CoinbaseTx.TxIDChainHash().CloneBytes(), block.CoinbaseTx.ExtendedBytes())
	if err != nil {
		ba.logger.Errorf("[BlockAssembly][%s][%s] error storing coinbase tx in tx store: %v", jobID, block.Hash().String(), err)
	}

	// TODO why is this needed?
	//_, err = ba.txMetaStore.Create(cntxt, block.CoinbaseTx)
	//if err != nil {
	//	ba.logger.Errorf("[BlockAssembly] error storing coinbase tx in tx meta store: %v", err)
	//}

	ba.logger.Infof("[BlockAssembly][%s][%s] add block to blockchain", jobID, block.Header.Hash())
	// add block to the blockchain
	if err = ba.blockchainClient.AddBlock(ctx, block, ""); err != nil {
		return nil, fmt.Errorf("[BlockAssembly][%s][%s] failed to add block: %w", jobID, block.Hash().String(), err)
	}

	ids, err := ba.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.Hash(), 1)
	if err != nil {
		return nil, fmt.Errorf("[BlockAssembly][%s][%s] failed to get block header ids: %w", jobID, block.Hash().String(), err)
	}

	var blockID uint32
	if len(ids) > 0 {
		blockID = ids[0]
	}

	// decouple the tracing context to not cancel the context when the subtree TTL is being saved in the background
	callerSpan := opentracing.SpanFromContext(ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	go func() {
		// TODO what do we do if this fails, the subtrees TTL and tx meta status still needs to be updated
		g, gCtx := errgroup.WithContext(setCtx)

		g.Go(func() error {
			timeStart := time.Now()
			ba.logger.Infof("[BlockAssembly][%s][%s] remove subtrees TTL", jobID, block.Header.Hash())

			if err := ba.removeSubtreesTTL(gCtx, block); err != nil {
				// TODO retry
				ba.logger.Errorf("[BlockAssembly][%s][%s] failed to remove subtrees TTL: %v", jobID, block.Header.Hash(), err)
			}

			ba.logger.Infof("[BlockAssembly][%s][%s] remove subtrees TTL DONE in %s", jobID, block.Header.Hash(), time.Since(timeStart).String())

			return nil
		})

		if !ba.localSetMined {
			g.Go(func() error {
				timeStart := time.Now()
				// add the transactions in this block to the txMeta block hashes
				ba.logger.Infof("[BlockAssembly][%s][%s] update tx mined status", jobID, block.Header.Hash())

				if err := model.UpdateTxMinedStatus(gCtx, ba.logger, ba.blockValidationClient, subtreesInJob, blockID); err != nil {
					// TODO retry
					ba.logger.Errorf("[BlockAssembly][%s][%s] error updating tx mined status: %v", jobID, block.Header.Hash(), err)
				}

				ba.logger.Infof("[BlockAssembly][%s][%s] update tx mined status DONE in %s", jobID, block.Header.Hash(), time.Since(timeStart).String())

				return nil
			})
		}

		if err = g.Wait(); err != nil {
			ba.logger.Errorf("[BlockAssembly][%s][InvalidateBlock] block is not valid: %v", block.String(), err)

			if err = ba.blockchainClient.InvalidateBlock(setCtx, block.Header.Hash()); err != nil {
				ba.logger.Errorf("[BlockAssembly][%s][InvalidateBlock] failed to invalidate block: %s", block.Header.Hash(), err)
			}
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
		prometheusBlockAssemblyUpdateSubtreesTTL.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	// decouple the tracing context to not cancel the context when the subtree TTL is being saved in the background
	callerSpan := opentracing.SpanFromContext(spanCtx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	subtreeTTLConcurrency, _ := gocore.Config().GetInt("blockassembly_subtreeTTLConcurrency", 32)

	g, gCtx := errgroup.WithContext(setCtx)
	g.SetLimit(subtreeTTLConcurrency)

	startTime := time.Now()
	ba.logger.Infof("[removeSubtreesTTL][%s] updating subtree TTLs", block.Hash().String())

	// update the subtree TTLs
	for _, subtreeHash := range block.Subtrees {
		subtreeHashBytes := subtreeHash.CloneBytes()
		subtreeHash := subtreeHash
		g.Go(func() error {
			// TODO this would be better as a batch operation
			err = ba.subtreeStore.SetTTL(gCtx, subtreeHashBytes, 0)
			if err != nil {
				// TODO should this retry? We are in a bad state when this happens
				ba.logger.Errorf("[removeSubtreesTTL][%s][%s] failed to update subtree TTL: %v", block.Hash().String(), subtreeHash.String(), err)
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	ba.logger.Infof("[removeSubtreesTTL][%s] updating subtree TTLs DONE in %s", block.Hash().String(), time.Since(startTime).String())

	return nil
}

func (ba *BlockAssembly) DeDuplicateBlockAssembly(_ context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error) {
	ba.blockAssembler.DeDuplicateTransactions()
	return &blockassembly_api.EmptyMessage{}, nil
}

func (ba *BlockAssembly) ResetBlockAssembly(_ context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error) {
	ba.blockAssembler.Reset()
	return &blockassembly_api.EmptyMessage{}, nil
}
