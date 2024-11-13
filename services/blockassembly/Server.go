package blockassembly

import (
	"context"
	"net/http"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/miner/cpuminer"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/bitcoin-sv/ubsv/util/retry"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
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
	stats          *gocore.Stat

	blockchainClient      blockchain.ClientI
	txStore               blob.Store
	utxoStore             utxostore.Store
	subtreeStore          blob.Store
	subtreeTTL            time.Duration
	jobStore              *ttlcache.Cache[chainhash.Hash, *subtreeprocessor.Job] // has built in locking
	blockSubmissionChan   chan *BlockSubmissionRequest
	blockAssemblyDisabled bool
}

type subtreeRetrySend struct {
	subtreeHash  chainhash.Hash
	subtreeBytes []byte
	retries      int
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, txStore blob.Store, utxoStore utxostore.Store, subtreeStore blob.Store,
	blockchainClient blockchain.ClientI) *BlockAssembly {
	// initialize Prometheus metrics, singleton, will only happen once
	initPrometheusMetrics()

	subtreeTTLMinutes, _ := gocore.Config().GetInt("blockassembly_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	ba := &BlockAssembly{
		logger:                logger,
		stats:                 gocore.NewStat("blockassembly"),
		blockchainClient:      blockchainClient,
		txStore:               txStore,
		utxoStore:             utxoStore,
		subtreeStore:          subtreeStore,
		subtreeTTL:            subtreeTTL,
		jobStore:              ttlcache.New[chainhash.Hash, *subtreeprocessor.Job](),
		blockSubmissionChan:   make(chan *BlockSubmissionRequest),
		blockAssemblyDisabled: gocore.Config().GetBool("blockassembly_disabled", false),
	}

	go ba.jobStore.Start()

	return ba
}

func (ba *BlockAssembly) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
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
		{Name: "BlockchainClient", Check: ba.blockchainClient.Health},
		{Name: "SubtreeStore", Check: ba.subtreeStore.Health},
		{Name: "TxStore", Check: ba.txStore.Health},
		{Name: "UTXOStore", Check: ba.utxoStore.Health},
		{Name: "FSM", Check: blockchain.CheckFSM(ba.blockchainClient)},
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (ba *BlockAssembly) HealthGRPC(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.HealthResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "HealthGRPC",
		tracing.WithParentStat(ba.stats),
		tracing.WithCounter(prometheusBlockAssemblyHealth),
		tracing.WithDebugLogMessage(ba.logger, "[HealthGRPC] called"),
	)
	defer deferFn()

	status, details, err := ba.Health(ctx, false)
	return &blockassembly_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.Now(),
	}, errors.WrapGRPC(err)
}

func (ba *BlockAssembly) Init(ctx context.Context) (err error) {
	// this is passed into the block assembler and subtree processor where new subtrees are created
	newSubtreeChanBuffer, _ := gocore.Config().GetInt("blockassembly_newSubtreeChanBuffer", 1_000)
	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, newSubtreeChanBuffer)

	// retry channel for subtrees that failed to be stored
	subtreeRetryChanBuffer, _ := gocore.Config().GetInt("blockassembly_subtreeRetryChanBuffer", 1_000)
	subtreeRetryChan := make(chan *subtreeRetrySend, subtreeRetryChanBuffer)

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
					options.WithFileExtension("subtree"),
				); err != nil {
					if errors.Is(err, errors.ErrBlobAlreadyExists) {
						ba.logger.Debugf("[BlockAssembly:Init][%s] subtreeRetryChan: subtree already exists", subtreeRetry.subtreeHash.String())
						continue
					}

					ba.logger.Errorf("[BlockAssembly:Init][%s] subtreeRetryChan: failed to retry store subtree: %s", subtreeRetry.subtreeHash.String(), err)

					if subtreeRetry.retries > 10 {
						ba.logger.Errorf("[BlockAssembly:Init][%s] subtreeRetryChan: failed to retry store subtree, retries exhausted", subtreeRetry.subtreeHash.String())
						continue
					}

					subtreeRetry.retries++
					go func() {
						// backoff and wait before re-adding to retry queue
						retry.BackoffAndSleep(subtreeRetry.retries, 2, time.Second)

						// re-add the subtree to the retry queue
						subtreeRetryChan <- subtreeRetry
					}()

					continue
				}

				// TODO #145
				// the repository in the blob server sometimes cannot find subtrees that were just stored
				// this is the dumbest way we can think of to fix it, at least temporarily
				time.Sleep(20 * time.Millisecond)

				if err = ba.blockchainClient.SendNotification(ctx, &blockchain.Notification{
					Type:     model.NotificationType_Subtree,
					Hash:     (&subtreeRetry.subtreeHash)[:],
					Base_URL: "",
					Metadata: &blockchain.NotificationMetadata{
						Metadata: nil,
					},
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
				err := ba.storeSubtree(ctx, newSubtreeRequest.Subtree, subtreeRetryChan)
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
		return errors.NewProcessingError("[BlockAssembly:Init][%s] failed to serialize subtree", subtree.RootHash().String(), err)
	}

	if err = ba.subtreeStore.Set(ctx,
		subtree.RootHash()[:],
		subtreeBytes,
		options.WithTTL(ba.subtreeTTL), // this sets the TTL for the subtree, it must be updated when a block is mined
		options.WithFileExtension("subtree"),
	); err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			ba.logger.Debugf("[BlockAssembly:Init][%s] subtree already exists", subtree.RootHash().String())
		} else {
			ba.logger.Errorf("[BlockAssembly:Init][%s] failed to store subtree: %s", subtree.RootHash().String(), err)

			// add to retry saving the subtree
			subtreeRetryChan <- &subtreeRetrySend{
				subtreeHash:  *subtree.RootHash(),
				subtreeBytes: subtreeBytes,
				retries:      0,
			}
		}

		return nil
	}

	// TODO #145
	// the repository in the blob server sometimes cannot find subtrees that were just stored
	// this is the dumbest way we can think of to fix it, at least temporarily
	time.Sleep(20 * time.Millisecond)

	if err = ba.blockchainClient.SendNotification(ctx, &blockchain.Notification{
		Type:     model.NotificationType_Subtree,
		Hash:     subtree.RootHash()[:],
		Base_URL: "",
		Metadata: &blockchain.NotificationMetadata{
			Metadata: nil,
		},
	}); err != nil {
		return errors.NewServiceError("[BlockAssembly:Init][%s] failed to send subtree notification", subtree.RootHash().String(), err)
	}
	return nil
}

// Start function
func (ba *BlockAssembly) Start(ctx context.Context) (err error) {
	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		if err = ba.blockchainClient.Restore(ctx); err != nil {
			ba.logger.Errorf("[BlockAssembly] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		ba.logger.Infof("[BlockAssembly] Node is restoring, waiting for FSM to transition to Running state")
		_ = ba.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain.FSMStateRUNNING)
		ba.logger.Infof("[BlockAssembly] Node finished restoring and has transitioned to Running state, continuing to start BlockAssembly service")
	}

	if err = ba.blockAssembler.Start(ctx); err != nil {
		return errors.NewServiceError("failed to start block assembler", err)
	}

	// this will block
	if err = util.StartGRPCServer(ctx, ba.logger, "blockassembly", func(server *grpc.Server) {
		blockassembly_api.RegisterBlockAssemblyAPIServer(server, ba)
	}); err != nil {
		return err
	}

	return nil
}

func (ba *BlockAssembly) Stop(_ context.Context) error {
	ba.jobStore.Stop()
	return nil
}

var txsProcessed = atomic.Uint64{}

func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (resp *blockassembly_api.AddTxResponse, err error) {
	_, _, deferFn := tracing.StartTracing(ctx, "AddTx",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblyAddTx),
		tracing.WithLogMessage(ba.logger, "[AddTx][%s] add tx called", utils.ReverseAndHexEncodeSlice(req.Txid)),
	)

	defer func() {
		if txsProcessed.Load()%1000 == 0 {
			// we should NOT be setting this on every call, it's a waste of resources
			prometheusBlockAssemblerTransactions.Set(float64(ba.blockAssembler.TxCount()))
			prometheusBlockAssemblerQueuedTransactions.Set(float64(ba.blockAssembler.QueueLength()))
			prometheusBlockAssemblerSubtrees.Set(float64(ba.blockAssembler.SubtreeCount()))
		}
		txsProcessed.Inc()

		deferFn()
	}()

	if len(req.Txid) != 32 {
		return nil, errors.WrapGRPC(
			errors.NewProcessingError("invalid txid length: %d for %s", len(req.Txid), utils.ReverseAndHexEncodeSlice(req.Txid)))
	}

	if !ba.blockAssemblyDisabled {
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

func (ba *BlockAssembly) RemoveTx(ctx context.Context, req *blockassembly_api.RemoveTxRequest) (*blockassembly_api.EmptyMessage, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "RemoveTx",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblyRemoveTx),
		tracing.WithLogMessage(ba.logger, "[RemoveTx][%s] called", utils.ReverseAndHexEncodeSlice(req.Txid)),
	)
	defer deferFn()

	if len(req.Txid) != 32 {
		return nil, errors.WrapGRPC(
			errors.NewProcessingError("invalid txid length: %d for %s", len(req.Txid), utils.ReverseAndHexEncodeSlice(req.Txid)))
	}

	hash := chainhash.Hash(req.Txid)

	if !ba.blockAssemblyDisabled {
		if err := ba.blockAssembler.RemoveTx(hash); err != nil {
			return nil, errors.WrapGRPC(err)
		}
	}

	return &blockassembly_api.EmptyMessage{}, nil
}

func (ba *BlockAssembly) AddTxBatch(ctx context.Context, batch *blockassembly_api.AddTxBatchRequest) (*blockassembly_api.AddTxBatchResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "AddTxBatch",
		tracing.WithParentStat(ba.stats),
		tracing.WithDebugLogMessage(ba.logger, "[AddTxBatch] called with %d transactions", len(batch.GetTxRequests())),
	)
	defer func() {
		prometheusBlockAssemblerTransactions.Set(float64(ba.blockAssembler.TxCount()))
		prometheusBlockAssemblerQueuedTransactions.Set(float64(ba.blockAssembler.QueueLength()))
		prometheusBlockAssemblerSubtrees.Set(float64(ba.blockAssembler.SubtreeCount()))
		deferFn()
	}()

	requests := batch.GetTxRequests()
	if len(requests) == 0 {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("no tx requests in batch"))
	}

	// this is never used, so we can remove it
	for _, req := range requests {
		startTxTime := time.Now()
		// create the subtree node
		if !ba.blockAssemblyDisabled {
			ba.blockAssembler.AddTx(util.SubtreeNode{
				Hash:        chainhash.Hash(req.Txid),
				Fee:         req.Fee,
				SizeInBytes: req.Size,
			})

			prometheusBlockAssemblyAddTx.Observe(float64(time.Since(startTxTime).Microseconds()) / 1_000_000)
		}
	}

	resp := &blockassembly_api.AddTxBatchResponse{
		Ok: true,
	}

	return resp, nil
}

func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*model.MiningCandidate, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetMiningCandidate",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblyGetMiningCandidateDuration),
		tracing.WithLogMessage(ba.logger, "[GetMiningCandidate] called"),
	)
	defer deferFn()

	isRunning, err := ba.blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateRUNNING)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	if !isRunning {
		return nil, errors.WrapGRPC(errors.NewStateError("cannot get mining candidate when FSM is not in RUNNING state"))
	}

	miningCandidate, subtrees, err := ba.blockAssembler.GetMiningCandidate(ctx)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	id, _ := chainhash.NewHash(miningCandidate.Id)

	ba.jobStore.Set(*id, &subtreeprocessor.Job{
		ID:              id,
		Subtrees:        subtrees,
		MiningCandidate: miningCandidate,
	}, jobTTL) // create a new job with a TTL, will be cleaned up automatically

	// decouple the tracing context to not cancel the context when the subtree TTL is being saved in the background
	callerSpan := tracing.DecoupleTracingSpan(ctx, "decouple")
	defer callerSpan.Finish()

	go func() {
		previousHash, err := chainhash.NewHash(miningCandidate.PreviousHash)
		if err != nil {
			ba.logger.Errorf("failed to convert previous hash: %s", err)
		}

		if err = ba.blockchainClient.SendNotification(callerSpan.Ctx, &blockchain.Notification{
			Type:     model.NotificationType_MiningOn,
			Hash:     previousHash[:],
			Base_URL: "",
			Metadata: &blockchain.NotificationMetadata{
				Metadata: nil,
			},
		}); err != nil {
			ba.logger.Errorf("failed to send mining on notification: %s", err)
		}
	}()

	return miningCandidate, nil
}

func (ba *BlockAssembly) SubmitMiningSolution(ctx context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "SubmitMiningSolution",
		tracing.WithParentStat(ba.stats),
		tracing.WithLogMessage(ba.logger, "[SubmitMiningSolution] called"),
	)
	defer deferFn()

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
	}

	return &blockassembly_api.SubmitMiningSolutionResponse{
		Ok: ok,
	}, nil
}

func (ba *BlockAssembly) submitMiningSolution(ctx context.Context, req *BlockSubmissionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error) {
	jobID := utils.ReverseAndHexEncodeSlice(req.Id)
	ctx, _, deferFn := tracing.StartTracing(ctx, "submitMiningSolution",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblySubmitMiningSolution),
		tracing.WithLogMessage(ba.logger, "[submitMiningSolution] called for job id %s", jobID),
	)

	defer deferFn()

	storeId, err := chainhash.NewHash(req.Id)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	jobItem := ba.jobStore.Get(*storeId)
	if jobItem == nil {
		return nil, errors.NewProcessingError("[BlockAssembly][%s] job not found", jobID)
	}
	job := jobItem.Value()

	hashPrevBlock, err := chainhash.NewHash(job.MiningCandidate.PreviousHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[BlockAssembly][%s] failed to convert hashPrevBlock", jobID, err))
	}

	if ba.blockAssembler.bestBlockHeader.Load().HashPrevBlock.IsEqual(hashPrevBlock) {
		return nil, errors.WrapGRPC(
			errors.NewProcessingError("[BlockAssembly][%s] already mining on top of the same block that is submitted", jobID))
	}

	coinbaseTx, err := bt.NewTxFromBytes(req.CoinbaseTx)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[BlockAssembly][%s] failed to convert coinbaseTx", jobID, err))
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
		return nil, errors.WrapGRPC(errors.NewProcessingError("[BlockAssembly][%s] failed to create topTree", jobID, err))
	}

	for _, hash := range subtreeHashes {
		err = topTree.AddNode(hash, 1, 0)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewProcessingError("[BlockAssembly][%s] failed to add node to topTree", jobID, err))
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
			return nil, errors.WrapGRPC(
				errors.NewProcessingError("[BlockAssembly][%s] error getting merkle proof for coinbase", jobID, err))
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
			return nil, errors.WrapGRPC(errors.NewProcessingError("[BlockAssembly][%s] failed to convert hashMerkleRoot", jobID, err))
		}
	}

	// sizeInBytes from the subtrees, 80 byte header and varint bytes for txcount
	blockSize := sizeInBytes + 80 + util.VarintSize(transactionCount)
	// add the size of the coinbase tx to the blocksize
	blockSize += uint64(coinbaseTx.Size())

	bits, err := model.NewNBitFromSlice(job.MiningCandidate.NBits)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}
	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        req.Version,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Timestamp:      req.Time,
			Bits:           *bits,
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
	if ok, err := block.Valid(ctx, ba.logger, nil, nil, nil, nil, nil, nil, nil); !ok {
		ba.logger.Errorf("[BlockAssembly][%s][%s] invalid block: %v - %v", jobID, block.Hash().String(), block.Header, err)
		return nil, errors.WrapGRPC(
			errors.NewProcessingError("[BlockAssembly][%s][%s] invalid block", jobID, block.Hash().String(), err))
	}

	ba.logger.Infof("[BlockAssembly][%s][%s] validating block DONE in %s", jobID, block.Header.Hash(), time.Since(startTime).String())

	// TODO context was being canceled, is this hiding a different problem?
	err = ba.txStore.Set(context.Background(), block.CoinbaseTx.TxIDChainHash().CloneBytes(), block.CoinbaseTx.ExtendedBytes())
	if err != nil {
		ba.logger.Errorf("[BlockAssembly][%s][%s] error storing coinbase tx in tx store: %v", jobID, block.Hash().String(), err)
	}

	// TODO why is this needed?
	// _, err = ba.txMetaStore.Create(cntxt, block.CoinbaseTx)
	// if err != nil {
	//	ba.logger.Errorf("[BlockAssembly] error storing coinbase tx in tx meta store: %v", err)
	// }

	ba.logger.Debugf("[BlockAssembly][%s][%s] add block to blockchain", jobID, block.Header.Hash())
	ba.logger.Debugf("[BlockAssembly][%s][%s] block difficulty: %s", jobID, block.Header.Hash(), block.Header.Bits.CalculateDifficulty().String())
	ba.logger.Debugf("[BlockAssembly][%s][%s] time since previous block: %s", jobID, block.Header.Hash(), time.Since(time.Unix(int64(ba.blockAssembler.bestBlockHeader.Load().Timestamp), 0)).String())
	// add block to the blockchain
	if err = ba.blockchainClient.AddBlock(ctx, block, ""); err != nil {
		return nil, errors.WrapGRPC(
			errors.NewServiceError("[BlockAssembly][%s][%s] failed to add block", jobID, block.Hash().String(), err))
	}

	// don't wait for blockchain to notify us of new block.
	// if we are mining initial blocks or mining 'immediately' then we won't get notified quick enough
	// and we'll fork unnecessarily
	ba.blockAssembler.UpdateBestBlock(ctx)

	// decouple the tracing context to not cancel the context when the subtree TTL is being saved in the background
	callerSpan := tracing.DecoupleTracingSpan(ctx, "decoupleSubtreeTTL")
	defer callerSpan.Finish()

	go func() {
		// TODO what do we do if this fails, the subtrees TTL and tx meta status still needs to be updated
		g, gCtx := errgroup.WithContext(callerSpan.Ctx)

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

		if err = g.Wait(); err != nil {
			ba.logger.Errorf("[BlockAssembly][%s][InvalidateBlock] block is not valid: %v", block.String(), err)

			if err = ba.blockchainClient.InvalidateBlock(callerSpan.Ctx, block.Header.Hash()); err != nil {
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
	ctx, _, deferFn := tracing.StartTracing(ctx, "BlockAssembly:removeSubtreesTTL",
		tracing.WithHistogram(prometheusBlockAssemblyUpdateSubtreesTTL),
	)
	defer deferFn()

	// decouple the tracing context to not cancel the context when the subtree TTL is being saved in the background
	callerSpan := tracing.DecoupleTracingSpan(ctx, "decoupleSubtreeTTL")
	defer callerSpan.Finish()

	subtreeTTLConcurrency, _ := gocore.Config().GetInt("blockassembly_subtreeTTLConcurrency", 32)

	g, gCtx := errgroup.WithContext(callerSpan.Ctx)
	g.SetLimit(subtreeTTLConcurrency)

	startTime := time.Now()

	ba.logger.Infof("[removeSubtreesTTL][%s] updating subtree TTLs", block.Hash().String())

	// update the subtree TTLs
	for _, subtreeHash := range block.Subtrees {
		subtreeHashBytes := subtreeHash.CloneBytes()
		subtreeHash := subtreeHash

		g.Go(func() error {
			// TODO this would be better as a batch operation
			if err := ba.subtreeStore.SetTTL(gCtx, subtreeHashBytes, 0, options.WithFileExtension("subtree")); err != nil {
				// TODO should this retry? We are in a bad state when this happens
				ba.logger.Errorf("[removeSubtreesTTL][%s][%s] failed to update subtree TTL: %v", block.Hash().String(), subtreeHash.String(), err)
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.WrapGRPC(err)
	}

	// update block subtrees_set to true
	if err = ba.blockchainClient.SetBlockSubtreesSet(ctx, block.Hash()); err != nil {
		return errors.WrapGRPC(
			errors.NewServiceError("[ValidateBlock][%s] failed to set block subtrees_set", block.Hash().String(), err))
	}

	ba.logger.Infof("[removeSubtreesTTL][%s] updating subtree TTLs DONE in %s", block.Hash().String(), time.Since(startTime).String())

	return nil
}

func (ba *BlockAssembly) DeDuplicateBlockAssembly(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "DeDuplicateBlockAssembly",
		tracing.WithParentStat(ba.stats),
		tracing.WithLogMessage(ba.logger, "[DeDuplicateBlockAssembly] called"),
	)
	defer deferFn()

	ba.blockAssembler.DeDuplicateTransactions()
	return &blockassembly_api.EmptyMessage{}, nil
}

func (ba *BlockAssembly) ResetBlockAssembly(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "ResetBlockAssembly",
		tracing.WithParentStat(ba.stats),
		tracing.WithLogMessage(ba.logger, "[ResetBlockAssembly] called"),
	)
	defer deferFn()

	ba.blockAssembler.Reset()

	return &blockassembly_api.EmptyMessage{}, nil
}

func (ba *BlockAssembly) GetBlockAssemblyState(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.StateMessage, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "GetBlockAssemblyState",
		tracing.WithParentStat(ba.stats),
		tracing.WithLogMessage(ba.logger, "[GetBlockAssemblyState] called"),
	)
	defer deferFn()

	return &blockassembly_api.StateMessage{
		BlockAssemblyState:    ba.blockAssembler.GetCurrentRunningState(),
		SubtreeProcessorState: ba.blockAssembler.subtreeProcessor.GetCurrentRunningState(),
		ResetWaitCount:        uint32(ba.blockAssembler.resetWaitCount.Load()),
		ResetWaitTime:         uint32(ba.blockAssembler.resetWaitTime.Load()),
		SubtreeCount:          uint32(ba.blockAssembler.SubtreeCount()), //nolint:gosec
		TxCount:               ba.blockAssembler.TxCount(),
		QueueCount:            ba.blockAssembler.QueueLength(),
		CurrentHeight:         ba.blockAssembler.bestBlockHeight.Load(),
		CurrentHash:           ba.blockAssembler.bestBlockHeader.Load().Hash().String(),
	}, nil
}

func (ba *BlockAssembly) GetCurrentDifficulty(ctx context.Context, _ *blockassembly_api.EmptyMessage) (resp *blockassembly_api.GetCurrentDifficultyResponse, err error) {
	cd := ba.blockAssembler.currentDifficulty
	dif := cd.CalculateDifficulty()
	f, _ := dif.Float64()

	return &blockassembly_api.GetCurrentDifficultyResponse{
		Difficulty: f,
	}, nil
}

// generate blocks
func (ba *BlockAssembly) GenerateBlocks(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) (*blockassembly_api.EmptyMessage, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "generateBlocks",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblerGenerateBlocks),
		tracing.WithLogMessage(ba.logger, "[generateBlocks] called"),
	)
	defer deferFn()

	if !ba.blockAssembler.chainParams.GenerateSupported {
		return nil, errors.NewProcessingError("generate is not supported")
	}

	for i := 0; i < int(req.Count); i++ {
		err := ba.generateBlock(ctx)
		if err != nil {
			return nil, errors.NewProcessingError("error generating block", err)
		}
	}

	return &blockassembly_api.EmptyMessage{}, nil
}

func (ba *BlockAssembly) generateBlock(ctx context.Context) error {
	// get a mining candidate
	miningCandidate, err := ba.GetMiningCandidate(ctx, &blockassembly_api.EmptyMessage{})
	if err != nil {
		return errors.NewProcessingError("error getting mining candidate", err)
	}

	// mine the block
	miningSolution, err := cpuminer.Mine(ctx, miningCandidate)
	if err != nil {
		return errors.NewProcessingError("error mining block", err)
	}

	var responseChan chan bool

	// submit the block
	req := &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:         miningSolution.Id,
			Nonce:      miningSolution.Nonce,
			CoinbaseTx: miningSolution.Coinbase,
			Time:       miningSolution.Time,
			Version:    miningSolution.Version,
		},
		responseChan: responseChan,
	}
	resp, err := ba.submitMiningSolution(ctx, req)
	if err != nil {
		ba.logger.Errorf("[generateBlock] error submitting block: %v", err)
		return errors.NewProcessingError("error submitting block", err)
	}

	if resp.Ok {
		return nil
	}

	return bt.ErrTxNil
}
