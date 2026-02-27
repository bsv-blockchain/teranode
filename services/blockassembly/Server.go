// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
//
// The blockassembly service is responsible for managing the block creation process,
// including transaction selection, block template generation, and mining integration.
// It serves as a critical component in the blockchain's consensus mechanism by:
//   - Continuously processing and organizing transactions into subtrees
//   - Constructing valid block templates for miners
//   - Processing mining solutions and submitting validated blocks to the blockchain
//   - Handling block reorganizations and maintaining block assembly state
//
// The service integrates with other Teranode components through well-defined interfaces
// and uses a subtree-based approach for efficient transaction management. It implements
// both synchronous and asynchronous processing paths to optimize for throughput and
// latency requirements of high-volume Bitcoin transaction processing.
package blockassembly

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/mining"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// addTxBatchGrpc = blockAssemblyStat.NewStat("AddTxBatch_grpc", true)

	// jobTTL defines the time-to-live for mining jobs
	// channelStats = blockAssemblyStat.NewStat("channels", false)
	jobTTL = 10 * time.Minute
)

// BlockSubmissionRequest encapsulates a request to submit a mined block solution
// along with a channel for receiving the processing result.
type BlockSubmissionRequest struct {
	// SubmitMiningSolutionRequest contains the actual mining solution data
	// including nonce, timestamp, and other block header fields
	*blockassembly_api.SubmitMiningSolutionRequest

	// responseChan receives the result of the submission processing
	// A nil error indicates successful submission
	// This channel is optional and only used when waiting for response is enabled
	responseChan chan error
}

// BlockAssembly represents the main service for block assembly operations.
type BlockAssembly struct {
	// UnimplementedBlockAssemblyAPIServer provides default implementations for gRPC methods
	blockassembly_api.UnimplementedBlockAssemblyAPIServer

	// blockAssembler handles the core block assembly logic
	blockAssembler *BlockAssembler

	// logger provides logging functionality
	logger ulogger.Logger

	// stats tracks operational statistics
	stats *gocore.Stat

	// settings contains configuration parameters
	settings *settings.Settings

	// blockchainClient interfaces with the blockchain
	blockchainClient blockchain.ClientI

	// txStore manages transaction storage
	txStore blob.Store

	// utxoStore manages UTXO storage
	utxoStore utxostore.Store

	// subtreeStore manages subtree storage
	subtreeStore blob.Store

	// jobStore caches mining jobs with TTL
	jobStore *ttlcache.Cache[chainhash.Hash, *subtreeprocessor.Job]

	// blockSubmissionChan handles block submission requests
	blockSubmissionChan chan *BlockSubmissionRequest

	// skipWaitForPendingBlocks stores the flag value for tests
	skipWaitForPendingBlocks bool

	// stopOnce ensures Stop() is only executed once
	stopOnce sync.Once
}

// subtreeRetrySend encapsulates the data needed for retrying subtree storage operations
// This is used when initial storage attempts fail and need to be retried with backoff
type subtreeRetrySend struct {
	// subtreeHash uniquely identifies the subtree being stored
	subtreeHash chainhash.Hash

	// subtreeBytes contains the serialized subtree data to be stored
	subtreeBytes []byte

	// subtreeMetaBytes contains the serialized subtree meta data to be stored
	subtreeMetaBytes []byte

	// retries tracks the number of storage attempts made for this subtree
	// Used to implement exponential backoff and maximum retry limits
	retries int
}

// New creates a new BlockAssembly instance.
//
// Parameters:
//   - logger: Logger for recording operations
//   - tSettings: Teranode settings configuration
//   - txStore: Transaction storage
//   - utxoStore: UTXO storage
//   - subtreeStore: Subtree storage
//   - blockchainClient: Interface to blockchain operations
//
// Returns:
//   - *BlockAssembly: New block assembly instance
func New(logger ulogger.Logger, tSettings *settings.Settings, txStore blob.Store, utxoStore utxostore.Store, subtreeStore blob.Store,
	blockchainClient blockchain.ClientI) *BlockAssembly {
	// initialize Prometheus metrics, singleton, will only happen once
	initPrometheusMetrics()

	ba := &BlockAssembly{
		logger:              logger,
		stats:               gocore.NewStat("blockassembly"),
		settings:            tSettings,
		blockchainClient:    blockchainClient,
		txStore:             txStore,
		utxoStore:           utxoStore,
		subtreeStore:        subtreeStore,
		jobStore:            ttlcache.New[chainhash.Hash, *subtreeprocessor.Job](),
		blockSubmissionChan: make(chan *BlockSubmissionRequest),
	}

	go ba.jobStore.Start()

	return ba
}

// Health checks the health status of the BlockAssembly service.
//
// Parameters:
//   - ctx: Context for cancellation
//   - checkLiveness: Whether to perform liveness check
//
// Returns:
//   - int: HTTP status code indicating health state
//   - string: Health status message
//   - error: Any error encountered during health check
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
	checks := make([]health.Check, 0, 6)

	// Check if the gRPC server is actually listening and accepting requests
	// Only check if the address is configured (not empty)
	if ba.settings.BlockAssembly.GRPCListenAddress != "" {
		checks = append(checks, health.Check{
			Name: "gRPC Server",
			Check: health.CheckGRPCServerWithSettings(ba.settings.BlockAssembly.GRPCListenAddress, ba.settings, func(ctx context.Context, conn *grpc.ClientConn) error {
				client := blockassembly_api.NewBlockAssemblyAPIClient(conn)
				_, err := client.HealthGRPC(ctx, &blockassembly_api.EmptyMessage{})
				return err
			}),
		})
	}

	if ba.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: ba.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(ba.blockchainClient)})
	}

	if ba.subtreeStore != nil {
		checks = append(checks, health.Check{Name: "SubtreeStore", Check: ba.subtreeStore.Health})
	}

	if ba.txStore != nil {
		checks = append(checks, health.Check{Name: "TxStore", Check: ba.txStore.Health})
	}

	if ba.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: ba.utxoStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC implements the gRPC health check endpoint.
//
// Parameters:
//   - ctx: Context for cancellation
//   - _: Empty message request (unused)
//
// Returns:
//   - *blockassembly_api.HealthResponse: Health check response
//   - error: Any error encountered during health check
func (ba *BlockAssembly) HealthGRPC(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.HealthResponse, error) {
	// Add context value to prevent circular dependency when checking gRPC server health
	ctx = context.WithValue(ctx, "skip-grpc-self-check", true)
	status, details, err := ba.Health(ctx, false)

	return &blockassembly_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.Now(),
	}, errors.WrapGRPC(err)
}

// Init initializes the BlockAssembly service.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - error: Any error encountered during initialization
func (ba *BlockAssembly) Init(ctx context.Context) (err error) {
	// this is passed into the block assembler and subtree processor where new subtrees are created
	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, ba.settings.BlockAssembly.NewSubtreeChanBuffer)

	// retry channel for subtrees that failed to be stored
	subtreeRetryChan := make(chan *subtreeRetrySend, ba.settings.BlockAssembly.SubtreeRetryChanBuffer)

	// init the block assembler for this server
	ba.blockAssembler, err = NewBlockAssembler(ctx, ba.logger, ba.settings, ba.stats, ba.utxoStore, ba.subtreeStore, ba.blockchainClient, newSubtreeChan)
	if err != nil {
		return errors.NewServiceError("failed to init block assembler", err)
	}

	// Apply the skip flag if it was set before Init
	if ba.skipWaitForPendingBlocks {
		ba.blockAssembler.SetSkipWaitForPendingBlocks(true)
	}

	// start background processors
	go ba.runSubtreeRetryProcessor(ctx, subtreeRetryChan)
	go ba.runNewSubtreeListener(ctx, newSubtreeChan, subtreeRetryChan)
	go ba.runBlockSubmissionListener(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				ba.logger.Infof("Stopping block assembler metrics updater")
				return
			case <-time.After(5 * time.Second):
				prometheusBlockAssemblerTransactions.Set(float64(ba.blockAssembler.TxCount()))
				prometheusBlockAssemblerQueuedTransactions.Set(float64(ba.blockAssembler.QueueLength()))
				prometheusBlockAssemblerSubtrees.Set(float64(ba.blockAssembler.SubtreeCount()))
			}
		}
	}()

	return nil
}

// GetBlockAssembler returns the BlockAssembler instance.
func (ba *BlockAssembly) GetBlockAssembler() *BlockAssembler {
	return ba.blockAssembler
}

// runSubtreeRetryProcessor handles retry logic for failed subtree storage operations.
// It processes subtrees that failed to be stored and retries them with exponential backoff.
func (ba *BlockAssembly) runSubtreeRetryProcessor(ctx context.Context, subtreeRetryChan chan *subtreeRetrySend) {
	for {
		select {
		case <-ctx.Done():
			ba.logger.Infof("Stopping subtree retry processor")
			return
		case subtreeRetry := <-subtreeRetryChan:
			ba.processSubtreeRetry(ctx, subtreeRetry, subtreeRetryChan)
		}
	}
}

// subtreeStorageWork represents a unit of work for subtree storage workers.
type subtreeStorageWork struct {
	seq      uint64                             // sequence number for ordering notifications
	request  subtreeprocessor.NewSubtreeRequest // the subtree request to process
	doneChan chan *subtreeStorageResult         // channel to signal completion
}

// subtreeStorageResult represents the result of a subtree storage operation.
type subtreeStorageResult struct {
	seq              uint64                             // sequence number for ordering
	request          subtreeprocessor.NewSubtreeRequest // original request
	err              error                              // error from storage operation
	skipNotification bool                               // whether notification should be skipped
	storedOK         bool                               // true if subtree was stored successfully
}

// runNewSubtreeListener handles incoming requests for new subtrees.
// It uses a worker pool for parallel storage and ensures notifications are sent in order.
func (ba *BlockAssembly) runNewSubtreeListener(ctx context.Context, newSubtreeChan <-chan subtreeprocessor.NewSubtreeRequest, subtreeRetryChan chan *subtreeRetrySend) {
	numWorkers := ba.settings.BlockAssembly.SubtreeStorageWorkers
	if numWorkers <= 0 {
		numWorkers = 4
	}

	workChan := make(chan *subtreeStorageWork, numWorkers*2)
	resultChan := make(chan *subtreeStorageResult, numWorkers*2)

	// Start storage workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ba.subtreeStorageWorker(ctx, workChan, subtreeRetryChan, resultChan)
		}()
	}

	// Start notification sender (processes results in order)
	notifyDone := make(chan struct{})
	go func() {
		defer close(notifyDone)
		ba.subtreeNotificationSender(ctx, resultChan)
	}()

	var seq uint64
	for {
		select {
		case <-ctx.Done():
			ba.logger.Infof("Stopping subtree listener")
			close(workChan)
			wg.Wait()
			close(resultChan)
			<-notifyDone
			return

		case newSubtreeRequest := <-newSubtreeChan:
			ba.logger.Infof("[runNewSubtreeListener][%s] New subtree request: %d", newSubtreeRequest.Subtree.RootHash().String(), seq)
			work := &subtreeStorageWork{
				seq:     seq,
				request: newSubtreeRequest,
			}
			seq++

			select {
			case workChan <- work:
			case <-ctx.Done():
				return
			}
		}
	}
}

// subtreeStorageWorker processes subtree storage work items in parallel.
func (ba *BlockAssembly) subtreeStorageWorker(ctx context.Context, workChan <-chan *subtreeStorageWork, subtreeRetryChan chan *subtreeRetrySend, resultChan chan<- *subtreeStorageResult) {
	for work := range workChan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result := &subtreeStorageResult{
			seq:              work.seq,
			request:          work.request,
			skipNotification: work.request.SkipNotification,
		}

		ba.logger.Infof("[subtreeStorageWorker][%s] Storing subtree (seq=%d)", work.request.Subtree.RootHash().String(), work.seq)

		// Store subtree and meta
		subtreeDone, allDone, err := ba.storeSubtreeData(ctx, work.request, subtreeRetryChan)
		result.err = err

		if err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				// Already exists is success for notification purposes
				result.storedOK = true
				result.err = nil
			} else {
				ba.logger.Errorf("%s", err.Error())
				result.storedOK = false
			}
		} else {
			// Wait for subtree storage to complete (notification can be sent)
			result.storedOK = <-subtreeDone
		}

		// Send result for ordered notification processing
		select {
		case resultChan <- result:
		case <-ctx.Done():
			return
		}

		// Wait for all work to complete before sending response to caller
		if allDone != nil {
			<-allDone
		}

		// Send error back to caller if channel exists
		if work.request.ErrChan != nil {
			work.request.ErrChan <- result.err
		}
	}
}

// subtreeNotificationSender sends subtree notifications in order after storage completes.
func (ba *BlockAssembly) subtreeNotificationSender(ctx context.Context, resultChan <-chan *subtreeStorageResult) {
	pending := make(map[uint64]*subtreeStorageResult)
	var nextSeq uint64

	for result := range resultChan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pending[result.seq] = result

		// Process all consecutive results we have
		for {
			r, ok := pending[nextSeq]
			if !ok {
				break
			}
			delete(pending, nextSeq)
			nextSeq++

			// Send notification if needed
			if !r.skipNotification && r.storedOK {
				ba.logger.Infof("[BlockAssembly:Init][%s] sending subtree notification", r.request.Subtree.RootHash().String())
				ba.sendSubtreeNotification(ctx, *r.request.Subtree.RootHash())
			} else {
				ba.logger.Infof("[BlockAssembly:Init][%s] skipping subtree notification (skip=%v, stored=%v)", r.request.Subtree.RootHash().String(), r.skipNotification, r.storedOK)
			}
		}
	}
}

// runBlockSubmissionListener handles incoming block submission requests.
// It processes mining solutions and submits validated blocks to the blockchain.
func (ba *BlockAssembly) runBlockSubmissionListener(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			ba.logger.Infof("Stopping block submission listener")
			return

		case blockSubmission := <-ba.blockSubmissionChan:
			_, err := ba.submitMiningSolution(ctx, blockSubmission)
			if err != nil {
				ba.logger.Warnf("Failed to submit block for job id %s %+v", chainhash.Hash(blockSubmission.Id), err)
			}

			if blockSubmission.responseChan != nil {
				blockSubmission.responseChan <- err
			}

			prometheusBlockAssemblySubmitMiningSolutionCh.Set(float64(len(ba.blockSubmissionChan)))
		}
	}
}

// processSubtreeRetry processes a single subtree retry request, handling both meta and subtree data storage.
func (ba *BlockAssembly) processSubtreeRetry(ctx context.Context, subtreeRetry *subtreeRetrySend, subtreeRetryChan chan *subtreeRetrySend) {
	dah := ba.blockAssembler.utxoStore.GetBlockHeight() + ba.settings.GlobalBlockHeightRetention

	// Store subtree meta if present
	if len(subtreeRetry.subtreeMetaBytes) > 0 {
		if err := ba.storeSubtreeMetaWithRetry(ctx, subtreeRetry, subtreeRetryChan, dah); err != nil {
			return
		}
	}

	// Store subtree data
	if err := ba.storeSubtreeDataWithRetry(ctx, subtreeRetry, subtreeRetryChan, dah); err != nil {
		return
	}

	isRunning, err := ba.blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateRUNNING)
	if err != nil {
		ba.logger.Errorf("[BlockAssembly:Init][%s] failed to check FSM state: %s", subtreeRetry.subtreeHash.String(), err)
		return
	}

	if !isRunning {
		ba.logger.Debugf("[BlockAssembly:Init][%s] FSM is not running, skipping notification", subtreeRetry.subtreeHash.String())
		return
	}

	// Send notification after successful storage
	ba.sendSubtreeNotification(ctx, subtreeRetry.subtreeHash)
}

// storeSubtreeMetaWithRetry attempts to store subtree metadata with retry logic.
func (ba *BlockAssembly) storeSubtreeMetaWithRetry(ctx context.Context, subtreeRetry *subtreeRetrySend, subtreeRetryChan chan *subtreeRetrySend, dah uint32) error {
	err := ba.subtreeStore.Set(ctx,
		subtreeRetry.subtreeHash[:],
		fileformat.FileTypeSubtreeMeta,
		subtreeRetry.subtreeMetaBytes,
		options.WithDeleteAt(dah),
	)

	if err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			ba.logger.Debugf("[BlockAssembly:Init][%s] subtreeRetryChan: subtree meta already exists", subtreeRetry.subtreeHash.String())
		} else {
			ba.logger.Errorf("[BlockAssembly:Init][%s] subtreeRetryChan: failed to retry store subtree meta: %s", subtreeRetry.subtreeHash.String(), err)
			ba.handleRetryLogic(ctx, subtreeRetry, subtreeRetryChan, "subtree meta")
		}
	}

	return err
}

// storeSubtreeDataWithRetry attempts to store subtree data with retry logic.
func (ba *BlockAssembly) storeSubtreeDataWithRetry(ctx context.Context, subtreeRetry *subtreeRetrySend, subtreeRetryChan chan *subtreeRetrySend, dah uint32) error {
	err := ba.subtreeStore.Set(ctx,
		subtreeRetry.subtreeHash[:],
		fileformat.FileTypeSubtree,
		subtreeRetry.subtreeBytes,
		options.WithDeleteAt(dah),
	)

	if err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			ba.logger.Debugf("[BlockAssembly:Init][%s] subtreeRetryChan: subtree already exists", subtreeRetry.subtreeHash.String())
		} else {
			ba.logger.Errorf("[BlockAssembly:Init][%s] subtreeRetryChan: failed to retry store subtree: %s", subtreeRetry.subtreeHash.String(), err)
			ba.handleRetryLogic(ctx, subtreeRetry, subtreeRetryChan, "subtree")
		}
	}

	return err
}

// handleRetryLogic manages the retry logic for failed storage operations.
func (ba *BlockAssembly) handleRetryLogic(ctx context.Context, subtreeRetry *subtreeRetrySend, subtreeRetryChan chan *subtreeRetrySend, itemType string) {
	if subtreeRetry.retries > 10 {
		ba.logger.Errorf("[BlockAssembly:Init][%s] subtreeRetryChan: failed to retry store %s, retries exhausted", subtreeRetry.subtreeHash.String(), itemType)
		return
	}

	subtreeRetry.retries++
	go func() {
		// backoff and wait before re-adding to retry queue
		if err := retry.BackoffAndSleep(ctx, subtreeRetry.retries, 2, time.Second); err != nil {
			ba.logger.Errorf("[BlockAssembly:Init][%s] subtreeRetryChan: context cancelled", subtreeRetry.subtreeHash.String())
			return
		}

		// re-add the subtree to the retry queue
		subtreeRetryChan <- subtreeRetry
	}()
}

// sendSubtreeNotification sends a notification about a successfully stored subtree.
// It only sends the notification if the FSM is in the running state.
func (ba *BlockAssembly) sendSubtreeNotification(ctx context.Context, subtreeHash chainhash.Hash) {
	isRunning, err := ba.blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateRUNNING)
	if err != nil {
		ba.logger.Errorf("[BlockAssembly:sendSubtreeNotification][%s] failed to get current state: %s", subtreeHash.String(), err)
		return
	}

	if !isRunning {
		return
	}

	if err = ba.blockchainClient.SendNotification(ctx, &blockchain.Notification{
		Type:     model.NotificationType_Subtree,
		Hash:     (&subtreeHash)[:],
		Base_URL: "",
		Metadata: &blockchain.NotificationMetadata{
			Metadata: nil,
		},
	}); err != nil {
		ba.logger.Errorf("[BlockAssembly:sendSubtreeNotification][%s] failed to send subtree notification: %s", subtreeHash.String(), err)
	}
}

// storeSubtreeData stores the subtree data and metadata to the blob store.
// It returns two channels:
//   - subtreeDone: signals when subtree storage is complete (bool = success)
//   - allDone: signals when all work including meta storage is complete
//
// Parameters:
//   - ctx: Context for the storage operation
//   - subtreeRequest: Request containing the subtree to store and associated metadata
//   - subtreeRetryChan: Channel for queuing failed storage attempts for retry
//
// Returns:
//   - subtreeDone: Channel that sends true when subtree stored OK, false on failure
//   - allDone: Channel that closes when all work is complete
//   - err: Any error encountered during setup
func (ba *BlockAssembly) storeSubtreeData(ctx context.Context, subtreeRequest subtreeprocessor.NewSubtreeRequest, subtreeRetryChan chan *subtreeRetrySend) (subtreeDone <-chan bool, allDone <-chan struct{}, err error) {
	subtree := subtreeRequest.Subtree

	ctx, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "storeSubtreeData",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblerSubtreeStoredHist),
		tracing.WithLogMessage(ba.logger, "[BlockAssembly:storeSubtreeData][%s] storing subtree: len %d", subtree.RootHash().String(), subtree.Length()),
	)
	defer deferFn()

	// Check whether this subtree already exists in the store
	if ok, _ := ba.subtreeStore.Exists(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree); ok {
		ba.logger.Debugf("[BlockAssembly:storeSubtreeData][%s] subtree already exists", subtree.RootHash().String())
		return nil, nil, errors.ErrBlobAlreadyExists
	}

	// Serialize subtree
	subtreeBytes, err := subtree.Serialize()
	if err != nil {
		return nil, nil, errors.NewProcessingError("[BlockAssembly:storeSubtreeData][%s] failed to serialize subtree", subtree.RootHash().String(), err)
	}

	dah := ba.blockAssembler.utxoStore.GetBlockHeight() + ba.settings.GlobalBlockHeightRetention

	subtreeDoneCh := make(chan bool, 1)
	subtreeStorageDone := make(chan struct{}) // Separate signal for coordinator
	metaDoneCh := make(chan struct{})
	allDoneCh := make(chan struct{})

	// Build, serialize, and store subtree meta in background
	if subtreeRequest.ParentTxMap != nil && ba.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta {
		go func() {
			defer close(metaDoneCh)
			subtreeMeta := subtreepkg.NewSubtreeMeta(subtreeRequest.Subtree)

			for idx, node := range subtreeRequest.Subtree.Nodes {
				if !node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
					txInpoints, found := subtreeRequest.ParentTxMap.Get(node.Hash)
					if !found {
						ba.logger.Errorf("[BlockAssembly:storeSubtreeData][%s] failed to find parent tx hashes for node %s: parent transaction not found in ParentTxMap", subtreeRequest.Subtree.RootHash().String(), node.Hash.String())
						return
					}

					if err := subtreeMeta.SetTxInpoints(idx, *txInpoints); err != nil {
						ba.logger.Errorf("[BlockAssembly:storeSubtreeData][%s] failed to set parent tx hashes: %s", node.Hash.String(), err)
						return
					}
				}
			}

			subtreeMetaBytes, err := subtreeMeta.Serialize()
			if err != nil {
				ba.logger.Errorf("[BlockAssembly:storeSubtreeData][%s] failed to serialize subtree meta: %s", subtree.RootHash().String(), err)
				return
			}

			if err := ba.subtreeStore.Set(ctx,
				subtree.RootHash()[:],
				fileformat.FileTypeSubtreeMeta,
				subtreeMetaBytes,
				options.WithDeleteAt(dah),
			); err != nil {
				if errors.Is(err, errors.ErrBlobAlreadyExists) {
					ba.logger.Debugf("[BlockAssembly:storeSubtreeData][%s] subtree meta already exists", subtree.RootHash().String())
				} else {
					ba.logger.Errorf("[BlockAssembly:storeSubtreeData][%s] failed to store subtree meta: %s", subtree.RootHash().String(), err)
					subtreeRetryChan <- &subtreeRetrySend{
						subtreeHash:      *subtree.RootHash(),
						subtreeBytes:     subtreeBytes,
						subtreeMetaBytes: subtreeMetaBytes,
						retries:          0,
					}
				}
			}
		}()
	} else {
		close(metaDoneCh)
	}

	// Store subtree in background
	go func() {
		defer close(subtreeStorageDone) // Signal coordinator when done
		storedOK := true
		if err := ba.subtreeStore.Set(ctx,
			subtree.RootHash()[:],
			fileformat.FileTypeSubtree,
			subtreeBytes,
			options.WithDeleteAt(dah),
		); err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				ba.logger.Debugf("[BlockAssembly:storeSubtreeData][%s] subtree already exists", subtree.RootHash().String())
			} else {
				ba.logger.Errorf("[BlockAssembly:storeSubtreeData][%s] failed to store subtree: %s", subtree.RootHash().String(), err)
				subtreeRetryChan <- &subtreeRetrySend{
					subtreeHash:  *subtree.RootHash(),
					subtreeBytes: subtreeBytes,
					retries:      0,
				}
				storedOK = false
			}
		}
		subtreeDoneCh <- storedOK
		close(subtreeDoneCh)
	}()

	// Coordinator: wait for both and signal all done
	// Note: We wait on subtreeStorageDone (not subtreeDoneCh) to avoid racing
	// with the worker that reads the storedOK value from subtreeDoneCh.
	go func() {
		defer close(allDoneCh)
		<-subtreeStorageDone
		<-metaDoneCh
	}()

	return subtreeDoneCh, allDoneCh, nil
}

// Start begins the BlockAssembly service operation.
//
// This method initializes and starts the BlockAssembly service by:
// 1. Setting up gRPC server in a separate goroutine
// 2. Starting the block assembler component
// 3. Waiting for either successful startup or gRPC server termination
//
// The function implements a robust startup pattern with proper synchronization
// to handle both successful initialization and error conditions gracefully.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - readyCh: Channel to signal when the service is ready to accept requests
//
// Returns:
//   - error: Any error encountered during startup or gRPC server operation
func (ba *BlockAssembly) Start(ctx context.Context, readyCh chan<- struct{}) (err error) {
	var (
		// closeOnce ensures the readyCh is closed exactly once, preventing panics
		// from multiple close attempts during concurrent startup scenarios
		closeOnce sync.Once

		// grpcReady channel signals when the gRPC server is ready to accept requests
		grpcReady = make(chan struct{})
	)

	// Defer closing readyCh to ensure it's always closed, even if startup fails
	// This prevents callers from waiting indefinitely for service readiness
	defer closeOnce.Do(func() { close(readyCh) })

	// Create errgroup for coordinating goroutines
	g, gCtx := errgroup.WithContext(ctx)

	// Start gRPC server in errgroup to properly handle shutdown
	g.Go(func() error {
		// StartGRPCServer blocks until the server shuts down or encounters an error
		// The server setup includes registering the BlockAssemblyAPI service
		return util.StartGRPCServer(gCtx, ba.logger, ba.settings, "blockassembly", ba.settings.BlockAssembly.GRPCListenAddress, func(server *grpc.Server) {
			// Register the BlockAssembly service with the gRPC server
			// This makes all BlockAssembly API methods available to clients
			blockassembly_api.RegisterBlockAssemblyAPIServer(server, ba)

			// Signal that the service is ready to accept requests
			// This is called once the gRPC server is successfully listening
			grpcReady <- struct{}{}
		}, nil)
	})

	<-grpcReady

	// This must succeed for the service to be functional
	if err = ba.blockAssembler.Start(ctx); err != nil {
		return errors.NewServiceError("failed to start block assembler", err)
	}

	// Signal that the service is ready to accept requests
	closeOnce.Do(func() { close(readyCh) }) // Start the block assembler component which handles the core block creation logic

	// Wait for gRPC server completion or error
	// This blocks until either:
	// 1. The gRPC server encounters an error and terminates
	// 2. The context is cancelled, causing graceful shutdown
	// 3. The server is explicitly stopped from elsewhere
	//
	// The function returns the error from gRPC server operation, which could be:
	// - nil if server shut down gracefully
	// - context cancellation error if shutdown was requested
	// - network or configuration errors if startup failed
	return g.Wait()
}

// Stop gracefully shuts down the BlockAssembly service.
// This method is idempotent and safe to call multiple times.
//
// Parameters:
//   - ctx: Context for cancellation (currently unused)
//
// Returns:
//   - error: Any error encountered during shutdown
func (ba *BlockAssembly) Stop(ctx context.Context) error {
	ba.stopOnce.Do(func() {
		ba.jobStore.Stop()

		// Stop the subtree processor to stop the announcement ticker and cleanup resources
		if ba.blockAssembler != nil && ba.blockAssembler.subtreeProcessor != nil {
			ba.blockAssembler.subtreeProcessor.Stop(ctx)
		}
	})

	return nil
}

// AddTx adds a transaction to the block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//   - req: Transaction addition request
//
// Returns:
//   - *blockassembly_api.AddTxResponse: Response indicating success
//   - error: Any error encountered during addition
func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (resp *blockassembly_api.AddTxResponse, err error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "AddTx",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblyAddTx),
		tracing.WithCounter(prometheusBlockAssemblyAddTxCounter),
		tracing.WithTag("txid", util.ReverseAndHexEncodeSlice(req.Txid)),
		tracing.WithLogMessage(ba.logger, "[AddTx][%s] add tx called", util.ReverseAndHexEncodeSlice(req.Txid)),
	)

	defer func() {
		deferFn()
	}()

	if len(req.Txid) != 32 {
		return nil, errors.WrapGRPC(
			errors.NewProcessingError("invalid txid length: %d for %s", len(req.Txid), util.ReverseAndHexEncodeSlice(req.Txid)))
	}

	ba.logger.Debugf("[AddTx] added tx %s to block assembler", chainhash.Hash(req.Txid).String())

	var txInpoints subtreepkg.TxInpoints
	if ba.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta {
		txInpoints, err = subtreepkg.NewTxInpointsFromBytes(req.TxInpoints)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewProcessingError("unable to deserialize tx inpoints", err))
		}
	} else {
		// Create empty TxInpoints if not storing for subtree meta
		txInpoints = subtreepkg.TxInpoints{}
	}

	if !ba.settings.BlockAssembly.Disabled {
		ba.blockAssembler.AddTxBatch(
			[]subtreepkg.Node{{Hash: chainhash.Hash(req.Txid), Fee: req.Fee, SizeInBytes: req.Size}},
			[]*subtreepkg.TxInpoints{&txInpoints},
		)
	}

	return &blockassembly_api.AddTxResponse{
		Ok: true,
	}, nil
}

// RemoveTx removes a transaction from the block assembly process.
// This method handles the removal of transactions that should no longer be
// considered for inclusion in future blocks. This can occur when transactions
// become invalid, are double-spent, or need to be excluded for other reasons.
//
// The function performs the following operations:
// - Validates the transaction ID format and length
// - Deserializes transaction input points for proper identification
// - Removes the transaction from the block assembler's active set
// - Updates internal metrics and logging for monitoring
//
// Transaction removal is important for maintaining the integrity of the block
// assembly process and ensuring that only valid, current transactions are
// included in mining candidates.
//
// Parameters:
//   - ctx: Context for the removal operation, allowing for cancellation and tracing
//   - req: Request containing the transaction ID and input points to remove
//
// Returns:
//   - *blockassembly_api.EmptyMessage: Empty response indicating successful removal
//   - error: Any error encountered during transaction removal or validation
func (ba *BlockAssembly) RemoveTx(ctx context.Context, req *blockassembly_api.RemoveTxRequest) (*blockassembly_api.EmptyMessage, error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "RemoveTx",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblyRemoveTx),
		tracing.WithLogMessage(ba.logger, "[RemoveTx][%s] called", util.ReverseAndHexEncodeSlice(req.Txid)),
	)
	defer deferFn()

	if len(req.Txid) != 32 {
		return nil, errors.WrapGRPC(
			errors.NewProcessingError("invalid txid length: %d for %s", len(req.Txid), util.ReverseAndHexEncodeSlice(req.Txid)))
	}

	hash := chainhash.Hash(req.Txid)

	if !ba.settings.BlockAssembly.Disabled {
		if err := ba.blockAssembler.RemoveTx(ctx, hash); err != nil {
			return nil, errors.WrapGRPC(err)
		}
	}

	return &blockassembly_api.EmptyMessage{}, nil
}

// AddTxBatch processes a batch of transactions for block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//   - batch: Batch of transactions to process
//
// Returns:
//   - *blockassembly_api.AddTxBatchResponse: Response indicating success
//   - error: Any error encountered during batch processing
func (ba *BlockAssembly) AddTxBatch(ctx context.Context, batch *blockassembly_api.AddTxBatchRequest) (*blockassembly_api.AddTxBatchResponse, error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "AddTxBatch",
		tracing.WithParentStat(ba.stats),
		tracing.WithDebugLogMessage(ba.logger, "[AddTxBatch] called with %d transactions", len(batch.GetTxRequests())),
	)
	defer func() {
		deferFn()
	}()

	requests := batch.GetTxRequests()
	if len(requests) == 0 {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("no tx requests in batch"))
	}

	// Build batch arrays
	nodes := make([]subtreepkg.Node, len(requests))
	txInpointsList := make([]*subtreepkg.TxInpoints, len(requests))

	var err error

	for i, req := range requests {
		var txInpoints subtreepkg.TxInpoints
		if ba.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta {
			txInpoints, err = subtreepkg.NewTxInpointsFromBytes(req.TxInpoints)
			if err != nil {
				return nil, errors.WrapGRPC(errors.NewProcessingError("unable to deserialize tx inpoints", err))
			}
		} else {
			// Create empty TxInpoints if not storing for subtree meta
			txInpoints = subtreepkg.TxInpoints{}
		}

		nodes[i] = subtreepkg.Node{
			Hash:        chainhash.Hash(req.Txid),
			Fee:         req.Fee,
			SizeInBytes: req.Size,
		}
		txInpointsList[i] = &txInpoints
	}

	prometheusBlockAssemblyAddTxCounter.Add(float64(len(nodes))) // gosec:nolint

	// Add entire batch in one call
	if !ba.settings.BlockAssembly.Disabled {
		ba.blockAssembler.AddTxBatch(nodes, txInpointsList)
	}

	return &blockassembly_api.AddTxBatchResponse{Ok: true}, nil
}

// AddTxBatchColumnar processes a batch of transactions using columnar data format.
// This method provides improved performance over AddTxBatch by reducing deserialization
// overhead and GC pressure through better memory locality and fewer allocations.
//
// OPTIMIZATION: TxInpoints are now pre-deserialized by the Client (which runs on multiple machines)
// and sent in columnar format. The Server reconstructs TxInpoints directly from columnar data
// WITHOUT calling NewTxInpointsFromBytes, eliminating deserialization work on the single-machine Server.
//
// The columnar format stores all transaction data in packed arrays:
// - All TXIDs in a single byte slice (32 bytes each)
// - All fees in a single array
// - All sizes in a single array
// - All parent tx hashes concatenated
// - Offset tables for parent hashes and vout indices
//
// Expected performance improvements:
// - Eliminates per-transaction TxInpoints deserialization on Server
// - Shifts deserialization work to distributed Clients
// - Reduces Server CPU usage significantly
// - Lower GC pressure from fewer intermediate allocations
//
// Parameters:
//   - ctx: Context for cancellation
//   - req: Columnar batch request with packed transaction data
//
// Returns:
//   - *blockassembly_api.AddTxBatchResponse: Response indicating success
//   - error: Any error encountered during batch processing
func (ba *BlockAssembly) AddTxBatchColumnar(ctx context.Context, req *blockassembly_api.AddTxBatchColumnarRequest) (*blockassembly_api.AddTxBatchResponse, error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "AddTxBatchColumnar",
		tracing.WithParentStat(ba.stats),
		tracing.WithDebugLogMessage(ba.logger, "[AddTxBatchColumnar] called with columnar batch"),
	)
	defer func() {
		deferFn()
	}()

	// Validate request structure
	if len(req.TxidsPacked)%32 != 0 {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("txids_packed length must be divisible by 32"))
	}

	txCount := len(req.TxidsPacked) / 32
	if txCount == 0 {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("no transactions in batch"))
	}

	if len(req.Fees) != txCount || len(req.Sizes) != txCount {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("mismatched array lengths: txids=%d, fees=%d, sizes=%d", txCount, len(req.Fees), len(req.Sizes)))
	}

	// Validate columnar TxInpoints structure
	if len(req.ParentTxOffsets) != txCount+1 {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError(
			"parent_tx_offsets must have exactly txCount+1 elements (got %d, expected %d)",
			len(req.ParentTxOffsets), txCount+1))
	}

	if len(req.ParentTxHashesPacked)%32 != 0 {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("parent_tx_hashes_packed length must be divisible by 32"))
	}

	totalParentHashes := len(req.ParentTxHashesPacked) / 32
	if len(req.VoutIdxOffsets) != totalParentHashes+1 {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("vout_idx_offsets must have exactly (total_parent_hashes+1) elements (got %d, expected %d)", len(req.VoutIdxOffsets), totalParentHashes+1))
	}

	if ba.settings.BlockAssembly.Disabled {
		return &blockassembly_api.AddTxBatchResponse{Ok: true}, nil
	}

	// Build batch arrays
	nodes := make([]subtreepkg.Node, txCount)
	txInpointsList := make([]*subtreepkg.TxInpoints, txCount)

	// Process each transaction using column-oriented access
	for i := 0; i < txCount; i++ {
		// Extract TXID (32 bytes) - no allocation, just slice reference
		txidStart := i * 32
		txid := req.TxidsPacked[txidStart : txidStart+32]

		// Reconstruct TxInpoints from columnar data WITHOUT deserialization
		// This is the key optimization - we build TxInpoints directly from pre-parsed data
		parentHashStart := req.ParentTxOffsets[i]
		parentHashEnd := req.ParentTxOffsets[i+1]
		numParentHashes := parentHashEnd - parentHashStart

		// Pre-allocate slices with exact capacity to avoid reallocation
		parentTxHashes := make([]chainhash.Hash, numParentHashes)
		idxs := make([][]uint32, numParentHashes)

		for j := uint32(0); j < numParentHashes; j++ {
			parentHashIdx := parentHashStart + j

			// Extract parent hash (32 bytes) - no allocation, direct copy
			hashOffset := parentHashIdx * 32
			copy(parentTxHashes[j][:], req.ParentTxHashesPacked[hashOffset:hashOffset+32])

			// Extract vout indices for this parent hash
			voutIdxStart := req.VoutIdxOffsets[parentHashIdx]
			voutIdxEnd := req.VoutIdxOffsets[parentHashIdx+1]

			// Reference the vout indices slice directly - no allocation
			idxs[j] = req.ParentVoutIndices[voutIdxStart:voutIdxEnd]
		}

		// Build node and txInpoints for this transaction
		nodes[i] = subtreepkg.Node{
			Hash:        chainhash.Hash(txid),
			Fee:         req.Fees[i],
			SizeInBytes: req.Sizes[i],
		}

		if ba.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta {
			txInpointsList[i] = &subtreepkg.TxInpoints{
				ParentTxHashes: parentTxHashes,
				Idxs:           idxs,
			}
		} else {
			txInpointsList[i] = &subtreepkg.TxInpoints{}
		}
	}

	prometheusBlockAssemblyAddTxCounter.Add(float64(len(nodes))) // gosec:nolint

	// Add entire batch in one call
	if !ba.settings.BlockAssembly.Disabled {
		ba.blockAssembler.AddTxBatch(nodes, txInpointsList)
	}

	return &blockassembly_api.AddTxBatchResponse{Ok: true}, nil
}

// TxCount returns the total number of transactions processed.
//
// Returns:
//   - uint64: Total transaction count
func (ba *BlockAssembly) TxCount() uint64 {
	return ba.blockAssembler.TxCount()
}

// GetMiningCandidate retrieves a candidate block for mining.
//
// Parameters:
//   - ctx: Context for cancellation
//   - req: Empty message request
//
// Returns:
//   - *model.MiningCandidate: Mining candidate block
//   - error: Any error encountered during retrieval
func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, req *blockassembly_api.GetMiningCandidateRequest) (*model.MiningCandidate, error) {
	ctx, _, endSpan := tracing.Tracer("blockassembly").Start(ctx, "GetMiningCandidate",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblyGetMiningCandidateDuration),
		tracing.WithLogMessage(ba.logger, "[GetMiningCandidate] called"),
	)
	defer endSpan()

	isRunning, err := ba.blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateRUNNING)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	if !isRunning {
		return nil, errors.WrapGRPC(errors.NewStateError("cannot get mining candidate when FSM is not in RUNNING state"))
	}

	includeSubtreeHashes := req.IncludeSubtrees

	miningCandidate, subtrees, err := ba.blockAssembler.GetMiningCandidate(ctx)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	ba.logger.Debugf("in GetMiningCandidate: miningCandidate: %+v", miningCandidate.Stringify(true))

	id, _ := chainhash.NewHash(miningCandidate.Id)

	ba.jobStore.Set(*id, &subtreeprocessor.Job{
		ID:              id,
		Subtrees:        subtrees,
		MiningCandidate: miningCandidate,
	}, jobTTL) // create a new job with a TTL, will be cleaned up automatically

	if includeSubtreeHashes {
		miningCandidate.SubtreeHashes = make([][]byte, len(subtrees))
		for i, subtree := range subtrees {
			miningCandidate.SubtreeHashes[i] = subtree.RootHash()[:]
		}
	}

	ba.logger.Infof("[GetMiningCandidate][%s] returning mining candidate with %d transactions, %d subtrees, total size %d bytes",
		util.ReverseAndHexEncodeSlice(miningCandidate.Id),
		miningCandidate.NumTxs+1, // +1 for coinbase
		len(miningCandidate.SubtreeHashes),
		miningCandidate.SizeWithoutCoinbase,
	)

	return miningCandidate, nil
}

// SubmitMiningSolution processes a mining solution submission.
// It validates the solution, creates a block, and adds it to the blockchain.
//
// Parameters:
//   - ctx: Context for cancellation
//   - req: Mining solution submission request
//
// Returns:
//   - *blockassembly_api.SubmitMiningSolutionResponse: Submission response
//   - error: Any error encountered during submission processing
func (ba *BlockAssembly) SubmitMiningSolution(ctx context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.OKResponse, error) {
	_, _, endSpan := tracing.Tracer("blockassembly").Start(ctx, "SubmitMiningSolution",
		tracing.WithParentStat(ba.stats),
		tracing.WithLogMessage(ba.logger, "[SubmitMiningSolution] called"),
	)
	defer endSpan()

	// Check if unmined transactions are still being loaded
	if ba.blockAssembler.unminedTransactionsLoading.Load() {
		ba.logger.Warnf("[SubmitMiningSolution] service not ready - unmined transactions are still being loaded")
		return nil, errors.NewServiceError("service not ready - unmined transactions are still being loaded")
	}

	var responseChan chan error

	if ba.settings.BlockAssembly.SubmitMiningSolutionWaitForResponse {
		responseChan = make(chan error)
		defer close(responseChan)
	}

	// we don't have the processing to handle multiple huge blocks at the same time, so we limit it to 1
	// at a time, this is a temporary solution for now
	request := &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: req,
		responseChan:                responseChan,
	}

	ba.blockSubmissionChan <- request

	var err error

	if ba.settings.BlockAssembly.SubmitMiningSolutionWaitForResponse {
		err = <-request.responseChan
	}

	if err != nil {
		err = errors.WrapGRPC(err)
	}

	return &blockassembly_api.OKResponse{
		Ok: err == nil, // The response only has Ok boolean in it.  If waitForResponse is false, err will always be nil.
	}, err
}

func (ba *BlockAssembly) submitMiningSolution(ctx context.Context, req *BlockSubmissionRequest) (*blockassembly_api.OKResponse, error) {
	jobID := util.ReverseAndHexEncodeSlice(req.SubmitMiningSolutionRequest.Id)

	ctx, _, endSpan := tracing.Tracer("blockassembly").Start(ctx, "submitMiningSolution",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblySubmitMiningSolution),
		tracing.WithLogMessage(ba.logger, "[submitMiningSolution] called for job id %s", jobID),
	)

	defer endSpan()

	storeID, err := chainhash.NewHash(req.SubmitMiningSolutionRequest.Id)
	if err != nil {
		return nil, err
	}

	jobItem := ba.jobStore.Get(*storeID)
	if jobItem == nil {
		return nil, errors.NewNotFoundError("[BlockAssembly][%s] job not found", jobID)
	}

	job := jobItem.Value()

	hashPrevBlock, err := chainhash.NewHash(job.MiningCandidate.PreviousHash)
	if err != nil {
		return nil, errors.NewProcessingError("[BlockAssembly][%s] failed to convert hashPrevBlock", jobID, err)
	}

	bestBlockHeader, _ := ba.blockAssembler.CurrentBlock()
	if bestBlockHeader.HashPrevBlock.IsEqual(hashPrevBlock) {
		return nil, errors.NewProcessingError("[BlockAssembly][%s] already mining on top of the same block that is submitted", jobID)
	}

	var coinbaseTx *bt.Tx

	if req.CoinbaseTx != nil {
		coinbaseTx, err = bt.NewTxFromBytes(req.CoinbaseTx)
		if err != nil {
			return nil, errors.NewProcessingError("[BlockAssembly][%s] failed to convert coinbaseTx", jobID, err)
		}

		// Validate coinbase has exactly one input before accessing Inputs[0]
		if len(coinbaseTx.Inputs) != 1 {
			return nil, errors.NewProcessingError("[BlockAssembly][%s] coinbase transaction must have exactly one input, got %d", jobID, len(coinbaseTx.Inputs))
		}

		if len(coinbaseTx.Inputs[0].UnlockingScript.Bytes()) < 2 || len(coinbaseTx.Inputs[0].UnlockingScript.Bytes()) > int(ba.blockAssembler.settings.ChainCfgParams.MaxCoinbaseScriptSigSize) {
			return nil, errors.NewProcessingError("[BlockAssembly][%s] bad coinbase length", jobID)
		}
	} else {
		// recreate coinbase tx here, nothing was passed in
		coinbaseTx, err = jobItem.Value().MiningCandidate.CreateCoinbaseTxCandidate(ba.blockAssembler.settings)
		if err != nil {
			return nil, errors.NewProcessingError("[BlockAssembly][%s] failed to create coinbase tx", jobID, err)
		}

		// set the new mining parameters on the coinbase (nonce)
		if req.Version != nil {
			coinbaseTx.Version = *req.Version
		}

		if req.Time != nil {
			coinbaseTx.LockTime = *req.Time
		}

		if req.Nonce != 0 {
			coinbaseTx.Inputs[0].SequenceNumber = req.Nonce
		}
	}

	// Final validation: ensure coinbase is valid (defense-in-depth)
	if len(coinbaseTx.Inputs) != 1 {
		return nil, errors.NewProcessingError("[BlockAssembly][%s] coinbase transaction must have exactly one input after processing, got %d", jobID, len(coinbaseTx.Inputs))
	}

	coinbaseTxIDHash := coinbaseTx.TxIDChainHash()

	var sizeInBytes uint64

	subtreesInJob := make([]*subtreepkg.Subtree, len(job.Subtrees))
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
		sizeInBytes = 0      // Don't double-count coinbase size, it's added later
	}

	hashMerkleRoot, err := ba.createMerkleTreeFromSubtrees(jobID, subtreesInJob, subtreeHashes, coinbaseTxIDHash)
	if err != nil {
		return nil, errors.NewProcessingError("[BlockAssembly][%s] failed to create merkle tree", jobID, err)
	}

	// sizeInBytes from the subtrees, 80 byte header and varint bytes for txcount
	blockSize := sizeInBytes + 80 + util.VarintSize(transactionCount)
	// add the size of the coinbase tx to the blocksize
	blockSize += uint64(coinbaseTx.Size())

	bits, err := model.NewNBitFromSlice(job.MiningCandidate.NBits)
	if err != nil {
		return nil, errors.NewProcessingError("[BlockAssembly][%s] failed to convert bits", jobID, err)
	}

	version := job.MiningCandidate.Version
	if req.Version != nil {
		version = *req.Version
	}

	nTime := job.MiningCandidate.Time
	if req.Time != nil {
		nTime = *req.Time
	}

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        version,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Timestamp:      nTime,
			Bits:           *bits,
			Nonce:          req.Nonce,
		},
		CoinbaseTx:       coinbaseTx,
		TransactionCount: transactionCount,
		SizeInBytes:      blockSize,
		Subtrees:         jobSubtreeHashes, // we need to store the hashes of the subtrees in the block, without the coinbase
		SubtreeSlices:    job.Subtrees,
	}

	// check fully valid, including whether difficulty in header is low enough
	// TODO add more checks to the Valid function, like whether the parent/child relationships are OK
	if ok, err := block.Valid(ctx, ba.logger, ba.subtreeStore, nil, nil, nil, nil, ba.settings, nil); !ok {
		ba.logger.Errorf("[BlockAssembly][%s][%s] invalid block: %v - %v", jobID, block.Hash().String(), block.Header, err)

		// the subtreeprocessor created an invalid block, we must reset
		ba.blockAssembler.Reset(false)

		// remove the job, we cannot use it anymore
		ba.jobStore.Delete(*storeID)

		return nil, errors.NewProcessingError("[BlockAssembly][%s][%s] invalid block", jobID, block.Hash().String(), err)
	}

	// decouple the tracing context to not cancel the context when the block is being saved, even if we cancel the request
	callerCtx, _, endSpan := tracing.DecoupleTracingSpan(ctx, "blockassembly", "decoupleBlockSaving")
	defer endSpan()

	if ba.txStore != nil {
		err = ba.txStore.Set(callerCtx, block.CoinbaseTx.TxIDChainHash().CloneBytes(), fileformat.FileTypeTx, block.CoinbaseTx.ExtendedBytes())
		if err != nil {
			ba.logger.Errorf("[BlockAssembly][%s][%s] error storing coinbase tx in tx store: %v", jobID, block.Hash().String(), err)
		}
	}

	ba.logger.Debugf("[BlockAssembly][%s][%s] add block to blockchain", jobID, block.Header.Hash())
	ba.logger.Debugf("[BlockAssembly][%s][%s] block difficulty: %s", jobID, block.Header.Hash(), block.Header.Bits.CalculateDifficulty().String())
	bestBlockHeader, _ = ba.blockAssembler.CurrentBlock()
	ba.logger.Debugf("[BlockAssembly][%s][%s] time since previous block: %s", jobID, block.Header.Hash(), time.Since(time.Unix(int64(bestBlockHeader.Timestamp), 0)).String())

	// add the new block to the blockchain
	if err = ba.blockchainClient.AddBlock(callerCtx, block, ""); err != nil {
		return nil, errors.NewProcessingError("[BlockAssembly][%s][%s] failed to add block", jobID, block.Hash().String(), err)
	}

	// Mark subtrees as set — block assembly built and validated these subtrees,
	// so they are ready for setTxMined processing. Without this, locally mined
	// blocks would never complete the mining lifecycle.
	if err = ba.blockchainClient.SetBlockSubtreesSet(callerCtx, block.Hash()); err != nil {
		ba.logger.Errorf("[BlockAssembly][%s][%s] failed to set block subtrees_set: %v", jobID, block.Header.Hash(), err)
	}

	// remove jobs, we have already mined a block
	// if we don't do this, all the subtrees will never be removed from memory
	ba.jobStore.DeleteAll()

	return &blockassembly_api.OKResponse{
		Ok: true,
	}, nil
}

func (ba *BlockAssembly) createMerkleTreeFromSubtrees(jobID string, subtreesInJob []*subtreepkg.Subtree, subtreeHashes []chainhash.Hash, coinbaseTxIDHash *chainhash.Hash) (*chainhash.Hash, error) {
	// Create a new subtree with the subtreeHashes of the subtrees
	topTree, err := subtreepkg.NewTreeByLeafCount(subtreepkg.CeilPowerOfTwo(len(subtreesInJob)))
	if err != nil {
		return nil, errors.NewProcessingError("[BlockAssembly][%s] failed to create topTree", jobID, err)
	}

	for _, hash := range subtreeHashes {
		if err = topTree.AddNode(hash, 1, 0); err != nil {
			return nil, errors.NewProcessingError("[BlockAssembly][%s] failed to add node to topTree", jobID, err)
		}
	}

	var (
		hashMerkleRoot *chainhash.Hash
	)

	if len(subtreesInJob) == 0 {
		hashMerkleRoot = coinbaseTxIDHash
	} else {
		calculatedMerkleRoot := topTree.RootHash()

		if hashMerkleRoot, err = chainhash.NewHash(calculatedMerkleRoot[:]); err != nil {
			return nil, errors.NewProcessingError("[BlockAssembly][%s] failed to convert hashMerkleRoot", jobID, err)
		}
	}

	return hashMerkleRoot, nil
}

// SubtreeCount returns the current number of subtrees managed by the block assembler.
// This method provides a real-time count of active subtrees that are available for
// inclusion in mining candidates. The count reflects the current state of the block
// assembly process and is used for monitoring, metrics, and operational visibility.
//
// The subtree count is an important indicator of the block assembler's current
// capacity and workload. It helps operators understand how many transaction
// groups are ready for block inclusion and can be used to monitor the health
// and performance of the transaction processing pipeline.
//
// Returns:
//   - int: The current number of active subtrees in the block assembler
func (ba *BlockAssembly) SubtreeCount() int {
	return ba.blockAssembler.SubtreeCount()
}

func (ba *BlockAssembly) ResetBlockAssembly(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "ResetBlockAssembly",
		tracing.WithParentStat(ba.stats),
		tracing.WithLogMessage(ba.logger, "[ResetBlockAssembly] called"),
	)
	defer deferFn()

	// Check if unmined transactions are still being loaded
	if ba.blockAssembler.unminedTransactionsLoading.Load() {
		ba.logger.Warnf("[ResetBlockAssembly] service not ready - unmined transactions are still being loaded")
		return nil, errors.NewServiceError("service not ready - unmined transactions are still being loaded")
	}

	ba.blockAssembler.Reset(false)

	return &blockassembly_api.EmptyMessage{}, nil
}

func (ba *BlockAssembly) ResetBlockAssemblyFully(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "ResetBlockAssemblyFully",
		tracing.WithParentStat(ba.stats),
		tracing.WithLogMessage(ba.logger, "[ResetBlockAssemblyFully] called"),
	)
	defer deferFn()

	// Check if unmined transactions are still being loaded
	if ba.blockAssembler.unminedTransactionsLoading.Load() {
		ba.logger.Warnf("[ResetBlockAssemblyFully] service not ready - unmined transactions are still being loaded")
		return nil, errors.NewServiceError("service not ready - unmined transactions are still being loaded")
	}

	ba.blockAssembler.Reset(true)

	return &blockassembly_api.EmptyMessage{}, nil
}

// GetBlockAssemblyState retrieves the current operational state of the block assembly service.
//
// This method provides comprehensive diagnostic information about the current state
// of the block assembly service and its components. It returns details including:
//   - The current operational state of both the block assembler and subtree processor
//   - Reset wait counters and timers
//   - Transaction and subtree counts
//   - Current blockchain tip information (height and hash)
//   - Transaction queue metrics
//
// This information is valuable for monitoring, debugging, and ensuring the service
// is operating correctly. It can be used both by automated monitoring systems and
// for manual troubleshooting.
//
// Parameters:
//   - ctx: Context for cancellation
//   - _: Empty message request (unused)
//
// Returns:
//   - *blockassembly_api.StateMessage: Detailed state information
//   - error: Any error encountered while gathering state information
func (ba *BlockAssembly) GetBlockAssemblyState(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.StateMessage, error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "GetBlockAssemblyState",
		tracing.WithParentStat(ba.stats),
		tracing.WithDebugLogMessage(ba.logger, "[GetBlockAssemblyState] called"),
	)
	defer deferFn()

	subtreeCountUint32, err := safeconversion.IntToUint32(ba.blockAssembler.SubtreeCount())
	if err != nil {
		return nil, errors.NewProcessingError("[GetBlockAssemblyState] error converting subtree count", err)
	}

	// this will block when the subtree processor is busy with someting else
	// wait only 1 second for this and continue
	subtreeHashesChan := make(chan []chainhash.Hash, 1)
	go func() {
		subtreeHashesChan <- ba.blockAssembler.subtreeProcessor.GetSubtreeHashes()
	}()

	var subtreeHashes []chainhash.Hash
	select {
	case subtreeHashes = <-subtreeHashesChan:
		// Successfully retrieved subtree hashes
	case <-time.After(1 * time.Second):
		// Timeout occurred, continue with empty slice
		subtreeHashes = []chainhash.Hash{}
	}

	subtreeHashesStrings := make([]string, 0, len(subtreeHashes))
	for _, hash := range subtreeHashes {
		subtreeHashesStrings = append(subtreeHashesStrings, hash.String())
	}

	removeMapLen32, err := safeconversion.IntToUint32(ba.blockAssembler.subtreeProcessor.GetRemoveMapLength())
	if err != nil {
		return nil, errors.NewProcessingError("[GetBlockAssemblyState] error converting remove map length", err)
	}

	currentHeader, currentHeight := ba.blockAssembler.CurrentBlock()

	subtreeSize, err := safeconversion.IntToUint32(ba.blockAssembler.subtreeProcessor.GetCurrentSubtreeSize())
	if err != nil {
		return nil, errors.NewProcessingError("[GetBlockAssemblyState] error converting current subtree size", err)
	}

	return &blockassembly_api.StateMessage{
		BlockAssemblyState:    StateStrings[ba.blockAssembler.GetCurrentRunningState()],
		SubtreeProcessorState: subtreeprocessor.StateStrings[ba.blockAssembler.subtreeProcessor.GetCurrentRunningState()],
		SubtreeCount:          subtreeCountUint32,
		SubtreeSize:           subtreeSize,
		TxCount:               ba.blockAssembler.TxCount(),
		QueueCount:            ba.blockAssembler.QueueLength(),
		CurrentHeight:         currentHeight,
		CurrentHash:           currentHeader.Hash().String(),
		RemoveMapCount:        removeMapLen32,
		Subtrees:              subtreeHashesStrings,
	}, nil
}

func (ba *BlockAssembly) GetBlockAssemblyTxs(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.GetBlockAssemblyTxsResponse, error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "GetBlockAssemblyTxsResponse",
		tracing.WithParentStat(ba.stats),
		tracing.WithLogMessage(ba.logger, "[GetBlockAssemblyTxsResponse] called"),
	)
	defer deferFn()

	txHashes := ba.blockAssembler.subtreeProcessor.GetTransactionHashes()
	txHashesStrings := make([]string, 0, len(txHashes))
	for _, hash := range txHashes {
		txHashesStrings = append(txHashesStrings, hash.String())
	}

	lenUint64, err := safeconversion.IntToUint64(len(txHashesStrings))
	if err != nil {
		return nil, errors.NewProcessingError("error converting transaction count", err)
	}

	return &blockassembly_api.GetBlockAssemblyTxsResponse{
		TxCount: lenUint64,
		Txs:     txHashesStrings,
	}, nil
}

// GetCurrentDifficulty retrieves the current mining difficulty target.
//
// This method provides access to the current difficulty target required for valid
// proof-of-work, which is critical information for miners. The difficulty is returned
// as a floating-point value derived from the current network state and consensus rules.
// This value determines how much computational work is required to find a valid block solution.
//
// Parameters:
//   - ctx: Context for cancellation (unused in current implementation)
//   - _: Empty message request (unused)
//
// Returns:
//   - *blockassembly_api.GetCurrentDifficultyResponse: Response containing the current difficulty
//   - error: Any error encountered during retrieval
func (ba *BlockAssembly) GetCurrentDifficulty(_ context.Context, _ *blockassembly_api.EmptyMessage) (resp *blockassembly_api.GetCurrentDifficultyResponse, err error) {
	blockHeader, _ := ba.blockAssembler.CurrentBlock()

	nBits, err := ba.blockAssembler.getNextNbits(blockHeader, time.Now().Unix())
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("error getting next nbits", err))
	}

	f, _ := nBits.CalculateDifficulty().Float64()

	return &blockassembly_api.GetCurrentDifficultyResponse{
		Difficulty: f,
		BlockHash:  blockHeader.Hash().CloneBytes(),
	}, nil
}

// GenerateBlocks generates the given number of blocks.
//
// This method provides a block generation capability primarily used for testing and
// development environments. It sequentially creates blocks by:
//   - Retrieving a mining candidate block template
//   - Finding a valid proof-of-work solution through mining
//   - Submitting the solution to add the block to the blockchain
//
// The operation requires the GenerateSupported flag to be enabled in chain configuration.
// An optional address parameter allows specifying where mining rewards should be sent.
//
// Parameters:
//   - ctx: Context for cancellation
//   - req: Block generation request containing count and optional reward address
//
// Returns:
//   - *blockassembly_api.EmptyMessage: Empty response on success
//   - error: Any error encountered during block generation
func (ba *BlockAssembly) GenerateBlocks(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) (*blockassembly_api.EmptyMessage, error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "GenerateBlocks",
		tracing.WithParentStat(ba.stats),
		tracing.WithHistogram(prometheusBlockAssemblerGenerateBlocks),
		tracing.WithLogMessage(ba.logger, "[generateBlocks] called"),
	)
	defer deferFn()

	// Check if unmined transactions are still being loaded
	if ba.blockAssembler.unminedTransactionsLoading.Load() {
		ba.logger.Warnf("[GenerateBlocks] service not ready - unmined transactions are still being loaded")
		return nil, errors.NewServiceError("service not ready - unmined transactions are still being loaded")
	}

	if !ba.blockAssembler.settings.ChainCfgParams.GenerateSupported {
		return nil, errors.NewProcessingError("generate is not supported")
	}

	for i := 0; i < int(req.Count); i++ {
		err := ba.generateBlock(ctx, req.Address)
		if err != nil {
			ba.logger.Errorf("[GenerateBlocks] failed to generate block %d of %d: %v", i+1, req.Count, err)
			return nil, errors.WrapGRPC(errors.NewProcessingError("error generating block %d of %d", i+1, req.Count, err))
		}
	}

	return &blockassembly_api.EmptyMessage{}, nil
}

// CheckBlockAssembly checks the block assembly state
func (ba *BlockAssembly) CheckBlockAssembly(_ context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.OKResponse, error) {
	err := ba.blockAssembler.subtreeProcessor.CheckSubtreeProcessor()

	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("error found in block assembly", err))
	}

	return &blockassembly_api.OKResponse{
		Ok: true,
	}, nil
}

// GetBlockAssemblyBlockCandidate retrieves the current block assembly block candidate.
//
// Parameters:
//   - ctx: Context for cancellation
//   - message: Empty message request
//
// Returns:
//   - *blockassembly_api.OKResponse: Response indicating success
func (ba *BlockAssembly) GetBlockAssemblyBlockCandidate(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.GetBlockAssemblyBlockCandidateResponse, error) {
	// get a mining candidate
	candidate, subtrees, err := ba.blockAssembler.GetMiningCandidate(ctx)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[CheckBlockAssemblyBlockTemplate] error getting mining candidate", err))
	}

	subtreeHashes := make([]*chainhash.Hash, len(subtrees))
	for i, subtree := range subtrees {
		subtreeHashes[i] = subtree.RootHash()
	}

	hashPrevBlock := chainhash.Hash(candidate.PreviousHash)

	// create a new nbits object with 0 difficulty
	nbits := model.NBit([4]byte{0xff, 0xff, 0xff, 0xff})

	// fake address for the coinbase tx
	address := "1MUMxUTXcPQ1kAqB7MtJWneeAwVW4cHzzp"

	blockSubsidy := util.GetBlockSubsidyForHeight(candidate.Height, ba.settings.ChainCfgParams)

	subtreeFees := uint64(0)
	for _, subtree := range subtrees {
		subtreeFees += subtree.Fees
	}

	coinbaseTx, err := model.CreateCoinbase(candidate.Height, blockSubsidy+subtreeFees, "block template", []string{address})
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[CheckBlockAssemblyBlockTemplate] error creating coinbase tx", err))
	}

	coinbaseSize := coinbaseTx.Size()

	coinbaseSizeUint64, err := safeconversion.IntToUint64(coinbaseSize)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[CheckBlockAssemblyBlockTemplate] error converting coinbase size", err))
	}

	merkleSubtreeHashes := make([]chainhash.Hash, len(subtrees))

	if len(subtrees) > 0 {
		ba.logger.Infof("[CheckBlockAssemblyBlockTemplate] submit job has subtrees: %d", len(subtrees))

		for i, subtree := range subtrees {
			// the job subtree hash needs to be stored for the block, before the coinbase is replaced in the first
			// subtree, which changes the id of the subtree
			if i == 0 {
				rootHash, err := subtrees[i].RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, coinbaseSizeUint64)
				if err != nil {
					return nil, errors.WrapGRPC(errors.NewProcessingError("[CheckBlockAssemblyBlockTemplate] error replacing root node in subtree", err))
				}

				merkleSubtreeHashes[i] = *rootHash
			} else {
				merkleSubtreeHashes[i] = *subtree.RootHash()
			}
		}
	}

	hashMerkleRoot, err := ba.createMerkleTreeFromSubtrees("CheckBlockAssemblyBlockTemplate", subtrees, merkleSubtreeHashes, coinbaseTx.TxIDChainHash())
	if err != nil {
		return nil, errors.NewProcessingError("[CheckBlockAssemblyBlockTemplate] failed to create merkle tree", err)
	}

	// create the block from the candidate
	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        candidate.Version,
			HashPrevBlock:  &hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Timestamp:      candidate.Time,
			Bits:           nbits,
			Nonce:          0,
		},
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(candidate.NumTxs),
		SizeInBytes:      candidate.GetSizeWithoutCoinbase() + coinbaseSizeUint64,
		Subtrees:         subtreeHashes,
		Height:           candidate.Height,
	}

	blockBytes, err := block.Bytes()
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[CheckBlockAssemblyBlockTemplate] error converting block to bytes", err))
	}

	return &blockassembly_api.GetBlockAssemblyBlockCandidateResponse{
		Block: blockBytes,
	}, nil
}

// generateBlock creates a new block by getting a mining candidate and mining it.
//
// Parameters:
//   - ctx: Context for cancellation
//   - address: Optional address for mining rewards
//
// Returns:
//   - error: Any error encountered during block generation
func (ba *BlockAssembly) generateBlock(ctx context.Context, address *string) error {
	// get a mining candidate
	miningCandidate, err := ba.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{})
	if err != nil {
		return errors.NewProcessingError("error getting mining candidate", err)
	}

	// mine the block
	miningSolution, err := mining.Mine(ctx, ba.blockAssembler.settings, miningCandidate, address)
	if err != nil {
		return errors.NewProcessingError("error mining block", err)
	}

	// submit the block
	req := &BlockSubmissionRequest{
		SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
			Id:         miningSolution.Id,
			Nonce:      miningSolution.Nonce,
			CoinbaseTx: miningSolution.Coinbase,
			Time:       miningSolution.Time,
			Version:    miningSolution.Version,
		},
	}

	// Store the current best block hash before submission
	previousBestHeader, _ := ba.blockAssembler.CurrentBlock()
	previousBestHash := previousBestHeader.Hash()

	resp, err := ba.submitMiningSolution(ctx, req)
	if err != nil {
		ba.logger.Errorf("[generateBlock] error submitting block: %v", err)
		return errors.NewProcessingError("error submitting block", err)
	}

	if !resp.Ok {
		return bt.ErrTxNil
	}

	// Wait for the best block header to be updated after successful submission
	// This prevents the "already mining on top of the same block" error when generating multiple blocks
	return ba.waitForBestBlockHeaderUpdate(ctx, previousBestHash)
}

// waitForBestBlockHeaderUpdate waits for the best block header to be updated after block submission
func (ba *BlockAssembly) waitForBestBlockHeaderUpdate(ctx context.Context, previousBestHash *chainhash.Hash) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			ba.logger.Warnf("[generateBlock] timeout waiting for best block header update after submitting block")
			// Continue anyway - the block was submitted successfully
			return nil
		case <-ticker.C:
			currentBestHeader, _ := ba.blockAssembler.CurrentBlock()
			currentBestHash := currentBestHeader.Hash()
			if !currentBestHash.IsEqual(previousBestHash) {
				// Best block has been updated
				return nil
			}
		}
	}
}

// SetSkipWaitForPendingBlocks sets the flag to skip waiting for pending blocks during startup.
// This is primarily used in test environments to prevent blocking on pending blocks.
func (ba *BlockAssembly) SetSkipWaitForPendingBlocks(skip bool) {
	ba.skipWaitForPendingBlocks = skip
	if ba.blockAssembler != nil {
		ba.blockAssembler.SetSkipWaitForPendingBlocks(skip)
	}
}
