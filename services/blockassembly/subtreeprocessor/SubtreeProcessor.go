// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
//
// The subtreeprocessor implements an efficient system for organizing transactions into
// hierarchical subtrees that can be quickly assembled into blocks. This approach enables:
//   - Efficient transaction storage and retrieval
//   - Dynamic adjustment of subtree sizes based on transaction volume
//   - Parallelized processing of transaction groups
//   - Optimized block candidate generation
//   - Fast response to blockchain reorganizations
//
// The package works closely with the blockassembly service to maintain transaction
// integrity during chain reorganizations and provides mechanisms for transaction
// deduplication and conflict resolution.

package subtreeprocessor

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"log"
	"math"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	subtreepkg "github.com/bitcoin-sv/teranode/pkg/go-subtree"
	txmap "github.com/bitcoin-sv/teranode/pkg/go-tx-map"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// Job represents a mining job with its associated data.
// A Job encapsulates all the information needed for a miner to attempt finding a valid
// proof-of-work solution, including the block template and associated transaction subtrees.
// Each job is uniquely identified to track mining attempts and solutions.

type Job struct {
	ID              *chainhash.Hash        // Unique identifier for the job
	Subtrees        []*subtreepkg.Subtree  // Collection of subtrees for the job
	MiningCandidate *model.MiningCandidate // Mining candidate information
}

// NewSubtreeRequest encapsulates a request to process a new subtree.
// This structure is used to communicate new subtrees between the block assembly service
// and the subtree processor, including an error channel for asynchronous result reporting.

type NewSubtreeRequest struct {
	Subtree     *subtreepkg.Subtree                                     // The subtree to process
	ParentTxMap *txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints] // Map of parent transactions
	ErrChan     chan error                                              // Channel for error reporting
}

// moveBlockRequest represents a request to move a block in the chain.
type moveBlockRequest struct {
	block   *model.Block // The block to move
	errChan chan error   // Channel for error reporting
}

// reorgBlocksRequest represents a request to reorganize blocks in the chain.
type reorgBlocksRequest struct {
	// moveBackBlocks contains blocks that need to be removed from the current chain
	moveBackBlocks []*model.Block

	// moveForwardBlocks contains blocks that need to be added to form the new chain
	moveForwardBlocks []*model.Block

	// errChan receives any error encountered during reorganization
	errChan chan error
}

// resetBlocks encapsulates the data needed for a processor reset operation.
type resetBlocks struct {
	// blockHeader represents the new block header to reset to
	blockHeader *model.BlockHeader

	// moveBackBlocks contains blocks that need to be removed during reset
	moveBackBlocks []*model.Block

	// moveForwardBlocks contains blocks that need to be added during reset
	moveForwardBlocks []*model.Block

	// responseCh receives the reset operation response
	responseCh chan ResetResponse

	// isLegacySync indicates whether this is a legacy synchronization operation
	isLegacySync bool
}

// ResetResponse encapsulates the response from a reset operation.
type ResetResponse struct {
	// Err contains any error encountered during the reset operation
	Err error
}

// SubtreeProcessor manages the processing of transaction subtrees and block assembly.
// It serves as the core component for organizing transactions into efficient structures
// for block creation. The processor maintains the current blockchain state, manages
// transaction queues, and coordinates with the block assembler to create valid block templates.
//
// Key responsibilities include:
//   - Organizing transactions into hierarchical subtrees
//   - Processing newly arrived transactions
//   - Handling chain reorganizations
//   - Managing transaction conflicts
//   - Optimizing subtree sizes based on transaction volume
//   - Providing transaction sets for block candidates

type SubtreeProcessor struct {
	// settings contains the configuration parameters for the processor
	settings *settings.Settings

	// currentItemsPerFile specifies the maximum number of items per subtree file
	currentItemsPerFile int

	// blockStartTime tracks when the current block started
	blockStartTime time.Time

	// subtreesInBlock tracks number of subtrees created in current block
	subtreesInBlock int

	// blockIntervals tracks recent intervals per subtree in previous blocks
	blockIntervals []time.Duration

	// maxBlockSamples is the number of block samples to keep for averaging
	maxBlockSamples int

	// txChan receives transaction batches for processing
	txChan chan *[]TxIDAndFee

	// getSubtreesChan handles requests to retrieve current subtrees
	getSubtreesChan chan chan []*subtreepkg.Subtree

	// moveForwardBlockChan receives requests to process new blocks
	moveForwardBlockChan chan moveBlockRequest

	// reorgBlockChan handles blockchain reorganization requests
	reorgBlockChan chan reorgBlocksRequest

	// deDuplicateTransactionsCh triggers transaction deduplication
	deDuplicateTransactionsCh chan struct{}

	// resetCh handles requests to reset the processor state
	resetCh chan *resetBlocks

	// removeTxCh receives transactions to be removed
	removeTxCh chan chainhash.Hash

	// lengthCh receives requests for the current length of the processor
	lengthCh chan chan int

	// checkSubtreeProcessorCh is used to check the subtree processor state
	checkSubtreeProcessorCh chan chan error

	// newSubtreeChan receives notifications about new subtrees
	newSubtreeChan chan NewSubtreeRequest

	// chainedSubtrees stores the ordered list of completed subtrees
	chainedSubtrees []*subtreepkg.Subtree

	// chainedSubtreeCount tracks the number of chained subtrees atomically
	chainedSubtreeCount atomic.Int32

	// currentSubtree represents the subtree currently being built
	currentSubtree *subtreepkg.Subtree

	// currentBlockHeader stores the current block header being processed
	currentBlockHeader *model.BlockHeader

	// Mutex provides thread-safe access to shared resources
	sync.Mutex

	// txCount tracks the total number of transactions processed
	txCount atomic.Uint64

	// batcher manages transaction batching operations
	batcher *TxIDAndFeeBatch

	// queue manages the transaction processing queue
	queue *LockFreeQueue

	// currentTxMap tracks transactions currently held in the subtree processor
	currentTxMap *txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints]

	// removeMap tracks transactions marked for removal
	removeMap *txmap.SwissMap

	// blockchainClient provides access to blockchain data
	blockchainClient blockchain.ClientI

	// subtreeStore provides persistent storage for subtrees
	subtreeStore blob.Store

	// utxoStore manages UTXO set storage
	utxoStore utxostore.Store

	// logger handles logging operations
	logger ulogger.Logger

	// stats tracks operational statistics
	stats *gocore.Stat

	// currentRunningState tracks the processor's operational state
	currentRunningState atomic.Value
}

type State uint32

// create state strings for the processor
var (
	// StateStarting indicates the processor is starting up
	StateStarting State = 0

	// StateRunning indicates the processor is actively processing
	StateRunning State = 1

	// StateDequeue indicates the processor is dequeuing transactions
	StateDequeue State = 2

	// StateGetSubtrees indicates the processor is retrieving subtrees
	StateGetSubtrees State = 3

	// StateReorg indicates the processor is reorganizing blocks
	StateReorg State = 4

	// StateMoveForwardBlock indicates the processor is moving forward a block
	StateMoveForwardBlock State = 5

	// StateDeduplicateTransactions indicates the processor is deduplicating transactions
	StateDeduplicateTransactions State = 6

	// StateResetBlocks indicates the processor is resetting blocks
	StateResetBlocks State = 7

	// StateRemoveTx indicates the processor is removing transactions
	StateRemoveTx State = 8

	// StateCheckSubtreeProcessor indicates the processor is checking its state
	StateCheckSubtreeProcessor State = 9
)

var StateStrings = map[State]string{
	StateStarting:                "starting",
	StateRunning:                 "running",
	StateDequeue:                 "dequeue",
	StateGetSubtrees:             "getSubtrees",
	StateReorg:                   "reorg",
	StateMoveForwardBlock:        "moveForwardBlock",
	StateDeduplicateTransactions: "deDuplicateTransactions",
	StateResetBlocks:             "resetBlocks",
	StateRemoveTx:                "removeTx",
	StateCheckSubtreeProcessor:   "checkSubtreeProcessor",
}

var (
	// ExpectedNumberOfSubtrees defines the expected number of subtrees in a block.
	// This is calculated based on a subtree being created approximately every second,
	// and is used for initial capacity allocation to optimize memory usage.
	ExpectedNumberOfSubtrees = 1024
)

// NewSubtreeProcessor creates and initializes a new SubtreeProcessor instance.
//
// Parameters:
//   - ctx: Context for cancellation
//   - logger: Logger instance for recording operations
//   - tSettings: Teranode settings configuration
//   - subtreeStore: Storage for subtrees
//   - utxoStore: Storage for UTXOs
//   - newSubtreeChan: Channel for new subtree notifications
//   - options: Optional configuration functions
//
// Returns:
//   - *SubtreeProcessor: Initialized subtree processor
//   - error: Any error encountered during initialization
func NewSubtreeProcessor(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, subtreeStore blob.Store,
	blockchainClient blockchain.ClientI, utxoStore utxostore.Store, newSubtreeChan chan NewSubtreeRequest, options ...Options) (*SubtreeProcessor, error) {
	initPrometheusMetrics()

	initialItemsPerFile := tSettings.BlockAssembly.InitialMerkleItemsPerSubtree

	firstSubtree, err := subtreepkg.NewTreeByLeafCount(initialItemsPerFile)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("error creating first subtree", err)
	}

	// We add a placeholder for the coinbase tx because we know this is the first subtree in the chain
	if err = firstSubtree.AddCoinbaseNode(); err != nil {
		return nil, errors.NewInvalidArgumentError("error adding coinbase placeholder to first subtree", err)
	}

	queue := NewLockFreeQueue()

	stp := &SubtreeProcessor{
		settings:                  tSettings,
		currentItemsPerFile:       initialItemsPerFile,
		blockStartTime:            time.Time{},
		subtreesInBlock:           0,
		blockIntervals:            make([]time.Duration, 0, 10),
		maxBlockSamples:           10,
		txChan:                    make(chan *[]TxIDAndFee, tSettings.SubtreeValidation.TxChanBufferSize),
		getSubtreesChan:           make(chan chan []*subtreepkg.Subtree),
		moveForwardBlockChan:      make(chan moveBlockRequest),
		reorgBlockChan:            make(chan reorgBlocksRequest),
		deDuplicateTransactionsCh: make(chan struct{}),
		resetCh:                   make(chan *resetBlocks),
		removeTxCh:                make(chan chainhash.Hash),
		lengthCh:                  make(chan chan int),
		checkSubtreeProcessorCh:   make(chan chan error),
		newSubtreeChan:            newSubtreeChan,
		chainedSubtrees:           make([]*subtreepkg.Subtree, 0, ExpectedNumberOfSubtrees),
		chainedSubtreeCount:       atomic.Int32{},
		currentSubtree:            firstSubtree,
		batcher:                   NewTxIDAndFeeBatch(tSettings.BlockAssembly.SubtreeProcessorBatcherSize),
		queue:                     queue,
		currentTxMap:              txmap.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints](),
		removeMap:                 txmap.NewSwissMap(0),
		blockchainClient:          blockchainClient,
		subtreeStore:              subtreeStore,
		utxoStore:                 utxoStore,
		logger:                    logger,
		stats:                     gocore.NewStat("subtreeProcessor").NewStat("Add", false),
		currentRunningState:       atomic.Value{},
	}
	stp.setCurrentRunningState(StateStarting)

	// need to make sure first coinbase tx is counted when we start
	stp.setTxCountFromSubtrees()

	for _, opts := range options {
		opts(stp)
	}

	stp.setCurrentRunningState(StateRunning)

	go func() {
		var (
			txReq *TxIDAndFee
			err   error
		)

		for {
			select {
			case getSubtreesChan := <-stp.getSubtreesChan:
				stp.setCurrentRunningState(StateGetSubtrees)

				logger.Infof("[SubtreeProcessor] get current subtrees")

				chainedCount := stp.chainedSubtreeCount.Load()
				completeSubtrees := make([]*subtreepkg.Subtree, 0, chainedCount)
				completeSubtrees = append(completeSubtrees, stp.chainedSubtrees...)

				// incomplete subtrees ?
				if chainedCount == 0 && stp.currentSubtree.Length() > 1 {
					incompleteSubtree, err := subtreepkg.NewTreeByLeafCount(stp.currentItemsPerFile)
					if err != nil {
						logger.Errorf("[SubtreeProcessor] error creating incomplete subtree: %s", err.Error())
						getSubtreesChan <- nil

						stp.setCurrentRunningState(StateRunning)

						continue
					}

					_ = incompleteSubtree.AddCoinbaseNode()
					for _, node := range stp.currentSubtree.Nodes[1:] {
						_ = incompleteSubtree.AddSubtreeNode(node)
					}

					incompleteSubtree.Fees = stp.currentSubtree.Fees
					completeSubtrees = append(completeSubtrees, incompleteSubtree)

					// store (and announce) new incomplete subtree to other miners
					send := NewSubtreeRequest{
						Subtree:     incompleteSubtree,
						ParentTxMap: stp.currentTxMap,
						ErrChan:     make(chan error),
					}
					newSubtreeChan <- send

					// wait for a response
					// if we don't then we can't mine initial blocks and run coinbase splitter together
					// this is because getMiningCandidate would create subtrees in the background and
					// submitMiningSolution would try to setDAH on something that might not yet exist
					<-send.ErrChan
				}

				getSubtreesChan <- completeSubtrees

				logger.Infof("[SubtreeProcessor] get current subtrees DONE")

				stp.setCurrentRunningState(StateRunning)

			case reorgReq := <-stp.reorgBlockChan:
				stp.setCurrentRunningState(StateReorg)
				logger.Infof("[SubtreeProcessor] reorgReq subtree processor: %d, %d", len(reorgReq.moveBackBlocks), len(reorgReq.moveForwardBlocks))
				err = stp.reorgBlocks(ctx, reorgReq.moveBackBlocks, reorgReq.moveForwardBlocks)
				reorgReq.errChan <- err
				logger.Infof("[SubtreeProcessor] reorgReq subtree processor DONE: %d, %d", len(reorgReq.moveBackBlocks), len(reorgReq.moveForwardBlocks))
				stp.setCurrentRunningState(StateRunning)

			case moveForwardReq := <-stp.moveForwardBlockChan:
				stp.setCurrentRunningState(StateMoveForwardBlock)

				logger.Infof("[SubtreeProcessor][%s] moveForwardBlock subtree processor", moveForwardReq.block.String())

				err = stp.moveForwardBlock(ctx, moveForwardReq.block, false)
				if err == nil {
					stp.currentBlockHeader = moveForwardReq.block.Header
				}
				moveForwardReq.errChan <- err
				logger.Infof("[SubtreeProcessor][%s] moveForwardBlock subtree processor DONE", moveForwardReq.block.String())
				stp.setCurrentRunningState(StateRunning)

			case <-stp.deDuplicateTransactionsCh:
				stp.setCurrentRunningState(StateDeduplicateTransactions)
				stp.deDuplicateTransactions()
				stp.setCurrentRunningState(StateRunning)

			case resetBlocksMsg := <-stp.resetCh:
				stp.setCurrentRunningState(StateResetBlocks)
				err = stp.reset(resetBlocksMsg.blockHeader, resetBlocksMsg.moveBackBlocks, resetBlocksMsg.moveForwardBlocks, resetBlocksMsg.isLegacySync)

				if resetBlocksMsg.responseCh != nil {
					resetBlocksMsg.responseCh <- ResetResponse{Err: err}
				}

				stp.setCurrentRunningState(StateRunning)

			case removeTxHash := <-stp.removeTxCh:
				// remove the given transaction from the subtrees
				stp.setCurrentRunningState(StateRemoveTx)

				if err = stp.removeTxFromSubtrees(ctx, removeTxHash); err != nil {
					stp.logger.Errorf("[SubtreeProcessor] error removing tx from subtrees: %s", err.Error())
				}

				stp.setCurrentRunningState(StateRunning)

			case lengthCh := <-stp.lengthCh:
				// return the length of the current subtree
				lengthCh <- stp.currentSubtree.Length()

			case errCh := <-stp.checkSubtreeProcessorCh:
				stp.setCurrentRunningState(StateCheckSubtreeProcessor)

				stp.checkSubtreeProcessor(errCh)

				stp.setCurrentRunningState(StateRunning)

			default:
				stp.setCurrentRunningState(StateDequeue)

				nrProcessed := 0
				mapLength := stp.removeMap.Length()
				// set the validFromMillis to the current time minus the double spend window - so in the past
				validFromMillis := time.Now().Add(-1 * stp.settings.BlockAssembly.DoubleSpendWindow).UnixMilli()

				for {
					txReq = stp.queue.dequeue(validFromMillis)
					if txReq == nil {
						time.Sleep(1 * time.Millisecond)
						break
					}

					// check if the tx needs to be removed
					if mapLength > 0 && stp.removeMap.Exists(txReq.node.Hash) {
						// remove from the map
						if err = stp.removeMap.Delete(txReq.node.Hash); err != nil {
							stp.logger.Errorf("[SubtreeProcessor] error removing tx from remove map: %s", err.Error())
						}

						continue
					}

					if txReq.node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
						stp.logger.Errorf("[SubtreeProcessor] error adding node: skipping request to add coinbase tx placeholder")
						continue
					}

					// check if the tx is already in the currentTxMap
					if _, ok := stp.currentTxMap.Get(txReq.node.Hash); ok {
						stp.logger.Warnf("[SubtreeProcessor] error adding node: tx %s already in currentTxMap", txReq.node.Hash.String())
						continue
					}

					// check txInpoints
					// for _, parent := range txReq.txInpoints {
					// 	if _, ok := stp.currentTxMap.Get(parent); !ok {
					// 		stp.logger.Errorf("[SubtreeProcessor] error adding node: parent %s not found in currentTxMap", parent.String())
					// 		continue
					// 	}
					// }

					if err = stp.addNode(txReq.node, &txReq.txInpoints, false); err != nil {
						stp.logger.Errorf("[SubtreeProcessor] error adding node: %s", err.Error())
					} else {
						stp.txCount.Add(1)
					}

					nrProcessed++
					if nrProcessed > stp.settings.BlockAssembly.SubtreeProcessorBatcherSize {
						break
					}
				}

				stp.setCurrentRunningState(StateRunning)
			}
		}
	}()

	return stp, nil
}

// setCurrentRunningState updates the current operational state of the processor.
//
// Parameters:
//   - state: New state to set
func (stp *SubtreeProcessor) setCurrentRunningState(state State) {
	stp.currentRunningState.Store(state)
	prometheusSubtreeProcessorCurrentState.Set(float64(state))
}

// GetCurrentRunningState returns the current operational state of the processor.
//
// Returns:
//   - string: Current state description
func (stp *SubtreeProcessor) GetCurrentRunningState() State {
	return stp.currentRunningState.Load().(State)
}

// GetCurrentLength returns the length of the current subtree
//
// Returns:
//   - int: Length of the current subtree
func (stp *SubtreeProcessor) GetCurrentLength() int {
	lengthCh := make(chan int)

	stp.lengthCh <- lengthCh

	return <-lengthCh
}

// Reset resets the processor to a clean state,  removing all subtrees and transactions
// This will be called from the block assembler in a channel select, making sure no other operations are happening
// Note - the queue will still be ingesting transactions
// Parameters:
//   - blockHeader: New block header to reset to
//   - moveBackBlocks: Blocks to move down in the chain
//   - moveForwardBlocks: Blocks to move up in the chain
//   - isLegacySync: Whether this is a legacy sync operation
//
// Returns:
//   - ResetResponse: Response containing any errors encountered
func (stp *SubtreeProcessor) Reset(blockHeader *model.BlockHeader, moveBackBlocks []*model.Block, moveForwardBlocks []*model.Block, isLegacySync bool) ResetResponse {
	responseCh := make(chan ResetResponse)
	stp.resetCh <- &resetBlocks{
		blockHeader:       blockHeader,
		moveBackBlocks:    moveBackBlocks,
		moveForwardBlocks: moveForwardBlocks,
		responseCh:        responseCh,
		isLegacySync:      isLegacySync,
	}

	return <-responseCh
}

func (stp *SubtreeProcessor) reset(blockHeader *model.BlockHeader, moveBackBlocks []*model.Block, moveForwardBlocks []*model.Block, isLegacySync bool) error {
	_, _, deferFn := tracing.Tracer("subtreeprocessor").Start(context.Background(), "reset",
		tracing.WithParentStat(stp.stats),
		tracing.WithHistogram(prometheusSubtreeProcessorReset),
		tracing.WithLogMessage(stp.logger, "[SubtreeProcessor][reset][%s] Resetting subtree processor with %d moveBackBlocks and %d moveForwardBlocks", len(moveBackBlocks), len(moveForwardBlocks)),
	)

	defer deferFn()

	stp.chainedSubtrees = make([]*subtreepkg.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.chainedSubtreeCount.Store(0)

	stp.currentSubtree, _ = subtreepkg.NewTreeByLeafCount(stp.currentItemsPerFile)
	stp.txCount.Store(0)

	// dequeue all transactions
	stp.logger.Warnf("[SubtreeProcessor][Reset] Dequeueing all transactions")

	validUntilMillis := time.Now().UnixMilli()

	for {
		txReq := stp.queue.dequeue(0)
		if txReq == nil || txReq.time > validUntilMillis {
			// we are done
			break
		}
	}

	for _, block := range moveBackBlocks {
		if err := stp.utxoStore.Delete(context.Background(), block.CoinbaseTx.TxIDChainHash()); err != nil {
			// no need to error out if the key doesn't exist anyway
			if !errors.Is(err, errors.ErrTxNotFound) {
				return errors.NewProcessingError("[SubtreeProcessor][Reset] error deleting utxos for tx %s", block.CoinbaseTx.String(), err)
			}
		}

		stp.currentBlockHeader = block.Header
	}

	// optimized version for legacy sync
	if isLegacySync {
		coinbaseTxsAdded := sync.Map{}

		g, gCtx := errgroup.WithContext(context.Background())

		for _, block := range moveForwardBlocks {
			g.Go(func() error {
				if err := stp.processCoinbaseUtxos(gCtx, block); err != nil {
					return err
				}

				coinbaseTxsAdded.Store(block.Hash().String(), block)

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			coinbaseTxsAdded.Range(func(key, value interface{}) bool {
				// remove all the coinbase transactions we added
				block := value.(*model.Block)
				if delErr := stp.utxoStore.Delete(context.Background(), block.CoinbaseTx.TxIDChainHash()); err != nil {
					stp.logger.Errorf("[SubtreeProcessor][Reset] error deleting utxos for coinbase tx %s: %v", block.CoinbaseTx.String(), delErr)
				}

				return true
			})

			return errors.NewProcessingError("[SubtreeProcessor][Reset] error processing coinbase utxos", err)
		}

		stp.currentBlockHeader = blockHeader
	} else {
		for _, block := range moveForwardBlocks {
			// A block has potentially some conflicting transactions that need to be processed when we
			// move forward the block
			ctx := context.Background()

			// TODO optimize this to only read in the conflicting transactions from the subtree
			// when the block actually has conflicting transactions, should be marked in the block
			conflictingNodes := make([]chainhash.Hash, 0, 1024)

			// get the conflicting transactions from the block subtrees
			for _, subtreeHash := range block.Subtrees {
				// get the conflicting transactions from the subtree
				subtreeReader, err := stp.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
				if err != nil {
					subtreeReader, err = stp.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
					if err != nil {
						return errors.NewProcessingError("[moveForwardBlock][%s] error getting subtree %s from store", block.String(), subtreeHash.String(), err)
					}
				}

				subtreeConflictingNodes, err := subtreepkg.DeserializeSubtreeConflictingFromReader(subtreeReader)
				if err != nil {
					return errors.NewProcessingError("[moveForwardBlock][%s] error deserializing subtree conflicting nodes", block.String(), err)
				}

				if len(subtreeConflictingNodes) > 0 {
					conflictingNodes = append(conflictingNodes, subtreeConflictingNodes...)
				}
			}

			if len(conflictingNodes) > 0 {
				losingTxHashesMap, err := utxostore.ProcessConflicting(ctx, stp.utxoStore, conflictingNodes)
				if err != nil {
					return errors.NewProcessingError("[moveForwardBlock][%s] error processing conflicting transactions in Reset()", block.String(), err)
				}

				if losingTxHashesMap.Length() > 0 {
					// mark all the losing txs in the subtrees in the blocks they were mined into as conflicting
					if err = stp.markConflictingTxsInSubtrees(ctx, losingTxHashesMap); err != nil {
						return errors.NewProcessingError("[moveForwardBlock][%s] error marking conflicting transactions", block.String(), err)
					}
				}
			}

			if err := stp.processCoinbaseUtxos(context.Background(), block); err != nil {
				return errors.NewProcessingError("[SubtreeProcessor][Reset] error processing coinbase utxos", err)
			}

			stp.currentBlockHeader = block.Header
		}
	}

	return nil
}

// GetCurrentBlockHeader returns the current block header being processed.
//
// Returns:
//   - *model.BlockHeader: Current block header
func (stp *SubtreeProcessor) GetCurrentBlockHeader() *model.BlockHeader {
	return stp.currentBlockHeader
}

// GetCurrentSubtree returns the subtree currently being built.
//
// Returns:
//   - *util.Subtree: Current subtree
func (stp *SubtreeProcessor) GetCurrentSubtree() *subtreepkg.Subtree {
	return stp.currentSubtree
}

// GetCurrentTxMap returns the map of transactions currently held in the subtree processor.
//
// Returns:
//   - *util.SyncedMap[chainhash.Hash, []chainhash.Hash]: Map of transactions
func (stp *SubtreeProcessor) GetCurrentTxMap() *txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints] {
	return stp.currentTxMap
}

// GetChainedSubtrees returns all completed subtrees in the chain.
//
// Returns:
//   - []*util.Subtree: Array of chained subtrees
func (stp *SubtreeProcessor) GetChainedSubtrees() []*subtreepkg.Subtree {
	return stp.chainedSubtrees
}

// GetUtxoStore returns the UTXO store instance.
//
// Returns:
//   - utxostore.Store: UTXO store instance
func (stp *SubtreeProcessor) GetUtxoStore() utxostore.Store {
	return stp.utxoStore
}

// SetCurrentItemsPerFile updates the maximum items per subtree file.
//
// Parameters:
//   - v: New maximum items value
func (stp *SubtreeProcessor) SetCurrentItemsPerFile(v int) {
	stp.currentItemsPerFile = v
}

// TxCount returns the total number of transactions processed.
//
// Returns:
//   - uint64: Total transaction count
func (stp *SubtreeProcessor) TxCount() uint64 {
	return stp.txCount.Load()
}

// QueueLength returns the current length of the transaction queue.
//
// Returns:
//   - int64: Current queue length
func (stp *SubtreeProcessor) QueueLength() int64 {
	return stp.queue.length()
}

// SubtreeCount returns the total number of subtrees.
// This method is primarily used for prometheus statistics.
//
// Returns:
//   - int: Total number of subtrees
func (stp *SubtreeProcessor) SubtreeCount() int {
	// not using len(chainSubtrees) to avoid Race condition
	// should we be using locks around all chainSubtree operations instead?
	// the subtree count isn't mission-critical - it's just for statistics
	return int(stp.chainedSubtreeCount.Load()) + 01
}

// adjustSubtreeSize calculates and sets a new subtree size based on recent block statistics
// to maintain approximately one subtree per second. The size will always be a power of 2
// and not smaller than 1024.
func (stp *SubtreeProcessor) adjustSubtreeSize() {
	if !stp.settings.BlockAssembly.UseDynamicSubtreeSize {
		return
	}

	// Calculate average interval between subtrees in this block
	if len(stp.blockIntervals) == 0 {
		return
	}

	// Filter out any intervals that are too small (likely spurious) or negative
	validIntervals := make([]time.Duration, 0)

	for _, interval := range stp.blockIntervals {
		if interval > time.Millisecond && interval < time.Hour {
			validIntervals = append(validIntervals, interval)
		}
	}

	if len(validIntervals) == 0 {
		return
	}

	// Calculate average interval
	var sum time.Duration
	for _, interval := range validIntervals {
		sum += interval
	}

	avgInterval := sum / time.Duration(len(validIntervals))

	stp.logger.Debugf("avgInterval=%v, validIntervals=%v\n", avgInterval, validIntervals)

	// Calculate ratio of target to actual interval
	// If we're creating subtrees faster than target, ratio > 1 and size should increase
	targetInterval := time.Second
	ratio := float64(targetInterval) / float64(avgInterval)
	currentSize := stp.currentItemsPerFile

	stp.logger.Debugf("ratio=%v, currentSize=%d, newSize before rounding=%d\n",
		ratio, currentSize, int(float64(currentSize)*ratio))

	// Calculate new size based on ratio
	newSize := int(float64(currentSize) * ratio)

	// Round to next power of 2
	newSize = int(math.Pow(2, math.Ceil(math.Log2(float64(newSize)))))
	stp.logger.Debugf("newSize after rounding=%d\n", newSize)

	// Cap the increase to 2x per block to avoid wild swings
	if newSize > currentSize*2 {
		newSize = currentSize * 2
		stp.logger.Debugf("newSize capped at 2x=%d\n", newSize)
	}

	// never go over maximum size
	maxSubtreeSize := stp.settings.BlockAssembly.MaximumMerkleItemsPerSubtree
	if newSize > maxSubtreeSize {
		newSize = maxSubtreeSize
		stp.logger.Debugf("newSize capped at maxSubtreeSize=%d\n", newSize)
	}

	// Never go below minimum size
	minSubtreeSize := stp.settings.BlockAssembly.MinimumMerkleItemsPerSubtree
	if newSize < minSubtreeSize {
		newSize = minSubtreeSize
	}

	if newSize != currentSize {
		stp.logger.Debugf("setting new size from %d to %d\n", currentSize, newSize)
		stp.currentItemsPerFile = newSize
	}

	prometheusSubtreeProcessorDynamicSubtreeSize.Set(float64(newSize))

	// Reset intervals for next block
	stp.blockIntervals = make([]time.Duration, 0)
}

// SetCurrentBlockHeader updates the current block header.
func (stp *SubtreeProcessor) SetCurrentBlockHeader(blockHeader *model.BlockHeader) {
	stp.Lock()
	defer stp.Unlock()

	stp.currentBlockHeader = blockHeader
	stp.blockStartTime = time.Now()
	stp.subtreesInBlock = 1
}

// addNode adds a new transaction node to the current subtree.
//
// Parameters:
//   - node: Transaction node to add
//   - skipNotification: Whether to skip notification of new subtrees
//
// Returns:
//   - error: Any error encountered during addition
func (stp *SubtreeProcessor) addNode(node subtreepkg.SubtreeNode, parents *subtreepkg.TxInpoints, skipNotification bool) (err error) {
	stp.Lock()
	defer stp.Unlock()

	if stp.currentSubtree == nil {
		stp.currentSubtree, err = subtreepkg.NewTreeByLeafCount(stp.currentItemsPerFile)
		if err != nil {
			return err
		}

		err = stp.currentSubtree.AddCoinbaseNode()
		if err != nil {
			return err
		}
	}

	err = stp.currentSubtree.AddSubtreeNode(node)
	if err != nil {
		return errors.NewProcessingError("error adding node to subtree", err)
	}

	// parent can only be set to nil, when they are already in the map
	if parents == nil {
		if nilParents, ok := stp.currentTxMap.Get(node.Hash); !ok {
			return errors.NewProcessingError("error adding node to subtree: txInpoints not found in currentTxMap for %s", node.Hash.String())
		} else {
			parents = &nilParents
		}
	} else {
		stp.currentTxMap.Set(node.Hash, *parents)
	}

	if parents == nil {
		return errors.NewProcessingError("error adding node to subtree: parents not found in currentTxMap for %s", node.Hash.String())
	}

	if stp.currentSubtree.IsComplete() {
		if err = stp.processCompleteSubtree(skipNotification); err != nil {
			return err
		}
	}

	return nil
}

func (stp *SubtreeProcessor) processCompleteSubtree(skipNotification bool) (err error) {
	if !skipNotification {
		stp.logger.Infof("[%s] append subtree", stp.currentSubtree.RootHash().String())
	}

	// Add the subtree to the chain
	stp.chainedSubtrees = append(stp.chainedSubtrees, stp.currentSubtree)
	stp.chainedSubtreeCount.Add(1)

	stp.subtreesInBlock++ // Track number of subtrees in current block

	oldSubtree := stp.currentSubtree
	oldSubtreeHash := oldSubtree.RootHash()

	// create a new subtree with the same height as the previous subtree
	stp.currentSubtree, err = subtreepkg.NewTree(stp.currentSubtree.Height)
	if err != nil {
		return errors.NewProcessingError("[%s] error creating new subtree", oldSubtreeHash.String(), err)
	}

	if !skipNotification {
		// Send the subtree to the newSubtreeChan, including a reference to the parent transactions map
		errCh := make(chan error)
		stp.newSubtreeChan <- NewSubtreeRequest{Subtree: oldSubtree, ParentTxMap: stp.currentTxMap, ErrChan: errCh}

		err = <-errCh
		if err != nil {
			return errors.NewProcessingError("[%s] error sending subtree to newSubtreeChan", oldSubtreeHash.String(), err)
		}
	}

	return nil
}

// Add adds a transaction node to the processor.
//
// Parameters:
//   - node: Transaction node to add
func (stp *SubtreeProcessor) Add(node subtreepkg.SubtreeNode, txInpoints subtreepkg.TxInpoints) {
	stp.queue.enqueue(&TxIDAndFee{node: node, txInpoints: txInpoints})
}

// AddDirectly adds a transaction node directly to the subtree processor without going through the queue.
// This method is used to add a transaction directly to the subtree processor without going through the queue.
// It is used for transactions that are already known to be valid and should be added immediately.
// This is useful for transactions that are part of the current block being processed.
//
// Parameters:
//   - node: Transaction node to add
//   - txInpoints: Transaction inpoints for the node
//
// Returns:
//   - error: Any error encountered during addition
func (stp *SubtreeProcessor) AddDirectly(node subtreepkg.SubtreeNode, txInpoints subtreepkg.TxInpoints) error {
	if _, ok := stp.currentTxMap.Get(node.Hash); ok {
		return errors.NewInvalidArgumentError("transaction already exists in currentTxMap")
	}

	return stp.addNode(node, &txInpoints, false)
}

// Remove prevents a transaction from being processed from the queue into a subtree, and removes it if already present.
// This can only take place before the delay time in the queue has passed.
//
// Parameters:
//   - hash: Hash of the transaction to remove
//
// Returns:
//   - error: Any error encountered during removal
func (stp *SubtreeProcessor) Remove(hash chainhash.Hash) error {
	// add to the removeMap to make sure it gets removed if processing
	// or if it comes in later after cleaning the subtrees
	if err := stp.removeMap.Put(hash); err != nil {
		return errors.NewProcessingError("error adding tx to remove map", err)
	}

	// send a remove request to the subtree processor, making sure it does not block
	go func() {
		stp.removeTxCh <- hash
	}()

	return nil
}

func (stp *SubtreeProcessor) removeTxFromSubtrees(ctx context.Context, hash chainhash.Hash) error {
	_, _, deferFn := tracing.Tracer("subtreeprocessor").Start(ctx, "removeTxFromSubtrees",
		tracing.WithParentStat(stp.stats),
		tracing.WithHistogram(prometheusSubtreeProcessorRemoveTx),
		tracing.WithLogMessage(stp.logger, "[SubtreeProcessor][removeTxFromSubtrees][%s] removing transaction from subtrees", hash),
	)

	defer deferFn()

	// find the transaction in the current and all chained subtrees
	foundIndex := stp.currentSubtree.NodeIndex(hash)
	foundSubtreeIndex := -1

	if foundIndex == -1 {
		// not found in the current subtree, check chained subtrees
		for subtreeIndex, subtree := range stp.chainedSubtrees {
			idx := subtree.NodeIndex(hash)
			if idx >= 0 {
				foundSubtreeIndex = subtreeIndex
				foundIndex = idx
			}
		}
	}

	if foundIndex >= 0 {
		// remove tx from the currentTxMap
		stp.currentTxMap.Delete(hash)

		// we found the transaction in a subtree
		if foundSubtreeIndex == -1 {
			// it was found in the current tree, remove it from there
			// further processing is not needed, as the subtrees in the chainedSubtrees are older than the current subtree
			return stp.currentSubtree.RemoveNodeAtIndex(foundIndex)
		}

		// it was found in a chained subtree, remove it from there and chain the subtrees again from the point it was removed
		// this is a bit more complex, as we need to remove the transaction from the subtree it is in and then make sure
		// the subtrees are chained correctly again
		if err := stp.chainedSubtrees[foundSubtreeIndex].RemoveNodeAtIndex(foundIndex); err != nil {
			return errors.NewProcessingError("[SubtreeProcessor][removeTxFromSubtrees][%s] error removing node from subtree", hash.String(), err)
		}

		// all the chained subtrees should be complete, as we now have a hole in the one we just removed from
		// we need to fill them up all again, including the current subtree
		if err := stp.reChainSubtrees(foundSubtreeIndex); err != nil {
			return errors.NewProcessingError("[SubtreeProcessor][removeTxFromSubtrees][%s] error rechaining subtrees", hash.String(), err)
		}
	}

	return nil
}

// reChainSubtrees will cycle through all subtrees from the given index and create new subtrees from the nodes
// in the same order as they were before
//
// Parameters:
//   - fromIndex: Starting index for rechaining
//
// Returns:
//   - error: Any error encountered during rechaining
func (stp *SubtreeProcessor) reChainSubtrees(fromIndex int) error {
	// copy the original subtrees from the given index into a new structure
	originalSubtrees := stp.chainedSubtrees[fromIndex:]
	originalSubtrees = append(originalSubtrees, stp.currentSubtree)

	// reset the chained subtrees and the current subtree
	stp.chainedSubtrees = stp.chainedSubtrees[:fromIndex]

	lenOriginalSubtreesInt32, err := safeconversion.IntToInt32(len(originalSubtrees))
	if err != nil {
		return errors.NewProcessingError("error converting original subtrees length", err)
	}

	stp.chainedSubtreeCount.Store(lenOriginalSubtreesInt32)

	stp.currentSubtree, _ = subtreepkg.NewTreeByLeafCount(stp.currentItemsPerFile)

	var (
		parents subtreepkg.TxInpoints
		found   bool
	)

	// add the nodes from the original subtrees to the new subtrees
	for _, subtree := range originalSubtrees {
		for _, node := range subtree.Nodes {
			// this adds to the current subtree
			parents, found = stp.currentTxMap.Get(node.Hash)
			if !found {
				// this should not happen, but if it does, we need to add the txInpoints to the currentTxMap
				return errors.NewProcessingError("error getting txInpoints from currentTxMap")
			}

			if err = stp.addNode(node, &parents, true); err != nil {
				return errors.NewProcessingError("error adding node to subtree", err)
			}
		}
	}

	return nil
}

// CheckSubtreeProcessor checks the integrity of the subtree processor.
// It verifies that all transactions in the current transaction map
// are present in the subtrees and that the size of the current transaction map
// matches the expected transaction count.
//
// Returns:
//   - error: Any error encountered during the check
func (stp *SubtreeProcessor) CheckSubtreeProcessor() error {
	errCh := make(chan error)

	stp.checkSubtreeProcessorCh <- errCh

	return <-errCh
}

// checkSubtreeProcessor performs a check on the subtree processor's state.
func (stp *SubtreeProcessor) checkSubtreeProcessor(errCh chan error) {
	stp.logger.Infof("[SubtreeProcessor] checking subtree processor")

	// check all the transactions in the currentTxMap are in the subtrees
	for subtreeIdx, subtree := range stp.chainedSubtrees {
		for nodeIdx, node := range subtree.Nodes {
			if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				// check that the coinbase placeholder is in the first subtree
				if subtreeIdx != 0 || nodeIdx != 0 {
					errCh <- errors.NewSubtreeError("[SubtreeProcessor] coinbase placeholder not in first subtree %d", subtreeIdx)

					break
				}

				continue
			}

			if _, ok := stp.currentTxMap.Get(node.Hash); !ok {
				errCh <- errors.NewSubtreeError("[SubtreeProcessor] tx %s from subtree %d not in currentTxMap", node.Hash.String(), subtreeIdx)

				break
			}
		}
	}

	// check all the transactions in the subtrees are in the currentTxMap
	for nodeIdx, node := range stp.currentSubtree.Nodes {
		if _, ok := stp.currentTxMap.Get(node.Hash); !ok {
			if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				// check that the coinbase placeholder is in the first subtree
				if nodeIdx != 0 {
					errCh <- errors.NewSubtreeError("[SubtreeProcessor] coinbase placeholder not in first node of subtree %d", nodeIdx)

					break
				}

				continue
			}

			errCh <- errors.NewSubtreeError("[SubtreeProcessor] tx %s from currentSubtree not in currentTxMap", node.Hash.String())

			break
		}
	}

	// make sure we have an up2date count of the transactions
	stp.setTxCountFromSubtrees()

	// check that the size of the currentTxMap is equal to the sum of all the subtrees - coinbase placeholder
	currentTxMapSize := stp.currentTxMap.Length()
	txCount := stp.TxCount()

	if currentTxMapSize != int(txCount)-1 { // nolint:gosec
		errCh <- errors.NewSubtreeError("[SubtreeProcessor] currentTxMap size %d does not match txCount %d", currentTxMapSize, txCount-1)
	}

	errCh <- nil

	stp.logger.Infof("[SubtreeProcessor] check subtree processor DONE")
}

// GetCompletedSubtreesForMiningCandidate retrieves all completed subtrees for block mining.
//
// Returns:
//   - []*util.Subtree: Array of completed subtrees
func (stp *SubtreeProcessor) GetCompletedSubtreesForMiningCandidate() []*subtreepkg.Subtree {
	stp.logger.Infof("GetCompletedSubtreesForMiningCandidate")

	var subtrees []*subtreepkg.Subtree

	subtreesChan := make(chan []*subtreepkg.Subtree)

	// get the subtrees from channel
	stp.getSubtreesChan <- subtreesChan

	subtrees = <-subtreesChan

	return subtrees
}

// MoveForwardBlock updates the subtrees when a new block is found.
//
// Parameters:
//   - block: Block to process
//
// Returns:
//   - error: Any error encountered during processing
func (stp *SubtreeProcessor) MoveForwardBlock(block *model.Block) error {
	errChan := make(chan error)

	stp.moveForwardBlockChan <- moveBlockRequest{
		block:   block,
		errChan: errChan,
	}

	return <-errChan
}

// Reorg handles blockchain reorganization by processing moved blocks.
//
// Parameters:
//   - moveBackBlocks: Blocks to move down in the chain
//   - moveForwardBlocks: Blocks to move up in the chain
//
// Returns:
//   - error: Any error encountered during reorganization
func (stp *SubtreeProcessor) Reorg(moveBackBlocks []*model.Block, moveForwardBlocks []*model.Block) error {
	errChan := make(chan error)
	stp.reorgBlockChan <- reorgBlocksRequest{
		moveBackBlocks:    moveBackBlocks,
		moveForwardBlocks: moveForwardBlocks,
		errChan:           errChan,
	}

	return <-errChan
}

// reorgBlocks adds all transactions that are in the block given to the current subtrees
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) reorgBlocks(ctx context.Context, moveBackBlocks []*model.Block, moveForwardBlocks []*model.Block) error {
	if moveBackBlocks == nil {
		return errors.NewProcessingError("you must pass in blocks to move down the chain")
	}

	if moveForwardBlocks == nil {
		return errors.NewProcessingError("you must pass in blocks to move up the chain")
	}

	stp.logger.Infof("reorgBlocks with %d moveBackBlocks and %d moveForwardBlocks", len(moveBackBlocks), len(moveForwardBlocks))

	for _, block := range moveBackBlocks {
		err := stp.moveBackBlock(ctx, block)
		if err != nil {
			return err
		}
	}

	for _, block := range moveForwardBlocks {
		// we skip the notifications for now and do them all at the end
		err := stp.moveForwardBlock(ctx, block, true)
		if err != nil {
			return err
		}
	}

	// announce all the subtrees to the network
	// this will also store it by the Server in the subtree store
	for _, subtree := range stp.chainedSubtrees {
		errCh := make(chan error)
		stp.newSubtreeChan <- NewSubtreeRequest{Subtree: subtree, ParentTxMap: stp.currentTxMap, ErrChan: errCh}

		err := <-errCh
		if err != nil {
			return errors.NewProcessingError("error sending subtree to newSubtreeChan", err)
		}
	}

	stp.setTxCountFromSubtrees()

	return nil
}

// setTxCountFromSubtrees recalculates the total transaction count
// by summing transactions across all subtrees, the current subtree,
// and the queue. This ensures the transaction count remains accurate
// after chain modifications.
func (stp *SubtreeProcessor) setTxCountFromSubtrees() {
	stp.txCount.Store(0)

	var (
		subtreeLen uint64
		err        error
	)

	for _, subtree := range stp.chainedSubtrees {
		subtreeLen, err = safeconversion.IntToUint64(subtree.Length())
		if err != nil {
			stp.logger.Errorf("error converting subtree length: %s", err)
			continue
		}

		stp.txCount.Add(subtreeLen)
	}

	currSubtreeLenUint64, err := safeconversion.IntToUint64(stp.currentSubtree.Length())
	if err != nil {
		stp.logger.Errorf("error converting current subtree length: %s", err)
		return
	}

	queueLenUint64, err := safeconversion.Int64ToUint64(stp.queue.length())
	if err != nil {
		stp.logger.Errorf("error converting queue length: %s", err)
		return
	}

	stp.txCount.Add(currSubtreeLenUint64)
	stp.txCount.Add(queueLenUint64)
}

// moveBackBlock processes a block during downward chain movement.
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: Block to process
//
// Returns:
//   - error: Any error encountered during processing
//
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) moveBackBlock(ctx context.Context, block *model.Block) (err error) {
	if block == nil {
		return errors.NewProcessingError("[moveBackBlock] you must pass in a block to moveBackBlock")
	}

	startTime := time.Now()

	prometheusSubtreeProcessorMoveBackBlock.Inc()

	// add all the transactions from the block, excluding the coinbase, which needs to be reverted in the utxo store
	stp.logger.Infof("[moveBackBlock][%s] with %d subtrees", block.String(), len(block.Subtrees))

	defer func() {
		stp.logger.Infof("[moveBackBlock][%s] with %d subtrees DONE in %s", block.String(), len(block.Subtrees), time.Since(startTime).String())

		err := recover()
		if err != nil {
			stp.logger.Errorf("[moveBackBlock] with block %s: %s", block.String(), err)
		}
	}()

	lastIncompleteSubtree := stp.currentSubtree
	chainedSubtrees := stp.chainedSubtrees

	// reset the subtree processor
	stp.currentSubtree, err = subtreepkg.NewTreeByLeafCount(stp.currentItemsPerFile)
	if err != nil {
		return errors.NewProcessingError("[moveBackBlock][%s] error creating new subtree", block.String(), err)
	}

	stp.chainedSubtrees = make([]*subtreepkg.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.chainedSubtreeCount.Store(0)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddCoinbaseNode()

	g, gCtx := errgroup.WithContext(ctx)

	util.SafeSetLimit(g, stp.settings.BlockAssembly.MoveBackBlockConcurrency)

	// get all the subtrees in parallel
	stp.logger.Warnf("[moveBackBlock][%s] with %d subtrees: get subtrees", block.String(), len(block.Subtrees))
	subtreesNodes := make([][]subtreepkg.SubtreeNode, len(block.Subtrees))
	subtreeMetaTxInpoints := make([][]subtreepkg.TxInpoints, len(block.Subtrees))

	for idx, subtreeHash := range block.Subtrees {
		idx := idx
		subtreeHash := subtreeHash

		g.Go(func() error {
			subtreeReader, err := stp.subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtree)
			if err != nil {
				subtreeReader, err = stp.subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
				if err != nil {
					return errors.NewServiceError("[moveBackBlock][%s] error getting subtree %s", block.String(), subtreeHash.String(), err)
				}
			}

			defer func() {
				_ = subtreeReader.Close()
			}()

			subtree := &subtreepkg.Subtree{}

			if err = subtree.DeserializeFromReader(subtreeReader); err != nil {
				return errors.NewProcessingError("[moveBackBlock][%s] error deserializing subtree", block.String(), err)
			}

			// TODO add metrics about how many txs we are reading per second
			subtreesNodes[idx] = subtree.Nodes

			subtreeMetaReader, err := stp.subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
			if err != nil {
				return errors.NewServiceError("[moveBackBlock][%s] error getting subtree meta %s", block.String(), subtreeHash.String(), err)
			}

			subtreeMeta, err := subtreepkg.NewSubtreeMetaFromReader(subtree, subtreeMetaReader)
			if err != nil {
				return errors.NewProcessingError("[moveBackBlock][%s] error deserializing subtree meta", block.String(), err)
			}

			subtreeMetaTxInpoints[idx] = subtreeMeta.TxInpoints

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("[moveBackBlock][%s] error getting subtrees", block.String(), err)
	}

	stp.logger.Warnf("[moveBackBlock][%s] with %d subtrees: get subtrees DONE", block.String(), len(block.Subtrees))

	stp.logger.Warnf("[moveBackBlock][%s] with %d subtrees: create new subtrees", block.String(), len(block.Subtrees))
	// run through the nodes of the subtrees in order and add to the new subtrees
	for idx, subtreeNode := range subtreesNodes {
		subtreeHash := block.Subtrees[idx]

		if idx == 0 {
			// process coinbase utxos
			if err = stp.utxoStore.Delete(ctx, block.CoinbaseTx.TxIDChainHash()); err != nil {
				return errors.NewServiceError("[moveBackBlock][%s][%s] error deleting utxos for tx %s", block.String(), block.CoinbaseTx.String(), subtreeHash.String(), err)
			}

			// skip the first transaction of the first subtree (coinbase)
			for i := 1; i < len(subtreeNode); i++ {
				if err = stp.addNode(subtreeNode[i], &subtreeMetaTxInpoints[idx][i], true); err != nil {
					return errors.NewProcessingError("[moveBackBlock][%s][%s] error adding node to subtree", block.String(), subtreeHash.String(), err)
				}
			}
		} else {
			for i, node := range subtreeNode {
				if err = stp.addNode(node, &subtreeMetaTxInpoints[idx][i], true); err != nil {
					return errors.NewProcessingError("[moveBackBlock][%s][%s] error adding node to subtree", block.String(), subtreeHash.String(), err)
				}
			}
		}
	}

	stp.logger.Warnf("[moveBackBlock][%s] with %d subtrees: create new subtrees DONE", block.String(), len(block.Subtrees))

	stp.logger.Warnf("[moveBackBlock][%s] with %d subtrees: add previous nodes to subtrees", block.String(), len(block.Subtrees))
	// add all the transactions from the previous state
	for _, subtree := range chainedSubtrees {
		for _, node := range subtree.Nodes {
			_ = stp.addNode(node, nil, true)
		}
	}

	// add all the transactions from the last incomplete subtree
	for _, node := range lastIncompleteSubtree.Nodes {
		_ = stp.addNode(node, nil, true)
	}

	stp.logger.Warnf("[moveBackBlock][%s] with %d subtrees: add previous nodes to subtrees DONE", block.String(), len(block.Subtrees))

	// we must set the current block header
	stp.currentBlockHeader = block.Header

	stp.setTxCountFromSubtrees()

	prometheusSubtreeProcessorMoveBackBlockDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	// Clear the block's processed timestamp
	if err := stp.blockchainClient.SetBlockProcessedAt(ctx, block.Header.Hash(), true); err != nil {
		// Don't return error here, as this is not critical for the operation
		stp.logger.Warnf("[moveBackBlock][%s] error clearing block processed_at timestamp: %v", block.String(), err)
	}

	return nil
}

// moveBackBlocks processes multiple blocks during downward chain movement.
// It adds all transactions from the given blocks to the current subtrees.
//
// Parameters:
//   - ctx: Context for cancellation
//   - blocks: Array of blocks to process
//
// Returns:
//   - error: Any error encountered during processing
func (stp *SubtreeProcessor) moveBackBlocks(ctx context.Context, blocks []*model.Block) (err error) {
	if len(blocks) == 0 || blocks[0] == nil {
		return errors.NewProcessingError("[moveBackBlocks] you must pass in a block to moveBackBlock")
	}

	startTime := time.Now()

	prometheusSubtreeProcessorMoveBackBlock.Inc()

	stp.logger.Infof("[moveBackBlocks] with %d blocks", len(blocks))

	lastIncompleteSubtree := stp.currentSubtree
	chainedSubtrees := stp.chainedSubtrees
	currentTxMap := stp.currentTxMap

	stp.currentSubtree, err = subtreepkg.NewTreeByLeafCount(stp.currentItemsPerFile)
	if err != nil {
		return errors.NewProcessingError("[moveBackBlocks] error creating new subtree", err)
	}

	stp.chainedSubtrees = make([]*subtreepkg.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.chainedSubtreeCount.Store(0)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddCoinbaseNode()

	for i := 0; i < len(blocks); i++ {
		if err = stp.moveBackBlockProcessBlock(ctx, blocks[i], i, startTime, currentTxMap); err != nil {
			return err
		}
	}

	stp.logger.Warnf("[moveBackBlocks] add previous nodes to subtrees")

	// add all the transactions from the previous state
	for _, subtree := range chainedSubtrees {
		for _, node := range subtree.Nodes {
			if !node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
				// we can set parent to nil, since the old subtree is already in the map
				if err = stp.addNode(node, nil, true); err != nil {
					return errors.NewProcessingError("[moveBackBlocks] error adding node to subtree", err)
				}
			}
		}
	}

	// add all the transactions from the last incomplete subtree
	for _, node := range lastIncompleteSubtree.Nodes {
		// we can set parent to nil, since the old subtree is already in the map
		if err = stp.addNode(node, nil, true); err != nil {
			return errors.NewProcessingError("[moveBackBlocks] error adding node to subtree", err)
		}
	}

	lastBlock := blocks[len(blocks)-1]

	stp.logger.Warnf("[moveBackBlock][%s] with %d subtrees: add previous nodes to subtrees DONE", lastBlock.String(), len(lastBlock.Subtrees))

	// we must set the current block header to the last block header we have added
	stp.currentBlockHeader = lastBlock.Header

	prometheusSubtreeProcessorMoveBackBlockDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	// Clear the block's processed timestamp
	if err = stp.blockchainClient.SetBlockProcessedAt(ctx, lastBlock.Header.Hash(), true); err != nil {
		// Don't return error here, as this is not critical for the operation
		stp.logger.Warnf("[moveBackBlock][%s] error clearing block processed_at timestamp: %v", lastBlock.String(), err)
	}

	return nil
}

func (stp *SubtreeProcessor) moveBackBlockProcessBlock(ctx context.Context, block *model.Block, i int, startTime time.Time,
	currentTxMap *txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints]) error {
	// add all the transactions from the block, excluding the coinbase, which needs to be reverted in the utxo store
	stp.logger.Infof("[moveBackBlocks][%s], block %d (block hash: %v), with %d subtrees", block.String(), i, block.Hash(), len(block.Subtrees))

	defer func() {
		stp.logger.Infof("[moveBackBlocks][%s], block %d (block hash: %v), with %d subtrees DONE in %s", block.String(), i, block.Hash(), len(block.Subtrees), time.Since(startTime).String())

		if err := recover(); err != nil {
			stp.logger.Errorf("[moveBackBlocks] with block %s: %s", block.String(), err)
		}
	}()

	// get all the subtrees in parallel
	stp.logger.Warnf("[moveBackBlocks][%s], block %d (block hash: %v)  with %d subtrees: get subtrees", block.String(), i, block.Hash(), len(block.Subtrees))
	subtreesNodes := make([][]subtreepkg.SubtreeNode, len(block.Subtrees))
	subtreeMetaParents := make(map[int][]subtreepkg.TxInpoints)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(stp.settings.BlockAssembly.MoveBackBlockConcurrency)

	for idx, subtreeHash := range block.Subtrees {
		idx := idx
		subtreeHash := subtreeHash

		g.Go(func() error {
			subtreeReader, err := stp.subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtree)
			if err != nil {
				subtreeReader, err = stp.subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
				if err != nil {
					return errors.NewServiceError("[moveBackBlocks][%s], block %d (block hash: %v), error getting subtree %s", block.String(), i, block.Hash(), subtreeHash.String(), err)
				}
			}

			defer func() {
				_ = subtreeReader.Close()
			}()

			subtree := &subtreepkg.Subtree{}

			if err = subtree.DeserializeFromReader(subtreeReader); err != nil {
				return errors.NewProcessingError("[moveBackBlocks][%s], block %d (block hash: %v), error deserializing subtree", block.String(), i, block.Hash(), err)
			}

			// TODO add metrics about how many txs we are reading per second
			subtreesNodes[idx] = subtree.Nodes

			subtreeMetaReader, err := stp.subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
			if err != nil {
				return errors.NewServiceError("[moveBackBlock][%s] error getting subtree meta %s", block.String(), subtreeHash.String(), err)
			}

			subtreeMeta, err := subtreepkg.NewSubtreeMetaFromReader(subtree, subtreeMetaReader)
			if err != nil {
				return errors.NewProcessingError("[moveBackBlock][%s] error deserializing subtree meta", block.String(), err)
			}

			subtreeMetaParents[idx] = subtreeMeta.TxInpoints

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("[moveBackBlocks][%s], block %d (block hash: %v), error getting subtrees", block.String(), i, block.Hash(), err)
	}

	stp.logger.Warnf("[moveBackBlocks][%s], block %d (block hash: %v), with %d subtrees: get subtrees DONE", block.String(), i, block.Hash(), len(block.Subtrees))

	stp.logger.Warnf("[moveBackBlocks][%s] with %d subtrees: create new subtrees", block.String(), i, block.Hash(), len(block.Subtrees))

	// run through the nodes of the subtrees in order and add to the new subtrees
	for idx, subtreeNode := range subtreesNodes {
		if idx == 0 {
			// process coinbase utxos
			if err := stp.utxoStore.Delete(ctx, block.CoinbaseTx.TxIDChainHash()); err != nil {
				return errors.NewServiceError("[moveBackBlocks][%s], block %d (block hash: %v), error deleting utxos for tx %s", block.String(), i, block.Hash(), block.CoinbaseTx.String(), err)
			}

			// skip the first transaction of the first subtree (coinbase)
			for i := 1; i < len(subtreeNode); i++ {
				if err := stp.addNode(subtreeNode[i], &subtreeMetaParents[idx][i], true); err != nil {
					return errors.NewProcessingError("[moveBackBlocks][%s], block %d (block hash: %v), error adding node to subtree", block.String(), i, block.Hash(), err)
				}
			}
		} else {
			for _, node := range subtreeNode {
				if err := stp.addNode(node, &subtreeMetaParents[idx][i], true); err != nil {
					return errors.NewProcessingError("[moveBackBlocks][%s], block %d (block hash: %v), error adding node to subtree", block.String(), i, block.Hash(), err)
				}
			}
		}
	}

	stp.logger.Warnf("[moveBackBlocks][%s], block %d (block hash: %v), with %d subtrees: create new subtrees DONE", block.String(), i, block.Hash(), len(block.Subtrees))

	return nil
}

// moveForwardBlock cleans out all transactions that are in the current subtrees and also in the block
// given. It is akin to moving up the blockchain to the next block.
func (stp *SubtreeProcessor) moveForwardBlock(ctx context.Context, block *model.Block, skipNotification bool) error {
	if block == nil {
		return errors.NewProcessingError("[moveForwardBlock] you must pass in a block to moveForwardBlock")
	}

	_, _, deferFn := tracing.Tracer("subtreeprocessor").Start(ctx, "moveForwardBlock",
		tracing.WithParentStat(stp.stats),
		tracing.WithCounter(prometheusSubtreeProcessorMoveForwardBlock),
		tracing.WithHistogram(prometheusSubtreeProcessorMoveForwardBlockDuration),
		tracing.WithLogMessage(stp.logger, "[moveForwardBlock][%s] with block", block.String()),
	)

	defer func() {
		deferFn()

		if err := recover(); err != nil {
			// print the stack trace
			stp.logger.Errorf("%s", debug.Stack())
			stp.logger.Errorf("[moveForwardBlock][%s] with block: %s", block.String(), err)
		}
	}()

	// TODO reactivate and test
	// if !block.Header.HashPrevBlock.IsEqual(stp.currentBlockHeader.Hash()) {
	//	return errors.NewProcessingError("the block passed in does not match the current block header: [%s] - [%s]", block.Header.StringDump(), stp.currentBlockHeader.StringDump())
	// }

	stp.logger.Debugf("[moveForwardBlock][%s] resetting subtrees: %v", block.String(), block.Subtrees)

	err := stp.processCoinbaseUtxos(ctx, block)
	if err != nil {
		return errors.NewProcessingError("[moveForwardBlock][%s] error processing coinbase utxos", block.String(), err)
	}

	// create a reverse lookup map of all the subtrees in the block
	blockSubtreesMap := make(map[chainhash.Hash]int, len(block.Subtrees))
	for idx, subtree := range block.Subtrees {
		blockSubtreesMap[*subtree] = idx
	}

	// get all the subtrees that were not in the block
	// this should clear out all subtrees from our own blocks, giving an empty blockSubtreesMap as a result
	// and preventing processing of the map
	chainedSubtrees := make([]*subtreepkg.Subtree, 0, ExpectedNumberOfSubtrees)
	for _, subtree := range stp.chainedSubtrees {
		id := *subtree.RootHash()
		if _, ok := blockSubtreesMap[id]; !ok {
			// only add the subtrees that were not in the block
			chainedSubtrees = append(chainedSubtrees, subtree)
		} else {
			// remove the subtree from the block subtrees map, we had it in our list
			delete(blockSubtreesMap, id)
		}
	}

	// clear the transaction ids from all the subtrees of the block that are left over
	var (
		transactionMap   txmap.TxMap
		conflictingNodes []chainhash.Hash
	)

	if len(blockSubtreesMap) > 0 {
		mapStartTime := time.Now()

		stp.logger.Debugf("[moveForwardBlock][%s] processing subtrees into transaction map", block.String())

		if transactionMap, conflictingNodes, err = stp.CreateTransactionMap(ctx, blockSubtreesMap, len(block.Subtrees)); err != nil {
			// TODO revert the created utxos
			return errors.NewProcessingError("[moveForwardBlock][%s] error creating transaction map", block.String(), err)
		}

		stp.logger.Debugf("[moveForwardBlock][%s] processing subtrees into transaction map DONE in %s: %d", block.String(), time.Since(mapStartTime).String(), transactionMap.Length())
	}

	var losingTxHashesMap txmap.TxMap

	// process conflicting txs
	if len(conflictingNodes) > 0 {
		// before we process the conflicting transactions, we need to make sure this block has been marked as mined
		// that would mean any previous block is also marked as mined and the data should be in a correct state
		// we can then process the conflicting transactions
		_, err = stp.waitForBlockBeingMined(ctx, block.Header.Hash())
		if err != nil {
			return errors.NewProcessingError("[moveForwardBlock][%s] error waiting for block to be mined", block.String(), err)
		}

		if losingTxHashesMap, err = utxostore.ProcessConflicting(ctx, stp.utxoStore, conflictingNodes); err != nil {
			return errors.NewProcessingError("[moveForwardBlock][%s] error processing conflicting transactions", block.String(), err)
		}

		if losingTxHashesMap.Length() > 0 {
			// mark all the losing txs in the subtrees in the blocks they were mined into as conflicting
			if err = stp.markConflictingTxsInSubtrees(ctx, losingTxHashesMap); err != nil {
				return errors.NewProcessingError("[moveForwardBlock][%s] error marking conflicting transactions", block.String(), err)
			}
		}
	}

	_ = conflictingNodes

	// reset the current subtree
	currentSubtree := stp.currentSubtree

	// reset the currentTxMap
	currentTxMap := stp.currentTxMap
	stp.currentTxMap = txmap.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()

	stp.currentSubtree, err = subtreepkg.NewTreeByLeafCount(stp.currentItemsPerFile)
	if err != nil {
		return errors.NewProcessingError("[moveForwardBlock][%s] error creating new subtree", block.String(), err)
	}

	stp.chainedSubtrees = make([]*subtreepkg.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.chainedSubtreeCount.Store(0)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddCoinbaseNode()

	remainderStartTime := time.Now()

	stp.logger.Debugf("[moveForwardBlock][%s] processing remainder tx hashes into subtrees", block.String())

	var (
		nodeParents subtreepkg.TxInpoints
		found       bool
	)

	if transactionMap != nil && transactionMap.Length() > 0 {
		remainderSubtrees := make([]*subtreepkg.Subtree, 0, len(chainedSubtrees)+1)

		remainderSubtrees = append(remainderSubtrees, chainedSubtrees...)
		remainderSubtrees = append(remainderSubtrees, currentSubtree)

		remainderTxHashesStartTime := time.Now()

		stp.logger.Debugf("[moveForwardBlock][%s] processRemainderTxHashes with %d subtrees", block.String(), len(chainedSubtrees))

		if err = stp.processRemainderTxHashes(ctx, remainderSubtrees, transactionMap, losingTxHashesMap, currentTxMap, skipNotification); err != nil {
			return errors.NewProcessingError("[moveForwardBlock][%s] error getting remainder tx hashes", block.String(), err)
		}

		stp.logger.Debugf("[moveForwardBlock][%s] processRemainderTxHashes with %d subtrees DONE in %s", block.String(), len(chainedSubtrees), time.Since(remainderTxHashesStartTime).String())

		// empty the queue to make sure we have all the transactions that could be in the block
		// we only have to do this when we have a transaction map, because otherwise we would be processing our own block
		dequeueStartTime := time.Now()

		stp.logger.Debugf("[moveForwardBlock][%s] processing queue while moveForwardBlock: %d", block.String(), stp.queue.length())

		err = stp.moveForwardBlockDeQueue(transactionMap, losingTxHashesMap, skipNotification)
		if err != nil {
			return errors.NewProcessingError("[moveForwardBlock][%s] error moving up block deQueue", block.String(), err)
		}

		stp.logger.Debugf("[moveForwardBlock][%s] processing queue while moveForwardBlock DONE in %s", block.String(), time.Since(dequeueStartTime).String())
	} else {
		// TODO find a way to this in parallel
		// there were no subtrees in the block, that were not in our block assembly
		// this was most likely our own block
		removeMapLength := stp.removeMap.Length()

		coinbaseID := block.CoinbaseTx.TxIDChainHash()

		for _, subtree := range chainedSubtrees {
			for _, node := range subtree.Nodes {
				// TODO is all this needed? This adds a lot to the processing time
				if !node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
					if removeMapLength > 0 && stp.removeMap.Exists(node.Hash) {
						if err = stp.removeMap.Delete(node.Hash); err != nil {
							stp.logger.Errorf("[moveForwardBlock][%s] error removing tx from remove map: %s", block.String(), err.Error())
						}
					} else {
						if nodeParents, found = currentTxMap.Get(node.Hash); !found {
							return errors.NewProcessingError("[moveForwardBlock][%s] error getting node txInpoints from currentTxMap for %s", block.String(), node.Hash.String())
						}

						if err = stp.addNode(node, &nodeParents, skipNotification); err != nil {
							return errors.NewProcessingError("[moveForwardBlock][%s] error adding node %s to subtree", block.String(), node.Hash.String(), err)
						}
					}
				}
			}
		}

		for _, node := range currentSubtree.Nodes {
			// TODO is all this needed? This adds a lot to the processing time
			if !node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
				if !coinbaseID.Equal(node.Hash) {
					if removeMapLength > 0 && stp.removeMap.Exists(node.Hash) {
						if err = stp.removeMap.Delete(node.Hash); err != nil {
							stp.logger.Errorf("[moveForwardBlock][%s] error removing tx from remove map: %s", block.String(), err.Error())
						}
					} else {
						if nodeParents, found = currentTxMap.Get(node.Hash); !found {
							return errors.NewProcessingError("[moveForwardBlock][%s] error getting node txInpoints from currentTxMap for %s", block.String(), node.Hash.String())
						}

						if err = stp.addNode(node, &nodeParents, skipNotification); err != nil {
							return errors.NewProcessingError("[moveForwardBlock][%s] error adding node %s to subtree", block.String(), node.Hash.String(), err)
						}
					}
				}
			}
		}
	}

	stp.logger.Debugf("[moveForwardBlock][%s] processing remainder tx hashes into subtrees DONE in %s", block.String(), time.Since(remainderStartTime).String())

	// set the correct count of the current subtrees
	stp.setTxCountFromSubtrees()

	// set the current block header
	stp.currentBlockHeader = block.Header

	// When starting a new block, calculate the average interval per subtree
	// from the previous block and adjust the subtree size
	if stp.currentBlockHeader != nil && stp.blockStartTime != (time.Time{}) {
		blockDuration := time.Since(stp.blockStartTime)

		if stp.subtreesInBlock > 0 {
			avgIntervalPerSubtree := blockDuration / time.Duration(stp.subtreesInBlock)
			stp.blockIntervals = append(stp.blockIntervals, avgIntervalPerSubtree)

			if len(stp.blockIntervals) > stp.maxBlockSamples {
				stp.blockIntervals = stp.blockIntervals[1:]
			}
		}
	}

	stp.adjustSubtreeSize()

	// Mark the block as processed
	if err = stp.blockchainClient.SetBlockProcessedAt(ctx, block.Header.Hash()); err != nil {
		// Don't return error here, as this is not critical for the operation
		stp.logger.Warnf("[moveForwardBlock][%s] error setting block processed_at timestamp: %v", block.String(), err)
	}

	return nil
}

func (stp *SubtreeProcessor) waitForBlockBeingMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	// try to wait for the block to be mined for maximum 30 sec
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return false, errors.NewProcessingError("[waitForBlockBeingMined] block not mined within 30 seconds", nil)
		default:
			blockMined, err := stp.blockchainClient.GetBlockIsMined(ctx, blockHash)
			if err != nil {
				return false, errors.NewProcessingError("[waitForBlockBeingMined] error getting block mined status", err)
			}

			if blockMined {
				return true, nil
			}

			time.Sleep(1 * time.Second)
		}
	}
}

// moveForwardBlockDeQueue processes the transaction queue during block movement.
//
// Parameters:
//   - transactionMap: Map of transactions that were in the block and need to be removed
//   - losingTxHashesMap: Map of transactions that were conflicting and need to be removed
//   - skipNotification: Whether to skip notification of new subtrees
//
// Returns:
//   - error: Any error encountered during processing
func (stp *SubtreeProcessor) moveForwardBlockDeQueue(transactionMap, losingTxHashesMap txmap.TxMap, skipNotification bool) (err error) {
	queueLength := stp.queue.length()
	if queueLength > 0 {
		nrProcessed := int64(0)
		validFromMillis := time.Now().Add(-1 * stp.settings.BlockAssembly.DoubleSpendWindow).UnixMilli()

		for {
			// TODO make sure to add the time delay here when activated
			item := stp.queue.dequeue(validFromMillis)
			if item == nil {
				break
			}

			if !transactionMap.Exists(item.node.Hash) && (losingTxHashesMap == nil || !losingTxHashesMap.Exists(item.node.Hash)) {
				_ = stp.addNode(item.node, &item.txInpoints, skipNotification)
			}

			nrProcessed++
			if nrProcessed > queueLength {
				break
			}
		}
	}

	return nil
}

// DeDuplicateTransactions removes duplicate transactions from the processor.
func (stp *SubtreeProcessor) DeDuplicateTransactions() {
	stp.deDuplicateTransactionsCh <- struct{}{}
}

// deDuplicateTransactions removes duplicate transactions from all subtrees
// and reconstructs the subtree chain with unique transactions only.
// This operation modifies the internal state of the processor.

func (stp *SubtreeProcessor) deDuplicateTransactions() {
	var err error

	stp.logger.Infof("[DeDuplicateTransactions] de-duplicating transactions")

	currentSubtree := stp.currentSubtree
	chainedSubtrees := stp.chainedSubtrees

	// reset the current subtree
	stp.currentSubtree, err = subtreepkg.NewTreeByLeafCount(stp.currentItemsPerFile)
	if err != nil {
		stp.logger.Errorf("[DeDuplicateTransactions] error creating new subtree in de-duplication: %s", err.Error())
		return
	}

	stp.chainedSubtrees = make([]*subtreepkg.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.chainedSubtreeCount.Store(0)

	txCountUint32, err := safeconversion.Uint64ToUint32(stp.txCount.Load())
	if err != nil {
		stp.logger.Errorf("[DeDuplicateTransactions] error converting tx count: %s", err.Error())
		return
	}

	deDuplicationMap := txmap.NewSplitSwissMapUint64(txCountUint32)
	removeMapLength := stp.removeMap.Length()

	for subtreeIdx, subtree := range chainedSubtrees {
		for nodeIdx, node := range subtree.Nodes {
			if removeMapLength > 0 && stp.removeMap.Exists(node.Hash) {
				if err = stp.removeMap.Delete(node.Hash); err != nil {
					stp.logger.Errorf("[DeDuplicateTransactions] error removing tx from remove map: %s", err.Error())
				}
			} else {
				subtreeSizeUint64, err := safeconversion.IntToUint64(subtreeIdx*subtree.Size() + nodeIdx)
				if err != nil {
					stp.logger.Errorf("[DeDuplicateTransactions] error converting subtree size: %s", err.Error())
					return
				}

				if err = deDuplicationMap.Put(node.Hash, subtreeSizeUint64); err != nil {
					stp.logger.Errorf("[DeDuplicateTransactions] found duplicate transaction in block assembly: %s - %v", node.Hash.String(), err)
				} else {
					_ = stp.addNode(node, nil, false)
				}
			}
		}
	}

	for nodeIdx, node := range currentSubtree.Nodes {
		if removeMapLength > 0 && stp.removeMap.Exists(node.Hash) {
			if err = stp.removeMap.Delete(node.Hash); err != nil {
				stp.logger.Errorf("[DeDuplicateTransactions] error removing tx from remove map: %s", err.Error())
			}
		} else {
			if err = deDuplicationMap.Put(node.Hash, uint64(nodeIdx)); err != nil { //nolint:gosec
				stp.logger.Errorf("[DeDuplicateTransactions] found duplicate transaction in block assembly: %s - %v", node.Hash.String(), err)
			} else {
				_ = stp.addNode(node, nil, false)
			}
		}
	}

	stp.setTxCountFromSubtrees()

	stp.logger.Infof("[DeDuplicateTransactions] de-duplicating transactions DONE")
}

// processCoinbaseUtxos processes UTXOs from coinbase transactions.
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: Block containing the coinbase transaction
//
// Returns:
//   - error: Any error encountered during processing
func (stp *SubtreeProcessor) processCoinbaseUtxos(ctx context.Context, block *model.Block) error {
	startTime := time.Now()

	prometheusSubtreeProcessorProcessCoinbaseTx.Inc()

	if block == nil || block.CoinbaseTx == nil {
		log.Printf("********************************************* block or coinbase is nil")
		return nil
	}

	utxos, err := utxostore.GetUtxoHashes(block.CoinbaseTx)
	if err != nil {
		return errors.NewProcessingError("[SubtreeProcessor][coinbase:%s] error extracting coinbase utxos", block.CoinbaseTx.TxIDChainHash(), err)
	}

	for _, u := range utxos {
		stp.logger.Debugf("[SubtreeProcessor][coinbase:%s] store utxo: %s", block.CoinbaseTx.TxIDChainHash(), u.String())
	}

	blockHeight := block.Height
	if blockHeight <= 0 {
		// lookup the block height for the current block from the blockchain service, we cannot rely on the block height
		// that is set in the utxo store, since that is the height of the current block in block assembly, which might
		// not be the same
		blockHeight = stp.utxoStore.GetBlockHeight()
		if blockHeight == 0 {
			return errors.NewServiceError("[SubtreeProcessor][coinbase:%s] error extracting coinbase height via utxo store", block.CoinbaseTx.TxIDChainHash())
		}
	}

	stp.logger.Debugf("[SubtreeProcessor][%s] height %d storeCoinbaseTx %s blockID %d", block.Header.Hash().String(), blockHeight, block.CoinbaseTx.TxIDChainHash().String(), block.ID)
	// we pass in the block height we are working on here, since the utxo store will recognize the tx as
	// a coinbase and add the correct spending height, which should be + 99
	if _, err = stp.utxoStore.Create(
		ctx,
		block.CoinbaseTx,
		blockHeight,
		utxostore.WithMinedBlockInfo(
			utxostore.MinedBlockInfo{
				BlockID:     block.ID,
				BlockHeight: blockHeight,
				SubtreeIdx:  0, // Coinbase is always the first transaction in the first subtree
			}),
	); err != nil {
		if errors.Is(err, errors.ErrTxExists) {
			// This will also be called for the 2 coinbase transactions that are duplicated on the network
			// These transactions were created twice:
			//   e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468
			//   d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599
			stp.logger.Warnf("[SubtreeProcessor] coinbase utxos for %s already exist. Skipping", block.CoinbaseTx.TxIDChainHash())
		} else {
			stp.logger.Errorf("[SubtreeProcessor] error storing utxos: %v", err)
			return err
		}
	}

	prometheusSubtreeProcessorProcessCoinbaseTxDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return nil
}

// processRemainderTxHashes processes remaining transaction hashes after reorganization.
//
// Parameters:
//   - ctx: Context for cancellation
//   - chainedSubtrees: List of subtrees to process
//   - transactionMap: Map of transactions that were in the block and need to be removed
//   - losingTxHashesMap: Map of transactions that were conflicting and need to be removed
//   - skipNotification: Whether to skip notification of new subtrees
//
// Returns:
//   - error: Any error encountered during processing
func (stp *SubtreeProcessor) processRemainderTxHashes(ctx context.Context, chainedSubtrees []*subtreepkg.Subtree,
	transactionMap, losingTxHashesMap txmap.TxMap, currentTxMap *txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints], skipNotification bool) error {
	var hashCount atomic.Int64

	// clean out the transactions from the old current subtree that were in the block
	// and add the remainderSubtreeNodes to the new current subtree
	g, _ := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, stp.settings.BlockAssembly.ProcessRemainderTxHashesConcurrency)

	// we need to process this in order, so we first process all subtrees in parallel, but keeping the order
	remainderSubtrees := make([][]subtreepkg.SubtreeNode, len(chainedSubtrees))
	removeMapLength := stp.removeMap.Length()

	for idx, subtree := range chainedSubtrees {
		idx := idx
		st := subtree

		g.Go(func() error {
			remainderSubtrees[idx] = make([]subtreepkg.SubtreeNode, 0, len(st.Nodes)/10) // expect max 10% of the nodes to be different
			// don't use the util function, keep the memory local in this function, no jumping between heap and stack
			// err = st.Difference(transactionMap, &remainderSubtrees[idx])
			for _, node := range st.Nodes {
				if !node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
					if removeMapLength > 0 && stp.removeMap.Exists(node.Hash) {
						if err := stp.removeMap.Delete(node.Hash); err != nil {
							stp.logger.Errorf("[SubtreeProcessor] error removing tx from remove map: %s", err.Error())
						}
					} else if !transactionMap.Exists(node.Hash) && (losingTxHashesMap == nil || !losingTxHashesMap.Exists(node.Hash)) {
						remainderSubtrees[idx] = append(remainderSubtrees[idx], node)
					}
				}
			}

			hashCount.Add(int64(len(remainderSubtrees[idx])))

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("error getting remainder tx difference", err)
	}

	// add all found tx hashes to the final list, in order
	for _, subtreeNodes := range remainderSubtrees {
		for _, node := range subtreeNodes {
			parents, ok := currentTxMap.Get(node.Hash)
			if !ok {
				return errors.NewProcessingError("[processRemainderTxHashes] error getting node txInpoints from currentTxMap for %s", node.Hash.String())
			}

			_ = stp.addNode(node, &parents, skipNotification)
		}
	}

	return nil
}

// CreateTransactionMap creates a map of transactions from the provided subtrees.
//
// Parameters:
//   - ctx: Context for cancellation
//   - blockSubtreesMap: Map of subtree hashes to their indices
//
// Returns:
//   - util.TxMap: Created transaction map
//   - error: Any error encountered during map creation
func (stp *SubtreeProcessor) CreateTransactionMap(ctx context.Context, blockSubtreesMap map[chainhash.Hash]int,
	totalSubtreesInBlock int) (txmap.TxMap, []chainhash.Hash, error) {
	startTime := time.Now()

	prometheusSubtreeProcessorCreateTransactionMap.Inc()

	concurrentSubtreeReads := stp.settings.BlockAssembly.SubtreeProcessorConcurrentReads

	// TODO this bit is slow !
	stp.logger.Infof("CreateTransactionMap with %d subtrees, concurrency %d", len(blockSubtreesMap), concurrentSubtreeReads)

	mapSize := len(blockSubtreesMap) * stp.currentItemsPerFile // TODO fix this assumption, should be gleaned from the block
	transactionMap := txmap.NewSplitSwissMap(mapSize)

	conflictingNodesPerSubtree := make([][]chainhash.Hash, totalSubtreesInBlock)

	g, ctx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, concurrentSubtreeReads)

	// get all the subtrees from the block that we have not yet cleaned out
	for subtreeHash, subtreeIdx := range blockSubtreesMap {
		st := subtreeHash

		g.Go(func() error {
			stp.logger.Debugf("getting subtree: %s", st.String())

			subtreeReader, err := stp.subtreeStore.GetIoReader(ctx, st[:], fileformat.FileTypeSubtree)
			if err != nil {
				subtreeReader, err = stp.subtreeStore.GetIoReader(ctx, st[:], fileformat.FileTypeSubtreeToCheck)
				if err != nil {
					return errors.NewServiceError("error getting subtree: %s", st.String(), err)
				}
			}

			// TODO add metrics about how many txs we are reading per second
			txHashBuckets, conflictingNodes, err := DeserializeHashesFromReaderIntoBuckets(subtreeReader, transactionMap.Buckets())
			if err != nil {
				return errors.NewProcessingError("error deserializing subtree: %s", st.String(), err)
			}

			bucketG := errgroup.Group{}

			for bucket, hashes := range txHashBuckets {
				bucket := bucket
				hashes := hashes
				// put the hashes into the transaction map in parallel, it has already been split into the correct buckets
				bucketG.Go(func() error {
					_ = transactionMap.PutMultiBucket(bucket, hashes, 0)
					return nil
				})
			}

			if err = bucketG.Wait(); err != nil {
				return errors.NewProcessingError("error putting hashes into transaction map", err)
			}

			conflictingNodesPerSubtree[subtreeIdx] = conflictingNodes

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, errors.NewProcessingError("error getting subtrees", err)
	}

	conflictingNodesPerSubtreeCount := 0
	for _, subtreeConflictingNodes := range conflictingNodesPerSubtree {
		conflictingNodesPerSubtreeCount += len(subtreeConflictingNodes)
	}

	conflictingNodes := make([]chainhash.Hash, 0, conflictingNodesPerSubtreeCount)

	for _, subtreeConflictingNodes := range conflictingNodesPerSubtree {
		if subtreeConflictingNodes != nil {
			conflictingNodes = append(conflictingNodes, subtreeConflictingNodes...)
		}
	}

	stp.logger.Infof("CreateTransactionMap with %d subtrees DONE", len(blockSubtreesMap))

	prometheusSubtreeProcessorCreateTransactionMapDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return transactionMap, conflictingNodes, nil
}

func (stp *SubtreeProcessor) markConflictingTxsInSubtrees(ctx context.Context, losingTxHashesMap txmap.TxMap) error {
	if losingTxHashesMap == nil || losingTxHashesMap.Length() == 0 {
		return nil
	}

	blockIdsMap, err := stp.getBLockIDsMap(ctx, losingTxHashesMap)
	if err != nil {
		return err
	}

	g, gCtx := errgroup.WithContext(ctx)

	// limit the number of concurrent writes to the subtrees
	util.SafeSetLimit(g, 32)

	// mark all the losing txs in the subtrees in the blocks they were mined into as conflicting
	for blockID, txHashes := range blockIdsMap {
		// get the block
		block, err := stp.blockchainClient.GetBlockByID(ctx, uint64(blockID))
		if err != nil {
			return errors.NewServiceError("error getting block %d", blockID)
		}

		// get the subtrees
		for _, subtreeHash := range block.Subtrees {
			subtreeHash := subtreeHash

			g.Go(func() error {
				subtree, conflictingTransactionsMap, err := stp.getSubtreeAndConflictingTransactionsMap(ctx, subtreeHash, txHashes)
				if err != nil {
					return err
				}

				// if we marked at least 1 node as conflicting, we should save the subtree again
				if len(conflictingTransactionsMap) > 0 {
					// create a slice of the conflicting nodes
					conflictingNodes := make([]chainhash.Hash, 0, len(conflictingTransactionsMap))
					for txHash := range conflictingTransactionsMap {
						conflictingNodes = append(conflictingNodes, txHash)
					}

					// sort the conflicting nodes by their index in the subtree
					slices.SortFunc(conflictingNodes, func(i, j chainhash.Hash) int {
						return conflictingTransactionsMap[i] - conflictingTransactionsMap[j]
					})

					// mark the transaction as conflicting in order of idx
					for _, txHash := range conflictingNodes {
						if err = subtree.AddConflictingNode(txHash); err != nil {
							return errors.NewProcessingError("error adding conflicting node %s to subtree %s", txHash.String(), subtreeHash.String(), err)
						}
					}

					subtreeBytes, err := subtree.Serialize()
					if err != nil {
						return errors.NewProcessingError("error serializing subtree %s", subtreeHash.String(), err)
					}

					if err = stp.subtreeStore.Set(gCtx,
						subtreeHash[:],
						fileformat.FileTypeSubtree,
						subtreeBytes,
						options.WithAllowOverwrite(true),
					); err != nil {
						return errors.NewServiceError("error saving subtree %s", subtreeHash.String(), err)
					}
				}

				return nil
			})
		}
	}

	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("error marking conflicting txs in subtrees", err)
	}

	return nil
}

func (stp *SubtreeProcessor) getSubtreeAndConflictingTransactionsMap(ctx context.Context, subtreeHash *chainhash.Hash, txHashes []chainhash.Hash) (*subtreepkg.Subtree, map[chainhash.Hash]int, error) {
	subtreeReader, err := stp.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		subtreeReader, err = stp.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return nil, nil, errors.NewServiceError("error getting subtree %s", subtreeHash.String())
		}
	}

	subtree := &subtreepkg.Subtree{}
	if err = subtree.DeserializeFromReader(subtreeReader); err != nil {
		return nil, nil, errors.NewProcessingError("error deserializing subtree %s", subtreeHash.String(), err)
	}

	conflictingTransactionsMap := make(map[chainhash.Hash]int, len(txHashes))

	for _, txHash := range txHashes {
		idx := subtree.NodeIndex(txHash)
		if idx >= 0 {
			conflictingTransactionsMap[txHash] = idx
		}
	}

	return subtree, conflictingTransactionsMap, nil
}

func (stp *SubtreeProcessor) getBLockIDsMap(ctx context.Context, losingTxHashesMap txmap.TxMap) (map[uint32][]chainhash.Hash, error) {
	// get all the blocks these transactions were mined into
	blockIdsMap := make(map[uint32][]chainhash.Hash)
	blockIdsMapMu := sync.Mutex{}

	g, gCtx := errgroup.WithContext(ctx)

	for _, txHash := range losingTxHashesMap.Keys() {
		txHash := txHash

		g.Go(func() error {
			txMeta, err := stp.utxoStore.Get(gCtx, &txHash, fields.BlockIDs)
			if err != nil {
				return errors.NewServiceError("error getting utxos for tx %s", txHash.String())
			}

			blockIdsMapMu.Lock()

			for _, blockID := range txMeta.BlockIDs {
				if _, ok := blockIdsMap[blockID]; !ok {
					blockIdsMap[blockID] = make([]chainhash.Hash, 0, 1024)
				}

				blockIdsMap[blockID] = append(blockIdsMap[blockID], txHash)
			}

			blockIdsMapMu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, errors.NewProcessingError("error getting mined blocks for conflicting txs", err)
	}

	return blockIdsMap, nil
}

// DeserializeHashesFromReaderIntoBuckets deserializes transaction hashes from a reader into buckets.
//
// Parameters:
//   - reader: Source reader containing hash data
//   - nBuckets: Number of buckets to distribute hashes into
//
// Returns:
//   - map[uint16][][32]byte: Map of bucketed hash arrays
//   - error: Any error encountered during deserialization
func DeserializeHashesFromReaderIntoBuckets(reader io.Reader, nBuckets uint16) (hashes map[uint16][]chainhash.Hash, conflictingNodes []chainhash.Hash, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in DeserializeHashesFromReaderIntoBuckets: %v", r)
		}
	}()

	buf := bufio.NewReaderSize(reader, 1024*1024*16) // 16MB buffer

	if _, err = buf.Discard(48); err != nil { // skip headers
		return nil, nil, errors.NewProcessingError("unable to read header", err)
	}

	// read number of leaves
	bytes8 := make([]byte, 8)
	if _, err = io.ReadFull(buf, bytes8); err != nil {
		return nil, nil, errors.NewProcessingError("unable to read number of leaves", err)
	}

	numLeaves := binary.LittleEndian.Uint64(bytes8)

	// read leaves
	hashes = make(map[uint16][]chainhash.Hash, nBuckets)
	for i := uint16(0); i < nBuckets; i++ {
		hashes[i] = make([]chainhash.Hash, 0, int(math.Ceil(float64(numLeaves/uint64(nBuckets))*1.1)))
	}

	var bucket uint16

	bytes48 := make([]byte, 48)

	for i := uint64(0); i < numLeaves; i++ {
		// read all the node data in 1 go
		if _, err = io.ReadFull(buf, bytes48); err != nil {
			return nil, nil, errors.NewProcessingError("unable to read node", err)
		}

		bucket = txmap.Bytes2Uint16Buckets(chainhash.Hash(bytes48[:32]), nBuckets)
		hashes[bucket] = append(hashes[bucket], chainhash.Hash(bytes48[:32]))
	}

	conflictingNodes = make([]chainhash.Hash, 0, 1024)

	// read conflicting txs
	if _, err = io.ReadFull(buf, bytes8); err != nil {
		return nil, nil, errors.NewProcessingError("unable to read number of conflicting txs", err)
	}

	numConflicting := binary.LittleEndian.Uint64(bytes8)

	if numConflicting > 0 {
		bytes32 := make([]byte, 32)
		for i := uint64(0); i < numConflicting; i++ {
			if _, err = io.ReadFull(buf, bytes32); err != nil {
				return nil, nil, errors.NewProcessingError("unable to read node", err)
			}

			conflictingNodes = append(conflictingNodes, chainhash.Hash(bytes32))
		}
	}

	return hashes, conflictingNodes, nil
}
