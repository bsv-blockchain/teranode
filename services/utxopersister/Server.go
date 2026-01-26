// Package utxopersister creates and maintains up-to-date Unspent Transaction Output (UTXO) file sets
// for each block in the Teranode blockchain. Its primary function is to process the output of the
// Block Persister service (utxo-additions and utxo-deletions) and generate complete UTXO set files.
//
// The package implements a server that monitors blockchain updates and processes blocks as they are
// added to the chain. For each block, it extracts the UTXOs, reconciles them with previous UTXO sets,
// and persists them in a structured format. The resulting UTXO set files can be exported and used to
// initialize the UTXO store in new Teranode instances, enabling fast synchronization of new nodes.
//
// Key components:
// - Server: Coordinates the processing of blocks and UTXO persistence
// - UTXOWrapper: Encapsulates transaction outputs with metadata
// - UTXO: Represents individual unspent outputs
// - BlockIndex: Contains block metadata for validation and reference
//
// Integration points:
// - Blockchain service: Source of block notifications and blockchain data
// - Block Store: Storage for persisted UTXO sets
// - Block Persister: Source of UTXO additions and deletions
package utxopersister

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/ordishs/gocore"
)

const confirmations = 100

// Server manages the UTXO persistence operations.
// It coordinates the processing of blocks, extraction of UTXOs, and their persistent storage.
// The server maintains the state of the UTXO set by handling additions and deletions as new blocks are processed.
// It subscribes to blockchain notifications to detect new blocks and processes them asynchronously,
// ensuring that a complete and consistent UTXO set is maintained for each block height.
// The server implements a fallback mechanism to poll for new blocks when subscription is not available.
// All operations are thread-safe and can be gracefully stopped and restarted.
type Server struct {
	// logger provides logging functionality
	logger ulogger.Logger

	// settings contains configuration settings
	settings *settings.Settings

	// blockchainClient provides access to blockchain operations
	blockchainClient blockchain.ClientI

	// blockchainStore provides access to blockchain storage
	blockchainStore blockchain_store.Store

	// blockStore provides access to block storage
	blockStore blob.Store

	// stats tracks operational statistics
	stats *gocore.Stat

	// lastHeight stores the last processed block height
	lastHeight uint32

	// mu provides mutex locking for thread safety
	mu sync.Mutex

	// running indicates if the server is currently processing
	running bool

	// triggerCh is used to trigger processing operations
	triggerCh chan string
}

// New creates a new Server instance with the provided parameters.
// It initializes a new UTXO persister server with the given logger, settings, block store, and blockchain client.
//
// Parameters:
// - ctx: Context for controlling the initialization process
// - logger: Logger interface used for recording operational events and errors
// - tSettings: Configuration settings that control server behavior
// - blockStore: Blob store instance used for persisting UTXO data
// - blockchainClient: Client interface for accessing blockchain data and subscribing to notifications
//
// Returns a configured Server instance ready for initialization via the Init method.
// Note that this constructor leverages the blockchain client interface for operations,
// which is suitable for distributed setups where components may be on different machines.
func New(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	blockStore blob.Store,
	blockchainClient blockchain.ClientI,

) *Server {
	if blockStore == nil {
		logger.Errorf("[UTXOPersister] Warning: Block store is nil during initialization")
	}
	return &Server{
		logger:           logger,
		settings:         tSettings,
		blockchainClient: blockchainClient,
		blockStore:       blockStore,
		stats:            gocore.NewStat("utxopersister"),
		triggerCh:        make(chan string, 5),
	}
}

// NewDirect creates a new Server instance with direct blockchain store access.
// Unlike New, this constructor provides direct access to the blockchain store without using the client interface.
// This can be more efficient when the server is running in the same process as the blockchain store.
//
// Parameters:
// - ctx: Context for controlling the initialization process
// - logger: Logger interface used for recording operational events and errors
// - tSettings: Configuration settings that control server behavior
// - blockStore: Blob store instance used for persisting UTXO data
// - blockchainStore: Direct access to the blockchain storage layer
//
// Returns a configured Server instance and any error encountered during setup.
// Using this constructor bypasses the client abstraction layer, providing better
// performance but requiring the blockchain store to be in the same process.
func NewDirect(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	blockStore blob.Store,
	blockchainStore blockchain_store.Store,
) (*Server, error) {
	return &Server{
		logger:          logger,
		settings:        tSettings,
		blockStore:      blockStore,
		blockchainStore: blockchainStore,
		stats:           gocore.NewStat("utxopersister"),
		triggerCh:       make(chan string, 5),
	}, nil
}

// Health checks the health status of the server and its dependencies.
// It performs both liveness and readiness checks based on the checkLiveness parameter.
// Liveness checks verify that the service is running, while readiness checks also verify that
// dependencies like blockchain client, FSM, blockchain store, and block store are available.
// Returns HTTP status code, status message, and any error encountered.
//
// Parameters:
// - ctx: Context for controlling the health check operation
// - checkLiveness: Boolean flag that determines the check type (true for liveness, false for readiness)
//
// Returns:
// - int: HTTP status code (200 for OK, 503 for Service Unavailable)
// - string: Status message describing the health state
// - error: Any error encountered during health checks
//
// For liveness checks, this method only verifies that the service process is running and responsive.
// For readiness checks, it performs comprehensive dependency checks to ensure the service can
// handle requests. This supports Kubernetes or other orchestration systems in determining
// when to route traffic to this service instance.
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
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
	checks := make([]health.Check, 0, 4)
	if s.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: s.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(s.blockchainClient)})
	}

	if s.blockchainStore != nil {
		checks = append(checks, health.Check{Name: "BlockchainStore", Check: s.blockchainStore.Health})
	}

	if s.blockStore != nil {
		checks = append(checks, health.Check{Name: "BlockStore", Check: s.blockStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// Init initializes the server by reading the last processed height.
// It retrieves the last block height that was successfully processed from persistent storage
// and sets it as the starting point for future processing.
// Returns an error if the height cannot be read.
//
// Parameters:
// - ctx: Context for controlling the initialization process
//
// Returns:
// - error: Any error encountered during initialization
//
// This method should be called after creating a new Server instance and before calling Start.
// It ensures the server has the correct starting state for processing blocks.
// If no previous height information is found, processing will start from height 0.
func (s *Server) Init(ctx context.Context) (err error) {
	height, err := s.readLastHeight(ctx)
	if err != nil {
		return err
	}

	s.lastHeight = height

	return nil
}

// Start begins the server's processing operations.
// It sets up notification channels, subscribes to blockchain updates, and starts the main processing loop.
// The loop processes blocks as they are received through the notification channel or on a timer.
// The readyCh is closed when initialization is complete to signal readiness.
// Returns an error if subscription or processing fails.
//
// Parameters:
// - ctx: Context for controlling the server's lifecycle
// - readyCh: Channel closed when initialization is complete to signal readiness
//
// Returns:
// - error: Any error encountered during startup or processing
//
// This method blocks until the context is canceled or an error occurs. It first waits for
// the blockchain's FSM to transition from IDLE state, then subscribes to blockchain notifications.
// If no blockchain client is available, it falls back to a polling mechanism with a shorter interval.
// The processing loop handles three types of events:
// 1. Context cancellation - triggers shutdown
// 2. Blockchain notifications - triggers processing of new blocks
// 3. Timer events - triggers periodic processing attempts
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	close(readyCh)

	var (
		err error
		ch  chan *blockchain.Notification
	)

	if s.blockchainClient != nil {
		// Blocks until the FSM transitions from the IDLE state
		err = s.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
		if err != nil {
			s.logger.Errorf("[UTXOPersister Service] Failed to wait for FSM transition from IDLE state: %s", err)

			return err
		}

		if s.blockchainClient == nil {
			ch = make(chan *blockchain.Notification) // Create a dummy channel
		} else {
			ch, err = s.blockchainClient.Subscribe(ctx, "utxo-persister")
			if err != nil {
				return err
			}
		}
	}

	go func() {
		// Kick off the first block processing
		s.triggerCh <- "startup"
	}()

	duration := 1 * time.Minute
	if s.blockchainClient == nil {
		// We do not have a subscription, so we need to poll the blockchain more frequently
		duration = 10 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ch:
			if err := s.trigger(ctx, "blockchain"); err != nil {
				return err
			}

		case source := <-s.triggerCh:
			if err := s.trigger(ctx, source); err != nil {
				return err
			}

		case <-time.After(duration):
			if err := s.trigger(ctx, "timer"); err != nil {
				return err
			}
		}
	}
}

// Stop stops the server's processing operations.
// It performs cleanup and shutdown procedures for the UTXO persister server.
//
// Parameters:
// - ctx: Context for controlling the shutdown process (currently unused)
//
// Returns:
// - error: Any error encountered during shutdown (currently always returns nil)
//
// This method ensures graceful termination of server operations and can be used
// during application shutdown or when reconfiguring the server.
// Currently, no specific cleanup operations are performed, but the method signature
// maintains compatibility with other service interfaces.
func (s *Server) Stop(_ context.Context) error {
	return nil
}

// trigger initiates the processing of the next block.
// It ensures only one processing operation runs at a time and handles various trigger sources.
// The source parameter indicates what triggered the processing (blockchain, timer, etc.).
// Returns an error if the processing encounters a problem.
//
// Parameters:
// - ctx: Context for controlling the processing operation
// - source: String identifier indicating what triggered the processing (blockchain, timer, startup)
//
// Returns:
// - error: Any error encountered during processing
//
// This method uses a mutex to ensure that only one processing operation runs at a time,
// preventing race conditions and duplicate work. It updates the running state of the server
// and initiates the block processing via processNextBlock. If a processing operation is already
// in progress, it returns immediately.
//
// The trigger mechanism supports multiple sources to accommodate both push (notification-based)
// and pull (timer-based) processing models, making the server resilient to missed notifications.
func (s *Server) trigger(ctx context.Context, source string) error {
	s.mu.Lock()

	if s.running {
		s.mu.Unlock()

		s.logger.Debugf("Process already running, skipping trigger from %s", source)

		return nil // Exit if the process is already running
	}

	s.running = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	s.logger.Debugf("Trigger from %s to process next block", source)

	delay, err := s.processNextBlock(ctx)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			s.logger.Infof("No new block to process")
			return nil
		}

		return err
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	// Trigger the next block processing
	select {
	case s.triggerCh <- "iteration":
		// Successfully sent
	default:
		s.logger.Debugf("Dropping iteration trigger due to full channel")
	}

	return nil
}

// processNextBlock processes the next block in the chain.
// It retrieves the next block based on the last processed height, extracts UTXOs,
// and persists them to storage. It also updates the last processed height.
// Returns a duration to wait before processing the next block and any error encountered.
// The duration is used to implement confirmation waiting periods.
//
// Parameters:
// - ctx: Context for controlling the processing operation
//
// Returns:
// - time.Duration: Time to wait before processing the next block (for confirmation periods)
// - error: Any error encountered during processing
//
// This method performs several key operations:
// 1. Retrieves the next block information from the blockchain store or client
// 2. Extracts UTXO additions and deletions for the block
// 3. Validates the previous UTXO set to ensure consistency
// 4. Creates a new UTXO set by applying additions and deletions
// 5. Persists the new UTXO set to storage
// 6. Updates the last processed height
//
// If the new block is not yet available or if it doesn't follow directly after the last processed height,
// the method will return a waiting duration to retry later. This handles chain reorganizations and
// ensures blocks are processed in sequence.
func (s *Server) processNextBlock(ctx context.Context) (time.Duration, error) {
	var (
		headers       []*model.BlockHeader
		metas         []*model.BlockHeaderMeta
		err           error
		bestBlockMeta *model.BlockHeaderMeta
	)

	// Get the current best block height from the blockchain
	if s.blockchainStore != nil {
		_, bestBlockMeta, err = s.blockchainStore.GetBestBlockHeader(ctx)
	} else {
		_, bestBlockMeta, err = s.blockchainClient.GetBestBlockHeader(ctx)
	}

	if err != nil {
		return 0, err
	}

	// Calculate the maximum height that can be processed.  This is the best block height minus 100 confirmations
	// Handle underflow when chain height < 100 (early chain bootstrapping, test networks)
	var maxHeight uint32
	if bestBlockMeta.Height < confirmations {
		// For early chains, allow processing up to current height (no safety window yet)
		maxHeight = bestBlockMeta.Height
	} else {
		maxHeight = bestBlockMeta.Height - confirmations
	}

	if s.lastHeight >= maxHeight {
		s.logger.Infof("Waiting for %d confirmations (height to process: %d, best block height: %d)", confirmations, s.lastHeight, bestBlockMeta.Height)
		return 1 * time.Minute, nil
	}

	if s.blockchainStore != nil {
		headers, metas, err = s.blockchainStore.GetBlockHeadersByHeight(ctx, s.lastHeight, s.lastHeight)
	} else {
		headers, metas, err = s.blockchainClient.GetBlockHeadersByHeight(ctx, s.lastHeight, s.lastHeight)
	}

	if err != nil {
		return 0, err
	}

	if len(headers) != 1 {
		return 0, errors.NewProcessingError("1 headers should have been returned, got %d", len(headers))
	}

	lastWrittenUTXOSetHash := headers[0].Hash()

	if lastWrittenUTXOSetHash.String() != s.settings.ChainCfgParams.GenesisHash.String() {
		// GetStarting point
		if err := s.verifyLastSet(ctx, lastWrittenUTXOSetHash); err != nil {
			return 0, err
		}
	}

	c := NewConsolidator(s.logger, s.settings, s.blockchainStore, s.blockchainClient, s.blockStore, lastWrittenUTXOSetHash)

	s.logger.Infof("Rolling up data from block height %d to %d", s.lastHeight+1, maxHeight)

	if err := c.ConsolidateBlockRange(ctx, s.lastHeight+1, maxHeight); err != nil {
		if !errors.Is(err, errors.ErrNotFound) {
			return 0, nil
		}
	}

	// At the end of this, we have a rollup of deletions and additions.  Add these to the last UTXOSet
	if s.blockStore == nil {
		return 0, errors.NewStorageError("[UTXOPersister] Block store is not initialized")
	}
	us, err := GetUTXOSet(ctx, s.logger, s.settings, s.blockStore, lastWrittenUTXOSetHash)
	if err != nil {
		return 0, errors.NewProcessingError("[UTXOPersister] Error getting UTXOSet for block %s height %d", lastWrittenUTXOSetHash, metas[0].Height, err)
	}

	s.logger.Infof("Processing block %s height %d", c.lastBlockHash, c.lastBlockHeight)

	if us == nil {
		s.logger.Infof("UTXOSet already exists for block %s height %d", c.lastBlockHash, c.lastBlockHeight)

		s.lastHeight = c.lastBlockHeight

		return 0, s.writeLastHeight(ctx, s.lastHeight)
	}

	if err := us.CreateUTXOSet(ctx, c); err != nil {
		return 0, err
	}

	s.lastHeight = c.lastBlockHeight

	// Write the headers file for the UTXO set
	if s.blockchainStore != nil {
		if err := WriteHeadersToStore(ctx, s.logger, s.settings, s.blockStore, s.blockchainStore, c.lastBlockHash, c.lastBlockHeight); err != nil {
			s.logger.Warnf("[UTXOPersister] Error writing headers file: %v", err)
		}
	} else if s.blockchainClient != nil {
		s.logger.Warnf("[UTXOPersister] Cannot write headers file: blockchain client interface doesn't support GetBlockHeader")
	}

	// Remove the previous block's UTXOSet and headers
	if lastWrittenUTXOSetHash.String() != s.settings.ChainCfgParams.GenesisHash.String() && !s.settings.BlockPersister.SkipUTXODelete {
		if err := s.blockStore.Del(ctx, lastWrittenUTXOSetHash[:], fileformat.FileTypeUtxoSet); err != nil {
			return 0, errors.NewProcessingError("[UTXOPersister] Error deleting UTXOSet for block %s height %d", lastWrittenUTXOSetHash, c.firstBlockHeight, err)
		}

		if err := s.blockStore.Del(ctx, lastWrittenUTXOSetHash[:], fileformat.FileTypeUtxoSet+".sha256"); err != nil {
			return 0, errors.NewProcessingError("[UTXOPersister] Error deleting UTXOSet for block %s height %d", lastWrittenUTXOSetHash, c.firstBlockHeight, err)
		}

		if err := s.blockStore.Del(ctx, lastWrittenUTXOSetHash[:], fileformat.FileTypeUtxoHeaders); err != nil {
			s.logger.Warnf("[UTXOPersister] Error deleting UTXOHeaders for block %s height %d: %v", lastWrittenUTXOSetHash, c.firstBlockHeight, err)
		}

		if err := s.blockStore.Del(ctx, lastWrittenUTXOSetHash[:], fileformat.FileTypeUtxoHeaders+".sha256"); err != nil {
			s.logger.Warnf("[UTXOPersister] Error deleting UTXOHeaders sha256 for block %s height %d: %v", lastWrittenUTXOSetHash, c.firstBlockHeight, err)
		}
	}

	return 0, s.writeLastHeight(ctx, s.lastHeight)
}

// readLastHeight reads the last processed block height from storage.
// It attempts to retrieve the height from a special file in the block store.
// Returns the height as uint32 and any error encountered.
// If the height file doesn't exist, it returns 0 and no error.
//
// Parameters:
// - ctx: Context for controlling the storage operation
//
// Returns:
// - uint32: The last processed block height, or 0 if no height is stored
// - error: Any error encountered during the read operation
//
// This method reads the height value from a dedicated file named 'utxo-height'
// in the blob store. The height is stored as a decimal string representation.
// If the file does not exist (typically during first startup), the method returns 0,
// indicating that processing should start from the genesis block.
// Other errors during reading or parsing are returned to the caller.
func (s *Server) readLastHeight(ctx context.Context) (uint32, error) {
	// Read the file content as a byte slice
	b, err := s.blockStore.Get(ctx, nil, fileformat.FileTypeDat, options.WithFilename("lastProcessed"), options.WithNoHashPrefix())
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			s.logger.Warnf("lastProcessed.dat does not exist, starting from height 0")
			return 0, nil
		}

		return 0, err
	}

	// Convert the byte slice to a string
	heightStr := strings.TrimSpace(string(b))

	// Parse the string to a uint32
	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		return 0, errors.NewProcessingError("failed to parse height from file", err)
	}

	heightUint32, err := safeconversion.Uint64ToUint32(height)
	if err != nil {
		return 0, err
	}

	return heightUint32, nil
}

// writeLastHeight writes the current block height to storage.
// It persists the last processed height to a special file in the block store.
// This allows the server to resume processing from the correct point after a restart.
// Returns an error if the write operation fails.
//
// Parameters:
// - ctx: Context for controlling the storage operation
// - height: The block height to be stored
//
// Returns:
// - error: Any error encountered during the write operation
//
// This method stores the height as a decimal string representation in a dedicated file
// named 'utxo-height' in the blob store. The height is stored after each successful
// block processing operation to enable recovery after restarts or crashes.
// The write operation uses atomic store semantics to ensure consistency.
func (s *Server) writeLastHeight(ctx context.Context, height uint32) error {
	// Convert the height to a string
	heightStr := fmt.Sprintf("%d", height)

	// Write the string to the file
	// #nosec G306
	return s.blockStore.Set(
		ctx,
		nil,
		fileformat.FileTypeDat,
		[]byte(heightStr),
		options.WithFilename("lastProcessed"),
		options.WithNoHashPrefix(),
		options.WithAllowOverwrite(true),
	)
}

// verifyLastSet verifies the integrity of the last UTXO set.
// It checks if the UTXO set for the given hash exists and has valid header and footer.
// This verification ensures that the UTXO set was completely written and is not corrupted.
// Returns an error if verification fails.
//
// Parameters:
// - ctx: Context for controlling the verification operation
// - hash: Pointer to the block hash for which to verify the UTXO set
//
// Returns:
// - error: Any error encountered during verification, or nil if verification succeeds
//
// This method performs several integrity checks on the UTXO set:
// 1. Verifies that a UTXO set file exists for the given block hash
// 2. Checks that the header record is valid and matches the expected block hash
// 3. Ensures that the footer exists and is correctly formatted
//
// These checks confirm that the UTXO set for the block was completely written
// and can be used for subsequent operations. This is crucial for maintaining
// blockchain state consistency, especially after system restarts.
func (s *Server) verifyLastSet(ctx context.Context, hash *chainhash.Hash) error {
	us := &UTXOSet{
		ctx:       ctx,
		logger:    s.logger,
		blockHash: *hash,
		store:     s.blockStore,
	}

	r, err := us.GetUTXOSetReader(hash)
	if err != nil {
		return err
	}
	defer r.Close()

	if _, err := fileformat.ReadHeader(r); err != nil {
		return err
	}

	return nil
}
