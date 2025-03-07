// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

// Server manages the UTXO persistence operations.
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
func New(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	blockStore blob.Store,
	blockchainClient blockchain.ClientI,

) *Server {
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
func (s *Server) Init(ctx context.Context) (err error) {
	height, err := s.readLastHeight(ctx)
	if err != nil {
		return err
	}

	s.lastHeight = height

	return nil
}

// Start begins the server's processing operations.
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	close(readyCh)

	var (
		err error
		ch  chan *blockchain.Notification
	)

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
func (s *Server) Stop(_ context.Context) error {
	return nil
}

// trigger initiates the processing of the next block.
// It ensures only one processing operation runs at a time and handles various trigger sources.
// The source parameter indicates what triggered the processing (blockchain, timer, etc.).
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
// It returns a duration to wait before processing the next block and any error encountered.
// The duration is used to implement confirmation waiting periods.
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
	maxHeight := bestBlockMeta.Height - 100

	if s.lastHeight >= maxHeight {
		s.logger.Infof("Waiting for 100 confirmations (height to process: %d, best block height: %d)", s.lastHeight, bestBlockMeta.Height)
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
		return 0, err
	}

	// At the end of this, we have a rollup of deletions and additions.  Add these to the last UTXOSet
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

	// Remove the previous block's UTXOSet
	if lastWrittenUTXOSetHash.String() != s.settings.ChainCfgParams.GenesisHash.String() && !s.settings.Block.SkipUTXODelete {
		if err := s.blockStore.Del(ctx, lastWrittenUTXOSetHash[:], options.WithFileExtension(utxosetExtension)); err != nil {
			return 0, errors.NewProcessingError("[UTXOPersister] Error deleting UTXOSet for block %s height %d", lastWrittenUTXOSetHash, c.firstBlockHeight, err)
		}

		if err := s.blockStore.Del(ctx, lastWrittenUTXOSetHash[:], options.WithFileExtension(utxosetExtension+".sha256")); err != nil {
			return 0, errors.NewProcessingError("[UTXOPersister] Error deleting UTXOSet for block %s height %d", lastWrittenUTXOSetHash, c.firstBlockHeight, err)
		}
	}

	return 0, s.writeLastHeight(ctx, s.lastHeight)
}

// readLastHeight reads the last processed block height from storage.
// Returns the height as uint32 and any error encountered.
// If the height file doesn't exist, it returns 0 and no error.
func (s *Server) readLastHeight(ctx context.Context) (uint32, error) {
	// Read the file content as a byte slice
	b, err := s.blockStore.Get(ctx, nil, options.WithFilename("lastProcessed"), options.WithFileExtension("dat"))
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

	heightUint32, err := util.SafeUint64ToUint32(height)
	if err != nil {
		return 0, err
	}

	return heightUint32, nil
}

// writeLastHeight writes the current block height to storage.
// It persists the last processed height for recovery purposes.
func (s *Server) writeLastHeight(ctx context.Context, height uint32) error {
	// Convert the height to a string
	heightStr := fmt.Sprintf("%d", height)

	// Write the string to the file
	// #nosec G306
	return s.blockStore.Set(
		ctx,
		nil,
		[]byte(heightStr),
		options.WithFilename("lastProcessed"),
		options.WithFileExtension("dat"),
		options.WithAllowOverwrite(true),
	)
}

// verifyLastSet verifies the integrity of the last UTXO set.
// It checks if the UTXO set for the given hash exists and has valid header and footer.
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

	if _, _, _, _, err := GetUTXOSetHeaderFromReader(r); err != nil {
		return err
	}

	if _, _, err := GetFooter(r); err != nil {
		return errors.NewProcessingError("error seeking to EOF marker", err)
	}

	return nil
}
