package utxopersister

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/ordishs/gocore"
)

// Server type carries the logger within it
type Server struct {
	logger           ulogger.Logger
	blockchainClient blockchain.ClientI
	blockchainStore  blockchain_store.Store
	blockStore       blob.Store
	stats            *gocore.Stat
	lastHeight       uint32
	mu               sync.Mutex
	running          bool
	triggerCh        chan string
	chainParams      *chaincfg.Params
}

func New(
	ctx context.Context,
	logger ulogger.Logger,
	blockStore blob.Store,
	blockchainClient blockchain.ClientI,

) *Server {
	network, _ := gocore.Config().Get("network", "mainnet")

	params, err := chaincfg.GetChainParams(network)
	if err != nil {
		logger.Fatalf("Unknown network: %s", network)
	}

	return &Server{
		logger:           logger,
		blockchainClient: blockchainClient,
		blockStore:       blockStore,
		stats:            gocore.NewStat("utxopersister"),
		triggerCh:        make(chan string, 5),
		chainParams:      params,
	}
}

func NewDirect(
	ctx context.Context,
	logger ulogger.Logger,
	blockStore blob.Store,
	blockchainStore blockchain_store.Store,
) (*Server, error) {
	network, _ := gocore.Config().Get("network", "mainnet")

	params, err := chaincfg.GetChainParams(network)
	if err != nil {
		logger.Fatalf("Unknown network: %s", network)
	}

	return &Server{
		logger:          logger,
		blockStore:      blockStore,
		blockchainStore: blockchainStore,
		stats:           gocore.NewStat("utxopersister"),
		triggerCh:       make(chan string, 5),
		chainParams:     params,
	}, nil
}

func (s *Server) Health(ctx context.Context) (int, string, error) {
	checks := []health.Check{
		{Name: "BlockchainClient", Check: s.blockchainClient.Health},
		{Name: "BlockchainStore", Check: s.blockchainStore.Health},
		{Name: "BlockStore", Check: s.blockStore.Health},
	}

	return health.CheckAll(ctx, checks)
}

func (s *Server) Init(ctx context.Context) (err error) {
	height, err := s.readLastHeight(ctx)
	if err != nil {
		return err
	}

	s.lastHeight = height

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) error {
	var (
		err error
		ch  chan *blockchain.Notification
	)

	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		if err = s.blockchainClient.Restore(ctx); err != nil {
			s.logger.Errorf("[Utxo Persister] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		s.logger.Infof("[Utxo Persister] Node is restoring, waiting for FSM to transition to Running state")
		_ = s.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain.FSMStateRUNNING)
		s.logger.Infof("[Utxo Persister] Node finished restoring and has transitioned to Running state, continuing to start Utxo Persister service")
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

func (s *Server) Stop(_ context.Context) error {
	return nil
}

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

	// Only process the next block if its height is more than 100 behind the current best block height
	if bestBlockMeta.Height-s.lastHeight < 100 {
		s.logger.Infof("Waiting for 100 confirmations (height to process: %d, best block height: %d)", s.lastHeight, bestBlockMeta.Height)
		return 1 * time.Minute, nil
	}

	// Get the last 2 block headers from this last processed height
	if s.blockchainStore != nil {
		headers, metas, err = s.blockchainStore.GetBlockHeadersFromHeight(ctx, s.lastHeight, 2)
	} else {
		headers, metas, err = s.blockchainClient.GetBlockHeadersFromHeight(ctx, s.lastHeight, 2)
	}

	if err != nil {
		return 0, err
	}

	if len(headers) != 2 {
		s.logger.Infof("2 headers should have been returned, got %d", len(headers))
		return 0, nil
	}

	// Sanity check
	if headers[0].HashPrevBlock.String() != headers[1].Hash().String() {
		return 0, errors.NewProcessingError("[UTXOPersister] Previous block hash does not match current block hash for block %s height %d", headers[0].Hash(), metas[0].Height)
	}

	header := headers[0]
	meta := metas[0]
	hash := header.Hash()

	previousBlockHash := header.HashPrevBlock
	if previousBlockHash.String() == s.chainParams.GenesisHash.String() {
		// Genesis block
		previousBlockHash = nil
	}

	us, err := GetUTXOSet(ctx, s.logger, s.blockStore, hash)
	if err != nil {
		return 0, errors.NewProcessingError("[UTXOPersister] Error getting UTXOSet for block %s height %d", hash, meta.Height, err)
	}

	s.logger.Infof("Processing block %s height %d", hash, meta.Height)

	if us == nil {
		s.logger.Infof("UTXOSet already exists for block %s height %d", hash, meta.Height)

		s.lastHeight++

		return 0, s.writeLastHeight(ctx, s.lastHeight)
	}

	if err := us.CreateUTXOSet(ctx, previousBlockHash); err != nil {
		return 0, errors.NewProcessingError("[UTXOPersister] Error processing UTXOSet for block %s height %d", hash, meta.Height, err)
	}

	s.lastHeight++

	// Remove the previous block's UTXOSet
	if previousBlockHash != nil {
		if err := s.blockStore.Del(ctx, previousBlockHash[:], options.WithFileExtension(utxosetExtension)); err != nil {
			return 0, errors.NewProcessingError("[UTXOPersister] Error deleting UTXOSet for block %s height %d", previousBlockHash, meta.Height, err)
		}

		if err := s.blockStore.Del(ctx, previousBlockHash[:], options.WithFileExtension(utxosetExtension+".sha256")); err != nil {
			return 0, errors.NewProcessingError("[UTXOPersister] Error deleting UTXOSet for block %s height %d", previousBlockHash, meta.Height, err)
		}
	}

	return 0, s.writeLastHeight(ctx, s.lastHeight)
}

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
	heightStr := string(b)

	// Parse the string to a uint32
	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		return 0, errors.NewProcessingError("failed to parse height from file: %w", err)
	}

	// nolint:gosec
	return uint32(height), nil
}

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
