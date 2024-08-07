package utxopersister

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

// Server type carries the logger within it
type Server struct {
	logger           ulogger.Logger
	blockchainClient blockchain.ClientI
	blockStore       blob.Store
	stats            *gocore.Stat
	lastHeight       uint32
	mu               sync.Mutex
	running          bool
	triggerCh        chan string
}

func New(
	ctx context.Context,
	logger ulogger.Logger,
	blockStore blob.Store,
	blockchainClient blockchain.ClientI,
) *Server {

	return &Server{
		logger:           logger,
		blockchainClient: blockchainClient,
		blockStore:       blockStore,
		stats:            gocore.NewStat("utxopersister"),
		triggerCh:        make(chan string, 5),
	}
}

func (s *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
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
	ch, err := s.blockchainClient.Subscribe(ctx, "utxo-persister")
	if err != nil {
		return err
	}

	go func() {
		// Kick off the first block processing
		s.triggerCh <- "startup"
	}()

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

		case <-time.After(1 * time.Minute):
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
		// s.logger.Infof("Process already running, skipping trigger from %s", source)
		return nil // Exit if the process is already running
	}

	s.running = true
	s.mu.Unlock()

	// Create an error channel to communicate any errors from the goroutine
	errCh := make(chan error, 1) // Buffered channel to prevent goroutine leaks

	go func() {
		defer func() {
			// Ensure we reset the running state
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
		}()

		s.logger.Debugf("Trigger from %s to process next block", source)

		errCh <- s.processNextBlock(ctx)
	}()

	err := <-errCh
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			s.logger.Infof("No new block to process")
			return nil
		} else {
			return err
		}
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

func (s *Server) processNextBlock(ctx context.Context) error {
	// Get the next block

	headers, metas, err := s.blockchainClient.GetBlockHeadersFromHeight(ctx, s.lastHeight, 1)
	if err != nil {
		return err
	}

	if len(headers) == 0 {
		return nil
	}

	header := headers[0]
	meta := metas[0]
	hash := header.Hash()

	pBlock := header.HashPrevBlock
	if pBlock.String() == "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f" {
		// Genesis block
		pBlock = nil
	}

	ud, err := GetUTXODiff(ctx, s.logger, s.blockStore, hash)
	if err != nil {
		return errors.NewProcessingError("[UTXOPersister] Error getting UTXODiff for block %s height %d", hash, meta.Height, err)
	}

	s.logger.Infof("Processing block %s height %d", hash, meta.Height)

	if ud == nil {
		s.logger.Infof("UTXOSet already exists for block %s height %d", hash, meta.Height)
		s.lastHeight++
		return nil
	}

	if err := ud.CreateUTXOSet(ctx, pBlock); err != nil {
		return errors.NewProcessingError("[UTXOPersister] Error processing UTXOSet for block %s height %d", hash, meta.Height, err)
	}

	s.lastHeight++
	return s.writeLastHeight(ctx, s.lastHeight)
}

func (s *Server) readLastHeight(ctx context.Context) (uint32, error) {
	// Read the file content as a byte slice
	b, err := s.blockStore.Get(ctx, nil, options.WithFileName("lastProcessed"), options.WithFileExtension("dat"))
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			s.logger.Warnf("lastProcessed.dat does not exist, starting from height 1")
			return 1, nil
		}
		return 0, err
	}

	// Convert the byte slice to a string
	heightStr := string(b)

	// Parse the string to a uint32
	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse height from file: %w", err)
	}

	return uint32(height), nil
}

func (s *Server) writeLastHeight(ctx context.Context, height uint32) error {
	// Convert the height to a string
	heightStr := fmt.Sprintf("%d", height)

	// Write the string to the file
	// #nosec G306
	return s.blockStore.Set(ctx, nil, []byte(heightStr), options.WithFileName("lastProcessed"), options.WithFileExtension("dat"))
}
