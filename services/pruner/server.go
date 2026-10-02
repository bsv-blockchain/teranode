// Package pruner provides the Pruner Service which handles periodic pruning of unmined transaction
// parents and delete-at-height (DAH) records in the UTXO store.
//
// Trigger mechanism (event-driven):
// The pruning trigger mode is controlled by pruner_block_trigger setting:
//   - "OnBlockPersisted" (default): Pruning triggers on BlockPersisted notifications from block persister.
//     If block persister is not running, automatically falls back to Block notifications.
//   - "OnBlockMined": Pruning triggers on Block notifications after block has mined_set=true,
//     regardless of block persister status.
//
// Pruner operations only execute when safe to do so (i.e., when block assembly is in "running"
// state and not performing reorgs or resets).
package pruner

import (
	"context"
	"encoding/binary"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/pruner/pruner_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
)

// pruneSignal carries the block height and hash to prune up to.
type pruneSignal struct {
	blockHeight uint32
	blockHash   chainhash.Hash
	// force marks a signal as originating from the fallback ticker rather than a
	// blockchain notification. prunerProcessor's already-processed guard
	// (blockHeight <= lastProcessedHeight) exists to skip stale/duplicate
	// notifications, but it also means a plain re-send of the last known height
	// can never reach Prune again. That matters because the underlying DAH sweep
	// keeps stamping rows with delete_at_height <= that height even while the
	// trigger height itself is frozen (a stalled block persister, or a missed
	// notification) — those newly-due rows need a RE-RUN at the SAME height to
	// be collected, not a higher one. force lets the ticker's re-send bypass
	// only that one guard; every other check (min height, catchup, mined-set
	// wait, block-assembly safety, the store's own cheap idle-tick EXISTS probe)
	// still applies exactly as it does for a real notification.
	force bool
}

// BlobDeletionObserver is an optional callback interface for testing.
// It allows tests to be notified when blob deletion processing completes.
type BlobDeletionObserver interface {
	OnBlobDeletionComplete(height uint32, successCount, failCount int64)
}

// Server implements the Pruner service which handles periodic pruner operations
// for the UTXO store. It uses event-driven triggers: BlockPersisted notifications (primary)
// and Block notifications with mined_set wait (fallback when persister not running or forced via setting).
type Server struct {
	pruner_api.UnsafePrunerAPIServer

	// Dependencies (injected via constructor)
	ctx                 context.Context
	logger              ulogger.Logger
	settings            *settings.Settings
	utxoStore           utxo.Store
	blockchainClient    blockchain.ClientI
	blockAssemblyClient blockassembly.ClientI

	// Internal state
	prunerService       pruner.Service
	lastProcessedHeight atomic.Uint32
	lastPersistedHeight atomic.Uint32
	// lastBlockHash caches the hash of the most recently observed notification's
	// block, so the fallback ticker (see fireFallbackTick) can reissue a matching
	// pruneSignal without a fresh notification. nil until the first notification
	// with a parseable hash has been seen.
	lastBlockHash atomic.Pointer[chainhash.Hash]
	pruneNotify   chan pruneSignal
	stats         *gocore.Stat

	// Blob deletion
	blobStores           map[storetypes.BlobStoreType]blob.Store
	blobNotify           chan pruneSignal
	blobDeletionObserver BlobDeletionObserver
}

// New creates a new Pruner server instance with the provided dependencies.
// This function initializes the server but does not start any background processes.
// Call Init() and then Start() to begin operation.
func New(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	utxoStore utxo.Store,
	blockchainClient blockchain.ClientI,
	blockAssemblyClient blockassembly.ClientI,
) *Server {
	return &Server{
		ctx:                 ctx,
		logger:              logger,
		settings:            tSettings,
		utxoStore:           utxoStore,
		blockchainClient:    blockchainClient,
		blockAssemblyClient: blockAssemblyClient,
		blobStores:          make(map[storetypes.BlobStoreType]blob.Store),
		pruneNotify:         make(chan pruneSignal, 1),
		blobNotify:          make(chan pruneSignal, 1),
		stats:               gocore.NewStat("pruner"),
	}
}

// Init initializes the pruner service. This is called before Start() and is responsible
// for setting up the pruner service provider from the UTXO store and subscribing to
// block persisted notifications for coordination with the block persister service.
func (s *Server) Init(ctx context.Context) error {
	s.ctx = ctx

	// Initialize metrics
	initPrometheusMetrics()

	// Initialize pruner service from UTXO store
	prunerProvider, ok := s.utxoStore.(pruner.PrunerServiceProvider)
	if !ok {
		return errors.NewServiceError("UTXO store does not provide pruner service")
	}

	var err error
	s.prunerService, err = prunerProvider.GetPrunerService()
	if err != nil {
		return errors.NewServiceError("failed to get pruner service", err)
	}
	if s.prunerService == nil {
		return errors.NewServiceError("pruner service not available from UTXO store")
	}

	// Validate block trigger mode
	blockTrigger := s.settings.Pruner.BlockTrigger
	if blockTrigger != settings.PrunerBlockTriggerOnBlockPersisted && blockTrigger != settings.PrunerBlockTriggerOnBlockMined {
		return errors.NewConfigurationError("pruner_block_trigger must be either '%s' or '%s' (got '%s')",
			settings.PrunerBlockTriggerOnBlockPersisted, settings.PrunerBlockTriggerOnBlockMined, blockTrigger)
	}

	// Blob deletion is now managed by blockchain service - no local DB needed

	// Subscribe to blockchain notifications for event-driven pruning.
	// The pruner_block_trigger setting controls which notification type is used:
	// - OnBlockPersisted mode: uses BlockPersisted notifications (block persister is running)
	// - OnBlockMined mode: uses Block notifications and waits for mined_set=true (no persister)
	subscriptionCh, err := s.blockchainClient.Subscribe(ctx, blockchain.SubscriberPruner)
	if err != nil {
		return errors.NewServiceError("failed to subscribe to blockchain notifications", err)
	}

	// Start a goroutine to handle blockchain notifications (and the fallback ticker)
	go s.notificationWorker(ctx, subscriptionCh)

	// Read initial persisted height from blockchain state
	if state, err := s.blockchainClient.GetState(ctx, "BlockPersisterHeight"); err == nil && len(state) >= 4 {
		height := binary.LittleEndian.Uint32(state)
		s.lastPersistedHeight.Store(height)
		s.logger.Infof("Loaded initial block persister height: %d", height)

		s.seedLastBlockHash(ctx, height)
	}

	return nil
}

// seedLastBlockHash fetches the block hash at height (the same
// GetBlockHeadersFromHeight(ctx, height, 1) call used elsewhere, e.g.
// blockvalidation) and caches it as the initial lastBlockHash, so the
// fallback ticker is armed immediately on startup rather than waiting for a
// future notification.
//
// This closes a restart gap: if the block persister is already stalled when
// the pruner (re)starts, no BlockPersisted notification will ever arrive to
// populate lastBlockHash, and the Block-notification capture in
// notificationWorker only helps once a NEW notification shows up after this
// point -- without this seed step, the ticker would stay permanently
// disarmed for exactly the stalled-persister incident it exists to guard
// against.
//
// Best-effort: any error is logged as a single-line warning and
// lastBlockHash is left unset (nil); a subsequent notification will still
// re-arm it per the usual paths.
//
// Init starts notificationWorker before calling this, and
// GetBlockHeadersFromHeight is a network round trip, so a live notification
// can race ahead and Store a fresher hash while this seed is still in
// flight. The store below is therefore a CompareAndSwap(nil, seeded), not an
// unconditional Store: it only fills lastBlockHash if nothing has claimed it
// yet, so the worker's live value always wins over the seed and a
// slower-but-stale seed can never clobber a faster-but-fresh notification.
func (s *Server) seedLastBlockHash(ctx context.Context, height uint32) {
	headers, _, err := s.blockchainClient.GetBlockHeadersFromHeight(ctx, height, 1)
	if err != nil || len(headers) == 0 {
		s.logger.Warnf("[pruner] failed to seed initial block hash at height %d for fallback ticker: %v", height, err)
		return
	}

	s.lastBlockHash.CompareAndSwap(nil, headers[0].Hash())
}

// notificationWorker consumes blockchain notifications and translates them into
// pruneSignal messages on s.pruneNotify. It also runs an optional fallback ticker
// (pruner_fallbackTickerSeconds) that periodically re-fires the last known signal,
// so a transient early-exit skip in prunerProcessor, or a stalled/slow block
// persister, cannot strand deletable rows with no notification ever arriving to
// retry them. Returns when ctx is done or subscriptionCh is closed.
func (s *Server) notificationWorker(ctx context.Context, subscriptionCh chan *blockchain_api.Notification) {
	var fallbackC <-chan time.Time

	if s.settings.Pruner.FallbackTickerSeconds > 0 {
		ticker := time.NewTicker(time.Duration(s.settings.Pruner.FallbackTickerSeconds) * time.Second)
		defer ticker.Stop()
		fallbackC = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			return

		case notification, ok := <-subscriptionCh:
			if !ok {
				return
			}
			if notification == nil {
				continue
			}

			switch notification.Type {
			case model.NotificationType_BlockPersisted:
				// Skip BlockPersisted notifications if using OnBlockMined trigger mode
				if s.settings.Pruner.BlockTrigger == settings.PrunerBlockTriggerOnBlockMined {
					s.logger.Debugf("Ignoring BlockPersisted notification (pruner_block_trigger=%s)", s.settings.Pruner.BlockTrigger)
					continue
				}

				// Track persisted height for coordination with block persister
				if notification.Metadata != nil && notification.Metadata.Metadata != nil {
					if heightStr, ok := notification.Metadata.Metadata["height"]; ok {
						if parsedHeight, err := strconv.ParseUint(heightStr, 10, 32); err == nil {
							height := uint32(parsedHeight)
							oldHeight := s.lastPersistedHeight.Swap(height)

							// Log at INFO level when Block Persister first becomes active (transition from 0)
							if oldHeight == 0 && height > 0 {
								s.logger.Infof("[pruner] block persister is now active (persisted height: %d)", height)
							} else {
								s.logger.Debugf("[pruner] updated persisted height to %d", height)
							}

							blockHash, err := chainhash.NewHash(notification.Hash)
							if err != nil {
								s.logger.Warnf("Failed to parse block hash from BlockPersisted notification: %v", err)
								continue
							}

							// Cache the hash alongside lastPersistedHeight (updated together, above)
							// so fireFallbackTick can reissue a matching pruneSignal without a
							// fresh notification.
							s.lastBlockHash.Store(blockHash)

							// Send signal to wake worker with latest height
							if height > s.lastProcessedHeight.Load() {
								sig := pruneSignal{blockHeight: height, blockHash: *blockHash}

								s.logger.Infof("[pruner][%s:%d] notified from BlockPersisted notification", blockHash.String(), height)

								// Drain old signal (if any) and replace with latest
								select {
								case <-s.pruneNotify:
								default:
								}
								s.pruneNotify <- sig
							}
						}
					}
				}

			case model.NotificationType_Block:
				// Extract and cache the block hash BEFORE the trigger-mode filter below,
				// for every Block notification regardless of mode. This is what re-arms
				// the fallback ticker in OnBlockPersisted mode when the block persister
				// itself is stalled: Block notifications come from block validation, not
				// the persister, so they keep arriving as the chain moves even while
				// BlockPersisted notifications have stopped. Without this, a pruner that
				// restarts while the persister is already stalled would never observe a
				// single BlockPersisted notification, lastBlockHash would stay nil
				// forever, and the fallback ticker -- the exact mechanism meant to
				// survive a stalled persister -- would be permanently disarmed.
				//
				// Deliberately NOT updating any height atomic here: lastPersistedHeight
				// means "the block persister confirmed up to this height," and this
				// notification carries no such confirmation (in OnBlockPersisted mode
				// especially, that would be actively wrong while the persister is
				// stalled); lastProcessedHeight is owned exclusively by prunerProcessor.
				// So this is hash-only, on purpose.
				if notification.Hash == nil {
					s.logger.Debugf("Block notification missing hash, skipping")
					continue
				}

				blockHash, err := chainhash.NewHash(notification.Hash)
				if err != nil {
					s.logger.Debugf("Failed to parse block hash from notification: %v", err)
					continue
				}

				s.lastBlockHash.Store(blockHash)

				// Skip triggering a prune signal if using OnBlockPersisted trigger mode --
				// the hash above is still cached to re-arm the fallback ticker, but this
				// notification type must not itself drive pruning in that mode.
				if s.settings.Pruner.BlockTrigger == settings.PrunerBlockTriggerOnBlockPersisted {
					s.logger.Debugf("Block notification received but pruner configured for BlockPersisted trigger")
					continue
				}

				// Get height from blockchain client using the block hash
				// This is the authoritative source and avoids race conditions with Block Assembly state updates
				header, meta, err := s.blockchainClient.GetBlockHeader(ctx, blockHash)
				if err != nil {
					s.logger.Debugf("Failed to get block header for Block notification hash %s: %v", blockHash, err)
					continue
				}
				if header == nil || meta == nil {
					s.logger.Debugf("Block notification for hash %s has no header/meta", blockHash)
					continue
				}
				height := meta.Height

				// Send signal to wake worker - processor will wait for mined_set if block assembly is running
				if height > s.lastProcessedHeight.Load() {
					sig := pruneSignal{blockHeight: height, blockHash: *blockHash}

					s.logger.Infof("[pruner][%s:%d] notified from Block notification", blockHash.String(), height)

					// Drain old signal (if any) and replace with latest
					select {
					case <-s.pruneNotify:
					default:
					}
					s.pruneNotify <- sig
				}
			}

		case <-fallbackC:
			s.fireFallbackTick()
		}
	}
}

// fireFallbackTick re-fires the last known pruneSignal on the fallback ticker
// interval, reusing state rather than inventing new values.
//
// The signal is marked force: true so it bypasses prunerProcessor's
// already-processed guard (see the pruneSignal.force doc comment). Without
// that, resending the same height the processor already completed would
// always be a no-op — which would defeat the whole point of the ticker for
// the incident it exists to guard against: the DAH sweep keeps stamping rows
// due at or below the trigger height even while that height itself is frozen
// (stalled persister / missed notification), so recovering them requires a
// genuine RE-RUN of Prune at the SAME height, not just a resent signal that
// gets dropped before reaching the store.
//
// A real, previously-captured block hash is required in BOTH trigger modes before
// this will fire anything: prunerProcessor's waitForBlockMinedStatus runs whenever
// blockAssemblyClient is configured (see worker.go), and that check applies "both
// trigger modes" per its own comment — it is not gated on pruner_block_trigger. A
// synthetic zero hash passed to GetBlockIsMined would look up a hash matching no
// real block, retrying for up to BlockAssemblyWaitTimeout (10 minutes by default)
// before the cycle gives up, which would stall the pruner processor loop far
// longer than the fallback tick interval itself. So unlike the brief's initial
// sketch — which only gated the hash requirement in OnBlockMined mode — this
// applies the same requirement in OnBlockPersisted mode too, since
// blockAssemblyClient is always non-nil in production (see daemon_services.go).
func (s *Server) fireFallbackTick() {
	h := s.lastPersistedHeight.Load()
	if s.settings.Pruner.BlockTrigger == settings.PrunerBlockTriggerOnBlockMined {
		h = s.lastProcessedHeight.Load()
	}

	if h == 0 {
		return // nothing known yet — never invent a height
	}

	hash := s.lastBlockHash.Load()
	if hash == nil {
		return // no real hash captured yet — see comment above
	}

	sig := pruneSignal{blockHeight: h, blockHash: *hash, force: true}

	// Drain old signal (if any) and replace with latest — same idiom the
	// notification path uses.
	select {
	case <-s.pruneNotify:
	default:
	}

	select {
	case s.pruneNotify <- sig:
		s.logger.Debugf("[pruner] fallback ticker fired at height %d", h)
	default:
	}
}

// Start begins the pruner service operation. It starts the pruner processor goroutine,
// then starts the gRPC server. Pruning is triggered by BlockPersisted and Block notifications.
// This function blocks until the server shuts down or encounters an error.
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Wait for blockchain FSM to be ready
	err := s.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		if errors.IsContextError(err) {
			s.logger.Infof("[Pruner Service] Shutting down during FSM wait")
			return err
		}
		s.logger.Errorf("[Pruner Service] Failed to wait for FSM transition from IDLE state: %s", err)
		return err
	}

	// Start the pruner service (Aerospike or SQL)
	if s.prunerService != nil {
		s.prunerService.Start(ctx)
	}

	// Start pruner processor goroutine
	go s.prunerProcessor(ctx)

	// Trigger initial pruning if there's work to do
	// This ensures the pruner starts working immediately on startup
	// rather than waiting for the next block notification
	go s.triggerInitialPruning(ctx)

	// Start blob deletion worker
	go s.blobDeletionWorker()

	// Start blob deletion metrics updater
	go s.updateBlobDeletionMetrics()

	// Note: Polling worker not needed - pruning is triggered by:
	// 1. BlockPersisted notifications (when block persister is running)
	// 2. Block notifications with mined_set wait (when persister not running)

	// Start gRPC server (BLOCKING - must be last)
	if err := util.StartGRPCServer(ctx, s.logger, s.settings, "pruner",
		s.settings.Pruner.GRPCListenAddress,
		func(server *grpc.Server) {
			pruner_api.RegisterPrunerAPIServer(server, s)
			closeOnce.Do(func() { close(readyCh) })
		}, nil); err != nil {
		return err
	}

	return nil
}

// triggerInitialPruning sends an initial pruning signal on startup if there's work to do.
func (s *Server) triggerInitialPruning(ctx context.Context) {
	var currentHeight uint32
	var blockHash chainhash.Hash

	blockTrigger := s.settings.Pruner.BlockTrigger

	switch blockTrigger {
	case settings.PrunerBlockTriggerOnBlockPersisted:
		currentHeight = s.lastPersistedHeight.Load()
		// Look up the actual block hash for the persisted height using headers only.
		// GetBlockHeadersByHeight walks the main chain (highest chainwork), so this
		// remains fork-safe while avoiding full block reconstruction/transfer when
		// only the hash is needed for the initial pruning signal.
		// Retries with backoff to handle transient blockchain-client unavailability
		// during startup, since in OnBlockPersisted mode there may be no subsequent
		// notification to trigger pruning if this fails.
		if currentHeight > 0 {
			const (
				maxHeaderLookupAttempts = 3
				headerLookupBackoff     = 500 * time.Millisecond
			)

			for attempt := 1; attempt <= maxHeaderLookupAttempts; attempt++ {
				headers, _, err := s.blockchainClient.GetBlockHeadersByHeight(ctx, currentHeight, currentHeight)
				if err == nil && len(headers) > 0 && headers[0] != nil {
					blockHash = *headers[0].Hash()
					break
				}

				if err != nil {
					s.logger.Warnf("[pruner] failed to get block header at persisted height %d for initial pruning (attempt %d/%d): %v", currentHeight, attempt, maxHeaderLookupAttempts, err)
				} else if len(headers) == 0 {
					s.logger.Warnf("[pruner] failed to get block header at persisted height %d for initial pruning (attempt %d/%d): header not found", currentHeight, attempt, maxHeaderLookupAttempts)
				} else {
					s.logger.Warnf("[pruner] failed to get block header at persisted height %d for initial pruning (attempt %d/%d): header was nil", currentHeight, attempt, maxHeaderLookupAttempts)
				}

				if attempt == maxHeaderLookupAttempts {
					return
				}

				timer := time.NewTimer(headerLookupBackoff)
				select {
				case <-timer.C:
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return
				}
			}
		}
	case settings.PrunerBlockTriggerOnBlockMined:
		tipHeader, tipMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil || tipHeader == nil || tipMeta == nil {
			s.logger.Warnf("[pruner] failed to get best block header for initial pruning: %v", err)
			return
		}
		currentHeight = tipMeta.Height
		blockHash = *tipHeader.Hash()
	}

	if currentHeight == 0 {
		s.logger.Infof("[pruner] no initial pruning needed (current height: 0)")
		return
	}

	if currentHeight <= s.lastProcessedHeight.Load() {
		s.logger.Debugf("[pruner] no initial pruning needed (current height: %d, last processed: %d)", currentHeight, s.lastProcessedHeight.Load())
		return
	}

	sig := pruneSignal{blockHeight: currentHeight, blockHash: blockHash}
	select {
	case s.pruneNotify <- sig:
		s.logger.Infof("[pruner][%s:%d] triggered initial pruning on startup (mode: %s)", blockHash.String(), currentHeight, blockTrigger)
	case <-time.After(5 * time.Second):
		s.logger.Warnf("[pruner] failed to queue initial pruning request (timeout)")
	case <-ctx.Done():
	}
}

// Stop gracefully shuts down the pruner service. Context cancellation will stop
// the polling worker and pruner processor goroutines.
func (s *Server) Stop(ctx context.Context) error {
	// Stop the pruner service if it has a Stop method
	if s.prunerService != nil {
		// Check if the pruner service implements Stop
		// Aerospike has Stop, SQL doesn't
		type stopper interface {
			Stop(ctx context.Context) error
		}
		if stoppable, ok := s.prunerService.(stopper); ok {
			if err := stoppable.Stop(ctx); err != nil {
				s.logger.Errorf("Error stopping pruner service: %v", err)
			}
		}
	}

	// Context cancellation will stop goroutines
	s.logger.Infof("Pruner service stopped")
	return nil
}

// Health implements the health check for the pruner service. When checkLiveness is true,
// it only checks if the service process is running. When false, it checks all dependencies
// including gRPC server, block assembly client, blockchain client, and UTXO store.
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// LIVENESS: Is the service process running?
		return http.StatusOK, "OK", nil
	}

	// READINESS: Can the service handle requests?
	checks := make([]health.Check, 0, 5)

	// Check gRPC server is listening
	if s.settings.Pruner.GRPCListenAddress != "" {
		checks = append(checks, health.Check{
			Name: "gRPC Server",
			Check: health.CheckGRPCServerWithSettings(s.settings.Pruner.GRPCListenAddress, s.settings, func(ctx context.Context, conn *grpc.ClientConn) error {
				// Simple connection check - if we can create a client, server is up
				return nil
			}),
		})
	}

	// Check block assembly client
	if s.blockAssemblyClient != nil {
		checks = append(checks, health.Check{
			Name:  "BlockAssemblyClient",
			Check: s.blockAssemblyClient.Health,
		})
	}

	// Check blockchain client
	if s.blockchainClient != nil {
		checks = append(checks, health.Check{
			Name:  "BlockchainClient",
			Check: s.blockchainClient.Health,
		})
		checks = append(checks, health.Check{
			Name:  "FSM",
			Check: blockchain.CheckFSM(s.blockchainClient),
		})
	}

	// Check UTXO store
	if s.utxoStore != nil {
		checks = append(checks, health.Check{
			Name:  "UTXOStore",
			Check: s.utxoStore.Health,
		})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC implements the gRPC health check endpoint.
func (s *Server) HealthGRPC(ctx context.Context, _ *pruner_api.EmptyMessage) (*pruner_api.HealthResponse, error) {
	// Add context value to prevent circular dependency when checking gRPC server
	ctx = context.WithValue(ctx, "skip-grpc-self-check", true)

	status, details, err := s.Health(ctx, false)

	return &pruner_api.HealthResponse{
		Ok:      status == http.StatusOK,
		Details: details,
	}, errors.WrapGRPC(err)
}

// SetBlobDeletionObserver sets an optional observer for blob deletion completion events.
// This is primarily used for testing to synchronize test execution with deletion processing.
func (s *Server) SetBlobDeletionObserver(observer BlobDeletionObserver) {
	s.blobDeletionObserver = observer
}
