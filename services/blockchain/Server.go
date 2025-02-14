// Package blockchain provides functionality for managing the Bitcoin blockchain.
package blockchain

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/settings"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/bitcoin-sv/teranode/util/kafka"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/looplab/fsm"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// subscriber represents a subscription to blockchain notifications.
type subscriber struct {
	subscription blockchain_api.BlockchainAPI_SubscribeServer // The gRPC subscription server
	source       string                                       // Source identifier of the subscription
	done         chan struct{}                                // Channel to signal when subscription is done
}

// Blockchain represents the main blockchain service structure.
type Blockchain struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	addBlockChan                  chan *blockchain_api.AddBlockRequest // Channel for adding blocks
	store                         blockchain_store.Store               // Storage interface for blockchain data
	logger                        ulogger.Logger                       // Logger instance
	settings                      *settings.Settings                   // Configuration settings
	newSubscriptions              chan subscriber                      // Channel for new subscriptions
	deadSubscriptions             chan subscriber                      // Channel for ended subscriptions
	subscribers                   map[subscriber]bool                  // Active subscribers map
	subscribersMu                 sync.RWMutex                         // Mutex for subscribers map
	notifications                 chan *blockchain_api.Notification    // Channel for notifications
	newBlock                      chan struct{}                        // Channel signaling new block events
	difficulty                    *Difficulty                          // Difficulty calculation instance
	blocksFinalKafkaAsyncProducer kafka.KafkaAsyncProducerI            // Kafka producer for final blocks
	kafkaChan                     chan *kafka.Message                  // Channel for Kafka messages
	stats                         *gocore.Stat                         // Statistics tracking
	finiteStateMachine            *fsm.FSM                             // FSM for blockchain state
	stateChangeTimestamp          time.Time                            // Timestamp of last state change
	AppCtx                        context.Context                      // Application context
	localTestStartState           string                               // Initial state for testing
}

// New creates a new Blockchain instance with the provided dependencies.
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blockchain_store.Store, blocksFinalKafkaAsyncProducer kafka.KafkaAsyncProducerI, localTestStartFromState ...string) (*Blockchain, error) {
	initPrometheusMetrics()

	d, err := NewDifficulty(store, logger, tSettings)
	if err != nil {
		logger.Errorf("[BlockAssembler] Couldn't create difficulty: %v", err)
	}

	b := &Blockchain{
		store:                         store,
		logger:                        logger,
		settings:                      tSettings,
		addBlockChan:                  make(chan *blockchain_api.AddBlockRequest, 10),
		newSubscriptions:              make(chan subscriber, 10),
		deadSubscriptions:             make(chan subscriber, 10),
		subscribers:                   make(map[subscriber]bool),
		notifications:                 make(chan *blockchain_api.Notification, 100),
		newBlock:                      make(chan struct{}, 10),
		difficulty:                    d,
		stats:                         gocore.NewStat("blockchain"),
		AppCtx:                        ctx,
		blocksFinalKafkaAsyncProducer: blocksFinalKafkaAsyncProducer,
	}

	if len(localTestStartFromState) >= 1 && localTestStartFromState[0] != "" {
		// Convert the string state to FSMStateType using the map
		_, ok := blockchain_api.FSMStateType_value[localTestStartFromState[0]]
		if !ok {
			// Handle the case where the state is not found in the map
			logger.Errorf("Invalid initial state: %s", localTestStartFromState[0])
		} else {
			b.localTestStartState = localTestStartFromState[0]
		}
	}

	return b, nil
}

// GetStoreFSMState retrieves the current FSM state from the store.
func (b *Blockchain) GetStoreFSMState(ctx context.Context) (string, error) {
	return b.store.GetFSMState(ctx)
}

// ResetFSMS resets the finite state machine to nil (used for testing).
func (b *Blockchain) ResetFSMS() {
	b.finiteStateMachine = nil
}

// Health checks the health status of the blockchain service.
func (b *Blockchain) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return health.CheckAll(ctx, checkLiveness, nil)
	}

	var brokersURL []string
	if b.blocksFinalKafkaAsyncProducer != nil { // tests may not set this
		brokersURL = b.blocksFinalKafkaAsyncProducer.BrokersURL()
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 2)
	checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})

	if b.store != nil {
		checks = append(checks, health.Check{Name: "BlockchainStore", Check: b.store.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC provides health check information via gRPC.
func (b *Blockchain) HealthGRPC(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.HealthResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "HealthGRPC",
		tracing.WithParentStat(b.stats),
		tracing.WithCounter(prometheusBlockchainHealth),
		tracing.WithDebugLogMessage(b.logger, "[HealthGRPC] called"),
	)
	defer deferFn()

	status, details, err := b.Health(ctx, false)

	return &blockchain_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.Now(),
	}, errors.WrapGRPC(err)
}

// Init initializes the blockchain service.
func (b *Blockchain) Init(ctx context.Context) error {
	b.finiteStateMachine = b.NewFiniteStateMachine()

	// check if we are in local testing mode with a defined target state for the FSM
	if b.localTestStartState != "" {
		b.finiteStateMachine.SetState(b.localTestStartState)

		err := b.store.SetFSMState(ctx, b.finiteStateMachine.Current())
		if err != nil {
			b.logger.Errorf("[Blockchain][Init] Error setting FSM state in blockchain store: %v", err)
		}

		return nil
	}

	// Set the FSM to the latest persisted state
	stateStr, err := b.store.GetFSMState(ctx)
	if err != nil {
		b.logger.Errorf("[Blockchain][Init] Error getting FSM state: %v", err)
	}

	if stateStr == "" { // if no state is stored, set the default state
		b.logger.Infof("[Blockchain][Init] Blockchain db doesn't have previous FSM state, storing FSM's default state: %v", b.finiteStateMachine.Current())

		err = b.store.SetFSMState(ctx, b.finiteStateMachine.Current())
		if err != nil {
			// TODO: just logging now, consider adding retry
			b.logger.Errorf("[Blockchain][Init] Error setting FSM state in blockchain store: %v", err)
		}
	} else { // if there is a state stored, set the FSM to that state
		b.logger.Infof("[Blockchain][Init] Blockchain db has previous FSM state: %v, setting FSM's current state to it.", stateStr)
		b.finiteStateMachine.SetState(stateStr)
	}

	prometheusBlockchainFSMCurrentState.Set(float64(blockchain_api.FSMStateType_value[b.finiteStateMachine.Current()]))

	return nil
}

// Start begins the blockchain service operations.
func (b *Blockchain) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	b.startKafka()

	go b.startSubscriptions()

	if err := b.startHTTP(); err != nil {
		return errors.WrapGRPC(err)
	}

	// this will block
	if err := util.StartGRPCServer(ctx, b.logger, b.settings, "blockchain", b.settings.BlockChain.GRPCListenAddress, func(server *grpc.Server) {
		blockchain_api.RegisterBlockchainAPIServer(server, b)
		closeOnce.Do(func() { close(readyCh) })
	}); err != nil {
		return errors.WrapGRPC(errors.NewServiceNotStartedError("[Blockchain][Start] can't start GRPC server", err))
	}

	return nil
}

// startHTTP initializes and starts the HTTP server for the blockchain service.
func (b *Blockchain) startHTTP() error {
	httpAddress := b.settings.BlockChain.HTTPListenAddress
	if httpAddress == "" {
		return errors.NewConfigurationError("[Miner] No blockchain_httpListenAddress specified")
	}

	go func() {
		e := echo.New()
		e.HideBanner = true
		e.HidePort = true

		e.Use(middleware.Recover())

		e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
			AllowOrigins: []string{"*"},
			AllowMethods: []string{echo.GET},
		}))

		e.GET("/invalidate/:hash", b.invalidateHandler)
		e.GET("/revalidate/:hash", b.revalidateHandler)

		if err := e.Start(httpAddress); err != nil {
			b.logger.Errorf("[Blockchain][Start] failed to start http server: %v", err)
		}
	}()

	return nil
}

// invalidateHandler handles HTTP requests to invalidate a block.
func (b *Blockchain) invalidateHandler(c echo.Context) error {
	hashStr := c.Param("hash")

	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		return c.String(http.StatusBadRequest, fmt.Sprintf("invalid hash: %v", err))
	}

	_, err = b.InvalidateBlock(b.AppCtx, &blockchain_api.InvalidateBlockRequest{
		BlockHash: hash.CloneBytes(),
	})

	if err != nil {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("error invalidating block: %v", err))
	}

	return c.String(http.StatusOK, fmt.Sprintf("block invalidated: %s", hashStr))
}

// revalidateHandler handles HTTP requests to revalidate a block.
func (b *Blockchain) revalidateHandler(c echo.Context) error {
	hashStr := c.Param("hash")

	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		return c.String(http.StatusBadRequest, fmt.Sprintf("invalid hash: %v", err))
	}

	_, err = b.RevalidateBlock(b.AppCtx, &blockchain_api.RevalidateBlockRequest{
		BlockHash: hash.CloneBytes(),
	})

	if err != nil {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("error revalidating block: %v", err))
	}

	return c.String(http.StatusOK, fmt.Sprintf("block revalidated: %s", hashStr))
}

// startKafka initializes and starts the Kafka producer.
func (b *Blockchain) startKafka() {
	b.logger.Infof("[Blockchain][startKafka] Starting Kafka producer for blocks")
	b.kafkaChan = make(chan *kafka.Message, 100)

	b.blocksFinalKafkaAsyncProducer.Start(b.AppCtx, b.kafkaChan)
}

// startSubscriptions manages blockchain subscriptions in a goroutine.
/* Must be started as a go routine unless you are in a test */
func (b *Blockchain) startSubscriptions() {
	for {
		select {
		case <-b.AppCtx.Done():
			b.logger.Infof("[Blockchain][startSubscriptions] Stopping channel listeners go routine")

			for sub := range b.subscribers {
				safeClose(sub.done)
			}

			return
		case notification := <-b.notifications:
			start := gocore.CurrentTime()

			func() {
				b.logger.Debugf("[Blockchain Server] Sending notification: %s", notification)

				for sub := range b.subscribers {
					b.logger.Debugf("[Blockchain][startSubscriptions] Sending notification to %s in background: %s", sub.source, notification.Stringify())

					go func(s subscriber) {
						b.logger.Debugf("[Blockchain][startSubscriptions] Sending notification to %s: %s", s.source, notification.Stringify())

						if err := s.subscription.Send(notification); err != nil {
							b.deadSubscriptions <- s
						}
					}(sub)
				}
			}()
			b.stats.NewStat("channel-subscription.Send", true).AddTime(start)

		case s := <-b.newSubscriptions:
			b.subscribersMu.Lock()
			b.subscribers[s] = true
			b.subscribersMu.Unlock()
		//	b.logger.Infof("[Blockchain][startSubscriptions] New Subscription received from %s (Total=%d).", s.source, len(b.subscribers))

		case s := <-b.deadSubscriptions:
			delete(b.subscribers, s)
			safeClose(s.done)
			b.logger.Infof("[Blockchain][startSubscriptions] Subscription removed (Total=%d).", len(b.subscribers))
		}
	}
}

// Stop gracefully stops the blockchain service.
func (b *Blockchain) Stop(_ context.Context) error {
	return nil
}

// AddBlock processes a request to add a new block to the blockchain.
// It validates the block, stores it, and notifies subscribers.
func (b *Blockchain) AddBlock(ctx context.Context, request *blockchain_api.AddBlockRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "AddBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainAddBlock),
		tracing.WithDebugLogMessage(b.logger, "[AddBlock] called from %s", request.PeerId),
	)
	defer deferFn()

	header, err := model.NewBlockHeaderFromBytes(request.Header)
	if err != nil {
		return nil, err
	}

	b.logger.Infof("[Blockchain][AddBlock] AddBlock called: %s", header.Hash().String())

	btCoinbaseTx, err := bt.NewTxFromBytes(request.CoinbaseTx)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][AddBlock] can't create the coinbase transaction", err))
	}

	subtreeHashes := make([]*chainhash.Hash, len(request.SubtreeHashes))
	for i, subtreeHash := range request.SubtreeHashes {
		subtreeHashes[i], err = chainhash.NewHash(subtreeHash)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][AddBlock] unable to create subtree hash", err))
		}
	}

	block := &model.Block{
		Header:           header,
		CoinbaseTx:       btCoinbaseTx,
		Subtrees:         subtreeHashes,
		TransactionCount: request.TransactionCount,
		SizeInBytes:      request.SizeInBytes,
	}

	ID, height, err := b.store.StoreBlock(ctx, block, request.PeerId)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	b.logger.Infof("[AddBlock] stored block %s (ID: %d, height: %d)", block.Hash(), ID, height)

	block.Height = height

	b.logger.Debugf("[AddBlock] checking for Kafka producer: %v", b.blocksFinalKafkaAsyncProducer != nil)

	if b.blocksFinalKafkaAsyncProducer != nil {
		key := block.Header.Hash().CloneBytes()

		value, err := block.Bytes()
		if err != nil {
			b.logger.Errorf("[AddBlock] error creating block bytes: %v", err)
			return nil, errors.WrapGRPC(err)
		}

		if len(value) >= 500_000 { // kafka default limit is actually 1MB and we don't ever expecta block to be even close to that
			b.logger.Warnf("[AddBlock] blocks-final message size %d bytes maybe too large for Kafka, block hash: %s (height: %d)", len(value), block.Header.Hash(), block.Height)
		}

		b.kafkaChan <- &kafka.Message{
			Key:   key,
			Value: value,
		}
	}

	_, _ = b.SendNotification(ctx, &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: block.Hash().CloneBytes(),
	})

	return &emptypb.Empty{}, nil
}

// GetBlock retrieves a block by its hash.
func (b *Blockchain) GetBlock(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlock),
		tracing.WithDebugLogMessage(b.logger, "[GetBlock] called for %s", utils.ReverseAndHexEncodeSlice(request.Hash)),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlock] request's hash is not valid", err))
	}

	block, height, err := b.store.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	subtreeHashes := make([][]byte, len(block.Subtrees))
	for i, subtreeHash := range block.Subtrees {
		subtreeHashes[i] = subtreeHash[:]
	}

	var coinbaseBytes []byte
	if block.CoinbaseTx != nil {
		coinbaseBytes = block.CoinbaseTx.Bytes()
	}

	return &blockchain_api.GetBlockResponse{
		Header:           block.Header.Bytes(),
		Height:           height,
		CoinbaseTx:       coinbaseBytes,
		SubtreeHashes:    subtreeHashes,
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
		Id:               block.ID,
	}, nil
}

// GetBlocks retrieves multiple blocks starting from a specific hash.
func (b *Blockchain) GetBlocks(ctx context.Context, req *blockchain_api.GetBlocksRequest) (*blockchain_api.GetBlocksResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlocks",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaders),
		tracing.WithLogMessage(b.logger, "[GetBlocks] called for %s", utils.ReverseAndHexEncodeSlice(req.Hash)),
	)
	defer deferFn()

	startHash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlocks] request's hash is not valid", err))
	}

	blocks, err := b.store.GetBlocks(ctx, startHash, req.Count)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blocks))

	for i, block := range blocks {
		blockBytes, err := block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}

		blockHeaderBytes[i] = blockBytes
	}

	return &blockchain_api.GetBlocksResponse{
		Blocks: blockHeaderBytes,
	}, nil
}

// GetBlockByHeight retrieves a block at a specific height in the blockchain.
func (b *Blockchain) GetBlockByHeight(ctx context.Context, request *blockchain_api.GetBlockByHeightRequest) (*blockchain_api.GetBlockResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockByHeight",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlock),
		tracing.WithLogMessage(b.logger, "[GetBlockByHeight] called for %d", request.Height),
	)
	defer deferFn()

	block, err := b.store.GetBlockByHeight(ctx, request.Height)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	subtreeHashes := make([][]byte, len(block.Subtrees))
	for i, subtreeHash := range block.Subtrees {
		subtreeHashes[i] = subtreeHash[:]
	}

	var coinbaseBytes []byte
	if block.CoinbaseTx != nil {
		coinbaseBytes = block.CoinbaseTx.Bytes()
	}

	return &blockchain_api.GetBlockResponse{
		Header:           block.Header.Bytes(),
		Height:           request.Height,
		CoinbaseTx:       coinbaseBytes,
		SubtreeHashes:    subtreeHashes,
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
		Id:               block.ID,
	}, nil
}

// GetBlockByID retrieves a block by its ID.
func (b *Blockchain) GetBlockByID(ctx context.Context, request *blockchain_api.GetBlockByIDRequest) (*blockchain_api.GetBlockResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockByHeight",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlock),
		tracing.WithLogMessage(b.logger, "[GetBlockByHeight] called for %d", request.Id),
	)
	defer deferFn()

	block, err := b.store.GetBlockByID(ctx, request.Id)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	subtreeHashes := make([][]byte, len(block.Subtrees))
	for i, subtreeHash := range block.Subtrees {
		subtreeHashes[i] = subtreeHash[:]
	}

	var coinbaseBytes []byte
	if block.CoinbaseTx != nil {
		coinbaseBytes = block.CoinbaseTx.Bytes()
	}

	return &blockchain_api.GetBlockResponse{
		Header:           block.Header.Bytes(),
		Height:           block.Height,
		CoinbaseTx:       coinbaseBytes,
		SubtreeHashes:    subtreeHashes,
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
		Id:               block.ID,
	}, nil
}

// GetBlockStats retrieves statistical information about the blockchain.
func (b *Blockchain) GetBlockStats(ctx context.Context, _ *emptypb.Empty) (*model.BlockStats, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockStats",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockStats),
	)
	defer deferFn()

	resp, err := b.store.GetBlockStats(ctx)

	return resp, errors.WrapGRPC(err)
}

// GetBlockGraphData retrieves data points for blockchain visualization.
func (b *Blockchain) GetBlockGraphData(ctx context.Context, req *blockchain_api.GetBlockGraphDataRequest) (*model.BlockDataPoints, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockGraphData",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockGraphData),
	)
	defer deferFn()

	resp, err := b.store.GetBlockGraphData(ctx, req.PeriodMillis)

	return resp, errors.WrapGRPC(err)
}

// GetLastNBlocks retrieves the most recent N blocks from the blockchain.
func (b *Blockchain) GetLastNBlocks(ctx context.Context, request *blockchain_api.GetLastNBlocksRequest) (*blockchain_api.GetLastNBlocksResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetLastNBlocks",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetLastNBlocks),
	)
	defer deferFn()

	blockInfo, err := b.store.GetLastNBlocks(ctx, request.NumberOfBlocks, request.IncludeOrphans, request.FromHeight)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetLastNBlocksResponse{
		Blocks: blockInfo,
	}, nil
}

// GetSuitableBlock finds a suitable block for mining purposes.
func (b *Blockchain) GetSuitableBlock(ctx context.Context, request *blockchain_api.GetSuitableBlockRequest) (*blockchain_api.GetSuitableBlockResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetSuitableBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetSuitableBlock),
	)
	defer deferFn()

	blockInfo, err := b.store.GetSuitableBlock(ctx, (*chainhash.Hash)(request.Hash))
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetSuitableBlockResponse{
		Block: blockInfo,
	}, nil
}

// GetNextWorkRequired calculates the required proof of work for the next block.
func (b *Blockchain) GetNextWorkRequired(ctx context.Context, request *blockchain_api.GetNextWorkRequiredRequest) (*blockchain_api.GetNextWorkRequiredResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetNextWorkRequired",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetNextWorkRequired),
	)
	defer deferFn()

	var nBits *model.NBit

	bytesLittleEndian := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesLittleEndian, b.settings.ChainCfgParams.PowLimitBits)
	defaultNbits, _ := model.NewNBitFromSlice(bytesLittleEndian)

	if b.difficulty == nil {
		b.logger.Debugf("difficulty is null")

		nBits = defaultNbits
	} else {
		hash, err := chainhash.NewHash(request.BlockHash)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetNextWorkRequired] request's block hash is not valid", err))
		}

		blockHeaders, metas, err := b.store.GetBlockHeaders(ctx, hash, 2)
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}

		if len(blockHeaders) == 0 {
			return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain] could not GetBlockHeaders for hash %s", hash.String()))
		}

		var testnetArgs []int64

		if b.settings.ChainCfgParams.ReduceMinDifficulty {
			testnetArgs = append(testnetArgs, int64(blockHeaders[0].Timestamp))
		}

		nBitsp, err := b.difficulty.CalcNextWorkRequired(ctx, blockHeaders[0], metas[0].Height, testnetArgs...)
		if err == nil {
			nBits = nBitsp
		} else {
			b.logger.Debugf("error in GetNextWorkRequired: %v", err)

			nBits = defaultNbits
		}

		b.logger.Debugf("difficulty adjustment. Difficulty set to %s", nBits.String())
	}

	return &blockchain_api.GetNextWorkRequiredResponse{
		Bits: nBits.CloneBytes(),
	}, nil
}

// GetHashOfAncestorBlock retrieves the hash of an ancestor block at a specific depth.
func (b *Blockchain) GetHashOfAncestorBlock(ctx context.Context, request *blockchain_api.GetHashOfAncestorBlockRequest) (*blockchain_api.GetHashOfAncestorBlockResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetHashOfAncestorBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetHashOfAncestorBlock),
	)
	defer deferFn()

	hash, err := b.store.GetHashOfAncestorBlock(ctx, (*chainhash.Hash)(request.Hash), int(request.Depth))
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetHashOfAncestorBlockResponse{
		Hash: hash[:],
	}, nil
}

// GetBlockExists checks if a block with the given hash exists in the blockchain.
func (b *Blockchain) GetBlockExists(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockExistsResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockExists",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockExists),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockExists] request's hash is not valid", err))
	}

	exists, err := b.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockExistsResponse{
		Exists: exists,
	}, nil
}

// GetBestBlockHeader retrieves the header of the current best block.
func (b *Blockchain) GetBestBlockHeader(ctx context.Context, empty *emptypb.Empty) (*blockchain_api.GetBlockHeaderResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBestBlockHeader",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBestBlockHeader),
	)
	defer deferFn()

	chainTip, meta, err := b.store.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockHeaderResponse{
		BlockHeader: chainTip.Bytes(),
		Height:      meta.Height,
		TxCount:     meta.TxCount,
		SizeInBytes: meta.SizeInBytes,
		Miner:       meta.Miner,
		BlockTime:   meta.BlockTime,
		Timestamp:   meta.Timestamp,
		ChainWork:   meta.ChainWork,
	}, nil
}

// CheckBlockIsInCurrentChain verifies if a block is part of the current main chain.
func (b *Blockchain) CheckBlockIsInCurrentChain(ctx context.Context, req *blockchain_api.CheckBlockIsCurrentChainRequest) (*blockchain_api.CheckBlockIsCurrentChainResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "CheckBlockIsInCurrentChain",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainCheckBlockIsInCurrentChain),
	)
	defer deferFn()

	result, err := b.store.CheckBlockIsInCurrentChain(ctx, req.BlockIDs)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.CheckBlockIsCurrentChainResponse{
		IsPartOfCurrentChain: result,
	}, nil
}

// GetBlockHeader retrieves the header of a specific block.
func (b *Blockchain) GetBlockHeader(ctx context.Context, req *blockchain_api.GetBlockHeaderRequest) (*blockchain_api.GetBlockHeaderResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBestBlockHeader",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeader),
	)
	defer deferFn()

	hash, err := chainhash.NewHash(req.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeader] request's hash is not valid", err))
	}

	blockHeader, meta, err := b.store.GetBlockHeader(ctx, hash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockHeaderResponse{
		BlockHeader: blockHeader.Bytes(),
		Height:      meta.Height,
		TxCount:     meta.TxCount,
		SizeInBytes: meta.SizeInBytes,
		Miner:       meta.Miner,
		BlockTime:   meta.BlockTime,
		Timestamp:   meta.Timestamp,
	}, nil
}

// GetBlockHeaders retrieves multiple block headers starting from a specific hash.
func (b *Blockchain) GetBlockHeaders(ctx context.Context, req *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockHeaders",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaders),
	)
	defer deferFn()

	startHash, err := chainhash.NewHash(req.StartHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeaders] request's hash is not valid", err))
	}

	blockHeaders, blockHeaderMetas, err := b.store.GetBlockHeaders(ctx, startHash, req.NumberOfHeaders)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	blockHeaderMetaBytes := make([][]byte, len(blockHeaders))
	for i, meta := range blockHeaderMetas {
		blockHeaderMetaBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        blockHeaderMetaBytes,
	}, nil
}

func (b *Blockchain) GetBlockHeadersToCommonAncestor(ctx context.Context, req *blockchain_api.GetBlockHeadersToCommonAncestorRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockHeaders",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaders),
	)
	defer deferFn()

	var err error

	targetHash, err := chainhash.NewHash(req.TargetHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeadersToCommonAncestor] request's hash is not valid", err))
	}

	blockLocatorHashes := make([]*chainhash.Hash, len(req.BlockLocatorHashes))

	for i, hash := range req.BlockLocatorHashes {
		blockLocatorHashes[i], err = chainhash.NewHash(hash)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeadersToCommonAncestor] request's hash is not valid", err))
		}
	}

	blockHeaders, blockHeaderMetas, err := getBlockHeadersToCommonAncestor(ctx, b.store, targetHash, blockLocatorHashes)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	blockHeaderMetaBytes := make([][]byte, len(blockHeaders))
	for i, meta := range blockHeaderMetas {
		blockHeaderMetaBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        blockHeaderMetaBytes,
	}, nil
}

// GetBlockHeadersFromTill retrieves block headers between two specified blocks.
func (b *Blockchain) GetBlockHeadersFromTill(ctx context.Context, req *blockchain_api.GetBlockHeadersFromTillRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockHeadersFromTill",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaders),
	)
	defer deferFn()

	startHash, err := chainhash.NewHash(req.StartHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeadersFromTill] request's start hash is not valid", err))
	}

	endHash, err := chainhash.NewHash(req.EndHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeadersFromTill] request's end hash is not valid", err))
	}

	blockHeaders, blockHeaderMetas, err := b.store.GetBlockHeadersFromTill(ctx, startHash, endHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	blockHeaderMetaBytes := make([][]byte, len(blockHeaders))
	for i, meta := range blockHeaderMetas {
		blockHeaderMetaBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        blockHeaderMetaBytes,
	}, nil
}

// GetBlockHeadersFromHeight retrieves block headers starting from a specific height.
func (b *Blockchain) GetBlockHeadersFromHeight(ctx context.Context, req *blockchain_api.GetBlockHeadersFromHeightRequest) (*blockchain_api.GetBlockHeadersFromHeightResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockHeadersFromHeight",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeadersFromHeight),
	)
	defer deferFn()

	blockHeaders, metas, err := b.store.GetBlockHeadersFromHeight(ctx, req.StartHeight, req.Limit)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	metasBytes := make([][]byte, len(metas))
	for i, meta := range metas {
		metasBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersFromHeightResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        metasBytes,
	}, nil
}

// GetBlockHeadersByHeight retrieves block headers between two specified heights.
func (b *Blockchain) GetBlockHeadersByHeight(ctx context.Context, req *blockchain_api.GetBlockHeadersByHeightRequest) (*blockchain_api.GetBlockHeadersByHeightResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockHeadersByHeight",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeadersByHeight),
	)
	defer deferFn()

	blockHeaders, metas, err := b.store.GetBlockHeadersByHeight(ctx, req.StartHeight, req.EndHeight)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	metasBytes := make([][]byte, len(metas))
	for i, meta := range metas {
		metasBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersByHeightResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        metasBytes,
	}, nil
}

// Subscribe handles subscription requests to blockchain notifications.
// It maintains the subscription until the context is cancelled or the client disconnects.
// The source parameter identifies the subscriber for logging purposes.
func (b *Blockchain) Subscribe(req *blockchain_api.SubscribeRequest, sub blockchain_api.BlockchainAPI_SubscribeServer) error {
	ctx, _, deferFn := tracing.StartTracing(sub.Context(), "Subscribe",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSubscribe),
	)
	defer deferFn()

	// Keep this subscription alive without endless loop - use a channel that blocks forever.
	ch := make(chan struct{})

	b.newSubscriptions <- subscriber{
		subscription: sub,
		done:         ch,
		source:       req.Source,
	}

	b.subscribersMu.RLock()
	noOfSubscribers := len(b.subscribers)
	b.subscribersMu.RUnlock()
	b.logger.Infof("[Blockchain] New Subscription received from %s (Total=%d).", req.Source, noOfSubscribers)

	for {
		select {
		case <-ctx.Done():
			// Client disconnected.
			b.logger.Infof("[Blockchain] GRPC client disconnected: %s", req.Source)
			return nil
		case <-ch:
			// Subscription ended.
			return nil
		}
	}
}

// GetState retrieves a value from the blockchain state storage by its key.
// It provides access to arbitrary state data stored in the blockchain.
func (b *Blockchain) GetState(ctx context.Context, req *blockchain_api.GetStateRequest) (*blockchain_api.StateResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetState",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetState),
	)
	defer deferFn()

	data, err := b.store.GetState(ctx, req.Key)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.StateResponse{
		Data: data,
	}, nil
}

// SetState stores a value in the blockchain state storage with the specified key.
// It allows storing arbitrary state data in the blockchain.
func (b *Blockchain) SetState(ctx context.Context, req *blockchain_api.SetStateRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "SetState",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSetState),
	)
	defer deferFn()

	err := b.store.SetState(ctx, req.Key, req.Data)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// GetBlockHeaderIDs retrieves block header IDs starting from a specific hash.
// It returns a list of block IDs for the requested number of headers.
func (b *Blockchain) GetBlockHeaderIDs(ctx context.Context, request *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeaderIDsResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockHeaderIDs",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaderIDs),
	)
	defer deferFn()

	startHash, err := chainhash.NewHash(request.StartHash)
	if err != nil {
		return nil, err
	}

	ids, err := b.store.GetBlockHeaderIDs(ctx, startHash, request.NumberOfHeaders)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockHeaderIDsResponse{
		Ids: ids,
	}, nil
}

// InvalidateBlock marks a block as invalid in the blockchain.
func (b *Blockchain) InvalidateBlock(ctx context.Context, request *blockchain_api.InvalidateBlockRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "InvalidateBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainInvalidateBlock),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockInvalidError("[Blockchain][InvalidateBlock] request's hash is not valid", err))
	}

	// invalidate block will also invalidate all child blocks
	err = b.store.InvalidateBlock(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	bestBlock, _, err := b.store.GetBestBlockHeader(ctx)
	if err != nil {
		b.logger.Errorf("[Blockchain] Error getting best block header: %v", err)
	} else {
		_, _ = b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: bestBlock.Hash().CloneBytes(),
		})
	}

	return &emptypb.Empty{}, nil
}

// RevalidateBlock restores a previously invalidated block.
func (b *Blockchain) RevalidateBlock(ctx context.Context, request *blockchain_api.RevalidateBlockRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "RevalidateBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainRevalidateBlock),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockInvalidError("[Blockchain][RevalidateBlock] request's hash is not valid", err))
	}

	// invalidate block will also invalidate all child blocks
	err = b.store.RevalidateBlock(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &emptypb.Empty{}, nil
}

// SendNotification broadcasts a notification to all subscribers.
func (b *Blockchain) SendNotification(ctx context.Context, req *blockchain_api.Notification) (*emptypb.Empty, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "RevalidateBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSendNotification),
	)
	defer deferFn()

	b.notifications <- req

	return &emptypb.Empty{}, nil
}

// GetBlockIsMined checks if a block has been mined in the blockchain.
func (b *Blockchain) GetBlockIsMined(ctx context.Context, req *blockchain_api.GetBlockIsMinedRequest) (*blockchain_api.GetBlockIsMinedResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockIsMined",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockIsMined),
	)
	defer deferFn()

	blockHash := chainhash.Hash(req.BlockHash)

	isMined, err := b.store.GetBlockIsMined(ctx, &blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockIsMinedResponse{
		IsMined: isMined,
	}, nil
}

// SetBlockMinedSet marks a block as mined in the blockchain.
func (b *Blockchain) SetBlockMinedSet(ctx context.Context, req *blockchain_api.SetBlockMinedSetRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "SetBlockMinedSet",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSetBlockMinedSet),
	)
	defer deferFn()

	blockHash := chainhash.Hash(req.BlockHash)

	err := b.store.SetBlockMinedSet(ctx, &blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &emptypb.Empty{}, nil
}

// GetBlocksMinedNotSet retrieves blocks that haven't been marked as mined.
func (b *Blockchain) GetBlocksMinedNotSet(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBlocksMinedNotSetResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlocksMinedNotSet",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlocksMinedNotSet),
	)
	defer deferFn()

	blocks, err := b.store.GetBlocksMinedNotSet(ctx)
	if err != nil {
		return nil, err
	}

	blockBytes := make([][]byte, len(blocks))
	for i, block := range blocks {
		blockBytes[i], err = block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetBlocksMinedNotSet] request's hash is not valid", err))
		}
	}

	return &blockchain_api.GetBlocksMinedNotSetResponse{
		BlockBytes: blockBytes,
	}, nil
}

// SetBlockSubtreesSet marks a block's subtrees as set in the blockchain.
func (b *Blockchain) SetBlockSubtreesSet(ctx context.Context, req *blockchain_api.SetBlockSubtreesSetRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "SetBlockSubtreesSet",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSetBlockSubtreesSet),
	)
	defer deferFn()

	blockHash := chainhash.Hash(req.BlockHash)

	err := b.store.SetBlockSubtreesSet(ctx, &blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	_, _ = b.SendNotification(ctx, &blockchain_api.Notification{
		Type: model.NotificationType_BlockSubtreesSet,
		Hash: blockHash.CloneBytes(),
	})

	return &emptypb.Empty{}, nil
}

// GetBlocksSubtreesNotSet retrieves blocks whose subtrees haven't been set.
func (b *Blockchain) GetBlocksSubtreesNotSet(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBlocksSubtreesNotSetResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlocksSubtreesNotSet",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlocksSubtreesNotSet),
	)
	defer deferFn()

	blocks, err := b.store.GetBlocksSubtreesNotSet(ctx)
	if err != nil {
		return nil, err
	}

	blockBytes := make([][]byte, len(blocks))
	for i, block := range blocks {
		blockBytes[i], err = block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetBlocksSubtreesNotSet] request's hash is not valid", err))
		}
	}

	return &blockchain_api.GetBlocksSubtreesNotSetResponse{
		BlockBytes: blockBytes,
	}, nil
}

// FSM related endpoints

// GetFSMCurrentState retrieves the current state of the finite state machine.
func (b *Blockchain) GetFSMCurrentState(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetFSMStateResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "GetFSMCurrentState",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetFSMCurrentState),
	)
	defer deferFn()

	var state string

	if b.finiteStateMachine == nil {
		return nil, errors.WrapGRPC(errors.NewStateInitializationError("FSM is not initialized"))
	}

	// Get the current state of the FSM
	state = b.finiteStateMachine.Current()

	// Convert the string state to FSMStateType using the map
	enumState, ok := blockchain_api.FSMStateType_value[state]
	if !ok {
		// Handle the case where the state is not found in the map
		return nil, errors.WrapGRPC(errors.NewProcessingError("invalid state: %s", state))
	}

	return &blockchain_api.GetFSMStateResponse{
		State: blockchain_api.FSMStateType(enumState),
	}, nil
}

// WaitForFSMtoTransitionToGivenState waits for the FSM to reach a specific state.
func (b *Blockchain) WaitForFSMtoTransitionToGivenState(_ context.Context, targetState blockchain_api.FSMStateType) error {
	for b.finiteStateMachine.Current() != targetState.String() {
		b.logger.Debugf("Waiting 1 second for FSM to transition to %v state, currently at: %v", targetState.String(), b.finiteStateMachine.Current())
		time.Sleep(1 * time.Second) // Wait and check again in 1 second
	}

	return nil
}

// WaitUntilFSMTransitionsFromIdleState waits for the FSM to transition from the IDLE state.
func (b *Blockchain) WaitUntilFSMTransitionFromIdleState(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// If the FSM is not initialized, we need to wait
	// or if the FSM is in the IDLE state, we need to wait
	for b.finiteStateMachine.Current() == "" || b.finiteStateMachine.Current() == blockchain_api.FSMStateType_IDLE.String() {
		b.logger.Debugf("Waiting 1 second for FSM to transition from IDLE state, currently at: %v", b.finiteStateMachine.Current())
		time.Sleep(1 * time.Second) // Wait and check again in 1 second
	}

	return &emptypb.Empty{}, nil
}

// SendFSMEvent sends an event to the finite state machine.
func (b *Blockchain) SendFSMEvent(ctx context.Context, eventReq *blockchain_api.SendFSMEventRequest) (*blockchain_api.GetFSMStateResponse, error) {
	b.logger.Infof("[Blockchain Server] Received FSM event req: %v, will send event to the FSM", eventReq)

	priorState := b.finiteStateMachine.Current()

	err := b.finiteStateMachine.Event(ctx, eventReq.Event.String())
	if err != nil {
		b.logger.Debugf("[Blockchain Server] Error sending event to FSM, state has not changed.")
		return nil, err
	}

	state := b.finiteStateMachine.Current()

	// set the state in persistent storage
	err = b.store.SetFSMState(ctx, state)
	// check if there was an error setting the state
	if err != nil {
		b.logger.Errorf("[Blockchain Server] Error setting the state in blockchain db: %v", err)
	}

	// Log the state immediately after storing it
	// b.logger.Infof("[Blockchain Server] state immediately after storing: %v", state)

	resp := &blockchain_api.GetFSMStateResponse{
		State: blockchain_api.FSMStateType(blockchain_api.FSMStateType_value[state]),
	}

	b.logger.Infof("[Blockchain Server] FSM current state: %v, response: %v", b.finiteStateMachine.Current(), resp)

	// For test purposes, we want to ensure that the state changes cannot happen too fast
	// This is to ensure that the state change is captured in the test
	duration := time.Since(b.stateChangeTimestamp)
	if duration < b.settings.BlockChain.FSMStateChangeDelay {
		b.logger.Warnf("[Blockchain Server] State transition too fast for tests. From %v to %v. Sleeping for %v before returning the response (should only happen in tests)", priorState, state, b.settings.BlockChain.FSMStateChangeDelay-duration)
		time.Sleep(b.settings.BlockChain.FSMStateChangeDelay - duration)
	}

	b.stateChangeTimestamp = time.Now()

	return resp, nil
}

// Run transitions the blockchain service to the running state.
func (b *Blockchain) Run(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// check whether the FSM is already in the RUNNING state
	if b.finiteStateMachine.Is(blockchain_api.FSMStateType_RUNNING.String()) {
		return &emptypb.Empty{}, nil
	}

	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_RUN,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	return nil, nil
}

// CatchUpBlocks transitions the service to catch up missing blocks.
func (b *Blockchain) CatchUpBlocks(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// check whether the FSM is already in the CATCHINGBLOCKS state
	if b.finiteStateMachine.Is(blockchain_api.FSMStateType_CATCHINGBLOCKS.String()) {
		return &emptypb.Empty{}, nil
	}

	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_CATCHUPBLOCKS,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	return nil, nil
}

// LegacySync transitions the service to legacy sync mode.
func (b *Blockchain) LegacySync(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// check whether the FSM is already in the LEGACYSYNC state
	if b.finiteStateMachine.Is(blockchain_api.FSMStateType_LEGACYSYNCING.String()) {
		return &emptypb.Empty{}, nil
	}

	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_LEGACYSYNC,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) Idle(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// check whether the FSM is already in the Idle state
	if b.finiteStateMachine.Is(blockchain_api.FSMStateType_IDLE.String()) {
		return &emptypb.Empty{}, nil
	}

	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_STOP,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) SetFSMState(ctx context.Context, req *blockchain_api.SetFSMStateRequest) (*emptypb.Empty, error) {
	b.finiteStateMachine.SetState(req.State.String())

	return &emptypb.Empty{}, nil
}

// Legacy endpoints

// GetBlockLocator retrieves a block locator for synchronization purposes.
func (b *Blockchain) GetBlockLocator(ctx context.Context, req *blockchain_api.GetBlockLocatorRequest) (*blockchain_api.GetBlockLocatorResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockLocator",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockLocator),
	)
	defer deferFn()

	blockHeader, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockLocator] request's hash is not valid", err))
	}

	blockHeight := req.Height

	locatorHashes, err := getBlockLocator(ctx, b.store, blockHeader, blockHeight)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewStorageError("[Blockchain][GetBlockLocator] error using blockchain store", err))
	}

	locator := make([][]byte, len(locatorHashes))
	for i, hash := range locatorHashes {
		locator[i] = hash.CloneBytes()
	}

	return &blockchain_api.GetBlockLocatorResponse{Locator: locator}, nil
}

// LocateBlockHeaders finds block headers using a locator.
func (b *Blockchain) LocateBlockHeaders(ctx context.Context, request *blockchain_api.LocateBlockHeadersRequest) (*blockchain_api.LocateBlockHeadersResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "LocateBlockHeaders",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainLocateBlockHeaders),
	)
	defer deferFn()

	locator := make([]*chainhash.Hash, len(request.Locator))
	for i, hash := range request.Locator {
		locator[i], _ = chainhash.NewHash(hash)
	}

	hashStop, _ := chainhash.NewHash(request.HashStop)

	// Get the blocks
	blockHeaders, err := b.store.LocateBlockHeaders(ctx, locator, hashStop, request.MaxHashes)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	return &blockchain_api.LocateBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
	}, nil
}

// GetBestHeightAndTime retrieves the current best block height and median time.
func (b *Blockchain) GetBestHeightAndTime(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBestHeightAndTimeResponse, error) {
	blockHeader, meta, err := b.store.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	// get the median block time for the last 11 blocks
	headers, _, err := b.store.GetBlockHeaders(ctx, blockHeader.Hash(), 11)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	prevTimeStamps := make([]time.Time, 0, 11)
	for _, header := range headers {
		prevTimeStamps = append(prevTimeStamps, time.Unix(int64(header.Timestamp), 0))
	}

	medianTimestamp, err := model.CalculateMedianTimestamp(prevTimeStamps)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[Blockchain][GetBestHeightAndTime] could not calculate median block time", err))
	}

	return &blockchain_api.GetBestHeightAndTimeResponse{
		Height: meta.Height,
		//nolint:gosec // Intentionally ignoring potential overflow for this specific use case
		Time: uint32(medianTimestamp.Unix()),
	}, nil
}

// safeClose safely closes a channel without panicking if it's already closed.
func safeClose[T any](ch chan T) {
	defer func() {
		_ = recover()
	}()

	close(ch)
}

// getBlockLocator creates a block locator for chain synchronization.
func getBlockLocator(ctx context.Context, store blockchain_store.Store, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	// From https://github.com/bitcoinsv/bsvd/blob/20910511e9006a12e90cddc9f292af8b82950f81/blockchain/chainview.go#L351
	if blockHeaderHash == nil {
		// return genesis block
		genesisBlock, err := store.GetBlockByHeight(ctx, 0)
		if err != nil {
			return nil, err
		}

		return []*chainhash.Hash{genesisBlock.Header.Hash()}, nil
	}

	// From https://github.com/bitcoinsv/bsvd/blob/20910511e9006a12e90cddc9f292af8b82950f81/blockchain/chainview.go#L351
	// Calculate the max number of entries that will ultimately be in the
	// block locator. See the description of the algorithm for how these
	// numbers are derived.
	var maxEntries uint8
	if blockHeaderHeight <= 12 {
		//nolint:gosec // Intentionally ignoring potential overflow for this specific use case
		maxEntries = uint8(blockHeaderHeight + 1)
	} else {
		// Requested hash itself + previous 10 entries + genesis block.
		// Then floor(log2(height-10)) entries for the skip portion.
		adjustedHeight := blockHeaderHeight - 10
		maxEntries = 12 + fastLog2Floor(adjustedHeight)
	}

	locator := make([]*chainhash.Hash, 0, maxEntries)

	step := uint32(1)

	ancestorBlockHeaderHash := blockHeaderHash

	ancestorBlockHeight := blockHeaderHeight // this needs to be signed

	for ancestorBlockHeaderHash != nil {
		locator = append(locator, ancestorBlockHeaderHash)

		// Nothing more to add once the genesis block has been added.
		if ancestorBlockHeight == 0 {
			break
		}

		// Calculate height of previous node to include ensuring the
		// final node is the genesis block.
		height := ancestorBlockHeight - step

		// check whether step > ancestorBlockHeight, which would give a negative value for height, but
		// since height is an uint32, it will wrap around to the maximum value and overflow
		if step > ancestorBlockHeight {
			height = 0
		}

		ancestorBlock, _, err := store.GetBlockInChainByHeightHash(ctx, height, ancestorBlockHeaderHash)
		if err != nil {
			return nil, err
		}

		ancestorBlockHeaderHash = ancestorBlock.Header.Hash()

		ancestorBlockHeight = height

		// Once 11 entries have been included, start doubling the
		// distance between included hashes.
		if len(locator) > 10 {
			step *= 2
		}
	}

	return locator, nil
}

func getBlockHeadersToCommonAncestor(ctx context.Context, store blockchain_store.Store, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	const (
		numberOfHeaders = 100
		searchLimit     = 10
	)

	var (
		commonAncestorMeta *model.BlockHeaderMeta
	)

	hashStart := hashTarget
	headerHistory := make([]*model.BlockHeader, 0, searchLimit*numberOfHeaders)
	headerMetaHistory := make([]*model.BlockHeaderMeta, 0, searchLimit*numberOfHeaders)

	for searchCount := 0; searchCount < searchLimit; searchCount++ {
		headers, headerMetas, err := store.GetBlockHeaders(ctx, hashStart, numberOfHeaders)
		if err != nil {
			return nil, nil, errors.NewStorageError("failed to get block headers", err)
		}

		headerMap := make(map[chainhash.Hash]int, len(headers))
		for idx, header := range headers {
			headerMap[*header.Hash()] = idx
		}

		headerHistory = append(headerHistory, headers...)
		headerMetaHistory = append(headerMetaHistory, headerMetas...)

		for _, hash := range blockLocatorHashes {
			if idx, ok := headerMap[*hash]; ok {
				commonAncestorMeta = headerMetas[idx]
				break
			}
		}

		if commonAncestorMeta != nil {
			break
		}

		if len(headers) <= 1 {
			break
		}

		// start over with the next 100 block headers
		// to find the common ancestor
		hashStart = headers[len(headers)-1].HashPrevBlock
	}

	if commonAncestorMeta == nil {
		return nil, nil, errors.NewNotFoundError("common ancestor hash not found after scanning last %d headers", searchLimit*numberOfHeaders)
	}

	_, metaTarget, err := store.GetBlockHeader(ctx, hashTarget)
	if err != nil {
		return nil, nil, errors.NewStorageError("failed to get block header", err)
	}

	numberOfMissingHeaders := uint64(metaTarget.Height - commonAncestorMeta.Height + 1)

	return headerHistory[:numberOfMissingHeaders], headerMetaHistory[:numberOfMissingHeaders], nil
}
