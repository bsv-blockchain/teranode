package blockchain

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/looplab/fsm"
	"github.com/ordishs/go-utils"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type subscriber struct {
	subscription blockchain_api.BlockchainAPI_SubscribeServer
	source       string
	done         chan struct{}
}

// Blockchain type carries the logger within it
type Blockchain struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	addBlockChan       chan *blockchain_api.AddBlockRequest
	store              blockchain_store.Store
	subtreeStore       blob.Store
	utxoStore          utxo.Store
	logger             ulogger.Logger
	newSubscriptions   chan subscriber
	deadSubscriptions  chan subscriber
	subscribers        map[subscriber]bool
	notifications      chan *blockchain_api.Notification
	newBlock           chan struct{}
	difficulty         *Difficulty
	blockKafkaProducer util.KafkaProducerI
	stats              *gocore.Stat
	finiteStateMachine *fsm.FSM
	client             ClientI
}

// New will return a server instance with the logger stored within it
func New(ctx context.Context, logger ulogger.Logger, store blockchain_store.Store, subtreeStore blob.Store,
	utxoStore utxo.Store) (*Blockchain, error) {

	initPrometheusMetrics()

	difficultyAdjustmentWindow, _ := gocore.Config().GetInt("difficulty_adjustment_window", 144)

	d, err := NewDifficulty(store, logger, difficultyAdjustmentWindow)
	if err != nil {
		logger.Errorf("[BlockAssembler] Couldn't create difficulty: %v", err)
	}

	return &Blockchain{
		store:             store,
		subtreeStore:      subtreeStore,
		utxoStore:         utxoStore,
		logger:            logger,
		addBlockChan:      make(chan *blockchain_api.AddBlockRequest, 10),
		newSubscriptions:  make(chan subscriber, 10),
		deadSubscriptions: make(chan subscriber, 10),
		subscribers:       make(map[subscriber]bool),
		notifications:     make(chan *blockchain_api.Notification, 100),
		newBlock:          make(chan struct{}, 10),
		difficulty:        d,
		stats:             gocore.NewStat("blockchain"),
	}, nil
}

func (b *Blockchain) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (b *Blockchain) Init(_ context.Context) error {

	b.finiteStateMachine = b.NewFiniteStateMachine()

	return nil
}

// Start function
func (b *Blockchain) Start(ctx context.Context) error {

	blocksKafkaURL, err, ok := gocore.Config().GetURL("kafka_blocksFinalConfig")
	if err == nil && ok {
		b.logger.Infof("[Blockchain] Starting Kafka producer for blocks")
		if _, b.blockKafkaProducer, err = util.ConnectToKafka(blocksKafkaURL); err != nil {
			return errors.WrapGRPC(errors.NewServiceUnavailableError("[Blockchain] error connecting to kafka", err))
		}
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				b.logger.Infof("[Blockchain] Stopping channel listeners go routine")
				for sub := range b.subscribers {
					safeClose(sub.done)
				}
				return
			case notification := <-b.notifications:
				start := gocore.CurrentTime()
				func() {
					b.logger.Debugf("[Blockchain Server] Sending notification: %s", notification)

					for sub := range b.subscribers {
						b.logger.Debugf("[Blockchain] Sending notification to %s in background: %s", sub.source, notification.Stringify())
						go func(s subscriber) {
							b.logger.Debugf("[Blockchain] Sending notification to %s: %s", s.source, notification.Stringify())
							if err := s.subscription.Send(notification); err != nil {
								b.deadSubscriptions <- s
							}
						}(sub)
					}
				}()
				b.stats.NewStat("channel-subscription.Send", true).AddTime(start)

			case s := <-b.newSubscriptions:
				b.subscribers[s] = true
				b.logger.Infof("[Blockchain] New Subscription received from %s (Total=%d).", s.source, len(b.subscribers))

			case s := <-b.deadSubscriptions:
				delete(b.subscribers, s)
				safeClose(s.done)
				b.logger.Infof("[Blockchain] Subscription removed (Total=%d).", len(b.subscribers))
			}
		}
	}()

	httpAddress, ok := gocore.Config().Get("blockchain_httpListenAddress")
	if !ok {
		return errors.NewConfigurationError("[Miner] No blockchain_httpListenAddress specified")
	} else {
		e := echo.New()
		e.HideBanner = true
		e.HidePort = true

		e.Use(middleware.Recover())

		e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
			AllowOrigins: []string{"*"},
			AllowMethods: []string{echo.GET},
		}))

		e.GET("/invalidate/:hash", func(c echo.Context) error {
			hashStr := c.Param("hash")
			hash, err := chainhash.NewHashFromStr(hashStr)
			if err != nil {
				return c.String(http.StatusBadRequest, fmt.Sprintf("invalid hash: %v", err))
			}

			_, err = b.InvalidateBlock(ctx, &blockchain_api.InvalidateBlockRequest{
				BlockHash: hash.CloneBytes(),
			})

			if err != nil {
				return c.String(http.StatusInternalServerError, fmt.Sprintf("error invalidating block: %v", err))
			}

			return c.String(http.StatusOK, fmt.Sprintf("block invalidated: %s", hashStr))
		})

		e.GET("/revalidate/:hash", func(c echo.Context) error {
			hashStr := c.Param("hash")
			hash, err := chainhash.NewHashFromStr(hashStr)
			if err != nil {
				return c.String(http.StatusBadRequest, fmt.Sprintf("invalid hash: %v", err))
			}

			_, err = b.RevalidateBlock(ctx, &blockchain_api.RevalidateBlockRequest{
				BlockHash: hash.CloneBytes(),
			})

			if err != nil {
				return c.String(http.StatusInternalServerError, fmt.Sprintf("error revalidating block: %v", err))
			}

			return c.String(http.StatusOK, fmt.Sprintf("block revalidated: %s", hashStr))
		})

		go func() {
			if err := e.Start(httpAddress); err != nil {
				b.logger.Errorf("[Blockchain] failed to start http server: %v", err)
			}
		}()

	}

	// this will block
	if err := util.StartGRPCServer(ctx, b.logger, "blockchain", func(server *grpc.Server) {
		blockchain_api.RegisterBlockchainAPIServer(server, b)
	}); err != nil {
		return errors.WrapGRPC(errors.NewServiceNotStartedError("[Blockchain] can't start GRPC server", err))
	}

	return nil
}

func (b *Blockchain) Stop(_ context.Context) error {
	return nil
}

func (b *Blockchain) HealthGRPC(_ context.Context, _ *emptypb.Empty) (*blockchain_api.HealthResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		b.stats.NewStat("Health", true).AddTime(start)
	}()

	prometheusBlockchainHealth.Inc()

	return &blockchain_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

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

	b.logger.Infof("[Blockchain] AddBlock called: %s", header.Hash().String())

	btCoinbaseTx, err := bt.NewTxFromBytes(request.CoinbaseTx)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain] can't create the coinbase transaction", err))
	}

	subtreeHashes := make([]*chainhash.Hash, len(request.SubtreeHashes))
	for i, subtreeHash := range request.SubtreeHashes {
		subtreeHashes[i], err = chainhash.NewHash(subtreeHash)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain] unable to create subtree hash", err))
		}
	}

	block := &model.Block{
		Header:           header,
		CoinbaseTx:       btCoinbaseTx,
		Subtrees:         subtreeHashes,
		TransactionCount: request.TransactionCount,
		SizeInBytes:      request.SizeInBytes,
	}

	_, height, err := b.store.StoreBlock(ctx, block, request.PeerId)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	block.Height = height

	b.logger.Debugf("[BlockPersister] checking for Kafka producer: %v", b.blockKafkaProducer != nil)
	if b.blockKafkaProducer != nil {
		go func(block *model.Block) {
			blockBytes, err := block.Bytes()
			if err != nil {
				b.logger.Errorf("[Blockchain] Error serializing block: %v", err)
			} else {
				b.logger.Debugf("[BlockPersister] sending block to kafka: %s", block.String())
				// producer has built-in retry mechanism
				if err = b.blockKafkaProducer.Send(block.Header.Hash().CloneBytes(), blockBytes); err != nil {
					// TODO: #938 Alter FSM state and keep going forever until it works and then reset the FSM state?
					b.logger.Errorf("[Blockchain] Error sending block to kafka: %v", err)
				}
			}
		}(block)
	}

	_, _ = b.SendNotification(ctx, &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: block.Hash().CloneBytes(),
	})

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) GetBlock(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlock),
		tracing.WithLogMessage(b.logger, "[GetBlock] called for %s", utils.ReverseAndHexEncodeSlice(request.Hash)),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain] request's hash is not valid", err))
	}

	block, height, err := b.store.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	subtreeHashes := make([][]byte, len(block.Subtrees))
	for i, subtreeHash := range block.Subtrees {
		subtreeHashes[i] = subtreeHash[:]
	}

	return &blockchain_api.GetBlockResponse{
		Header:           block.Header.Bytes(),
		Height:           height,
		CoinbaseTx:       block.CoinbaseTx.Bytes(),
		SubtreeHashes:    subtreeHashes,
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
	}, nil
}

func (b *Blockchain) GetBlocks(ctx context.Context, req *blockchain_api.GetBlocksRequest) (*blockchain_api.GetBlocksResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlocks",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaders),
		tracing.WithLogMessage(b.logger, "[GetBlocks] called for %s", utils.ReverseAndHexEncodeSlice(req.Hash)),
	)
	defer deferFn()

	startHash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain] request's hash is not valid", err))
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

	return &blockchain_api.GetBlockResponse{
		Header:           block.Header.Bytes(),
		Height:           request.Height,
		CoinbaseTx:       block.CoinbaseTx.Bytes(),
		SubtreeHashes:    subtreeHashes,
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
	}, nil
}

func (b *Blockchain) GetBlockStats(ctx context.Context, _ *emptypb.Empty) (*model.BlockStats, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockStats",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockStats),
	)
	defer deferFn()

	resp, err := b.store.GetBlockStats(ctx)
	return resp, errors.WrapGRPC(err)
}

func (b *Blockchain) GetBlockGraphData(ctx context.Context, req *blockchain_api.GetBlockGraphDataRequest) (*model.BlockDataPoints, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockGraphData",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockGraphData),
	)
	defer deferFn()

	resp, err := b.store.GetBlockGraphData(ctx, req.PeriodMillis)
	return resp, errors.WrapGRPC(err)
}

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

func (b *Blockchain) GetNextWorkRequired(ctx context.Context, request *blockchain_api.GetNextWorkRequiredRequest) (*blockchain_api.GetNextWorkRequiredResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetNextWorkRequired",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetNextWorkRequired),
	)
	defer deferFn()

	var nBits *model.NBit
	nBitsString, _ := gocore.Config().Get("mining_n_bits", "2000ffff") // TEMP By default, we want hashes with 2 leading zeros. genesis was 1d00ffff

	if b.difficulty == nil {
		b.logger.Debugf("difficulty is null")
		nBits, _ = model.NewNBitFromString(nBitsString)
	} else {

		hash, err := chainhash.NewHash(request.BlockHash)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain] request's block hash is not valid", err))
		}

		blockHeader, meta, err := b.store.GetBlockHeader(ctx, hash)
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}

		nBitsp, err := b.difficulty.GetNextWorkRequired(ctx, blockHeader, meta.Height)
		if err == nil {
			nBits = nBitsp
		} else {
			b.logger.Debugf("error in GetNextWorkRequired: %v", err)
			nBits, _ = model.NewNBitFromString(nBitsString)
		}

		b.logger.Debugf("difficulty adjustment. Difficulty set to %s", nBits.String())
	}

	return &blockchain_api.GetNextWorkRequiredResponse{
		Bits: nBits.CloneBytes(),
	}, nil
}

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

func (b *Blockchain) GetBlockExists(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockExistsResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockExists",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockExists),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain] request's hash is not valid", err))
	}

	exists, err := b.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockExistsResponse{
		Exists: exists,
	}, nil
}

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
	}, nil
}

func (b *Blockchain) GetBlockHeader(ctx context.Context, req *blockchain_api.GetBlockHeaderRequest) (*blockchain_api.GetBlockHeaderResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBestBlockHeader",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeader),
	)
	defer deferFn()

	hash, err := chainhash.NewHash(req.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain] request's hash is not valid", err))
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

func (b *Blockchain) GetBlockHeaders(ctx context.Context, req *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockHeaders",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaders),
	)
	defer deferFn()

	startHash, err := chainhash.NewHash(req.StartHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain] request's hash is not valid", err))
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

func (b *Blockchain) InvalidateBlock(ctx context.Context, request *blockchain_api.InvalidateBlockRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "InvalidateBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainInvalidateBlock),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockInvalidError("[Blockchain] request's hash is not valid", err))
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

func (b *Blockchain) RevalidateBlock(ctx context.Context, request *blockchain_api.RevalidateBlockRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "RevalidateBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainRevalidateBlock),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockInvalidError("[Blockchain] request's hash is not valid", err))
	}

	// invalidate block will also invalidate all child blocks
	err = b.store.RevalidateBlock(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) SendNotification(ctx context.Context, req *blockchain_api.Notification) (*emptypb.Empty, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "RevalidateBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSendNotification),
	)
	defer deferFn()

	b.notifications <- req

	return &emptypb.Empty{}, nil
}

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
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain] request's hash is not valid", err))
		}
	}

	return &blockchain_api.GetBlocksMinedNotSetResponse{
		BlockBytes: blockBytes,
	}, nil
}

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
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain] request's hash is not valid", err))
		}
	}

	return &blockchain_api.GetBlocksSubtreesNotSetResponse{
		BlockBytes: blockBytes,
	}, nil
}

func (b *Blockchain) SendFSMEvent(ctx context.Context, eventReq *blockchain_api.SendFSMEventRequest) (*blockchain_api.GetFSMStateResponse, error) {
	b.logger.Debugf("[Blockchain Server] Received FSM event req: %v, will send event to the FSM", eventReq)

	err := b.finiteStateMachine.Event(ctx, eventReq.Event.String())
	if err != nil {
		return nil, err
	}
	state := b.finiteStateMachine.Current()

	// Log the state immediately after storing it
	b.logger.Infof("[Blockchain Server] state immediately after storing: %v", state)

	resp := &blockchain_api.GetFSMStateResponse{
		State: blockchain_api.FSMStateType(blockchain_api.FSMStateType_value[state]),
	}

	b.logger.Debugf("[Blockchain Server] FSM current state: %v", b.finiteStateMachine.Current(), ", response: %v", resp)

	return resp, nil
}

func (b *Blockchain) Run(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_RUN,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	b.client.StoreFSMState(b.finiteStateMachine.Current())

	return nil, nil
}

func (b *Blockchain) Mine(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_MINE,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	b.client.StoreFSMState(b.finiteStateMachine.Current())

	return nil, nil
}

func (b *Blockchain) Restore(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_RESTORE,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	b.client.StoreFSMState(b.finiteStateMachine.Current())

	return nil, nil
}

func (b *Blockchain) CatchUpBlocks(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_CATCHUPBLOCKS,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	b.client.StoreFSMState(b.finiteStateMachine.Current())

	return nil, nil
}

func (b *Blockchain) CatchUpTransactions(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_CATCHUPTXS,
	}

	b.logger.Infof("[Blockchain] sending CatchUpTransactions event")

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		b.logger.Errorf("[Blockchain] error sending CatchUpTransactions event: %v", err)
		return nil, err
	}
	b.logger.Infof("[Blockchain] Storing CatchUpTransactions state")
	b.client.StoreFSMState(b.finiteStateMachine.Current())

	return nil, nil
}

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

	// Convert the string state to FSMEventType using the map
	enumState, ok := blockchain_api.FSMStateType_value[state]
	if !ok {
		// Handle the case where the state is not found in the map
		return nil, errors.WrapGRPC(errors.NewProcessingError("invalid state: %s", state))
	}

	return &blockchain_api.GetFSMStateResponse{
		State: blockchain_api.FSMStateType(enumState),
	}, nil
}

func (b *Blockchain) GetBlockLocator(ctx context.Context, req *blockchain_api.GetBlockLocatorRequest) (*blockchain_api.GetBlockLocatorResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBlockLocator",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockLocator),
	)
	defer deferFn()

	blockHeader, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain] request's hash is not valid", err))
	}
	blockHeight := req.Height

	locatorHashes, err := getBlockLocator(ctx, b.store, blockHeader, blockHeight)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewStorageError("[Blockchain] error using blockchain store", err))
	}

	locator := make([][]byte, len(locatorHashes))
	for i, hash := range locatorHashes {
		locator[i] = hash.CloneBytes()
	}

	return &blockchain_api.GetBlockLocatorResponse{Locator: locator}, nil
}

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
		return nil, errors.WrapGRPC(errors.NewProcessingError("[Blockchain] could not calculate median block time", err))
	}

	return &blockchain_api.GetBestHeightAndTimeResponse{
		Height: meta.Height,
		Time:   uint32(medianTimestamp.Unix()),
	}, nil
}

func safeClose[T any](ch chan T) {
	defer func() {
		_ = recover()
	}()

	close(ch)
}

func (b *Blockchain) SetClient(client ClientI) {
	b.client = client
}

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
		maxEntries = uint8(blockHeaderHeight) + 1
	} else {
		// Requested hash itself + previous 10 entries + genesis block.
		// Then floor(log2(height-10)) entries for the skip portion.
		adjustedHeight := uint32(blockHeaderHeight) - 10
		maxEntries = 12 + fastLog2Floor(adjustedHeight)
	}
	locator := make([]*chainhash.Hash, 0, maxEntries)

	step := int32(1)
	ancestorBlockHeaderHash := blockHeaderHash
	ancestorBlockHeight := int32(blockHeaderHeight) // this needs to be signed
	for ancestorBlockHeaderHash != nil {
		locator = append(locator, ancestorBlockHeaderHash)

		// Nothing more to add once the genesis block has been added.
		if ancestorBlockHeight == 0 {
			break
		}

		// Calculate height of previous node to include ensuring the
		// final node is the genesis block.
		height := int32(ancestorBlockHeight) - step
		if height < 0 {
			height = 0
		}

		ancestorBlock, err := store.GetBlockByHeight(ctx, uint32(height))
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
