package blockchain

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/looplab/fsm"
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
}

// New will return a server instance with the logger stored within it
func New(ctx context.Context, logger ulogger.Logger) (*Blockchain, error) {
	initPrometheusMetrics()

	blockchainStoreURL, err, found := gocore.Config().GetURL("blockchain_store")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("no blockchain_store setting found")
	}

	s, err := blockchain_store.NewStore(logger, blockchainStoreURL)
	if err != nil {
		return nil, err
	}

	difficultyAdjustmentWindow, _ := gocore.Config().GetInt("difficulty_adjustment_window", 144)

	d, err := NewDifficulty(s, logger, difficultyAdjustmentWindow)
	if err != nil {
		logger.Errorf("[BlockAssembler] Couldn't create difficulty: %v", err)
	}

	return &Blockchain{
		store:             s,
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
			return errors.WrapGRPC(errors.New(errors.ERR_SERVICE_UNAVAILABLE, "[Blockchain] error connecting to kafka", err))
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

					if notification.Type == model.NotificationType_FSMEvent {
						notificationMetadata := notification.Metadata.GetMetadata()
						event := blockchain_api.FSMEventType(blockchain_api.FSMEventType_value[notificationMetadata["event"]])
						b.logger.Debugf("[Blockchain Server] Sending FSM event: %s", event.String())
						err := b.finiteStateMachine.Event(ctx, event.String())
						if err != nil {
							b.logger.Errorf("[Blockchain Server] Error sending FSM event: %v", err)
						}
						b.logger.Debugf("[Blockchain Server] FSM current state: %s", b.finiteStateMachine.Current())
					}

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
		b.logger.Fatalf("[Miner] No blockchain_httpListenAddress specified")
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
		return errors.WrapGRPC(errors.New(errors.ERR_SERVICE_NOT_STARTED, "[Blockchain] can't start GRPC server", err))
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
	start, stat, ctx1 := util.NewStatFromContext(ctx, "AddBlock", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainAddBlock.Inc()

	header, err := model.NewBlockHeaderFromBytes(request.Header)
	if err != nil {
		return nil, err
	}

	b.logger.Infof("[Blockchain] AddBlock called: %s", header.Hash().String())

	btCoinbaseTx, err := bt.NewTxFromBytes(request.CoinbaseTx)
	if err != nil {
		return nil, errors.WrapGRPC(errors.New(errors.ERR_INVALID_ARGUMENT, "[Blockchain] can't create the coinbase transaction", err))
	}

	subtreeHashes := make([]*chainhash.Hash, len(request.SubtreeHashes))
	for i, subtreeHash := range request.SubtreeHashes {
		subtreeHashes[i], err = chainhash.NewHash(subtreeHash)
		if err != nil {
			return nil, errors.WrapGRPC(errors.New(errors.ERR_INVALID_ARGUMENT, "[Blockchain] unable to create subtree hash", err))
		}
	}

	block := &model.Block{
		Header:           header,
		CoinbaseTx:       btCoinbaseTx,
		Subtrees:         subtreeHashes,
		TransactionCount: request.TransactionCount,
		SizeInBytes:      request.SizeInBytes,
	}

	_, err = b.store.StoreBlock(ctx1, block, request.PeerId)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	b.logger.Debugf("[BlockPersister] checking for Kafka producer: %v", b.blockKafkaProducer != nil)
	if b.blockKafkaProducer != nil {
		// TODO add a retry mechanism
		go func(block *model.Block) {
			blockBytes, err := block.Bytes()
			if err != nil {
				b.logger.Errorf("[Blockchain] Error serializing block: %v", err)
			} else {
				b.logger.Infof("[BlockPersister] sending block to kafka: %s", block.String())
				if err = b.blockKafkaProducer.Send(block.Header.Hash().CloneBytes(), blockBytes); err != nil {
					b.logger.Errorf("[Blockchain] Error sending block to kafka: %v", err)
				}
			}
		}(block)
	}

	_, _ = b.SendNotification(ctx1, &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: block.Hash().CloneBytes(),
	})

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) GetBlock(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlock", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBlock.Inc()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.New(errors.ERR_BLOCK_NOT_FOUND, "[Blockchain] request's hash is not valid", err))
	}

	block, height, err := b.store.GetBlock(ctx1, blockHash)
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
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlocks", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBlockHeaders.Inc()

	startHash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.New(errors.ERR_BLOCK_NOT_FOUND, "[Blockchain] request's hash is not valid", err))
	}

	blocks, err := b.store.GetBlocks(ctx1, startHash, req.Count)
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
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlock", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBlock.Inc()

	block, err := b.store.GetBlockByHeight(ctx1, request.Height)
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
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlockStats", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	resp, err := b.store.GetBlockStats(ctx1)
	return resp, errors.WrapGRPC(err)
}

func (b *Blockchain) GetBlockGraphData(ctx context.Context, req *blockchain_api.GetBlockGraphDataRequest) (*model.BlockDataPoints, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlockGraphData", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	resp, err := b.store.GetBlockGraphData(ctx1, req.PeriodMillis)
	return resp, errors.WrapGRPC(err)
}

func (b *Blockchain) GetLastNBlocks(ctx context.Context, request *blockchain_api.GetLastNBlocksRequest) (*blockchain_api.GetLastNBlocksResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetLastNBlocks", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetLastNBlocks.Inc()

	blockInfo, err := b.store.GetLastNBlocks(ctx1, request.NumberOfBlocks, request.IncludeOrphans, request.FromHeight)
	if err != nil {
		return nil, errors.WrapGRPC(err)

	}

	return &blockchain_api.GetLastNBlocksResponse{
		Blocks: blockInfo,
	}, nil
}

func (b *Blockchain) GetSuitableBlock(ctx context.Context, request *blockchain_api.GetSuitableBlockRequest) (*blockchain_api.GetSuitableBlockResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetSuitableBlock", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetSuitableBlock.Inc()

	blockInfo, err := b.store.GetSuitableBlock(ctx1, (*chainhash.Hash)(request.Hash))
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetSuitableBlockResponse{
		Block: blockInfo,
	}, nil
}

func (b *Blockchain) GetNextWorkRequired(ctx context.Context, request *blockchain_api.GetNextWorkRequiredRequest) (*blockchain_api.GetNextWorkRequiredResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetNextWorkRequired", b.stats)
	defer func() {
		stat.AddTime(start)
	}()
	var nBits model.NBit

	prometheusBlockchainGetNextWorkRequired.Inc()
	nBitsString, _ := gocore.Config().Get("mining_n_bits", "2000ffff") // TEMP By default, we want hashes with 2 leading zeros. genesis was 1d00ffff

	if b.difficulty == nil {
		b.logger.Debugf("difficulty is null")
		nBits = model.NewNBitFromString(nBitsString)
	} else {

		hash, err := chainhash.NewHash(request.BlockHash)
		if err != nil {
			return nil, errors.WrapGRPC(errors.New(errors.ERR_BLOCK_NOT_FOUND, "[Blockchain] request's block hash is not valid", err))
		}

		blockHeader, meta, err := b.store.GetBlockHeader(ctx1, hash)
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}

		nBitsp, err := b.difficulty.GetNextWorkRequired(ctx1, blockHeader, meta.Height)
		if err == nil {
			nBits = *nBitsp
		} else {
			b.logger.Debugf("error in GetNextWorkRequired: %v", err)
			nBits = model.NewNBitFromString(nBitsString)
		}

		b.logger.Debugf("difficulty adjustment. Difficulty set to %s", nBits.String())
	}

	return &blockchain_api.GetNextWorkRequiredResponse{
		Bits: nBits.CloneBytes(),
	}, nil
}

func (b *Blockchain) GetHashOfAncestorBlock(ctx context.Context, request *blockchain_api.GetHashOfAncestorBlockRequest) (*blockchain_api.GetHashOfAncestorBlockResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetHashOfAncestorBlock", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetHashOfAncestorBlock.Inc()

	hash, err := b.store.GetHashOfAncestorBlock(ctx1, (*chainhash.Hash)(request.Hash), int(request.Depth))
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetHashOfAncestorBlockResponse{
		Hash: hash[:],
	}, nil
}

func (b *Blockchain) GetBlockExists(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockExistsResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlockExists", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBlockExists.Inc()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.New(errors.ERR_BLOCK_NOT_FOUND, "[Blockchain] request's hash is not valid", err))
	}

	exists, err := b.store.GetBlockExists(ctx1, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockExistsResponse{
		Exists: exists,
	}, nil
}

func (b *Blockchain) GetBestBlockHeader(ctx context.Context, empty *emptypb.Empty) (*blockchain_api.GetBlockHeaderResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBestBlockHeader", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBestBlockHeader.Inc()

	chainTip, meta, err := b.store.GetBestBlockHeader(ctx1)
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
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlockHeader", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBlockHeader.Inc()

	hash, err := chainhash.NewHash(req.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.New(errors.ERR_BLOCK_NOT_FOUND, "[Blockchain] request's hash is not valid", err))
	}

	blockHeader, meta, err := b.store.GetBlockHeader(ctx1, hash)
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
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlockHeaders", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBlockHeaders.Inc()

	startHash, err := chainhash.NewHash(req.StartHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.New(errors.ERR_BLOCK_NOT_FOUND, "[Blockchain] request's hash is not valid", err))
	}

	blockHeaders, heights, err := b.store.GetBlockHeaders(ctx1, startHash, req.NumberOfHeaders)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	return &blockchain_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
		Heights:      heights,
	}, nil
}

func (b *Blockchain) GetBlockHeadersFromHeight(ctx context.Context, req *blockchain_api.GetBlockHeadersFromHeightRequest) (*blockchain_api.GetBlockHeadersFromHeightResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlockHeadersFromHeight", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	blockHeaders, metas, err := b.store.GetBlockHeadersFromHeight(ctx1, req.StartHeight, req.Limit)
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
	start, stat, _ := util.NewStatFromContext(sub.Context(), "Subscribe", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainSubscribe.Inc()

	// Keep this subscription alive without endless loop - use a channel that blocks forever.
	ch := make(chan struct{})

	b.newSubscriptions <- subscriber{
		subscription: sub,
		done:         ch,
		source:       req.Source,
	}

	for {
		select {
		case <-sub.Context().Done():
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
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetState", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetState.Inc()

	data, err := b.store.GetState(ctx1, req.Key)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.StateResponse{
		Data: data,
	}, nil
}

func (b *Blockchain) SetState(ctx context.Context, req *blockchain_api.SetStateRequest) (*emptypb.Empty, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "SetState", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainSetState.Inc()

	err := b.store.SetState(ctx1, req.Key, req.Data)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) GetBlockHeaderIDs(ctx context.Context, request *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeaderIDsResponse, error) {
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
	start, stat, ctx1 := util.NewStatFromContext(ctx, "InvalidateBlock", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainSetState.Inc()

	blockHash, err := chainhash.NewHash(request.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.New(errors.ERR_BLOCK_INVALID, "[Blockchain] request's hash is not valid", err))
	}

	// invalidate block will also invalidate all child blocks
	err = b.store.InvalidateBlock(ctx1, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	bestBlock, _, err := b.store.GetBestBlockHeader(ctx1)
	if err != nil {
		b.logger.Errorf("[Blockchain] Error getting best block header: %v", err)
	} else {
		_, _ = b.SendNotification(ctx1, &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: bestBlock.Hash().CloneBytes(),
		})
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) RevalidateBlock(ctx context.Context, request *blockchain_api.RevalidateBlockRequest) (*emptypb.Empty, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "RevalidateBlock", b.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainSetState.Inc()

	blockHash, err := chainhash.NewHash(request.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.New(errors.ERR_BLOCK_INVALID, "[Blockchain] request's hash is not valid", err))
	}

	// invalidate block will also invalidate all child blocks
	err = b.store.RevalidateBlock(ctx1, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) SendNotification(_ context.Context, req *blockchain_api.Notification) (*emptypb.Empty, error) {
	start, stat, _ := util.NewStatFromContext(context.Background(), "SendNotification", b.stats)
	defer func() {
		stat.AddTime(start)
	}()
	if req.Type == model.NotificationType_FSMEvent {
		b.logger.Infof("[Blockchain Server] SendNotification FSMevent called: %s", req.GetMetadata())
	}

	prometheusBlockchainSendNotification.Inc()
	b.notifications <- req

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) SetBlockMinedSet(ctx context.Context, req *blockchain_api.SetBlockMinedSetRequest) (*emptypb.Empty, error) {
	blockHash := chainhash.Hash(req.BlockHash)
	err := b.store.SetBlockMinedSet(ctx, &blockHash)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) GetBlocksMinedNotSet(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBlocksMinedNotSetResponse, error) {
	blocks, err := b.store.GetBlocksMinedNotSet(ctx)
	if err != nil {
		return nil, err
	}

	blockBytes := make([][]byte, len(blocks))
	for i, block := range blocks {
		blockBytes[i], err = block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(errors.New(errors.ERR_INVALID_ARGUMENT, "[Blockchain] request's hash is not valid", err))
		}
	}

	return &blockchain_api.GetBlocksMinedNotSetResponse{
		BlockBytes: blockBytes,
	}, nil
}

func (b *Blockchain) SetBlockSubtreesSet(ctx context.Context, req *blockchain_api.SetBlockSubtreesSetRequest) (*emptypb.Empty, error) {
	blockHash := chainhash.Hash(req.BlockHash)
	err := b.store.SetBlockSubtreesSet(ctx, &blockHash)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) GetBlocksSubtreesNotSet(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBlocksSubtreesNotSetResponse, error) {
	blocks, err := b.store.GetBlocksSubtreesNotSet(ctx)
	if err != nil {
		return nil, err
	}

	blockBytes := make([][]byte, len(blocks))
	for i, block := range blocks {
		blockBytes[i], err = block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(errors.New(errors.ERR_INVALID_ARGUMENT, "[Blockchain] request's hash is not valid", err))
		}
	}

	return &blockchain_api.GetBlocksSubtreesNotSetResponse{
		BlockBytes: blockBytes,
	}, nil
}

func (b *Blockchain) GetFSMCurrentState(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetFSMStateResponse, error) {
	var state string

	if b.finiteStateMachine == nil {
		return nil, fmt.Errorf("FSM is not initialized")
	}

	// Get the current state of the FSM
	state = b.finiteStateMachine.Current()

	// Convert the string state to FSMEventType using the map
	enumState, ok := blockchain_api.FSMStateType_value[state]
	if !ok {
		// Handle the case where the state is not found in the map
		return nil, errors.New(errors.ERR_PROCESSING, "invalid state: "+state)
	}

	return &blockchain_api.GetFSMStateResponse{
		State: blockchain_api.FSMStateType(enumState),
	}, nil
}

func safeClose[T any](ch chan T) {
	defer func() {
		_ = recover()
	}()

	close(ch)
}
