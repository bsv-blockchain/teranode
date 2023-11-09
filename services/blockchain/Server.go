package blockchain

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var stats = gocore.NewStat("blockchain")

type subscriber struct {
	subscription blockchain_api.BlockchainAPI_SubscribeServer
	source       string
	done         chan struct{}
}

// Blockchain type carries the logger within it
type Blockchain struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	addBlockChan        chan *blockchain_api.AddBlockRequest
	store               blockchain_store.Store
	logger              utils.Logger
	newSubscriptions    chan subscriber
	deadSubscriptions   chan subscriber
	subscribers         map[subscriber]bool
	notifications       chan *blockchain_api.Notification
	newBlock            chan struct{}
	subscriptionCtx     context.Context
	cancelSubscriptions context.CancelFunc
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockchain_grpcListenAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger) (*Blockchain, error) {
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

	subscriptionCtx, cancelSubscriptions := context.WithCancel(context.Background())

	return &Blockchain{
		store:               s,
		logger:              logger,
		addBlockChan:        make(chan *blockchain_api.AddBlockRequest, 10),
		newSubscriptions:    make(chan subscriber, 10),
		deadSubscriptions:   make(chan subscriber, 10),
		subscribers:         make(map[subscriber]bool),
		notifications:       make(chan *blockchain_api.Notification, 100),
		newBlock:            make(chan struct{}, 10),
		subscriptionCtx:     subscriptionCtx,
		cancelSubscriptions: cancelSubscriptions,
	}, nil
}

func (b *Blockchain) Init(ctx context.Context) error {
	return nil
}

// Start function
func (b *Blockchain) Start(ctx context.Context) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				b.logger.Infof("[Blockchain] Stopping channel listeners go routine")
				return
			case notification := <-b.notifications:
				start := gocore.CurrentNanos()
				func() {
					b.logger.Debugf("[Blockchain] Sending notification: %s", notification.Stringify())
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
				stats.NewStat("channel-subscription.Send", true).AddTime(start)

			case s := <-b.newSubscriptions:
				b.subscribers[s] = true
				b.logger.Infof("[Blockchain] New Subscription received from %s (Total=%d).", s.source, len(b.subscribers))

			case s := <-b.deadSubscriptions:
				delete(b.subscribers, s)
				close(s.done)
				b.logger.Infof("[Blockchain] Subscription removed (Total=%d).", len(b.subscribers))
			}
		}
	}()

	// this will block
	if err := util.StartGRPCServer(ctx, b.logger, "blockchain", func(server *grpc.Server) {
		blockchain_api.RegisterBlockchainAPIServer(server, b)
	}); err != nil {
		return err
	}

	return nil
}

func (b *Blockchain) Stop(_ context.Context) error {
	b.cancelSubscriptions()

	return nil
}

func (b *Blockchain) Health(_ context.Context, _ *emptypb.Empty) (*blockchain_api.HealthResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		stats.NewStat("Health", true).AddTime(start)
	}()

	prometheusBlockchainHealth.Inc()

	return &blockchain_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (b *Blockchain) AddBlock(ctx context.Context, request *blockchain_api.AddBlockRequest) (*emptypb.Empty, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "AddBlock", stats)
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
		return nil, err
	}

	subtreeHashes := make([]*chainhash.Hash, len(request.SubtreeHashes))
	for i, subtreeHash := range request.SubtreeHashes {
		subtreeHashes[i], err = chainhash.NewHash(subtreeHash)
		if err != nil {
			return nil, err
		}
	}

	block := &model.Block{
		Header:           header,
		CoinbaseTx:       btCoinbaseTx,
		Subtrees:         subtreeHashes,
		TransactionCount: request.TransactionCount,
		SizeInBytes:      request.SizeInBytes,
	}

	_, err = b.store.StoreBlock(ctx1, block)
	if err != nil {
		return nil, err
	}

	// if !request.External {
	_, _ = b.SendNotification(ctx1, &blockchain_api.Notification{
		Type: model.NotificationType_MiningOn,
		Hash: block.Hash().CloneBytes(),
	})
	// }

	_, _ = b.SendNotification(ctx1, &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: block.Hash().CloneBytes(),
	})

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) GetBlock(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlock", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBlock.Inc()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	block, height, err := b.store.GetBlock(ctx1, blockHash)
	if err != nil {
		return nil, err
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

func (b *Blockchain) GetLastNBlocks(ctx context.Context, request *blockchain_api.GetLastNBlocksRequest) (*blockchain_api.GetLastNBlocksResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetLastNBlocks", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetLastNBlocks.Inc()

	blockInfo, err := b.store.GetLastNBlocks(ctx1, request.NumberOfBlocks, request.IncludeOrphans)
	if err != nil {
		return nil, err
	}

	return &blockchain_api.GetLastNBlocksResponse{
		Blocks: blockInfo,
	}, nil
}

func (b *Blockchain) GetBlockExists(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockExistsResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlockExists", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBlockExists.Inc()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	exists, err := b.store.GetBlockExists(ctx1, blockHash)
	if err != nil {
		return nil, err
	}

	return &blockchain_api.GetBlockExistsResponse{
		Exists: exists,
	}, nil
}

func (b *Blockchain) GetBestBlockHeader(ctx context.Context, empty *emptypb.Empty) (*blockchain_api.GetBlockHeaderResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBestBlockHeader", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBestBlockHeader.Inc()

	chainTip, meta, err := b.store.GetBestBlockHeader(ctx1)
	if err != nil {
		return nil, err
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
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlockHeader", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBlockHeader.Inc()

	hash, err := chainhash.NewHash(req.BlockHash)
	if err != nil {
		return nil, err
	}

	blockHeader, meta, err := b.store.GetBlockHeader(ctx1, hash)
	if err != nil {
		return nil, err
	}

	return &blockchain_api.GetBlockHeaderResponse{
		BlockHeader: blockHeader.Bytes(),
		Height:      meta.Height,
		TxCount:     meta.TxCount,
		SizeInBytes: meta.SizeInBytes,
		Miner:       meta.Miner,
	}, nil
}

func (b *Blockchain) GetBlockHeaders(ctx context.Context, req *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetBlockHeaders", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetBlockHeaders.Inc()

	startHash, err := chainhash.NewHash(req.StartHash)
	if err != nil {
		return nil, err
	}

	blockHeaders, heights, err := b.store.GetBlockHeaders(ctx1, startHash, req.NumberOfHeaders)
	if err != nil {
		return nil, err
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

func (b *Blockchain) Subscribe(req *blockchain_api.SubscribeRequest, sub blockchain_api.BlockchainAPI_SubscribeServer) error {
	start, stat, _ := util.NewStatFromContext(sub.Context(), "Subscribe", stats)
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
	start, stat, ctx1 := util.NewStatFromContext(ctx, "GetState", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainGetState.Inc()

	data, err := b.store.GetState(ctx1, req.Key)
	if err != nil {
		return nil, err
	}

	return &blockchain_api.StateResponse{
		Data: data,
	}, nil
}

func (b *Blockchain) SetState(ctx context.Context, req *blockchain_api.SetStateRequest) (*emptypb.Empty, error) {
	start, stat, ctx1 := util.NewStatFromContext(ctx, "SetState", stats)
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

func (b *Blockchain) SendNotification(_ context.Context, req *blockchain_api.Notification) (*emptypb.Empty, error) {
	start, stat, _ := util.NewStatFromContext(context.Background(), "SendNotification", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockchainSendNotification.Inc()

	b.notifications <- req

	return &emptypb.Empty{}, nil
}
