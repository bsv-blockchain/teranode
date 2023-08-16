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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	prometheusBlockchainAddBlock prometheus.Counter
)

func init() {
	prometheusBlockchainAddBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockchain_add_block",
			Help: "Number of blocks added to the blockchain service",
		},
	)
}

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
	_, found := gocore.Config().Get("blockchain_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger) (*Blockchain, error) {
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
				b.logger.Debugf("[Blockchain] Sending notification: %s", notification.Stringify())
				for sub := range b.subscribers {
					go func(s subscriber) {
						//b.logger.Debugf("[Blockchain] Sending notification to %s: %s", s.source, notification.Stringify())
						if err := s.subscription.Send(notification); err != nil {
							b.deadSubscriptions <- s
						}
					}(sub)
				}

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
	return &blockchain_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (b *Blockchain) AddBlock(ctx context.Context, request *blockchain_api.AddBlockRequest) (*emptypb.Empty, error) {
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
	}

	err = b.store.StoreBlock(ctx, block)
	if err != nil {
		return nil, err
	}

	prometheusBlockchainAddBlock.Inc()

	_, _ = b.SendNotification(ctx, &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: block.Hash().CloneBytes(),
	})

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) GetBlock(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockResponse, error) {
	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	block, height, err := b.store.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	subtreeHashes := make([][]byte, len(block.Subtrees))
	for i, subtreeHash := range block.Subtrees {
		subtreeHashes[i] = subtreeHash[:]
	}

	return &blockchain_api.GetBlockResponse{
		Header:        block.Header.Bytes(),
		Height:        height,
		CoinbaseTx:    block.CoinbaseTx.Bytes(),
		SubtreeHashes: subtreeHashes,
	}, nil
}

func (b *Blockchain) GetBlockExists(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockExistsResponse, error) {
	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	exists, err := b.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	return &blockchain_api.GetBlockExistsResponse{
		Exists: exists,
	}, nil
}

func (b *Blockchain) GetBestBlockHeader(ctx context.Context, empty *emptypb.Empty) (*blockchain_api.BestBlockHeaderResponse, error) {
	chainTip, height, err := b.store.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	return &blockchain_api.BestBlockHeaderResponse{
		BlockHeader: chainTip.Bytes(),
		Height:      height,
	}, nil
}

func (b *Blockchain) GetBlockHeaders(ctx context.Context, req *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	startHash, err := chainhash.NewHash(req.StartHash)
	if err != nil {
		return nil, err
	}

	blockHeaders, err := b.store.GetBlockHeaders(ctx, startHash, req.NumberOfHeaders)
	if err != nil {
		return nil, err
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	return &blockchain_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
	}, nil
}

func (b *Blockchain) Subscribe(req *blockchain_api.SubscribeRequest, sub blockchain_api.BlockchainAPI_SubscribeServer) error {
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
	data, err := b.store.GetState(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	return &blockchain_api.StateResponse{
		Data: data,
	}, nil
}

func (b *Blockchain) SetState(ctx context.Context, req *blockchain_api.SetStateRequest) (*emptypb.Empty, error) {
	err := b.store.SetState(ctx, req.Key, req.Data)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) SendNotification(ctx context.Context, req *blockchain_api.Notification) (*emptypb.Empty, error) {
	b.notifications <- req

	return &emptypb.Empty{}, nil
}
