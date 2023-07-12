package blockchain

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockchain/blockchain_api"
	blockchain_store "github.com/TAAL-GmbH/ubsv/stores/blockchain"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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
	done         chan struct{}
}

// Blockchain type carries the logger within it
type Blockchain struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	addBlockChan      chan *blockchain_api.AddBlockRequest
	store             blockchain_store.Store
	logger            utils.Logger
	grpcServer        *grpc.Server
	newSubscriptions  chan subscriber
	deadSubscriptions chan subscriber
	subscribers       map[subscriber]bool
	notifications     chan *blockchain_api.Notification
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

	return &Blockchain{
		store:             s,
		logger:            logger,
		addBlockChan:      make(chan *blockchain_api.AddBlockRequest, 10),
		newSubscriptions:  make(chan subscriber, 10),
		deadSubscriptions: make(chan subscriber, 10),
		subscribers:       make(map[subscriber]bool),
		notifications:     make(chan *blockchain_api.Notification, 100),
	}, nil
}

// Start function
func (b *Blockchain) Start() error {
	address, ok := gocore.Config().Get("blockchain_grpcAddress")
	if !ok {
		return errors.New("no blockchain_grpcAddress setting found")
	}

	var err error
	b.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
	})
	if err != nil {
		return fmt.Errorf("could not create GRPC server [%w]", err)
	}

	gocore.SetAddress(address)

	go func() {
		for {
			select {
			case notification := <-b.notifications:
				for sub := range b.subscribers {
					go func(s subscriber) {
						if err := s.subscription.Send(notification); err != nil {
							b.deadSubscriptions <- s
						}
					}(sub)
				}

			case s := <-b.newSubscriptions:
				b.subscribers[s] = true
				b.logger.Infof("New Subscription received (Total=%d).", len(b.subscribers))

			case s := <-b.deadSubscriptions:
				delete(b.subscribers, s)
				close(s.done)
				b.logger.Infof("Subscription removed (Total=%d).", len(b.subscribers))
			}
		}
	}()

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	blockchain_api.RegisterBlockchainAPIServer(b.grpcServer, b)

	// Register reflection service on gRPC server.
	reflection.Register(b.grpcServer)

	b.logger.Infof("Blockchain GRPC service listening on %s", address)

	if err = b.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (b *Blockchain) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	b.grpcServer.GracefulStop()
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

func (b *Blockchain) SubscribeBestBlockHeader(_ *emptypb.Empty, stream blockchain_api.BlockchainAPI_SubscribeBestBlockHeaderServer) error {
	// Start a ticker that executes each 10 seconds
	// TODO change this to an event when a new chaintip is added
	timer := time.NewTicker(10 * time.Second)

	var lastHeaderHashStr string
	for {
		select {
		// Exit on stream context done
		case <-stream.Context().Done():
			return nil
		case <-timer.C:
			header, height, err := b.store.GetBestBlockHeader(stream.Context())
			if err != nil {
				b.logger.Errorf("error getting chain tip: %s", err.Error())
				continue
			}
			currentHeaderHashStr := header.Hash().String()
			if currentHeaderHashStr == lastHeaderHashStr {
				continue
			}

			// Send the Hardware stats on the stream
			err = stream.Send(&blockchain_api.BestBlockHeaderResponse{
				BlockHeader: header.Bytes(),
				Height:      height,
			})
			if err != nil {
				b.logger.Errorf("error sending chain tip: %s", err.Error())
				continue
			}

			lastHeaderHashStr = header.Hash().String()
		}
	}
}

func (b *Blockchain) Subscribe(_ *emptypb.Empty, sub blockchain_api.BlockchainAPI_SubscribeServer) error {
	// Keep this subscription alive without endless loop - use a channel that blocks forever.
	ch := make(chan struct{})

	b.newSubscriptions <- subscriber{
		subscription: sub,
		done:         ch,
	}

	<-ch

	return nil
}

func (b *Blockchain) SendNotification(ctx context.Context, req *blockchain_api.Notification) (*emptypb.Empty, error) {
	b.notifications <- req

	return &emptypb.Empty{}, nil
}
