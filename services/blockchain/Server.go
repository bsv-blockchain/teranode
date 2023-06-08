package blockchain

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/blockchain/blockchain_api"
	"github.com/TAAL-GmbH/ubsv/stores/blockchain"
	"github.com/libsv/go-bc"
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

// Blockchain type carries the logger within it
type Blockchain struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	addBlockChan chan *blockchain_api.AddBlockRequest
	store        blockchain.Store
	logger       utils.Logger
	grpcServer   *grpc.Server
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

	s, err := blockchain.NewStore(logger, blockchainStoreURL)
	if err != nil {
		return nil, err
	}

	return &Blockchain{
		store:        s,
		logger:       logger,
		addBlockChan: make(chan *blockchain_api.AddBlockRequest, 10),
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

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	blockchain_api.RegisterBlockchainAPIServer(b.grpcServer, b)

	// Register reflection service on gRPC server.
	reflection.Register(b.grpcServer)

	b.logger.Infof("GRPC server listening on %s", address)

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

func (b *Blockchain) AddBlock(ctx context.Context, request *blockchain_api.AddBlockRequest) (*blockchain_api.AddBlockResponse, error) {
	block, err := bc.NewBlockFromBytes(request.Block)
	if err != nil {
		return nil, err
	}

	err = b.store.StoreBlock(ctx, block)
	if err != nil {
		return nil, err
	}

	prometheusBlockchainAddBlock.Inc()

	return &blockchain_api.AddBlockResponse{
		Ok: true,
	}, nil
}

func (b *Blockchain) GetBlock(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockResponse, error) {
	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	block, err := b.store.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	return &blockchain_api.GetBlockResponse{
		Block: block.Bytes(),
	}, nil
}

func (b *Blockchain) ChainTip(ctx context.Context, empty *emptypb.Empty) (*blockchain_api.ChainTipResponse, error) {
	chainTip, height, err := b.store.GetChainTip(ctx)
	if err != nil {
		return nil, err
	}

	return &blockchain_api.ChainTipResponse{
		BlockHeader: chainTip.Bytes(),
		Height:      height,
	}, nil
}
