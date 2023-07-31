package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	blockvalidation_api "github.com/TAAL-GmbH/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	txmeta_store "github.com/TAAL-GmbH/ubsv/stores/txmeta"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
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
	prometheusBlockValidationBlockFound prometheus.Counter
)

func init() {
	prometheusBlockValidationBlockFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockvalidation_block_found",
			Help: "Number of blocks found",
		},
	)
}

// BlockValidationServer type carries the logger within it
type BlockValidationServer struct {
	blockvalidation_api.UnimplementedBlockValidationAPIServer
	logger           utils.Logger
	grpcServer       *grpc.Server
	blockchainClient blockchain.ClientI
	utxoStore        utxostore.Interface
	subtreeStore     blob.Store
	txMetaStore      txmeta_store.Store

	blockFoundCh    chan *model.Block
	subtreeFoundCh  chan *util.Subtree
	blockValidation *BlockValidation
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockvalidation_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, utxoStore utxostore.Interface, subtreeStore blob.Store, txMetaStore txmeta_store.Store,
	validatorClient *validator.Client) (*BlockValidationServer, error) {

	blockchainClient, err := blockchain.NewClient()
	if err != nil {
		return nil, err
	}

	bVal := &BlockValidationServer{
		utxoStore:        utxoStore,
		logger:           logger,
		blockchainClient: blockchainClient,
		subtreeStore:     subtreeStore,
		txMetaStore:      txMetaStore,
		blockFoundCh:     make(chan *model.Block, 2),
		subtreeFoundCh:   make(chan *util.Subtree, 100),
		blockValidation:  NewBlockValidation(logger, blockchainClient, subtreeStore, txMetaStore, validatorClient),
	}

	return bVal, nil
}

// Start function
func (u *BlockValidationServer) Start() error {

	address, ok := gocore.Config().Get("blockvalidation_grpcAddress")
	if !ok {
		return errors.New("no blockvalidation_grpcAddress setting found")
	}

	var err error
	u.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
	})
	if err != nil {
		return fmt.Errorf("could not create GRPC server [%w]", err)
	}

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	blockvalidation_api.RegisterBlockValidationAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("BlockchainValidation GRPC service listening on %s", address)

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (u *BlockValidationServer) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()
}

func (u *BlockValidationServer) Health(_ context.Context, _ *emptypb.Empty) (*blockvalidation_api.HealthResponse, error) {
	return &blockvalidation_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *BlockValidationServer) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*emptypb.Empty, error) {
	prometheusBlockValidationBlockFound.Inc()

	hash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, err
	}

	blockBytes, err := u.blockValidation.doHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", req.GetBaseUrl(), hash.String()))
	if err != nil {
		return nil, fmt.Errorf("failed to get block from peer [%w]", err)
	}

	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create block from bytes [%w]", err)
	}

	// validate the block in the background
	go func() {
		err = u.blockValidation.BlockFound(context.Background(), block, req.GetBaseUrl())
		if err != nil {
			u.logger.Errorf("failed to process block [%s] [%v]", block.String(), err)
		}
	}()

	return &emptypb.Empty{}, nil
}

func (u *BlockValidationServer) SubtreeFound(ctx context.Context, req *blockvalidation_api.SubtreeFoundRequest) (*emptypb.Empty, error) {
	prometheusBlockValidationBlockFound.Inc()

	subtreeHash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, fmt.Errorf("failed to create subtree hash from bytes [%w]", err)
	}

	if req.GetBaseUrl() == "" {
		return nil, fmt.Errorf("base url is empty")
	}

	// validate the subtree in the background
	// TODO make sure we are not processing the same subtree twice at the same time
	go func() {
		ok := u.blockValidation.validateSubtree(context.Background(), subtreeHash, req.GetBaseUrl())
		if !ok {
			u.logger.Errorf("invalid subtree found [%s]", subtreeHash.String())
			return
		}
	}()

	return &emptypb.Empty{}, nil
}
