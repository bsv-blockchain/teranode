package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	blockvalidation_api "github.com/TAAL-GmbH/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	txmeta_store "github.com/TAAL-GmbH/ubsv/stores/txmeta"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
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
	prometheusBlockValidationBlockFound   prometheus.Counter
	prometheusBlockValidationSubtreeFound prometheus.Counter
)

func init() {
	prometheusBlockValidationBlockFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockvalidation_block_found",
			Help: "Number of blocks found",
		},
	)
	prometheusBlockValidationSubtreeFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockvalidation_subtree_found",
			Help: "Number of subtrees found",
		},
	)
}

type processBlockFound struct {
	hash    *chainhash.Hash
	baseURL string
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
	validatorClient  *validator.Client

	blockFoundCh        chan processBlockFound
	blockValidation     *BlockValidation
	processingSubtreeMu sync.Mutex
	processingSubtree   map[chainhash.Hash]bool
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockvalidation_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, utxoStore utxostore.Interface, subtreeStore blob.Store, txMetaStore txmeta_store.Store,
	validatorClient *validator.Client) *BlockValidationServer {

	bVal := &BlockValidationServer{
		utxoStore:         utxoStore,
		logger:            logger,
		subtreeStore:      subtreeStore,
		txMetaStore:       txMetaStore,
		validatorClient:   validatorClient,
		blockFoundCh:      make(chan processBlockFound, 100),
		processingSubtree: make(map[chainhash.Hash]bool),
	}

	return bVal
}

func (u *BlockValidationServer) Init(ctx context.Context) (err error) {
	if u.blockchainClient, err = blockchain.NewClient(); err != nil {
		return fmt.Errorf("failed to create blockchain client [%w]", err)
	}

	u.blockValidation = NewBlockValidation(u.logger, u.blockchainClient, u.subtreeStore, u.txMetaStore, u.validatorClient)

	// process blocks found from channel
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case b := <-u.blockFoundCh:
				{
					if err = u.processBlockFound(ctx, b.hash, b.baseURL); err != nil {
						u.logger.Errorf("failed to process block [%s] [%v]", b.hash.String(), err)
					}
				}
			}
		}
	}()

	return nil
}

// Start function
func (u *BlockValidationServer) Start(ctx context.Context) error {

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

func (u *BlockValidationServer) Stop(ctx context.Context) error {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()

	return nil
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

	// first check if the block exists, it is very expensive to do all the checks below
	exists, err := u.blockchainClient.GetBlockExists(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to check if block exists [%w]", err)
	}

	if exists {
		u.logger.Warnf("block found that already exists [%s]", hash.String())
		return &emptypb.Empty{}, nil
	}

	// process the block in the background, in the order we receive them, but without blocking the grpc call
	go func() {
		u.blockFoundCh <- processBlockFound{
			hash:    hash,
			baseURL: req.GetBaseUrl(),
		}
	}()

	return &emptypb.Empty{}, nil
}

func (u *BlockValidationServer) processBlockFound(ctx context.Context, hash *chainhash.Hash, baseUrl string) error {
	u.logger.Infof("processing block found [%s]", hash.String())

	// first check if the block exists, it might have already been processed
	exists, err := u.blockchainClient.GetBlockExists(ctx, hash)
	if err != nil {
		return fmt.Errorf("failed to check if block exists [%w]", err)
	}
	if exists {
		u.logger.Warnf("not processing block that already was found [%s]", hash.String())
		return nil
	}

	blockBytes, err := u.blockValidation.doHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", baseUrl, hash.String()))
	if err != nil {
		return fmt.Errorf("failed to get block %s from peer [%w]", hash.String(), err)
	}

	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return fmt.Errorf("failed to create block %s from bytes [%w]", hash.String(), err)
	}

	if block == nil {
		return fmt.Errorf("block could not be created from bytes: %v", blockBytes)
	}

	// catchup if we are missing the parent block
	parentExists, err := u.blockchainClient.GetBlockExists(ctx, block.Header.HashPrevBlock)
	if err != nil {
		return fmt.Errorf("failed to check if parent block %s exists [%w]", block.Header.HashPrevBlock.String(), err)
	}

	if !parentExists {
		u.logger.Infof("parent block %s does not exist, processing it first", block.Header.HashPrevBlock.String())
		err = u.processBlockFound(ctx, block.Header.HashPrevBlock, baseUrl)
		if err != nil {
			return fmt.Errorf("failed to process parent block %s [%w]", block.Header.HashPrevBlock.String(), err)
		}
	}

	// validate the block
	err = u.blockValidation.BlockFound(context.Background(), block, baseUrl)
	if err != nil {
		u.logger.Errorf("failed block validation BlockFound [%s] [%v]", block.String(), err)
	}

	return nil
}

func (u *BlockValidationServer) SubtreeFound(ctx context.Context, req *blockvalidation_api.SubtreeFoundRequest) (*emptypb.Empty, error) {
	prometheusBlockValidationSubtreeFound.Inc()
	u.logger.Infof("processing subtree found [%s]", utils.ReverseAndHexEncodeSlice(req.Hash))

	subtreeHash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, fmt.Errorf("failed to create subtree hash from bytes [%w]", err)
	}

	exists, err := u.subtreeStore.Exists(ctx, subtreeHash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to check if subtree exists [%w]", err)
	}

	if exists {
		u.logger.Warnf("subtree found that already exists [%s]", subtreeHash.String())
		return &emptypb.Empty{}, nil
	}

	if req.GetBaseUrl() == "" {
		return nil, fmt.Errorf("base url is empty")
	}

	// validate the subtree in the background
	go func() {
		// check if we are already processing this subtree
		u.processingSubtreeMu.Lock()
		processing, ok := u.processingSubtree[*subtreeHash]
		if ok && processing {
			u.processingSubtreeMu.Unlock()
			return
		}

		// add to processing map
		u.processingSubtree[*subtreeHash] = true
		u.processingSubtreeMu.Unlock()

		// remove from processing map when done
		defer func() {
			u.processingSubtreeMu.Lock()
			delete(u.processingSubtree, *subtreeHash)
			u.processingSubtreeMu.Unlock()
		}()

		ok = u.blockValidation.validateSubtree(context.Background(), subtreeHash, req.GetBaseUrl())
		if !ok {
			u.logger.Errorf("invalid subtree found [%s]", subtreeHash.String())
			return
		}
	}()

	return &emptypb.Empty{}, nil
}
