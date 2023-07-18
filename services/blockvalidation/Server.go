package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blobserver/blobserver_api"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	blockvalidation_api "github.com/TAAL-GmbH/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	txmeta_store "github.com/TAAL-GmbH/ubsv/stores/txmeta"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
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

	// todo: this should be done in a peer manager type thing
	go func() {
		// subscribe to all peers
		peersList, ok := gocore.Config().Get("blockvalidation_peers")
		if ok {
			peers := strings.Split(peersList, ",")
			for _, peer := range peers {
				grpcConn, err := utils.GetGRPCClient(context.Background(), peer, &utils.ConnectionOptions{
					OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
					Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
					MaxRetries:  3,
				})
				if err != nil {
					logger.Errorf("could not connect to peer [%s] [%v]", peer, err)
					continue
				}
				api := blobserver_api.NewBlobServerAPIClient(grpcConn)
				client, err := api.Subscribe(context.Background(), &emptypb.Empty{})
				if err != nil {
					logger.Errorf("could not subscribe to peer [%s] [%v]", peer, err)
					continue
				}

				var msg interface{}
				for {
					err = client.RecvMsg(&msg)
					if err != nil {
						logger.Errorf("could not receive message from peer [%s] [%v]", peer, err)
						break
					}

					notification, ok := msg.(*blobserver_api.Notification)
					if !ok {
						logger.Errorf("received message from peer [%s] that was not a notification", peer)
						continue
					}

					switch notification.GetType() {
					case blobserver_api.Type_Block:
						// get block over http from baseUrl
						blockResponse, err := api.GetBlock(context.Background(), &blobserver_api.HashOrHeight{
							Value: &blobserver_api.HashOrHeight_Hash{
								Hash: notification.GetHash(),
							},
						})
						if err != nil {
							logger.Errorf("could not get block from peer [%s] [%v]", peer, err)
							continue
						}
						block, err := model.NewBlockFromBytes(blockResponse.Blob)
						if err != nil {
							logger.Errorf("could not get block from peer [%s] [%v]", peer, err)
							continue
						}
						err = bVal.blockValidation.BlockFound(context.Background(), block, notification.BaseUrl)
						if err != nil {
							logger.Errorf("could not process block from peer [%s] [%v]", peer, err)
							continue
						}
					case blobserver_api.Type_Subtree:
						hash := chainhash.Hash(notification.GetHash())
						ok = bVal.blockValidation.validateSubtree(context.Background(), &hash, notification.BaseUrl)
						if !ok {
							logger.Errorf("could not validate subtree from peer [%s]", peer)
							continue
						}
					}
				}
			}
		}
	}()

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

	blockHeader, err := model.NewBlockHeaderFromBytes(req.BlockHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to create block header from bytes [%w]", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(req.Coinbase)
	if err != nil {
		return nil, fmt.Errorf("failed to create coinbase tx from bytes [%w]", err)
	}

	subtrees := make([]*chainhash.Hash, len(req.SubtreeHashes))
	for i, subtree := range req.SubtreeHashes {
		subtreeHash, err := chainhash.NewHash(subtree)
		if err != nil {
			return nil, fmt.Errorf("failed to create subtree hash from bytes [%w]", err)
		}
		subtrees[i] = subtreeHash
	}

	block := &model.Block{
		Header:     blockHeader,
		CoinbaseTx: coinbaseTx,
		Subtrees:   subtrees,
	}

	return nil, u.blockValidation.BlockFound(ctx, block, "")
}
