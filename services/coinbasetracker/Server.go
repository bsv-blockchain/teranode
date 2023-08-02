package coinbasetracker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	coinbasetracker_api "github.com/TAAL-GmbH/ubsv/services/coinbasetracker/coinbasetracker_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"

	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CoinbaseTrackerServer type carries the logger within it
type CoinbaseTrackerServer struct {
	coinbasetracker_api.UnimplementedCoinbasetrackerAPIServer
	logger           utils.Logger
	grpcServer       *grpc.Server
	blockchainClient blockchain.ClientI
	coinbaseTracker  *CoinbaseTracker
}

func Enabled() bool {
	_, found := gocore.Config().Get("coinbasetracker_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger) (*CoinbaseTrackerServer, error) {

	blockchainClient, err := blockchain.NewClient()
	if err != nil {
		return nil, err
	}

	con := &CoinbaseTrackerServer{
		logger:           logger,
		blockchainClient: blockchainClient,
		coinbaseTracker:  NewCoinbaseTracker(logger, blockchainClient),
	}

	return con, nil
}

// Start function
func (u *CoinbaseTrackerServer) Start() error {

	address, ok := gocore.Config().Get("coinbasetracker_grpcAddress")
	if !ok {
		return errors.New("no coinbasetracker_grpcAddress setting found")
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

	coinbasetracker_api.RegisterCoinbasetrackerAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("coinbaseTracker GRPC service listening on %s", address)

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	// get best block from node
	// get best block from db
	// if different fill in the gaps
	// subscribe to new blocks through the blob server

	return nil
}

func (u *CoinbaseTrackerServer) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()
}

func (u *CoinbaseTrackerServer) Health(_ context.Context, _ *emptypb.Empty) (*coinbasetracker_api.HealthResponse, error) {
	return &coinbasetracker_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *CoinbaseTrackerServer) GetUtxoxs(ctx context.Context, req *coinbasetracker_api.GetUtxoRequest) (*coinbasetracker_api.GetUtxoResponse, error) {
	pubKeyHash, err := chainhash.NewHash(req.Publickey)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubkey hash hash from bytes [%w]", err)
	}

	utxos, err := u.coinbaseTracker.GetUtxos(ctx, pubKeyHash, req.Amount)
	if err != nil {
		return nil, err
	}

	respUtxos := make([]*coinbasetracker_api.Utxo, len(utxos))
	for i, utxo := range utxos {
		respUtxos[i] = &coinbasetracker_api.Utxo{
			TxId:     utxo.TxID,
			Vout:     utxo.Vout,
			Script:   *utxo.LockingScript,
			Satoshis: utxo.Satoshis,
		}

	}
	resp := &coinbasetracker_api.GetUtxoResponse{}
	resp.Utxos = respUtxos
	return resp, nil
}

func (u *CoinbaseTrackerServer) SubmitTransaction(ctx context.Context, req *coinbasetracker_api.SubmitTransactionRequest) (*emptypb.Empty, error) {

	err := u.coinbaseTracker.SubmitTransaction(ctx, req.Tx)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
