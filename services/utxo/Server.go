package utxo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/TAAL-GmbH/ubsv/tracing"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	empty = &chainhash.Hash{}
)

// UTXOStore type carries the logger within it
type UTXOStore struct {
	utxostore_api.UnsafeUtxoStoreAPIServer
	logger     utils.Logger
	grpcServer *grpc.Server
	mu         sync.Mutex
	store      map[chainhash.Hash]chainhash.Hash
}

func Enabled() bool {
	_, found := gocore.Config().Get("utxostore_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger) (*UTXOStore, error) {
	return &UTXOStore{
		logger: logger,
		store:  make(map[chainhash.Hash]chainhash.Hash),
	}, nil
}

// Start function
func (u *UTXOStore) Start() error {

	address, _, ok := gocore.Config().GetURL("utxostore")
	if !ok {
		return errors.New("no utxostore_grpcAddress setting found")
	}

	// LEVEL 0 - no security / no encryption
	var opts []grpc.ServerOption
	_, prometheusOn := gocore.Config().Get("prometheusEndpoint")
	if prometheusOn {
		opts = append(opts,
			grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor),
			grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		)
	}

	u.grpcServer = grpc.NewServer(tracing.AddGRPCServerOptions(opts)...)

	gocore.SetAddress(address.Host)

	lis, err := net.Listen("tcp", address.Host)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	utxostore_api.RegisterUtxoStoreAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("GRPC server listening on %s", address)

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (u *UTXOStore) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()
}

func (u *UTXOStore) Health(_ context.Context, _ *emptypb.Empty) (*utxostore_api.HealthResponse, error) {
	return &utxostore_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *UTXOStore) Store(_ context.Context, req *utxostore_api.StoreRequest) (*utxostore_api.StoreResponse, error) {
	utxoHash, err := chainhash.NewHash(req.UxtoHash)
	if err != nil {
		return nil, err
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	spendingTxid, found := u.store[*utxoHash]
	if found {
		if spendingTxid.IsEqual(empty) {
			return &utxostore_api.StoreResponse{
				Status: utxostore_api.Status_OK,
			}, nil
		}

		return &utxostore_api.StoreResponse{
			Status: utxostore_api.Status_SPENT,
		}, nil

	}

	u.store[*utxoHash] = *empty

	return &utxostore_api.StoreResponse{
		Status: utxostore_api.Status_OK,
	}, nil
}

func (u *UTXOStore) Spend(_ context.Context, req *utxostore_api.SpendRequest) (*utxostore_api.SpendResponse, error) {
	utxoHash, err := chainhash.NewHash(req.UxtoHash)
	if err != nil {
		return nil, err
	}

	spendingHash, err := chainhash.NewHash(req.SpendingTxid)
	if err != nil {
		return nil, err
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	existingHash, found := u.store[*utxoHash]
	if found {
		if existingHash.IsEqual(empty) {
			u.store[*utxoHash] = *spendingHash

			return &utxostore_api.SpendResponse{
				Status: utxostore_api.Status_OK,
			}, nil
		}

		if existingHash.IsEqual(spendingHash) {
			return &utxostore_api.SpendResponse{
				Status: utxostore_api.Status_OK,
			}, nil
		}
	}
	return &utxostore_api.SpendResponse{
		Status:       utxostore_api.Status_SPENT,
		SpendingTxid: existingHash.CloneBytes(),
	}, nil
}

func (u *UTXOStore) Reset(_ context.Context, req *utxostore_api.ResetRequest) (*utxostore_api.ResetResponse, error) {
	utxoHash, err := chainhash.NewHash(req.UxtoHash)
	if err != nil {
		return nil, err
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	spendingHash, found := u.store[*utxoHash]
	if found {
		if !spendingHash.IsEqual(empty) {
			u.store[*utxoHash] = *empty
		}

		return &utxostore_api.ResetResponse{
			Status: utxostore_api.Status_OK,
		}, nil
	}

	return &utxostore_api.ResetResponse{
		Status: utxostore_api.Status_NOT_FOUND,
	}, nil
}
