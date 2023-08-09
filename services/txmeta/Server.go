package txmeta

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/txmeta/store"
	"github.com/TAAL-GmbH/ubsv/services/txmeta/txmeta_api"
	"github.com/TAAL-GmbH/ubsv/stores/txmeta"
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
	prometheusTxMetaHealth   prometheus.Counter
	prometheusTxMetaSet      prometheus.Counter
	prometheusTxMetaSetMined prometheus.Counter
	prometheusTxMetaGet      prometheus.Counter
)

func init() {
	prometheusTxMetaHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txmeta_health",
			Help: "Number of calls done to txmeta health",
		},
	)
	prometheusTxMetaSet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txmeta_set",
			Help: "Number of calls done to txmeta set",
		},
	)
	prometheusTxMetaSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txmeta_set_mined",
			Help: "Number of calls done to txmeta set mined",
		},
	)
	prometheusTxMetaGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txmeta_get",
			Help: "Number of calls done to txmeta get",
		},
	)
}

// Server type carries the logger within it
type Server struct {
	txmeta_api.UnsafeTxMetaAPIServer
	logger     utils.Logger
	grpcServer *grpc.Server
	store      txmeta.Store
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, txMetaStoreURL *url.URL) (*Server, error) {
	s, err := store.New(logger, txMetaStoreURL)
	if err != nil {
		return nil, err
	}

	return &Server{
		logger: logger,
		store:  s,
	}, nil
}

func (u *Server) Init(ctx context.Context) error {
	return nil
}

// Start function
func (u *Server) Start(ctx context.Context) error {
	address, _, ok := gocore.Config().GetURL("txmeta_store")
	if !ok {
		return errors.New("no txmeta_store setting found")
	}

	var err error
	u.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
	})
	if err != nil {
		return fmt.Errorf("could not create GRPC server [%w]", err)
	}

	gocore.SetAddress(address.Host)

	lis, err := net.Listen("tcp", address.Host)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	txmeta_api.RegisterTxMetaAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("Tx Meta Store GRPC service listening on %s", address)

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (u *Server) Stop(ctx context.Context) error {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()

	return nil
}

func (u *Server) Health(_ context.Context, _ *emptypb.Empty) (*txmeta_api.HealthResponse, error) {
	prometheusTxMetaHealth.Inc()

	return &txmeta_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *Server) Create(ctx context.Context, request *txmeta_api.CreateRequest) (*txmeta_api.CreateResponse, error) {
	prometheusTxMetaSet.Inc()

	hash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	utxoHashes := make([]*chainhash.Hash, len(request.UtxoHashes))
	var utxoHash *chainhash.Hash
	for index, utxoHashBytes := range request.UtxoHashes {
		utxoHash, err = chainhash.NewHash(utxoHashBytes)
		if err != nil {
			return nil, err
		}
		utxoHashes[index] = utxoHash
	}

	parentTxHashes := make([]*chainhash.Hash, len(request.ParentTxHashes))
	var parentTxHash *chainhash.Hash
	for index, parentTxHashBytes := range request.ParentTxHashes {
		parentTxHash, err = chainhash.NewHash(parentTxHashBytes)
		if err != nil {
			return nil, err
		}
		parentTxHashes[index] = parentTxHash
	}

	err = u.store.Create(ctx, hash, request.Fee, parentTxHashes, utxoHashes, request.LockTime)
	if err != nil {
		return nil, err
	}

	return &txmeta_api.CreateResponse{
		Status: txmeta_api.Status_StatusUnconfirmed,
	}, nil
}

func (u *Server) SetMined(ctx context.Context, request *txmeta_api.SetMinedRequest) (*txmeta_api.SetMinedResponse, error) {
	prometheusTxMetaSetMined.Inc()

	hash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	blockHash, err := chainhash.NewHash(request.BlockHash)
	if err != nil {
		return nil, err
	}

	err = u.store.SetMined(ctx, hash, blockHash)
	if err != nil {
		return nil, err
	}

	return &txmeta_api.SetMinedResponse{
		Status: txmeta_api.Status_StatusConfirmed,
	}, nil
}

func (u *Server) Get(ctx context.Context, request *txmeta_api.GetRequest) (*txmeta_api.GetResponse, error) {
	prometheusTxMetaGet.Inc()

	hash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	tx, err := u.store.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	utxoHashes := make([][]byte, len(tx.UtxoHashes))
	for index, utxoHash := range tx.UtxoHashes {
		utxoHashes[index] = utxoHash.CloneBytes()
	}

	parentTxHashes := make([][]byte, len(tx.ParentTxHashes))
	for index, parentTxHash := range tx.ParentTxHashes {
		parentTxHashes[index] = parentTxHash.CloneBytes()
	}

	blockHashes := make([][]byte, len(tx.BlockHashes))
	for index, blockHash := range tx.BlockHashes {
		blockHashes[index] = blockHash.CloneBytes()
	}

	return &txmeta_api.GetResponse{
		Status:         txmeta_api.Status(tx.Status),
		Fee:            tx.Fee,
		ParentTxHashes: parentTxHashes,
		UtxoHashes:     utxoHashes,
		FirstSeen:      timestamppb.New(tx.FirstSeen),
		BlockHashes:    blockHashes,
		BlockHeight:    tx.BlockHeight,
	}, nil
}

func (u *Server) Delete(ctx context.Context, request *txmeta_api.DeleteRequest) (*txmeta_api.DeleteResponse, error) {
	//TODO implement me
	panic("implement me")
}
