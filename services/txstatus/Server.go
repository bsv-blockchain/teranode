package txstatus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/txstatus/txstatus_api"
	"github.com/TAAL-GmbH/ubsv/stores/txstatus"
	"github.com/TAAL-GmbH/ubsv/stores/txstatus/memory"
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
	prometheusTxStatusHealth   prometheus.Counter
	prometheusTxStatusSet      prometheus.Counter
	prometheusTxStatusSetMined prometheus.Counter
	prometheusTxStatusGet      prometheus.Counter
)

func init() {
	prometheusTxStatusHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txstatus_health",
			Help: "Number of calls done to txstatus health",
		},
	)
	prometheusTxStatusSet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txstatus_set",
			Help: "Number of calls done to txstatus set",
		},
	)
	prometheusTxStatusSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txstatus_set_mined",
			Help: "Number of calls done to txstatus set mined",
		},
	)
	prometheusTxStatusGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "txstatus_get",
			Help: "Number of calls done to txstatus get",
		},
	)
}

// Server type carries the logger within it
type Server struct {
	txstatus_api.UnsafeTxStatusAPIServer
	logger     utils.Logger
	grpcServer *grpc.Server
	store      txstatus.Store
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, txStatusStoreURL *url.URL) (*Server, error) {

	// Only memory store has been implemented...

	var s txstatus.Store
	switch txStatusStoreURL.Path {
	case "/splitbyhash":
		logger.Infof("[TxStatusStore] using splitbyhash memory store")
		//s = memory.NewSplitByHash(true)
	default:
		logger.Infof("[TxStatusStore] using default memory store")
		s = memory.New()
	}

	return &Server{
		logger: logger,
		store:  s,
	}, nil
}

// Start function
func (u *Server) Start() error {
	address, _, ok := gocore.Config().GetURL("txstatus_store")
	if !ok {
		return errors.New("no txstatus_store setting found")
	}

	var err error
	u.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
	})
	if err != nil {
		return fmt.Errorf("could not create GRPC server [%w]", err)
	}

	gocore.SetAddress(address.Host)

	lis, err := net.Listen("tcp", address.Host)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	txstatus_api.RegisterTxStatusAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("GRPC server listening on %s", address)

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (u *Server) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()
}

func (u *Server) Health(_ context.Context, _ *emptypb.Empty) (*txstatus_api.HealthResponse, error) {
	prometheusTxStatusHealth.Inc()

	return &txstatus_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *Server) Set(ctx context.Context, request *txstatus_api.SetRequest) (*txstatus_api.SetResponse, error) {
	prometheusTxStatusSet.Inc()

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

	err = u.store.Create(ctx, hash, request.Fee, parentTxHashes, utxoHashes)
	if err != nil {
		return nil, err
	}

	return &txstatus_api.SetResponse{
		Status: txstatus_api.Status_StatusUnconfirmed,
	}, nil
}

func (u *Server) SetMined(ctx context.Context, request *txstatus_api.SetMinedRequest) (*txstatus_api.SetMinedResponse, error) {
	prometheusTxStatusSetMined.Inc()

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

	return &txstatus_api.SetMinedResponse{
		Status: txstatus_api.Status_StatusConfirmed,
	}, nil
}

func (u *Server) Get(ctx context.Context, request *txstatus_api.GetRequest) (*txstatus_api.GetResponse, error) {
	prometheusTxStatusGet.Inc()

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

	return &txstatus_api.GetResponse{
		Status:         txstatus_api.Status(tx.Status),
		Fee:            tx.Fee,
		ParentTxHashes: parentTxHashes,
		UtxoHashes:     utxoHashes,
		FirstSeen:      timestamppb.New(tx.FirstSeen),
		BlockHashes:    blockHashes,
		BlockHeight:    tx.BlockHeight,
	}, nil
}

func (u *Server) Delete(ctx context.Context, hash *chainhash.Hash) error {
	//TODO implement me
	panic("implement me")
}
