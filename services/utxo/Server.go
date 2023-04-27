package utxo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/store/memory"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/TAAL-GmbH/ubsv/tracing"
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
	prometheusUtxoGet   prometheus.Counter
	prometheusUtxoStore prometheus.Counter
	// prometheusUtxoReStore    prometheus.Counter
	// prometheusUtxoStoreSpent prometheus.Counter
	prometheusUtxoSpend prometheus.Counter
	// prometheusUtxoReSpend    prometheus.Counter
	// prometheusUtxoSpendSpent prometheus.Counter
	prometheusUtxoReset prometheus.Counter
)

func init() {
	prometheusUtxoGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_get",
			Help: "Number of utxo get calls done to utxostore",
		},
	)
	prometheusUtxoStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_store",
			Help: "Number of utxo store calls done to utxostore",
		},
	)
	//prometheusUtxoStoreSpent = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_store_spent",
	//		Help: "Number of utxo store calls that were already spent to utxostore",
	//	},
	//)
	//prometheusUtxoReStore = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_restore",
	//		Help: "Number of utxo restore calls done to utxostore",
	//	},
	//)
	prometheusUtxoSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_spend",
			Help: "Number of utxo spend calls done to utxostore",
		},
	)
	//prometheusUtxoReSpend = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_respend",
	//		Help: "Number of utxo respend calls done to utxostore",
	//	},
	//)
	//prometheusUtxoSpendSpent = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_spend_spent",
	//		Help: "Number of utxo spend calls that were already spent done to utxostore",
	//	},
	//)
	prometheusUtxoReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_reset",
			Help: "Number of utxo reset calls done to utxostore",
		},
	)
}

// UTXOStore type carries the logger within it
type UTXOStore struct {
	utxostore_api.UnsafeUtxoStoreAPIServer
	logger     utils.Logger
	grpcServer *grpc.Server
	store      store.UTXOStore
}

func Enabled() bool {
	_, found := gocore.Config().Get("utxostore_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, opts ...Options) (*UTXOStore, error) {
	utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("no utxostore setting found")
	}

	var s store.UTXOStore
	switch utxostoreURL.Path {
	case "/splitbyhash":
		logger.Infof("[UTXOStore] using splitbyhash memory store")
		s = memory.NewSplitByHash(true)
	case "/swiss":
		logger.Infof("[UTXOStore] using swissmap memory store")
		s = memory.NewSwissMap(true)
	case "/xsyncmap":
		logger.Infof("[UTXOStore] using xsyncmap memory store")
		s = memory.NewXSyncMap(true)
	default:
		logger.Infof("[UTXOStore] using default memory store")
		s = memory.New(true)
	}

	if len(opts) > 0 {
		for _, opt := range opts {
			opt(s)
		}
	}

	return &UTXOStore{
		logger: logger,
		store:  s,
	}, nil
}

// Start function
func (u *UTXOStore) Start() error {

	address, _, ok := gocore.Config().GetURL("utxostore")
	if !ok {
		return errors.New("no utxostore_grpcAddress setting found")
	}

	// // LEVEL 0 - no security / no encryption
	// var opts []grpc.ServerOption
	// _, prometheusOn := gocore.Config().Get("prometheusEndpoint")
	// if prometheusOn {
	// 	opts = append(opts,
	// 		grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor),
	// 		grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	// 	)
	// }

	// u.grpcServer = grpc.NewServer(tracing.AddGRPCServerOptions(opts)...)
	var err error
	u.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
	})
	if err != nil {
		return fmt.Errorf("Could not create GRPC server [%w]", err)
	}

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
	prometheusUtxoGet.Inc()

	return &utxostore_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *UTXOStore) Store(ctx context.Context, req *utxostore_api.StoreRequest) (*utxostore_api.StoreResponse, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Store")
	defer traceSpan.Finish()

	utxoHash, err := chainhash.NewHash(req.UxtoHash)
	if err != nil {
		return nil, err
	}

	resp, err := u.store.Store(traceSpan.Ctx, utxoHash)
	if err != nil {
		return nil, err
	}

	prometheusUtxoStore.Inc()

	return &utxostore_api.StoreResponse{
		Status: utxostore_api.Status(resp.Status),
	}, nil
}

func (u *UTXOStore) Spend(ctx context.Context, req *utxostore_api.SpendRequest) (*utxostore_api.SpendResponse, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Spend")
	defer traceSpan.Finish()

	utxoHash, err := chainhash.NewHash(req.UxtoHash)
	if err != nil {
		return nil, err
	}

	spendingHash, err := chainhash.NewHash(req.SpendingTxid)
	if err != nil {
		return nil, err
	}

	resp, err := u.store.Spend(traceSpan.Ctx, utxoHash, spendingHash)
	if err != nil {
		return nil, err
	}

	prometheusUtxoSpend.Inc()

	var spendingTxID []byte
	if resp.SpendingTxID != nil {
		spendingTxID = resp.SpendingTxID[:]
	}

	return &utxostore_api.SpendResponse{
		Status:       utxostore_api.Status(resp.Status),
		SpendingTxid: spendingTxID,
	}, nil
}

func (u *UTXOStore) Reset(ctx context.Context, req *utxostore_api.ResetRequest) (*utxostore_api.ResetResponse, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Reset")
	defer traceSpan.Finish()

	utxoHash, err := chainhash.NewHash(req.UxtoHash)
	if err != nil {
		return nil, err
	}

	resp, err := u.store.Reset(traceSpan.Ctx, utxoHash)
	if err != nil {
		return nil, err
	}

	prometheusUtxoReset.Inc()

	return &utxostore_api.ResetResponse{
		Status: utxostore_api.Status(resp.Status),
	}, nil
}
