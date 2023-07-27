package seeder

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	seeder_api "github.com/TAAL-GmbH/ubsv/services/seeder/seeder_api"
	"github.com/TAAL-GmbH/ubsv/services/seeder/store"
	"github.com/TAAL-GmbH/ubsv/services/seeder/store/memory"
	"github.com/TAAL-GmbH/ubsv/services/validator/utxo"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server type carries the logger within it
type Server struct {
	seeder_api.UnimplementedSeederAPIServer
	seederStore store.SeederStore
	utxoStore   utxostore.Interface
	logger      utils.Logger
	grpcServer  *grpc.Server
}

var (
	prometheusSeederSuccessfulOps *prometheus.CounterVec
	prometheusSeederErrors        *prometheus.CounterVec
)

func init() {

	prometheusSeederSuccessfulOps = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "seeder_successful_ops",
			Help: "Number of successful seeder ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)
	prometheusSeederErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "seeder_errors",
			Help: "Number of seeder errors",
		},
		[]string{
			"function", //function raising the error
			"error",    // error returned
		},
	)
}

func Enabled() bool {
	_, found := gocore.Config().Get("seeder_grpcAddress")
	return found
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger utils.Logger) *Server {

	seederStore := memory.NewMemorySeederStore()

	utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no utxostore setting found")
	}

	// TODO online it seems the Seeder keeps connecting to aerospike
	s, err := utxo.NewStore(logger, utxostoreURL)
	if err != nil {
		panic(err)
	}

	return &Server{
		logger:      logger,
		seederStore: seederStore,
		utxoStore:   s,
	}
}

// Start function
func (v *Server) Start() error {

	address, ok := gocore.Config().Get("seeder_grpcAddress")
	if !ok {
		return errors.New("no seeder_grpcAddress setting found")
	}

	var err error
	v.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
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

	seeder_api.RegisterSeederAPIServer(v.grpcServer, v)

	// Register reflection service on gRPC server.
	reflection.Register(v.grpcServer)

	v.logger.Infof("Seeder GRPC service listening on %s", address)

	if err = v.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (v *Server) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	v.grpcServer.GracefulStop()
}

func (v *Server) Health(_ context.Context, _ *emptypb.Empty) (*seeder_api.HealthResponse, error) {
	return &seeder_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (v *Server) CreateSpendableTransactions(ctx context.Context, req *seeder_api.CreateSpendableTransactionsRequest) (*emptypb.Empty, error) {
	var privateKey *bec.PrivateKey

	if len(req.PrivateKey) > 0 {
		privateKey, _ = bec.PrivKeyFromBytes(bec.S256(), req.PrivateKey)
	} else {
		// Create a random private key
		var err error
		privateKey, err = bec.NewPrivateKey(bec.S256())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	for i := uint32(0); i < req.NumberOfTransactions; i++ {
		// Create a random 32 byte array
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			prometheusSeederErrors.WithLabelValues("CreateSpendableTransactions", "ByteArrayCreation").Inc()
			return nil, status.Error(codes.Internal, err.Error())
		}

		txid, err := chainhash.NewHash(b)
		if err != nil {
			prometheusSeederErrors.WithLabelValues("CreateSpendableTransactions", "NewHash").Inc()
			return nil, status.Error(codes.Internal, err.Error())
		}

		lockingScript, err := bscript.NewP2PKHFromPubKeyBytes(privateKey.PubKey().SerialiseCompressed())
		if err != nil {
			prometheusSeederErrors.WithLabelValues("CreateSpendableTransactions", "NewP2PKHFromPubKeyBytes").Inc()
			return nil, status.Error(codes.Internal, err.Error())
		}

		for j := uint32(0); j < req.NumberOfOutputs; j++ {
			hash, err := util.UTXOHash(txid, j, *lockingScript, req.SatoshisPerOutput)
			if err != nil {
				prometheusSeederErrors.WithLabelValues("CreateSpendableTransactions", "UTXOHash").Inc()
				return nil, status.Error(codes.Internal, err.Error())
			}

			ctx := context.Background()
			g, ctx := errgroup.WithContext(ctx)

			g.Go(func() error {
				if _, err := v.utxoStore.Store(ctx, hash, 0); err != nil {
					v.logger.Fatalf("Error occurred in seeder: %v\n%s\n", err, debug.Stack())
					prometheusSeederErrors.WithLabelValues("CreateSpendableTransactions", "Store").Inc()
					return status.Error(codes.Internal, err.Error())
				}
				prometheusSeederSuccessfulOps.WithLabelValues("CreateSpendableTransactions", "Store").Inc()
				return nil
			})

			if err := g.Wait(); err != nil {
				prometheusSeederErrors.WithLabelValues("CreateSpendableTransactions", "StoreRoutine").Inc()
				return nil, err
			}
		}

		if err := v.seederStore.Push(context.Background(), &store.SpendableTransaction{
			Txid:              txid,
			NumberOfOutputs:   int32(req.NumberOfOutputs),
			SatoshisPerOutput: int64(req.SatoshisPerOutput),
			PrivateKey:        privateKey,
		}); err != nil {
			prometheusSeederErrors.WithLabelValues("CreateSpendableTransactions", "PushingTx").Inc()
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	prometheusSeederSuccessfulOps.WithLabelValues("CreateSpendableTransactions", "SeedingComplete").Inc()
	return &emptypb.Empty{}, nil
}

func (v *Server) NextSpendableTransaction(ctx context.Context, req *seeder_api.NextSpendableTransactionRequest) (*seeder_api.NextSpendableTransactionResponse, error) {

	fn := func(*store.SpendableTransaction) bool {
		return true
	}

	if len(req.PrivateKey) > 0 {
		fn = func(tx *store.SpendableTransaction) bool {
			return bytes.Equal(tx.PrivateKey.Serialise(), req.PrivateKey)
		}
	}

	tx, err := v.seederStore.PopWithFilter(ctx, fn)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &seeder_api.NextSpendableTransactionResponse{
		Txid:              tx.Txid.CloneBytes(),
		NumberOfOutputs:   uint32(tx.NumberOfOutputs),
		SatoshisPerOutput: uint64(tx.SatoshisPerOutput),
		PrivateKey:        tx.PrivateKey.Serialise(),
	}, nil
}

func (v *Server) ShowAllSpendableTransactions(_ *emptypb.Empty, stream seeder_api.SeederAPI_ShowAllSpendableTransactionsServer) error {
	it := v.seederStore.Iterator()

	for {
		tx, err := it.Next()
		if err != nil {
			if err.Error() == "no more transactions" {
				v.logger.Infof("Seeding finished: No more transactions")
				break
			} else {
				return err
			}
		}

		if err := stream.Send(&seeder_api.NextSpendableTransactionResponse{
			Txid:              tx.Txid.CloneBytes(),
			NumberOfOutputs:   uint32(tx.NumberOfOutputs),
			SatoshisPerOutput: uint64(tx.SatoshisPerOutput),
			PrivateKey:        tx.PrivateKey.Serialise(),
		}); err != nil {
			prometheusSeederErrors.WithLabelValues("ShowAllSpendableTransactions", "Send").Inc()
			return err
		}

	}
	prometheusSeederSuccessfulOps.WithLabelValues("ShowAllSpendableTransactions", "Send").Inc()
	return nil
}
