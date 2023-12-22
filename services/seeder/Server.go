package seeder

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	seeder_api "github.com/bitcoin-sv/ubsv/services/seeder/seeder_api"
	"github.com/bitcoin-sv/ubsv/services/seeder/store"
	"github.com/bitcoin-sv/ubsv/services/seeder/store/memory"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxostore_factory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server type carries the logger within it
type Server struct {
	seeder_api.UnimplementedSeederAPIServer
	seederStore store.SeederStore
	utxoStore   utxostore.Interface
	logger      ulogger.Logger
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
	_, found := gocore.Config().Get("seeder_grpcListenAddress")
	return found
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger ulogger.Logger) *Server {
	seederStore := memory.NewMemorySeederStore()
	return &Server{
		logger:      logger,
		seederStore: seederStore,
	}
}

func (v *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (v *Server) Init(ctx context.Context) error {
	utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		return fmt.Errorf("could not get utxostore setting [%w]", err)
	}
	if !found {
		return fmt.Errorf("no utxostore setting found")
	}

	// TODO online it seems the Seeder keeps connecting to aerospike
	v.utxoStore, err = utxostore_factory.NewStore(ctx, v.logger, utxostoreURL, "Seeder")
	if err != nil {
		return fmt.Errorf("could not create utxo store [%w]", err)
	}

	return nil
}

// Start function
func (v *Server) Start(ctx context.Context) (err error) {
	// this will block
	if err = util.StartGRPCServer(ctx, v.logger, "seeder", func(server *grpc.Server) {
		seeder_api.RegisterSeederAPIServer(server, v)
	}); err != nil {
		return err
	}

	return nil
}

func (v *Server) Stop(_ context.Context) error {
	return nil
}

func (v *Server) HealthGRPC(_ context.Context, _ *emptypb.Empty) (*seeder_api.HealthResponse, error) {
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

		lockingScript, err := bscript.NewP2PKHFromPubKeyBytes(privateKey.PubKey().SerialiseCompressed())
		if err != nil {
			prometheusSeederErrors.WithLabelValues("CreateSpendableTransactions", "NewP2PKHFromPubKeyBytes").Inc()
			return nil, status.Error(codes.Internal, err.Error())
		}

		btTx := bt.NewTx()
		for j := uint32(0); j < req.NumberOfOutputs; j++ {
			_ = btTx.PayTo(lockingScript, req.SatoshisPerOutput)
		}

		if err = v.utxoStore.Store(ctx, btTx); err != nil {
			v.logger.Fatalf("Error occurred in seeder: %v\n%s\n", err, debug.Stack())
			prometheusSeederErrors.WithLabelValues("CreateSpendableTransactions", "Store").Inc()
			return nil, status.Error(codes.Internal, err.Error())
		}

		if err = v.seederStore.Push(ctx, &store.SpendableTransaction{
			Txid:              btTx.TxIDChainHash(),
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
