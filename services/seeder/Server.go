package seeder

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"time"

	seeder_api "github.com/TAAL-GmbH/ubsv/services/seeder/seeder_api"
	"github.com/TAAL-GmbH/ubsv/services/seeder/store"
	"github.com/TAAL-GmbH/ubsv/services/seeder/store/memory"
	utxostore "github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/validator/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
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
	utxoStore   utxostore.UTXOStore
	logger      utils.Logger
	grpcServer  *grpc.Server
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

	v.logger.Infof("GRPC server listening on %s", address)

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
			return nil, status.Error(codes.Internal, err.Error())
		}

		txid, err := chainhash.NewHash(b)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		lockingScript, err := bscript.NewP2PKHFromPubKeyBytes(privateKey.PubKey().SerialiseCompressed())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		for j := uint32(0); j < req.NumberOfOutputs; j++ {
			hash, err := util.UTXOHash(txid, j, *lockingScript, req.SatoshisPerOutput)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}

			if _, err := v.utxoStore.Store(ctx, hash); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		}

		if err := v.seederStore.Push(context.Background(), &store.SpendableTransaction{
			Txid:              txid,
			NumberOfOutputs:   int32(req.NumberOfOutputs),
			SatoshisPerOutput: int64(req.SatoshisPerOutput),
			PrivateKey:        privateKey,
		}); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

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
			return err
		}

	}

	return nil
}
