package coinbase

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// var (
// 	prometheusCoinbaseAddBlock prometheus.Counter
// )

// func init() {
// 	prometheusCoinbaseAddBlock = promauto.NewCounter(
// 		prometheus.CounterOpts{
// 			Name: "coinbase_add_block",
// 			Help: "Number of blocks added to the coinbase service",
// 		},
// 	)
// }

// Server type carries the logger within it
type Server struct {
	coinbase_api.UnimplementedCoinbaseAPIServer
	coinbase *Coinbase
	logger   utils.Logger
}

func Enabled() bool {
	_, found := gocore.Config().Get("coinbase_grpcListenAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger) *Server {
	initPrometheusMetrics()
	return &Server{
		logger: logger,
	}
}

func (s *Server) Init(ctx context.Context) error {
	coinbaseStoreURL, err, found := gocore.Config().GetURL("coinbase_store")
	if err != nil {
		return fmt.Errorf("failed to get coinbase_store setting: %s", err)
	}
	if !found {
		return fmt.Errorf("no coinbase_store setting found")
	}

	// We will reuse the blockchain service here to store the coinbase UTXOs
	// you could use the same database as the blockchain service, but we will allow for a different one
	store, err := blockchain.NewStore(s.logger, coinbaseStoreURL)
	if err != nil {
		return fmt.Errorf("failed to create coinbase store: %s", err)
	}

	s.coinbase, err = NewCoinbase(s.logger, store)
	if err != nil {
		return fmt.Errorf("failed to create new coinbase: %s", err)
	}

	if err = s.coinbase.Init(ctx); err != nil {
		return fmt.Errorf("failed to init coinbase: %s", err)
	}

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) error {
	// this will block
	if err := util.StartGRPCServer(ctx, s.logger, "coinbase", func(server *grpc.Server) {
		coinbase_api.RegisterCoinbaseAPIServer(server, s)
	}); err != nil {
		return err
	}

	return nil
}

func (s *Server) Stop(_ context.Context) error {
	return nil
}

func (s *Server) Health(_ context.Context, _ *emptypb.Empty) (*coinbase_api.HealthResponse, error) {
	prometheusHealth.Inc()
	return &coinbase_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (s *Server) RequestFunds(ctx context.Context, req *coinbase_api.RequestFundsRequest) (*coinbase_api.RequestFundsResponse, error) {
	prometheusRequestFunds.Inc()

	fundingTx, err := s.coinbase.RequestFunds(ctx, req.Address)
	if err != nil {
		return nil, err
	}

	return &coinbase_api.RequestFundsResponse{
		Tx: fundingTx.ExtendedBytes(),
	}, nil
}
