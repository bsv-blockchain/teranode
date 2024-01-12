package coinbase

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
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

var stats = gocore.NewStat("coinbase")

// Server type carries the logger within it
type Server struct {
	coinbase_api.UnimplementedCoinbaseAPIServer
	coinbase *Coinbase
	logger   ulogger.Logger
}

func Enabled() bool {
	_, found := gocore.Config().Get("coinbase_grpcListenAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger) *Server {
	initPrometheusMetrics()
	return &Server{
		logger: logger,
	}
}

func (v *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
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

func (s *Server) HealthGRPC(_ context.Context, _ *emptypb.Empty) (*coinbase_api.HealthResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		stats.NewStat("Health_grpc").AddTime(start)
	}()

	prometheusHealth.Inc()
	return &coinbase_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (s *Server) RequestFunds(ctx context.Context, req *coinbase_api.RequestFundsRequest) (*coinbase_api.RequestFundsResponse, error) {
	start := gocore.CurrentTime()
	stat := stats.NewStat("RequestFunds_grpc", true)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusRequestFunds.Inc()

	ctx1 := util.ContextWithStat(ctx, stat)
	fundingTx, err := s.coinbase.RequestFunds(ctx1, req.Address, req.DisableDistribute)
	if err != nil {
		return nil, err
	}

	return &coinbase_api.RequestFundsResponse{
		Tx: fundingTx.ExtendedBytes(),
	}, nil
}

func (s *Server) DistributeTransaction(ctx context.Context, req *coinbase_api.DistributeTransactionRequest) (*coinbase_api.DistributeTransactionResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		stats.NewStat("DistributeTransaction").AddTime(start)
	}()

	tx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		return nil, fmt.Errorf("could not parse transaction bytes: %v", err)
	}

	if !tx.IsExtended() {
		return nil, fmt.Errorf("transaction is not extended")
	}

	prometheusDistributeTransaction.Inc()

	responses, _ := s.coinbase.DistributeTransaction(ctx, tx)

	resp := &coinbase_api.DistributeTransactionResponse{
		Txid:      tx.TxIDChainHash().String(),
		Timestamp: timestamppb.Now(),
		Responses: make([]*coinbase_api.ResponseWrapper, len(responses)),
	}

	for _, response := range responses {
		wrapper := &coinbase_api.ResponseWrapper{
			Address:       response.Addr,
			Retries:       response.Retries,
			DurationNanos: response.Duration.Nanoseconds(),
		}

		if response.Error != nil {
			wrapper.Error = response.Error.Error()
		}

		if response.Error != nil {
			wrapper.Error = response.Error.Error()
		}
		resp.Responses = append(resp.Responses, wrapper)
	}

	return resp, nil
}

func (s *Server) GetBalance(ctx context.Context, _ *emptypb.Empty) (*coinbase_api.GetBalanceResponse, error) {
	return s.coinbase.getBalance(ctx)
}
