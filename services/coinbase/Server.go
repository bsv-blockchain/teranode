package coinbase

import (
	"context"
	"net/http"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	bc "github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server type carries the logger within it
type Server struct {
	coinbase_api.UnimplementedCoinbaseAPIServer
	blockchainClient bc.ClientI
	coinbase         *Coinbase
	logger           ulogger.Logger
	stats            *gocore.Stat
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, blockchainClient bc.ClientI) *Server {
	initPrometheusMetrics()

	return &Server{
		logger:           logger,
		blockchainClient: blockchainClient,
		stats:            gocore.NewStat("coinbase"),
	}
}

func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := []health.Check{
		{Name: "BlockchainClient", Check: s.blockchainClient.Health},
		{Name: "FSM", Check: bc.CheckFSM(s.blockchainClient)},
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (s *Server) HealthGRPC(ctx context.Context, _ *emptypb.Empty) (*coinbase_api.HealthResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "HealthGRPC",
		tracing.WithParentStat(s.stats),
		tracing.WithCounter(prometheusHealth),
		tracing.WithDebugLogMessage(s.logger, "[HealthGRPC] called"),
	)
	defer deferFn()

	status, details, err := s.Health(ctx, false)

	return &coinbase_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.New(time.Now()),
	}, errors.WrapGRPC(err)
}

func (s *Server) Init(ctx context.Context) error {
	coinbaseStoreURL, err, found := gocore.Config().GetURL("coinbase_store")
	if err != nil {
		return errors.NewConfigurationError("failed to get coinbase_store setting", err)
	}
	if !found {
		return errors.NewConfigurationError("no coinbase_store setting found")
	}

	// We will reuse the blockchain service here to store the coinbase UTXOs
	// you could use the same database as the blockchain service, but we will allow for a different one
	store, err := blockchain.NewStore(s.logger, coinbaseStoreURL)
	if err != nil {
		return errors.NewStorageError("failed to create coinbase store: %s", err)
	}

	s.coinbase, err = NewCoinbase(s.logger, s.blockchainClient, store)
	if err != nil {
		return errors.NewServiceError("failed to create new coinbase: %s", err)
	}

	if err = s.coinbase.Init(ctx); err != nil {
		return errors.NewServiceError("failed to init coinbase: %s", err)
	}

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) error {

	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		err := s.blockchainClient.Restore(ctx)
		if err != nil {
			s.logger.Errorf("[Coinbase] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		s.logger.Infof("[Coinbase] Node is restoring, waiting for FSM to transition to Running state")
		_ = s.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, bc.FSMStateRUNNING)
		s.logger.Infof("[Coinbase] Node finished restoring and has transitioned to Running state, continuing to start Coinbase service")
	}

	if err := s.coinbase.peerSync.Start(ctx); err != nil {
		return err
	}

	// this will block
	if err := util.StartGRPCServer(ctx, s.logger, "coinbase", func(server *grpc.Server) {
		coinbase_api.RegisterCoinbaseAPIServer(server, s)
	}); err != nil {
		return err
	}

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	err := s.coinbase.peerSync.Stop(ctx)
	return err
}

func (s *Server) RequestFunds(ctx context.Context, req *coinbase_api.RequestFundsRequest) (*coinbase_api.RequestFundsResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "RequestFunds",
		tracing.WithParentStat(s.stats),
		tracing.WithHistogram(prometheusRequestFunds),
		tracing.WithDebugLogMessage(s.logger, "[RequestFunds] called for %s", req.Address),
	)
	defer deferFn()

	fundingTx, err := s.coinbase.RequestFunds(ctx, req.Address, req.DisableDistribute)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &coinbase_api.RequestFundsResponse{
		Tx: fundingTx.ExtendedBytes(),
	}, nil
}

func (s *Server) DistributeTransaction(ctx context.Context, req *coinbase_api.DistributeTransactionRequest) (*coinbase_api.DistributeTransactionResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "DistributeTransaction",
		tracing.WithParentStat(s.stats),
		tracing.WithHistogram(prometheusDistributeTransaction),
		tracing.WithLogMessage(s.logger, "[DistributeTransaction] called"),
	)
	defer deferFn()

	tx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("could not parse transaction bytes: %v", err))
	}

	if !tx.IsExtended() {
		return nil, errors.WrapGRPC(errors.NewTxInvalidError("transaction is not extended"))
	}

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
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetBalance",
		tracing.WithParentStat(s.stats),
		tracing.WithHistogram(prometheusGetBalance),
		tracing.WithLogMessage(s.logger, "[GetBalance] called"),
	)
	defer deferFn()

	balance, err := s.coinbase.getBalance(ctx)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return balance, nil
}
