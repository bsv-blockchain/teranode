package asset

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain"

	"github.com/bitcoin-sv/ubsv/stores/utxo"

	"github.com/bitcoin-sv/ubsv/services/asset/centrifuge_impl"
	"github.com/bitcoin-sv/ubsv/services/asset/http_impl"
	"github.com/bitcoin-sv/ubsv/services/asset/repository"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// Server type carries the logger within it
type Server struct {
	logger           ulogger.Logger
	utxoStore        utxo.Store
	txStore          blob.Store
	subtreeStore     blob.Store
	blockStore       blob.Store
	httpAddr         string
	httpServer       *http_impl.HTTP
	centrifugeAddr   string
	centrifugeServer *centrifuge_impl.Centrifuge
	blockchainClient blockchain.ClientI
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger ulogger.Logger, utxoStore utxo.Store, txStore blob.Store, subtreeStore blob.Store, blockStore blob.Store, blockchainClient blockchain.ClientI) *Server {
	s := &Server{
		logger:           logger,
		utxoStore:        utxoStore,
		txStore:          txStore,
		subtreeStore:     subtreeStore,
		blockStore:       blockStore,
		blockchainClient: blockchainClient,
	}

	return s
}

func (v *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (v *Server) Init(ctx context.Context) (err error) {
	var httpOk, centrifugeOk bool
	v.httpAddr, httpOk = gocore.Config().Get("asset_httpListenAddress")

	if !httpOk {
		return errors.NewConfigurationError("no asset_httpListenAddress setting found")
	}

	repo, err := repository.NewRepository(v.logger, v.utxoStore, v.txStore, v.blockchainClient, v.subtreeStore, v.blockStore)

	if err != nil {
		return errors.NewServiceError("error creating repository", err)
	}

	if httpOk {
		v.httpServer, err = http_impl.New(v.logger, repo)
		if err != nil {
			return errors.NewServiceError("error creating http server", err)
		}

		err = v.httpServer.Init(ctx)
		if err != nil {
			return errors.NewServiceError("error initializing http server", err)
		}
	}

	if !gocore.Config().GetBool("asset_centrifuge_disable", false) {
		v.centrifugeAddr, centrifugeOk = gocore.Config().Get("asset_centrifugeListenAddress", ":8101")
		if centrifugeOk && v.httpServer != nil {
			v.centrifugeServer, err = centrifuge_impl.New(v.logger, repo, v.httpServer)
			if err != nil {
				return errors.NewServiceError("error creating centrifuge server: %s", err)
			}

			err = v.centrifugeServer.Init(ctx)
			if err != nil {
				return errors.NewServiceError("error initializing centrifuge server: %s", err)
			}
		}
	}

	return nil
}

// Start function
func (v *Server) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		_, err := v.blockchainClient.Restore(ctx, &emptypb.Empty{})
		if err != nil {
			v.logger.Errorf("[Asset] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		v.logger.Infof("[Asset] Node is restoring, waiting for FSM to transition to Running state")
		_ = v.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain_api.FSMStateType_RUNNING)
		v.logger.Infof("[Asset] Node finished restoring and has transitioned to Running state, continuing to start Asset service")
	}

	if v.httpServer != nil {
		g.Go(func() error {
			err := v.httpServer.Start(ctx, v.httpAddr)
			if err != nil {
				v.logger.Errorf("[Asset] error in http server: %v", err)
			}
			return err
		})

	}

	if v.centrifugeServer != nil {
		g.Go(func() error {
			return v.centrifugeServer.Start(ctx, v.centrifugeAddr)
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewServiceError("the main server has ended with error: %w", err)
	}

	return nil
}

func (v *Server) Stop(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if v.httpServer != nil {
		v.logger.Infof("[Asset] Stopping http server")
		if err := v.httpServer.Stop(ctx); err != nil {
			v.logger.Errorf("[Asset] error stopping http server: %v", err)
		}
	}

	return nil
}
