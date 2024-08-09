package asset

import (
	"context"

	"github.com/bitcoin-sv/ubsv/errors"

	"github.com/bitcoin-sv/ubsv/stores/utxo"

	"github.com/bitcoin-sv/ubsv/services/asset/asset_api"
	"github.com/bitcoin-sv/ubsv/services/asset/centrifuge_impl"
	"github.com/bitcoin-sv/ubsv/services/asset/grpc_impl"
	"github.com/bitcoin-sv/ubsv/services/asset/http_impl"
	"github.com/bitcoin-sv/ubsv/services/asset/repository"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type peerWithContext struct {
	peer       PeerI
	cancelFunc context.CancelFunc
}

// Server type carries the logger within it
type Server struct {
	logger           ulogger.Logger
	utxoStore        utxo.Store
	txStore          blob.Store
	subtreeStore     blob.Store
	blockStore       blob.Store
	grpcAddr         string
	httpAddr         string
	grpcServer       *grpc_impl.GRPC
	httpServer       *http_impl.HTTP
	peers            map[string]peerWithContext
	notificationCh   chan *asset_api.Notification
	useP2P           bool
	centrifugeAddr   string
	centrifugeServer *centrifuge_impl.Centrifuge
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger ulogger.Logger, utxoStore utxo.Store, txStore blob.Store, subtreeStore blob.Store, blockStore blob.Store) *Server {
	s := &Server{
		logger:         logger,
		utxoStore:      utxoStore,
		txStore:        txStore,
		subtreeStore:   subtreeStore,
		blockStore:     blockStore,
		peers:          make(map[string]peerWithContext),
		notificationCh: make(chan *asset_api.Notification, 100),
	}

	return s
}

func (v *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (v *Server) Init(ctx context.Context) (err error) {
	var grpcOk, httpOk, centrifugeOk bool
	v.grpcAddr, grpcOk = gocore.Config().Get("asset_grpcListenAddress")
	v.httpAddr, httpOk = gocore.Config().Get("asset_httpListenAddress")

	if !grpcOk && !httpOk {
		return errors.NewConfigurationError("no asset_grpcListenAddress or asset_httpListenAddress setting found")
	}

	blockchainClient, err := blockchain.NewClient(ctx, v.logger, "services/asset")
	if err != nil {
		return errors.NewServiceError("error creating blockchain client", err)
	}

	repo, err := repository.NewRepository(v.logger, v.utxoStore, v.txStore, blockchainClient, v.subtreeStore, v.blockStore)
	if err != nil {
		return errors.NewServiceError("error creating repository", err)
	}

	v.centrifugeAddr, centrifugeOk = gocore.Config().Get("asset_centrifugeListenAddress", ":8101")

	if grpcOk {
		v.grpcServer, err = grpc_impl.New(v.logger, repo, func() []string {
			var peers []string
			for k := range v.peers {
				peers = append(peers, k)
			}
			return peers
		})
		if err != nil {
			return errors.NewServiceError("error creating grpc server", err)
		}

		err = v.grpcServer.Init(ctx)
		if err != nil {
			return errors.NewServiceError("error initializing grpc server", err)
		}
	}

	if httpOk {
		v.httpServer, err = http_impl.New(v.logger, repo, v.notificationCh)
		if err != nil {
			return errors.NewServiceError("error creating http server", err)
		}

		err = v.httpServer.Init(ctx)
		if err != nil {
			return errors.NewServiceError("error initializing http server", err)
		}
	}

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

	if gocore.Config().GetBool("feature_libP2P", false) {
		v.useP2P = true
	}

	return nil
}

// Start function
func (v *Server) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	if v.grpcServer != nil {
		g.Go(func() error {
			err := v.grpcServer.Start(ctx, v.grpcAddr)
			if err != nil {
				v.logger.Errorf("[Asset] error in grpc server: %v", err)
			}
			return err
		})

		AssetHttpAddressURL, _, _ := gocore.Config().GetURL("asset_httpAddress")
		securityLevel, _ := gocore.Config().GetInt("securityLevelHTTP", 0)

		if AssetHttpAddressURL.Scheme == "http" && securityLevel == 1 {
			AssetHttpAddressURL.Scheme = "https"
			v.logger.Warnf("asset_httpAddress is HTTP but securityLevel is 1, changing to HTTPS")
		} else if AssetHttpAddressURL.Scheme == "https" && securityLevel == 0 {
			AssetHttpAddressURL.Scheme = "http"
			v.logger.Warnf("asset_httpAddress is HTTPS but securityLevel is 0, changing to HTTP")
		}
	}

	if v.httpServer != nil {
		v.grpcServer.AddHttpSubscriber(v.notificationCh)

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

	if v.grpcServer != nil {
		v.logger.Infof("[Asset] Stopping grpc server")
		if err := v.grpcServer.Stop(ctx); err != nil {
			v.logger.Errorf("[Asset] error stopping grpc server: %v", err)
		}
	}

	if v.httpServer != nil {
		v.logger.Infof("[Asset] Stopping http server")
		if err := v.httpServer.Stop(ctx); err != nil {
			v.logger.Errorf("[Asset] error stopping http server: %v", err)
		}
	}

	return nil
}
