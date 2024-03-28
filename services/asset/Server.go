package asset

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/asset/asset_api"
	"github.com/bitcoin-sv/ubsv/services/asset/centrifuge_impl"
	"github.com/bitcoin-sv/ubsv/services/asset/grpc_impl"
	"github.com/bitcoin-sv/ubsv/services/asset/http_impl"
	"github.com/bitcoin-sv/ubsv/services/asset/repository"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/bootstrap"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
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
	utxoStore        utxo.Interface
	txStore          blob.Store
	subtreeStore     blob.Store
	txMetaStore      txmeta.Store
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
func NewServer(logger ulogger.Logger, utxoStore utxo.Interface, txStore blob.Store, txMetaStore txmeta.Store, subtreeStore blob.Store) *Server {
	s := &Server{
		logger:         logger,
		utxoStore:      utxoStore,
		txStore:        txStore,
		txMetaStore:    txMetaStore,
		subtreeStore:   subtreeStore,
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
		return fmt.Errorf("no asset_grpcListenAddress or asset_httpListenAddress setting found")
	}

	blockchainClient, err := blockchain.NewClient(ctx, v.logger)
	if err != nil {
		return fmt.Errorf("error creating blockchain client: %s", err)
	}

	repo, err := repository.NewRepository(v.logger, v.utxoStore, v.txStore, v.txMetaStore, blockchainClient, v.subtreeStore)
	if err != nil {
		return fmt.Errorf("error creating repository: %s", err)
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
			return fmt.Errorf("error creating grpc server: %s", err)
		}

		err = v.grpcServer.Init(ctx)
		if err != nil {
			return fmt.Errorf("error initializing grpc server: %s", err)
		}
	}

	if httpOk {
		v.httpServer, err = http_impl.New(v.logger, repo, v.notificationCh)
		if err != nil {
			return fmt.Errorf("error creating http server: %s", err)
		}

		err = v.httpServer.Init(ctx)
		if err != nil {
			return fmt.Errorf("error initializing http server: %s", err)
		}
	}

	if centrifugeOk && v.httpServer != nil {
		v.centrifugeServer, err = centrifuge_impl.New(v.logger, repo, v.httpServer)
		if err != nil {
			return fmt.Errorf("error creating centrifuge server: %s", err)
		}

		err = v.centrifugeServer.Init(ctx)
		if err != nil {
			return fmt.Errorf("error initializing centrifuge server: %s", err)
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

		// We need to react to new nodes connecting to the network and we do this by subscribing to
		// the bootstrap service.  Each time a new node connects to the network, we will start a new
		// Asset subscription for that node.

		// We define a channel that will receive a notification whenever a new block or subtree is announced
		// by any Peer.  This will be handled by a WebSocket service that will push messages to any registered
		// browser clients.

		// TODO - This may need to be moved to a separate location in the code

		AssetGrpcAddress, _ := gocore.Config().Get("asset_grpcAddress")

		AssetHttpAddressURL, _, _ := gocore.Config().GetURL("asset_httpAddress")
		securityLevel, _ := gocore.Config().GetInt("securityLevelHTTP", 0)

		if AssetHttpAddressURL.Scheme == "http" && securityLevel == 1 {
			AssetHttpAddressURL.Scheme = "https"
			v.logger.Warnf("asset_httpAddress is HTTP but securityLevel is 1, changing to HTTPS")
		} else if AssetHttpAddressURL.Scheme == "https" && securityLevel == 0 {
			AssetHttpAddressURL.Scheme = "http"
			v.logger.Warnf("asset_httpAddress is HTTPS but securityLevel is 0, changing to HTTP")
		}

		AssetClientName, _ := gocore.Config().Get("asset_clientName")

		if v.useP2P {
			// if using p2p we are already subscribed but will need to get the best block from peers.
			v.logger.Infof("Using libP2P")
		} else if gocore.Config().GetBool("feature_bootstrap", true) {
			v.logger.Infof("Using Asset service")
			// Start a subscription to the Asset service

			g.Go(func() error {
				bootstrapClient := bootstrap.NewClient(v.logger, "BLOB_SERVER", AssetClientName).WithCallback(func(p bootstrap.Peer) {
					if p.AssetGrpcAddress != "" {
						v.logger.Infof("[Asset] Connecting to asset service at: %s", p.AssetGrpcAddress)
						if pp, ok := v.peers[p.AssetGrpcAddress]; ok {
							v.logger.Infof("[Asset] Already connected to blob server at: %s, stopping...", p.AssetGrpcAddress)
							_ = pp.peer.Stop()
							pp.cancelFunc()
							delete(v.peers, p.AssetGrpcAddress)
						}

						// Start a subscription to the new peer's blob server
						peerCtx, peerCtxCancel := context.WithCancel(ctx)

						peer := NewPeer(peerCtx, v.logger, "asset_bs", p.AssetGrpcAddress, v.notificationCh)

						v.peers[p.AssetGrpcAddress] = peerWithContext{
							peer:       peer,
							cancelFunc: peerCtxCancel,
						}
						g.Go(func() error {
							return v.peers[p.AssetGrpcAddress].peer.Start(peerCtx)
						})
					}

					if p.AssetHttpAddress != "" {
						// get the best block header and send to the block validation for processing
						v.logger.Infof("[Asset] Getting best block header from server at: %s", p.AssetHttpAddress)
						blockHeaderBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/bestblockheader", p.AssetHttpAddress))
						if err != nil {
							v.logger.Errorf("[Asset] error getting best block header from %s: %s", p.AssetHttpAddress, err)
							return
						}
						blockHeader, err := model.NewBlockHeaderFromBytes(blockHeaderBytes)
						if err != nil {
							v.logger.Errorf("[Asset] error parsing best block header from %s: %s", p.AssetHttpAddress, err)
							return
						}

						validationClient := blockvalidation.NewClient(ctx, v.logger)
						if err = validationClient.BlockFound(ctx, blockHeader.Hash(), p.AssetHttpAddress, false); err != nil {
							v.logger.Errorf("[Asset] error validating block from %s: %s", p.AssetHttpAddress, err)
						}
					}
				}).WithAssetGrpcAddress(AssetGrpcAddress).WithAssetHttpAddress(AssetHttpAddressURL.String())

				return bootstrapClient.Start(ctx)
			})
		} else {
			v.logger.Warnf("No P2P or Asset client is running")
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
		return fmt.Errorf("the main server has ended with error: %w", err)
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
