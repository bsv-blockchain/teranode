package blobserver

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blobserver/blobserver_api"
	"github.com/bitcoin-sv/ubsv/services/blobserver/centrifuge_impl"
	"github.com/bitcoin-sv/ubsv/services/blobserver/grpc_impl"
	"github.com/bitcoin-sv/ubsv/services/blobserver/http_impl"
	"github.com/bitcoin-sv/ubsv/services/blobserver/repository"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/bootstrap"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type peerWithContext struct {
	peer       PeerI
	cancelFunc context.CancelFunc
}

// Server type carries the logger within it
type Server struct {
	logger           utils.Logger
	utxoStore        utxo.Interface
	txStore          blob.Store
	subtreeStore     blob.Store
	grpcAddr         string
	httpAddr         string
	centrifugeAddr   string
	grpcServer       *grpc_impl.GRPC
	httpServer       *http_impl.HTTP
	centrifugeServer *centrifuge_impl.Centrifuge
	peers            map[string]peerWithContext
	notificationCh   chan *blobserver_api.Notification
}

func Enabled() bool {
	_, grpcOk := gocore.Config().Get("blobserver_grpcListenAddress")
	_, httpOk := gocore.Config().Get("blobserver_httpListenAddress")
	return grpcOk || httpOk
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger utils.Logger, utxoStore utxo.Interface, txStore blob.Store, subtreeStore blob.Store) *Server {
	s := &Server{
		logger:         logger,
		utxoStore:      utxoStore,
		txStore:        txStore,
		subtreeStore:   subtreeStore,
		peers:          make(map[string]peerWithContext),
		notificationCh: make(chan *blobserver_api.Notification, 100),
	}

	return s
}

func (v *Server) Init(ctx context.Context) (err error) {
	var grpcOk, httpOk, centrifugeOk bool
	v.grpcAddr, grpcOk = gocore.Config().Get("blobserver_grpcListenAddress")
	v.httpAddr, httpOk = gocore.Config().Get("blobserver_httpListenAddress")
	v.centrifugeAddr, centrifugeOk = gocore.Config().Get("blobserver_centrifugeListenAddress", ":8101")

	if !grpcOk && !httpOk {
		return fmt.Errorf("no blobserver_grpcListenAddress or blobserver_httpListenAddress setting found")
	}

	repo, err := repository.NewRepository(ctx, v.logger, v.utxoStore, v.txStore, v.subtreeStore)
	if err != nil {
		return fmt.Errorf("error creating repository: %s", err)
	}

	if grpcOk {
		v.grpcServer, err = grpc_impl.New(v.logger, repo)
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
		v.centrifugeServer, err = centrifuge_impl.New(v.logger, repo)
		if err != nil {
			return fmt.Errorf("error creating centrifuge server: %s", err)
		}

		err = v.centrifugeServer.Init(ctx)
		if err != nil {
			return fmt.Errorf("error initializing centrifuge server: %s", err)
		}
	}

	return nil
}

// Start function
func (v *Server) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	if v.grpcServer != nil {
		g.Go(func() error {
			return v.grpcServer.Start(ctx, v.grpcAddr)
		})

		// We need to react to new nodes connecting to the network and we do this by subscribing to
		// the bootstrap service.  Each time a new node connects to the network, we will start a new
		// blobserver subscription for that node.

		// We define a channel that will receive a notification whenever a new block or subtree is announced
		// by any Peer.  This will be handled by a WebSocket service that will push messages to any registered
		// browser clients.

		// TODO - This may need to be moved to a separate location in the code
		blobServerGrpcAddress, _ := gocore.Config().Get("blobserver_grpcAddress")
		blobServerHttpAddress, _ := gocore.Config().Get("blobserver_httpAddress")

		blobServerClientName, _ := gocore.Config().Get("blobserver_clientName")

		// Start a subscription to the bootstrap service

		g.Go(func() error {
			bootstrapClient := bootstrap.NewClient("BLOB_SERVER", blobServerClientName).WithCallback(func(p bootstrap.Peer) {
				if p.BlobServerGrpcAddress != "" {
					v.logger.Infof("[BlobServer] Connecting to blob server at: %s", p.BlobServerGrpcAddress)
					if pp, ok := v.peers[p.BlobServerGrpcAddress]; ok {
						v.logger.Infof("[BlobServer] Already connected to blob server at: %s, stopping...", p.BlobServerGrpcAddress)
						_ = pp.peer.Stop()
						pp.cancelFunc()
						delete(v.peers, p.BlobServerGrpcAddress)
					}

					// Start a subscription to the new peer's blob server
					peerCtx, peerCtxCancel := context.WithCancel(ctx)

					peer := NewPeer(peerCtx, "blobserver_bs", p.BlobServerGrpcAddress, v.notificationCh)

					v.peers[p.BlobServerGrpcAddress] = peerWithContext{
						peer:       peer,
						cancelFunc: peerCtxCancel,
					}
					g.Go(func() error {
						return v.peers[p.BlobServerGrpcAddress].peer.Start(peerCtx)
					})
				}

				if p.BlobServerHttpAddress != "" {
					// get the best block header and send to the block validation for processing
					v.logger.Infof("[BlobServer] Getting best block header from server at: %s", p.BlobServerHttpAddress)
					blockHeaderBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/bestblockheader", p.BlobServerHttpAddress))
					if err != nil {
						v.logger.Errorf("[BlobServer] error getting best block header from %s: %s", p.BlobServerHttpAddress, err)
						return
					}
					blockHeader, err := model.NewBlockHeaderFromBytes(blockHeaderBytes)
					if err != nil {
						v.logger.Errorf("[BlobServer] error parsing best block header from %s: %s", p.BlobServerHttpAddress, err)
						return
					}

					validationClient := blockvalidation.NewClient(ctx)
					if err = validationClient.BlockFound(ctx, blockHeader.Hash(), p.BlobServerHttpAddress); err != nil {
						v.logger.Errorf("[BlobServer] error validating block from %s: %s", p.BlobServerHttpAddress, err)
					}
				}
			}).WithBlobServerGrpcAddress(blobServerGrpcAddress).WithBlobServerHttpAddress(blobServerHttpAddress)

			return bootstrapClient.Start(ctx)
		})
	}

	if v.httpServer != nil {
		g.Go(func() error {
			return v.httpServer.Start(ctx, v.httpAddr)
		})
	}

	if v.centrifugeServer != nil {
		g.Go(func() error {
			return v.centrifugeServer.Start(ctx, v.centrifugeAddr)
		})
	}

	// listen to self - this is not working yet
	//go func() {
	//	centrifugeClient := client.New(v.logger)
	//	if err := centrifugeClient.Start(ctx, "localhost:8092"); err != nil {
	//		v.logger.Errorf("[BlobServer] failed to start centrifuge client", err)
	//	}
	//}()

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (v *Server) Stop(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if v.grpcServer != nil {
		v.logger.Infof("[BlobServer] Stopping grpc server")
		if err := v.grpcServer.Stop(ctx); err != nil {
			v.logger.Errorf("[BlobServer] error stopping grpc server", "error", err)
		}
	}

	if v.httpServer != nil {
		v.logger.Infof("[BlobServer] Stopping http server")
		if err := v.httpServer.Stop(ctx); err != nil {
			v.logger.Errorf("[BlobServer] error stopping http server", "error", err)
		}
	}

	return nil
}
