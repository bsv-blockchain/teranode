package blobserver

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/services/blobserver/grpc_impl"
	"github.com/bitcoin-sv/ubsv/services/blobserver/http_impl"
	"github.com/bitcoin-sv/ubsv/services/blobserver/repository"
	"github.com/bitcoin-sv/ubsv/services/bootstrap"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// Server type carries the logger within it
type Server struct {
	logger       utils.Logger
	utxoStore    utxo.Interface
	txStore      blob.Store
	subtreeStore blob.Store
	grpcAddr     string
	httpAddr     string
	grpcServer   *grpc_impl.GRPC
	httpServer   *http_impl.HTTP
}

func Enabled() bool {
	_, grpcOk := gocore.Config().Get("blobserver_grpcListenAddress")
	_, httpOk := gocore.Config().Get("blobserver_httpListenAddress")
	return grpcOk || httpOk
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger utils.Logger, utxoStore utxo.Interface, txStore blob.Store, subtreeStore blob.Store) *Server {
	s := &Server{
		logger:       logger,
		utxoStore:    utxoStore,
		txStore:      txStore,
		subtreeStore: subtreeStore,
	}

	return s
}

func (v *Server) Init(ctx context.Context) (err error) {
	var grpcOk, httpOk bool
	v.grpcAddr, grpcOk = gocore.Config().Get("blobserver_grpcListenAddress")
	v.httpAddr, httpOk = gocore.Config().Get("blobserver_httpListenAddress")

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
		v.httpServer, err = http_impl.New(v.logger, repo)
		if err != nil {
			return fmt.Errorf("error creating http server: %s", err)
		}

		err = v.httpServer.Init(ctx)
		if err != nil {
			return fmt.Errorf("error initializing http server: %s", err)
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

		// TODO - This may need to be moved to a separate location in the code
		blobServerGrpcAddress, _ := gocore.Config().Get("blobserver_grpcAddress")
		blobServerHttpAddress, _ := gocore.Config().Get("blobserver_httpAddress")

		blobServerClientName, _ := gocore.Config().Get("blobserver_clientName")

		// Start a subscription to the bootstrap service

		g.Go(func() error {
			bootstrapClient := bootstrap.NewClient("BLOB_SERVER", blobServerClientName).WithCallback(func(p bootstrap.Peer) {
				if p.BlobServerGrpcAddress != "" {
					// Start a subscription to the new peer's blob server
					g.Go(func() error {
						v.logger.Infof("[BlobServer] Connecting to blob server at: %s", p.BlobServerGrpcAddress)
						return NewClient(ctx, "blobserver_bs", p.BlobServerGrpcAddress).Start(ctx)
					})
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
