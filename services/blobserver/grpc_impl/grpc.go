package grpc_impl

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blobserver"
	"github.com/bitcoin-sv/ubsv/services/blobserver/blobserver_api"
	"github.com/bitcoin-sv/ubsv/services/blobserver/repository"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

type subscriber struct {
	subscription blobserver_api.BlobServerAPI_SubscribeServer
	source       string
	done         chan struct{}
}

type GRPC struct {
	blobserver_api.UnimplementedBlobServerAPIServer
	logger               utils.Logger
	baseURL              string
	getPeers             func() []string
	repository           *repository.Repository
	grpcServer           *grpc.Server
	blockchainClient     blockchain.ClientI
	newSubscriptions     chan subscriber
	newHttpSubscriptions chan chan *blobserver_api.Notification
	deadSubscriptions    chan subscriber
	subscribers          map[subscriber]bool
	httpSubscribers      map[chan *blobserver_api.Notification]bool
	notifications        chan *blobserver_api.Notification
}

func New(logger utils.Logger, repo *repository.Repository, getPeers func() []string) (*GRPC, error) {
	initPrometheusMetrics()

	u, err, found := gocore.Config().GetURL("blobserver_httpAddress")
	if err != nil {
		logger.Fatalf("blobserver_httpAddress is not a valid URL: %v", err)
	}

	if !found {
		remoteAddress, err := utils.GetPublicIPAddress()
		if err != nil {
			logger.Fatalf("Failed to get public IP address: %v", err)
		}

		blobServerPort, _ := gocore.Config().GetInt("blobserver_http_port")
		if blobServerPort == 0 {
			logger.Fatalf("blobserver_http_port is not set")
		}

		scheme := "http"
		if logger.LogLevel() > 0 {
			scheme = "https"
			blobServerPort, _ = gocore.Config().GetInt("blobserver_https_port")
			if blobServerPort == 0 {
				logger.Fatalf("blobserver_https_port is not set")
			}
		}

		u, err = url.ParseRequestURI(fmt.Sprintf("%s://%s:%d", scheme, remoteAddress, blobServerPort))
		if err != nil {
			logger.Fatalf("Failed to parse url: %v", err)
		}

		// Warn if there is a mismatch between log level and scheme
		if logger.LogLevel() == 0 && u.Scheme != "http" {
			logger.Warnf("blobserver_httpAddress scheme is not http, but logLevel is set to 0.")
		} else if u.Scheme != "https" {
			logger.Warnf("blobserver_httpAddress scheme is not https, but logLevel is set to %d.", logger.LogLevel())
		}
	}

	g := &GRPC{
		logger:               logger,
		baseURL:              u.String(),
		getPeers:             getPeers,
		repository:           repo,
		newSubscriptions:     make(chan subscriber, 10),
		newHttpSubscriptions: make(chan chan *blobserver_api.Notification, 10),
		deadSubscriptions:    make(chan subscriber, 10),
		subscribers:          make(map[subscriber]bool),
		httpSubscribers:      make(map[chan *blobserver_api.Notification]bool),
		notifications:        make(chan *blobserver_api.Notification, 100),
	}

	return g, nil
}

func (g *GRPC) Init(ctx context.Context) (err error) {
	g.logger.Infof("[BlobServer] GRPC service initializing")

	g.blockchainClient, err = blockchain.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("could not create blockchain client [%w]", err)
	}

	return nil
}

func (g *GRPC) Start(ctx context.Context, addr string) error {
	g.logger.Infof("[BlobServer] GRPC service starting")

	// Subscribe to the blockchain service
	blockchainSubscription, err := g.blockchainClient.Subscribe(ctx, "blobserver")
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				g.logger.Infof("[BlobServer] GRPC service shutting down")
				return
			case notification := <-blockchainSubscription:
				if notification == nil {
					continue
				}

				g.logger.Debugf("Sending %s notification: %s to %d subscribers", blobserver_api.Type(notification.Type).String(), notification.Hash.String(), len(g.subscribers))

				g.notifications <- &blobserver_api.Notification{
					Type:    blobserver_api.Type(notification.Type),
					Hash:    notification.Hash[:],
					BaseUrl: g.baseURL,
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				// Close all subscription channels
				for sub := range g.subscribers {
					safeClose(sub.done)
				}
				return
			case notification := <-g.notifications:
				for sub := range g.subscribers {
					go func(s subscriber) {
						g.logger.Debugf("Sending %s/%s notification: %s to subscriber %s", blobserver_api.Type(notification.Type).String(), notification.BaseUrl, utils.ReverseAndHexEncodeSlice(notification.Hash), s.source)
						if err := s.subscription.Send(notification); err != nil {
							g.deadSubscriptions <- s
						}
					}(sub)
				}
				for sub := range g.httpSubscribers {
					go func(s chan *blobserver_api.Notification) {
						s <- notification
					}(sub)
				}
			case s := <-g.newHttpSubscriptions:
				g.httpSubscribers[s] = true
				g.logger.Infof("[BlobServer] New HTTP Subscription received (Total=%d).", len(g.httpSubscribers))

			case s := <-g.newSubscriptions:
				g.subscribers[s] = true
				g.logger.Infof("[BlobServer] New Subscription received [%s] (Total=%d).", s.source, len(g.subscribers))

			case s := <-g.deadSubscriptions:
				delete(g.subscribers, s)
				safeClose(s.done)
				g.logger.Infof("[BlobServer] Subscription removed [%s] (Total=%d).", s.source, len(g.subscribers))
			}
		}
	}()

	// this will block
	if err := util.StartGRPCServer(ctx, g.logger, "blobserver", func(server *grpc.Server) {
		blobserver_api.RegisterBlobServerAPIServer(server, g)
		g.grpcServer = server
	}); err != nil {
		return err
	}

	return nil
}

func (g *GRPC) Stop(ctx context.Context) error {
	g.logger.Infof("[BlobServer] GRPC (impl) service shutting down")
	g.grpcServer.GracefulStop()
	return nil
}

func (g *GRPC) Health(_ context.Context, _ *emptypb.Empty) (*blobserver_api.HealthResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		blobserver.BlobServerStat.NewStat("Health").AddTime(start)
	}()

	prometheusBlobServerGRPCHealth.Inc()
	g.logger.Debugf("[BlobServer_grpc] Health check")

	return &blobserver_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (g *GRPC) GetBlock(ctx context.Context, request *blobserver_api.GetBlockRequest) (*blobserver_api.GetBlockResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		blobserver.BlobServerStat.NewStat("GetBlock").AddTime(start)
	}()

	prometheusBlobServerGRPCGetBlock.Inc()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	block, err := g.repository.GetBlockByHash(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	height, _ := util.ExtractCoinbaseHeight(block.CoinbaseTx)
	if height == 0 {
		_, meta, _ := g.repository.GetBlockHeader(ctx, blockHash)
		height = meta.Height
	}

	subtreeHashes := make([][]byte, len(block.Subtrees))
	for i, subtreeHash := range block.Subtrees {
		subtreeHashes[i] = subtreeHash[:]
	}

	return &blobserver_api.GetBlockResponse{
		Header:           block.Header.Bytes(),
		Height:           height,
		CoinbaseTx:       block.CoinbaseTx.Bytes(),
		SubtreeHashes:    subtreeHashes,
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
	}, nil
}

func (g *GRPC) GetBlockHeader(ctx context.Context, req *blobserver_api.GetBlockHeaderRequest) (*blobserver_api.GetBlockHeaderResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		blobserver.BlobServerStat.NewStat("GetBlockHeader").AddTime(start)
	}()

	hash, err := chainhash.NewHash(req.BlockHash)
	if err != nil {
		return nil, err
	}

	blockHeader, meta, err := g.repository.GetBlockHeader(ctx, hash)
	if err != nil {
		return nil, err
	}

	prometheusBlobServerGRPCGetBlockHeader.Inc()

	return &blobserver_api.GetBlockHeaderResponse{
		BlockHeader: blockHeader.Bytes(),
		Height:      meta.Height,
	}, nil
}

func (g *GRPC) GetBlockHeaders(ctx context.Context, req *blobserver_api.GetBlockHeadersRequest) (*blobserver_api.GetBlockHeadersResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		blobserver.BlobServerStat.NewStat("GetBlockHeaders").AddTime(start)
	}()

	prometheusBlobServerGRPCGetBlockHeaders.Inc()

	startHash, err := chainhash.NewHash(req.StartHash)
	if err != nil {
		return nil, err
	}

	nrOfHeaders := req.NumberOfHeaders
	if nrOfHeaders == 0 {
		nrOfHeaders = 100
	}
	if nrOfHeaders > 1000 {
		nrOfHeaders = 1000 // max is 1000
	}

	blockHeaders, heights, err := g.repository.GetBlockHeaders(ctx, startHash, nrOfHeaders)
	if err != nil {
		return nil, err
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	return &blobserver_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
		Heights:      heights,
	}, nil
}

func (g *GRPC) GetBestBlockHeader(ctx context.Context, _ *emptypb.Empty) (*blobserver_api.GetBlockHeaderResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		blobserver.BlobServerStat.NewStat("GetBestBlockHeader").AddTime(start)
	}()

	prometheusBlobServerGRPCGetBestBlockHeader.Inc()

	blockHeader, meta, err := g.repository.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	return &blobserver_api.GetBlockHeaderResponse{
		BlockHeader: blockHeader.Bytes(),
		Height:      meta.Height,
		TxCount:     meta.TxCount,
		SizeInBytes: meta.SizeInBytes,
		Miner:       meta.Miner,
	}, nil
}

func (g *GRPC) GetNodes(_ context.Context, _ *emptypb.Empty) (*blobserver_api.GetNodesResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		blobserver.BlobServerStat.NewStat("GetNodes").AddTime(start)
	}()

	prometheusBlobServerGRPCGetNodes.Inc()

	return &blobserver_api.GetNodesResponse{
		Nodes: g.getPeers(),
	}, nil
}

func (g *GRPC) AddHttpSubscriber(ch chan *blobserver_api.Notification) {
	start := gocore.CurrentNanos()
	defer func() {
		blobserver.BlobServerStat.NewStat("AddHttpSubscriber").AddTime(start)
	}()

	g.newHttpSubscriptions <- ch
}

func (g *GRPC) Subscribe(req *blobserver_api.SubscribeRequest, sub blobserver_api.BlobServerAPI_SubscribeServer) error {
	start := gocore.CurrentNanos()
	defer func() {
		blobserver.BlobServerStat.NewStat("Subscribe").AddTime(start)
	}()

	prometheusBlobServerGRPCSubscribe.Inc()
	g.logger.Debugf("[BlobServer_grpc] Subscribe: %s", req.Source)

	// Keep this subscription alive without endless loop - use a channel that blocks forever.
	ch := make(chan struct{})

	g.newSubscriptions <- subscriber{
		subscription: sub,
		source:       req.Source,
		done:         ch,
	}

	for {
		select {
		case <-sub.Context().Done():
			// Client disconnected.
			g.logger.Infof("[BlobServer] GRPC client disconnected: %s", req.Source)
			return nil
		case <-ch:
			// Subscription ended.
			return nil
		}
	}
}

func safeClose[T any](ch chan T) {
	defer func() {
		_ = recover()
	}()

	close(ch)
}
