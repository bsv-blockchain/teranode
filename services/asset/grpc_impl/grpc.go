package grpc_impl

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/asset/asset_api"
	"github.com/bitcoin-sv/ubsv/services/asset/repository"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var AssetStat = gocore.NewStat("Asset")

type subscriber struct {
	subscription asset_api.AssetAPI_SubscribeServer
	source       string
	done         chan struct{}
}

type GRPC struct {
	asset_api.UnimplementedAssetAPIServer
	logger               ulogger.Logger
	baseURL              string
	getPeers             func() []string
	repository           *repository.Repository
	grpcServer           *grpc.Server
	blockchainClient     blockchain.ClientI
	newSubscriptions     chan subscriber
	newHttpSubscriptions chan chan *asset_api.Notification
	deadSubscriptions    chan subscriber
	subscribers          map[subscriber]bool
	httpSubscribers      map[chan *asset_api.Notification]bool
	notifications        chan *asset_api.Notification
}

func New(logger ulogger.Logger, repo *repository.Repository, getPeers func() []string) (*GRPC, error) {
	initPrometheusMetrics()

	u, err, found := gocore.Config().GetURL("asset_httpAddress")
	if err != nil {
		logger.Fatalf("asset_httpAddress is not a valid URL: %v", err)
	}

	if !found {
		// TODO is this block of code correct?
		remoteAddress, err := utils.GetPublicIPAddress()
		if err != nil {
			logger.Fatalf("Failed to get public IP address: %v", err)
		}

		AssetPort, _ := gocore.Config().GetInt("asset_http_port")
		if AssetPort == 0 {
			logger.Fatalf("asset_http_port is not set")
		}

		scheme := "http"
		if logger.LogLevel() > 0 {
			scheme = "https"
			AssetPort, _ = gocore.Config().GetInt("asset_https_port")
			if AssetPort == 0 {
				logger.Fatalf("asset_https_port is not set")
			}
		}

		u, err = url.ParseRequestURI(fmt.Sprintf("%s://%s:%d", scheme, remoteAddress, AssetPort))
		if err != nil {
			logger.Fatalf("Failed to parse url: %v", err)
		}

		// Warn if there is a mismatch between log level and scheme
		if logger.LogLevel() == 0 && u.Scheme != "http" {
			logger.Warnf("asset_httpAddress scheme is not http, but logLevel is set to 0.")
		} else if u.Scheme != "https" {
			logger.Warnf("asset_httpAddress scheme is not https, but logLevel is set to %d.", logger.LogLevel())
		}
	}

	g := &GRPC{
		logger:               logger,
		baseURL:              u.String(),
		getPeers:             getPeers,
		repository:           repo,
		newSubscriptions:     make(chan subscriber, 10),
		newHttpSubscriptions: make(chan chan *asset_api.Notification, 10),
		deadSubscriptions:    make(chan subscriber, 10),
		subscribers:          make(map[subscriber]bool),
		httpSubscribers:      make(map[chan *asset_api.Notification]bool),
		notifications:        make(chan *asset_api.Notification, 100),
	}

	return g, nil
}

func (g *GRPC) Init(ctx context.Context) (err error) {
	g.logger.Infof("[Asset] GRPC service initializing")

	g.blockchainClient, err = blockchain.NewClient(ctx, g.logger)
	if err != nil {
		return fmt.Errorf("could not create blockchain client [%w]", err)
	}

	return nil
}

func (g *GRPC) Start(ctx context.Context, addr string) error {
	g.logger.Infof("[Asset] GRPC service starting")

	// Subscribe to the blockchain service
	blockchainSubscription, err := g.blockchainClient.Subscribe(ctx, "Asset")
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				g.logger.Infof("[Asset] GRPC service shutting down")
				return
			case notification := <-blockchainSubscription:
				if notification == nil {
					continue
				}

				g.logger.Debugf("Sending %s notification: %s to %d subscribers", asset_api.Type(notification.Type).String(), notification.Hash.String(), len(g.subscribers))

				g.notifications <- &asset_api.Notification{
					Type:    asset_api.Type(notification.Type),
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
						g.logger.Debugf("Sending %s/%s notification: %s to subscriber %s", asset_api.Type(notification.Type).String(), notification.BaseUrl, utils.ReverseAndHexEncodeSlice(notification.Hash), s.source)
						if err := s.subscription.Send(notification); err != nil {
							g.deadSubscriptions <- s
						}
					}(sub)
				}
				for sub := range g.httpSubscribers {
					go func(s chan *asset_api.Notification) {
						s <- notification
					}(sub)
				}
			case s := <-g.newHttpSubscriptions:
				g.httpSubscribers[s] = true
				g.logger.Infof("[Asset] New HTTP Subscription received (Total=%d).", len(g.httpSubscribers))

			case s := <-g.newSubscriptions:
				g.subscribers[s] = true
				g.logger.Infof("[Asset] New Subscription received [%s] (Total=%d).", s.source, len(g.subscribers))

			case s := <-g.deadSubscriptions:
				delete(g.subscribers, s)
				safeClose(s.done)
				g.logger.Infof("[Asset] Subscription removed [%s] (Total=%d).", s.source, len(g.subscribers))
			}
		}
	}()

	// this will block
	if err := util.StartGRPCServer(ctx, g.logger, "asset", func(server *grpc.Server) {
		asset_api.RegisterAssetAPIServer(server, g)
		g.grpcServer = server
	}); err != nil {
		return err
	}

	return nil
}

func (g *GRPC) Stop(ctx context.Context) error {
	g.logger.Infof("[Asset] GRPC (impl) service shutting down")
	g.grpcServer.GracefulStop()
	return nil
}

func (g *GRPC) Health(_ context.Context, _ *emptypb.Empty) (*asset_api.HealthResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("Health").AddTime(start)
	}()

	prometheusAssetGRPCHealth.Inc()
	g.logger.Debugf("[Asset_grpc] Health check")

	return &asset_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (g *GRPC) HealthGRPC(_ context.Context, _ *emptypb.Empty) (*asset_api.HealthResponse, error) {
	return &asset_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil

}

func (g *GRPC) GetBlock(ctx context.Context, request *asset_api.GetBlockRequest) (*asset_api.GetBlockResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBlock").AddTime(start)
	}()

	prometheusAssetGRPCGetBlock.Inc()

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

	return &asset_api.GetBlockResponse{
		Header:           block.Header.Bytes(),
		Height:           height,
		CoinbaseTx:       block.CoinbaseTx.Bytes(),
		SubtreeHashes:    subtreeHashes,
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
	}, nil
}

func (g *GRPC) GetBlockStats(ctx context.Context, _ *emptypb.Empty) (*model.BlockStats, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBlockStats").AddTime(start)
	}()

	return g.repository.GetBlockStats(ctx)
}

func (g *GRPC) GetBlockGraphData(ctx context.Context, in *asset_api.GetBlockGraphDataRequest) (*model.BlockDataPoints, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBlockGraphData").AddTime(start)
	}()

	return g.repository.GetBlockGraphData(ctx, in.PeriodMillis)
}

func (g *GRPC) GetBlockHeader(ctx context.Context, req *asset_api.GetBlockHeaderRequest) (*asset_api.GetBlockHeaderResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBlockHeader").AddTime(start)
	}()

	hash, err := chainhash.NewHash(req.BlockHash)
	if err != nil {
		return nil, err
	}

	blockHeader, meta, err := g.repository.GetBlockHeader(ctx, hash)
	if err != nil {
		return nil, err
	}

	prometheusAssetGRPCGetBlockHeader.Inc()

	return &asset_api.GetBlockHeaderResponse{
		BlockHeader: blockHeader.Bytes(),
		Height:      meta.Height,
	}, nil
}

func (g *GRPC) GetBlockHeaders(ctx context.Context, req *asset_api.GetBlockHeadersRequest) (*asset_api.GetBlockHeadersResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBlockHeaders").AddTime(start)
	}()

	prometheusAssetGRPCGetBlockHeaders.Inc()

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

	return &asset_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
		Heights:      heights,
	}, nil
}

func (g *GRPC) GetBestBlockHeader(ctx context.Context, _ *emptypb.Empty) (*asset_api.GetBlockHeaderResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBestBlockHeader").AddTime(start)
	}()

	prometheusAssetGRPCGetBestBlockHeader.Inc()

	blockHeader, meta, err := g.repository.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	return &asset_api.GetBlockHeaderResponse{
		BlockHeader: blockHeader.Bytes(),
		Height:      meta.Height,
		TxCount:     meta.TxCount,
		SizeInBytes: meta.SizeInBytes,
		Miner:       meta.Miner,
	}, nil
}

func (g *GRPC) GetNodes(_ context.Context, _ *emptypb.Empty) (*asset_api.GetNodesResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetNodes").AddTime(start)
	}()

	prometheusAssetGRPCGetNodes.Inc()

	return &asset_api.GetNodesResponse{
		Nodes: g.getPeers(),
	}, nil
}

func (g *GRPC) Get(ctx context.Context, request *asset_api.GetSubtreeRequest) (*asset_api.GetSubtreeResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("Get").AddTime(start)
	}()

	hash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	subtreeBytes, err := g.repository.SubtreeStore.Get(ctx, hash[:])
	if err != nil {
		return nil, err
	}

	return &asset_api.GetSubtreeResponse{
		Subtree: subtreeBytes,
	}, nil
}

func (g *GRPC) Exists(ctx context.Context, request *asset_api.ExistsSubtreeRequest) (*asset_api.ExistsSubtreeResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("Exists").AddTime(start)
	}()

	hash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, err
	}

	exists, err := g.repository.SubtreeStore.Exists(ctx, hash[:])
	if err != nil {
		return nil, err
	}

	return &asset_api.ExistsSubtreeResponse{
		Exists: exists,
	}, nil
}

func (g *GRPC) Set(ctx context.Context, request *asset_api.SetSubtreeRequest) (*emptypb.Empty, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("Set").AddTime(start)
	}()

	ttl := time.Duration(request.Ttl) * time.Second
	return &emptypb.Empty{}, g.repository.SubtreeStore.Set(ctx, request.Hash, request.Subtree, options.WithTTL(ttl))
}

func (g *GRPC) SetTTL(ctx context.Context, request *asset_api.SetSubtreeTTLRequest) (*emptypb.Empty, error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("SetTTL").AddTime(start)
	}()

	ttl := time.Duration(request.Ttl) * time.Second
	return &emptypb.Empty{}, g.repository.SubtreeStore.SetTTL(ctx, request.Hash, ttl)
}

func (g *GRPC) AddHttpSubscriber(ch chan *asset_api.Notification) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("AddHttpSubscriber").AddTime(start)
	}()

	g.newHttpSubscriptions <- ch
}

func (g *GRPC) Subscribe(req *asset_api.SubscribeRequest, sub asset_api.AssetAPI_SubscribeServer) error {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("Subscribe").AddTime(start)
	}()

	prometheusAssetGRPCSubscribe.Inc()
	g.logger.Debugf("[Asset_grpc] Subscribe: %s", req.Source)

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
			g.logger.Infof("[Asset] GRPC client disconnected: %s", req.Source)
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
