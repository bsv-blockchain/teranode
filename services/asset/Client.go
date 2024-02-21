package asset

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/asset/asset_api"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client  asset_api.AssetAPIClient
	logger  ulogger.Logger
	running *atomic.Bool
	conn    *grpc.ClientConn
}

type BestBlockHeader struct {
	Header *model.BlockHeader
	Height uint32
}

func NewClient(ctx context.Context, logger ulogger.Logger, address string) (*Client, error) {
	var err error
	var blobConn *grpc.ClientConn
	var assetClient asset_api.AssetAPIClient

	// retry a few times to connect to the blob service
	maxRetries, _ := gocore.Config().GetInt("asset_maxRetries", 3)
	retrySleep, _ := gocore.Config().GetInt("asset_retrySleep", 1000)

	retries := 0
	for {
		blobConn, err = util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
			OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
			Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
			MaxRetries:  3,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to init blob service connection: %v", err)
		}

		assetClient = asset_api.NewAssetAPIClient(blobConn)

		_, err = assetClient.HealthGRPC(ctx, &emptypb.Empty{})
		if err != nil {
			if retries < maxRetries {
				retries++
				logger.Warnf("failed to connect to asset service, retrying %d: %v", retries, err)
				time.Sleep(time.Duration(retries*retrySleep) * time.Millisecond)
				continue
			}

			logger.Errorf("failed to connect to asset service, retried %d times: %v", maxRetries, err)
			return nil, err
		}
		break
	}

	running := atomic.Bool{}
	running.Store(true)

	return &Client{
		client:  assetClient,
		logger:  logger,
		running: &running,
		conn:    blobConn,
	}, nil
}

func (c *Client) Health(ctx context.Context) (bool, error) {
	response, err := c.client.HealthGRPC(ctx, &emptypb.Empty{})
	if err != nil {
		return false, err
	}

	return response.Ok, nil
}

func (c *Client) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	resp, err := c.client.GetBlock(ctx, &asset_api.GetBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return nil, err
	}

	header, err := model.NewBlockHeaderFromBytes(resp.Header)
	if err != nil {
		return nil, err
	}

	coinbaseTx, err := bt.NewTxFromBytes(resp.CoinbaseTx)
	if err != nil {
		return nil, err
	}

	subtreeHashes := make([]*chainhash.Hash, 0, len(resp.SubtreeHashes))
	for _, subtreeHash := range resp.SubtreeHashes {
		hash, err := chainhash.NewHash(subtreeHash)
		if err != nil {
			return nil, err
		}
		subtreeHashes = append(subtreeHashes, hash)
	}

	return model.NewBlock(header, coinbaseTx, subtreeHashes, resp.TransactionCount, resp.SizeInBytes)
}

func (c *Client) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, uint32, error) {
	resp, err := c.client.GetBestBlockHeader(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, 0, err
	}

	header, err := model.NewBlockHeaderFromBytes(resp.BlockHeader)
	if err != nil {
		return nil, 0, err
	}

	return header, resp.Height, nil
}

func (c *Client) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, uint32, error) {
	resp, err := c.client.GetBlockHeader(ctx, &asset_api.GetBlockHeaderRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return nil, 0, err
	}

	header, err := model.NewBlockHeaderFromBytes(resp.BlockHeader)
	if err != nil {
		return nil, 0, err
	}

	return header, resp.Height, nil
}

func (c *Client) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []uint32, error) {
	resp, err := c.client.GetBlockHeaders(ctx, &asset_api.GetBlockHeadersRequest{
		StartHash:       blockHash.CloneBytes(),
		NumberOfHeaders: numberOfHeaders,
	})
	if err != nil {
		return nil, nil, err
	}

	headers := make([]*model.BlockHeader, 0, len(resp.BlockHeaders))
	for _, headerBytes := range resp.BlockHeaders {
		header, err := model.NewBlockHeaderFromBytes(headerBytes)
		if err != nil {
			return nil, nil, err
		}
		headers = append(headers, header)
	}

	return headers, resp.Heights, nil
}

func (c *Client) Subscribe(ctx context.Context, source string) (chan *model.Notification, error) {
	ch := make(chan *model.Notification)

	go func() {
		<-ctx.Done()
		c.logger.Infof("[Asset] context done, closing subscription: %s", source)
		c.running.Store(false)
		err := c.conn.Close()
		if err != nil {
			c.logger.Errorf("[Asset] failed to close connection", err)
		}
	}()

	go func() {
		defer close(ch)

		for c.running.Load() {
			stream, err := c.client.Subscribe(ctx, &asset_api.SubscribeRequest{
				Source: source,
			})
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			for c.running.Load() {
				resp, err := stream.Recv()
				if err != nil {
					if !strings.Contains(err.Error(), context.Canceled.Error()) {
						c.logger.Errorf("[Asset] failed to receive notification: %v", err)
					}
					time.Sleep(1 * time.Second)
					break
				}

				hash, err := chainhash.NewHash(resp.Hash)
				if err != nil {
					c.logger.Errorf("[Asset] failed to parse hash", err)
					continue
				}

				c.logger.Debugf("[Asset] received notification %s: %s", model.NotificationType(resp.Type).String(), hash.String())
				ch <- &model.Notification{
					Type:    model.NotificationType(resp.Type),
					Hash:    hash,
					BaseURL: resp.BaseUrl,
				}
			}
		}
	}()

	return ch, nil
}

func (c *Client) Get(ctx context.Context, subtreeHash []byte) ([]byte, error) {
	response, err := c.client.Get(ctx, &asset_api.GetSubtreeRequest{
		Hash: subtreeHash,
	})
	if err != nil {
		return nil, err
	}

	return response.Subtree, nil
}

func (c *Client) Exists(ctx context.Context, subtreeHash []byte) (bool, error) {
	response, err := c.client.Exists(ctx, &asset_api.ExistsSubtreeRequest{
		Hash: subtreeHash,
	})
	if err != nil {
		return false, err
	}

	return response.Exists, nil
}

func (c *Client) GetNodes(_ context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*asset_api.GetNodesResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *Client) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	blobOptions := options.NewSetOptions(opts...)

	_, err := c.client.Set(ctx, &asset_api.SetSubtreeRequest{
		Hash:    key[:],
		Subtree: value,
		Ttl:     uint32(blobOptions.TTL.Seconds()),
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	_, err := c.client.SetTTL(ctx, &asset_api.SetSubtreeTTLRequest{
		Hash: key[:],
		Ttl:  uint32(int64(ttl.Seconds())),
	})
	if err != nil {
		return err
	}

	return nil
}
func (c *Client) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	//TODO implement me
	panic("implement me")
}
