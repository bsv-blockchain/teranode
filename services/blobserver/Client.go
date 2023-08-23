package blobserver

import (
	"context"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blobserver/blobserver_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client  blobserver_api.BlobServerAPIClient
	logger  utils.Logger
	running bool
	conn    *grpc.ClientConn
}

type BestBlockHeader struct {
	Header *model.BlockHeader
	Height uint32
}

func NewClient(ctx context.Context, logger utils.Logger, address string) (*Client, error) {
	baConn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client:  blobserver_api.NewBlobServerAPIClient(baConn),
		logger:  logger,
		running: true,
		conn:    baConn,
	}, nil
}

func (c Client) Health(ctx context.Context) (*blobserver_api.HealthResponse, error) {
	return c.client.Health(ctx, &emptypb.Empty{})
}

func (c Client) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	resp, err := c.client.GetBlock(ctx, &blobserver_api.GetBlockRequest{
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

	return model.NewBlock(header, coinbaseTx, subtreeHashes)
}

func (c Client) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, uint32, error) {
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

func (c Client) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, uint32, error) {
	resp, err := c.client.GetBlockHeader(ctx, &blobserver_api.GetBlockHeaderRequest{
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

func (c Client) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []uint32, error) {
	resp, err := c.client.GetBlockHeaders(ctx, &blobserver_api.GetBlockHeadersRequest{
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

func (c Client) Subscribe(ctx context.Context, source string) (chan *model.Notification, error) {
	ch := make(chan *model.Notification)

	go func() {
		<-ctx.Done()
		c.logger.Infof("[BlobServer] context done, closing subscription: %s", source)
		c.running = false
		err := c.conn.Close()
		if err != nil {
			c.logger.Errorf("[BlobServer] failed to close connection", err)
		}
	}()

	go func() {
		defer close(ch)

		for c.running {
			stream, err := c.client.Subscribe(ctx, &blobserver_api.SubscribeRequest{
				Source: source,
			})
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			for c.running {
				resp, err := stream.Recv()
				if err != nil {
					if !strings.Contains(err.Error(), context.Canceled.Error()) {
						c.logger.Errorf("[BlobServer] failed to receive notification: %v", err)
					}
					time.Sleep(1 * time.Second)
					break
				}

				hash, err := chainhash.NewHash(resp.Hash)
				if err != nil {
					c.logger.Errorf("[BlobServer] failed to parse hash", err)
					continue
				}

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
