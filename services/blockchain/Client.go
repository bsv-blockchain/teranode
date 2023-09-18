package blockchain

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client  blockchain_api.BlockchainAPIClient
	logger  utils.Logger
	running bool
	conn    *grpc.ClientConn
}

type BestBlockHeader struct {
	Header *model.BlockHeader
	Height uint32
}

func NewClient(ctx context.Context) (ClientI, error) {
	logger := gocore.Log("blkcC")

	blockchainGrpcAddress, ok := gocore.Config().Get("blockchain_grpcAddress")
	if !ok {
		return nil, fmt.Errorf("no blockchain_grpcAddress setting found")
	}

	var err error
	var baConn *grpc.ClientConn
	// retry a few times to connect to the blockchain service
	retries := 0
	maxRetries := 5
	for {
		baConn, err = util.GetGRPCClient(ctx, blockchainGrpcAddress, &util.ConnectionOptions{
			MaxRetries: 3,
		})
		if err != nil {
			if retries < maxRetries {
				logger.Errorf("[Blockchain] failed to connect to blockchain service, retrying %d: %v", retries, err)
				retries++
				time.Sleep(2 * time.Second)
				continue
			}

			logger.Errorf("[Blockchain] failed to connect to blockchain service, retried %d times: %v", maxRetries, err)
			return nil, err
		}
		break
	}

	return &Client{
		client:  blockchain_api.NewBlockchainAPIClient(baConn),
		logger:  logger,
		running: true,
		conn:    baConn,
	}, nil
}

func NewClientWithAddress(ctx context.Context, logger utils.Logger, address string) (ClientI, error) {
	baConn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client: blockchain_api.NewBlockchainAPIClient(baConn),
		logger: logger,
	}, nil
}

func (c Client) Health(ctx context.Context) (*blockchain_api.HealthResponse, error) {
	return c.client.Health(ctx, &emptypb.Empty{})
}

func (c Client) AddBlock(ctx context.Context, block *model.Block) error {
	req := &blockchain_api.AddBlockRequest{
		Header:           block.Header.Bytes(),
		CoinbaseTx:       block.CoinbaseTx.Bytes(),
		SubtreeHashes:    make([][]byte, 0),
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
	}

	for _, subtreeHash := range block.Subtrees {
		req.SubtreeHashes = append(req.SubtreeHashes, subtreeHash[:])
	}

	if _, err := c.client.AddBlock(ctx, req); err != nil {
		return err
	}

	return nil
}

func (c Client) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	resp, err := c.client.GetBlock(ctx, &blockchain_api.GetBlockRequest{
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

func (c Client) GetLastNBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	resp, err := c.client.GetLastNBlocks(ctx, &blockchain_api.GetLastNBlocksRequest{
		NumberOfBlocks: n,
	})
	if err != nil {
		return nil, err
	}

	return resp.Blocks, nil
}

func (c Client) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	resp, err := c.client.GetBlockExists(ctx, &blockchain_api.GetBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return false, err
	}

	return resp.Exists, nil
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

func (c Client) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBlockHeader(ctx, &blockchain_api.GetBlockHeaderRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return nil, nil, err
	}

	header, err := model.NewBlockHeaderFromBytes(resp.BlockHeader)
	if err != nil {
		return nil, nil, err
	}

	meta := &model.BlockHeaderMeta{
		Height:      resp.Height,
		TxCount:     resp.TxCount,
		SizeInBytes: resp.SizeInBytes,
		Miner:       resp.Miner,
	}

	return header, meta, nil
}

func (c Client) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []uint32, error) {
	resp, err := c.client.GetBlockHeaders(ctx, &blockchain_api.GetBlockHeadersRequest{
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

func (c Client) SendNotification(ctx context.Context, notification *model.Notification) error {
	_, err := c.client.SendNotification(ctx, &blockchain_api.Notification{
		Type: notification.Type,
		Hash: notification.Hash[:],
	})

	if err != nil {
		return err
	}

	return nil
}

func (c Client) Subscribe(ctx context.Context, source string) (chan *model.Notification, error) {
	ch := make(chan *model.Notification)

	go func() {
		<-ctx.Done()
		c.logger.Infof("[Blockchain] context done, closing subscription: %s", source)
		c.running = false
		err := c.conn.Close()
		if err != nil {
			c.logger.Errorf("[Blockchain] failed to close connection", err)
		}
	}()

	go func() {
		defer close(ch)

		for c.running {
			stream, err := c.client.Subscribe(ctx, &blockchain_api.SubscribeRequest{
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
						c.logger.Errorf("[Blockchain] failed to receive notification: %v", err)
					}
					time.Sleep(1 * time.Second)
					break
				}

				hash, err := chainhash.NewHash(resp.Hash)
				if err != nil {
					c.logger.Errorf("[Blockchain] failed to parse hash", err)
					continue
				}

				utils.SafeSend(ch, &model.Notification{
					Type: resp.Type,
					Hash: hash,
				})
			}
		}
	}()

	return ch, nil
}

func (c Client) GetState(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.client.GetState(ctx, &blockchain_api.GetStateRequest{
		Key: key,
	})
	if err != nil {
		return nil, err
	}

	return resp.Data, nil
}

func (c Client) SetState(ctx context.Context, key string, data []byte) error {
	_, err := c.client.SetState(ctx, &blockchain_api.SetStateRequest{
		Key:  key,
		Data: data,
	})
	if err != nil {
		return err
	}

	return nil
}
