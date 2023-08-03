package blockchain

import (
	"context"
	"fmt"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockchain/blockchain_api"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client blockchain_api.BlockchainAPIClient
	logger utils.Logger
}

type BestBlockHeader struct {
	Header *model.BlockHeader
	Height uint32
}

func NewClient() (ClientI, error) {
	ctx := context.Background()

	blockchainGrpcAddress, ok := gocore.Config().Get("blockchain_grpcAddress")
	if !ok {
		return nil, fmt.Errorf("no blockchain_grpcAddress setting found")
	}
	baConn, err := utils.GetGRPCClient(ctx, blockchainGrpcAddress, &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client: blockchain_api.NewBlockchainAPIClient(baConn),
		logger: gocore.Log("blkcC"),
	}, nil
}

func NewClientWithAddress(logger utils.Logger, address string) (ClientI, error) {
	ctx := context.Background()

	baConn, err := utils.GetGRPCClient(ctx, address, &utils.ConnectionOptions{
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

	return model.NewBlock(header, coinbaseTx, subtreeHashes)
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

func (c Client) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, error) {
	resp, err := c.client.GetBlockHeaders(ctx, &blockchain_api.GetBlockHeadersRequest{
		StartHash:       blockHash.CloneBytes(),
		NumberOfHeaders: numberOfHeaders,
	})
	if err != nil {
		return nil, err
	}

	headers := make([]*model.BlockHeader, 0, len(resp.BlockHeaders))
	for _, headerBytes := range resp.BlockHeaders {
		header, err := model.NewBlockHeaderFromBytes(headerBytes)
		if err != nil {
			return nil, err
		}
		headers = append(headers, header)
	}

	return headers, nil
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
		defer close(ch)

		for {
			stream, err := c.client.Subscribe(ctx, &blockchain_api.SubscribeRequest{
				Source: source,
			})
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			for {
				resp, err := stream.Recv()
				if err != nil {
					time.Sleep(1 * time.Second)
					break
				}

				hash, err := chainhash.NewHash(resp.Hash)
				if err != nil {
					c.logger.Errorf("failed to parse hash", err)
					continue
				}

				ch <- &model.Notification{
					Type: resp.Type,
					Hash: hash,
				}
			}
		}
	}()

	return ch, nil
}
