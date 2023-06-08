package blockchain

import (
	"context"
	"fmt"

	"github.com/TAAL-GmbH/ubsv/services/blockchain/blockchain_api"
	"github.com/libsv/go-bc"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client blockchain_api.BlockchainAPIClient
}

func NewClient() (*Client, error) {
	ctx := context.Background()

	blockAssemblyGrpcAddress, ok := gocore.Config().Get("blockchain_grpcAddress")
	if !ok {
		return nil, fmt.Errorf("no blockchain_grpcAddress setting found")
	}
	baConn, err := utils.GetGRPCClient(ctx, blockAssemblyGrpcAddress, &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client: blockchain_api.NewBlockchainAPIClient(baConn),
	}, nil
}

func (c Client) Health(ctx context.Context) (*blockchain_api.HealthResponse, error) {
	return c.client.Health(ctx, &emptypb.Empty{})
}

func (c Client) AddBlock(ctx context.Context, block *bc.Block) error {
	resp, err := c.client.AddBlock(ctx, &blockchain_api.AddBlockRequest{
		Block: block.Bytes(),
	})
	if err != nil {
		return err
	}

	if !resp.Ok {
		return fmt.Errorf("blockchain service could not add block")
	}

	return nil
}

func (c Client) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*bc.Block, error) {
	resp, err := c.client.GetBlock(ctx, &blockchain_api.GetBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return nil, err
	}

	return bc.NewBlockFromBytes(resp.Block)
}

func (c Client) ChainTip(ctx context.Context) (*bc.BlockHeader, uint64, error) {
	resp, err := c.client.ChainTip(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, 0, err
	}

	blockHeader, err := bc.NewBlockHeaderFromBytes(resp.BlockHeader)
	if err != nil {
		return nil, 0, err
	}

	return blockHeader, resp.Height, nil
}
