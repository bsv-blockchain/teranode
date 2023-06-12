package blockchain

import (
	"context"
	"fmt"

	"github.com/TAAL-GmbH/ubsv/model"
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

func (c Client) AddBlock(ctx context.Context, block *model.Block) error {
	req := &blockchain_api.AddBlockRequest{
		Header:        block.Header.Bytes(),
		SubtreeHashes: make([][]byte, 0),
	}

	for _, subtreeHash := range block.Subtrees {
		h := subtreeHash.RootHash()
		req.SubtreeHashes = append(req.SubtreeHashes, h[:])
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

	header, err := bc.NewBlockHeaderFromBytes(resp.Header)
	if err != nil {
		return nil, err
	}

	return &model.Block{
		Header: header,
		// TODO - convert to merkle subtree
		// Subtrees: resp.SubtreeHashes,
	}, nil
}
