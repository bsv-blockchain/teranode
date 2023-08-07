package blockchain

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockchain/blockchain_api"
	"github.com/TAAL-GmbH/ubsv/stores/blockchain"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

type LocalClient struct {
	store  blockchain.Store
	logger utils.Logger
}

func NewLocalClient(logger utils.Logger, store blockchain.Store) (ClientI, error) {
	return &LocalClient{
		logger: logger,
		store:  store,
	}, nil
}

func (c LocalClient) Health(_ context.Context) (*blockchain_api.HealthResponse, error) {
	return &blockchain_api.HealthResponse{
		Ok: true,
	}, nil
}

func (c LocalClient) AddBlock(ctx context.Context, block *model.Block) error {
	return c.store.StoreBlock(ctx, block)
}

func (c LocalClient) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	block, _, err := c.store.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (c LocalClient) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	exists, err := c.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (c LocalClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, uint32, error) {
	return c.store.GetBestBlockHeader(ctx)
}

func (c LocalClient) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, error) {
	return c.store.GetBlockHeaders(ctx, blockHash, numberOfHeaders)
}

func (c LocalClient) SendNotification(ctx context.Context, notification *model.Notification) error {
	return c.SendNotification(ctx, notification)
}

func (c LocalClient) Subscribe(ctx context.Context, source string) (chan *model.Notification, error) {
	return c.Subscribe(ctx, source)
}

func (c LocalClient) GetState(ctx context.Context, key string) ([]byte, error) {
	return c.GetState(ctx, key)
}

func (c LocalClient) SetState(ctx context.Context, key string, data []byte) error {
	return c.SetState(ctx, key, data)
}
