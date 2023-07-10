package dao

import (
	"context"
	"errors"

	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type DAO struct {
	UtxoStore utxostore.Interface
	// TxStore           *TxStore
	BlockchainClient blockchain.ClientI
	// SubtreeStore     blockstore.Interface
}

func NewDAO() (*DAO, error) {
	blockchainClient, err := blockchain.NewClient()
	if err != nil {
		return nil, err
	}

	return &DAO{
		BlockchainClient: blockchainClient,
	}, nil
}

func (dao *DAO) GetTransaction(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (dao *DAO) GetBlockByHash(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	block, err := dao.BlockchainClient.GetBlock(ctx, hash)
	if err != nil {
		return nil, err
	}

	return block.Bytes()
}

func (dao *DAO) GetBlockByHeight(ctx context.Context, height uint32) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (dao *DAO) GetBlockHeaderByHash(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	blockHeaders, err := dao.BlockchainClient.GetBlockHeaders(ctx, hash, 1)
	if err != nil {
		return nil, err
	}

	if len(blockHeaders) != 1 {
		return nil, errors.New("block header not found")
	}

	return blockHeaders[0].Bytes(), nil
}

func (dao *DAO) GetBlockHeaderByHeight(ctx context.Context, height uint32) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (dao *DAO) GetSubtree(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (dao *DAO) GetUtxo(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	resp, err := dao.UtxoStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	return resp.SpendingTxID.CloneBytes(), nil
}
