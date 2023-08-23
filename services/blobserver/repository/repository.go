package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

type Repository struct {
	logger           utils.Logger
	UtxoStore        utxo.Interface
	TxStore          blob.Store
	SubtreeStore     blob.Store
	BlockchainClient blockchain.ClientI
}

func NewRepository(ctx context.Context, logger utils.Logger, utxoStore utxo.Interface, TxStore blob.Store, SubtreeStore blob.Store) (*Repository, error) {
	blockchainClient, err := blockchain.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &Repository{
		logger:           logger,
		BlockchainClient: blockchainClient,
		UtxoStore:        utxoStore,
		TxStore:          TxStore,
		SubtreeStore:     SubtreeStore,
	}, nil
}

func (r *Repository) GetTransaction(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	r.logger.Debugf("[Repository] GetTransaction: %s", hash.String())
	tx, err := r.TxStore.Get(ctx, hash.CloneBytes())
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (r *Repository) GetBlockByHash(ctx context.Context, hash *chainhash.Hash) (*model.Block, error) {
	r.logger.Debugf("[Repository] GetBlockByHash: %s", hash.String())
	block, err := r.BlockchainClient.GetBlock(ctx, hash)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (r *Repository) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	r.logger.Debugf("[Repository] GetBlockByHeight: %d", height)
	return nil, errors.New("not implemented")
}

func (r *Repository) GetBlockHeaderByHash(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, uint32, error) {
	r.logger.Debugf("[Repository] GetBlockHeader: %s", hash.String())
	blockHeaders, height, err := r.BlockchainClient.GetBlockHeader(ctx, hash)
	if err != nil {
		return nil, 0, err
	}

	return blockHeaders, height, nil
}

func (r *Repository) GetBlockHeaderByHeight(ctx context.Context, height uint32) (*model.BlockHeader, error) {
	r.logger.Debugf("[Repository] GetBlockHeaderByHeight: %d", height)
	return nil, errors.New("not implemented")
}

func (r *Repository) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash) ([]*model.BlockHeader, []uint32, error) {
	r.logger.Debugf("[Repository] GetBlockHeaders: %s", hash.String())
	blockHeaders, heights, err := r.BlockchainClient.GetBlockHeaders(ctx, hash, 100)
	if err != nil {
		return nil, nil, err
	}

	return blockHeaders, heights, nil
}

func (r *Repository) GetSubtreeBytes(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	subtree, err := r.GetSubtree(ctx, hash)
	if err != nil {
		return nil, err
	}

	subtreeNodeBytes, err := subtree.SerializeNodes()
	if err != nil {
		return nil, fmt.Errorf("error in SerializeNodes: %w", err)
	}

	return subtreeNodeBytes, nil
}
func (r *Repository) GetSubtree(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, error) {
	r.logger.Debugf("[Repository] GetSubtree: %s", hash.String())
	subtreeBytes, err := r.SubtreeStore.Get(ctx, hash.CloneBytes())
	if err != nil {
		return nil, fmt.Errorf("error in GetSubtree Get method: %w", err)
	}

	subtree, err := util.NewSubtreeFromBytes(subtreeBytes)
	if err != nil {
		return nil, fmt.Errorf("error in NewSubtreeFromBytes: %w", err)
	}

	return subtree, nil
}

func (r *Repository) GetUtxoBytes(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	resp, err := r.GetUtxo(ctx, hash)
	if err != nil {
		return nil, err
	}

	return resp.SpendingTxID.CloneBytes(), nil
}

func (r *Repository) GetUtxo(ctx context.Context, hash *chainhash.Hash) (*utxo.UTXOResponse, error) {
	r.logger.Debugf("[Repository] GetUtxo: %s", hash.String())
	resp, err := r.UtxoStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (r *Repository) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, uint32, error) {
	r.logger.Debugf("[Repository] GetBestBlockHeader")

	header, height, err := r.BlockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, 0, err
	}

	return header, height, nil
}
