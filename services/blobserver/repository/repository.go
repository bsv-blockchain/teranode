package repository

import (
	"context"
	"errors"
	"fmt"

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

func (r *Repository) GetBlockByHash(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	r.logger.Debugf("[Repository] GetBlockByHash: %s", hash.String())
	block, err := r.BlockchainClient.GetBlock(ctx, hash)
	if err != nil {
		return nil, err
	}

	return block.Bytes()
}

func (r *Repository) GetBlockByHeight(ctx context.Context, height uint32) ([]byte, error) {
	r.logger.Debugf("[Repository] GetBlockByHeight: %d", height)
	return nil, errors.New("not implemented")
}

func (r *Repository) GetBlockHeaderByHash(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	r.logger.Debugf("[Repository] GetBlockHeaderByHash: %s", hash.String())
	blockHeaders, err := r.BlockchainClient.GetBlockHeaders(ctx, hash, 1)
	if err != nil {
		return nil, err
	}

	if len(blockHeaders) != 1 {
		return nil, errors.New("block header not found")
	}

	return blockHeaders[0].Bytes(), nil
}

func (r *Repository) GetBlockHeaderByHeight(ctx context.Context, height uint32) ([]byte, error) {
	r.logger.Debugf("[Repository] GetBlockHeaderByHeight: %d", height)
	return nil, errors.New("not implemented")
}

func (r *Repository) GetSubtree(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	r.logger.Debugf("[Repository] GetSubtree: %s", hash.String())
	subtreeBytes, err := r.SubtreeStore.Get(ctx, hash.CloneBytes())
	if err != nil {
		return nil, fmt.Errorf("error in GetSubtree Get method: %w", err)
	}

	subtree, err := util.NewSubtreeFromBytes(subtreeBytes)
	if err != nil {
		return nil, fmt.Errorf("error in NewSubtreeFromBytes: %w", err)
	}

	subtreeNodeBytes, err := subtree.SerializeNodes()
	if err != nil {
		return nil, fmt.Errorf("error in SerializeNodes: %w", err)
	}

	return subtreeNodeBytes, nil
}

func (r *Repository) GetUtxo(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	r.logger.Debugf("[Repository] GetUtxo: %s", hash.String())
	resp, err := r.UtxoStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	return resp.SpendingTxID.CloneBytes(), nil
}
