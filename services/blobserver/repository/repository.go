package repository

import (
	"context"
	"errors"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Repository struct {
	UtxoStore        utxo.Interface
	TxStore          blob.Store
	SubtreeStore     blob.Store
	BlockchainClient blockchain.ClientI
}

func NewRepository(ctx context.Context, utxoStore utxo.Interface, TxStore blob.Store, SubtreeStore blob.Store) (*Repository, error) {
	blockchainClient, err := blockchain.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &Repository{
		BlockchainClient: blockchainClient,
		UtxoStore:        utxoStore,
		TxStore:          TxStore,
		SubtreeStore:     SubtreeStore,
	}, nil
}

func (r *Repository) GetTransaction(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	tx, err := r.TxStore.Get(ctx, hash.CloneBytes())
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (r *Repository) GetBlockByHash(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	block, err := r.BlockchainClient.GetBlock(ctx, hash)
	if err != nil {
		return nil, err
	}

	return block.Bytes()
}

func (r *Repository) GetBlockByHeight(ctx context.Context, height uint32) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (r *Repository) GetBlockHeaderByHash(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
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
	return nil, errors.New("not implemented")
}

func (r *Repository) GetSubtree(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	subtreeBytes, err := r.SubtreeStore.Get(ctx, hash.CloneBytes())
	if err != nil {
		return nil, err
	}

	subtree, err := util.NewSubtreeFromBytes(subtreeBytes)
	if err != nil {
		return nil, err
	}

	subtreeNodeBytes, err := subtree.SerializeNodes()
	if err != nil {
		return nil, err
	}

	return subtreeNodeBytes, nil
}

func (r *Repository) GetUtxo(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	resp, err := r.UtxoStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	return resp.SpendingTxID.CloneBytes(), nil
}
