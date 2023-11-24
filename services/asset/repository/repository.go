package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

type Repository struct {
	logger           utils.Logger
	UtxoStore        utxo.Interface
	TxStore          blob.Store
	TxMetaStore      txmeta.Store
	SubtreeStore     blob.Store
	BlockchainClient blockchain.ClientI
}

func NewRepository(logger utils.Logger, utxoStore utxo.Interface, txStore blob.Store, txMetaStore txmeta.Store,
	blockchainClient blockchain.ClientI, SubtreeStore blob.Store) (*Repository, error) {

	return &Repository{
		logger:           logger,
		BlockchainClient: blockchainClient,
		UtxoStore:        utxoStore,
		TxStore:          txStore,
		TxMetaStore:      txMetaStore,
		SubtreeStore:     SubtreeStore,
	}, nil
}

func (r *Repository) Health(ctx context.Context) (int, string, error) {
	var sb strings.Builder
	errs := make([]error, 0)

	code, details, err := r.TxStore.Health(ctx)
	if err != nil {
		errs = append(errs, err)
		_, _ = sb.WriteString(fmt.Sprintf("TxStore: BAD %d - %q: %v\n", code, details, err))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("TxStore: GOOD %d - %q\n", code, details))
	}

	code, details, err = r.UtxoStore.Health(ctx)
	if err != nil {
		errs = append(errs, err)
		_, _ = sb.WriteString(fmt.Sprintf("UTXOStore: BAD %d - %q: %v\n", code, details, err))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("UTXOStore: GOOD %d - %q\n", code, details))
	}

	code, details, err = r.SubtreeStore.Health(ctx)
	if err != nil {
		errs = append(errs, err)
		_, _ = sb.WriteString(fmt.Sprintf("SubtreeStore: BAD %d - %q: %v\n", code, details, err))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("SubtreeStore: GOOD %d - %q\n", code, details))
	}

	if len(errs) > 0 {
		return -1, sb.String(), errors.New("Health errors occurred")
	}

	return 0, sb.String(), nil
}

func (r *Repository) GetTransaction(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	r.logger.Debugf("[Repository] GetTransaction: %s", hash.String())

	txMeta, err := r.TxMetaStore.Get(ctx, hash)
	if err == nil && txMeta != nil {
		return txMeta.Tx.ExtendedBytes(), nil
	}

	tx, err := r.TxStore.Get(ctx, hash.CloneBytes())
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (r *Repository) GetTransactionMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	r.logger.Debugf("[Repository] GetTransaction: %s", hash.String())
	txMeta, err := r.TxMetaStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	return txMeta, nil
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

func (r *Repository) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	r.logger.Debugf("[Repository] GetBlockHeader: %s", hash.String())
	blockHeader, blockHeaderMeta, err := r.BlockchainClient.GetBlockHeader(ctx, hash)
	if err != nil {
		return nil, nil, err
	}

	return blockHeader, blockHeaderMeta, nil
}

func (r *Repository) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	r.logger.Debugf("[Repository] GetLastNBlocks: %d", n)

	blockInfo, err := r.BlockchainClient.GetLastNBlocks(ctx, n, includeOrphans, fromHeight)
	if err != nil {
		return nil, err
	}

	return blockInfo, nil
}

func (r *Repository) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []uint32, error) {
	r.logger.Debugf("[Repository] GetBlockHeaders: %s", hash.String())
	blockHeaders, heights, err := r.BlockchainClient.GetBlockHeaders(ctx, hash, numberOfHeaders)
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

func (r *Repository) GetUtxo(ctx context.Context, hash *chainhash.Hash) (*utxo.Response, error) {
	r.logger.Debugf("[Repository] GetUtxo: %s", hash.String())
	resp, err := r.UtxoStore.Get(ctx, &utxo.Spend{Hash: hash})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (r *Repository) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	r.logger.Debugf("[Repository] GetBestBlockHeader")

	header, meta, err := r.BlockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, nil, err
	}

	return header, meta, nil
}
