package repository

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"io"
	"strings"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Repository struct {
	logger           ulogger.Logger
	UtxoStore        utxo.Interface
	TxStore          blob.Store
	TxMetaStore      txmeta.Store
	SubtreeStore     blob.Store
	BlockchainClient blockchain.ClientI
	CoinbaseProvider coinbase_api.CoinbaseAPIClient
}

func NewRepository(logger ulogger.Logger, utxoStore utxo.Interface, txStore blob.Store, txMetaStore txmeta.Store,
	blockchainClient blockchain.ClientI, SubtreeStore blob.Store) (*Repository, error) {

	// SAO - Loading the grpc client directly without using the coinbase.NewClient() method as it causes a circular dependency
	coinbaseGrpcAddress, ok := gocore.Config().Get("coinbase_grpcAddress")
	if !ok {
		return nil, fmt.Errorf("no coinbase_grpcAddress setting found")
	}
	baConn, err := util.GetGRPCClient(context.Background(), coinbaseGrpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	cbc := coinbase_api.NewCoinbaseAPIClient(baConn)

	return &Repository{
		logger:           logger,
		BlockchainClient: blockchainClient,
		CoinbaseProvider: cbc,
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
func (r *Repository) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	return r.BlockchainClient.GetBlockStats(ctx)
}

func (r *Repository) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	return r.BlockchainClient.GetBlockGraphData(ctx, periodMillis)
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
	block, err := r.BlockchainClient.GetBlockByHeight(ctx, height)
	if err != nil {
		return nil, err
	}

	return block, nil
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

func (r *Repository) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	r.logger.Debugf("[Repository] GetBlockHeadersFromHeight: %d-%d", height, limit)
	blockHeaders, metas, err := r.BlockchainClient.GetBlockHeadersFromHeight(ctx, height, limit)
	if err != nil {
		return nil, nil, err
	}

	return blockHeaders, metas, nil
}

func (r *Repository) GetSubtreeBytes(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	subtreeBytes, err := r.SubtreeStore.Get(ctx, hash.CloneBytes())
	if err != nil {
		return nil, err
	}

	return subtreeBytes, nil
}

func (r *Repository) GetSubtreeReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	return r.SubtreeStore.GetIoReader(ctx, hash.CloneBytes())
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

// GetSubtreeHead returns the head of the subtree, which only includes the Fees and SizeInBytes
func (r *Repository) GetSubtreeHead(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, int, error) {
	r.logger.Debugf("[Repository] GetSubtree: %s", hash.String())
	subtreeBytes, err := r.SubtreeStore.GetHead(ctx, hash.CloneBytes(), 56)
	if err != nil {
		return nil, 0, fmt.Errorf("error in GetSubtree GetHead method: %w", err)
	}

	if len(subtreeBytes) != 56 {
		return nil, 0, ubsverrors.ErrNotFound
	}

	subtree := &util.Subtree{}
	buf := bytes.NewBuffer(subtreeBytes)

	// read root hash
	_, err = chainhash.NewHash(buf.Next(32))
	if err != nil {
		return nil, 0, fmt.Errorf("unable to read root hash: %v", err)
	}

	// read fees
	subtree.Fees = binary.LittleEndian.Uint64(buf.Next(8))

	// read sizeInBytes
	subtree.SizeInBytes = binary.LittleEndian.Uint64(buf.Next(8))

	// read number of leaves
	numNodes := binary.LittleEndian.Uint64(buf.Next(8))

	return subtree, int(numNodes), nil
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

func (r *Repository) GetBalance(ctx context.Context) (uint64, uint64, error) {
	res, err := r.CoinbaseProvider.GetBalance(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, 0, err
	}

	return res.NumberOfUtxos, res.TotalSatoshis, nil
}
