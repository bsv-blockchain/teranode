package repository

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/health"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Repository struct {
	logger              ulogger.Logger
	UtxoStore           utxo.Store
	TxStore             blob.Store
	SubtreeStore        blob.Store
	BlockPersisterStore blob.Store
	BlockchainClient    blockchain.ClientI
	// coinbaseAvailable bool
	CoinbaseProvider coinbase_api.CoinbaseAPIClient
}

func NewRepository(logger ulogger.Logger, utxoStore utxo.Store, txStore blob.Store,
	blockchainClient blockchain.ClientI, subtreeStore blob.Store, blockPersisterStore blob.Store) (*Repository, error) {
	var cbc coinbase_api.CoinbaseAPIClient

	coinbaseGrpcAddress, ok := gocore.Config().Get("coinbase_grpcAddress")
	if ok && len(coinbaseGrpcAddress) > 0 {
		baConn, err := util.GetGRPCClient(context.Background(), coinbaseGrpcAddress, &util.ConnectionOptions{
			MaxRetries: 3,
		})
		if err != nil {
			return nil, err
		}

		cbc = coinbase_api.NewCoinbaseAPIClient(baConn)
	}

	return &Repository{
		logger:              logger,
		BlockchainClient:    blockchainClient,
		CoinbaseProvider:    cbc,
		UtxoStore:           utxoStore,
		TxStore:             txStore,
		SubtreeStore:        subtreeStore,
		BlockPersisterStore: blockPersisterStore,
	}, nil
}

func (repo *Repository) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := []health.Check{
		{Name: "BlockchainClient", Check: repo.BlockchainClient.Health},
		{Name: "UtxoStore", Check: repo.UtxoStore.Health},
		{Name: "TxStore", Check: repo.TxStore.Health},
		{Name: "SubtreeStore", Check: repo.SubtreeStore.Health},
		{Name: "BlockPersisterStore", Check: repo.BlockPersisterStore.Health},
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (repo *Repository) GetTransaction(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	repo.logger.Debugf("[Repository] GetTransaction: %s", hash.String())

	txMeta, err := repo.UtxoStore.Get(ctx, hash)
	if err == nil && txMeta != nil {
		return txMeta.Tx.ExtendedBytes(), nil
	}

	repo.logger.Warnf("[Repository] GetTransaction: %s not found in txmeta store: %v", hash.String(), err)

	tx, err := repo.TxStore.Get(ctx, hash.CloneBytes())
	if err != nil {
		return nil, err
	}

	return tx, nil
}
func (repo *Repository) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	return repo.BlockchainClient.GetBlockStats(ctx)
}

func (repo *Repository) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	return repo.BlockchainClient.GetBlockGraphData(ctx, periodMillis)
}

func (repo *Repository) GetTransactionMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	repo.logger.Debugf("[Repository] GetTransaction: %s", hash.String())

	txMeta, err := repo.UtxoStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	return txMeta, nil
}

func (repo *Repository) GetBlockByHash(ctx context.Context, hash *chainhash.Hash) (*model.Block, error) {
	repo.logger.Debugf("[Repository] GetBlockByHash: %s", hash.String())

	block, err := repo.BlockchainClient.GetBlock(ctx, hash)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (repo *Repository) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	repo.logger.Debugf("[Repository] GetBlockByHeight: %d", height)

	block, err := repo.BlockchainClient.GetBlockByHeight(ctx, height)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (repo *Repository) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	repo.logger.Debugf("[Repository] GetBlockHeader: %s", hash.String())

	blockHeader, blockHeaderMeta, err := repo.BlockchainClient.GetBlockHeader(ctx, hash)
	if err != nil {
		return nil, nil, err
	}

	return blockHeader, blockHeaderMeta, nil
}

func (repo *Repository) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	repo.logger.Debugf("[Repository] GetLastNBlocks: %d", n)

	blockInfo, err := repo.BlockchainClient.GetLastNBlocks(ctx, n, includeOrphans, fromHeight)
	if err != nil {
		return nil, err
	}

	return blockInfo, nil
}

func (repo *Repository) GetBlocks(ctx context.Context, hash *chainhash.Hash, n uint32) ([]*model.Block, error) {
	repo.logger.Debugf("[Repository] GetNBlocks: %d", n)

	blocks, err := repo.BlockchainClient.GetBlocks(ctx, hash, n)
	if err != nil {
		return nil, err
	}

	return blocks, nil
}

func (repo *Repository) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	repo.logger.Debugf("[Repository] GetBlockHeaders: %s", hash.String())

	blockHeaders, blockHeaderMetas, err := repo.BlockchainClient.GetBlockHeaders(ctx, hash, numberOfHeaders)
	if err != nil {
		return nil, nil, err
	}

	return blockHeaders, blockHeaderMetas, nil
}

func (repo *Repository) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	repo.logger.Debugf("[Repository] GetBlockHeadersFromHeight: %d-%d", height, limit)

	blockHeaders, metas, err := repo.BlockchainClient.GetBlockHeadersFromHeight(ctx, height, limit)
	if err != nil {
		return nil, nil, err
	}

	return blockHeaders, metas, nil
}

func (repo *Repository) GetSubtreeBytes(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	subtreeBytes, err := repo.SubtreeStore.Get(ctx, hash.CloneBytes(), options.WithFileExtension("subtree"))
	if err != nil {
		return nil, err
	}

	return subtreeBytes, nil
}

func (repo *Repository) GetSubtreeReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	return repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), options.WithFileExtension("subtree"))
}

func (repo *Repository) GetSubtreeDataReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	return repo.BlockPersisterStore.GetIoReader(ctx, hash.CloneBytes(), options.WithFileExtension("subtree"))
}

func (repo *Repository) GetSubtree(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, error) {
	repo.logger.Debugf("[Repository] GetSubtree: %s", hash.String())

	subtreeBytes, err := repo.SubtreeStore.Get(ctx, hash.CloneBytes(), options.WithFileExtension("subtree"))
	if err != nil {
		return nil, errors.NewServiceError("error in GetSubtree Get method", err)
	}

	subtree, err := util.NewSubtreeFromBytes(subtreeBytes)
	if err != nil {
		return nil, errors.NewProcessingError("error in NewSubtreeFromBytes", err)
	}

	return subtree, nil
}

// GetSubtreeHead returns the head of the subtree, which only includes the Fees and SizeInBytes
func (repo *Repository) GetSubtreeHead(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, int, error) {
	repo.logger.Debugf("[Repository] GetSubtree: %s", hash.String())

	subtreeBytes, err := repo.SubtreeStore.GetHead(ctx, hash.CloneBytes(), 56, options.WithFileExtension("subtree"))
	if err != nil {
		return nil, 0, errors.NewServiceError("error in GetSubtree GetHead method: %w", err)
	}

	if len(subtreeBytes) != 56 {
		return nil, 0, errors.ErrNotFound
	}

	subtree := &util.Subtree{}
	buf := bytes.NewBuffer(subtreeBytes)

	// read root hash
	_, err = chainhash.NewHash(buf.Next(32))
	if err != nil {
		return nil, 0, errors.NewProcessingError("unable to read root hash", err)
	}

	// read fees
	subtree.Fees = binary.LittleEndian.Uint64(buf.Next(8))

	// read sizeInBytes
	subtree.SizeInBytes = binary.LittleEndian.Uint64(buf.Next(8))

	// read number of leaves
	numNodes := binary.LittleEndian.Uint64(buf.Next(8))

	return subtree, int(numNodes), nil // nolint:gosec
}

func (repo *Repository) GetUtxoBytes(ctx context.Context, spend *utxo.Spend) ([]byte, error) {
	resp, err := repo.GetUtxo(ctx, spend)
	if err != nil {
		return nil, err
	}

	return resp.SpendingTxID.CloneBytes(), nil
}

func (repo *Repository) GetUtxo(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	repo.logger.Debugf("[Repository] GetUtxo: %s", spend.UTXOHash.String())

	resp, err := repo.UtxoStore.GetSpend(ctx, spend)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (repo *Repository) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	repo.logger.Debugf("[Repository] GetBestBlockHeader")

	header, meta, err := repo.BlockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, nil, err
	}

	return header, meta, nil
}

func (repo *Repository) GetBalance(ctx context.Context) (uint64, uint64, error) {
	if repo.CoinbaseProvider == nil {
		return 0, 0, nil
	}

	res, err := repo.CoinbaseProvider.GetBalance(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, 0, err
	}

	return res.NumberOfUtxos, res.TotalSatoshis, nil
}
