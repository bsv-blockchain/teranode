// Package repository provides access to blockchain data storage and retrieval operations.
// It implements the necessary interfaces to interact with various data stores and
// blockchain clients.
package repository

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Interface interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	GetTxMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error)
	GetTransaction(ctx context.Context, hash *chainhash.Hash) ([]byte, error)
	GetBlockStats(ctx context.Context) (*model.BlockStats, error)
	GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error)
	GetTransactionMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error)
	GetBlockByHash(ctx context.Context, hash *chainhash.Hash) (*model.Block, error)
	GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error)
	GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error)
	GetBlocks(ctx context.Context, hash *chainhash.Hash, n uint32) ([]*model.Block, error)
	GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetSubtreeBytes(ctx context.Context, hash *chainhash.Hash) ([]byte, error)
	GetSubtreeTxIDsReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error)
	GetSubtreeDataReaderFromBlockPersister(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error)
	GetSubtreeDataReader(ctx context.Context, subtreeHash *chainhash.Hash) (*io.PipeReader, error)
	GetSubtree(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, error)
	GetSubtreeExists(ctx context.Context, hash *chainhash.Hash) (bool, error)
	GetSubtreeHead(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, int, error)
	GetUtxoBytes(ctx context.Context, spend *utxo.Spend) ([]byte, error)
	GetUtxo(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error)
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetLegacyBlockReader(ctx context.Context, hash *chainhash.Hash, wireBlock ...bool) (*io.PipeReader, error)
	GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, height uint32) ([]*chainhash.Hash, error)
}

// Repository provides access to blockchain data storage and retrieval operations.
// It implements the necessary interfaces to interact with various data stores and
// blockchain clients.
type Repository struct {
	logger              ulogger.Logger
	settings            *settings.Settings
	UtxoStore           utxo.Store
	TxStore             blob.Store
	SubtreeStore        blob.Store
	BlockPersisterStore blob.Store
	BlockchainClient    blockchain.ClientI
}

// NewRepository creates a new Repository instance with the provided dependencies.
// It initializes connections to various data stores and sets up the coinbase provider
// if configured.
//
// Parameters:
//   - logger: Logger instance for repository operations
//   - utxoStore: Store for UTXO data
//   - txStore: Store for transaction data
//   - blockchainClient: Client interface for blockchain operations
//   - subtreeStore: Store for subtree data
//   - blockPersisterStore: Store for block persistence
//
// Returns:
//   - *Repository: Newly created repository instance
//   - error: Any error encountered during creation
func NewRepository(logger ulogger.Logger, tSettings *settings.Settings, utxoStore utxo.Store, txStore blob.Store,
	blockchainClient blockchain.ClientI, subtreeStore blob.Store, blockPersisterStore blob.Store) (*Repository, error) {

	return &Repository{
		logger:              logger,
		settings:            tSettings,
		BlockchainClient:    blockchainClient,
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
	checks := make([]health.Check, 0, 5)

	if repo.BlockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: repo.BlockchainClient.Health})
	}

	if repo.UtxoStore != nil {
		checks = append(checks, health.Check{Name: "UtxoStore", Check: repo.UtxoStore.Health})
	}

	if repo.TxStore != nil {
		checks = append(checks, health.Check{Name: "TxStore", Check: repo.TxStore.Health})
	}

	if repo.SubtreeStore != nil {
		checks = append(checks, health.Check{Name: "SubtreeStore", Check: repo.SubtreeStore.Health})
	}

	if repo.BlockPersisterStore != nil {
		checks = append(checks, health.Check{Name: "BlockPersisterStore", Check: repo.BlockPersisterStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (repo *Repository) GetTxMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return repo.UtxoStore.Get(ctx, hash)
}

// GetTransaction retrieves transaction data by its hash.
// It first checks the UTXO store, then falls back to the transaction store if needed.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the transaction to retrieve
//
// Returns:
//   - []byte: Transaction data
//   - error: Any error encountered during retrieval
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

// GetBlockStats retrieves statistical information about the blockchain blocks.
// It delegates to the blockchain client to fetch the statistics.
//
// Parameters:
//   - ctx: Context for the operation
//
// Returns:
//   - *model.BlockStats: Block statistics data
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	return repo.BlockchainClient.GetBlockStats(ctx)
}

// GetBlockGraphData retrieves time-series data points for block metrics.
// The data is aggregated based on the specified time period.
//
// Parameters:
//   - ctx: Context for the operation
//   - periodMillis: Time period in milliseconds for data aggregation
//
// Returns:
//   - *model.BlockDataPoints: Block data points over time
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	return repo.BlockchainClient.GetBlockGraphData(ctx, periodMillis)
}

// GetTransactionMeta retrieves metadata for a transaction by its hash.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the transaction
//
// Returns:
//   - *meta.Data: Transaction metadata
//   - error: Any error encountered during retrieval
func (repo *Repository) GetTransactionMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	repo.logger.Debugf("[Repository] GetTransaction: %s", hash.String())

	txMeta, err := repo.UtxoStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	return txMeta, nil
}

// GetBlockByHash retrieves a block by its hash.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the block to retrieve
//
// Returns:
//   - *model.Block: Block data
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlockByHash(ctx context.Context, hash *chainhash.Hash) (*model.Block, error) {
	repo.logger.Debugf("[Repository] GetBlockByHash: %s", hash.String())

	block, err := repo.BlockchainClient.GetBlock(ctx, hash)
	if err != nil {
		return nil, err
	}

	return block, nil
}

// GetBlockByHeight retrieves a block by its height in the blockchain.
//
// Parameters:
//   - ctx: Context for the operation
//   - height: Block height
//
// Returns:
//   - *model.Block: Block data
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	repo.logger.Debugf("[Repository] GetBlockByHeight: %d", height)

	block, err := repo.BlockchainClient.GetBlockByHeight(ctx, height)
	if err != nil {
		return nil, err
	}

	return block, nil
}

// GetBlockHeader retrieves a block header and its metadata by block hash.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the block
//
// Returns:
//   - *model.BlockHeader: Block header data
//   - *model.BlockHeaderMeta: Block header metadata
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	repo.logger.Debugf("[Repository] GetBlockHeader: %s", hash.String())

	blockHeader, blockHeaderMeta, err := repo.BlockchainClient.GetBlockHeader(ctx, hash)
	if err != nil {
		return nil, nil, err
	}

	return blockHeader, blockHeaderMeta, nil
}

// GetLastNBlocks retrieves the most recent N blocks from the blockchain.
//
// Parameters:
//   - ctx: Context for the operation
//   - n: Number of blocks to retrieve
//   - includeOrphans: Whether to include orphaned blocks
//   - fromHeight: Starting height for block retrieval
//
// Returns:
//   - []*model.BlockInfo: Array of block information
//   - error: Any error encountered during retrieval
func (repo *Repository) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	repo.logger.Debugf("[Repository] GetLastNBlocks: %d", n)

	blockInfo, err := repo.BlockchainClient.GetLastNBlocks(ctx, n, includeOrphans, fromHeight)
	if err != nil {
		return nil, err
	}

	return blockInfo, nil
}

// GetBlocks retrieves a sequence of blocks starting from a specific block hash.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Starting block hash
//   - n: Number of blocks to retrieve
//
// Returns:
//   - []*model.Block: Array of blocks
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlocks(ctx context.Context, hash *chainhash.Hash, n uint32) ([]*model.Block, error) {
	repo.logger.Debugf("[Repository] GetNBlocks: %d", n)

	blocks, err := repo.BlockchainClient.GetBlocks(ctx, hash, n)
	if err != nil {
		return nil, err
	}

	return blocks, nil
}

// GetBlockHeaders retrieves a sequence of block headers starting from a specific hash.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Starting block hash
//   - numberOfHeaders: Number of headers to retrieve
//
// Returns:
//   - []*model.BlockHeader: Array of block headers
//   - []*model.BlockHeaderMeta: Array of block header metadata
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	repo.logger.Debugf("[Repository] GetBlockHeaders: %s", hash.String())

	blockHeaders, blockHeaderMetas, err := repo.BlockchainClient.GetBlockHeaders(ctx, hash, numberOfHeaders)
	if err != nil {
		return nil, nil, err
	}

	return blockHeaders, blockHeaderMetas, nil
}

func (repo *Repository) GetBlockHeadersToCommonAncestor(ctx context.Context, hasTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return repo.BlockchainClient.GetBlockHeadersToCommonAncestor(ctx, hasTarget, blockLocatorHashes)
}

// GetBlockHeadersFromHeight retrieves block headers starting from a specific height.
//
// Parameters:
//   - ctx: Context for the operation
//   - height: Starting block height
//   - limit: Maximum number of headers to retrieve
//
// Returns:
//   - []*model.BlockHeader: Array of block headers
//   - []*model.BlockHeaderMeta: Array of block header metadata
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	repo.logger.Debugf("[Repository] GetBlockHeadersFromHeight: %d-%d", height, limit)

	blockHeaders, metas, err := repo.BlockchainClient.GetBlockHeadersFromHeight(ctx, height, limit)
	if err != nil {
		return nil, nil, err
	}

	return blockHeaders, metas, nil
}

// GetSubtreeBytes retrieves the raw bytes of a subtree.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the subtree
//
// Returns:
//   - []byte: Subtree data
//   - error: Any error encountered during retrieval
func (repo *Repository) GetSubtreeBytes(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	subtreeBytes, err := repo.SubtreeStore.Get(ctx, hash.CloneBytes(), options.WithFileExtension("subtree"))
	if err != nil {
		return nil, err
	}

	return subtreeBytes, nil
}

// GetSubtreeReader provides a reader interface for accessing subtree data.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the subtree
//
// Returns:
//   - io.ReadCloser: Reader for subtree data
//   - error: Any error encountered during retrieval
func (repo *Repository) GetSubtreeTxIDsReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	return repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), options.WithFileExtension("subtree"))
}

// GetSubtreeDataReaderFromBlockPersister provides a reader interface for accessing subtree data from block persister.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the subtree
//
// Returns:
//   - io.ReadCloser: Reader for subtree data
//   - error: Any error encountered during retrieval
func (repo *Repository) GetSubtreeDataReaderFromBlockPersister(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	return repo.BlockPersisterStore.GetIoReader(ctx, hash.CloneBytes(), options.WithFileExtension("subtreeData"))
}

// GetSubtree retrieves and deserializes a complete subtree structure.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the subtree
//
// Returns:
//   - *util.Subtree: Deserialized subtree structure
//   - error: Any error encountered during retrieval
func (repo *Repository) GetSubtree(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, error) {
	ctx, _, _ = tracing.StartTracing(ctx, "Repository:GetSubtree",
		tracing.WithLogMessage(repo.logger, "[Repository] GetSubtree: %s", hash.String()),
	)

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

func (repo *Repository) GetSubtreeExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	return repo.SubtreeStore.Exists(ctx, hash.CloneBytes(), options.WithFileExtension("subtree"))
}

// GetSubtreeHead retrieves only the head portion of a subtree, containing fees and size information.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the subtree
//
// Returns:
//   - *util.Subtree: Partial subtree containing head information
//   - int: Number of nodes in the subtree
//   - error: Any error encountered during retrieval
func (repo *Repository) GetSubtreeHead(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, int, error) {
	repo.logger.Debugf("[Repository] GetSubtree: %s", hash.String())

	subtreeBytes, err := repo.SubtreeStore.GetHead(ctx, hash.CloneBytes(), 56, options.WithFileExtension("subtree"))
	if err != nil {
		return nil, 0, errors.NewServiceError("error in GetSubtree GetHead method", err)
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

	numNodesInt, err := util.SafeUint64ToInt(numNodes)
	if err != nil {
		return nil, 0, err
	}

	return subtree, numNodesInt, nil
}

// GetUtxoBytes retrieves the spending transaction ID for a specific UTXO.
//
// Parameters:
//   - ctx: Context for the operation
//   - spend: UTXO spend information
//
// Returns:
//   - []byte: Spending transaction ID bytes
//   - error: Any error encountered during retrieval
func (repo *Repository) GetUtxoBytes(ctx context.Context, spend *utxo.Spend) ([]byte, error) {
	resp, err := repo.GetUtxo(ctx, spend)
	if err != nil {
		return nil, err
	}

	return resp.SpendingTxID.CloneBytes(), nil
}

// GetUtxo retrieves detailed spend information for a specific UTXO.
//
// Parameters:
//   - ctx: Context for the operation
//   - spend: UTXO spend information
//
// Returns:
//   - *utxo.SpendResponse: Detailed spend information
//   - error: Any error encountered during retrieval
func (repo *Repository) GetUtxo(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	repo.logger.Debugf("[Repository] GetUtxo: %s", spend.UTXOHash.String())

	resp, err := repo.UtxoStore.GetSpend(ctx, spend)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// GetBestBlockHeader retrieves the header of the current best block in the blockchain.
//
// Parameters:
//   - ctx: Context for the operation
//
// Returns:
//   - *model.BlockHeader: Best block header
//   - *model.BlockHeaderMeta: Best block header metadata
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	repo.logger.Debugf("[Repository] GetBestBlockHeader")

	header, meta, err := repo.BlockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, nil, err
	}

	return header, meta, nil
}

// GetBlockLocator retrieves a sequence of block hashes at exponentially increasing distances
// back from the provided block hash or the best block if no hash is specified.
//
// Parameters:
//   - ctx: Context for the operation
//   - blockHeaderHash: Starting block hash (nil for best block)
//   - height: Block height to start from
//
// Returns:
//   - []*chainhash.Hash: Array of block hashes forming the locator
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, height uint32) ([]*chainhash.Hash, error) {
	repo.logger.Debugf("[Repository] GetBlockLocator from hash: %v", blockHeaderHash)

	locator, err := repo.BlockchainClient.GetBlockLocator(ctx, blockHeaderHash, height)
	if err != nil {
		return nil, err
	}

	return locator, nil
}
