// Package repository provides blockchain data access across multiple storage backends.
package repository

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/semaphore"
)

// Interface defines blockchain data repository operations.
// It abstracts access to blocks, transactions, subtrees, and UTXO data across
// multiple storage backends (UTXO store, transaction store, subtree store, and
// block persister store).
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
	GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlockHeadersFromCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error)
	GetSubtreeBytes(ctx context.Context, hash *chainhash.Hash) ([]byte, error)
	GetSubtreeTxIDsReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error)
	GetSubtreeDataReaderFromBlockPersister(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error)
	GetSubtreeDataReader(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error)
	GetSubtree(ctx context.Context, hash *chainhash.Hash) (*subtree.Subtree, error)
	GetSubtreeData(ctx context.Context, hash *chainhash.Hash) (*subtree.Data, error)
	GetSubtreeTransactions(ctx context.Context, hash *chainhash.Hash) (map[chainhash.Hash]*bt.Tx, error)
	GetSubtreeExists(ctx context.Context, hash *chainhash.Hash) (bool, error)
	GetSubtreeHead(ctx context.Context, hash *chainhash.Hash) (*subtree.Subtree, int, error)
	FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash) ([]uint32, []uint32, []int, error)
	GetUtxo(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error)
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetLegacyBlockReader(ctx context.Context, hash *chainhash.Hash, wireBlock ...bool) (*io.PipeReader, error)
	GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, height uint32) ([]*chainhash.Hash, error)
	GetBlockByID(ctx context.Context, id uint64) (*model.Block, error)
	GetBlockchainClient() blockchain.ClientI
	GetBlockvalidationClient() blockvalidation.Interface
	GetP2PClient() p2p.ClientI
}

// Repository implements blockchain data access across multiple storage backends.
// It coordinates between UTXO, transaction, subtree, and block stores to provide
// a unified data access layer.
//
// Thread-safe: delegates to thread-safe store implementations.
type Repository struct {
	logger                ulogger.Logger
	settings              *settings.Settings
	UtxoStore             utxo.Store
	TxStore               blob.Store
	SubtreeStore          blob.Store
	BlockPersisterStore   blob.Store
	BlockchainClient      blockchain.ClientI
	BlockvalidationClient blockvalidation.Interface
	P2PClient             p2p.ClientI

	// Per-method concurrency semaphores (nil = unlimited)
	semGetTransaction         *semaphore.Weighted
	semGetTransactionMeta     *semaphore.Weighted
	semGetSubtreeData         *semaphore.Weighted
	semGetSubtreeDataReader   *semaphore.Weighted
	semGetSubtreeTransactions *semaphore.Weighted
	semGetSubtreeExists       *semaphore.Weighted
	semGetSubtreeHead         *semaphore.Weighted
	semGetUtxo                *semaphore.Weighted
	semGetLegacyBlockReader   *semaphore.Weighted
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
	blockchainClient blockchain.ClientI, blockvalidationClient blockvalidation.Interface, subtreeStore blob.Store,
	blockPersisterStore blob.Store, p2pClient p2p.ClientI) (*Repository, error) {

	repo := &Repository{
		logger:                logger,
		settings:              tSettings,
		BlockchainClient:      blockchainClient,
		BlockvalidationClient: blockvalidationClient,
		UtxoStore:             utxoStore,
		TxStore:               txStore,
		SubtreeStore:          subtreeStore,
		BlockPersisterStore:   blockPersisterStore,
		P2PClient:             p2pClient,
	}

	// Initialize per-method semaphores
	// 0 = unlimited, -1 = runtime.NumCPU(), >0 = exact limit
	initSemaphore := func(c int, name string) *semaphore.Weighted {
		if c == 0 {
			return nil // Unlimited
		}
		if c == -1 {
			c = runtime.NumCPU()
		}
		if c > 0 {
			logger.Infof("[Repository] %s semaphore: %d", name, c)
			return semaphore.NewWeighted(int64(c))
		}
		return nil
	}

	repo.semGetTransaction = initSemaphore(tSettings.Asset.ConcurrencyGetTransaction, "GetTransaction")
	repo.semGetTransactionMeta = initSemaphore(tSettings.Asset.ConcurrencyGetTransactionMeta, "GetTransactionMeta")
	repo.semGetSubtreeData = initSemaphore(tSettings.Asset.ConcurrencyGetSubtreeData, "GetSubtreeData")
	repo.semGetSubtreeDataReader = initSemaphore(tSettings.Asset.ConcurrencyGetSubtreeDataReader, "GetSubtreeDataReader")
	repo.semGetSubtreeTransactions = initSemaphore(tSettings.Asset.ConcurrencyGetSubtreeTransactions, "GetSubtreeTransactions")
	repo.semGetSubtreeExists = initSemaphore(tSettings.Asset.ConcurrencyGetSubtreeExists, "GetSubtreeExists")
	repo.semGetSubtreeHead = initSemaphore(tSettings.Asset.ConcurrencyGetSubtreeHead, "GetSubtreeHead")
	repo.semGetUtxo = initSemaphore(tSettings.Asset.ConcurrencyGetUtxo, "GetUtxo")
	repo.semGetLegacyBlockReader = initSemaphore(tSettings.Asset.ConcurrencyGetLegacyBlockReader, "GetLegacyBlockReader")

	return repo, nil
}

// acquireSemaphorePermit attempts to acquire a permit from the given semaphore.
// If sem is nil (unlimited concurrency), returns immediately without error.
// Respects the parent context's deadline; adds a 30-second fallback timeout only if no deadline is set.
func acquireSemaphorePermit(ctx context.Context, sem *semaphore.Weighted, methodName string) error {
	if sem == nil {
		return nil // Unlimited concurrency
	}

	// Only add a fallback timeout if the parent context has no deadline
	acquireCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		acquireCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	if err := sem.Acquire(acquireCtx, 1); err != nil {
		if errors.Is(err, context.Canceled) {
			return errors.NewContextCanceledError(
				fmt.Sprintf("[Repository:%s] operation canceled while waiting for semaphore permit", methodName), err)
		} else if errors.Is(err, context.DeadlineExceeded) {
			return errors.NewServiceUnavailableError(
				fmt.Sprintf("[Repository:%s] operation timed out waiting for semaphore permit", methodName))
		}
		return errors.NewProcessingError(
			fmt.Sprintf("[Repository:%s] failed to acquire semaphore permit", methodName), err)
	}
	return nil
}

// releaseSemaphorePermit releases a permit back to the semaphore.
// If sem is nil (unlimited concurrency), does nothing.
func releaseSemaphorePermit(sem *semaphore.Weighted) {
	if sem != nil {
		sem.Release(1)
	}
}

// Health performs health checks on the repository and its dependencies.
// This method implements the standard Teranode health check protocol, supporting both
// liveness and readiness probes. Liveness checks verify that the repository service is
// running and responsive, while readiness checks also verify that all dependencies
// are available and operational.
//
// The health check behavior:
// - For liveness checks: Verifies basic repository operation without checking dependent services
// - For readiness checks: Performs comprehensive verification of all connected stores and services
//
// This method follows the Kubernetes health check pattern, returning appropriate HTTP status
// codes and descriptive messages to indicate the current operational state.
//
// Parameters:
//   - ctx: Context for the health check operation, allowing for cancellation and timeouts
//   - checkLiveness: When true, performs liveness check only; when false, performs full readiness check
//
// Returns:
//   - int: HTTP status code indicating health status (200 for healthy, 503 for unhealthy)
//   - string: Human-readable description of the health status with details about any issues
//   - error: Any error encountered during health checking that prevented proper evaluation
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

// GetTxMeta retrieves transaction metadata by its hash from the UTXO store.
// This method provides access to transaction metadata including block height,
// confirmation status, and other associated information stored in the UTXO store.
//
// The function delegates directly to the UTXO store's Get method, which returns
// comprehensive metadata about the transaction if it exists in the store.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - hash: Hash of the transaction for which to retrieve metadata
//
// Returns:
//   - *meta.Data: Transaction metadata containing block height, confirmation details, and other information
//   - error: Any error encountered during metadata retrieval, including transaction not found errors
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
	if err := acquireSemaphorePermit(ctx, repo.semGetTransaction, "GetTransaction"); err != nil {
		return nil, err
	}
	defer releaseSemaphorePermit(repo.semGetTransaction)

	repo.logger.Debugf("[Repository] GetTransaction: %s", hash.String())

	txMeta, err := repo.UtxoStore.Get(ctx, hash)
	if err == nil && txMeta != nil {
		return txMeta.Tx.ExtendedBytes(), nil
	}

	repo.logger.Warnf("[Repository] GetTransaction: %s not found in txmeta store: %v", hash.String(), err)

	tx, err := repo.TxStore.Get(ctx, hash.CloneBytes(), fileformat.FileTypeTx)
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
	if err := acquireSemaphorePermit(ctx, repo.semGetTransactionMeta, "GetTransactionMeta"); err != nil {
		return nil, err
	}
	defer releaseSemaphorePermit(repo.semGetTransactionMeta)

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

// GetBlockByID retrieves a block by its ID.
//
// Parameters:
//   - ctx: Context for the operation
//   - id: The ID of the block to retrieve
//
// Returns:
//   - *model.Block: The retrieved block
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	repo.logger.Debugf("[Repository] GetBlockByID: %d", id)

	block, err := repo.BlockchainClient.GetBlockByID(ctx, id)
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

// GetBlockHeadersToCommonAncestor retrieves block headers from a target hash back to a common ancestor.
// This method is used in blockchain synchronization to find the point where two chains diverge
// and retrieve the headers needed to bring a client up to date with the main chain.
//
// The function uses block locator hashes to efficiently find the common ancestor between
// the target block and the client's current chain state, then returns headers from that
// point forward up to the specified maximum number of headers.
//
// This is a critical operation for peer-to-peer synchronization and chain reorganization
// handling, allowing nodes to efficiently discover and download missing block headers.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - hashTarget: Target block hash to work backwards from
//   - blockLocatorHashes: Array of block hashes representing the client's current chain state
//   - maxHeaders: Maximum number of headers to return to prevent excessive response sizes
//
// Returns:
//   - []*model.BlockHeader: Array of block headers from common ancestor to target
//   - []*model.BlockHeaderMeta: Array of corresponding block header metadata
//   - error: Any error encountered during header retrieval or common ancestor detection
func (repo *Repository) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return repo.BlockchainClient.GetBlockHeadersToCommonAncestor(ctx, hashTarget, blockLocatorHashes, maxHeaders)
}

// GetBlockHeadersFromCommonAncestor retrieves block headers from a common ancestor to a target hash (chain tip).
// This method is used in blockchain synchronization to find the point where two chains diverge
// and retrieve the headers needed to bring a client up to date with the main chain.
//
// The function uses block locator hashes to efficiently find the common ancestor between
// the target block and the client's current chain state, then returns headers from that
// point forward up to the specified maximum number of headers.
//
// This is a critical operation for peer-to-peer synchronization and chain reorganization
// handling, allowing nodes to efficiently discover and download missing block headers.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - hashTarget: Target block hash to work backwards from
//   - blockLocatorHashes: Array of block hashes representing the client's current chain state
//   - maxHeaders: Maximum number of headers to return to prevent excessive response sizes
//
// Returns:
//   - []*model.BlockHeader: Array of block headers from common ancestor to target
//   - []*model.BlockHeaderMeta: Array of corresponding block header metadata
//   - error: Any error encountered during header retrieval or common ancestor detection
func (repo *Repository) GetBlockHeadersFromCommonAncestor(ctx context.Context, chainTipHash *chainhash.Hash, blockLocatorHashes []chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return repo.BlockchainClient.GetBlockHeadersFromCommonAncestor(ctx, chainTipHash, blockLocatorHashes, maxHeaders)
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

// GetBlocksByHeight retrieves full blocks within a specified height range.
// This method provides an efficient way to fetch complete blocks including
// headers, subtrees, and transaction metadata for a range of consecutive blocks.
// It's particularly optimized for operations like subtree searching where
// multiple blocks need to be examined for specific subtree hashes.
//
// Parameters:
//   - ctx: Context for the operation
//   - startHeight: Starting block height (inclusive)
//   - endHeight: Ending block height (inclusive)
//
// Returns:
//   - []*model.Block: Array of complete blocks in ascending height order
//   - error: Any error encountered during retrieval
func (repo *Repository) GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error) {
	repo.logger.Debugf("[Repository] GetBlocksByHeight: %d-%d", startHeight, endHeight)

	blocks, err := repo.BlockchainClient.GetBlocksByHeight(ctx, startHeight, endHeight)
	if err != nil {
		return nil, err
	}

	return blocks, nil
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
	subtreeReader, err := repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), fileformat.FileTypeSubtree)
	if err != nil {
		subtreeReader, err = repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		_ = subtreeReader.Close()
	}()

	var subtreeBytes []byte

	subtreeBytes, err = io.ReadAll(subtreeReader)
	if err != nil {
		return nil, errors.NewServiceError("error reading subtree bytes", err)
	}

	return subtreeBytes, nil
}

// GetSubtreeTxIDsReader provides a reader interface for accessing subtree data.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the subtree
//
// Returns:
//   - io.ReadCloser: Reader for subtree data
//   - error: Any error encountered during retrieval
func (repo *Repository) GetSubtreeTxIDsReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	reader, err := repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), fileformat.FileTypeSubtree)
	if err != nil {
		reader, err = repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return nil, err
		}
	}

	return reader, nil
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
	return repo.BlockPersisterStore.GetIoReader(ctx, hash.CloneBytes(), fileformat.FileTypeSubtreeData)
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
func (repo *Repository) GetSubtree(ctx context.Context, hash *chainhash.Hash) (*subtree.Subtree, error) {
	ctx, _, _ = tracing.Tracer("repository").Start(ctx, "GetSubtree",
		tracing.WithLogMessage(repo.logger, "[Repository] GetSubtree: %s", hash.String()),
	)

	subtreeReader, err := repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), fileformat.FileTypeSubtree)
	if err != nil {
		subtreeReader, err = repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return nil, errors.NewServiceError("error in GetSubtree Get method", err)
		}
	}

	defer func() {
		_ = subtreeReader.Close()
	}()

	subtree, err := subtree.NewSubtreeFromReader(subtreeReader)
	if err != nil {
		return nil, errors.NewProcessingError("error in NewSubtreeFromBytes", err)
	}

	return subtree, nil
}

// GetSubtreeData retrieves and deserializes a complete subtree data structure.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the subtree
//
// Returns:
//   - *util.SubtreeData: Deserialized subtree data structure
//   - error: Any error encountered during retrieval
func (repo *Repository) GetSubtreeData(ctx context.Context, hash *chainhash.Hash) (*subtree.Data, error) {
	if err := acquireSemaphorePermit(ctx, repo.semGetSubtreeData, "GetSubtreeData"); err != nil {
		return nil, err
	}
	defer releaseSemaphorePermit(repo.semGetSubtreeData)

	return repo.getSubtreeDataInternal(ctx, hash)
}

// getSubtreeDataInternal contains the core logic for GetSubtreeData without semaphore protection.
// This is used internally to avoid nested semaphore acquisition.
func (repo *Repository) getSubtreeDataInternal(ctx context.Context, hash *chainhash.Hash) (*subtree.Data, error) {
	ctx, _, _ = tracing.Tracer("repository").Start(ctx, "GetSubtreeData",
		tracing.WithLogMessage(repo.logger, "[Repository] GetSubtreeData: %s", hash.String()),
	)

	st, err := repo.GetSubtree(ctx, hash)
	if err != nil {
		return nil, err
	}

	r, err := repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), fileformat.FileTypeSubtreeData)
	if err != nil {
		return nil, errors.NewServiceError("[GetSubtreeData][%s] error in GetSubtreeData Get method", hash.String(), err)
	}

	defer func() {
		if err = r.Close(); err != nil {
			repo.logger.Errorf("[GetSubtreeData][%s] failed to close subtree data reader: %s", hash.String(), err.Error())
		}
	}()

	// read all transactions from the subtree data file
	subtreeData, err := subtree.NewSubtreeDataFromReader(st, r)
	if err != nil {
		return nil, errors.NewProcessingError("[GetSubtreeData][%s] error in NewSubtreeDataFromReader", hash.String(), err)
	}

	return subtreeData, nil
}

func (repo *Repository) GetSubtreeTransactions(ctx context.Context, hash *chainhash.Hash) (map[chainhash.Hash]*bt.Tx, error) {
	if err := acquireSemaphorePermit(ctx, repo.semGetSubtreeTransactions, "GetSubtreeTransactions"); err != nil {
		return nil, err
	}
	defer releaseSemaphorePermit(repo.semGetSubtreeTransactions)

	ctx, _, _ = tracing.Tracer("repository").Start(ctx, "GetSubtreeTransactions",
		tracing.WithLogMessage(repo.logger, "[Repository] GetSubtreeTransactions: %s", hash.String()),
	)

	// Call internal method to avoid nested semaphore acquisition
	subtreeData, err := repo.getSubtreeDataInternal(ctx, hash)
	if err != nil {
		// always return an empty map if no transactions are found
		return make(map[chainhash.Hash]*bt.Tx), err
	}

	if subtreeData == nil || len(subtreeData.Txs) == 0 {
		// always return an empty map if no transactions are found
		return make(map[chainhash.Hash]*bt.Tx), errors.ErrNotFound
	}

	transactionMap := make(map[chainhash.Hash]*bt.Tx, len(subtreeData.Txs))

	for _, tx := range subtreeData.Txs {
		transactionMap[*tx.TxIDChainHash()] = tx
	}

	return transactionMap, nil
}

// GetSubtreeExists checks whether a subtree exists in the subtree store.
// This method provides a lightweight way to verify subtree existence without
// retrieving the actual subtree data, which is useful for validation and
// conditional processing logic.
//
// The function delegates to the subtree store's Exists method, checking for
// the presence of a subtree file with the specified hash and file type.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - hash: Hash of the subtree to check for existence
//
// Returns:
//   - bool: True if the subtree exists in the store, false otherwise
//   - error: Any error encountered during the existence check
func (repo *Repository) GetSubtreeExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	if err := acquireSemaphorePermit(ctx, repo.semGetSubtreeExists, "GetSubtreeExists"); err != nil {
		return false, err
	}
	defer releaseSemaphorePermit(repo.semGetSubtreeExists)

	if exists, err := repo.SubtreeStore.Exists(ctx, hash.CloneBytes(), fileformat.FileTypeSubtree); err == nil {
		return exists, nil
	}

	return repo.SubtreeStore.Exists(ctx, hash.CloneBytes(), fileformat.FileTypeSubtreeToCheck)
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
func (repo *Repository) GetSubtreeHead(ctx context.Context, hash *chainhash.Hash) (*subtree.Subtree, int, error) {
	if err := acquireSemaphorePermit(ctx, repo.semGetSubtreeHead, "GetSubtreeHead"); err != nil {
		return nil, 0, err
	}
	defer releaseSemaphorePermit(repo.semGetSubtreeHead)

	repo.logger.Debugf("[Repository] GetSubtree: %s", hash.String())

	subtreeReader, err := repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), fileformat.FileTypeSubtree)
	if err != nil {
		subtreeReader, err = repo.SubtreeStore.GetIoReader(ctx, hash.CloneBytes(), fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return nil, 0, errors.NewServiceError("error in GetSubtree GetHead method", err)
		}
	}

	defer func() {
		_ = subtreeReader.Close()
	}()

	var subtreeBytes [56]byte

	n, err := io.ReadFull(subtreeReader, subtreeBytes[:])
	if err != nil {
		return nil, 0, errors.NewServiceError("error reading subtree head bytes", err)
	}

	if n != 56 {
		return nil, 0, errors.ErrNotFound
	}

	subtree := &subtree.Subtree{}
	buf := bytes.NewBuffer(subtreeBytes[:])

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

	numNodesInt, err := safeconversion.Uint64ToInt(numNodes)
	if err != nil {
		return nil, 0, err
	}

	return subtree, numNodesInt, nil
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
	if err := acquireSemaphorePermit(ctx, repo.semGetUtxo, "GetUtxo"); err != nil {
		return nil, err
	}
	defer releaseSemaphorePermit(repo.semGetUtxo)

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

// GetBlockchainClient returns the blockchain client interface used by the repository.
//
// Returns:
//   - *blockchain.ClientI: Blockchain client interface
func (repo *Repository) GetBlockchainClient() blockchain.ClientI {
	return repo.BlockchainClient
}

// GetBlockvalidationClient returns the block validation client interface used by the repository.
//
// Returns:
//   - blockvalidation.Interface: Block validation client interface
func (repo *Repository) GetBlockvalidationClient() blockvalidation.Interface {
	return repo.BlockvalidationClient
}

// GetP2PClient returns the P2P client interface used by the repository.
//
// Returns:
//   - p2p.ClientI: P2P client interface
func (repo *Repository) GetP2PClient() p2p.ClientI {
	return repo.P2PClient
}
