// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
package sql

import (
	"context"
	"database/sql"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/work"
	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/lib/pq"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"modernc.org/sqlite"
)

// StoreBlock persists a new block to the database and updates chain state.
// This implements the blockchain.Store.StoreBlock interface method.
//
// The method performs the following operations:
// - Stores the block data in the database with appropriate metadata
// - Calculates the cumulative chain work for the new block
// - Extracts miner information from the coinbase transaction if available
// - Updates the in-memory cache with the new block information
// - Resets response caches to ensure consistency
//
// Parameters:
//   - ctx: Context for the database operation, allows for cancellation and timeouts
//   - block: The complete block structure to be stored
//   - peerID: Identifier of the peer that provided this block
//   - opts: Optional parameters to modify storage behavior (e.g., minedSet, subtreesSet flags)
//
// Returns:
//   - uint64: The unique database ID assigned to the stored block
//   - uint32: The height of the block in the blockchain
//   - error: Any error encountered during storage operations
func (s *SQL) StoreBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) (uint64, uint32, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:StoreBlock")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	newBlockID, height, chainWork, err := s.storeBlock(ctx, block, peerID, opts...)
	if err != nil {
		return 0, height, err
	}

	var miner string

	if block.CoinbaseTx != nil && block.CoinbaseTx.OutputCount() != 0 {
		var err error

		miner, err = util.ExtractCoinbaseMiner(block.CoinbaseTx)
		if err != nil {
			s.logger.Errorf("error extracting mine from coinbase tx: %v", err)
		}
	}

	newBlockIDUint32, err := util.SafeUint64ToUint32(newBlockID)
	if err != nil {
		return 0, height, errors.NewProcessingError("failed to convert newBlockID", err)
	}

	timeUint32, err := util.SafeInt64ToUint32(time.Now().Unix())
	if err != nil {
		return 0, height, errors.NewProcessingError("failed to convert time", err)
	}

	meta := &model.BlockHeaderMeta{
		ID:          newBlockIDUint32,
		Height:      height,
		TxCount:     block.TransactionCount,
		SizeInBytes: block.SizeInBytes,
		Miner:       miner,
		ChainWork:   chainWork,
		BlockTime:   block.Header.Timestamp,
		Timestamp:   timeUint32,
	}

	ok := s.blocksCache.AddBlockHeader(block.Header, meta)
	if !ok {
		if err := s.ResetBlocksCache(ctx); err != nil {
			s.logger.Errorf("error clearing caches: %v", err)
		}
	}

	s.ResetResponseCache()

	return newBlockID, height, nil
}

// getPreviousBlockInfo retrieves essential information about a block's parent.
// The function first attempts to retrieve this information from the in-memory cache,
// falling back to the database if necessary.
//
// Parameters:
//   - ctx: Context for the database operation
//   - prevBlockHash: Hash of the previous (parent) block
//
// Returns:
//   - id: Database ID of the previous block
//   - chainWork: Cumulative proof-of-work for the previous block
//   - height: Blockchain height of the previous block
//   - invalid: Whether the previous block is marked as invalid
//   - err: Any error encountered during retrieval
func (s *SQL) getPreviousBlockInfo(ctx context.Context, prevBlockHash chainhash.Hash) (id uint64, chainWork []byte, height uint32, invalid bool, err error) {
	// Try to get previous block info from cache first
	prevHeader, prevMeta := s.blocksCache.GetBlockHeader(prevBlockHash)
	if prevHeader != nil && prevMeta != nil {
		id = uint64(prevMeta.ID)
		chainWork = prevMeta.ChainWork
		height = prevMeta.Height
		invalid = false // Assuming cache only stores valid chain info implicitly

		return id, chainWork, height, invalid, nil
	}

	// Fallback to DB if not in cache
	q := `
		SELECT
		 b.id
		,b.chain_work
		,b.height
		,b.invalid
		FROM blocks b
		WHERE b.hash = $1
	`
	err = s.db.QueryRowContext(ctx, q, prevBlockHash[:]).Scan(
		&id,
		&chainWork,
		&height,
		&invalid,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Wrap the error for context
			return 0, nil, 0, false, errors.NewStorageError("previous block %s not found", prevBlockHash.String(), err)
		}
		// Return other DB errors directly
		return 0, nil, 0, false, err
	}

	return id, chainWork, height, invalid, nil
}

// storeBlock is the internal implementation that performs the actual database operations
// to persist a block. It handles both genesis and regular blocks differently.
//
// The function performs the following key steps:
// - Retrieves and validates previous block data
// - Handles special case for genesis block
// - Calculates cumulative chain work
// - Prepares block data and executes appropriate SQL insert statement
// - Handles database-specific errors and conflicts
//
// Parameters:
//   - ctx: Context for the database operation
//   - block: The block structure to be stored
//   - peerID: Identifier of the peer that provided this block
//   - opts: Optional parameters to modify storage behavior
//
// Returns:
//   - uint64: The unique database ID assigned to the stored block
//   - uint32: The height of the block in the blockchain
//   - []byte: The calculated cumulative chain work for this block
//   - error: Any error encountered during the operation
func (s *SQL) storeBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) (uint64, uint32, []byte, error) {
	// Apply options
	storeBlockOptions := options.StoreBlockOptions{}
	for _, opt := range opts {
		opt(&storeBlockOptions)
	}

	var (
		coinbaseTxID string
		q            string
	)

	if block.CoinbaseTx != nil {
		coinbaseTxID = block.CoinbaseTx.TxID()
	}

	genesis, height, previousBlockID, previousChainWork, previousBlockInvalid, err := s.getPreviousBlockData(ctx, coinbaseTxID, block)
	if err != nil {
		return 0, 0, nil, err
	}

	if genesis {
		// genesis block
		q = `
INSERT INTO blocks (
	id
	,parent_id
	,version
	,hash
	,previous_hash
	,merkle_root
	,block_time
	,n_bits
	,nonce
	,height
	,chain_work
	,tx_count
	,size_in_bytes
	,subtree_count
	,subtrees
	,peer_id
	,coinbase_tx
	,invalid
	,mined_set
	,subtrees_set
) VALUES (0, $1, $2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, $16, $17, $18, $19)
RETURNING id
		`
	} else {
		q = `
INSERT INTO blocks (
	parent_id
	,version
	,hash
	,previous_hash
	,merkle_root
	,block_time
	,n_bits
	,nonce
	,height
	,chain_work
	,tx_count
	,size_in_bytes
	,subtree_count
	,subtrees
	,peer_id
	,coinbase_tx
	,invalid
	,mined_set
	,subtrees_set
) VALUES ($1, $2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, $16, $17, $18, $19)
RETURNING id
`
	}

	cumulativeChainWorkBytes, err := calculateAndPrepareChainWork(previousChainWork, block)
	if err != nil {
		return 0, 0, nil, err // Return error from calculation
	}

	subtreeBytes, err := block.SubTreeBytes()
	if err != nil {
		return 0, 0, nil, errors.NewStorageError("failed to get subtree bytes", err)
	}

	var coinbaseBytes []byte
	if block.CoinbaseTx != nil {
		coinbaseBytes = block.CoinbaseTx.Bytes()
	}

	var rows *sql.Rows

	rows, err = s.db.QueryContext(ctx, q,
		previousBlockID,
		block.Header.Version,
		block.Hash().CloneBytes(),
		block.Header.HashPrevBlock.CloneBytes(),
		block.Header.HashMerkleRoot.CloneBytes(),
		block.Header.Timestamp,
		block.Header.Bits.CloneBytes(),
		block.Header.Nonce,
		height,
		cumulativeChainWorkBytes,
		block.TransactionCount,
		block.SizeInBytes,
		len(block.Subtrees),
		subtreeBytes,
		peerID,
		coinbaseBytes,
		previousBlockInvalid,
		storeBlockOptions.MinedSet,
		storeBlockOptions.SubtreesSet,
	)

	if err != nil {
		return 0, 0, nil, s.parseSQLError(err, block)
	}

	defer rows.Close()

	rowFound := rows.Next()
	if !rowFound {
		return 0, 0, nil, errors.NewBlockExistsError("block already exists: %s", block.Hash())
	}

	var newBlockID uint64
	if err = rows.Scan(&newBlockID); err != nil {
		return 0, 0, nil, errors.NewStorageError("failed to scan new block id", err)
	}

	return newBlockID, height, cumulativeChainWorkBytes, nil
}

// parseSQLError unwraps and translates SQL-specific errors into domain-specific errors.
// This helper function detects database constraint violations from different SQL backends
// (PostgreSQL and SQLite) and converts them into appropriate application errors.
//
// Parameters:
//   - err: The original SQL error to parse
//   - block: The block being processed, used for error context
//
// Returns:
//   - error: A domain-specific error with appropriate context
func (*SQL) parseSQLError(err error, block *model.Block) error {
	// check whether this is a postgres exists constraint error
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" { // Duplicate constraint violation
		return errors.NewBlockExistsError("block already exists in the database: %s", block.Hash().String(), err)
	}

	// check whether this is a sqlite exists constraint error
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) && (sqliteErr.Code()&0xff) == SQLITE_CONSTRAINT {
		return errors.NewBlockExistsError("block already exists in the database: %s", block.Hash().String(), err)
	}

	// otherwise, return the generic error
	return errors.NewStorageError("failed to store block", err)
}

// getPreviousBlockData determines if this is a genesis block and retrieves information
// about the previous block necessary for storing a new block.
//
// The function performs special handling for the genesis block where previous values
// are initialized with defaults. For non-genesis blocks, it retrieves the previous
// block's data and validates the block height encoding in the coinbase transaction.
//
// Parameters:
//   - ctx: Context for database operations
//   - coinbaseTxID: Transaction ID of the coinbase transaction
//   - block: The block being processed
//
// Returns:
//   - genesis: Whether this is the genesis block
//   - height: Height of the current block
//   - previousBlockID: Database ID of the previous block
//   - previousChainWork: Cumulative proof-of-work for previous block
//   - previousBlockInvalid: Whether the previous block is marked as invalid
//   - err: Any error encountered during processing
func (s *SQL) getPreviousBlockData(
	ctx context.Context,
	coinbaseTxID string,
	block *model.Block,
) (
	genesis bool,
	height uint32,
	previousBlockID uint64,
	previousChainWork []byte,
	previousBlockInvalid bool,
	err error,
) {
	if coinbaseTxID == s.chainParams.GenesisBlock.Transactions[0].TxHash().String() {
		// genesis block
		genesis = true
		height = 0
		previousBlockID = 0
		previousChainWork = make([]byte, 32)
	} else {
		// Handle Non-Genesis Block
		var previousHeight uint32

		previousBlockID, previousChainWork, previousHeight, previousBlockInvalid, err = s.getPreviousBlockInfo(ctx, *block.Header.HashPrevBlock)
		if err != nil {
			// Check specifically for the ErrNoRows error from the database query
			if errors.Is(err, sql.ErrNoRows) {
				// Rewrap the error with context about the *current* block being stored
				return false, 0, 0, nil, false, errors.NewStorageError("error storing block %s: previous block %s not found", block.Hash().String(), block.Header.HashPrevBlock.String(), err)
			}
			// Return other errors from getPreviousBlockInfo (which might include its own StorageError wrapping)
			return false, 0, 0, nil, false, err
		}

		height = previousHeight + 1

		// BIP34 Coinbase Height Validation using the helper function
		if err := s.validateCoinbaseHeight(block, height); err != nil {
			return false, 0, 0, nil, false, err
		}
	}

	return genesis, height, previousBlockID, previousChainWork, previousBlockInvalid, nil
}

// calculateAndPrepareChainWork computes the cumulative proof-of-work for a new block.
// It converts the previous chain work bytes to a hash, calculates the new cumulative
// chain work by adding the block's work, and returns the result in byte format.
//
// Parameters:
//   - previousChainWorkBytes: The byte representation of the previous block's cumulative work
//   - block: The block being processed
//
// Returns:
//   - []byte: The byte representation of the new cumulative chain work
//   - error: Any error encountered during the calculation
func calculateAndPrepareChainWork(previousChainWorkBytes []byte, block *model.Block) ([]byte, error) {
	prevChainWorkHash, err := chainhash.NewHash(bt.ReverseBytes(previousChainWorkBytes))
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert previous chain work bytes to hash for block %s: %w", block.Hash().String(), err)
	}

	cumulativeChainWorkHash, err := getCumulativeChainWork(prevChainWorkHash, block)
	if err != nil {
		return nil, errors.NewProcessingError("failed to calculate cumulative chain work for block %s: %w", block.Hash().String(), err)
	}

	cumulativeChainWorkBytes := bt.ReverseBytes(cumulativeChainWorkHash.CloneBytes())

	return cumulativeChainWorkBytes, nil
}

// validateCoinbaseHeight ensures that blocks comply with BIP34 requirements.
// BIP34 requires that the coinbase transaction must include the correct block height
// in its first input script for all blocks version 2 or higher after the activation height.
//
// Parameters:
//   - block: The block being validated
//   - currentHeight: The calculated height for this block
//
// Returns:
//   - error: Validation error if the coinbase height doesn't match or can't be extracted,
//     or nil if validation passes or isn't required
func (s *SQL) validateCoinbaseHeight(block *model.Block, currentHeight uint32) error {
	// Check that the coinbase transaction includes the correct block height for all
	// blocks that are version 2 or higher. BIP34 activation height is 227835.
	// Also check if CoinbaseTx exists.
	if block.CoinbaseTx != nil && block.Header.Version > 1 {
		blockHeight, err := block.ExtractCoinbaseHeight()
		if err != nil {
			// Define BIP34 activation height (consider getting from chainParams)
			bip34ActivationHeight := uint32(227835)
			if currentHeight < bip34ActivationHeight {
				// Log warning for pre-BIP34 blocks where extraction might fail legitimately
				s.logger.Warnf("failed to extract coinbase height for block %s (height %d), pre-BIP34 activation: %v", block.Hash(), currentHeight, err)
				return nil // Don't fail validation for pre-BIP34 blocks
			}
			// Fail for post-BIP34 blocks if extraction fails
			return errors.NewStorageError("failed to extract coinbase height for block %s (height %d): %w", block.Hash().String(), currentHeight, err)
		}

		// Define BIP34 activation height again (or use variable from above)
		bip34ActivationHeight := uint32(227835)
		// Check height match only if extraction succeeded and after BIP34 activation
		if currentHeight >= bip34ActivationHeight && blockHeight != currentHeight {
			return errors.NewStorageError("coinbase transaction height (%d) does not match block height (%d) for block %s", blockHeight, currentHeight, block.Hash().String())
		}
	}

	return nil // No validation needed or validation passed
}

// getCumulativeChainWork calculates the total proof-of-work up to and including this block.
// It delegates to the work calculator to add the current block's work to the previous
// cumulative chain work value.
//
// Parameters:
//   - chainWork: The cumulative proof-of-work hash up to the previous block
//   - block: The block whose work should be added to the cumulative total
//
// Returns:
//   - *chainhash.Hash: The new cumulative chain work hash
//   - error: Any error encountered during calculation
func getCumulativeChainWork(chainWork *chainhash.Hash, block *model.Block) (*chainhash.Hash, error) {
	newWork, err := work.CalculateWork(chainWork, block.Header.Bits)
	if err != nil {
		return nil, err
	}

	return newWork, nil
}
