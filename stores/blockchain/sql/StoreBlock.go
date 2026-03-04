// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the StoreBlock method and its supporting functions, which
// handle the critical operation of persisting new blocks to the blockchain database.
// The implementation includes chain work calculation, block height validation,
// special handling for the genesis block, and maintenance of the blockchain state.
// It also manages caching to ensure optimal performance while maintaining data consistency.
package sql

import (
	"context"
	"database/sql"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

// StoreBlock persists a new block to the database and updates chain state.
// This implements the blockchain.Store.StoreBlock interface method.
//
// This method is the cornerstone of blockchain persistence in Teranode, serving as the
// critical path for extending the blockchain with new blocks. It's responsible for
// maintaining blockchain integrity and consensus while supporting high-throughput block
// processing in BSV's unlimited scaling architecture.
//
// The implementation follows a sophisticated multi-stage approach:
//
// 1. Concurrency Management:
//   - Uses a mutex to ensure thread-safety during block insertion
//   - Prevents race conditions that could compromise blockchain integrity
//   - Particularly important during chain reorganizations or parallel block processing
//
// 2. Block Validation and Positioning:
//   - Validates the block's structure and relationship to existing blocks
//   - Handles special cases like genesis blocks with zero previous values
//   - Enforces BIP34 compliance by validating coinbase height encoding
//   - Calculates the block's position in the blockchain (height)
//
// 3. Consensus Determination:
//   - Computes cumulative proof-of-work by adding block's work to previous total
//   - Uses the work calculator to derive work from difficulty bits (nBits)
//   - Maintains the chain with highest cumulative work as the valid blockchain
//
// 4. Database Persistence:
//   - Persists block data, transactions, and metadata atomically
//   - Uses database-specific optimizations for PostgreSQL and SQLite
//   - Handles constraint violations and other database errors gracefully
//   - Extracts miner information from coinbase transactions for analytics
//
// 5. Cache Management:
//   - Updates the in-memory caches for optimized future access
//   - Invalidates affected caches during chain reorganizations
//   - Maintains cache consistency with database state
//
// 6. Optional Processing:
//   - Supports functional options pattern for flexible behavior modification
//   - Handles flags for mined status and subtree processing
//
// In Teranode's high-throughput architecture, this method is designed to efficiently
// process blocks at scale while maintaining strict blockchain validation rules and
// ensuring consistent behavior across different database backends.
//
// This method is thread-safe and can be called concurrently from multiple goroutines.
// It uses database transactions to ensure atomicity of the block storage operation and
// maintains cache consistency through strategic invalidation when necessary.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - block: The complete block structure to be stored, including header and transactions
//   - peerID: Identifier of the peer that provided this block, used for tracking and diagnostics
//   - opts: Optional parameters using the functional options pattern to modify storage behavior,
//     such as marking blocks as mined or enabling subtree processing
//
// Returns:
//   - uint64: The unique database ID assigned to the stored block, used for referencing
//     the block in subsequent operations
//   - uint32: The height of the block in the blockchain
//   - error: Any error encountered during the operation, specifically:
//   - ValidationError for blocks that violate consensus rules (e.g., invalid coinbase height)
//   - BlockAlreadyExistsError for duplicate block submissions
//   - StorageError for database failures
//   - ProcessingError for internal processing failures
func (s *SQL) StoreBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) (uint64, uint32, error) {
	ctx, _, deferFn := tracing.Tracer("sql").Start(ctx, "sql:StoreBlock")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Apply options
	storeBlockOptions := options.StoreBlockOptions{}
	for _, opt := range opts {
		opt(&storeBlockOptions)
	}

	newBlockID, height, _, _, err := s.storeBlock(ctx, block, peerID, storeBlockOptions)
	if err != nil {
		return 0, height, err
	}

	// Reset response cache to invalidate cached block headers and best block
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
	// Query database for previous block info
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
// to persist a block. It handles both genesis and regular blocks differently, with special
// processing for the initial block in the chain.
//
// This function is the core implementation of block persistence in Teranode, responsible for
// maintaining the blockchain's structural integrity and consensus rules during block insertion.
// It's designed to handle the complexities of blockchain state management while optimizing for
// high-throughput performance in BSV's unlimited scaling architecture.
//
// The implementation follows a systematic approach to block processing:
//
// 1. Block Positioning and Validation:
//   - Determines if this is a genesis block or a regular block
//   - For regular blocks, retrieves and validates previous block data
//   - Establishes the block's height by incrementing the previous block's height
//   - Validates the block height encoded in the coinbase transaction (BIP34 compliance)
//   - Inherits validity status from the previous block (invalid parents create invalid children)
//
// 2. Consensus Calculation:
//   - Calculates cumulative chain work by adding the block's work to the previous total
//   - Uses the work calculator to derive work from difficulty bits (nBits)
//   - Maintains the chain with highest cumulative work as the valid blockchain
//
// 3. Block Data Preparation:
//   - Extracts and formats all required fields for database storage
//   - Prepares block hash, version, timestamp, and transaction information
//   - Extracts miner information from coinbase transaction when available
//   - Handles special fields like chainwork representation and parent references
//
// 4. Database Persistence:
//   - Executes optimized SQL insert statements with database-specific adaptations
//   - Uses prepared statements and batching for efficient transaction processing
//   - Handles constraint violations and other database errors with appropriate domain translation
//   - Applies optional flags for mined status and subtree processing
//
// This function is critical to blockchain integrity as it maintains the chain's
// continuity and ensures that all blocks are properly linked with correct heights
// and cumulative work calculations, forming the foundation of Bitcoin's consensus mechanism.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - block: The complete block structure to be stored, including header and transactions
//   - peerID: Identifier of the peer that provided this block, used for tracking
//   - storeBlockOptions: Configuration options using the functional options pattern
//
// Returns:
//   - uint64: The unique database ID assigned to the stored block
//   - uint32: The height of the block in the blockchain
//   - []byte: The calculated cumulative chain work for this block as a byte array
//   - error: Any error encountered during the operation, including validation failures
func (s *SQL) storeBlock(ctx context.Context, block *model.Block, peerID string, storeBlockOptions options.StoreBlockOptions) (uint64, uint32, []byte, bool, error) {
	var (
		coinbaseTxID string
		q            string
	)

	if block.CoinbaseTx != nil {
		coinbaseTxID = block.CoinbaseTx.TxID()
	}

	genesis, height, previousBlockID, previousChainWork, previousBlockInvalid, err := s.getPreviousBlockData(ctx, coinbaseTxID, block)
	if err != nil {
		return 0, 0, nil, false, err
	}

	storeAsInvalid := previousBlockInvalid || storeBlockOptions.Invalid

	// Determine if we should use a custom ID or auto-increment
	// ID=0 is special: it means "not set" for regular blocks, but "use ID=0" for genesis
	useCustomID := storeBlockOptions.ID != 0

	// Genesis blocks cannot have custom IDs (except during initialization with ID=0)
	if genesis && storeBlockOptions.ID > 0 {
		return 0, 0, nil, false, errors.NewInvalidArgumentError("genesis block cannot have custom ID")
	}

	if genesis {
		// genesis block - use ID=0 if specified, otherwise auto-increment
		if storeBlockOptions.ID == 0 {
			// Genesis initialization with explicit ID=0
			useCustomID = true
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
	,persisted_at
) VALUES ($1, $2, $3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, $16, $17, $18, $19, $20, $21)
RETURNING id
			`
		} else {
			// Genesis without explicit ID - use auto-increment
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
	,persisted_at
) VALUES ($1, $2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, $16, $17, $18, $19, $20)
RETURNING id
			`
		}
	} else {
		if useCustomID {
			// non-genesis block with custom ID
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
	,persisted_at
) VALUES ($1, $2, $3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, $16, $17, $18, $19, $20, $21)
RETURNING id
			`
		} else {
			// non-genesis block with auto-increment
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
	,persisted_at
) VALUES ($1, $2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, $16, $17, $18, $19, $20)
RETURNING id
			`
		}
	}

	cumulativeChainWorkBytes, err := calculateAndPrepareChainWork(previousChainWork, block)
	if err != nil {
		return 0, 0, nil, false, err // Return error from calculation
	}

	subtreeBytes, err := block.SubTreeBytes()
	if err != nil {
		return 0, 0, nil, false, errors.NewStorageError("failed to get subtree bytes", err)
	}

	var coinbaseBytes []byte
	if block.CoinbaseTx != nil {
		coinbaseBytes = block.CoinbaseTx.Bytes()
	}

	var rows *sql.Rows

	// persisted_at is either NULL or set to the current timestamp
	var persistedAt interface{}

	if storeBlockOptions.PersistedAt {
		now := time.Now()
		if s.engine == util.Postgres {
			persistedAt = now
		} else {
			// SQLite stores timestamps as TEXT - format as "YYYY-MM-DD HH:MM:SS"
			persistedAt = now.UTC().Format("2006-01-02 15:04:05")
		}
	}

	if useCustomID {
		// When using custom ID, the ID is the first parameter
		rows, err = s.db.QueryContext(ctx, q,
			storeBlockOptions.ID,
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
			storeAsInvalid,
			storeBlockOptions.MinedSet,
			storeBlockOptions.SubtreesSet,
			persistedAt,
		)
	} else {
		// When using auto-increment, no ID parameter is needed
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
			storeAsInvalid,
			storeBlockOptions.MinedSet,
			storeBlockOptions.SubtreesSet,
			persistedAt,
		)
	}

	if err != nil {
		return 0, 0, nil, false, s.parseSQLError(err, block)
	}

	defer rows.Close()

	rowFound := rows.Next()
	if !rowFound {
		return 0, 0, nil, false, errors.NewBlockExistsError("block already exists: %s", block.Hash())
	}

	var newBlockID uint64
	if err = rows.Scan(&newBlockID); err != nil {
		return 0, 0, nil, false, errors.NewStorageError("failed to scan new block id", err)
	}

	return newBlockID, height, cumulativeChainWorkBytes, storeAsInvalid, nil
}

// parseSQLError unwraps and translates SQL-specific errors into domain-specific errors.
// This helper function detects database constraint violations from different SQL backends
// (PostgreSQL and SQLite) and converts them into appropriate application errors.
//
// The function provides database engine abstraction by handling the different error
// formats and codes returned by PostgreSQL and SQLite. It specifically looks for:
// - Unique constraint violations (duplicate blocks)
// - Foreign key constraint violations (missing referenced blocks)
// - Other database-specific errors
//
// This abstraction is crucial for maintaining a consistent error handling approach
// across different database backends, allowing the application code to work with
// domain-specific errors rather than database-specific ones.
//
// Parameters:
//   - err: The original SQL error to parse, which may be a wrapped error from the database driver
//   - block: The block being processed, used for providing context in the error message
//     and for extracting the block hash for error identification
//
// Returns:
//   - error: A domain-specific error with appropriate context, typically wrapped as
//     a BlockAlreadyExistsError for duplicates or a more general StorageError for other issues
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
// This function is a critical component in Teranode's blockchain construction process,
// responsible for establishing the correct position of a new block in the blockchain graph.
// It handles the special case of the genesis block (the first block in the chain) and
// ensures proper linking between blocks to maintain the blockchain's integrity.
//
// In BSV's UTXO-based design, each block must correctly reference its parent block,
// and this function validates this relationship while retrieving essential metadata
// needed for consensus rules and chain organization.
//
// The implementation handles two distinct scenarios:
//
// 1. Genesis Block Processing:
//   - Identifies genesis blocks based on network parameters or previous hash value
//   - Initializes previous values with defaults (zero height, empty chain work, etc.)
//   - Sets up the foundation for the entire blockchain
//
// 2. Regular Block Processing:
//   - Retrieves the previous block's data from the database using its hash
//   - Validates that the previous block exists in the database
//   - Establishes the correct block height by incrementing the previous block's height
//   - Retrieves the cumulative chain work to be extended by the new block
//   - Determines validity inheritance (invalid parents create invalid children)
//
// This function is essential for maintaining blockchain continuity and implementing
// the core consensus rules of Bitcoin, particularly the requirement that blocks form
// an unbroken chain of references back to the genesis block. It also supports Teranode's
// high-throughput architecture by efficiently retrieving only the necessary data for
// block positioning and validation.
//
// Parameters:
//   - ctx: Context for database operations, allowing for cancellation and timeouts
//   - coinbaseTxID: Transaction ID of the coinbase transaction, used for error reporting
//   - block: The block being processed, containing the previous block hash reference
//
// Returns:
//   - genesis: Boolean flag indicating whether this is the genesis block
//   - height: Height of the current block in the blockchain
//   - previousBlockID: Database ID of the previous block for referential integrity
//   - previousChainWork: Cumulative proof-of-work for previous block to be extended
//   - previousBlockInvalid: Whether the previous block is marked as invalid, which
//     affects validation status of the current block
//   - err: Any error encountered during processing, including database errors or
//     if the previous block cannot be found
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
// This function implements a core component of Bitcoin's Nakamoto consensus mechanism,
// which determines the canonical blockchain based on cumulative proof-of-work rather than
// simple block height. In BSV's blockchain design, the chain with the highest cumulative
// work represents the most computationally expensive chain to produce and is therefore
// considered the valid blockchain.
//
// The cumulative chain work calculation is particularly important for:
//   - Determining the "best chain" during reorganizations
//   - Providing an objective measure of blockchain security
//   - Enabling nodes to agree on the canonical chain without central coordination
//   - Quantifying the economic cost of attacking the blockchain
//
// In Teranode's high-throughput architecture, this calculation must be both precise
// and efficient, as it's performed for every block added to the chain and directly
// impacts consensus decisions. The implementation uses optimized big integer operations
// to handle the potentially very large numbers involved in work calculations for
// long blockchains.
//
// The calculation process involves:
// 1. Converting the previous chain work from byte format to a chainhash.Hash
// 2. Calling getCumulativeChainWork to add the new block's work to the previous total
// 3. Converting the resulting hash back to bytes for database storage
//
// Parameters:
//   - previousChainWorkBytes: The byte representation of the previous block's cumulative work
//     (for genesis block, this will be empty/zeros)
//   - block: The block being processed, containing difficulty bits used to calculate work
//
// Returns:
//   - []byte: The byte representation of the new cumulative chain work for efficient storage
//   - error: Any error encountered during the calculation process
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
// This function implements an important Bitcoin consensus rule defined in BIP34
// (Bitcoin Improvement Proposal 34), which was activated at block height 227,836 on
// the main Bitcoin network. The rule requires miners to include the block height in
// the coinbase transaction's input script, providing several important benefits:
//
//  1. Prevents coinbase transaction hash collisions that could occur when miners
//     used identical coinbase transactions in different blocks
//  2. Enhances blockchain analysis by making it easier to identify which block
//     a coinbase transaction belongs to
//  3. Provides an additional integrity check for block validation
//  4. Enables certain scripting capabilities that rely on knowing the current height
//
// The implementation performs a systematic validation process:
//
// 1. Applicability Check:
//   - Determines if BIP34 validation applies based on block version and height
//   - For blocks before the activation height or with version < 2, validation is skipped
//
// 2. Height Extraction:
//   - Accesses the coinbase transaction (first transaction in the block)
//   - Examines the first input's script for the encoded height value
//   - Uses Bitcoin's script parsing rules to extract the height as a number
//
// 3. Validation:
//   - Compares the extracted height with the expected block height
//   - Returns a ValidationError if heights don't match or extraction fails
//
// In Teranode's implementation of the BSV protocol, this validation is particularly
// important for maintaining consensus compatibility with other Bitcoin implementations
// and ensuring the integrity of the blockchain during high-throughput block processing.
//
// Parameters:
//   - block: The block being validated, containing the coinbase transaction
//   - currentHeight: The calculated height for this block based on its position in the chain
//
// Returns:
//   - error: ValidationError if the coinbase height doesn't match or can't be extracted,
//     or nil if validation passes or isn't required for this block
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
// This function implements a fundamental component of Bitcoin's Nakamoto consensus mechanism,
// which uses proof-of-work as an objective measure to determine the canonical blockchain.
// In BSV's blockchain design, the cumulative chain work serves as the primary metric for
// identifying the valid chain, superseding simple block height as the determinant of the
// "best chain."
//
// The implementation follows Bitcoin's established proof-of-work calculation methodology:
//
//  1. Each block's individual work is derived from its difficulty target (nBits field)
//     using the formula: work = 2^256 / (target+1), representing the expected number
//     of hash operations required to find a block at that difficulty
//
//  2. The cumulative chain work is the sum of all individual block work values in the chain,
//     stored as a large integer (represented as a hash) to handle the potentially enormous
//     values that accumulate over time
//
//  3. The work calculator component handles the mathematical operations efficiently,
//     including the conversion between different representations of the work values
//
// This calculation is critical for several aspects of blockchain operation:
//
//   - Consensus Formation: Enables nodes to objectively identify the canonical blockchain
//     without central coordination
//
//   - Chain Selection: During reorganizations, the chain with the most cumulative work
//     is automatically selected as the valid chain
//
//   - Security Quantification: Provides a measure of the blockchain's security by
//     quantifying the computational resources required to rewrite history
//
//   - Network Analysis: Allows for estimation of the network's total computational power
//     and mining difficulty adjustments
//
// In Teranode's high-throughput architecture for BSV, maintaining accurate chain work
// calculations is essential for ensuring consensus compatibility with other Bitcoin
// implementations while supporting the unlimited scaling vision of the protocol.
//
// Parameters:
//   - chainWork: The cumulative proof-of-work hash up to the previous block
//   - block: The block whose work should be added to the cumulative total,
//     containing the nBits field used to calculate the block's individual work
//
// Returns:
//   - *chainhash.Hash: The new cumulative chain work hash, representing the total
//     computational effort expended to create the blockchain up to this block
//   - error: Any error encountered during calculation, typically related to
//     difficulty target parsing or arithmetic operations
func getCumulativeChainWork(chainWork *chainhash.Hash, block *model.Block) (*chainhash.Hash, error) {
	newWork, err := work.CalculateWork(chainWork, block.Header.Bits)
	if err != nil {
		return nil, err
	}

	return newWork, nil
}
