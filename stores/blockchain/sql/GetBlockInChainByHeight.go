package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

/*
GetBlockInChainByHeightHash returns a block by height for a chain determined by the start hash.
This is useful for getting the block at a given height in a chain that may have a different tip.
*/
func (s *SQL) GetBlockInChainByHeightHash(ctx context.Context, height uint32, startHash *chainhash.Hash) (block *model.Block, invalid bool, err error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockInChainByHeightHash")
	defer deferFn()

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockInChainByHeightHash-%d-%s", height, startHash.String())))

	cached := s.responseCache.Get(cacheID)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().(*model.Block); ok && cacheData != nil {
			s.logger.Debugf("GetBlockByHeight cache hit")
			return cacheData, false, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		WITH RECURSIVE ChainBlocks AS (
			SELECT id, parent_id, height
			FROM blocks
			WHERE hash = $2
			
			UNION ALL
			
			SELECT bb.id, bb.parent_id, bb.height
			FROM blocks bb
			JOIN ChainBlocks cb ON bb.id = cb.parent_id
			WHERE bb.id != cb.id
		)
		SELECT
		 b.ID
		,b.version
		,b.block_time
		,b.n_bits
		,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.tx_count
		,b.size_in_bytes
		,b.coinbase_tx
		,b.subtree_count
		,b.subtrees
		,b.invalid
		FROM blocks b
		WHERE b.id IN (
			SELECT id FROM ChainBlocks
			WHERE height = $1
			LIMIT 1
		)
	`

	block = &model.Block{
		Header: &model.BlockHeader{},
	}

	var (
		subtreeCount     uint64
		transactionCount uint64
		sizeInBytes      uint64
		subtreeBytes     []byte
		hashPrevBlock    []byte
		hashMerkleRoot   []byte
		coinbaseTx       []byte
		nBits            []byte
	)

	if err = s.db.QueryRowContext(ctx, q, height, startHash.CloneBytes()).Scan(
		&block.ID,
		&block.Header.Version,
		&block.Header.Timestamp,
		&nBits,
		&block.Header.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&transactionCount,
		&sizeInBytes,
		&coinbaseTx,
		&subtreeCount,
		&subtreeBytes,
		&invalid,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, errors.NewBlockNotFoundError("failed to get block in-chain by height", err)
		}

		return nil, false, errors.NewStorageError("failed to get block in-chain by height", err)
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	block.Header.Bits = *bits

	block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, false, errors.NewInvalidArgumentError("failed to convert hashPrevBlock: %s", utils.ReverseAndHexEncodeSlice(hashPrevBlock), err)
	}

	block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, false, errors.NewInvalidArgumentError("failed to convert hashMerkleRoot: %s", utils.ReverseAndHexEncodeSlice(hashMerkleRoot), err)
	}

	block.TransactionCount = transactionCount
	block.SizeInBytes = sizeInBytes

	if len(coinbaseTx) > 0 {
		block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTx)
		if err != nil {
			return nil, false, errors.NewInvalidArgumentError("failed to convert coinbaseTx", err)
		}
	}

	err = block.SubTreesFromBytes(subtreeBytes)
	if err != nil {
		return nil, false, errors.NewInvalidArgumentError("failed to convert subtrees", err)
	}

	s.responseCache.Set(cacheID, block, s.cacheTTL)

	return block, invalid, nil
}
