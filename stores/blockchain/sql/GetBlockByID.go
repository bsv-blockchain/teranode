package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

func (s *SQL) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockByID")
	defer deferFn()

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockByID-%d", id)))

	cached := s.responseCache.Get(cacheID)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().(*model.Block); ok && cacheData != nil {
			s.logger.Debugf("GetBlockByID cache hit")
			return cacheData, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
		 b.ID
		,b.height
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
		FROM blocks b
		WHERE id = $1
	`

	block := &model.Block{
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
		err              error
	)

	if err = s.db.QueryRowContext(ctx, q, id).Scan(
		&block.ID,
		&block.Height,
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
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewBlockNotFoundError("failed to get block by height", err)
		}

		return nil, errors.NewStorageError("failed to get block by height", err)
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	block.Header.Bits = *bits

	block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("failed to convert hashPrevBlock: %s", utils.ReverseAndHexEncodeSlice(hashPrevBlock), err)
	}

	block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("failed to convert hashMerkleRoot: %s", utils.ReverseAndHexEncodeSlice(hashMerkleRoot), err)
	}

	block.TransactionCount = transactionCount
	block.SizeInBytes = sizeInBytes

	if len(coinbaseTx) > 0 {
		block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTx)
		if err != nil {
			return nil, errors.NewInvalidArgumentError("failed to convert coinbaseTx", err)
		}
	}

	err = block.SubTreesFromBytes(subtreeBytes)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("failed to convert subtrees", err)
	}

	s.responseCache.Set(cacheID, block, s.cacheTTL)

	return block, nil
}
