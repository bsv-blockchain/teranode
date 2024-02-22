package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type getBlockCache struct {
	block  *model.Block
	height uint32
}

func (s *SQL) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "GetBlock")
	defer func() {
		stat.AddTime(start)
	}()

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheId := chainhash.HashH([]byte(fmt.Sprintf("getBlock-%s", blockHash.String())))
	cached := cache.Get(cacheId)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().(*getBlockCache); ok && cacheData != nil {
			s.logger.Debugf("GetBlock cache hit")
			return cacheData.block, cacheData.height, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
	   b.version
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
		,b.height
		FROM blocks b
		WHERE b.hash = $1
	`

	block := &model.Block{
		Header: &model.BlockHeader{},
	}

	var subtreeCount uint64
	var transactionCount uint64
	var sizeInBytes uint64
	var subtreeBytes []byte
	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var coinbaseTx []byte
	var height uint32
	var nBits []byte
	var err error

	if err = s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
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
		&height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, 0, fmt.Errorf("error in GetBlock: %w", err)
		}
		return nil, 0, err
	}

	block.Header.Bits = model.NewNBitFromSlice(nBits)

	block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
	}
	block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert hashMerkleRoot: %w", err)
	}
	block.TransactionCount = transactionCount
	block.SizeInBytes = sizeInBytes

	block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert coinbaseTx: %w", err)
	}

	err = block.SubTreesFromBytes(subtreeBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert subtrees: %w", err)
	}

	cache.Set(cacheId, &getBlockCache{
		block:  block,
		height: height,
	}, cacheTTL)

	return block, height, nil
}
