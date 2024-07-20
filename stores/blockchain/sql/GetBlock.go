package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type getBlockCache struct {
	block  *model.Block
	height uint32
}

func (s *SQL) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	start, stat, ctx := tracing.StartStatFromContext(ctx, "GetBlock")
	defer func() {
		stat.AddTime(start)
	}()

	// header, meta, er := cache.GetBlock(*blockHash)
	// if er != nil {
	// 	return nil, 0, fmt.Errorf("error in GetBlock: %w", er)
	// }
	// if header != nil {
	// 	block := &model.Block{
	// 		Header:           header,
	// 		TransactionCount: meta.TxCount,
	// 		SizeInBytes:      meta.SizeInBytes,
	// 		Height:           meta.Height,
	// 	}
	// 	return block, meta.Height, nil
	// }

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheId := chainhash.HashH([]byte(fmt.Sprintf("getBlock-%s", blockHash.String())))
	cached := s.responseCache.Get(cacheId)
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
		,b.size_in_bytes
		,b.coinbase_tx
		,b.height
	  ,b.tx_count
		,b.subtree_count
		,b.subtrees
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
		&sizeInBytes,
		&coinbaseTx,
		&height,
		&transactionCount,
		&subtreeCount,
		&subtreeBytes,
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

	// set the block height on the block
	block.Height = height

	s.responseCache.Set(cacheId, &getBlockCache{
		block:  block,
		height: height,
	}, s.cacheTTL)

	return block, height, nil
}
