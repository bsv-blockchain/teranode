package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/work"
	"github.com/bitcoin-sv/ubsv/tracing"
	sqlite_errors "github.com/bitcoin-sv/ubsv/util/sqlite"
	"github.com/lib/pq"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"modernc.org/sqlite"
)

func (s *SQL) StoreBlock(ctx context.Context, block *model.Block, peerID string) (uint64, uint32, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:StoreBlock")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	newBlockID, height, err := s.storeBlock(ctx, block, peerID)
	if err != nil {
		return 0, height, err
	}

	var miner string
	if block.CoinbaseTx.OutputCount() != 0 {
		miner = block.CoinbaseTx.Outputs[0].LockingScript.String()
	}

	meta := &model.BlockHeaderMeta{
		// nolint: gosec
		ID:          uint32(newBlockID),
		Height:      height,
		TxCount:     block.TransactionCount,
		SizeInBytes: block.SizeInBytes,
		Miner:       miner,
		// BlockTime   uint32 `json:"block_time"`    // Time of the block.
		// Timestamp   uint32 `json:"timestamp"`     // Timestamp of the block.
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

func (s *SQL) storeBlock(ctx context.Context, block *model.Block, peerID string) (uint64, uint32, error) {
	var (
		err                  error
		previousBlockID      uint64
		previousChainWork    []byte
		previousHeight       uint32
		height               uint32
		previousBlockInvalid bool
	)

	q := `
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
	) VALUES ($1, $2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, $16, $17)
		RETURNING id
	`

	var coinbaseTxID string

	if block.CoinbaseTx != nil {
		coinbaseTxID = block.CoinbaseTx.TxID()
	}

	if coinbaseTxID == "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b" {
		// genesis block
		previousBlockID = 0
		previousChainWork = make([]byte, 32)
		previousHeight = 0
		height = 0

		// genesis block has a different insert statement, because it has no parent
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
		) VALUES (0, $1, $2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, $16, $17)
			RETURNING id
		`
	} else {
		// Get the previous block that this incoming block is on top of to get the height and chain work.
		qq := `
			SELECT
			 b.id
			,b.chain_work
			,b.height
			,b.invalid
			FROM blocks b
			WHERE b.hash = $1
		`
		if err = s.db.QueryRowContext(ctx, qq, block.Header.HashPrevBlock[:]).Scan(
			&previousBlockID,
			&previousChainWork,
			&previousHeight,
			&previousBlockInvalid,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return 0, 0, errors.NewStorageError("error storing block %s as previous block %s not found", block.Hash().String(), block.Header.HashPrevBlock.String(), err)
			}
			return 0, 0, err
		}
		height = previousHeight + 1

		if block.CoinbaseTx != nil {
			// Check that the coinbase transaction includes the correct block height for all
			// blocks that are version 2 or higher.
			// BIP34 - Block number 227,835 (timestamp 2013-03-24 15:49:13 GMT) was the last version 1 block.
			if block.Header.Version > 1 {
				blockHeight, err := block.ExtractCoinbaseHeight()
				if err != nil {
					if height < 227835 {
						s.logger.Warnf("failed to extract coinbase height for block %s: %v", block.Hash(), err)
					} else {
						return 0, 0, err
					}
				}

				if height >= 227835 && blockHeight != height {
					return 0, 0, errors.NewStorageError("coinbase transaction height (%d) does not match block height (%d)", blockHeight, height)
				}
			}
		}
	}

	subtreeBytes, err := block.SubTreeBytes()
	if err != nil {
		return 0, 0, errors.NewStorageError("failed to get subtree bytes", err)
	}

	chainWorkHash, err := chainhash.NewHash(bt.ReverseBytes(previousChainWork))
	if err != nil {
		return 0, 0, errors.NewProcessingError("failed to convert chain work hash", err)
	}
	cumulativeChainWork, err := getCumulativeChainWork(chainWorkHash, block)
	if err != nil {
		return 0, 0, errors.NewProcessingError("failed to calculate cumulative chain work", err)
	}

	hashPrevBlock, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	hashMerkleRoot, _ := chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")
	nbits, _ := model.NewNBitFromString("1d00ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: hashMerkleRoot,
		Timestamp:      1231469665,
		Bits:           *nbits,
		Nonce:          2573394689,
	}
	_ = blockHeader

	blockHeader2 := block.Header
	_ = blockHeader2

	var coinbaseBytes []byte
	if block.CoinbaseTx != nil {
		coinbaseBytes = block.CoinbaseTx.Bytes()
	}

	rows, err := s.db.QueryContext(ctx, q,
		previousBlockID,
		block.Header.Version,
		block.Hash().CloneBytes(),
		block.Header.HashPrevBlock.CloneBytes(),
		block.Header.HashMerkleRoot.CloneBytes(),
		block.Header.Timestamp,
		block.Header.Bits.CloneBytes(),
		block.Header.Nonce,
		height,
		bt.ReverseBytes(cumulativeChainWork.CloneBytes()),
		block.TransactionCount,
		block.SizeInBytes,
		len(block.Subtrees),
		subtreeBytes,
		peerID,
		coinbaseBytes,
		previousBlockInvalid,
	)
	if err != nil {
		// check whether this is a postgres exists constraint error
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" { // Duplicate constraint violation
			return 0, 0, errors.NewBlockExistsError("block already exists in the database: %s", block.Hash().String(), err)
		}

		// check whether this is a sqlite exists constraint error
		var sqliteErr *sqlite.Error
		if errors.As(err, &sqliteErr) && sqliteErr.Code() == sqlite_errors.SQLITE_CONSTRAINT {
			return 0, 0, errors.NewBlockExistsError("block already exists in the database: %s", block.Hash().String(), err)
		}

		// otherwise, return the generic error
		return 0, 0, errors.NewStorageError("failed to store block", err)
	}

	defer rows.Close()

	rowFound := rows.Next()
	if !rowFound {
		return 0, 0, errors.NewBlockExistsError("block already exists: %s", block.Hash())
	}

	var newBlockID uint64
	if err = rows.Scan(&newBlockID); err != nil {
		return 0, 0, errors.NewStorageError("failed to scan new block id", err)
	}

	return newBlockID, height, nil
}

func getCumulativeChainWork(chainWork *chainhash.Hash, block *model.Block) (*chainhash.Hash, error) {
	newWork, err := work.CalculateWork(chainWork, block.Header.Bits)
	if err != nil {
		return nil, err
	}

	return newWork, nil
}
