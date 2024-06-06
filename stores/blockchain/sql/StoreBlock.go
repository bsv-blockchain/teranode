package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/work"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) StoreBlock(ctx context.Context, block *model.Block, peerID string) (uint64, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "StoreBlock")
	defer func() {
		stat.AddTime(start)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	newBlockId, height, err := s.storeBlock(ctx, block, peerID)
	if err != nil {
		return 0, err
	}

	var miner string
	if block.CoinbaseTx.OutputCount() != 0 {
		miner = block.CoinbaseTx.Outputs[0].LockingScript.String()
	}

	meta := &model.BlockHeaderMeta{
		ID:          uint32(newBlockId),
		Height:      uint32(height),
		TxCount:     block.TransactionCount,
		SizeInBytes: block.SizeInBytes,
		Miner:       miner,
		// BlockTime   uint32 `json:"block_time"`    // Time of the block.
		// Timestamp   uint32 `json:"timestamp"`     // Timestamp of the block.
	}

	ok := s.blocksCache.AddBlockHeader(block.Header, meta)
	if !ok {
		if err := s.Reset(ctx); err != nil {
			s.logger.Errorf("error clearing caches: %v", err)
		}
	}

	return newBlockId, nil
}

func (s *SQL) storeBlock(ctx context.Context, block *model.Block, peerID string) (uint64, uint64, error) {
	var err error
	var previousBlockId uint64
	var previousChainWork []byte
	var previousHeight uint64
	var height uint64
	previousBlockInvalid := false

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

	coinbaseTxID := block.CoinbaseTx.TxID()
	if coinbaseTxID == "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b" {
		// genesis block
		previousBlockId = 0
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
			&previousBlockId,
			&previousChainWork,
			&previousHeight,
			&previousBlockInvalid,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return 0, 0, fmt.Errorf("error storing block %s as previous block %s not found: %w", block.Hash().String(), block.Header.HashPrevBlock.String(), err)
			}
			return 0, 0, err
		}
		height = previousHeight + 1

		// Check that the coinbase transaction includes the correct block height for all
		// blocks that are version 2 or higher.
		// BIP34 - Block number 227,835 (timestamp 2013-03-24 15:49:13 GMT) was the last version 1 block.
		if block.Header.Version > 1 {
			blockHeight, err := block.ExtractCoinbaseHeight()
			if err != nil {
				return 0, 0, err
			}
			if blockHeight != uint32(height) {
				return 0, 0, fmt.Errorf("coinbase transaction height (%d) does not match block height (%d)", blockHeight, height)
			}
		}
	}

	subtreeBytes, err := block.SubTreeBytes()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get subtree bytes: %w", err)
	}

	chainWorkHash, err := chainhash.NewHash(bt.ReverseBytes(previousChainWork))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to convert chain work hash: %w", err)
	}
	cumulativeChainWork, err := getCumulativeChainWork(chainWorkHash, block)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to calculate cumulative chain work: %w", err)
	}

	hashPrevBlock, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	hashMerkleRoot, _ := chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")
	nbits := model.NewNBitFromString("1d00ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: hashMerkleRoot,
		Timestamp:      1231469665,
		Bits:           nbits,
		Nonce:          2573394689,
	}
	_ = blockHeader

	blockHeader2 := block.Header
	_ = blockHeader2

	rows, err := s.db.QueryContext(ctx, q,
		previousBlockId,
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
		block.CoinbaseTx.Bytes(),
		previousBlockInvalid,
	)
	if err != nil {
		return 0, 0, err
	}

	defer rows.Close()

	rowFound := rows.Next()
	if !rowFound {
		return 0, 0, fmt.Errorf("block already exists: %s", block.Hash())
	}

	var newBlockId uint64
	if err = rows.Scan(&newBlockId); err != nil {
		return 0, 0, err
	}

	return newBlockId, height, nil
}

func getCumulativeChainWork(chainWork *chainhash.Hash, block *model.Block) (*chainhash.Hash, error) {
	newWork, err := work.CalculateWork(chainWork, block.Header.Bits)
	if err != nil {
		return nil, err
	}

	return newWork, nil
}
