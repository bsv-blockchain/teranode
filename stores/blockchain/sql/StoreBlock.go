package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/TAAL-GmbH/arc/blocktx/store"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/work"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (s *SQL) StoreBlock(ctx context.Context, block *model.Block) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blocktx").NewStat("InsertBlock").AddTime(start)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	var previousBlockId uint64
	var previousChainWork []byte
	var orphaned bool
	var previousHeight uint64
	var height uint64

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
		,subtree_count
		,subtrees
        ,coinbase_tx
        ,orphaned
	) VALUES ($1, $2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15)
		RETURNING id
	`

	coinbaseTxID := block.CoinbaseTx.TxID()
	if coinbaseTxID == "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b" {
		// genesis block
		previousBlockId = 0
		previousChainWork = make([]byte, 32)
		previousHeight = 0
		orphaned = false
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
			,subtree_count
			,subtrees
			,coinbase_tx
			,orphaned
		) VALUES (0, $1, $2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15)
			RETURNING id
		`
	} else {
		qq := `
			SELECT
			 b.id
			,b.chain_work
			,b.orphaned
			,b.height
			FROM blocks b
			WHERE b.hash = $1
		`
		if err = s.db.QueryRowContext(ctx, qq, block.Header.HashPrevBlock[:]).Scan(
			&previousBlockId,
			&previousChainWork,
			&orphaned,
			&previousHeight,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("previous block not found: %w", store.ErrBlockNotFound)
			}
			return err
		}
		height = previousHeight + 1

		// check whether there is another block with the same height that is not orphaned
		var activeBlockId uint64
		qq = `
			SELECT
			 b.id
			FROM blocks b
			WHERE b.height = $1
			  AND b.orphaned = FALSE
		`
		_ = s.db.QueryRowContext(ctx, qq, height).Scan(&activeBlockId)
		if activeBlockId != 0 {
			// this block is an orphan
			orphaned = true
		}

		// TODO handle longer orphan chains

		// Check that the coinbase transaction includes the correct block height for all
		// blocks that are version 2 or higher.
		// BIP34 - Block number 227,835 (timestamp 2013-03-24 15:49:13 GMT) was the last version 1 block.
		if block.Header.Version > 1 {
			blockHeight, err := block.ExtractCoinbaseHeight()
			if err != nil {
				return err
			}
			if blockHeight != uint32(height) {
				return fmt.Errorf("coinbase transaction height (%d) does not match block height (%d)", blockHeight, height)
			}
		}
	}

	subtreeBytes, err := block.SubTreeBytes()
	if err != nil {
		return fmt.Errorf("failed to get subtree bytes: %w", err)
	}

	chainWorkHash, err := chainhash.NewHash(bt.ReverseBytes(previousChainWork))
	if err != nil {
		return fmt.Errorf("failed to convert chain work hash: %w", err)
	}
	cumulativeChainWork, err := getCumulativeChainWork(chainWorkHash, block)
	if err != nil {
		return fmt.Errorf("failed to calculate cumulative chain work: %w", err)
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
		len(block.Subtrees),
		subtreeBytes,
		block.CoinbaseTx.Bytes(),
		orphaned,
	)
	if err != nil {
		return err
	}

	rowFound := rows.Next()
	if !rowFound {
		return fmt.Errorf("block already exists: %s", block.Hash())
	}

	if err = rows.Scan(&previousBlockId); err != nil {
		return err
	}

	return nil
}

func getCumulativeChainWork(chainWork *chainhash.Hash, block *model.Block) (*chainhash.Hash, error) {
	newWork, err := work.CalculateWork(chainWork, block.Header.Bits)
	if err != nil {
		return nil, err
	}

	return newWork, nil
}
