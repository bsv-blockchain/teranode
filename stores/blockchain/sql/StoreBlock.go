package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/TAAL-GmbH/arc/blocktx/store"
	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
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

	coinbaseTxID := block.CoinbaseTx.TxID()
	if coinbaseTxID == "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b" {
		// genesis block
		previousBlockId = 0
		previousChainWork = make([]byte, 32)
		orphaned = false
		previousHeight = 0
		height = 0
	} else {
		q := `
			SELECT
			 b.id
			,b.chain_work
			,b.orphaned
			,b.height
			FROM blocks b
			WHERE b.hash = $1
		`
		if err = s.db.QueryRowContext(ctx, q, block.Header.HashPrevBlock[:]).Scan(
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
		q = `
			SELECT
			 b.id
			FROM blocks b
			WHERE b.height = $1
			  AND b.orphaned = FALSE
		`
		_ = s.db.QueryRowContext(ctx, q, height).Scan(&activeBlockId)
		if activeBlockId != 0 {
			// this block is an orphan
			orphaned = true
		}
	}

	q := `
		INSERT INTO blocks (
		 parentId
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
		) VALUES (
		 $1
		,$2
		,$3
		,$4
		,$5
		,$6
		,$7
		,$8
		,$9
		,$10
		,$11
		,$12
		,$13
		,$14
		,$15
		)
		ON CONFLICT DO NOTHING
		RETURNING id
	`

	subtreeBytes, err := block.SubTreeBytes()
	if err != nil {
		return fmt.Errorf("failed to get subtree bytes: %w", err)
	}

	chainWorkHash, err := chainhash.NewHash(previousChainWork)
	if err != nil {
		return fmt.Errorf("failed to convert chain work hash: %w", err)
	}
	cumulativeChainWork, err := getCumulativeChainWork(chainWorkHash, block)
	if err != nil {
		return fmt.Errorf("failed to calculate cumulative chain work: %w", err)
	}

	if err = s.db.QueryRowContext(ctx, q,
		previousBlockId,
		block.Header.Version,
		block.Hash().CloneBytes(),
		block.Header.HashPrevBlock.CloneBytes(),
		block.Header.HashMerkleRoot.CloneBytes(),
		block.Header.Timestamp,
		block.Header.Bits,
		block.Header.Nonce,
		height,
		cumulativeChainWork.CloneBytes(),
		block.TransactionCount,
		len(block.Subtrees),
		subtreeBytes,
		block.CoinbaseTx.Bytes(),
		orphaned,
	).Scan(&previousBlockId); err != nil {
		return err
	}

	return nil
}

func getCumulativeChainWork(chainWork *chainhash.Hash, block *model.Block) (*chainhash.Hash, error) {
	newWork, err := util.CalculateWork(chainWork, block.Header.Bits)
	if err != nil {
		return nil, err
	}

	return newWork, nil
}
