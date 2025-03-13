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

func (s *SQL) storeBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) (uint64, uint32, []byte, error) {
	var (
		err                  error
		previousBlockID      uint64
		previousChainWork    []byte
		previousHeight       uint32
		height               uint32
		previousBlockInvalid bool
	)

	// Apply options
	storeBlockOptions := options.StoreBlockOptions{}
	for _, opt := range opts {
		opt(&storeBlockOptions)
	}

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
		,mined_set
		,subtrees_set
	) VALUES ($1, $2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, $16, $17, $18, $19)
		RETURNING id
	`

	var coinbaseTxID string

	if block.CoinbaseTx != nil {
		coinbaseTxID = block.CoinbaseTx.TxID()
	}

	if coinbaseTxID == s.chainParams.GenesisBlock.Transactions[0].TxHash().String() {
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
			,mined_set
			,subtrees_set
		) VALUES (0, $1, $2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14, $15, $16, $17, $18, $19)
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
				return 0, 0, nil, errors.NewStorageError("error storing block %s as previous block %s not found", block.Hash().String(), block.Header.HashPrevBlock.String(), err)
			}

			return 0, 0, nil, err
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
						return 0, 0, nil, err
					}
				}

				if height >= 227835 && blockHeight != height {
					return 0, 0, nil, errors.NewStorageError("coinbase transaction height (%d) does not match block height (%d)", blockHeight, height)
				}
			}
		}
	}

	subtreeBytes, err := block.SubTreeBytes()
	if err != nil {
		return 0, 0, nil, errors.NewStorageError("failed to get subtree bytes", err)
	}

	chainWorkHash, err := chainhash.NewHash(bt.ReverseBytes(previousChainWork))
	if err != nil {
		return 0, 0, nil, errors.NewProcessingError("failed to convert chain work hash", err)
	}

	cumulativeChainWork, err := getCumulativeChainWork(chainWorkHash, block)
	if err != nil {
		return 0, 0, nil, errors.NewProcessingError("failed to calculate cumulative chain work", err)
	}

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
		storeBlockOptions.MinedSet,
		storeBlockOptions.SubtreesSet,
	)
	if err != nil {
		// check whether this is a postgres exists constraint error
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" { // Duplicate constraint violation
			return 0, 0, nil, errors.NewBlockExistsError("block already exists in the database: %s", block.Hash().String(), err)
		}

		// check whether this is a sqlite exists constraint error
		var sqliteErr *sqlite.Error
		if errors.As(err, &sqliteErr) && sqliteErr.Code() == SQLITE_CONSTRAINT {
			return 0, 0, nil, errors.NewBlockExistsError("block already exists in the database: %s", block.Hash().String(), err)
		}

		// otherwise, return the generic error
		return 0, 0, nil, errors.NewStorageError("failed to store block", err)
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

	return newBlockID, height, cumulativeChainWork.CloneBytes(), nil
}

func getCumulativeChainWork(chainWork *chainhash.Hash, block *model.Block) (*chainhash.Hash, error) {
	newWork, err := work.CalculateWork(chainWork, block.Header.Bits)
	if err != nil {
		return nil, err
	}

	return newWork, nil
}
