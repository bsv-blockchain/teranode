package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/TAAL-GmbH/arc/blocktx/store"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *SQL) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blockchain").NewStat("GetBlock").AddTime(start)
	}()

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
			return nil, 0, store.ErrBlockNotFound
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

	return block, height, nil
}

func (s *SQL) GetLastNBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blockchain").NewStat("GetLastNBlocks").AddTime(start)
	}()

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
		,b.height
		,b.inserted_at
		FROM blocks b
		--WHERE orphaned = false
		ORDER BY b.height DESC
		LIMIT $1
	`

	rows, err := s.db.QueryContext(ctx, q, n)
	if err != nil {
		return nil, err
	}

	blockInfos := make([]*model.BlockInfo, 0)

	for rows.Next() {
		var hashPrevBlock []byte
		var hashMerkleRoot []byte
		var coinbaseBytes []byte
		var nBits []byte
		var seenAt CustomTime

		header := &model.BlockHeader{}
		info := &model.BlockInfo{}

		if err = rows.Scan(
			&header.Version,
			&header.Timestamp,
			&nBits,
			&header.Nonce,
			&hashPrevBlock,
			&hashMerkleRoot,
			&info.TransactionCount,
			&info.Size,
			&coinbaseBytes,
			&info.Height,
			&seenAt,
		); err != nil {
			return nil, err
		}

		header.Bits = model.NewNBitFromSlice(nBits)

		header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
		}

		header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, fmt.Errorf("failed to convert hashMerkleRoot: %w", err)
		}

		info.BlockHeader = header.Bytes()

		coinbaseTx, err := bt.NewTxFromBytes(coinbaseBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to convert coinbaseTx: %w", err)
		}

		// Add up the sum of the coinbase tx outputs.
		info.CoinbaseValue = 0
		for _, output := range coinbaseTx.Outputs {
			info.CoinbaseValue += output.Satoshis
		}

		info.SeenAt = timestamppb.New(seenAt.Time)

		blockInfos = append(blockInfos, info)
	}

	return blockInfos, nil
}

/* The following code exists to be able to handle the fact that the inserted_at is a TEXT field in
   sqlite and a TIMESTAMP field in postgres. This is because the sqlite driver does not support
	 TIMESTAMP fields.
*/

const SQLiteTimestampFormat = "2006-01-02 15:04:05"

type CustomTime struct {
	time.Time
}

// Scan implements the sql.Scanner interface.
func (ct *CustomTime) Scan(value interface{}) error {
	switch v := value.(type) {
	case time.Time:
		ct.Time = v
		return nil
	case []byte:
		t, err := time.Parse(SQLiteTimestampFormat, string(v))
		if err != nil {
			return err
		}
		ct.Time = t
		return nil
	case string:
		t, err := time.Parse(SQLiteTimestampFormat, v)
		if err != nil {
			return err
		}
		ct.Time = t
		return nil
	}
	return fmt.Errorf("unsupported type: %T", value)
}

// Value implements the driver.Valuer interface.
func (ct CustomTime) Value() (driver.Value, error) {
	return ct.Time, nil
}
