package sql

import (
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/model/time"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// processBlockRows processes SQL query results for blocks and converts them to model.BlockInfo objects.
// If includeHash is true, it expects the hash field to be included in the query results.
func (s *SQL) processBlockRows(rows *sql.Rows) ([]*model.BlockInfo, error) {
	blockInfos := make([]*model.BlockInfo, 0)

	for rows.Next() {
		info, err := s.scanBlockRow(rows)
		if err != nil {
			return nil, err
		}

		blockInfos = append(blockInfos, info)
	}

	return blockInfos, nil
}

// scanBlockRow scans a single row from the SQL result set and converts it to a BlockInfo object.
func (s *SQL) scanBlockRow(rows *sql.Rows) (*model.BlockInfo, error) {
	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		coinbaseBytes  []byte
		nBits          []byte
		seenAt         time.CustomTime
	)

	header := &model.BlockHeader{}
	info := &model.BlockInfo{}

	err := rows.Scan(
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
	)

	if err != nil {
		return nil, errors.NewStorageError("failed to scan row", err)
	}

	// Process nBits
	bits, _ := model.NewNBitFromSlice(nBits)
	header.Bits = *bits

	// Process previous block hash
	header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
	}

	// Process merkle root hash
	header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
	}

	// Set the block header bytes
	info.BlockHeader = header.Bytes()

	// Process coinbase transaction if available
	if len(coinbaseBytes) > 0 {
		// Parse coinbase transaction
		coinbaseTx, err := bt.NewTxFromBytes(coinbaseBytes)
		if err != nil {
			return nil, errors.NewProcessingError("failed to convert coinbaseTx", err)
		}

		// Calculate coinbase value
		info.CoinbaseValue = 0
		for _, output := range coinbaseTx.Outputs {
			info.CoinbaseValue += output.Satoshis
		}

		// Extract miner information
		info.Miner, err = util.ExtractCoinbaseMiner(coinbaseTx)
		if err != nil {
			return nil, errors.NewProcessingError("failed to extract miner", err)
		}
	}

	// Set the timestamp
	info.SeenAt = timestamppb.New(seenAt.Time)

	return info, nil
}
