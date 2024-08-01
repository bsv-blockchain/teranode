package sql

import (
	"context"
	"database/sql"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	start, stat, ctx := tracing.StartStatFromContext(ctx, "GetBlockHeader")
	defer func() {
		stat.AddTime(start)
	}()

	header, meta, er := s.blocksCache.GetBlockHeader(*blockHash)
	if er != nil {
		return nil, nil, errors.NewStorageError("error in GetBlockHeader", er)
	}
	if header != nil {
		return header, meta, nil
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
		FROM blocks b
		WHERE b.hash = $1
	`

	blockHeader := &model.BlockHeader{}
	blockHeaderMeta := &model.BlockHeaderMeta{}

	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var nBits []byte
	var coinbaseBytes []byte

	var err error
	if err = s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&blockHeader.Version,
		&blockHeader.Timestamp,
		&nBits,
		&blockHeader.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&blockHeaderMeta.SizeInBytes,
		&coinbaseBytes,
		&blockHeaderMeta.Height,
		&blockHeaderMeta.TxCount,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, errors.NewStorageError("error in GetBlockHeader", errors.ErrNotFound)
		}
		return nil, nil, err
	}

	blockHeader.Bits = model.NewNBitFromSlice(nBits)

	blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
	}
	blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbaseBytes)
	if err != nil {
		return nil, nil, errors.NewProcessingError("failed to convert coinbaseTx", err)
	}

	miner, err := util.ExtractCoinbaseMiner(coinbaseTx)
	if err != nil {
		return nil, nil, errors.NewProcessingError("failed to extract miner", err)
	}

	blockHeaderMeta.Miner = miner

	return blockHeader, blockHeaderMeta, nil
}
