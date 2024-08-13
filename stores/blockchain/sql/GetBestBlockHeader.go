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

func (s *SQL) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBestBlockHeader")
	defer deferFn()

	header, meta, er := s.blocksCache.GetBestBlockHeader()
	if er != nil {
		return nil, nil, errors.NewStorageError("error in GetBestBlockHeader", er)
	}
	if header != nil {
		return header, meta, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
		 b.id
	    ,b.version
		,b.block_time
	    ,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.n_bits
		,b.height
		,b.tx_count
		,b.size_in_bytes
		,b.coinbase_tx
		FROM blocks b
		WHERE invalid = false
		ORDER BY chain_work DESC, peer_id ASC, id ASC
		LIMIT 1
	`

	blockHeader := &model.BlockHeader{}
	blockHeaderMeta := &model.BlockHeaderMeta{}

	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var nBits []byte
	var coinbaseBytes []byte

	var err error
	if err = s.db.QueryRowContext(ctx, q).Scan(
		&blockHeaderMeta.ID,
		&blockHeader.Version,
		&blockHeader.Timestamp,
		&blockHeader.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&nBits,
		&blockHeaderMeta.Height,
		&blockHeaderMeta.TxCount,
		&blockHeaderMeta.SizeInBytes,
		&coinbaseBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, errors.NewStorageError("error in GetBestBlockHeader", err)
		}
		return nil, nil, err
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	blockHeader.Bits = *bits

	blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, nil, errors.NewStorageError("failed to convert hashPrevBlock", err)
	}
	blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, nil, errors.NewStorageError("failed to convert hashMerkleRoot", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbaseBytes)
	if err != nil {
		return nil, nil, errors.NewStorageError("failed to convert coinbaseTx", err)
	}

	miner, err := util.ExtractCoinbaseMiner(coinbaseTx)
	if err != nil {
		return nil, nil, errors.NewStorageError("failed to extract miner", err)
	}

	blockHeaderMeta.Miner = miner

	return blockHeader, blockHeaderMeta, nil
}
