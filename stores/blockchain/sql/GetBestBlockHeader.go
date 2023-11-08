package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type getBestBlockHeaderCache struct {
	blockHeader *model.BlockHeader
	meta        *model.BlockHeaderMeta
}

func (s *SQL) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	start := gocore.CurrentNanos()
	defer func() {
		stats.NewStat("GetBlock").AddTime(start)
	}()

	cached, ok := cache.Load("GetBestBlockHeader")
	if ok {
		s.logger.Debugf("GetBestBlockHeader cache hit")
		return cached.(*getBestBlockHeaderCache).blockHeader, cached.(*getBestBlockHeaderCache).meta, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// TODO prefer our own blocks over the ones from the network
	//      exclude invalid blocks
	q := `
		SELECT
	   b.version
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
		ORDER BY chain_work DESC, id ASC
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
			return nil, nil, fmt.Errorf("error in GetBestBlockHeader: %w", err)
		}
		return nil, nil, err
	}

	blockHeader.Bits = model.NewNBitFromSlice(nBits)

	blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
	}
	blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert hashMerkleRoot: %w", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbaseBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert coinbaseTx: %w", err)
	}

	miner, err := util.ExtractCoinbaseMiner(coinbaseTx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to extract miner: %w", err)
	}

	blockHeaderMeta.Miner = miner

	// set cache
	cache.Store("GetBestBlockHeader", &getBestBlockHeaderCache{
		blockHeader: blockHeader,
		meta:        blockHeaderMeta,
	})

	return blockHeader, blockHeaderMeta, nil
}
