package packedsql

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
)

type masterRow struct {
	hash                   []byte
	flags                  int16
	coinbaseSpendingHeight int64
	totalCount             int32
	page0Count             int32
	pagesTotal             int32
	spends                 []byte
	blockRefs              []byte
	deleteAtHeight         *int64
	unminedSince           *int64
	version                int64
	lockTime               int64
	fee                    int64
	sizeInBytes            int64
	createdAt              int64
	utxoHashes             []byte
	inputs                 []byte
	outputs                []byte
}

type pageRow struct {
	page           uint32
	spendableCount int32
	spends         []byte
	utxoHashes     []byte
}

type txRows struct {
	master masterRow
	pages  []pageRow
}

const insertMasterSQL = `INSERT INTO packed_txs (hash, flags, coinbase_spending_height, total_count, page0_count,
    spent_count, pages_total, pages_spent, spends, block_refs, delete_at_height,
    unmined_since, preserve_until, version, lock_time, fee, size_in_bytes, created_at,
    utxo_hashes, inputs, outputs)
VALUES ($1,$2,$3,$4,$5,0,$6,0,$7,$8,$9,$10,NULL,$11,$12,$13,$14,$15,$16,$17,$18)
ON CONFLICT (hash) DO NOTHING`

const insertPageSQL = `INSERT INTO packed_tx_pages (hash, page, spendable_count, spent_count, spends, utxo_hashes)
VALUES ($1,$2,$3,0,$4,$5)
ON CONFLICT (hash, page) DO NOTHING`

func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	rows, md, err := s.buildTxRows(tx, blockHeight, options)
	if err != nil {
		return nil, err
	}

	if err = s.insertTxRows(ctx, rows); err != nil {
		return nil, err
	}

	return md, nil
}

func (s *Store) buildTxRows(tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*txRows, *meta.Data, error) {
	var (
		md  *meta.Data
		err error
	)

	if options.SkipExtendedInputs {
		md, err = util.TxMetaDataFromTxNoFee(tx)
	} else {
		md, err = util.TxMetaDataFromTx(tx)
	}

	if err != nil {
		return nil, nil, errors.NewProcessingError("packedsql: failed to build tx meta data", err)
	}

	if options.Conflicting {
		md.Conflicting = true
	}

	if options.Locked {
		md.Locked = true
	}

	txHash := tx.TxIDChainHash()
	if options.TxID != nil {
		txHash = options.TxID
	}

	isCoinbase := tx.IsCoinbase()
	if options.IsCoinbase != nil {
		isCoinbase = *options.IsCoinbase
	}

	var flags int16

	var coinbaseSpendingHeight int64

	if isCoinbase {
		flags |= flagCoinbase
		coinbaseSpendingHeight = int64(blockHeight) + int64(s.settings.ChainCfgParams.CoinbaseMaturity)
	}

	if options.Frozen {
		flags |= flagFrozen
	}

	if options.Conflicting {
		flags |= flagConflicting
	}

	if options.Locked {
		flags |= flagLocked
	}

	outputCount := uint32(len(tx.Outputs)) //nolint:gosec
	pageSize := s.pageSize
	pagesTotal := uint32(0)

	if outputCount > pageSize {
		pagesTotal = (outputCount - 1) / pageSize
	}

	rows := &txRows{
		master: masterRow{
			hash:                   txHash[:],
			flags:                  flags,
			coinbaseSpendingHeight: coinbaseSpendingHeight,
			pagesTotal:             int32(pagesTotal), //nolint:gosec
			version:                int64(tx.Version),
			lockTime:               int64(tx.LockTime),
			fee:                    int64(md.Fee),         //nolint:gosec
			sizeInBytes:            int64(md.SizeInBytes), //nolint:gosec
			createdAt:              time.Now().UnixMilli(),
			inputs:                 packInputs(tx, options.SkipExtendedInputs),
			outputs:                packOutputs(tx),
		},
		pages: make([]pageRow, 0, pagesTotal),
	}

	genesisHeight := s.settings.ChainCfgParams.GenesisActivationHeight
	totalSpendable := int32(0)

	for page := uint32(0); page <= pagesTotal; page++ {
		start := page * pageSize
		end := min(start+pageSize, outputCount)
		slots := end - start
		spends := make([]byte, int(slots)*slotSpendSize)
		hashes := make([]byte, int(slots)*slotHashSize)
		spendable := int32(0)

		for v := start; v < end; v++ {
			output := tx.Outputs[v]
			if output == nil || !utxo.ShouldStoreOutputAsUTXO(output, blockHeight, genesisHeight) {
				continue
			}

			utxoHash, err := util.UTXOHashFromOutput(txHash, output, v)
			if err != nil {
				return nil, nil, errors.NewProcessingError("packedsql: failed to compute utxo hash for vout %d", v, err)
			}

			copy(hashes[(v-start)*slotHashSize:], utxoHash[:])

			spendable++
		}

		totalSpendable += spendable

		if page == 0 {
			rows.master.page0Count = spendable
			rows.master.spends = spends
			rows.master.utxoHashes = hashes
		} else {
			rows.pages = append(rows.pages, pageRow{
				page:           page,
				spendableCount: spendable,
				spends:         spends,
				utxoHashes:     hashes,
			})
		}
	}

	rows.master.totalCount = totalSpendable

	if len(options.MinedBlockInfos) > 0 {
		rows.master.blockRefs = packBlockRefs(options.MinedBlockInfos)
	} else {
		unminedSince := int64(blockHeight)
		rows.master.unminedSince = &unminedSince
	}

	if dah := s.unspendableMinedTxDAH(totalSpendable, blockHeight, options); dah != nil {
		rows.master.deleteAtHeight = dah
	}

	return rows, md, nil
}

func (s *Store) unspendableMinedTxDAH(totalSpendable int32, blockHeight uint32, options *utxo.CreateOptions) *int64 {
	if options.Conflicting || len(options.MinedBlockInfos) == 0 || totalSpendable > 0 {
		return nil
	}

	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	if retention == 0 {
		return nil
	}

	dah := int64(blockHeight) + int64(retention)

	return &dah
}

func packInputs(tx *bt.Tx, skipExtended bool) []byte {
	items := make([][]byte, len(tx.Inputs))

	for i, input := range tx.Inputs {
		if skipExtended {
			stripped := *input
			stripped.PreviousTxSatoshis = 0
			stripped.PreviousTxScript = nil
			items[i] = stripped.ExtendedBytes(false)
		} else {
			items[i] = input.ExtendedBytes(false)
		}
	}

	return packOffsetBlob(items)
}

func packOutputs(tx *bt.Tx) []byte {
	items := make([][]byte, len(tx.Outputs))

	for i, output := range tx.Outputs {
		items[i] = output.Bytes()
	}

	return packOffsetBlob(items)
}

func (s *Store) insertTxRows(ctx context.Context, rows *txRows) error {
	if len(rows.pages) == 0 {
		return s.insertTxRowsOn(ctx, s.pool, rows)
	}

	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin create transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	if err = s.insertTxRowsOn(ctx, dbTx, rows); err != nil {
		return err
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("packedsql: failed to commit create transaction", err)
	}

	return nil
}

func (s *Store) insertTxRowsOn(ctx context.Context, q pgxQuerier, rows *txRows) error {
	m := &rows.master

	args := []any{
		m.hash, m.flags, m.coinbaseSpendingHeight, m.totalCount, m.page0Count,
		m.pagesTotal, m.spends, m.blockRefs, m.deleteAtHeight, m.unminedSince,
		m.version, m.lockTime, m.fee, m.sizeInBytes, m.createdAt,
		m.utxoHashes, m.inputs, m.outputs,
	}

	ct, err := q.Exec(ctx, insertMasterSQL, args...)
	if err != nil {
		return errors.NewStorageError("packedsql: failed to insert transaction", err)
	}

	if ct.RowsAffected() == 0 {
		return errors.NewTxExistsError("packedsql: transaction %s already exists", chainhash.Hash(m.hash[:slotHashSize]))
	}

	for _, p := range rows.pages {
		if _, err = q.Exec(ctx, insertPageSQL, m.hash, p.page, p.spendableCount, p.spends, p.utxoHashes); err != nil {
			return errors.NewStorageError("packedsql: failed to insert page %d", p.page, err)
		}
	}

	return nil
}
