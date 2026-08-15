package packedsql

import (
	"bytes"
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
)

type packedRow struct {
	hash                   []byte
	flags                  int16
	coinbaseSpendingHeight int64
	totalCount             int32
	page0Count             int32
	spentCount             int32
	pagesTotal             int32
	pagesSpent             int32
	spends                 []byte
	blockRefs              []byte
	deleteAtHeight         *int64
	unminedSince           *int64
	preserveUntil          *int64
	version                int64
	lockTime               int64
	fee                    int64
	sizeInBytes            int64
	createdAt              int64
	utxoHashes             []byte
	inputs                 []byte
	outputs                []byte
}

const selectRowSQL = `SELECT hash, flags, coinbase_spending_height, total_count, page0_count,
    spent_count, pages_total, pages_spent, spends, block_refs, delete_at_height,
    unmined_since, preserve_until, version, lock_time, fee, size_in_bytes, created_at,
    utxo_hashes, inputs, outputs
FROM packed_txs WHERE hash = $1`

func scanPackedRow(row pgx.Row) (*packedRow, error) {
	r := &packedRow{}

	err := row.Scan(&r.hash, &r.flags, &r.coinbaseSpendingHeight, &r.totalCount, &r.page0Count,
		&r.spentCount, &r.pagesTotal, &r.pagesSpent, &r.spends, &r.blockRefs, &r.deleteAtHeight,
		&r.unminedSince, &r.preserveUntil, &r.version, &r.lockTime, &r.fee, &r.sizeInBytes, &r.createdAt,
		&r.utxoHashes, &r.inputs, &r.outputs)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (s *Store) fetchRow(ctx context.Context, hash *chainhash.Hash) (*packedRow, error) {
	r, err := scanPackedRow(s.pool.QueryRow(ctx, selectRowSQL, hash[:]))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewTxNotFoundError("packedsql: transaction %s not found", hash, err)
		}

		return nil, errors.NewStorageError("packedsql: failed to fetch transaction %s", hash, err)
	}

	return r, nil
}

func (s *Store) fetchPages(ctx context.Context, hash *chainhash.Hash) ([]pageRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT page, spendable_count, spends, utxo_hashes FROM packed_tx_pages WHERE hash = $1 ORDER BY page`,
		hash[:])
	if err != nil {
		return nil, errors.NewStorageError("packedsql: failed to fetch pages for %s", hash, err)
	}

	defer rows.Close()

	var pages []pageRow

	for rows.Next() {
		var p pageRow
		if err = rows.Scan(&p.page, &p.spendableCount, &p.spends, &p.utxoHashes); err != nil {
			return nil, errors.NewStorageError("packedsql: failed to scan page for %s", hash, err)
		}

		pages = append(pages, p)
	}

	return pages, rows.Err()
}

func containsField(slice []fields.FieldName, item fields.FieldName) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}

	return false
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(f) > 0 {
		bins = f
	}

	r, err := s.fetchRow(ctx, hash)
	if err != nil {
		return nil, err
	}

	return s.rowToMeta(ctx, r, bins)
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	result, err := s.Get(ctx, hash, utxo.MetaFields...)
	if err != nil {
		return err
	}

	if result != nil {
		*data = *result
	}

	return nil
}

func (s *Store) rowToMeta(ctx context.Context, r *packedRow, bins []fields.FieldName) (*meta.Data, error) {
	data := &meta.Data{
		Fee:         uint64(r.fee),         //nolint:gosec
		SizeInBytes: uint64(r.sizeInBytes), //nolint:gosec
		IsCoinbase:  r.flags&flagCoinbase != 0,
		Frozen:      r.flags&flagFrozen != 0,
		Conflicting: r.flags&flagConflicting != 0,
		Locked:      r.flags&flagLocked != 0,
		CreatedAt:   r.createdAt,
	}

	if r.unminedSince != nil && *r.unminedSince >= 0 && *r.unminedSince <= 0xFFFFFFFF {
		data.UnminedSince = uint32(*r.unminedSince) //nolint:gosec
	}

	tx := bt.Tx{
		Version:  uint32(r.version),  //nolint:gosec
		LockTime: uint32(r.lockTime), //nolint:gosec
	}

	needInputs := containsField(bins, fields.Tx) || containsField(bins, fields.Inputs) ||
		containsField(bins, fields.TxInpoints) || containsField(bins, fields.Utxos)
	if needInputs {
		n := offsetBlobCount(r.inputs)
		tx.Inputs = make([]*bt.Input, n)

		for i := 0; i < n; i++ {
			item, err := offsetBlobItem(r.inputs, i)
			if err != nil {
				return nil, err
			}

			tx.Inputs[i] = &bt.Input{}
			if _, err = tx.Inputs[i].ReadFromExtended(bytes.NewReader(item)); err != nil {
				return nil, errors.NewTxInvalidError("packedsql: could not read input %d", i, err)
			}
		}
	}

	if containsField(bins, fields.Tx) || containsField(bins, fields.Outputs) || containsField(bins, fields.Utxos) {
		n := offsetBlobCount(r.outputs)
		tx.Outputs = make([]*bt.Output, n)

		for i := 0; i < n; i++ {
			item, err := offsetBlobItem(r.outputs, i)
			if err != nil {
				return nil, err
			}

			tx.Outputs[i] = &bt.Output{}
			if _, err = tx.Outputs[i].ReadFrom(bytes.NewReader(item)); err != nil {
				return nil, errors.NewTxInvalidError("packedsql: could not read output %d", i, err)
			}
		}
	}

	if containsField(bins, fields.BlockIDs) || containsField(bins, fields.BlockHeights) || containsField(bins, fields.SubtreeIdxs) {
		data.BlockIDs, data.BlockHeights, data.SubtreeIdxs = unpackBlockRefs(r.blockRefs)
	}

	if containsField(bins, fields.ConflictingChildren) {
		children, err := s.conflictingChildrenOf(ctx, r.hash)
		if err != nil {
			return nil, err
		}

		data.ConflictingChildren = children
	}

	if containsField(bins, fields.Utxos) {
		if err := s.fillSpendingDatas(ctx, r, data, len(tx.Outputs)); err != nil {
			return nil, err
		}
	}

	if containsField(bins, fields.Tx) {
		data.Tx = &tx
	}

	if containsField(bins, fields.TxInpoints) {
		txInpoints, err := subtree.NewTxInpointsFromInputs(tx.Inputs)
		if err != nil {
			return nil, errors.NewProcessingError("packedsql: failed to create tx inpoints from inputs", err)
		}

		data.TxInpoints = txInpoints
	}

	return data, nil
}

func (s *Store) conflictingChildrenOf(ctx context.Context, hash []byte) ([]chainhash.Hash, error) {
	rows, err := s.pool.Query(ctx, `SELECT child_hash FROM conflicting_children WHERE hash = $1`, hash)
	if err != nil {
		return nil, errors.NewStorageError("packedsql: failed to fetch conflicting children", err)
	}

	defer rows.Close()

	children := make([]chainhash.Hash, 0, 16)

	for rows.Next() {
		var child []byte
		if err = rows.Scan(&child); err != nil {
			return nil, err
		}

		children = append(children, chainhash.Hash(child))
	}

	return children, rows.Err()
}

func (s *Store) frozenVouts(ctx context.Context, hash []byte) (map[uint32]bool, error) {
	if s == nil {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx, `SELECT vout FROM utxo_overrides WHERE hash = $1 AND frozen`, hash)
	if err != nil {
		return nil, errors.NewStorageError("packedsql: failed to fetch overrides", err)
	}

	defer rows.Close()

	frozen := make(map[uint32]bool)

	for rows.Next() {
		var vout uint32
		if err = rows.Scan(&vout); err != nil {
			return nil, err
		}

		frozen[vout] = true
	}

	return frozen, rows.Err()
}

func (s *Store) fillSpendingDatas(ctx context.Context, r *packedRow, data *meta.Data, outputCount int) error {
	spendBlobs := [][]byte{r.spends}

	if r.pagesTotal > 0 {
		hash := chainhash.Hash(r.hash)

		pages, err := s.fetchPages(ctx, &hash)
		if err != nil {
			return err
		}

		for _, p := range pages {
			spendBlobs = append(spendBlobs, p.spends)
		}
	}

	var frozen map[uint32]bool

	if r.flags&flagHasOverrides != 0 {
		var err error
		if frozen, err = s.frozenVouts(ctx, r.hash); err != nil {
			return err
		}
	}

	data.SpendingDatas = make([]*spendpkg.SpendingData, outputCount)
	txFrozen := r.flags&flagFrozen != 0

	for v := 0; v < outputCount; v++ {
		page := pageOfVout(uint32(v), s.pageSize) //nolint:gosec
		slot := slotOfVout(uint32(v), s.pageSize) //nolint:gosec

		if int(page) >= len(spendBlobs) {
			break
		}

		blob := spendBlobs[page]
		off := int(slot) * slotSpendSize

		if off+slotSpendSize > len(blob) {
			continue
		}

		if txFrozen || frozen[uint32(v)] { //nolint:gosec
			data.SpendingDatas[v] = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, v)
			continue
		}

		data.SpendingDatas[v] = unpackSpendingData(blob[off : off+slotSpendSize])
	}

	return nil
}

func (s *Store) GetSpend(ctx context.Context, sp *utxo.Spend) (*utxo.SpendResponse, error) {
	page := pageOfVout(sp.Vout, s.pageSize)
	slot := slotOfVout(sp.Vout, s.pageSize)
	hashFrom := int(slot)*slotHashSize + 1
	spendFrom := int(slot)*slotSpendSize + 1

	var (
		storedHash             []byte
		storedSpend            []byte
		flags                  int16
		coinbaseSpendingHeight int64
		err                    error
	)

	if page == 0 {
		err = s.pool.QueryRow(ctx,
			`SELECT substring(utxo_hashes FROM $2 FOR 32), substring(spends FROM $3 FOR 36), flags, coinbase_spending_height
			 FROM packed_txs WHERE hash = $1`,
			sp.TxID[:], hashFrom, spendFrom).Scan(&storedHash, &storedSpend, &flags, &coinbaseSpendingHeight)
	} else {
		err = s.pool.QueryRow(ctx,
			`SELECT substring(p.utxo_hashes FROM $3 FOR 32), substring(p.spends FROM $4 FOR 36), m.flags, m.coinbase_spending_height
			 FROM packed_tx_pages p JOIN packed_txs m ON m.hash = p.hash
			 WHERE p.hash = $1 AND p.page = $2`,
			sp.TxID[:], page, hashFrom, spendFrom).Scan(&storedHash, &storedSpend, &flags, &coinbaseSpendingHeight)
	}

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil
		}

		return nil, errors.NewStorageError("packedsql: failed to read spend %s:%d", sp.TxID, sp.Vout, err)
	}

	if sp.UTXOHash != nil && !bytes.Equal(storedHash, sp.UTXOHash[:]) {
		return nil, errors.NewUtxoHashMismatchError("packedsql: utxo hash mismatch for %s:%d", sp.TxID, sp.Vout)
	}

	spendingData := unpackSpendingData(storedSpend)
	utxoStatus := utxo.CalculateUtxoStatus(spendingData, uint32(coinbaseSpendingHeight), s.GetBlockHeight()) //nolint:gosec

	frozen := flags&flagFrozen != 0

	var spendableIn *int64

	if flags&flagHasOverrides != 0 {
		var oFrozen bool

		err = s.pool.QueryRow(ctx,
			`SELECT frozen, spendable_in FROM utxo_overrides WHERE hash = $1 AND vout = $2`,
			sp.TxID[:], sp.Vout).Scan(&oFrozen, &spendableIn)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewStorageError("packedsql: failed to read override %s:%d", sp.TxID, sp.Vout, err)
		}

		frozen = frozen || oFrozen
	}

	if frozen {
		utxoStatus = utxo.Status_FROZEN
		spendingData = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, int(sp.Vout))
	}

	if flags&flagConflicting != 0 {
		utxoStatus = utxo.Status_CONFLICTING
	}

	if flags&flagLocked != 0 {
		utxoStatus = utxo.Status_LOCKED
	}

	if spendableIn != nil && int64(s.GetBlockHeight()) < *spendableIn {
		utxoStatus = utxo.Status_IMMATURE
	}

	return &utxo.SpendResponse{
		Status:       int(utxoStatus),
		SpendingData: spendingData,
		LockTime:     uint32(coinbaseSpendingHeight), //nolint:gosec
	}, nil
}
