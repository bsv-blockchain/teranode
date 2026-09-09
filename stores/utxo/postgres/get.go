package postgres

import (
	"bytes"
	"context"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// maxINClauseSize limits the number of hashes per IN clause to avoid exceeding
// Postgres parameter limits.
const maxINClauseSize = 400

// minDecorateChunkSize is the floor on the per-chunk hash count when
// BatchPreviousOutputsDecorate splits work across concurrent queries. Chunks
// smaller than this add round-trip/planning overhead that outweighs the extra
// parallelism, so very small blocks stay in fewer chunks.
const minDecorateChunkSize = 50

// batchGetItem represents a single Get request queued into the batcher.
type batchGetItem struct {
	hash *chainhash.Hash
	bins []fields.FieldName
	done chan batchGetResult
}

type batchGetResult struct {
	Data *meta.Data
	Err  error
}

// Get retrieves UTXO metadata for a given transaction hash.
// When getBatcher is active, simple gets are batched for throughput.
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, requestedFields ...fields.FieldName) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(requestedFields) > 0 {
		bins = requestedFields
	}

	// Use the batcher only for fields the batch SELECT actually populates. Any
	// field needing a column the batch query omits (Tx body, ConflictingChildren)
	// must fall through to getInternal or it comes back silently zero-valued.
	if s.getBatcher != nil && !contains(bins, fields.Tx) && !contains(bins, fields.Outputs) &&
		!contains(bins, fields.Utxos) && !contains(bins, fields.TxInpoints) && !contains(bins, fields.Inputs) &&
		!contains(bins, fields.ConflictingChildren) {
		done := make(chan batchGetResult, 1)
		s.getBatcher.Put(&batchGetItem{hash: hash, bins: bins, done: done})
		select {
		case result := <-done:
			return result.Data, result.Err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return s.getInternal(ctx, hash, bins)
}

// sendGetBatch pipelines N SELECT queries via SendBatch.
func (s *Store) sendGetBatch(batch []*batchGetItem) {
	s.batchStats.getItems.Add(int64(len(batch)))
	s.batchStats.getBatches.Add(1)
	ctx := context.Background()

	// Single-item fast path: direct query, no SendBatch overhead.
	if len(batch) == 1 {
		result, err := s.getInternal(ctx, batch[0].hash, batch[0].bins)
		batch[0].done <- batchGetResult{Data: result, Err: err}
		return
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		for _, item := range batch {
			item.done <- batchGetResult{Err: errors.NewStorageError("[Get] acquire", err)}
		}
		return
	}
	defer conn.Release()

	// One ANY-array query instead of N pipelined SELECTs: a single round-trip,
	// one plan lookup, and one index probe per partition rather than N. At 10K
	// workers this removes ~92K plan-cycle + per-statement protocol costs/sec
	// from the server, which is the dominant Get-side overhead at scale.
	hashes := make([][]byte, len(batch))
	for i, item := range batch {
		hashes[i] = item.hash[:]
	}

	rows, err := conn.Query(ctx, `
		SELECT hash, version, lock_time, fee, size_in_bytes, coinbase,
		       locked, conflicting, frozen, unmined_since,
		       mined_info
		FROM unnest($1::bytea[]) AS h(v) JOIN txs ON txs.hash = h.v`,
		hashes,
	)
	if err != nil {
		for _, item := range batch {
			item.done <- batchGetResult{Err: errors.NewStorageError("[Get] batch query", err)}
		}
		return
	}

	// Dispatch by hash: a single query returns one row per DISTINCT hash, so a
	// batch containing the same hash twice still maps both items to that row.
	found := make(map[chainhash.Hash]*meta.Data, len(batch))
	for rows.Next() {
		data := &meta.Data{}
		var hashBytes []byte
		var version, lockTime int64
		var unminedSince *int64
		var minedInfo []byte

		if scanErr := rows.Scan(&hashBytes, &version, &lockTime, &data.Fee, &data.SizeInBytes,
			&data.IsCoinbase, &data.Locked, &data.Conflicting, &data.Frozen,
			&unminedSince, &minedInfo); scanErr != nil {
			rows.Close()
			for _, item := range batch {
				item.done <- batchGetResult{Err: errors.NewStorageError("[Get] batch scan", scanErr)}
			}
			return
		}

		if unminedSince != nil {
			data.UnminedSince = uint32(*unminedSince)
		}
		data.BlockIDs, data.BlockHeights, data.SubtreeIdxs = decodeMinedInfo(minedInfo)

		var h chainhash.Hash
		copy(h[:], hashBytes)
		found[h] = data
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		for _, item := range batch {
			item.done <- batchGetResult{Err: errors.NewStorageError("[Get] batch rows", err)}
		}
		return
	}
	rows.Close()

	for _, item := range batch {
		if data, ok := found[*item.hash]; ok {
			item.done <- batchGetResult{Data: data}
		} else {
			item.done <- batchGetResult{Err: errors.NewTxNotFoundError("transaction %s not found", item.hash)}
		}
	}
}

// GetMeta retrieves only the metadata for a transaction (no Tx body).
func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	result, err := s.getInternal(ctx, hash, utxo.MetaFields)
	if err != nil {
		return err
	}
	if result != nil {
		*data = *result
	}
	return nil
}

// getInternal is the shared get implementation used by Get and GetMeta.
func (s *Store) getInternal(ctx context.Context, hash *chainhash.Hash, bins []fields.FieldName) (*meta.Data, error) {
	data := &meta.Data{}

	// Single SELECT from txs — all metadata, state, raw_tx, and packed columns.
	var (
		version             int64
		lockTime            int64
		unminedSince        *int64
		rawTx               []byte
		minedInfo           []byte
		conflictingChildren [][]byte
		outCount            int32
		outFrozensBitmap    []byte // nil = no output frozen
	)

	// Fetch raw_tx / conflicting_children only when a requested field consumes
	// them. The CASE is evaluated lazily per row, so when the flag is false the
	// column reference never runs and a TOASTed raw_tx is NOT detoasted — a
	// metadata-only Get of a wide tx previously dragged the whole out-of-line
	// blob (many TOAST chunk pages) off disk to throw it away.
	needRawTx := contains(bins, fields.Tx) || contains(bins, fields.Inputs) ||
		contains(bins, fields.TxInpoints) || contains(bins, fields.Utxos) ||
		contains(bins, fields.Outputs)
	needConflictingChildren := contains(bins, fields.ConflictingChildren)

	err := s.pool.QueryRow(ctx, `
		SELECT version, lock_time, fee, size_in_bytes, coinbase,
		       locked, conflicting, frozen, unmined_since,
		       CASE WHEN $2 THEN raw_tx END,
		       mined_info,
		       CASE WHEN $3 THEN conflicting_children END,
		       out_count, out_frozens
		FROM txs WHERE hash = $1`,
		hash[:], needRawTx, needConflictingChildren,
	).Scan(&version, &lockTime, &data.Fee, &data.SizeInBytes, &data.IsCoinbase,
		&data.Locked, &data.Conflicting, &data.Frozen, &unminedSince, &rawTx,
		&minedInfo, &conflictingChildren,
		&outCount, &outFrozensBitmap)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewTxNotFoundError("transaction %s not found", hash, err)
		}
		return nil, errors.NewStorageError("[Get] query tx %s", hash, err)
	}

	if unminedSince != nil {
		data.UnminedSince = uint32(*unminedSince)
	}

	// Deserialize raw_tx for Tx/Inputs/TxInpoints/Outputs/Utxos fields
	// (needRawTx computed above; rawTx is NULL when it was not requested).
	var tx *bt.Tx
	if rawTx != nil && needRawTx {
		tx, err = bt.NewTxFromBytes(rawTx)
		if err != nil {
			return nil, errors.NewProcessingError("failed to deserialize raw_tx", err)
		}
	} else {
		// Create a minimal Tx with version/locktime for output assembly.
		tx = &bt.Tx{
			Version:  uint32(version),
			LockTime: uint32(lockTime),
		}
	}
	// raw_tx contains locking_script + satoshis for every output; no
	// separate outputs-table query is needed.

	// Decode block info from the packed mined_info bytea.
	if contains(bins, fields.BlockIDs) {
		data.BlockIDs, data.BlockHeights, data.SubtreeIdxs = decodeMinedInfo(minedInfo)
	}

	// Fetch conflicting children from array.
	if contains(bins, fields.ConflictingChildren) && len(conflictingChildren) > 0 {
		data.ConflictingChildren = make([]chainhash.Hash, 0, len(conflictingChildren))
		for _, childBytes := range conflictingChildren {
			if len(childBytes) == chainhash.HashSize {
				data.ConflictingChildren = append(data.ConflictingChildren, chainhash.Hash(childBytes))
			}
		}
	}

	// Fetch UTXOs with spend status from spends table.
	// Frozen flag per output is unpacked from the out_frozens bitmap (already
	// scanned); no separate outputs-table query is needed for frozen.
	if contains(bins, fields.Utxos) {
		outFrozens := unpackBitmap(outFrozensBitmap, int(outCount))
		rows, err := s.pool.Query(ctx, `
			SELECT prev_output_idx, spending_data
			FROM spends
			WHERE prev_tx_hash = $1
			ORDER BY prev_output_idx`,
			hash[:],
		)
		if err != nil {
			return nil, errors.NewStorageError("[Get] query spends for %s", hash, err)
		}
		defer rows.Close()

		data.SpendingDatas = make([]*spendpkg.SpendingData, len(tx.Outputs))
		for rows.Next() {
			var (
				idx               int
				spendingDataBytes []byte
			)
			if err := rows.Scan(&idx, &spendingDataBytes); err != nil {
				return nil, errors.NewStorageError("[Get] scan spend row for %s", hash, err)
			}

			// prev_output_idx is an INT with no CHECK constraint in the spends
			// table; a corrupt, truncated, or orphaned spend row could carry an
			// index beyond this tx's output count. Guard before using it to
			// subscript data.SpendingDatas, or a malformed row turns a Get into an
			// index-out-of-range panic reachable from any caller-supplied tx hash.
			if idx < 0 || idx >= len(data.SpendingDatas) {
				return nil, errors.NewProcessingError("[Get] spends row for %s has out-of-bounds output index %d (tx has %d outputs)", hash, idx, len(data.SpendingDatas))
			}

			// Check per-output frozen from the unpacked bitmap.
			outputFrozen := data.Frozen
			if !outputFrozen && idx < len(outFrozens) {
				outputFrozen = outFrozens[idx]
			}

			if outputFrozen {
				data.SpendingDatas[idx] = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, idx)
			} else if spendingDataBytes != nil {
				sd, err := spendpkg.NewSpendingDataFromBytes(spendingDataBytes)
				if err != nil {
					return nil, errors.NewProcessingError("failed to parse spending data", err)
				}
				data.SpendingDatas[idx] = sd
			}
		}
		if err := rows.Err(); err != nil {
			return nil, errors.NewStorageError("[Get] iterate spends for %s", hash, err)
		}

		// Check frozen outputs not yet covered by a spends row (unspent but frozen).
		for i := range tx.Outputs {
			if data.SpendingDatas[i] == nil && i < len(outFrozens) && outFrozens[i] {
				data.SpendingDatas[i] = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, i)
			}
		}
	}

	// Attach Tx to meta if requested.
	if contains(bins, fields.Tx) {
		data.Tx = tx
	}

	// Build TxInpoints from inputs.
	if contains(bins, fields.TxInpoints) {
		var err error
		data.TxInpoints, err = subtree.NewTxInpointsFromInputs(tx.Inputs)
		if err != nil {
			return nil, errors.NewProcessingError("failed to create tx inpoints from inputs", err)
		}
	}

	return data, nil
}

// GetSpend retrieves the spend status for a specific UTXO.
// Packed form: reads the flat per-output columns on the txs row.
func (s *Store) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	// prev_output_idx is INT4 — reject out-of-range vouts at this entry point.
	voutInt32, err := voutToInt32(spend.Vout)
	if err != nil {
		return nil, err
	}

	var (
		utxoHashBytes          []byte
		coinbaseSpendingHeight int64 // scan as int64 to avoid uint32 truncation
		spendingDataBytes      []byte
		frozen                 bool
		spendableIn            *int32
		conflicting            bool
		locked                 bool
	)

	// O(1) packed access: 16-byte substr stride + get_bit bitmap probe.
	// Returns 0 rows when: tx is missing, OR output index is OOB ($2 >= out_count).
	// get_bit is only reached when $2 < out_count (WHERE guard), so it cannot
	// raise an out-of-range error.
	err = s.pool.QueryRow(ctx, `
		SELECT
		    substr(t.utxo_hashes, $2::int * 16 + 1, 16),
		    t.coinbase_spending_height,
		    sp.spending_data,
		    (t.out_frozens IS NOT NULL AND get_bit(t.out_frozens, $2::int) = 1) OR t.frozen,
		    CASE WHEN array_length(t.spendable_ins, 1) >= $2::int + 1 THEN t.spendable_ins[$2::int + 1] END,
		    t.conflicting, t.locked
		FROM txs t
		LEFT JOIN spends sp ON sp.prev_tx_hash = t.hash AND sp.prev_output_idx = $2
		WHERE t.hash = $1 AND $2::int < t.out_count`,
		spend.TxID[:], voutInt32,
	).Scan(&utxoHashBytes, &coinbaseSpendingHeight, &spendingDataBytes, &frozen, &spendableIn, &conflicting, &locked)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &utxo.SpendResponse{
				Status: int(utxo.Status_NOT_FOUND),
			}, nil
		}
		return nil, errors.NewStorageError("[GetSpend] query %s:%d", spend.TxID, spend.Vout, err)
	}

	// Validate UTXO hash matches — but only when the caller supplied one.
	// spend.UTXOHash is legitimately nil for callers that locate a UTXO by
	// (txid, vout) alone (e.g. the public /api/v1/utxos endpoint), so guard the
	// dereference to match the SQL reference store (sql/sql.go) and avoid an
	// externally-reachable nil-pointer panic. utxo_hashes stores a 16-byte prefix,
	// so compare against the caller hash's first 16 bytes.
	if spend.UTXOHash != nil && !bytes.Equal(utxoHashBytes, spend.UTXOHash[:16]) {
		return nil, errors.NewUtxoHashMismatchError("utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
	}

	var spendingData *spendpkg.SpendingData
	if len(spendingDataBytes) > 0 {
		spendingData, err = spendpkg.NewSpendingDataFromBytes(spendingDataBytes)
		if err != nil {
			return nil, errors.NewProcessingError("[GetSpend] parse spending data for %s:%d", spend.TxID, spend.Vout, err)
		}
	}

	// CalculateUtxoStatus expects uint32; coinbaseSpendingHeight fits since block
	// heights are well below 2^32.
	utxoStatus := utxo.CalculateUtxoStatus(spendingData, uint32(coinbaseSpendingHeight), s.GetBlockHeight())

	if frozen {
		utxoStatus = utxo.Status_FROZEN
		spendingData = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, int(spend.Vout))
	}
	if conflicting {
		utxoStatus = utxo.Status_CONFLICTING
	}
	if locked {
		utxoStatus = utxo.Status_LOCKED
	}
	if spendableIn != nil && s.GetBlockHeight() < uint32(*spendableIn) {
		utxoStatus = utxo.Status_IMMATURE
	}

	return &utxo.SpendResponse{
		Status:       int(utxoStatus),
		SpendingData: spendingData,
		LockTime:     uint32(coinbaseSpendingHeight),
	}, nil
}

// BatchDecorate efficiently fetches metadata for multiple transactions.
func (s *Store) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, requestedFields ...fields.FieldName) error {
	bins := utxo.MetaFieldsWithTx
	if len(requestedFields) > 0 {
		bins = requestedFields
	}

	// Filter out nil entries.
	items := make([]*utxo.UnresolvedMetaData, 0, len(unresolvedMetaDataSlice))
	for _, item := range unresolvedMetaDataSlice {
		if item != nil {
			items = append(items, item)
		}
	}
	if len(items) == 0 {
		return nil
	}

	// Process in chunks.
	for i := 0; i < len(items); i += maxINClauseSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := i + maxINClauseSize
		if end > len(items) {
			end = len(items)
		}

		if err := s.batchDecorateChunk(ctx, items[i:end], bins); err != nil {
			return err
		}
	}

	return nil
}

// batchDecorateChunk fetches metadata for a chunk of transactions using bulk queries.
func (s *Store) batchDecorateChunk(ctx context.Context, items []*utxo.UnresolvedMetaData, bins []fields.FieldName) error {
	// Build hash list and mapping.
	hashes := make([][]byte, len(items))
	hashToItems := make(map[chainhash.Hash][]*utxo.UnresolvedMetaData, len(items))
	for i, item := range items {
		hashes[i] = item.Hash[:]
		hashToItems[item.Hash] = append(hashToItems[item.Hash], item)
	}

	// Query 1: Bulk fetch from txs (metadata + state + raw_tx). unnest-JOIN keeps a
	// single stable prepared-plan entry regardless of batch size (like = ANY, unlike
	// a generated IN-list) AND drives per-row runtime partition pruning: = ANY on the
	// hash-partitioned parent descended ALL 8 leaf pkey btrees per probe (measured
	// ~4.9 descents per looked-up row); the nested-loop join descends exactly one.
	const q = `SELECT hash, version, lock_time, fee, size_in_bytes, coinbase,
	             locked, conflicting, frozen, unmined_since, raw_tx,
	             mined_info
	      FROM unnest($1::bytea[]) AS h(v)
	      JOIN txs ON txs.hash = h.v`

	rows, err := s.pool.Query(ctx, q, hashes)
	if err != nil {
		return errors.NewStorageError("[BatchDecorate] query txs", err)
	}

	hashToTx := make(map[chainhash.Hash]*txRow, len(items))

	for rows.Next() {
		var (
			hashBytes    []byte
			unminedSince *int64
			version      int64
			lockTime     int64
			rawTx        []byte
			minedInfo    []byte
		)
		row := &txRow{data: &meta.Data{}}
		if err := rows.Scan(&hashBytes, &version, &lockTime, &row.data.Fee, &row.data.SizeInBytes, &row.data.IsCoinbase,
			&row.data.Locked, &row.data.Conflicting, &row.data.Frozen, &unminedSince, &rawTx,
			&minedInfo); err != nil {
			rows.Close()
			return errors.NewStorageError("[BatchDecorate] scan txs row", err)
		}
		row.version = uint32(version)
		row.lockTime = uint32(lockTime)
		row.rawTx = rawTx
		copy(row.hash[:], hashBytes)
		if unminedSince != nil {
			row.data.UnminedSince = uint32(*unminedSince)
		}
		// Decode block info from the packed mined_info bytea onto the data object.
		row.data.BlockIDs, row.data.BlockHeights, row.data.SubtreeIdxs = decodeMinedInfo(minedInfo)
		hashToTx[row.hash] = row
	}
	// Check rows.Err() BEFORE treating absent hashes as not-found: a mid-stream
	// failure (connection reset, statement timeout) stops rows.Next() early with
	// the error parked here. Without this, a truncated result set would wrongly
	// mark existing transactions as TxNotFound and the function would return nil.
	if err := rows.Err(); err != nil {
		rows.Close()
		return errors.NewStorageError("[BatchDecorate] iterate txs", err)
	}
	rows.Close()

	// Mark not-found transactions.
	for _, item := range items {
		if _, found := hashToTx[item.Hash]; !found {
			item.Err = errors.NewTxNotFoundError("transaction %s not found", &item.Hash)
		}
	}

	if len(hashToTx) == 0 {
		return nil
	}

	needInputs := contains(bins, fields.Tx) || contains(bins, fields.Inputs) || contains(bins, fields.TxInpoints) || contains(bins, fields.Utxos)
	needOutputs := contains(bins, fields.Tx) || contains(bins, fields.Outputs) || contains(bins, fields.Utxos)

	// Deserialize raw_tx ONCE per row for both inputs and outputs (raw_tx was
	// already fetched from txs in Query 1). This path previously parsed the
	// same blob twice when both were requested — pure CPU waste on the hottest
	// read path. Error behaviour is preserved per consumer: the outputs path
	// fails hard on a corrupt raw_tx (as batchDecorateOutputs did), while the
	// inputs-only path skips the row (as the old inline loop did).
	if needInputs || needOutputs {
		for _, row := range hashToTx {
			if row.rawTx == nil {
				continue
			}
			parsedTx, parseErr := bt.NewTxFromBytes(row.rawTx)
			if parseErr != nil {
				if needOutputs {
					return errors.NewProcessingError("batchDecorate: failed to deserialize raw_tx for %s", row.hash, parseErr)
				}
				continue
			}
			if row.data.Tx == nil {
				row.data.Tx = &bt.Tx{Version: row.version, LockTime: row.lockTime}
			}
			if needInputs {
				row.data.Tx.Inputs = parsedTx.Inputs
			}
			if needOutputs {
				row.data.Tx.Outputs = parsedTx.Outputs
			}
		}
	}

	// Assemble results.
	for hash, matchedItems := range hashToItems {
		row, found := hashToTx[hash]
		if !found {
			continue
		}

		var tx *bt.Tx
		if contains(bins, fields.Tx) || contains(bins, fields.TxInpoints) {
			tx = &bt.Tx{
				Version:  row.version,
				LockTime: row.lockTime,
			}
			if needInputs && row.data.Tx != nil {
				tx.Inputs = row.data.Tx.Inputs
			}
			if needOutputs && row.data.Tx != nil {
				tx.Outputs = row.data.Tx.Outputs
			}
		}

		if contains(bins, fields.TxInpoints) && row.data.Tx != nil && len(row.data.Tx.Inputs) > 0 {
			row.data.TxInpoints, _ = subtree.NewTxInpointsFromInputs(row.data.Tx.Inputs)
		}

		if contains(bins, fields.Tx) || needInputs || needOutputs {
			row.data.Tx = tx
		} else {
			row.data.Tx = nil
		}

		for _, item := range matchedItems {
			item.Data = row.data
		}
	}

	return nil
}

// PreviousOutputsDecorate fetches output information for transaction inputs.
func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	return s.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{tx})
}

// BatchPreviousOutputsDecorate fetches previous output information for inputs
// across multiple transactions in bulk.
func (s *Store) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	if len(txs) == 0 {
		return nil
	}

	// Collect all (parentTxHash, outputIdx) pairs that need decoration.
	type inputRef struct {
		txIdx    int
		inputIdx int
		outIdx   uint32
	}
	needsByParent := make(map[chainhash.Hash][]inputRef)

	for txIdx, tx := range txs {
		if tx == nil {
			continue
		}
		for inputIdx, input := range tx.Inputs {
			if input == nil || input.PreviousTxScript != nil {
				continue // already decorated or nil
			}
			parentHash := *input.PreviousTxIDChainHash()
			needsByParent[parentHash] = append(needsByParent[parentHash], inputRef{
				txIdx:    txIdx,
				inputIdx: inputIdx,
				outIdx:   input.PreviousTxOutIndex,
			})
		}
	}

	if len(needsByParent) == 0 {
		return nil
	}

	// Collect unique parent hashes for chunked IN queries.
	parentHashes := make([][]byte, 0, len(needsByParent))
	for h := range needsByParent {
		hCopy := h
		parentHashes = append(parentHashes, hCopy[:])
	}

	// Parallelise chunk queries when configured. Each chunk fetches a disjoint
	// set of parent hashes, and the input slots a chunk writes to are disjoint
	// across chunks by construction (different parents → different inputRefs →
	// different tx.Inputs[] elements), so workers write directly without shared
	// state. needsByParent is read-only after construction. missingInputs is the
	// only cross-worker counter and uses atomic.Int64.
	//
	// During legacy IBD this chunk loop is the dominant per-block cost: ~3 s of
	// serial single-backend lookups into the 84M-row outputs heap at disk QD~1.
	// Running chunks concurrently overlaps the per-query round-trips and lifts
	// the SSD's queue depth, which is the actual IBD bottleneck on modest HW.
	concurrency := s.settings.UtxoStore.BatchPreviousOutputsDecorateConcurrency
	if concurrency < 1 {
		concurrency = 1
	}

	// Size chunks so the work splits into ~concurrency parallel queries (raising
	// DB/disk queue depth), bounded by the IN-clause cap and a sane minimum.
	// Serial behaviour (concurrency=1) keeps the full IN-clause cap per query.
	chunkSize := maxINClauseSize
	if concurrency > 1 && len(parentHashes) > 0 {
		chunkSize = (len(parentHashes) + concurrency - 1) / concurrency
		if chunkSize < minDecorateChunkSize {
			chunkSize = minDecorateChunkSize
		}
		if chunkSize > maxINClauseSize {
			chunkSize = maxINClauseSize
		}
	}

	var missingInputs atomic.Int64
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(s.logger, g, concurrency)

	for chunkStart := 0; chunkStart < len(parentHashes); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(parentHashes) {
			chunkEnd = len(parentHashes)
		}
		chunk := parentHashes[chunkStart:chunkEnd]

		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}

			// Fetch raw_tx from txs (contains all outputs: locking_script+satoshis).
			const q = `SELECT hash, raw_tx FROM unnest($1::bytea[]) AS h(v) JOIN txs ON txs.hash = h.v`

			rows, err := s.pool.Query(gCtx, q, chunk)
			if err != nil {
				return errors.NewStorageError("[BatchPreviousOutputsDecorate] query txs", err)
			}
			defer rows.Close()

			// Track which (parentHash, outIdx) pairs this chunk resolved so we
			// can count the unresolved ones afterwards.
			type foundKey struct {
				hash chainhash.Hash
				idx  uint32
			}
			found := make(map[foundKey]struct{}, len(chunk))

			for rows.Next() {
				var hashBytes []byte
				var rawTx []byte
				if err := rows.Scan(&hashBytes, &rawTx); err != nil {
					return errors.NewStorageError("[BatchPreviousOutputsDecorate] scan txs row", err)
				}
				var h chainhash.Hash
				copy(h[:], hashBytes)

				if rawTx == nil {
					continue
				}
				parsed, parseErr := bt.NewTxFromBytes(rawTx)
				if parseErr != nil {
					return errors.NewProcessingError("BatchPreviousOutputsDecorate: failed to deserialize raw_tx", parseErr)
				}

				// Dispatch each output to every input that needs (h, idx). The
				// refs for h live only in this chunk (disjoint hashes), so these
				// writes never race another worker.
				for _, ref := range needsByParent[h] {
					idx := ref.outIdx
					if int(idx) < len(parsed.Outputs) && parsed.Outputs[idx] != nil {
						out := parsed.Outputs[idx]
						txs[ref.txIdx].Inputs[ref.inputIdx].PreviousTxScript = out.LockingScript
						txs[ref.txIdx].Inputs[ref.inputIdx].PreviousTxSatoshis = out.Satoshis
						found[foundKey{hash: h, idx: idx}] = struct{}{}
					}
				}
			}
			if err := rows.Err(); err != nil {
				return err
			}

			// Count input slots in this chunk left unresolved.
			var localMissing int64
			for _, hb := range chunk {
				var h chainhash.Hash
				copy(h[:], hb)
				for _, ref := range needsByParent[h] {
					if _, ok := found[foundKey{hash: h, idx: ref.outIdx}]; !ok {
						localMissing++
					}
				}
			}
			if localMissing > 0 {
				missingInputs.Add(localMissing)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	if m := missingInputs.Load(); m > 0 {
		return errors.NewProcessingError("failed to decorate previous outputs: %d missing", m)
	}

	return nil
}

// contains checks if a slice of FieldName contains a specific item.
func contains(slice []fields.FieldName, item fields.FieldName) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

// txRow holds intermediate results for a single transaction during bulk fetch.
type txRow struct {
	data     *meta.Data
	version  uint32
	lockTime uint32
	hash     chainhash.Hash
	rawTx    []byte
}
