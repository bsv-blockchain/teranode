package postgres

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgxExecutor is the subset of the pgx API shared by *pgxpool.Conn and pgx.Tx,
// letting insertConflictingChildrenDirect run either directly on a pooled
// connection or inside the conflicting-create transaction.
type pgxExecutor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// ---------------------------------------------------------------------------
// Batch types
// ---------------------------------------------------------------------------

// batchCreateItem represents a single Create() request queued into the batcher.
type batchCreateItem struct {
	tx          *bt.Tx
	blockHeight uint32
	options     *utxo.CreateOptions
	done        chan batchCreateResult
}

// batchCreateResult holds the result routed back to a Create() caller.
type batchCreateResult struct {
	Data *meta.Data
	Err  error
}

// ---------------------------------------------------------------------------
// Array packing — pack per-output data into flat byte forms for the txs row
// ---------------------------------------------------------------------------

// outputArrayParams holds the PACKED per-output columns for one txs row.
// locking_script and satoshis are recovered from raw_tx at read time (see
// batchDecorateOutputs), so they are not stored separately here.
//
// utxoHashes is a flat concatenation: output i at byte offset i*16 — the first
// 16 bytes (128-bit prefix) of each output's UTXO hash (read in SQL via
// substr(utxo_hashes, i*16+1, 16) — substr is 1-based). spendableBits
// is a bitmap whose encoding matches PostgreSQL get_bit(): bit i lives in byte
// i/8 at the (i%8)-th LEAST significant position. out_frozens is NOT built at
// create time — no output is frozen on create, and NULL means "none frozen".
type outputArrayParams struct {
	utxoHashes             []byte // nil when no outputs
	spendableBits          []byte // nil when no outputs
	outCount               int32
	spendableCount         int32
	coinbaseSpendingHeight int32 // 0 = non-coinbase; same height for every output (column is INT4)
}

// setPackedBit sets bit i in buf using the PostgreSQL get_bit()/set_bit()
// encoding: byte i/8, least-significant-bit-first within the byte.
func setPackedBit(buf []byte, i int) {
	buf[i/8] |= 1 << (i % 8)
}

// getPackedBit reads bit i from buf using the same encoding (i must be within
// len(buf)*8).
func getPackedBit(buf []byte, i int) bool {
	return buf[i/8]&(1<<(i%8)) != 0
}

// unpackBitmap expands a packed bitmap into count per-output booleans (the
// read-time reconstruction of the old BOOLEAN[] form — the wire contract for
// consumers is unchanged). A nil bitmap returns nil ("no flag set" common
// case, e.g. out_frozens before any freeze). Bits beyond len(bitmap)*8 read
// false, mirroring the old OOB-subscript-is-NULL behaviour.
func unpackBitmap(bitmap []byte, count int) []bool {
	if bitmap == nil || count <= 0 {
		return nil
	}
	out := make([]bool, count)
	for i := 0; i < count && i < len(bitmap)*8; i++ {
		out[i] = getPackedBit(bitmap, i)
	}
	return out
}

// buildOutputArrays packs transaction outputs into the flat per-output columns.
// Outputs are packed at their TRUE index i (a nil output — never produced by a
// valid tx — leaves a zero 32-byte slot with spendable bit clear).
func buildOutputArrays(txHash *chainhash.Hash, btTx *bt.Tx, isCoinbase bool, blockHeight uint32, coinbaseMaturity int, genesisActivationHeight uint32) (outputArrayParams, error) {
	if countNonNilOutputs(btTx) == 0 {
		return outputArrayParams{}, nil
	}
	count := len(btTx.Outputs)

	p := outputArrayParams{
		utxoHashes:    make([]byte, count*16),
		spendableBits: make([]byte, (count+7)/8),
		outCount:      int32(count),
	}
	if isCoinbase {
		// coinbase_spending_height is INT4: heights stay far below 2^31, but
		// guard the widened sum before narrowing so a corrupt input can never
		// silently wrap.
		h := int64(blockHeight) + int64(coinbaseMaturity)
		if h > math.MaxInt32 {
			return outputArrayParams{}, errors.NewProcessingError("coinbase spending height %d exceeds int32 range", h)
		}
		p.coinbaseSpendingHeight = int32(h)
	}

	for i, output := range btTx.Outputs {
		if output == nil {
			continue
		}
		iUint32, err := safeconversion.IntToUint32(i)
		if err != nil {
			return outputArrayParams{}, err
		}
		utxoHash, err := util.UTXOHashFromOutput(txHash, output, iUint32)
		if err != nil {
			return outputArrayParams{}, err
		}
		copy(p.utxoHashes[i*16:], utxoHash[:16])
		// A zero-value OP_RETURN / data-carrier output can never be spent, so it
		// must not count toward the "fully spent" pruning check. Mirror the
		// aerospike store's ShouldStoreOutputAsUTXO gate. (nil script → no deref,
		// treated as spendable; non-nil zero-value scripts are inspected.)
		if output.LockingScript == nil || utxo.ShouldStoreOutputAsUTXO(output, blockHeight, genesisActivationHeight) {
			setPackedBit(p.spendableBits, i)
			p.spendableCount++
		}
	}
	return p, nil
}

// preparedCreate holds pre-computed data for one item in a create batch.
type preparedCreate struct {
	txHash     *chainhash.Hash
	txMeta     *meta.Data
	isCoinbase bool
	outArrs    outputArrayParams
}

// ---------------------------------------------------------------------------
// Create — public API
// ---------------------------------------------------------------------------

// Create stores a new transaction's outputs as UTXOs. When a createBatcher is
// active (after Start()), requests are enqueued for batch processing. Otherwise,
// the INSERT is executed directly for single-item operation (used in tests).
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	startTime := time.Now()
	defer func() {
		if prometheusDirectCreate != nil {
			prometheusDirectCreate.Inc()
		}
		if prometheusDirectCreateDuration != nil {
			prometheusDirectCreateDuration.Observe(time.Since(startTime).Seconds())
		}
	}()

	if _, err := blockHeightToInt32(blockHeight); err != nil {
		return nil, err
	}

	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// If batcher is active, enqueue and wait.
	//
	// NOTE: the coinbase is NOT special-cased to a direct insert here. That was
	// tried and reverted: with synchronous_commit=on (enforced, financial-data
	// durability — never off), the batched path issues one multi-row UNNEST
	// INSERT and so amortises a single WAL fsync across the whole batch, whereas
	// a lone direct coinbase insert pays a full fsync by itself (~9ms vs ~4ms
	// batched at steady state). Keeping the coinbase batched lets it share the
	// fsync. Reducing move_forward further requires taking the coinbase write
	// off the synchronous critical path, not routing it around the batcher.
	if s.createBatcher != nil {
		return s.createBatched(ctx, tx, blockHeight, options)
	}

	// No batcher (single-item test path) or a coinbase bypassing the batcher —
	// execute the INSERT directly.
	return s.createDirect(ctx, tx, blockHeight, options)
}

// createBatched enqueues a Create request into the batcher for bulk processing.
func (s *Store) createBatched(ctx context.Context, tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*meta.Data, error) {
	done := make(chan batchCreateResult, 1)
	s.createBatcher.Put(&batchCreateItem{
		tx:          tx,
		blockHeight: blockHeight,
		options:     options,
		done:        done,
	})

	select {
	case result := <-done:
		return result.Data, result.Err
	case <-ctx.Done():
		s.logger.Warnf("[createBatched] context cancelled while waiting for batcher result")
		return nil, ctx.Err()
	}
}

// createDirect executes the INSERT directly for a single transaction.
func (s *Store) createDirect(ctx context.Context, tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*meta.Data, error) {
	var txMeta *meta.Data
	var err error
	if options.SkipExtendedInputs {
		// Below-checkpoint fast path: inputs are not decorated, so GetFees would error.
		// Use the no-fee variant — fee is set to 0, TxInpoints still computed.
		txMeta, err = util.TxMetaDataFromTxNoFee(tx)
	} else {
		txMeta, err = util.TxMetaDataFromTx(tx)
	}
	if err != nil {
		return nil, errors.NewProcessingError("failed to get tx meta data", err)
	}

	if options.Conflicting {
		txMeta.Conflicting = true
	}
	if options.Locked {
		txMeta.Locked = true
	}

	var txHash *chainhash.Hash
	if options.TxID != nil {
		txHash = options.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	isCoinbase := tx.IsCoinbase()
	if options.IsCoinbase != nil {
		isCoinbase = *options.IsCoinbase
	}

	var unminedSince interface{}
	if len(options.MinedBlockInfos) == 0 {
		unminedSince = int32(blockHeight) // unmined_since is INT4; heights < 2^31
	}

	rawTx := tx.ExtendedBytes()

	// Build the packed mined_info bytea (fixed 12-byte records) from any mined-block
	// infos — the on-row replacement for the old block_ids/block_heights/subtree_idxs
	// arrays. packMinedInfo returns nil (→ SQL NULL) for the unmined common case.
	var minedInfo []byte
	// minedAtHeight records the height a tx created ALREADY mined (below-checkpoint
	// quick validation / legacy catch-up) was mined at, exactly as SetMinedMulti
	// does. The DAH fold stamps delete_at_height only when mined_at_height IS NOT
	// NULL, so without it a later fully-spent completion could never stamp the row
	// and it would never prune. Use the lowest mined block height (earliest
	// mining). INT4 column; heights < 2^31.
	var minedAtHeight *int32
	if len(options.MinedBlockInfos) > 0 {
		bids := make([]uint32, len(options.MinedBlockInfos))
		bhs := make([]uint32, len(options.MinedBlockInfos))
		sis := make([]int, len(options.MinedBlockInfos))
		minH := int32(options.MinedBlockInfos[0].BlockHeight)
		for i, info := range options.MinedBlockInfos {
			bids[i] = info.BlockID
			bhs[i] = info.BlockHeight
			sis[i] = info.SubtreeIdx
			if int32(info.BlockHeight) < minH {
				minH = int32(info.BlockHeight)
			}
		}
		minedInfo = packMinedInfo(bids, bhs, sis)
		minedAtHeight = &minH
	}

	outArrs, err := buildOutputArrays(txHash, tx, isCoinbase, blockHeight, int(s.settings.ChainCfgParams.CoinbaseMaturity), s.settings.ChainCfgParams.GenesisActivationHeight)
	if err != nil {
		return nil, err
	}

	// Zero-spendable txs (e.g. all-OP_RETURN data carriers) own no UTXO a spend can
	// ever complete, so the fold and reconciler — both gated on spendable_count > 0 —
	// never stamp them. SetMinedMulti stamps this case inline at mine time, but a tx
	// created ALREADY mined (legacy catch-up / below-checkpoint quick validation)
	// bypasses SetMinedMulti entirely, so without this stamp it would never become
	// prune-eligible and leak forever. Mirror mined.go: delete_at_height = mined + 1 +
	// retention and feed pending_deletes (the only pruner path) in the same statement.
	// spent_bits is all-zero at create, so only the zero-spendable case can complete
	// here; the fully-spent branch is unreachable at create time.
	var deleteAtHeight interface{}
	if minedAtHeight != nil && outArrs.outCount > 0 && outArrs.spendableCount == 0 {
		deleteAtHeight = *minedAtHeight + 1 + int32(s.settings.GetUtxoStoreBlockHeightRetention())
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to acquire connection", err)
	}
	defer conn.Release()

	// spent_bits is pre-sized to (out_count+7)/8 zero bytes so every later fold is
	// a same-size in-place overwrite (see the schema.go spent_bits contract).
	const insertSQL = `
		WITH ins AS (
			INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
				locked, conflicting, frozen, unmined_since,
				mined_info, conflicting_children, mined_at_height,
				utxo_hashes, out_count, spendable_count, out_spendables, coinbase_spending_height,
				delete_at_height, spent_bits)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
			        $15, $16, $17, $18, $19, $20,
			        decode(repeat('00', ($16 + 7) / 8), 'hex'))
			ON CONFLICT (hash) DO NOTHING
			RETURNING hash, delete_at_height
		),
		_pd AS (
			INSERT INTO pending_deletes (hash, delete_at_height)
			SELECT hash, delete_at_height FROM ins WHERE delete_at_height IS NOT NULL
			ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height
		)
		SELECT hash FROM ins`
	insertArgs := []interface{}{
		txHash[:], int32(tx.Version), int64(tx.LockTime),
		int64(txMeta.Fee), int32(txMeta.SizeInBytes), isCoinbase, rawTx,
		options.Locked, options.Conflicting, options.Frozen, unminedSince,
		minedInfo, [][]byte(nil), minedAtHeight,
		outArrs.utxoHashes, outArrs.outCount, outArrs.spendableCount,
		outArrs.spendableBits, outArrs.coinbaseSpendingHeight,
		deleteAtHeight,
	}

	// Conflicting txs need the txs INSERT AND the N parent conflicting_children
	// UPDATEs to be atomic: a failure (or disconnect) after the child row commits
	// but before the parents are stamped would leave a torn write the early
	// TxExistsError return on retry can never repair (ON CONFLICT DO NOTHING →
	// pgx.ErrNoRows → return before the child UPDATEs re-run). Wrap both in one
	// transaction for this rare path. The common, non-conflicting path stays a
	// single auto-committing INSERT (no extra round-trip).
	if txMeta.Conflicting {
		txn, beginErr := conn.Begin(ctx)
		if beginErr != nil {
			return nil, errors.NewStorageError("failed to begin conflicting-create tx", beginErr)
		}
		defer func() { _ = txn.Rollback(ctx) }()

		var insertedHash []byte
		if err = txn.QueryRow(ctx, insertSQL, insertArgs...).Scan(&insertedHash); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NewTxExistsError("transaction already exists (coinbase=%v):", isCoinbase)
			}
			return nil, errors.NewStorageError("failed to create UTXO", err)
		}

		// insertConflictingChildrenDirect is idempotent (the @> guard), so a retry
		// after a partial failure re-applies the missing parent stamps cleanly.
		if err = s.insertConflictingChildrenDirect(ctx, txn, txHash, tx); err != nil {
			return nil, err
		}

		if err = txn.Commit(ctx); err != nil {
			return nil, errors.NewStorageError("failed to commit conflicting-create tx", err)
		}

		result := s.buildCreateMeta(txMeta, options, isCoinbase, blockHeight)
		return result, nil
	}

	// The pending_unmined projection is write-behind (see
	// pending_unmined_projector.go): the synchronous _pu CTE arm was removed so
	// creates no longer pay a second heap insert + random-hash PK btree insert
	// into the side table. out_frozens is NULL on create (no output frozen —
	// freeze materialises the bitmap on demand); spendable_ins is NULL on create
	// (set by ReAssignUTXO). RETURNING hash makes the ON CONFLICT DO NOTHING /
	// no-row case detectable via pgx.ErrNoRows. spent_bits is pre-sized to
	// (out_count+7)/8 zero bytes so every later fold is a same-size in-place
	// overwrite (see the schema.go spent_bits contract).
	// The non-conflicting direct path shares the identical INSERT — reuse
	// insertSQL (declared above) rather than a byte-for-byte duplicate.

	var insertedHash []byte
	if err = conn.QueryRow(ctx, insertSQL, insertArgs...).Scan(&insertedHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewTxExistsError("transaction already exists (coinbase=%v):", isCoinbase)
		}
		return nil, errors.NewStorageError("failed to create UTXO", err)
	}

	// Write-behind projection into pending_unmined (was the _pu CTE arm). Only
	// unmined, non-conflicting rows belong to the invariant set.
	if unminedSince != nil && !options.Conflicting {
		s.enqueuePendingUnmined(insertedHash, int32(blockHeight))
	}

	result := s.buildCreateMeta(txMeta, options, isCoinbase, blockHeight)
	return result, nil
}

// ---------------------------------------------------------------------------
// sendCreateBatch — batch callback for the go-batcher
// ---------------------------------------------------------------------------

func (s *Store) sendCreateBatch(batch []*batchCreateItem) {
	s.batchStats.createItems.Add(int64(len(batch)))
	s.batchStats.createBatches.Add(1)
	ctx := context.Background()

	// Single-item: the direct INSERT path handles every option (mined,
	// conflicting, locked, frozen) in a single round-trip.
	if len(batch) == 1 {
		item := batch[0]
		result, err := s.createDirect(ctx, item.tx, item.blockHeight, item.options)
		item.done <- batchCreateResult{Data: result, Err: err}
		return
	}

	// Bulk path: a single UNNEST'd INSERT handles the common cases — plain
	// validator-path creates and single-block mined creates (legacy catch-up).
	// The two rare exceptions fall back to the proven per-item createDirect:
	//   - Conflicting txs need a separate conflicting_children insert.
	//   - A tx mined in >1 block at once needs per-row variable-length array
	//     columns, which a flat UNNEST cannot express.
	// (UNNEST benchmarked faster than the old COPY+staging+merge path across
	// the entire configured batch-size range, so there is no large-batch case
	// where staging would win — see BenchmarkCreatePaths.)
	bulk := make([]*batchCreateItem, 0, len(batch))
	for _, item := range batch {
		if item.options.Conflicting || len(item.options.MinedBlockInfos) > 1 {
			result, err := s.createDirect(ctx, item.tx, item.blockHeight, item.options)
			item.done <- batchCreateResult{Data: result, Err: err}
			continue
		}
		bulk = append(bulk, item)
	}
	if len(bulk) > 0 {
		s.sendCreateBatchUNNEST(ctx, bulk)
	}
}

// ---------------------------------------------------------------------------
// sendCreateBatchUNNEST — direct INSERT…SELECT FROM UNNEST. The single bulk
// create path: no staging tables, no COPY protocol, no merge step. Handles
// plain validator-path creates and single-block mined creates (mined_info
// populated when the item carries exactly one MinedBlockInfo). Conflicting and
// multi-block items are filtered out by sendCreateBatch and never reach here.
// It issues a SINGLE auto-committing INSERT for the whole batch, which is
// implicitly atomic on its own — there is no second statement to coordinate (the
// conflicting path, which does need a multi-statement transaction, runs in
// createDirect, not here).
// ---------------------------------------------------------------------------
// defaultPostgresCreateBatchMaxBytes bounds the estimated wire size of a single
// bulk-create INSERT. pgx rejects any wire message whose body reaches
// maxMessageBodyLen (0x3fffffff-1 ≈ 1 GiB) with "message body too large"; a block
// whose combined raw_tx payload exceeds that (e.g. testnet block 1512872 at
// 1.386 GiB of ~17 MB txs) can otherwise never commit and freezes chain sync.
// 512 MiB keeps a full 2x margin below the ceiling.
const defaultPostgresCreateBatchMaxBytes = 512 << 20 // 536870912

// pgxMaxMessageBodyLen mirrors pgproto3.maxMessageBodyLen — the hard ceiling on a
// single pgx wire message body. A chunk's estimate must stay strictly below it.
const pgxMaxMessageBodyLen int64 = 0x3fffffff - 1 // 1073741823

// createBatchChunkOverhead upper-bounds the per-INSERT fixed wire framing (20
// binary-array headers + the Bind/Execute envelope), charged once per chunk on
// top of the per-row estimates.
const createBatchChunkOverhead int64 = 1024

// createBatchUNNESTSQL is the bulk create INSERT, hoisted to a package const so
// the byte-bounded chunk driver can issue it once per chunk over array sub-slices.
//
// The pending_unmined projection is write-behind (see
// pending_unmined_projector.go); the synchronous _pu CTE arm was removed from
// this statement. The outer SELECT still RETURNs only hash, preserving the scan
// contract in runChunk (rows.Scan(&hashBytes)). spent_bits is pre-sized to
// (out_count+7)/8 zero bytes so every later fold is a same-size in-place
// overwrite (see the schema.go spent_bits contract).
//
// A zero-spendable row created already mined (u.mined AND out_count > 0 AND
// spendable_count = 0) is stamped delete_at_height = block_height + 1 + $21
// (retention) and fed into pending_deletes here — the fold/reconciler skip
// spendable_count = 0 and this path bypasses SetMinedMulti's inline stamp, so
// without it the row would never become prune-eligible (mirrors createDirect
// and mined.go).
const createBatchUNNESTSQL = `
			WITH ins AS (
			INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
				locked, conflicting, frozen, unmined_since, mined_info, mined_at_height,
				utxo_hashes, out_count, spendable_count, out_spendables, coinbase_spending_height,
				delete_at_height, spent_bits)
			SELECT u.hash, u.version, u.lock_time, u.fee, u.size_in_bytes, u.coinbase, u.raw_tx,
			       u.locked, u.conflicting, u.frozen,
			       CASE WHEN u.mined THEN NULL::int ELSE u.unmined_since END,
			       -- Packed mined_info: one 12-byte record (block_id||height||subtree_idx,
			       -- each int4 big-endian via int4send) for a mined create, NULL otherwise.
			       CASE WHEN u.mined THEN int4send(u.block_id) || int4send(u.block_height) || int4send(u.subtree_idx) ELSE NULL::bytea END,
			       CASE WHEN u.mined THEN u.block_height ELSE NULL::int END,
			       u.utxo_hashes, u.out_count, u.spendable_count, u.out_spendables, u.coinbase_spending_height,
			       CASE WHEN u.mined AND u.out_count > 0 AND u.spendable_count = 0
			            THEN (u.block_height + 1 + $21)::int ELSE NULL::int END,
			       decode(repeat('00', (u.out_count + 7) / 8), 'hex')
			FROM UNNEST($1::bytea[], $2::int[], $3::bigint[], $4::bigint[], $5::int[],
			            $6::boolean[], $7::bytea[], $8::boolean[], $9::boolean[], $10::boolean[],
			            $11::int[], $12::boolean[], $13::int[], $14::int[], $15::int[],
			            $16::bytea[], $17::int[], $18::int[], $19::bytea[], $20::int[])
			     AS u(hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
			          locked, conflicting, frozen, unmined_since, mined, block_id, block_height, subtree_idx,
			          utxo_hashes, out_count, spendable_count, out_spendables, coinbase_spending_height)
			ON CONFLICT (hash) DO NOTHING
			RETURNING hash, delete_at_height
			),
			_pd AS (
			INSERT INTO pending_deletes (hash, delete_at_height)
			SELECT hash, delete_at_height FROM ins WHERE delete_at_height IS NOT NULL
			ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height
			)
			SELECT hash FROM ins`

// planCreateChunks partitions [0,len(rowBytes)) into contiguous, non-empty,
// byte-bounded windows whose estimated wire size (chunkOverhead + Σ rowBytes in
// the window) stays at or below threshold, so each window's INSERT is one pgx
// message safely below maxMessageBodyLen. A row whose own estimate would reach
// hardLimit cannot be sent in any INSERT; its index is returned in oversized and
// excluded from every window. windows ∪ oversized cover [0,len) with no gap or
// overlap, every window is non-empty, and chunkOverhead is charged once per
// window (not per row). Pure and DB-free so it can be unit-tested directly.
func planCreateChunks(rowBytes []int64, threshold, chunkOverhead, hardLimit int64) (windows [][2]int, oversized []int) {
	chunkStart := 0
	chunkSum := chunkOverhead
	flush := func(end int) {
		if chunkStart < end {
			windows = append(windows, [2]int{chunkStart, end})
		}
		chunkStart = end
		chunkSum = chunkOverhead
	}
	for k := 0; k < len(rowBytes); k++ {
		// A single row that alone reaches the hard wire ceiling can never be
		// sent. Flush the pending window, record it, and skip it.
		if chunkOverhead+rowBytes[k] >= hardLimit {
			flush(k)
			oversized = append(oversized, k)
			chunkStart = k + 1
			chunkSum = chunkOverhead

			continue
		}
		// Emit the pending window before a row that would push it over
		// threshold. The chunkSum>chunkOverhead guard keeps every window
		// non-empty (a lone over-threshold-but-sendable row becomes its own
		// window, still < hardLimit because threshold ≤ hardLimit/2).
		if chunkSum > chunkOverhead && chunkSum+rowBytes[k] > threshold {
			flush(k)
		}
		chunkSum += rowBytes[k]
	}
	flush(len(rowBytes))

	return windows, oversized
}

func (s *Store) sendCreateBatchUNNEST(ctx context.Context, batch []*batchCreateItem) {
	n := len(batch)
	hashes := make([][]byte, 0, n)
	versions := make([]int32, 0, n) // version is INT4 (consensus uint32 via int32 bit-cast)
	lockTimes := make([]int64, 0, n)
	fees := make([]int64, 0, n)
	sizes := make([]int32, 0, n) // size_in_bytes is INT4 (bounded by the ~1 GiB bytea cap)
	coinbases := make([]bool, 0, n)
	rawTxs := make([][]byte, 0, n)
	lockeds := make([]bool, 0, n)
	conflictings := make([]bool, 0, n)
	frozens := make([]bool, 0, n)
	unminedSinces := make([]int32, 0, n) // unmined_since is INT4
	// Mined-block columns. mined[i] discriminates: when true the row is mined in
	// exactly one block and block_id/height/subtree_idx are packed into a single
	// 12-byte mined_info record (built in SQL via int4send) with unmined_since NULL;
	// when false the tx is unmined (mined_info NULL, unmined_since = blockHeight).
	// Matches createDirect. All three feed the packed record (and block_height also
	// feeds the INT4 mined_at_height), so they are bound as int4[].
	mineds := make([]bool, 0, n)
	blockIDs := make([]int32, 0, n)
	blockHeights := make([]int32, 0, n)
	subtreeIdxs := make([]int32, 0, n)

	prepared := make([]preparedCreate, n)
	valid := make([]bool, n)

	// Per-valid-row pgx wire-byte estimate and its batch index, used to split the
	// INSERT into byte-bounded chunks (see planCreateChunks). Both are appended at
	// the same point as the 20 column slices below, so slice index k stays aligned
	// across all of them and maps back to the batch via kToBatchIdx[k].
	rowBytes := make([]int64, 0, n)
	kToBatchIdx := make([]int, 0, n)

	// Packed per-output columns — one element per tx row, carried directly in
	// the per-tx UNNEST arrays (no flat per-output fan-out, no re-aggregation
	// CTE). nil elements become SQL NULL (txs with no outputs).
	packedUtxoHashes := make([][]byte, 0, n)
	packedSpendableBits := make([][]byte, 0, n)
	outCounts := make([]int32, 0, n)
	spendableCounts := make([]int32, 0, n)
	coinbaseSpendingHeights := make([]int32, 0, n) // coinbase_spending_height is INT4

	for i, item := range batch {
		var txMeta *meta.Data
		var err error
		if item.options.SkipExtendedInputs {
			// Below-checkpoint fast path: inputs not decorated, fee computation would
			// fail. Use the no-fee variant — fee=0, TxInpoints still computed.
			txMeta, err = util.TxMetaDataFromTxNoFee(item.tx)
		} else {
			txMeta, err = util.TxMetaDataFromTx(item.tx)
		}
		if err != nil {
			item.done <- batchCreateResult{Err: errors.NewProcessingError("failed to get tx meta data", err)}
			continue
		}
		if item.options.Locked {
			txMeta.Locked = true
		}

		var txHash *chainhash.Hash
		if item.options.TxID != nil {
			txHash = item.options.TxID
		} else {
			txHash = item.tx.TxIDChainHash()
		}

		isCoinbase := item.tx.IsCoinbase()
		if item.options.IsCoinbase != nil {
			isCoinbase = *item.options.IsCoinbase
		}

		outArrs, err := buildOutputArrays(txHash, item.tx, isCoinbase, item.blockHeight, int(s.settings.ChainCfgParams.CoinbaseMaturity), s.settings.ChainCfgParams.GenesisActivationHeight)
		if err != nil {
			item.done <- batchCreateResult{Err: err}
			continue
		}

		prepared[i] = preparedCreate{
			txHash:     txHash,
			txMeta:     txMeta,
			isCoinbase: isCoinbase,
			outArrs:    outArrs,
		}
		valid[i] = true

		hashes = append(hashes, txHash[:])
		versions = append(versions, int32(item.tx.Version))
		lockTimes = append(lockTimes, int64(item.tx.LockTime))
		fees = append(fees, int64(txMeta.Fee))
		sizes = append(sizes, int32(txMeta.SizeInBytes))
		coinbases = append(coinbases, isCoinbase)
		rawTx := item.tx.ExtendedBytes() // re-serializes on every call; capture once
		rawTxs = append(rawTxs, rawTx)
		lockeds = append(lockeds, item.options.Locked)
		conflictings = append(conflictings, false) // conflicting items routed to createDirect
		frozens = append(frozens, item.options.Frozen)
		unminedSinces = append(unminedSinces, int32(item.blockHeight))

		// sendCreateBatch guarantees len(MinedBlockInfos) <= 1 here.
		mined := len(item.options.MinedBlockInfos) == 1
		mineds = append(mineds, mined)
		var bID, bHeight, sIdx int32
		if mined {
			info := item.options.MinedBlockInfos[0]
			bID, bHeight, sIdx = int32(info.BlockID), int32(info.BlockHeight), int32(info.SubtreeIdx)
		}
		blockIDs = append(blockIDs, bID)
		blockHeights = append(blockHeights, bHeight)
		subtreeIdxs = append(subtreeIdxs, sIdx)

		packedUtxoHashes = append(packedUtxoHashes, outArrs.utxoHashes)
		packedSpendableBits = append(packedSpendableBits, outArrs.spendableBits)
		outCounts = append(outCounts, outArrs.outCount)
		spendableCounts = append(spendableCounts, outArrs.spendableCount)
		coinbaseSpendingHeights = append(coinbaseSpendingHeights, outArrs.coinbaseSpendingHeight)

		// Upper-biased per-row estimate: 177 fixed bytes (16 scalar columns'
		// 4-byte length prefixes + bodies = 129, the always-present 32-byte hash
		// + its prefix = 36, the three variable bytea length prefixes = 12) plus
		// the three variable bytea bodies. Never under-counts the payload that
		// actually overflows the pgx message limit.
		rowBytes = append(rowBytes, 177+int64(len(rawTx))+int64(len(outArrs.utxoHashes))+int64(len(outArrs.spendableBits)))
		kToBatchIdx = append(kToBatchIdx, i)
	}

	if len(hashes) == 0 {
		return
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		for i, item := range batch {
			if valid[i] {
				item.done <- batchCreateResult{Err: errors.NewStorageError("failed to acquire connection", err)}
			}
		}
		return
	}
	defer conn.Release()

	// Autocommit UNNEST INSERT, issued in byte-bounded chunks so no single pgx
	// message reaches maxMessageBodyLen (~1 GiB). Each tx row carries its packed
	// per-output values directly in the per-tx arrays — no per-output row fan-out
	// and no re-aggregation CTE. out_frozens and spendable_ins are NULL on create
	// (freeze / ReAssignUTXO materialise them later). The common case (a normal
	// block) is a single chunk == a single INSERT, identical to before; only an
	// oversized block (e.g. testnet 1512872 at 1.386 GiB) splits into several.
	threshold := int64(s.settings.UtxoStore.PostgresCreateBatchMaxBytes)
	if threshold <= 0 {
		threshold = defaultPostgresCreateBatchMaxBytes
	}

	if threshold > pgxMaxMessageBodyLen/2 {
		// Never let a chunk approach the wire ceiling, whatever the config says.
		threshold = pgxMaxMessageBodyLen / 2
	}

	newHashSet := make(map[chainhash.Hash]struct{}, len(hashes))

	// $21: retention added to a zero-spendable mined row's completion height when
	// stamping delete_at_height inline (see createBatchUNNESTSQL).
	retention := int32(s.settings.GetUtxoStoreBlockHeightRetention())

	// attemptChunk runs one INSERT of rows [lo,hi) and drains its RETURNING hashes
	// into newHashSet. Returns the RAW error (unwrapped) so the caller can classify
	// a Postgres deadlock. The INSERT is ON CONFLICT DO NOTHING and autocommits, so
	// a failed attempt rolls back entirely and a re-run re-inserts idempotently —
	// re-confirming the same RETURNING hashes (a set, so partial entries from a
	// failed attempt are harmless: a successful retry re-adds them, and on total
	// failure the chunk's items are marked invalid and never consult newHashSet).
	attemptChunk := func(lo, hi int) error {
		rows, err := conn.Query(ctx, createBatchUNNESTSQL,
			hashes[lo:hi], versions[lo:hi], lockTimes[lo:hi], fees[lo:hi], sizes[lo:hi],
			coinbases[lo:hi], rawTxs[lo:hi], lockeds[lo:hi], conflictings[lo:hi], frozens[lo:hi],
			unminedSinces[lo:hi], mineds[lo:hi], blockIDs[lo:hi], blockHeights[lo:hi], subtreeIdxs[lo:hi],
			packedUtxoHashes[lo:hi], outCounts[lo:hi], spendableCounts[lo:hi], packedSpendableBits[lo:hi], coinbaseSpendingHeights[lo:hi],
			retention,
		)
		if err != nil {
			return err
		}
		// Drain/close rows before the next Query on the same pooled conn (and before
		// conn.Release()). Do not hoist this Close out.
		defer rows.Close()

		for rows.Next() {
			var hashBytes []byte
			if scanErr := rows.Scan(&hashBytes); scanErr != nil {
				return scanErr
			}

			var h chainhash.Hash

			copy(h[:], hashBytes)
			newHashSet[h] = struct{}{}
		}

		return rows.Err()
	}

	// runChunk wraps attemptChunk with a bounded, jittered retry on a Postgres
	// deadlock (SQLSTATE 40P01). Concurrent create batchers can deadlock on the UTXO
	// insert; Postgres aborts one side (rolled back) while the other commits, so a
	// retry that lands AFTER the winner succeeds — the standard deadlock remedy. The
	// jitter desynchronises multiple deadlocked batchers so they do not re-collide in
	// lockstep. Without this, a deterministic concurrent pattern re-deadlocks on every
	// immediate window-level retry, which upstream permanently poisons the legacy
	// window committer (flushWorker) and freezes IBD until a process restart.
	runChunk := func(lo, hi int) error {
		for attempt := 0; ; attempt++ {
			err := attemptChunk(lo, hi)
			if err == nil {
				return nil
			}

			if attempt < pgDeadlockMaxRetries && isPgDeadlock(err) {
				backoff := deadlockRetryBackoff(attempt)
				s.logger.Warnf("[createBatched] postgres deadlock (40P01) on create-batch INSERT of %d rows, retry %d/%d after %s", hi-lo, attempt+1, pgDeadlockMaxRetries, backoff)

				select {
				case <-time.After(backoff):
					continue
				case <-ctx.Done():
					return errors.NewStorageError("create batch rows error", ctx.Err())
				}
			}

			return errors.NewStorageError("create batch rows error", err)
		}
	}

	windows, oversized := planCreateChunks(rowBytes, threshold, createBatchChunkOverhead, pgxMaxMessageBodyLen)

	// A single tx too large for any INSERT can never be stored inline. Report it
	// to ONLY that item with an actionable error and drop it from the valid set
	// so neither signalAll nor the completion loop touches it again. Not reachable
	// for real BSV txs (policy/consensus max ≪ 1 GiB); insurance against exactly
	// the failure class being fixed here.
	for _, k := range oversized {
		bi := kToBatchIdx[k]
		batch[bi].done <- batchCreateResult{Err: errors.NewStorageError(
			"create row for tx %s estimated %d wire bytes exceeds the postgres message limit %d",
			batch[bi].tx.TxIDChainHash().String(), rowBytes[k], pgxMaxMessageBodyLen)}
		valid[bi] = false
	}

	// Chunks autocommit independently, so on a mid-batch failure the rows from
	// earlier chunks are already durable. Report the error ONLY to the failed
	// chunk and the chunks that never ran — clearing valid[bi] so neither the
	// projection nor the completion loop below double-touches them (the done
	// channel is buffered for one result and read once) — and let the committed
	// chunks flow through to pending_unmined projection + success/TxExists exactly
	// as if the whole batch had succeeded. Signalling the error to EVERY item
	// would wrongly fail txs already in the DB and skip their projection.
	for wi, w := range windows {
		if err := runChunk(w[0], w[1]); err != nil {
			for _, ww := range windows[wi:] {
				for k := ww[0]; k < ww[1]; k++ {
					bi := kToBatchIdx[k]
					if valid[bi] {
						batch[bi].done <- batchCreateResult{Err: err}
						valid[bi] = false
					}
				}
			}

			break
		}
	}

	// Write-behind projection into pending_unmined (was the _pu CTE arm on the
	// bulk INSERT). Enqueue every committed unmined row; items in a failed or
	// unrun chunk had valid cleared above and are skipped. Conflicting items never
	// reach this path (routed to createDirect), but keep the guard. Oversized rows
	// (valid cleared earlier) were never inserted into txs, so they must not be
	// projected.
	for k := range hashes {
		if !valid[kToBatchIdx[k]] {
			continue
		}
		if !mineds[k] && !conflictings[k] {
			s.enqueuePendingUnmined(hashes[k], unminedSinces[k])
		}
	}

	for i, item := range batch {
		if !valid[i] {
			continue
		}
		p := &prepared[i]
		if _, wasNew := newHashSet[*p.txHash]; wasNew {
			// Consume the hash so only the first item with this hash in the
			// batch is treated as newly created. Concurrent duplicate creates
			// of the same tx coalesce into a single bulk INSERT whose ON CONFLICT
			// DO NOTHING inserts exactly one row (RETURNING it once); without
			// consuming it here, every same-hash item would be marked created and
			// each caller would notify block assembly. Subsequent same-hash items
			// must return TxExists, matching createDirect's ON CONFLICT semantics.
			delete(newHashSet, *p.txHash)
			result := s.buildCreateMeta(p.txMeta, item.options, p.isCoinbase, item.blockHeight)
			item.done <- batchCreateResult{Data: result}
		} else {
			item.done <- batchCreateResult{
				Err: errors.NewTxExistsError("transaction already exists (coinbase=%v):", p.isCoinbase),
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildCreateMeta populates the meta.Data from computed values.
func (s *Store) buildCreateMeta(txMeta *meta.Data, options *utxo.CreateOptions, isCoinbase bool, blockHeight uint32) *meta.Data {
	txMeta.IsCoinbase = isCoinbase
	txMeta.Frozen = options.Frozen
	if len(options.MinedBlockInfos) == 0 {
		txMeta.UnminedSince = blockHeight
	}
	if len(options.MinedBlockInfos) > 0 {
		txMeta.BlockIDs = make([]uint32, len(options.MinedBlockInfos))
		txMeta.BlockHeights = make([]uint32, len(options.MinedBlockInfos))
		txMeta.SubtreeIdxs = make([]int, len(options.MinedBlockInfos))
		for i, info := range options.MinedBlockInfos {
			txMeta.BlockIDs[i] = info.BlockID
			txMeta.BlockHeights[i] = info.BlockHeight
			txMeta.SubtreeIdxs[i] = info.SubtreeIdx
		}
	}
	return txMeta
}

// insertConflictingChildrenDirect updates the parent txs rows to append this child
// to their conflicting_children array.
func (s *Store) insertConflictingChildrenDirect(ctx context.Context, exec pgxExecutor, childTxHash *chainhash.Hash, tx *bt.Tx) error {
	seen := make(map[chainhash.Hash]struct{}, len(tx.Inputs))

	for _, input := range tx.Inputs {
		parentHash := *input.PreviousTxIDChainHash()
		if _, ok := seen[parentHash]; ok {
			continue
		}
		seen[parentHash] = struct{}{}

		_, err := exec.Exec(ctx, `
			UPDATE txs SET conflicting_children = COALESCE(conflicting_children, '{}') || $2::bytea[]
			WHERE hash = $1
			  AND NOT (COALESCE(conflicting_children, '{}') @> $2::bytea[])`,
			parentHash[:], [][]byte{childTxHash[:]},
		)
		if err != nil {
			return errors.NewStorageError("failed to insert conflicting_children", err)
		}
	}

	return nil
}

// countNonNilOutputs counts non-nil outputs in a transaction.
func countNonNilOutputs(tx *bt.Tx) int {
	count := 0
	for _, o := range tx.Outputs {
		if o != nil {
			count++
		}
	}
	return count
}

// buildINClauseLocal generates a SQL IN clause placeholder string and args.
// startIdx is the 1-based parameter index ($startIdx, $startIdx+1, ...).
func buildINClauseLocal(hashes [][]byte, startIdx int) (string, []interface{}) {
	placeholders := make([]string, len(hashes))
	args := make([]interface{}, len(hashes))
	for i, h := range hashes {
		placeholders[i] = fmt.Sprintf("$%d", startIdx+i)
		args[i] = h
	}
	return "(" + strings.Join(placeholders, ",") + ")", args
}
