package utxoset

import (
	"bytes"
	"context"
	"sort"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// The below-checkpoint netted write.
//
// Below the highest hardcoded checkpoint quick validation hands the store each block as lists of
// fixed ranges of transaction positions, with outpoint-only spends. This write applies such a
// list as chunks with no order between them, each chunk one database transaction that writes,
// for each new transaction of the chunk:
//
//   - its mined record, which is also the claim: inserted ON CONFLICT DO NOTHING, and a
//     transaction whose record already exists was written by an earlier attempt and is skipped;
//   - the deletes of the coins it spends from outside the list;
//   - its outputs that nothing in the list spends, as coins.
//
// It writes no spend-journal rows. Below the checkpoint no reader needs them: un-mine is refused
// there, the pruner reads no rows, and the two readers that used them on a repeat (the
// same-spender acceptance and the parent read of a fall-through to normal validation) are not
// reached, because a transaction that exists writes nothing and quick validation never falls
// through below the checkpoint.
//
// What makes any order of chunk commits safe is that the netting is computed from the list
// alone, never from what the store holds. A parent and its child in the list are the same
// pair on every attempt, so the parent's output the child spends is never a coin, whichever of
// the two committed first. Each chunk commits a transaction's record, deletes and coins together,
// so a transaction is either fully written or not at all, and a repeat writes exactly the ones
// that are missing.
//
// A transaction of the list can hold coins this write never wrote: one that reached the store
// unmined before its chunk (the identity route), which holds every output as a coin and has made
// its spends. It is not written again. Its chunk deletes its own outputs the list spends instead,
// so only the parent's chunk ever touches the parent's coins, a child never does, and the result
// is the same whichever commits first or whether the child's record already exists, as it can
// when the parent arrives between two attempts.
//
// Other writers can store a transaction of the list while this write runs. The node does not
// stop them during the sync: unsolicited legacy tx messages, propagation and the validator's
// Kafka consumer all store transactions in CATCHINGBLOCKS and LEGACYSYNCING. Two rules keep
// such a writer from leaving an output the list spends as a live coin.
//
//   - Each chunk takes the transaction lock of every transaction it holds, the lock every create
//     takes before its claim, and reads the identity rows again under that lock. A transaction
//     stored unmined before the lock takes the identity route. A writer that comes after it
//     waits for the chunk to commit.
//   - Each chunk writes the mined record of every transaction it holds, so a writer that comes
//     after the commit finds the record and its claim does not take. Without the record, a
//     transaction with no coin to write would be stored a second time as unmined, with every
//     output a coin, while its parent's chunk still runs.
//
// The first identity probe, before any chunk, only sizes the work. The read under the lock
// decides.
//
// A list that asks for locked creates writes its coins locked, exactly as the ordinary create
// does, because the plan takes the flag from the list's options. Netted outputs are never
// written, so they have nothing to lock, and a transaction that writes nothing has no coin for
// SetLocked to find, which it treats as nothing to do. SetLocked reaches a coin by its txid and
// key range alone, so the unlock needs no mined record, and finds the coins this write made.
//
// A coinbase is never in the list. Its create path is the one that refuses the historic
// duplicate coinbases, and that check is not repeated here.

// nettedBelowChunkTxs, when positive, fixes the transactions per chunk of the below-checkpoint
// netted write. Zero chooses the size from the list. A variable so a test can force one
// transaction per chunk.
var nettedBelowChunkTxs = 0

// nettedBelowMaxChunkOutputs is the most outputs a chunk of the below-checkpoint netted write
// holds; a chunk has one transaction at least. The chunks run in parallel and each holds its
// outputs several times while it writes: in its plan, in the sorted copies and in the encoded
// statement. On mainnet at block 814,043 on 2026-10-08 the chunks of a 30,595-transaction block
// held about 28 million outputs, the write used 13 GB, and the OOM killer stopped the node. At
// 200,000 the same block still peaked at about 7.7 GB, against the 6 GB target: 16 chunks held
// 3.2 million outputs at about 1.4 KB each. At 50,000 they hold 800,000, about 1.1 GB. A variable
// so a test can set a small limit.
var nettedBelowMaxChunkOutputs = 50_000

// nettedCoinsBatchRows is the most coin rows one statement of netBelowCoins writes, so a single
// transaction with millions of outputs does not build one statement of all of them. A variable
// so a test can set a small limit.
var nettedCoinsBatchRows = 50_000

// nettedCoinsBatchBytes is the most bytes one statement of netBelowCoins sends, each row counted at
// its script and coinRowOverhead. Postgres refuses a protocol message of 1 GB or more: on mainnet
// at block 863,817 on 2026-10-09, 50,000 rows with large scripts failed with "message body too
// large" on every attempt. A variable so a test can set a small limit.
var nettedCoinsBatchBytes int64 = 256 << 20

// coinRowOverhead is the bytes of a coin row beside its script: the fixed columns, the txid and
// the array framing of each value.
const coinRowOverhead = 128

// coinBatches cuts rows, in sequence, into batches of at most maxRows rows and maxBytes bytes; a
// batch has one row at least.
func coinBatches(rows []int, scripts [][]byte, maxRows int, maxBytes int64) [][]int {
	var (
		batches [][]int
		start   int
		bytes   int64
	)

	for i, r := range rows {
		n := int64(len(scripts[r])) + coinRowOverhead

		if i > start && (i-start >= maxRows || bytes+n > maxBytes) {
			batches = append(batches, rows[start:i])
			start, bytes = i, 0
		}

		bytes += n
	}

	if start < len(rows) {
		batches = append(batches, rows[start:])
	}

	return batches
}

// nettedBelowFault is a test hook called before each chunk of the below-checkpoint netted write
// commits, with the list positions of the chunk's transactions. An error from it stops that
// chunk there, as a crash would. nil in production.
var nettedBelowFault func(chunk []int) error

const (
	nettedBelowMinChunk = 32
	nettedBelowMaxChunk = 256
)

// nettedBelowGate reports whether the list takes the below-checkpoint netted write. It does when
// it carries a block at or below the highest hardcoded checkpoint and its spends are
// outpoint-only, which is quick validation's fast path. A list that qualifies but asks for
// something this write does not model is refused with an error rather than written another,
// slower way.
//
// A list that asks for locked creates is not refused: quick validation sends one below the
// checkpoint whenever blockvalidation_quick_validate_skip_utxo_lock is off, which is its default,
// and unlocks the block's transactions with SetLocked once the block commits. The write gives
// each coin it writes the locked flag, as the ordinary locked create does (see netBelowCoins).
func (s *Store) nettedBelowGate(o *utxo.CreateOptions) (bool, error) {
	mi, mined := minedBlock(o.MinedBlockInfos)
	if !mined || !model.BelowCheckpoint(s.checkpoints, mi.BlockHeight) {
		return false, nil
	}

	if !o.IgnoreFlags.SkipUTXOHashCheck || !o.SkipExtendedInputs {
		return false, nil
	}

	if o.Frozen || o.Conflicting || o.SpendOnly || o.CreateOnly {
		return true, errors.NewProcessingError("[utxoset][SpendAndCreateMulti] a list below the checkpoint is netted, and the netted write does not model frozen, conflicting, create-only or spend-only creates")
	}

	return true, nil
}

// nettedBelowWorkers is how many chunks run at once: the setting, at most half the pool.
func (s *Store) nettedBelowWorkers() int {
	n := 16
	if s.settings != nil && s.settings.UtxoStore.NettedBelowWorkers > 0 {
		n = s.settings.UtxoStore.NettedBelowWorkers
	}

	return max(1, min(n, int(s.pool.Config().MaxConns)/2))
}

// nettedBelowChunkSize spreads a list over the workers: at least nettedBelowMinChunk and at most
// nettedBelowMaxChunk transactions per chunk.
func nettedBelowChunkSize(n, workers int) int {
	if nettedBelowChunkTxs > 0 {
		return nettedBelowChunkTxs
	}

	size := (n + workers - 1) / workers

	return max(nettedBelowMinChunk, min(nettedBelowMaxChunk, size))
}

// netBelowChunks cuts items, in list order, into chunks of at most size transactions and at most
// maxOutputs outputs; a chunk has one transaction at least.
func netBelowChunks(items []*netBelowTx, size, maxOutputs int) [][]*netBelowTx {
	var (
		chunks  [][]*netBelowTx
		start   int
		outputs int
	)

	for i, it := range items {
		n := len(it.tx.Outputs)

		if i > start && (i-start >= size || outputs+n > maxOutputs) {
			chunks = append(chunks, items[start:i])
			start, outputs = i, 0
		}

		outputs += n
	}

	if start < len(items) {
		chunks = append(chunks, items[start:])
	}

	return chunks
}

type netBelowTx struct {
	pos   int
	tx    *bt.Tx
	txid  chainhash.Hash
	ident bool
	// netted[vin] is true when input vin spends an output of an earlier transaction of the
	// list. nil when none does.
	netted []bool
	// listSpent are the transaction's own outputs a later transaction of the list spends.
	listSpent []uint32
	result    utxo.SpendAndCreateMultiResult
}

func (s *Store) netBelow(ctx context.Context, txs []*bt.Tx, list *utxo.SpendAndCreateMultiList,
	blockHeight uint32) ([]utxo.SpendAndCreateMultiResult, error) {
	mi, _ := minedBlock(list.Options.MinedBlockInfos)

	for i, tx := range txs {
		if tx.IsCoinbase() {
			return nil, errors.NewProcessingError("[utxoset][SpendAndCreateMulti] transaction %d of a list below the checkpoint is a coinbase, which keeps its own create path", i)
		}
	}

	ident, err := s.identityHeld(ctx, s.pool, list.TxIDs)
	if err != nil {
		return nil, err
	}

	position := make(map[chainhash.Hash]int, len(txs))
	for i := range txs {
		position[list.TxIDs[i]] = i
	}

	items := make([]*netBelowTx, len(txs))
	spender := make(map[nettedKey]nettedSpender)

	for i, tx := range txs {
		it := &netBelowTx{pos: i, tx: tx, txid: list.TxIDs[i], ident: ident[list.TxIDs[i]]}

		for vin, in := range tx.Inputs {
			p, ok := position[*in.PreviousTxIDChainHash()]
			if !ok {
				continue
			}

			if int(in.PreviousTxOutIndex) >= len(txs[p].Outputs) {
				return nil, errors.NewProcessingError("[utxoset][SpendAndCreateMulti] transaction %s spends output %d of %s, which has %d outputs",
					it.txid.String(), in.PreviousTxOutIndex, list.TxIDs[p].String(), len(txs[p].Outputs))
			}

			if it.netted == nil {
				it.netted = make([]bool, len(tx.Inputs))
			}

			it.netted[vin] = true
			items[p].listSpent = append(items[p].listSpent, in.PreviousTxOutIndex)

			parent := list.TxIDs[p]
			spender[newNettedKey(parent[:], Pack(parent[:], in.PreviousTxOutIndex))] =
				nettedSpender{txid: it.txid[:], vin: int32(vin)} //nolint:gosec // an input index fits int32
		}

		items[i] = it
	}

	if err := s.ensureTxMinedPartition(ctx, mi.BlockHeight); err != nil {
		return nil, err
	}

	if err := s.ensureTxBodyPartition(ctx, blockHeight); err != nil {
		return nil, err
	}

	workers := s.nettedBelowWorkers()
	size := nettedBelowChunkSize(len(items), workers)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, chunk := range netBelowChunks(items, size, nettedBelowMaxChunkOutputs) {
		g.Go(func() error { return s.netBelowChunk(gCtx, chunk, list, blockHeight, spender) })
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	results := make([]utxo.SpendAndCreateMultiResult, len(items))
	for i, it := range items {
		results[i] = it.result
	}

	return results, nil
}

// identityHeldSQL returns which of the given transactions have an identity row: they reached
// the store unmined.
const identityHeldSQL = `
SELECT t.txid FROM unnest($1::smallint[], $2::bytea[]) AS t(leaf, txid)
 WHERE EXISTS (SELECT 1 FROM tx_ident i WHERE i.leaf = t.leaf AND i.txid = t.txid)`

// identityHeld reads the identity rows of txids through q: the pool for the first probe, and a
// chunk's database transaction for the read under the chunk's locks.
func (s *Store) identityHeld(ctx context.Context, q querier, txids []chainhash.Hash) (map[chainhash.Hash]bool, error) {
	leaves := make([]int16, len(txids))
	ids := make([][]byte, len(txids))

	for i := range txids {
		leaves[i] = LeafFor(txids[i][:])
		ids[i] = txids[i][:]
	}

	rows, err := q.Query(ctx, identityHeldSQL, leaves, ids)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SpendAndCreateMulti] identity probe", err)
	}

	defer rows.Close()

	held := map[chainhash.Hash]bool{}

	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			return nil, errors.NewStorageError("[utxoset][SpendAndCreateMulti] identity probe scan", err)
		}

		var h chainhash.Hash
		copy(h[:], id)
		held[h] = true
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][SpendAndCreateMulti] identity probe rows", err)
	}

	return held, nil
}

// nettedOwnSpentSQL deletes coins a transaction of the list holds for outputs the list spends.
// A coin that is not there was never written, or is already gone, and is no fault.
const nettedOwnSpentSQL = `
DELETE FROM utxo u USING unnest($1::smallint[], $2::uuid[], $3::bytea[]) AS k(leaf, ukey, txid)
 WHERE u.leaf = k.leaf
   AND u.ukey = k.ukey
   AND u.txid = k.txid`

// nettedMinedSQL is the claim: the mined record of each transaction, inserted unless it exists.
// The returned txids are the transactions this attempt writes.
const nettedMinedSQL = `
INSERT INTO tx_mined (txid, mined_height, block_id, subtree_idx, created_height,
                      size_in_bytes, fee, tx_inpoints, locktime, created_at, flags)
SELECT t.txid, t.mined_height, t.block_id, t.subtree_idx, t.created_height,
       t.size_in_bytes, t.fee, NULL, NULL, t.created_at, t.flags
  FROM unnest($1::bytea[], $2::int[], $3::int[], $4::int[], $5::int[], $6::int[],
              $7::bigint[], $8::bigint[], $9::smallint[])
    AS t(txid, mined_height, block_id, subtree_idx, created_height, size_in_bytes,
         fee, created_at, flags)
ON CONFLICT (txid, mined_height, block_id) DO NOTHING
RETURNING txid`

// nettedDeleteSQL deletes the coins the chunk's new transactions spend from outside the list. It
// is spendJournalSQL's delete with the same predicates (the full txid recheck, the per-key flag
// mask written as an inequality the planner can estimate, the maturity test) and without the
// journal insert: below the checkpoint nothing reads the journal.
const nettedDeleteSQL = `
WITH k AS (
    SELECT * FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[], $5::int[], $6::smallint[])
        AS t(leaf, ukey, txid, k, spent_height, mask)
)
DELETE FROM utxo u USING k
 WHERE u.leaf           = k.leaf
   AND u.ukey           = k.ukey
   AND u.txid           = k.txid
   AND (u.flags & k.mask) < 1
   AND u.spendable_from <= k.spent_height
RETURNING k.k`

// nettedCoinsSQL writes the chunk's surviving outputs as coins.
const nettedCoinsSQL = `
INSERT INTO utxo (satoshis, created_height, spendable_from, mined_height, block_id,
                  leaf, flags, ukey, txid, script)
SELECT * FROM unnest($1::bigint[], $2::int[], $3::int[], $4::int[], $5::int[],
                     $6::smallint[], $7::smallint[], $8::uuid[], $9::bytea[], $10::bytea[])`

// nettedBodiesSQL writes the bodies the plan carries, which is none below the checkpoint when
// utxostore_skipTxBodyBelowCheckpoint is on.
const nettedBodiesSQL = `
INSERT INTO tx_body (created_height, txid, raw_tx)
SELECT * FROM unnest($1::int[], $2::bytea[], $3::bytea[])`

func (s *Store) netBelowChunk(ctx context.Context, chunk []*netBelowTx, list *utxo.SpendAndCreateMultiList,
	blockHeight uint32, spender map[nettedKey]nettedSpender) error {
	createItems := make([]*createItem, len(chunk))
	for k, it := range chunk {
		createItems[k] = &createItem{tx: it.tx, blockHeight: blockHeight, options: list.Options.ItemOptions(it.pos)}
	}

	plan := s.planCreates(createItems)

	for k := range chunk {
		if plan.errs[k] != nil {
			return plan.errs[k]
		}
	}

	for i := range plan.owner {
		if !plan.minedRows[i] {
			return errors.NewProcessingError("[utxoset][SpendAndCreateMulti] a transaction of a list below the checkpoint did not take the block-path create")
		}
	}

	// Outputs the list spends are never coins.
	_ = plan.takeNetted(spender)

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] begin netted chunk", err)
	}

	committed := false

	defer func() {
		if !committed {
			_ = dbTx.Rollback(ctx)
		}
	}()

	// The transaction locks come before the fence, the order runMinedPlan relies on in every
	// other create. The reverse order deadlocks: this chunk holds the fence shared and waits on a
	// transaction lock, while the writer that holds that lock waits for the fence behind the
	// stamp's exclusive request.
	if err = s.netBelowLockAndProbe(ctx, dbTx, chunk); err != nil {
		return err
	}

	fence, err := s.takeFenceShared(ctx, dbTx)
	if err != nil {
		return err
	}

	var identRows, newRows []int

	for i, owner := range plan.owner {
		if chunk[owner].ident {
			identRows = append(identRows, i)
		} else {
			newRows = append(newRows, i)
		}
	}

	if err = s.judgeFencedCreates(ctx, dbTx, plan, fence); err != nil {
		return err
	}

	// A transaction stored unmined before its block takes today's block-path create, which
	// records its containment from the identity row and writes nothing else.
	if len(identRows) > 0 {
		if err = s.runMinedPlan(ctx, dbTx, plan.subset(identRows)); err != nil {
			return err
		}
	}

	claimed, err := s.netBelowClaim(ctx, dbTx, plan.subset(newRows))
	if err != nil {
		return err
	}

	if err = s.netBelowDeletes(ctx, dbTx, chunk, claimed, list.Options.IgnoreFlags, blockHeight); err != nil {
		return err
	}

	if err = s.netBelowOwnSpent(ctx, dbTx, chunk); err != nil {
		return err
	}

	if err = s.netBelowCoins(ctx, dbTx, plan.subset(newRows), claimed); err != nil {
		return err
	}

	if err = s.netBelowFault(chunk); err != nil {
		return err
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] commit netted chunk", err)
	}

	committed = true

	for k, it := range chunk {
		if !it.ident && claimed[string(it.txid[:])] {
			it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxCreated, Meta: plan.perItem[k]}
			continue
		}

		it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxExisted}
	}

	return nil
}

// netBelowFault calls the test hook, if one is set, with the list positions of the chunk.
func (s *Store) netBelowFault(chunk []*netBelowTx) error {
	if nettedBelowFault == nil {
		return nil
	}

	positions := make([]int, len(chunk))
	for k, it := range chunk {
		positions[k] = it.pos
	}

	return nettedBelowFault(positions)
}

// netBelowLockAndProbe takes the transaction lock of every transaction of the chunk and then
// reads their identity rows again, in the chunk's database transaction. A transaction that
// another writer stored unmined after the first probe is marked ident here, so the chunk takes
// the identity route for it and deletes its own outputs that the list spends. The lock is the one
// every create takes before its claim, so no such writer can commit between this read and the
// chunk's commit. One call locks all the chunk's transactions in key order, so two chunks cannot
// wait on each other in a cycle.
//
// Every transaction of the chunk then writes its mined record, also one with no coin to write and
// no coin to delete. A writer that comes after the commit finds that record and its claim does
// not take. Without the record, such a transaction could be stored a second time as unmined, with
// every output a coin, while the chunk of its parent still runs.
func (s *Store) netBelowLockAndProbe(ctx context.Context, dbTx pgx.Tx, chunk []*netBelowTx) error {
	ids := make([][]byte, len(chunk))

	var unknown []chainhash.Hash

	for k, it := range chunk {
		ids[k] = it.txid[:]

		if !it.ident {
			unknown = append(unknown, it.txid)
		}
	}

	if err := s.lockTxids(ctx, dbTx, ids); err != nil {
		return err
	}

	if len(unknown) == 0 {
		return nil
	}

	held, err := s.identityHeld(ctx, dbTx, unknown)
	if err != nil {
		return err
	}

	for _, it := range chunk {
		if held[it.txid] {
			it.ident = true
		}
	}

	return nil
}

// netBelowClaim inserts the mined records and returns the txids whose record is new.
func (s *Store) netBelowClaim(ctx context.Context, dbTx pgx.Tx, p *createPlan) (map[string]bool, error) {
	claimed := make(map[string]bool, len(p.owner))
	if len(p.owner) == 0 {
		return claimed, nil
	}

	s.countCreatesAheadOfTip(p)

	rows, err := dbTx.Query(ctx, nettedMinedSQL, p.txids, p.minedHeight, p.blockID, p.subtreeIdx,
		p.heights, p.sizes, p.fees, p.createdAt, p.txFlags)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SpendAndCreateMulti] claim netted chunk", err)
	}

	defer rows.Close()

	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			return nil, errors.NewStorageError("[utxoset][SpendAndCreateMulti] claim scan", err)
		}

		claimed[string(id)] = true
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][SpendAndCreateMulti] claim rows", err)
	}

	return claimed, nil
}

// netBelowDeletes deletes the outside coins of the chunk's newly claimed transactions. Every one
// must be there: a new transaction has never spent before, so a missing coin is a store fault.
func (s *Store) netBelowDeletes(ctx context.Context, dbTx pgx.Tx, chunk []*netBelowTx, claimed map[string]bool,
	flags utxo.IgnoreFlags, blockHeight uint32) error {
	var items []*spendItem

	for _, it := range chunk {
		if it.ident || !claimed[string(it.txid[:])] {
			continue
		}

		items = append(items, &spendItem{tx: it.tx, blockHeight: blockHeight, ignoreFlags: flags, skip: it.netted})
	}

	if len(items) == 0 {
		return nil
	}

	plan := planSpends(items)
	if len(plan.idx) == 0 {
		return nil
	}

	plan.sortRows()

	rows, err := dbTx.Query(ctx, nettedDeleteSQL, plan.leaves, plan.ukeys, plan.txids, plan.idx, plan.heights, plan.masks)
	if err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] spend netted chunk", err)
	}

	done := make(map[int32]struct{}, len(plan.idx))

	for rows.Next() {
		var k int32
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][SpendAndCreateMulti] spend scan", err)
		}

		done[k] = struct{}{}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] spend rows", err)
	}

	if len(done) == len(plan.idx) {
		return nil
	}

	for i, k := range plan.idx {
		if _, ok := done[k]; ok {
			continue
		}

		tx := plan.itemTxs[plan.owner[i]]
		in := tx.Inputs[plan.ownerVin[i]]

		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] below the checkpoint, transaction %s spends %s:%d, which is not a spendable coin in the store; %d of %d spends of the chunk missing",
			tx.TxIDChainHash().String(), in.PreviousTxIDChainHash().String(), in.PreviousTxOutIndex, len(plan.idx)-len(done), len(plan.idx))
	}

	return nil
}

// netBelowOwnSpent deletes, for each transaction of the chunk that reached the store unmined
// (the identity route), its own coins for outputs the list spends. Only its chunk deletes them,
// never its child's, so the outcome does not depend on which chunk commits first or on whether
// the child was written by an earlier attempt.
func (s *Store) netBelowOwnSpent(ctx context.Context, dbTx pgx.Tx, chunk []*netBelowTx) error {
	type key struct {
		leaf int16
		ukey [16]byte
		txid []byte
	}

	var keys []key

	for _, it := range chunk {
		if !it.ident || len(it.listSpent) == 0 {
			continue
		}

		leaf := LeafFor(it.txid[:])

		for _, vout := range it.listSpent {
			keys = append(keys, key{leaf: leaf, ukey: Pack(it.txid[:], vout), txid: it.txid[:]})
		}
	}

	if len(keys) == 0 {
		return nil
	}

	sort.Slice(keys, func(a, b int) bool {
		if keys[a].leaf != keys[b].leaf {
			return keys[a].leaf < keys[b].leaf
		}

		return bytes.Compare(keys[a].ukey[:], keys[b].ukey[:]) < 0
	})

	leaves := make([]int16, len(keys))
	ukeys := make([][16]byte, len(keys))
	txids := make([][]byte, len(keys))

	for i, k := range keys {
		leaves[i], ukeys[i], txids[i] = k.leaf, k.ukey, k.txid
	}

	if _, err := dbTx.Exec(ctx, nettedOwnSpentSQL, leaves, ukeys, txids); err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] delete list-spent coins of an existing transaction", err)
	}

	return nil
}

// netBelowCoins writes the surviving outputs and bodies of the newly claimed transactions,
// sorted by leaf and packed key. Each coin carries the flags the plan gave it, which include the
// locked flag when the list asks for locked creates.
func (s *Store) netBelowCoins(ctx context.Context, dbTx pgx.Tx, p *createPlan, claimed map[string]bool) error {
	var rows []int

	for c, id := range p.utxoTxids {
		if claimed[string(id)] {
			rows = append(rows, c)
		}
	}

	if len(rows) > 0 {
		sort.Slice(rows, func(a, b int) bool {
			x, y := rows[a], rows[b]
			if p.utxoLeaves[x] != p.utxoLeaves[y] {
				return p.utxoLeaves[x] < p.utxoLeaves[y]
			}

			return bytes.Compare(p.utxoUkeys[x][:], p.utxoUkeys[y][:]) < 0
		})

		// In batches of nettedCoinsBatchRows rows and nettedCoinsBatchBytes bytes, in the sorted
		// sequence: the driver builds each statement in memory, several times the size of its
		// rows, and Postgres refuses a message of 1 GB or more.
		for _, batch := range coinBatches(rows, p.utxoScripts, nettedCoinsBatchRows, nettedCoinsBatchBytes) {
			if _, err := dbTx.Exec(ctx, nettedCoinsSQL,
				permute(p.utxoSats, batch), permute(p.utxoHeights, batch), permute(p.utxoSpendable, batch),
				permute(p.utxoMined, batch), permute(p.utxoBlockIDs, batch), permute(p.utxoLeaves, batch),
				permute(p.utxoFlags, batch), permute(p.utxoUkeys, batch), permute(p.utxoTxids, batch),
				permute(p.utxoScripts, batch)); err != nil {
				return errors.NewStorageError("[utxoset][SpendAndCreateMulti] coins of netted chunk", err)
			}
		}
	}

	var (
		heights []int32
		txids   [][]byte
		bodies  [][]byte
	)

	for i, id := range p.txids {
		if claimed[string(id)] && p.bodies[i] != nil {
			heights = append(heights, p.heights[i])
			txids = append(txids, id)
			bodies = append(bodies, p.bodies[i])
		}
	}

	if len(bodies) > 0 {
		if _, err := dbTx.Exec(ctx, nettedBodiesSQL, heights, txids, bodies); err != nil {
			return errors.NewStorageError("[utxoset][SpendAndCreateMulti] bodies of netted chunk", err)
		}
	}

	return nil
}
