package utxoset

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/jackc/pgx/v5"
)

// noteConflictSQL records the contesting transaction on every parent whose coin it wants.
//
// A transaction that loses a double-spend race is stored as conflicting rather than
// discarded, because resolving the conflict later has to find it. Finding it means asking the
// PARENT whose coin was contested, so the route runs from the parent, and this statement is
// what writes it. Without it, conflict resolution has no route from a contested coin to the
// transactions competing for it.
//
// It writes to conflict_children rather than to a column on tx_ident, and that is the fix for
// a real hole rather than a tidy-up. A contested parent is very often MINED, and a mined
// transaction has no identity row: the stamp moved it into tx_mined. The old UPDATE therefore
// matched nothing at all for exactly the parents that matter most, and it succeeded while
// doing so, because an UPDATE that touches no row is not an error.
//
// The 32-byte boundary test the old statement carried is gone with the packed column it
// guarded. One row per child cannot be matched straddling its neighbours, so the whole class
// of defect no longer exists rather than being defended against.
//
// ON CONFLICT DO NOTHING against the window's own unique index is what makes re-offering the
// same losing transaction free. See the schema comment for why that index is per window and
// why the reader still has to say DISTINCT.
//
// One ARRAY of parents, so a transaction reaching for coins of twenty parents is one
// statement. $1 is the height, $2 the parents, $3 the one child.
const noteConflictSQL = `
INSERT INTO conflict_children (noted_height, parent_txid, child_txid)
SELECT $1::int, p.parent, $3::bytea
  FROM unnest($2::bytea[]) AS p(parent)
ON CONFLICT DO NOTHING`

// packMembership renders mined-block information into the packed form tx_ident carries:
// 12-byte triples of block id, block height and subtree index, big-endian, in the order the
// caller supplied. Insertion order is load-bearing -- the conformance suite requires subtree
// indexes to come back in the order they were written rather than sorted.
func packMembership(infos []utxo.MinedBlockInfo) []byte {
	if len(infos) == 0 {
		return nil
	}

	b := make([]byte, 0, len(infos)*12)

	for _, mi := range infos {
		var e [12]byte

		binary.BigEndian.PutUint32(e[0:4], mi.BlockID)
		binary.BigEndian.PutUint32(e[4:8], mi.BlockHeight)
		binary.BigEndian.PutUint32(e[8:12], uint32(mi.SubtreeIdx)) //nolint:gosec // subtree index is never negative

		b = append(b, e[:]...)
	}

	return b
}

// offChainSinceAt decides whether a newly created transaction belongs in the mempool set.
//
// The rule is "was this transaction created because a block contains it". Block information
// present means mined, and a transaction cannot be mined and waiting to be mined at the same
// time. That is the same rule the sql store applies, which keys on whether any block info was
// supplied at all.
//
// This USED to additionally require the block information to claim the block was on the longest
// chain, on the reasoning that at create time the block is still being validated, so marking
// the transaction and letting a later stamp clear it failed in the safe direction. It did not
// fail safe, it failed at scale. The block-application path never claims the longest chain,
// because at that moment the claim would be untrue, so every transaction created by a sync was
// stored in both states at once. On the mainnet box that reached 3.8 million rows, 91% of the
// store, and the damage was not cosmetic: the pass that preserves the parents of transactions
// waiting to be mined walks that set, and it runs BEFORE the reclaim, so with millions of rows
// to walk it never finished and nothing was ever reclaimed. Disk grew without bound and the
// database was eventually killed for memory.
//
// The hazard the old rule was guarding against is real but is someone else's job. A transaction
// from a block that later loses is put back in the mempool set by the un-mine path, which sets
// the marker with a fresh clock taken from the current tip. That mechanism exists, is tested,
// and is what both reference stores rely on for the identical exposure.
//
// An explicit un-mine is the one kind of block information that does NOT mean the transaction
// is in a block, so it still waits.
func offChainSinceAt(infos []utxo.MinedBlockInfo, blockHeight uint32) *int32 {
	for _, mi := range infos {
		if !mi.UnsetMined {
			return nil
		}
	}

	h := int32(blockHeight) //nolint:gosec // block height fits int32 for any reachable chain

	return &h
}

// minedBlock returns the block a create says contains the transaction, and whether it says so
// at all.
//
// A create carrying mined-block information is a block-path create: below the checkpoint every
// create, at the tip only block assembly's coinbase. It claims on tx_mined and its coins know
// their block. Anything else is a mempool create and claims on tx_ident with the unconfirmed
// sentinel on its coins.
//
// An explicit un-mine is the one kind of block information that does NOT mean mined, which is
// the same exemption offChainSinceAt makes, and for the same reason.
func minedBlock(infos []utxo.MinedBlockInfo) (utxo.MinedBlockInfo, bool) {
	for _, mi := range infos {
		if mi.UnsetMined {
			continue
		}

		return mi, true
	}

	return utxo.MinedBlockInfo{}, false
}

// Create records a transaction's spendable outputs in the UTXO table.
//
// Only SPENDABLE outputs get a row. A provably-unspendable output — an OP_RETURN data
// carrier — creates nothing, because a row that can never be deleted would sit in the
// table forever and the UTXO table's size is the entire budget. This mirrors the
// postgres store's spendable_count and the aerospike store's ShouldStoreOutputAsUTXO
// gate, so all three agree on what "spendable" means.
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	// Parsed up front rather than only on the batched path, because the single path now needs
	// to know whether this create carries block information before it opens its transaction:
	// that is what decides which membership window has to exist.
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Batched when configured, which is the normal path. The batcher collects calls arriving
	// from many goroutines and sends them as one round trip, which is what the other two
	// implementations of this interface do and what this store was missing.
	//
	// The conflicting case takes the direct path. It writes to the PARENTS of the incoming
	// transaction rather than only to the transaction itself, so two items in one batch can
	// touch the same row, which is exactly the overlap the batched path assumes away.
	if s.createBatcher != nil {
		if !options.Conflicting {
			done := make(chan createResult, 1)

			s.createBatcher.PutCtx(ctx, &createItem{
				tx:          tx,
				blockHeight: blockHeight,
				options:     options,
				done:        done,
			})

			select {
			case res := <-done:
				return res.data, res.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	// Both windows BEFORE the transaction is opened, never inside it: the DDL needs its own
	// pool connection, and taking one while holding a transaction from the same pool
	// deadlocks the pool under concurrency, with no timeout.
	if err := s.ensureTxBodyPartition(ctx, blockHeight); err != nil {
		return nil, err
	}

	if mi, mined := minedBlock(options.MinedBlockInfos); mined {
		if err := s.ensureTxMinedPartition(ctx, mi.BlockHeight); err != nil {
			return nil, err
		}
	}

	// A conflicting create notes the contest on its parents, and that note lands in a
	// height-partitioned window created alongside the spend journal's leaf. It is ensured
	// HERE, before the transaction opens, for the same reason the two above are: the DDL
	// needs its own pool connection, and taking one while holding a transaction from the same
	// pool deadlocks the pool under concurrency, with no timeout.
	notedHeight := s.GetBlockHeight()

	if options.Conflicting {
		if err := s.ensureSpendJournalPartition(ctx, notedHeight); err != nil {
			return nil, err
		}
	}

	// A transaction of its own, because the create claim's advisory lock is
	// transaction-scoped: on the pool it would be released at the end of its own statement
	// and would guard nothing. Nothing here is worth more than one commit, so the single
	// path pays one BEGIN and one COMMIT rather than the three statements it used to.
	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][Create] begin", err)
	}

	committed := false

	defer func() {
		if !committed {
			_ = dbTx.Rollback(ctx)
		}
	}()

	data, cerr := s.createIn(ctx, dbTx, tx, blockHeight, notedHeight, opts...)

	// ErrTxExists is committed rather than rolled back. The claim wrote nothing at all for a
	// transaction the store already holds, so the two are equivalent for the claim itself --
	// but the conflicting path also notes the contest on the incoming transaction's PARENTS,
	// and that note has to survive, because conflict resolution's only route from a contested
	// coin to the transactions competing for it is the parent's list.
	if cerr != nil && !errors.Is(cerr, errors.ErrTxExists) {
		return nil, cerr
	}

	if err := dbTx.Commit(ctx); err != nil {
		return nil, errors.NewStorageError("[utxoset][Create] commit", err)
	}

	committed = true

	return data, cerr
}

// appendCreate adds one transaction to the plan: its identity row, its serialized bytes, and
// one coin row per spendable output.
//
// Shared by the single and the batched path so the two cannot drift apart on what they store.
// Nothing is appended until every failure is behind us, so a transaction this rejects leaves
// no half-written row in the arrays.
func (s *Store) appendCreate(p *createPlan, item int, tx *bt.Tx, blockHeight uint32,
	options *utxo.CreateOptions) (*meta.Data, error) {
	if options == nil {
		options = &utxo.CreateOptions{}
	}

	txHash := tx.TxIDChainHash()
	leaf := LeafFor(txHash[:])
	isCoinbase := tx.IsCoinbase()

	// Coinbase maturity and the ReAssignUTXO delay fold into one precomputed height, so the
	// spend hot path never branches on "is this a coinbase".
	//
	// Zero for an ordinary output, NOT the creation height. No consensus rule stops a normal
	// output being spent below the height it was created at, and encoding one would reject
	// valid spends during a reorg or whenever a caller passes a height that is not strictly
	// increasing.
	var spendableFrom int32
	if isCoinbase {
		spendableFrom = int32(blockHeight) + int32(s.settings.ChainCfgParams.CoinbaseMaturity)
	}

	// The caller's state options MUST reach the row. spend.go checks all three flags when
	// deciding whether a spend may proceed, so dropping them here does not fail loudly, it
	// creates an ordinary spendable output and lets every downstream guard pass quietly on a
	// zero bit.
	var flags int16
	if isCoinbase {
		flags |= FlagCoinbase
	}

	if options.Frozen {
		flags |= FlagFrozen
	}

	if options.Conflicting {
		flags |= FlagConflicting
	}

	if options.Locked {
		flags |= FlagLocked
	}

	// The inputs, stored rather than re-derived. Block assembly rebuilds a mining candidate
	// from the fee, the size and these, never from the serialized transaction, which is what
	// lets the body age out of its window while the transaction stays mineable.
	var inpoints []byte

	if !isCoinbase {
		ip, ierr := subtree.NewTxInpointsFromTx(tx)
		if ierr != nil {
			return nil, errors.NewProcessingError("[utxoset][Create] inpoints %s", txHash.String(), ierr)
		}

		if inpoints, ierr = ip.Serialize(); ierr != nil {
			return nil, errors.NewProcessingError("[utxoset][Create] serialise inpoints %s", txHash.String(), ierr)
		}
	}

	genesisHeight := s.settings.ChainCfgParams.GenesisActivationHeight

	// Which of the two claims this create takes, and the block facts that go on its coins.
	// Both heights and the block id are 0 for a mempool create, and mined_height 0 is the
	// unconfirmed sentinel the coin carries until something stamps it.
	var (
		minedHeight int32
		blockID     int32
		subtreeIdx  int32
	)

	mi, mined := minedBlock(options.MinedBlockInfos)
	if mined {
		minedHeight = int32(mi.BlockHeight) //nolint:gosec // a height fits int32 for any reachable chain
		blockID = int32(mi.BlockID)         //nolint:gosec // a block id fits int32
		subtreeIdx = int32(mi.SubtreeIdx)   //nolint:gosec // a subtree index fits int32
	}

	// The serialized bytes, or nothing at all for a transaction mined below the hardcoded
	// checkpoint when the operator has asked for that (see bodyCheckpoints on Store).
	//
	// SERIALISED ONLY WHEN IT IS KEPT. tx.Bytes() renders the whole transaction into a fresh
	// buffer, ~1.35 KB on mainnet, and this runs in the create prepare stage, which is already
	// GC-bound. Building it and then dropping it on the skip path would pay the entire cost the
	// setting exists to avoid, in the one process the setting is meant to relieve.
	//
	// nil rather than an empty slice, because nil is what pgx sends as SQL NULL inside a
	// bytea[], and NULL is what the statement's body CTE tests on. An empty slice would write
	// a zero-length body row, which is a row the reader would then try to decode.
	//
	// The boundary is model.BelowCheckpoint over the store's own checkpoint list, never a
	// hand-written comparison: model/checkpoint.go is the single definition every
	// below-checkpoint gate shares, including the outpoint-only spend gate this rides behind,
	// and it excludes genesis. A nil list -- the setting off, or a network with no checkpoints
	// -- makes it false for every height, which is how "off" is expressed.
	//
	// The gate is on the MINED height, not on the height the create was filed at. Only a
	// transaction a block places below the checkpoint has its bytes in a subtree data file; a
	// mempool arrival carries no mined height at all and keeps its body whatever the setting
	// says, which is why `mined` has to hold before the boundary is even consulted.
	var body []byte
	if !mined || !model.BelowCheckpoint(s.bodyCheckpoints, mi.BlockHeight) {
		body = tx.Bytes()
	}

	// The identity row. k is this transaction's position in the statement, and the result
	// comes back keyed on it, so a caller learns whether its OWN claim took rather than
	// whether the batch as a whole wrote anything.
	p.idx = append(p.idx, int32(len(p.owner))) //nolint:gosec // bounded by batch size
	p.owner = append(p.owner, item)
	p.txs = append(p.txs, tx)
	p.leaves = append(p.leaves, leaf)
	p.txids = append(p.txids, txHash[:])
	p.heights = append(p.heights, int32(blockHeight))
	p.offChain = append(p.offChain, offChainSinceAt(options.MinedBlockInfos, blockHeight))
	p.membership = append(p.membership, packMembership(options.MinedBlockInfos))
	p.sizes = append(p.sizes, int32(tx.Size()))
	p.inpoints = append(p.inpoints, inpoints)
	p.locktimes = append(p.locktimes, int32(tx.LockTime))
	p.createdAt = append(p.createdAt, time.Now().UnixMilli())
	p.txFlags = append(p.txFlags, flags)
	p.bodies = append(p.bodies, body)
	p.minedRows = append(p.minedRows, mined)
	p.minedHeight = append(p.minedHeight, minedHeight)
	p.blockID = append(p.blockID, blockID)
	p.subtreeIdx = append(p.subtreeIdx, subtreeIdx)
	p.lo = append(p.lo, Pack(txHash[:], 0))
	p.hi = append(p.hi, Pack(txHash[:], ^uint32(0)))

	for vout, out := range tx.Outputs {
		if out == nil {
			continue
		}

		if out.LockingScript != nil && !utxo.ShouldStoreOutputAsUTXO(out, blockHeight, genesisHeight) {
			continue // provably unspendable: no coin row, ever
		}

		var script []byte
		if out.LockingScript != nil {
			script = *out.LockingScript
		}

		p.coinSats = append(p.coinSats, int64(out.Satoshis))
		p.coinHeights = append(p.coinHeights, int32(blockHeight))
		p.coinSpendable = append(p.coinSpendable, spendableFrom)
		p.coinLeaves = append(p.coinLeaves, leaf)
		p.coinFlags = append(p.coinFlags, flags)
		p.coinUkeys = append(p.coinUkeys, Pack(txHash[:], uint32(vout)))
		p.coinTxids = append(p.coinTxids, txHash[:])
		p.coinScripts = append(p.coinScripts, script)
		p.coinMined = append(p.coinMined, minedHeight)
		p.coinBlockIDs = append(p.coinBlockIDs, blockID)
	}

	return &meta.Data{
		Tx:          tx,
		Fee:         0,
		SizeInBytes: uint64(tx.Size()),
		IsCoinbase:  isCoinbase,
	}, nil
}

// createIn is Create inside an EXISTING database transaction, so SpendAndCreate can run it in
// the same one as the spend.
//
// It takes a pgx.Tx rather than the wider querier, and that is a requirement rather than
// tightening for its own sake: the claim's idempotence rests on a transaction-scoped advisory
// lock, which on a pool connection would be released at the end of the statement that took it.
// Every caller therefore opens a transaction, and Create's own is what pays for its single
// path.
//
// It is a plan of one. There is deliberately no second statement for the single-transaction
// case: what a create writes, and the claim that decides whether it writes at all, is this
// store's whole idempotence rule, and two copies of it is a defect waiting for one to be
// edited alone.
// notedHeight is the height a conflict note is stamped with, and it is a PARAMETER rather
// than a read of s.GetBlockHeight() here so that it cannot differ from the height whose
// window the caller ensured. The note lands in a height-partitioned window that only the
// caller can create -- the DDL needs its own pool connection, and this function already holds
// a transaction from the same pool -- so a second read of the tip that crossed a 48-block
// boundary in between would insert into a partition that does not exist. It is ignored unless
// the create is conflicting.
func (s *Store) createIn(ctx context.Context, dbTx pgx.Tx, tx *bt.Tx, blockHeight, notedHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	plan := s.planCreates([]*createItem{{tx: tx, blockHeight: blockHeight, options: options}})
	if plan.errs[0] != nil {
		return nil, plan.errs[0]
	}

	if err := s.lockTxids(ctx, dbTx, plan.txids); err != nil {
		return nil, err
	}

	if options.Conflicting {
		txHash := tx.TxIDChainHash()

		if cerr := s.noteConflictOnParents(ctx, dbTx, tx, txHash[:], notedHeight); cerr != nil {
			return nil, cerr
		}
	}

	if err := s.runCreatePlan(ctx, dbTx, plan); err != nil {
		return nil, err
	}

	return plan.perItem[0], plan.errs[0]
}

// noteConflictOnParents tells every parent of a losing transaction that it is being contested.
//
// One statement for the whole input set, not one per parent. notedHeight is the store's
// current chain height as the CALLER read it, so the window it lands in is the one the caller
// ensured; see createIn for why that cannot be re-read here.
func (s *Store) noteConflictOnParents(ctx context.Context, q querier, tx *bt.Tx, txid []byte,
	notedHeight uint32) error {
	seen := make(map[chainhash.Hash]struct{}, len(tx.Inputs))
	parents := make([][]byte, 0, len(tx.Inputs))

	for _, in := range tx.Inputs {
		if in == nil {
			continue
		}

		parent := in.PreviousTxIDChainHash()
		if parent == nil {
			continue
		}

		// One note per parent, however many of its outputs this transaction reaches for.
		if _, dup := seen[*parent]; dup {
			continue
		}

		seen[*parent] = struct{}{}

		parents = append(parents, parent[:])
	}

	if len(parents) == 0 {
		return nil
	}

	if _, err := q.Exec(ctx, noteConflictSQL, int32(notedHeight), parents, txid); err != nil { //nolint:gosec // a chain height fits int32
		return errors.NewStorageError("[utxoset][Create] note conflict on %d parents", len(parents), err)
	}

	return nil
}
