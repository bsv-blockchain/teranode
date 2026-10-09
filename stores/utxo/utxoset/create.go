package utxoset

import (
	"context"
	"math"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
)

// noteConflictSQL records the contesting transaction on every parent whose UTXO it wants.
//
// A transaction that loses a double-spend race is stored as conflicting rather than
// discarded, because resolving the conflict later has to find it. Finding it means asking the
// PARENT whose UTXO was contested, so the route runs from the parent, and this statement is
// what writes it. Without it, conflict resolution has no route from a contested UTXO to the
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
// One ARRAY of parents, so a transaction reaching for UTXOs of twenty parents is one
// statement. $1 is the height, $2 the parents, $3 the one child.
const noteConflictSQL = `
INSERT INTO conflict_children (noted_height, parent_txid, child_txid)
SELECT $1::int, p.parent, $3::bytea
  FROM unnest($2::bytea[]) AS p(parent)
ON CONFLICT DO NOTHING`

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
// A create carrying mined-block information is a block-carrying create: below the checkpoint
// every create, at the tip only block assembly's coinbase. Whether it takes the block-path
// claim, writing the pair onto its UTXOs at birth, or the identity claim with a containment
// row beside it, is decided by appendCreate against the store's checkpoint list, not here.
// A create with no block information takes the identity claim with the unconfirmed sentinel
// on its UTXOs.
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
	if !s.seedingCreate(options) {
		if err := s.ensureTxBodyPartition(ctx, blockHeight); err != nil {
			return nil, err
		}

		if mi, mined := minedBlock(options.MinedBlockInfos); mined {
			if err := s.ensureTxMinedPartition(ctx, mi.BlockHeight); err != nil {
				return nil, err
			}
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
	// UTXO to the transactions competing for it is the parent's list.
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
// one UTXO row per spendable output.
//
// Shared by the single and the batched path so the two cannot drift apart on what they store.
// Nothing is appended until every failure is behind us, so a transaction this rejects leaves
// no half-written row in the arrays.
func (s *Store) appendCreate(p *createPlan, item int, tx *bt.Tx, blockHeight uint32,
	options *utxo.CreateOptions) (*meta.Data, error) {
	if options == nil {
		options = &utxo.CreateOptions{}
	}

	txHash := createTxID(tx, options)
	leaf := LeafFor(txHash[:])
	rebuilt := isRebuilt(tx)

	// "Coinbase" is the caller's to say when it says it. The seeder rebuilds a coinbase from its
	// unspent outputs alone, with no input to recognise it by, and passes WithSetCoinbase; read
	// from the shape alone it would lose its coinbase flag and its maturity.
	isCoinbase := tx.IsCoinbase()
	if options.IsCoinbase != nil {
		isCoinbase = *options.IsCoinbase
	}

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
	//
	// The parsed form is kept as well, for the metadata returned to the caller. See the return
	// below for who reads it.
	var (
		inpoints   []byte
		txInpoints subtree.TxInpoints
	)

	if !isCoinbase {
		ip, ierr := subtree.NewTxInpointsFromTx(tx)
		if ierr != nil {
			return nil, errors.NewProcessingError("[utxoset][Create] inpoints %s", txHash.String(), ierr)
		}

		txInpoints = ip

		if inpoints, ierr = ip.Serialize(); ierr != nil {
			return nil, errors.NewProcessingError("[utxoset][Create] serialise inpoints %s", txHash.String(), ierr)
		}
	}

	genesisHeight := s.settings.ChainCfgParams.GenesisActivationHeight

	// The block this create says contains the transaction, if it says so at all, and which of
	// the two claims it takes. The store applies the checkpoint test ITSELF, so a caller
	// cannot write a pair onto a UTXO the chain has not proven.
	//
	// At or below the highest checkpoint the chain is header-proven, so a block-carrying
	// create takes the block-path claim and writes the pair onto its UTXOs at birth; the
	// value is final from the first moment and the deep stamp never has to visit it. Above
	// the checkpoint the same create takes the identity claim like a create that carries no
	// block: an identity row with a NULL marker (the call has no longest-chain input to say
	// otherwise), a containment row for the block, and UTXOs at (0,0) for the stamp to fill
	// 288 blocks later, at a depth a reorg cannot reach. The coinbase at the tip goes this way
	// too. So above the checkpoint every UTXO at (0,0) has an identity row and every non-zero
	// pair was written by the stamp, and a chain switch can never leave a losing block's pair
	// on a live UTXO. Both heights and the block id are 0 for a create with no block, and
	// mined_height 0 is the unconfirmed sentinel the UTXO carries until the stamp writes it.
	var (
		minedHeight int32
		blockID     int32
		subtreeIdx  int32
		atBirth     bool
	)

	mi, mined := minedBlock(options.MinedBlockInfos)
	if mined {
		minedHeight = int32(mi.BlockHeight) //nolint:gosec // a height fits int32 for any reachable chain
		blockID = int32(mi.BlockID)         //nolint:gosec // a block id fits int32
		subtreeIdx = int32(mi.SubtreeIdx)   //nolint:gosec // a subtree index fits int32
		atBirth = model.BelowCheckpoint(s.checkpoints, mi.BlockHeight) || s.seedingCreate(options)
	}

	// What the UTXOs carry from birth: the block's pair when the store lets the create write
	// it, the sentinel otherwise.
	var utxoMinedHeight, utxoBlockID int32
	if atBirth {
		utxoMinedHeight, utxoBlockID = minedHeight, blockID
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
	//
	// A transaction the caller filed under its own ID is kept only if it really hashes to that
	// ID. The seeder's rebuilt transactions do not: their spent outputs are missing, so their
	// bytes are not the transaction's, and storing them would hand any later reader a body
	// that does not match the ID it was asked for.
	var body []byte
	if (!mined || !model.BelowCheckpoint(s.bodyCheckpoints, mi.BlockHeight)) && faithful(tx, options, txHash) {
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
	p.sizes = append(p.sizes, int32(txSize(tx))) //nolint:gosec // a transaction's size fits int32
	if rebuilt {
		p.fees = append(p.fees, nil)
	} else {
		p.fees = append(p.fees, txFee(tx))
	}
	p.inpoints = append(p.inpoints, inpoints)
	p.locktimes = append(p.locktimes, int32(tx.LockTime))
	p.createdAt = append(p.createdAt, time.Now().UnixMilli())
	p.txFlags = append(p.txFlags, flags)
	p.bodies = append(p.bodies, body)
	p.minedRows = append(p.minedRows, atBirth)
	p.seedRows = append(p.seedRows, s.seedingCreate(options))
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
			continue // provably unspendable: no UTXO row, ever
		}

		var script []byte
		if out.LockingScript != nil {
			script = *out.LockingScript
		}

		p.utxoSats = append(p.utxoSats, int64(out.Satoshis))
		p.utxoHeights = append(p.utxoHeights, int32(blockHeight))
		p.utxoSpendable = append(p.utxoSpendable, spendableFrom)
		p.utxoLeaves = append(p.utxoLeaves, leaf)
		p.utxoFlags = append(p.utxoFlags, flags)
		p.utxoUkeys = append(p.utxoUkeys, Pack(txHash[:], uint32(vout)))
		p.utxoTxids = append(p.utxoTxids, txHash[:])
		p.utxoScripts = append(p.utxoScripts, script)
		p.utxoMined = append(p.utxoMined, utxoMinedHeight)
		p.utxoBlockIDs = append(p.utxoBlockIDs, utxoBlockID)
	}

	var fee uint64
	if f := p.fees[len(p.fees)-1]; f != nil {
		fee = uint64(*f) //nolint:gosec // txFee never returns a negative fee
	}

	// The returned record has to say what was written, not just what the transaction is. Its
	// callers act on it without reading the row back, and the sql and aerospike stores return
	// the same three fields from their own Create.
	//
	// Locked: the validator creates a transaction locked, hands it to block assembly, and then
	// unlocks it, but only when the record it got back from the create says Locked. Without it
	// the unlock was skipped, the row stayed locked, and every child spending it was refused
	// with ErrTxLocked until the next block.
	//
	// Conflicting: subtree validation, blessing a missing transaction, checks the
	// counter-conflicting set only when the validator's returned record says Conflicting, and
	// the txmeta Kafka message the validator publishes carries the bit into the cache.
	//
	// TxInpoints: published in the same txmeta message. A cached entry for a non-coinbase with
	// no parents is treated as a miss (processTxMetaUsingCache), so leaving them out turned
	// every cache entry this store produced into a store read.
	return &meta.Data{
		Tx:          tx,
		TxInpoints:  txInpoints,
		Fee:         fee,
		SizeInBytes: uint64(txSize(tx)), //nolint:gosec // a size is never negative
		IsCoinbase:  isCoinbase,
		Conflicting: options.Conflicting,
		Locked:      options.Locked,
	}, nil
}

// txFee is the transaction's fee, or nil when the store cannot know it.
//
// A coinbase pays none. Otherwise the fee needs every input's value, which the transaction
// carries only when it arrives extended: the validator extends before it creates, and so does
// block validation above the checkpoint. The below-checkpoint fast path creates transactions
// without extending them; their fee is unknown rather than zero, so it is stored as NULL, and
// the block reward check is skipped there for that reason (model.Block.checkBlockRewardAndFees).
// Inputs worth less than the outputs are not this store's verdict to give; the validator has
// already refused such a transaction, so nil is returned rather than a wrapped negative.
func txFee(tx *bt.Tx) *int64 {
	if tx.IsCoinbase() {
		zero := int64(0)
		return &zero
	}

	if !tx.IsExtended() {
		return nil
	}

	fee, err := util.GetFees(tx)
	if err != nil || fee > math.MaxInt64 {
		return nil
	}

	f := int64(fee)

	return &f
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
// a transaction from the same pool -- so a second read of the tip that crossed a leaf
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
		if cerr := s.noteConflictOnParents(ctx, dbTx, tx, plan.txids[0], notedHeight); cerr != nil {
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

// createTxID is the ID a create files tx under: the caller's when it gives one. The seeder must,
// because a transaction it rebuilt from a UTXO snapshot does not hash to its real ID.
func createTxID(tx *bt.Tx, options *utxo.CreateOptions) chainhash.Hash {
	if options != nil && options.TxID != nil {
		return *options.TxID
	}

	return *tx.TxIDChainHash()
}

// isRebuilt reports a transaction rebuilt from its unspent outputs, with nil in the place of
// every spent one. Its bytes are not the transaction's, and serialising it panics.
func isRebuilt(tx *bt.Tx) bool {
	for _, out := range tx.Outputs {
		if out == nil {
			return true
		}
	}

	return false
}

// faithful reports whether tx's own bytes are the transaction filed under txHash.
func faithful(tx *bt.Tx, options *utxo.CreateOptions, txHash chainhash.Hash) bool {
	if isRebuilt(tx) {
		return false
	}

	if options == nil || options.TxID == nil {
		return true
	}

	return *tx.TxIDChainHash() == txHash
}

// txSize is tx.Size(), and for a rebuilt transaction the size of the parts it has: tx.Size()
// dereferences every output and panics on the missing ones.
func txSize(tx *bt.Tx) int {
	if !isRebuilt(tx) {
		return tx.Size()
	}

	size := 4 + len(bt.VarInt(uint64(len(tx.Inputs))).Bytes()) + len(bt.VarInt(uint64(len(tx.Outputs))).Bytes()) + 4

	for _, in := range tx.Inputs {
		if in != nil {
			size += in.Size()
		}
	}

	for _, out := range tx.Outputs {
		if out != nil {
			size += out.Size()
		}
	}

	return size
}

// seedingCreate reports whether a create is a seed's, written by the seeding route.
//
// A seed from a UTXO snapshot loads every unspent output at once, with its block height. Through
// the normal routes each transaction also gets a containment row, and above the checkpoint an
// identity row and coins at the unconfirmed sentinel; all of it waits for a stamp that never
// visits heights below a seed, and on mainnet the containment rows alone would be tens of
// gigabytes on the disk until the seed's hook clears them. The seeding route writes the coins
// alone, with their height and block already set, as the stamp would leave them.
//
// It needs seeding=true on the store URL, which only a seed's settings set, and it applies only
// to a create shaped like the seeder's: create-only, with the caller's txid and a block. Every
// other create takes its normal route even in seeding mode.
func (s *Store) seedingCreate(options *utxo.CreateOptions) bool {
	if !s.seeding || options == nil || !options.CreateOnly || options.TxID == nil {
		return false
	}

	_, mined := minedBlock(options.MinedBlockInfos)

	return mined
}
