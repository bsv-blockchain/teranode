package utxoset

import (
	"bytes"
	"context"
	"sort"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
)

// spendItem is one transaction's worth of spend within a plan.
//
// It is not a queue entry. This store has no spend batcher: the only way in is SpendAndCreate,
// which runs inside one database transaction so a rejected transaction leaves no coin
// destroyed. The struct survives because planSpends is multi-item by design, which is the
// shape a batched SpendAndCreate would need.
type spendItem struct {
	tx          *bt.Tx
	blockHeight uint32
	ignoreFlags utxo.IgnoreFlags
}

// spendPlan is the argument set for one call of the spend statement, however many
// transactions went into it, plus the mapping needed to give each caller its own answers.
//
// Building it in one place is what stops the batched and unbatched paths carrying separate
// copies of the same predicates. The single-transaction path is a batch of one.
type spendPlan struct {
	leaves   []int16
	ukeys    [][16]byte
	txids    [][]byte
	idx      []int32
	heights  []int32
	spenders [][]byte
	owner    []int // global index -> which item in the batch
	ownerVin []int // global index -> which input of that item
	perItem  [][]*utxo.Spend
	itemTxs  []*bt.Tx
	// masks[k] is the set of coin flags that refuse row k's spend, derived from the option
	// its own caller passed. Per row rather than per plan, because a batch can mix a conflict
	// resolution's waived spend with an ordinary one.
	masks []int16
	// skipClaim[i] suppresses the previous-output comparison for item i. Set only by the
	// gated below-checkpoint outpoint-only path, which is the one caller entitled to it. It
	// does not reach a reassigned coin: claimMismatch refuses that combination outright,
	// because a coin whose only stored authentication is a digest cannot also waive the claim
	// the digest is computed from.
	skipClaim []bool
}

// spendGuardMask is the set of coin flags that refuse a spend for one caller's options.
//
// Frozen is never waivable: no store offers an option for it, because the alert system's
// immobilisation is not something a caller may talk its way past. The other two are, and the
// waiver exists for exactly one caller. Conflict resolution marks a loser conflicting, locks
// the contested parent, and then has to spend that parent's coin on behalf of the winner --
// through both of its own marks. Every other caller gets the full mask.
func spendGuardMask(f utxo.IgnoreFlags) int16 {
	mask := FlagFrozen | FlagLocked | FlagConflicting

	if f.IgnoreConflicting {
		mask &^= FlagConflicting
	}

	if f.IgnoreLocked {
		mask &^= FlagLocked
	}

	return mask
}

// planSpends flattens a batch of transactions into one set of arrays.
func planSpends(items []*spendItem) *spendPlan {
	total := 0
	for _, it := range items {
		if it.tx != nil && !it.tx.IsCoinbase() {
			total += len(it.tx.Inputs)
		}
	}

	p := &spendPlan{
		leaves:   make([]int16, 0, total),
		ukeys:    make([][16]byte, 0, total),
		txids:    make([][]byte, 0, total),
		idx:      make([]int32, 0, total),
		heights:  make([]int32, 0, total),
		spenders: make([][]byte, 0, total),
		masks:    make([]int16, 0, total),
		owner:    make([]int, 0, total),
		ownerVin: make([]int, 0, total),
		perItem:  make([][]*utxo.Spend, len(items)),
		itemTxs:  make([]*bt.Tx, len(items)),

		skipClaim: make([]bool, len(items)),
	}

	for i, it := range items {
		p.itemTxs[i] = it.tx
		p.skipClaim[i] = it.ignoreFlags.SkipUTXOHashCheck

		if it.tx == nil || it.tx.IsCoinbase() {
			continue
		}

		spends := make([]*utxo.Spend, len(it.tx.Inputs))
		spendingTxID := it.tx.TxIDChainHash()
		mask := spendGuardMask(it.ignoreFlags)

		// One backing array for the records and one for the spenders, rather than a fresh
		// allocation per input. This loop runs once per input of every transaction in every
		// block, so the whole set costs three allocations regardless of input count where the
		// obvious form would cost two more per input.
		records := make([]utxo.Spend, len(it.tx.Inputs))
		spenders := make([]spendpkg.SpendingData, len(it.tx.Inputs))

		for vin, in := range it.tx.Inputs {
			parent := in.PreviousTxIDChainHash()

			p.leaves = append(p.leaves, LeafFor(parent[:]))
			p.ukeys = append(p.ukeys, Pack(parent[:], in.PreviousTxOutIndex))
			p.txids = append(p.txids, parent[:])
			p.idx = append(p.idx, int32(len(p.owner))) //nolint:gosec // bounded by batch size
			p.heights = append(p.heights, int32(it.blockHeight))
			p.spenders = append(p.spenders, spendingTxID[:])
			p.masks = append(p.masks, mask)
			p.owner = append(p.owner, i)
			p.ownerVin = append(p.ownerVin, vin)

			// The spender belongs on the record from the moment the record exists. Conflict
			// resolution hands these same records straight back to this store's Unspend,
			// which restores on the spender and REFUSES a record that cannot name the
			// transaction that took the coin. Handing out records without one turned every
			// conflict-resolution failure into the manual-intervention escalation, whatever
			// had actually gone wrong, because the rollback itself could never succeed.
			//
			// The coin hash is deliberately left unset. Computing it is a double hash per
			// input, which is a real cost on this path, and nothing that reads these records
			// uses it: this store's Unspend restores on the outpoint and the spender.
			spenders[vin] = spendpkg.SpendingData{TxID: spendingTxID, Vin: vin}
			records[vin] = utxo.Spend{
				TxID:         parent,
				Vout:         in.PreviousTxOutIndex,
				SpendingData: &spenders[vin],
			}
			spends[vin] = &records[vin]
		}

		p.perItem[i] = spends
	}

	p.sortRows()

	return p
}

// sortRows puts the plan's rows in one global order, by leaf, coin key and txid, so every
// statement built from a plan asks for its rows in the same order as every other.
//
// Two batches that spend overlapping coins take their row locks in statement order. In array
// order that is submission order, which a submitter controls, so two batches carrying the same
// pair of coins the other way round would deadlock and both be redone as singles after the
// deadlock timeout. In one global order there is no cycle to form. It costs a sort of the
// batch and nothing else: k is reassigned after the sort, so the RETURNING mapping is exact,
// and the sort is stable, so of two rows for one coin the earlier item still comes first.
func (p *spendPlan) sortRows() {
	n := len(p.owner)
	if n < 2 {
		return
	}

	order := make([]int, n)
	for i := range order {
		order[i] = i
	}

	sort.SliceStable(order, func(a, b int) bool {
		x, y := order[a], order[b]
		if p.leaves[x] != p.leaves[y] {
			return p.leaves[x] < p.leaves[y]
		}

		if c := bytes.Compare(p.ukeys[x][:], p.ukeys[y][:]); c != 0 {
			return c < 0
		}

		return bytes.Compare(p.txids[x], p.txids[y]) < 0
	})

	p.leaves = permute(p.leaves, order)
	p.ukeys = permute(p.ukeys, order)
	p.txids = permute(p.txids, order)
	p.heights = permute(p.heights, order)
	p.spenders = permute(p.spenders, order)
	p.masks = permute(p.masks, order)
	p.owner = permute(p.owner, order)
	p.ownerVin = permute(p.ownerVin, order)

	for k := range p.idx {
		p.idx[k] = int32(k) //nolint:gosec // bounded by batch size
	}
}

// permute returns xs reordered so that the result's i-th element is xs[order[i]].
func permute[T any](xs []T, order []int) []T {
	out := make([]T, len(xs))
	for i, from := range order {
		out[i] = xs[from]
	}

	return out
}

// claimMismatch compares what a spending transaction CLAIMED about the coin against what the
// store has just handed back for it, and returns the rejection when they differ.
//
// This is the control the other two stores get from the UTXO hash, and the reason that hash
// has content. A transaction may be submitted in EXTENDED FORMAT, meaning it carries its own
// copy of every coin it spends -- the coin's value in satoshis and its locking script, the
// rules for who may move it. The validator deliberately does not re-derive those when they
// arrive; it validates against whatever the transaction brought. So the no-inflation check
// sums the submitter's satoshis and script verification runs against the submitter's script.
// The hash the other two stores compare at the spend is computed from those carried copies,
// which is what makes it an authentication of the SUBMITTER rather than a consistency check on
// the store.
//
// This store needs no hash to do the same job. Its DELETE returns the coin's real satoshis and
// script in the same round trip, because the spend is also the decorate fetch, so it holds the
// truth at exactly the moment the other two are comparing digests. Comparing the values
// directly is cheaper -- a byte comparison instead of a double hash per input, on the hottest
// path in the store -- and strictly stronger, because it compares the values themselves rather
// than a digest of them.
//
// A nil PreviousTxScript means the input carried no claim at all, which is the un-decorated
// below-checkpoint outpoint-only path. There is nothing to authenticate, and that path
// switches script validation off in the same breath, so nothing acts on a claim either.
//
// The error is deliberately the one the other two stores already raise for this condition.
// Their needsSpendRollback reads it as a genuine invalidity rather than a transient failure,
// and conflict resolution reads it as "this transaction is invalid" rather than "this
// transaction lost a race" -- so a false claim is rejected outright instead of being kept as a
// conflicting transaction whose descendants get walked.
//
// Rejection is all-or-nothing on every path a caller can reach in production: SpendAndCreate
// runs the spend inside one database transaction and rolls it back on any per-input error, and
// SpendAndCreate is the only entry point the validator, block validation and conflict
// resolution use. The batched Spend path has no enclosing transaction, so a rejected
// transaction there leaves the rows the DELETE already took. That is a pre-existing property
// of that path for every error class, not something this comparison introduces, and no caller
// outside tests reaches it.
// The skipClaim decision is a PARAMETER rather than a gate at the call site, and that is the
// point of it being here. It used to be an if around the whole call, written independently of
// the reassignment check inside; the two then interacted silently, and an outpoint-only spend
// of a reassigned coin was authenticated by nothing at all. One function decides now, so the
// exemption cannot be granted without the exception to it being considered in the same breath.
func claimMismatch(in *bt.Input, sp *utxo.Spend, satoshis int64, script []byte, hashOverride []byte,
	skipClaim bool) error {
	// A REASSIGNED coin is the one case with no way out. There is nothing on the row to
	// compare a claim against -- the satoshis and the script are the confiscated owner's,
	// since ReAssignUTXO is handed a hash and nothing else -- so the digest is the only
	// authentication available, and a spend that presents no claim, or is excused from
	// presenting one, would be authorised by the outpoint alone. The outpoint is exactly what
	// the confiscated party still knows.
	if len(hashOverride) > 0 {
		if skipClaim {
			return errors.NewUtxoHashMismatchError(
				"[utxoset][Spend] %s:%d was reassigned and cannot be spent outpoint-only; the new output has to be presented",
				sp.TxID, sp.Vout)
		}

		if in == nil || in.PreviousTxScript == nil {
			return errors.NewUtxoHashMismatchError(
				"[utxoset][Spend] %s:%d was reassigned and was offered with no output to check; the spending transaction must arrive extended",
				sp.TxID, sp.Vout)
		}

		return reassignedClaimMismatch(in, sp, hashOverride)
	}

	if skipClaim {
		return nil
	}

	if in == nil || in.PreviousTxScript == nil {
		return nil
	}

	if in.PreviousTxSatoshis == uint64(satoshis) && //nolint:gosec // satoshis are never negative
		bytes.Equal(*in.PreviousTxScript, script) {
		return nil
	}

	// The scripts are reported by length rather than by content: one of them is attacker
	// supplied and unbounded, and this string reaches the logs.
	return errors.NewUtxoHashMismatchError(
		"[utxoset][Spend] %s:%d was offered as %d satoshis with a %d-byte script, the store holds %d satoshis with a %d-byte script",
		sp.TxID, sp.Vout, in.PreviousTxSatoshis, len(*in.PreviousTxScript), satoshis, len(script))
}

// reassignedClaimMismatch is the one case where this store has to fall back on a digest.
//
// ReAssignUTXO is handed a utxo.Spend, which carries a UTXO hash and no room for a locking
// script or an amount, so a reassigned coin's row still holds the OLD owner's script and
// satoshis and there is nothing to compare a claim against. hash_override is what the store
// does hold about the new output, so the claim is hashed and matched to it. That is exactly
// the check the aerospike and sql stores run on every spend; here it applies only to the
// coins an alert has moved, which are vanishingly few.
//
// It inverts the outcome for both parties, which is the point. The old owner's claim -- the
// script the coin still literally carries -- now hashes to something else and is refused,
// while the new owner's, which matches nothing on the row, is accepted.
func reassignedClaimMismatch(in *bt.Input, sp *utxo.Spend, hashOverride []byte) error {
	claimed, err := util.UTXOHashFromInput(in)
	if err != nil {
		return errors.NewUtxoHashMismatchError("[utxoset][Spend] %s:%d was reassigned and the offered output cannot be hashed",
			sp.TxID, sp.Vout, err)
	}

	if bytes.Equal(claimed[:], hashOverride) {
		return nil
	}

	return errors.NewUtxoHashMismatchError(
		"[utxoset][Spend] %s:%d was reassigned; the offered %d satoshis with a %d-byte script hash to %x, the store expects %x",
		sp.TxID, sp.Vout, in.PreviousTxSatoshis, len(*in.PreviousTxScript), claimed[:], hashOverride)
}

// decorateInput writes the coin's real satoshis and locking script onto the input, which is
// how the spend doubles as the decorate fetch: script validation reads them straight off the
// input and never fetches a parent transaction.
//
// A REASSIGNED coin is left alone, and it has to be. The row's satoshis and script are the old
// owner's -- ReAssignUTXO was given a hash and nothing else -- so overwriting would replace
// the new owner's correct output with the confiscated one, and script validation would then
// run the old locking script against the new owner's unlocking script and fail every time.
// What the input already carries has just been authenticated against hash_override by
// claimMismatch, so it is the better source, not merely the only one.
func decorateInput(in *bt.Input, satoshis int64, script []byte, hashOverride []byte) {
	if len(hashOverride) > 0 {
		return
	}

	in.PreviousTxSatoshis = uint64(satoshis) //nolint:gosec // satoshis are never negative
	in.PreviousTxScript = bscript.NewFromBytes(script)
}

// runSpendPlan issues the statement and fills in each caller's answers, including the
// per-input errors that conflict detection reads.
func (s *Store) runSpendPlan(ctx context.Context, q pgx.Tx, p *spendPlan) error {
	if len(p.owner) == 0 {
		return nil
	}

	rows, err := q.Query(ctx, spendJournalSQL, p.leaves, p.ukeys, p.txids, p.idx,
		p.heights, p.spenders, p.masks)
	if err != nil {
		return errors.NewStorageError("[utxoset][Spend] delete", err)
	}

	done := make(map[int32]struct{}, len(p.owner))

	for rows.Next() {
		var (
			k            int32
			satoshis     int64
			script       []byte
			hashOverride []byte
		)

		if err := rows.Scan(&k, &satoshis, &script, &hashOverride); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][Spend] scan", err)
		}

		done[k] = struct{}{}

		// The decorate fetch, free: the input now carries what script validation needs, so
		// nothing has to read the parent transaction for it.
		item := p.owner[k]
		vin := p.ownerVin[k]

		if in := p.itemTxs[item].Inputs[vin]; in != nil {
			// Before the overwrite, and it has to be: the overwrite is what destroys the
			// claim this is checking.
			if err := claimMismatch(in, p.perItem[item][vin], satoshis, script, hashOverride,
				p.skipClaim[item]); err != nil {
				p.perItem[item][vin].Err = err
			}

			decorateInput(in, satoshis, script, hashOverride)
		}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Spend] rows", err)
	}

	if len(done) == len(p.owner) {
		return nil
	}

	return s.classifyPlanMisses(ctx, q, p, done)
}

// classifyPlanMisses turns "the DELETE did not take this row" into a specific error, per
// input, for every caller in the batch.
//
// The distinction cannot be skipped: absence alone cannot tell "already spent" from "never
// existed" from "exists but is not yet spendable", and the validator behaves differently for
// each.
func (s *Store) classifyPlanMisses(ctx context.Context, q pgx.Tx, p *spendPlan, done map[int32]struct{}) error {
	rows, err := q.Query(ctx, classifySQL, p.leaves, p.ukeys, p.txids, p.idx)
	if err != nil {
		return errors.NewStorageError("[utxoset][Spend] classify", err)
	}

	present := make(map[int32]struct{})

	for rows.Next() {
		var (
			k             int32
			flags         int16
			spendableFrom int32
			hashOverride  []byte
		)

		if err := rows.Scan(&k, &flags, &spendableFrom, &hashOverride); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][Spend] classify scan", err)
		}

		present[k] = struct{}{}

		sp := p.perItem[p.owner[k]][p.ownerVin[k]]

		// Only the flags THIS caller's mask actually refuses on. A conflict resolution that
		// waived the lock and then found the coin immature must be told it is immature, not
		// handed back the flag it already said to ignore.
		//
		// The order is terminal before transient, which is the opposite of the precedence
		// GetSpend reports and deliberately so. Frozen and conflicting are settled facts about
		// the coin; ErrTxLocked means "held by an operation in flight", and the validator and
		// legacy netsync both READ IT AS RETRYABLE. Reporting the transient error for a coin
		// that is also permanently refused would send the caller round a retry loop that can
		// never come out. GetSpend has no retry to mislead, so it reports the most specific
		// state instead, matching both reference stores.
		refusing := flags & p.masks[k]

		switch {
		case refusing&FlagFrozen != 0:
			sp.Err = errors.ErrFrozen
		case refusing&FlagConflicting != 0:
			sp.Err = errors.ErrTxConflicting
		case refusing&FlagLocked != 0:
			sp.Err = errors.ErrTxLocked
		case spendableFrom > p.heights[k]:
			// Exists, but not yet spendable. NOT a double spend, and reporting it as one
			// would be wrong. One column carries two different holds and they get different
			// errors, because their callers act on them differently.
			//
			// THE REASSIGNMENT DELAY IS TESTED FIRST, and the order matters because a
			// reassigned COINBASE would otherwise be answered by the wrong branch. The alert
			// system's hold is ErrFrozen: it must not be reported as merely locked, as a plain
			// processing error, or as coinbase immaturity, because the shared rollback
			// predicate lists ErrFrozen and lists none of those. Get it wrong and a
			// multi-input transaction that fails on one held input strands its other inputs
			// marked spent by a transaction that can never be accepted. The sql store carries
			// a test pinning the classification (spendable_in_frozen_test.go).
			//
			// hash_override is what identifies the hold as a reassignment's. reassignSQL is
			// the only writer of it on a live coin, and it writes hash_override and
			// spendable_from in the same statement, overwriting whatever maturity the coin
			// carried before. So on a coin with an override the surviving hold IS the reassign
			// delay, whether or not the coin began life as a coinbase.
			//
			// Everything left is a coinbase inside its maturity window, which is
			// ErrTxCoinbaseImmature: the coin becomes spendable at a known height and nothing
			// is wrong with it.
			switch {
			case len(hashOverride) > 0:
				sp.Err = errors.NewUtxoFrozenError("[utxoset] utxo %s:%d was reassigned and is held until height %d (current %d)",
					sp.TxID, sp.Vout, spendableFrom, p.heights[k])
			case flags&FlagCoinbase != 0:
				sp.Err = errors.NewTxCoinbaseImmatureError("[utxoset] coinbase %s:%d not spendable until height %d (current %d)",
					sp.TxID, sp.Vout, spendableFrom, p.heights[k])
			default:
				sp.Err = errors.NewUtxoFrozenError("[utxoset] utxo %s:%d is held until height %d (current %d)",
					sp.TxID, sp.Vout, spendableFrom, p.heights[k])
			}
		default:
			// Present, eligible, and yet not deleted. The DELETE and this lookup run under
			// different snapshots, so the only way here is a row that appeared between them:
			// a create or an unspend that committed a moment ago. Leaving the record without
			// an error would report the input as spent while its coin sits live in the table,
			// and the caller would go on to store a child whose parent coin nobody consumed.
			// A storage error is what callers retry on, and a retry finds the coin.
			sp.Err = errors.NewStorageError("[utxoset] utxo %s:%d appeared after the spend's snapshot, retry",
				sp.TxID, sp.Vout)
		}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Spend] classify rows", err)
	}

	// Anything neither deleted nor present is gone from the coin table: already spent, or
	// never there at all. The coin table cannot tell those apart on its own, so ErrSpent is
	// the provisional answer and the journal is asked next for who took it. An input the
	// journal cannot explain either goes on to nameUnknownParents, which decides between
	// "spent" and "never seen".
	missing := false

	for _, k := range p.idx {
		if _, ok := done[k]; ok {
			continue
		}

		if _, ok := present[k]; !ok {
			p.perItem[p.owner[k]][p.ownerVin[k]].Err = errors.ErrSpent
			missing = true
		}
	}

	if !missing {
		return nil
	}

	return s.namePlanSpenders(ctx, q, p, done)
}

// namePlanSpenders reads the journal for every coin that is no longer there, and does two
// different jobs with the answer.
//
// If the transaction the journal names is the one now spending, this is a REPLAY of our own
// earlier work rather than a competing spend, and it must succeed. Delete-on-spend destroys
// the coin row, so a block interrupted part-way through application leaves its coins already
// gone; re-offering that block asks the store to take them again. Calling that a double spend
// is not merely unhelpful, it is fatal: the block can never be applied, the tip never
// advances, and no restart helps. Mainnet wedged at height 97389 exactly this way, with all
// 200 inputs of one transaction reported as already spent by that same transaction.
//
// The replay still has to decorate the input, because the spend is also the decorate fetch and
// script validation has no other source for the satoshis and the locking script. The journal
// row captured both at the moment of the delete, so it can serve them unchanged.
//
// If the journal names a DIFFERENT transaction, this is a real double spend and the caller
// needs to know who won, so it can mark the loser conflicting and walk its descendants.
//
// Bounded by the journal's retention. Beyond that the store genuinely cannot say, and a replay
// that old cannot be recognised either, which is a stated limit of delete-on-spend rather than
// a gap to paper over.
func (s *Store) namePlanSpenders(ctx context.Context, q pgx.Tx, p *spendPlan, done map[int32]struct{}) error {
	rows, err := q.Query(ctx, spenderSQL, p.leaves, p.ukeys, p.txids, p.idx)
	if err != nil {
		return errors.NewStorageError("[utxoset][Spend] find spender", err)
	}

	defer rows.Close()

	// A coin can appear in the journal more than once, having been spent, restored by an
	// unspend, and spent again. One matching row is enough to make this a replay, so once an
	// input is settled that way no later row may unsettle it.
	replayed := make(map[int32]struct{})

	for rows.Next() {
		var (
			k            int32
			spender      []byte
			satoshis     int64
			script       []byte
			hashOverride []byte
		)

		if err := rows.Scan(&k, &spender, &satoshis, &script, &hashOverride); err != nil {
			return errors.NewStorageError("[utxoset][Spend] spender scan", err)
		}

		// An input THIS statement just took is not a replay of earlier work, it is this
		// work. The journal row naming this spender is the one the same statement wrote a
		// moment ago, so without this guard the replay branch below reads its own writes,
		// declares a replay, and clears whatever verdict runSpendPlan reached -- including a
		// rejected false claim about the coin, which is then committed.
		if _, taken := done[k]; taken {
			continue
		}

		sp := p.perItem[p.owner[k]][p.ownerVin[k]]
		if sp == nil {
			continue
		}

		if _, settled := replayed[k]; settled {
			continue
		}

		if bytes.Equal(spender, p.spenders[k]) {
			replayed[k] = struct{}{}

			// The replay decorates from the journal rather than from the coin row, which
			// makes this the SECOND place the store hands a caller's claim back
			// unexamined. It gets the same comparison as the first, against the payload
			// the journal captured at the moment of the delete.
			in := p.itemTxs[p.owner[k]].Inputs[p.ownerVin[k]]

			sp.Err = nil
			sp.ConflictingTxID = nil

			sp.Err = claimMismatch(in, sp, satoshis, script, hashOverride, p.skipClaim[p.owner[k]])

			if in != nil {
				decorateInput(in, satoshis, script, hashOverride)
			}

			continue
		}

		if !errors.Is(sp.Err, errors.ErrSpent) {
			continue
		}

		h, herr := chainhash.NewHash(spender)
		if herr != nil {
			return errors.NewStorageError("[utxoset][Spend] spender hash", herr)
		}

		sp.ConflictingTxID = h
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Spend] spender rows", err)
	}

	return s.nameUnknownParents(ctx, q, p)
}

// parentKnownSQL asks, for a set of outpoints, whether the store holds the parent transaction
// at all -- in any of the four places it can be held.
//
// Each probe sits inside a LATERAL with an OFFSET 0 fence, the shape minedByTxidSQL and
// firstMinedRowSQL use, so each is one index descent per key rather than a subquery the
// planner is free to pull up and hash against the whole table. Written as four ORed EXISTS
// clauses the planner does exactly that: measured elsewhere in this store on 40,000 coins, an
// unfenced EXISTS over the coin table planned as a hashed SubPlan across every leaf partition.
//
// The coin probe is bounded by the packed-key RANGE and rechecked on the full 32-byte txid,
// which schema.go requires of every by-txid coin access, and it asks about ANY output of the
// parent rather than the one being spent: a parent whose membership window has retired is
// known to this store only through a surviving coin, which is exactly what a pruned SV Node
// can say about one.
const parentKnownSQL = `
SELECT k.vin
  FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[], $5::int[])
       AS k(leaf, txid, lo, hi, vin)
  LEFT JOIN LATERAL (
    SELECT 1 AS hit FROM tx_ident i
     WHERE i.leaf = k.leaf AND i.txid = k.txid LIMIT 1 OFFSET 0) a ON TRUE
  LEFT JOIN LATERAL (
    SELECT 1 AS hit FROM tx_mined m
     WHERE m.txid = k.txid LIMIT 1 OFFSET 0) b ON TRUE
  LEFT JOIN LATERAL (
    SELECT 1 AS hit FROM preserved_parent pp
     WHERE pp.txid = k.txid LIMIT 1 OFFSET 0) c ON TRUE
  LEFT JOIN LATERAL (
    SELECT 1 AS hit FROM utxo u
     WHERE u.leaf = k.leaf AND u.ukey BETWEEN k.lo AND k.hi AND u.txid = k.txid
     LIMIT 1 OFFSET 0) d ON TRUE
 WHERE a.hit IS NOT NULL OR b.hit IS NOT NULL OR c.hit IS NOT NULL OR d.hit IS NOT NULL`

// nameUnknownParents downgrades ErrSpent to ErrTxNotFound for the inputs whose parent this
// store has never held.
//
// The two are not interchangeable to the caller. ErrSpent means a competing transaction took
// the coin, and the validator answers it by marking the loser conflicting and walking its
// descendants. ErrTxNotFound means the parent has not arrived, and the validator answers it by
// fetching the parent and retrying. Reporting the first for an outpoint the store has never
// seen makes the node declare a double spend against a transaction that does not exist, which
// is both wrong and unrecoverable without operator action. Both reference stores distinguish
// them, because both read the parent's record before they touch a coin.
//
// This store does not read the parent's record on the spend path, deliberately -- the spend IS
// the read, straight off the coin, and that is where its speed comes from. So the question is
// asked only here, for the inputs nothing else could explain: no live coin, and no journal row
// naming a spender. That is the error path, never the hot one.
//
// ONE CASE STAYS AMBIGUOUS. A parent whose membership window has retired, whose coins are all
// gone, and whose spend is older than journal retention leaves no trace here at all, so it is
// indistinguishable from one that never arrived, and it is reported as NOT FOUND like the rest.
// That is the conservative choice: it asks the caller to fetch a parent rather than to condemn a
// transaction as a double spend on no evidence.
func (s *Store) nameUnknownParents(ctx context.Context, q pgx.Tx, p *spendPlan) error {
	var (
		leaves []int16
		txids  [][]byte
		los    [][16]byte
		his    [][16]byte
		vins   []int32
	)

	for _, k := range p.idx {
		sp := p.perItem[p.owner[k]][p.ownerVin[k]]
		// Only the inputs still unexplained: ErrSpent with nobody named as having taken it.
		if sp == nil || !errors.Is(sp.Err, errors.ErrSpent) || sp.ConflictingTxID != nil {
			continue
		}

		leaves = append(leaves, p.leaves[k])
		txids = append(txids, p.txids[k])
		los = append(los, Pack(p.txids[k], 0))
		his = append(his, Pack(p.txids[k], ^uint32(0)))
		vins = append(vins, k)
	}

	if len(vins) == 0 {
		return nil
	}

	rows, err := q.Query(ctx, parentKnownSQL, leaves, txids, los, his, vins)
	if err != nil {
		return errors.NewStorageError("[utxoset][Spend] parent known", err)
	}

	defer rows.Close()

	known := make(map[int32]struct{}, len(vins))

	for rows.Next() {
		var k int32

		if err := rows.Scan(&k); err != nil {
			return errors.NewStorageError("[utxoset][Spend] parent known scan", err)
		}

		known[k] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Spend] parent known rows", err)
	}

	for _, k := range vins {
		if _, ok := known[k]; ok {
			continue
		}

		sp := p.perItem[p.owner[k]][p.ownerVin[k]]
		sp.Err = errors.NewTxNotFoundError("[utxoset] %s:%d has no parent transaction in this store",
			sp.TxID, sp.Vout)
	}

	return nil
}
