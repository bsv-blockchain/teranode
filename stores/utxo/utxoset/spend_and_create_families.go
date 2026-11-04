package utxoset

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
)

// Family chunks: how SpendAndCreateMulti writes a list above the checkpoint.
//
// A FAMILY is a set of the list's transactions joined by in-list edges, an input whose parent is
// earlier in the list, directly or through a chain. Most families on mainnet are one transaction.
// A CHUNK is up to multiCreateChunkTxs transactions written in ONE database transaction. Whole
// families are packed into chunks in list order, so an output created and spent inside one chunk
// is netted exactly as before: no coin row, and a spend-journal row naming the child.
//
// A family too big for one chunk is cut into slices in dependency order, each slice a chunk. An
// edge whose parent and child land in different chunks is a CUT EDGE. The parent's chunk writes
// that output as a real coin, because nothing in its chunk spends it, and the child's chunk
// spends it like any outside coin. A chunk waits for the commit of every chunk that holds the
// parent of a cut edge; chunks with no edge between them run in parallel.
//
// The rule that makes a stopped write safe: every commit leaves a true state.
// A chunk locks its transactions, spends the outside coins of them, stores them, and commits, all
// in one database transaction. It does not ask first which already exist: it spends and stores,
// and when the create claim finds one stored, or a spend is refused because that same transaction
// already spent the coin, it rolls the chunk back and redoes it without that transaction. A transaction
// is stored if and only if its spends are made, so a failed or crashed chunk leaves nothing, and
// a writer never has to know whether another writer of the same transaction exists or is alive.
// Two writers of one transaction take its advisory lock in turn, and the second finds it stored.

// chunkStatus is what one chunk round has decided about a transaction.
type chunkStatus uint8

const (
	chunkLive chunkStatus = iota
	chunkExisted
	chunkFailed
	chunkParentFailed
)

// familyChunk is one database transaction's worth of the list.
type familyChunk struct {
	idx int
	// pos is the list positions of its transactions, ascending, which is a dependency order.
	pos []int
	// deps is the earlier chunks that hold a parent of a member: the chunks this one waits for.
	deps  []int
	count int
	bytes int
}

// chunkPlan is a list cut into chunks.
type chunkPlan struct {
	chunks  []*familyChunk
	chunkOf []int
	// families is the number of families, largest the size of the biggest, and cutEdges the
	// number of in-list edges whose parent and child are in different chunks.
	families int
	largest  int
	cutEdges int
}

// ranks returns, for every chunk, the length of the longest chain of chunks that starts at it,
// itself included, and the chunks that depend on each chunk. Dependants have higher indexes, so
// one pass from the end sees each chunk's dependants ranked already. The largest rank is the
// critical path of the write: the number of chunks that must commit one after another.
func (p *chunkPlan) ranks() (rank []int, dependants [][]int) {
	n := len(p.chunks)
	rank = make([]int, n)
	dependants = make([][]int, n)

	for _, ch := range p.chunks {
		for _, d := range ch.deps {
			dependants[d] = append(dependants[d], ch.idx)
		}
	}

	for i := n - 1; i >= 0; i-- {
		rank[i] = 1

		for _, c := range dependants[i] {
			rank[i] = max(rank[i], rank[c]+1)
		}
	}

	return rank, dependants
}

// planFamilyChunks groups the list into families and packs them into chunks of at most maxTxs
// transactions and maxBytes bytes. parents[i] is the list positions of i's parents, all earlier.
// A single transaction over maxBytes still gets a chunk to itself.
func planFamilyChunks(parents [][]int, sizes []int, maxTxs, maxBytes int) *chunkPlan {
	n := len(parents)
	root := make([]int, n)

	for i := range root {
		root[i] = i
	}

	find := func(x int) int {
		for root[x] != x {
			root[x] = root[root[x]]
			x = root[x]
		}

		return x
	}

	for i, ps := range parents {
		for _, p := range ps {
			if a, b := find(i), find(p); a != b {
				root[max(a, b)] = min(a, b)
			}
		}
	}

	// Families in the order of their first member, members ascending.
	var families [][]int

	slot := make(map[int]int)

	for i := 0; i < n; i++ {
		r := find(i)

		k, ok := slot[r]
		if !ok {
			k = len(families)
			slot[r] = k
			families = append(families, nil)
		}

		families[k] = append(families[k], i)
	}

	plan := &chunkPlan{chunkOf: make([]int, n), families: len(families)}

	var cur *familyChunk

	closeCur := func() {
		if cur != nil && cur.count > 0 {
			plan.chunks = append(plan.chunks, cur)
		}

		cur = &familyChunk{}
	}

	closeCur()

	add := func(i int) {
		cur.pos = append(cur.pos, i)
		cur.count++
		cur.bytes += sizes[i]
	}

	for _, fam := range families {
		plan.largest = max(plan.largest, len(fam))

		famBytes := 0
		for _, i := range fam {
			famBytes += sizes[i]
		}

		if len(fam) <= maxTxs && famBytes <= maxBytes {
			if cur.count > 0 && (cur.count+len(fam) > maxTxs || cur.bytes+famBytes > maxBytes) {
				closeCur()
			}

			for _, i := range fam {
				add(i)
			}

			continue
		}

		// A family too big for one chunk starts its own, and its last slice stays open so a
		// following small family can share it.
		closeCur()

		for _, slice := range sliceFamily(fam, parents, sizes, maxTxs, maxBytes) {
			if cur.count > 0 {
				closeCur()
			}

			for _, i := range slice {
				add(i)
			}
		}
	}

	closeCur()

	for k, ch := range plan.chunks {
		ch.idx = k
		sort.Ints(ch.pos)

		for _, i := range ch.pos {
			plan.chunkOf[i] = k
		}
	}

	for _, ch := range plan.chunks {
		seen := map[int]struct{}{}

		for _, i := range ch.pos {
			for _, p := range parents[i] {
				if pc := plan.chunkOf[p]; pc != ch.idx {
					plan.cutEdges++

					if _, dup := seen[pc]; !dup {
						seen[pc] = struct{}{}
						ch.deps = append(ch.deps, pc)
					}
				}
			}
		}

		sort.Ints(ch.deps)
	}

	return plan
}

// sliceFamily cuts one family, whose members are ascending list positions and so already a
// dependency order, into runs that each fit the bounds. It tries two dependency orders, the list's
// and a chain-following one, and keeps whichever cuts fewer edges.
func sliceFamily(fam []int, parents [][]int, sizes []int, maxTxs, maxBytes int) [][]int {
	byList := cutRuns(fam, sizes, maxTxs, maxBytes)
	byChain := cutRuns(chainOrder(fam, parents), sizes, maxTxs, maxBytes)

	if cutsOf(byChain, parents) < cutsOf(byList, parents) {
		return byChain
	}

	return byList
}

// cutRuns fills runs greedily from order.
func cutRuns(order, sizes []int, maxTxs, maxBytes int) [][]int {
	var runs [][]int

	cur := make([]int, 0, min(len(order), maxTxs))
	bytes := 0

	for _, i := range order {
		if len(cur) > 0 && (len(cur)+1 > maxTxs || bytes+sizes[i] > maxBytes) {
			runs = append(runs, cur)
			cur, bytes = make([]int, 0, min(len(order), maxTxs)), 0
		}

		cur = append(cur, i)
		bytes += sizes[i]
	}

	if len(cur) > 0 {
		runs = append(runs, cur)
	}

	return runs
}

// cutsOf is the number of edges between members of different runs.
func cutsOf(runs [][]int, parents [][]int) int {
	runOf := make(map[int]int)

	for k, run := range runs {
		for _, i := range run {
			runOf[i] = k
		}
	}

	cuts := 0

	for k, run := range runs {
		for _, i := range run {
			for _, p := range parents[i] {
				if runOf[p] != k {
					cuts++
				}
			}
		}
	}

	return cuts
}

// chainOrder is a dependency order of fam that follows a chain to its end before it starts the
// next branch, so a chain stays together when the order is cut into runs. It is Kahn's algorithm
// with a stack where the usual one has a queue.
func chainOrder(fam []int, parents [][]int) []int {
	pending := make(map[int]int, len(fam))
	children := make(map[int][]int, len(fam))

	for _, i := range fam {
		pending[i] = len(parents[i])

		for _, p := range parents[i] {
			children[p] = append(children[p], i)
		}
	}

	var stack []int

	for k := len(fam) - 1; k >= 0; k-- {
		if pending[fam[k]] == 0 {
			stack = append(stack, fam[k])
		}
	}

	out := make([]int, 0, len(fam))

	for len(stack) > 0 {
		i := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		out = append(out, i)

		kids := children[i]
		for k := len(kids) - 1; k >= 0; k-- {
			c := kids[k]

			if pending[c]--; pending[c] == 0 {
				stack = append(stack, c)
			}
		}
	}

	return out
}

// multiChunkFault is a test hook called inside a chunk's database transaction at "locked", after
// its transactions' locks are held and the existing ones found; "spent", after its outside
// coins are deleted; and "created", after its records are written and before the commit; and
// after the commit as "committed". An error from it stops the chunk. errMultiCrash stops it as a
// crash would. The open transaction is rolled back either way, which is what the server does
// for a connection that is gone, and the write returns the error. nil in production.
var multiChunkFault func(chunk int, stage string) error

func chunkFault(chunk int, stage string) error {
	if multiChunkFault != nil {
		return multiChunkFault(chunk, stage)
	}

	return nil
}

// existingTxidsSQL returns the txids among $1-$4 that the store already holds, by the three
// guards the create claim carries (createIdentPlanSQL): an identity row, a containment row at or
// above the claim floor $5, and a live UTXO of the transaction's own. It is read under the
// transactions' advisory locks, which every create of a transaction takes before its claim, so
// the answer stays true until this transaction ends.
const existingTxidsSQL = `
SELECT t.txid
  FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[]) AS t(leaf, txid, lo, hi)
 WHERE EXISTS (SELECT 1 FROM tx_ident i WHERE i.leaf = t.leaf AND i.txid = t.txid)
    OR EXISTS (SELECT 1 FROM tx_mined m
                WHERE m.txid = t.txid AND m.mined_height >= $5::int
                LIMIT 1 OFFSET 0)
    OR EXISTS (SELECT 1 FROM utxo u
                WHERE u.leaf = t.leaf AND u.ukey >= t.lo AND u.ukey <= t.hi AND u.txid = t.txid
                ORDER BY u.ukey LIMIT 1 OFFSET 0)`

// familyStats is what a family write did and where its time went, for the slow-list log line.
// The chunk timings are summed across chunks that run in parallel, so they can exceed wall time.
type familyStats struct {
	chunks      int
	families    int
	largest     int
	cutEdges    int
	rounds      atomic.Int64
	deadlocks   atomic.Int64
	planNs      atomic.Int64
	lockNs      atomic.Int64
	probeNs     atomic.Int64
	spendNs     atomic.Int64
	createNs    atomic.Int64
	commitNs    atomic.Int64
	committed   atomic.Int64
	bodyBytes   atomic.Int64
	waitingNs   atomic.Int64
	nettedEdges atomic.Int64
}

// multiFamilyDone is a test hook called with the statistics of each family write when it ends. nil
// in production.
var multiFamilyDone func(*familyStats)

// writeFamilies is the write above the checkpoint. See the comment at the top of this file.
func (w *nettedWrite) writeFamilies(ctx context.Context) ([]utxo.SpendAndCreateMultiResult, error) {
	start := time.Now()

	sizes := make([]int, len(w.items))
	for i, it := range w.items {
		sizes[i] = it.tx.Size()
	}

	plan := planFamilyChunks(w.list.ParentsInList, sizes, multiCreateChunkTxs, spendAndCreateBatchByteBudget)

	stats := &familyStats{chunks: len(plan.chunks), families: plan.families, largest: plan.largest, cutEdges: plan.cutEdges}

	defer func() {
		w.logFamiliesIfSlow(start, stats)

		if multiFamilyDone != nil {
			multiFamilyDone(stats)
		}
	}()

	// The partitions come before any transaction is opened: the DDL needs its own pool
	// connection, and taking one while holding a transaction from the same pool can deadlock it.
	if err := w.s.ensureSpendJournalPartition(ctx, w.blockHeight); err != nil {
		return nil, err
	}

	if err := w.s.ensureTxBodyPartition(ctx, w.blockHeight); err != nil {
		return nil, err
	}

	if mi, mined := minedBlock(w.list.Options.MinedBlockInfos); mined {
		if err := w.s.ensureTxMinedPartition(ctx, mi.BlockHeight); err != nil {
			return nil, err
		}
	}

	if err := w.runChunks(ctx, plan, stats); err != nil {
		return nil, err
	}

	return w.results(), nil
}

// runChunks runs the chunks of a plan, a chunk only once every chunk it depends on has committed,
// on up to multiConcurrency workers.
//
// A chunk is handed to a worker only when it is ready. A worker is never parked waiting for a
// parent: parked, it would hold one of the few workers while a runnable chunk further down the
// list waited for a slot, and a family whose chunks are mostly independent would run nearly one
// chunk at a time. Of the chunks that are ready, the one with the longest chain of chunks still
// waiting behind it goes first, so a long dependency chain is never held up by independent chunks
// that could have run beside it.
//
// The chunks share the caller's context, not one a failure cancels: a sibling cancelled
// mid-commit is the case multiCommitTimeout exists for, and the chunks are independent, each safe
// to commit on its own. After a failure no chunk is started, and the chunks that depended on the
// failed one are skipped, so the write returns that first error.
func (w *nettedWrite) runChunks(ctx context.Context, plan *chunkPlan, stats *familyStats) error {
	n := len(plan.chunks)
	if n == 0 {
		return nil
	}

	pending := make([]int, n)

	for _, ch := range plan.chunks {
		pending[ch.idx] = len(ch.deps)
	}

	rank, dependants := plan.ranks()

	var (
		mu        sync.Mutex
		cond      = sync.NewCond(&mu)
		wg        sync.WaitGroup
		remaining = n
		skipped   = make([]bool, n)
		readyAt   = make([]time.Time, n)
		ready     []int
		firstErr  error
	)

	push := func(idx int) {
		readyAt[idx] = time.Now()
		ready = append(ready, idx)
	}

	// pop takes the ready chunk with the longest chain behind it, the lower index on a tie.
	pop := func() int {
		best := 0

		for i, idx := range ready {
			if r, b := rank[idx], rank[ready[best]]; r > b || (r == b && idx < ready[best]) {
				best = i
			}
		}

		idx := ready[best]
		ready[best] = ready[len(ready)-1]
		ready = ready[:len(ready)-1]

		return idx
	}

	for idx, p := range pending {
		if p == 0 {
			push(idx)
		}
	}

	// finish settles one chunk and releases the chunks that were waiting for it. The caller
	// holds mu.
	finish := func(idx int, failed bool) {
		remaining--

		for _, c := range dependants[idx] {
			skipped[c] = skipped[c] || failed

			if pending[c]--; pending[c] == 0 {
				push(c)
			}
		}

		cond.Broadcast()
	}

	for i := 0; i < w.s.multiConcurrency(); i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			mu.Lock()
			defer mu.Unlock()

			for {
				for len(ready) == 0 && remaining > 0 {
					cond.Wait()
				}

				if remaining == 0 {
					return
				}

				idx := pop()

				if skipped[idx] || firstErr != nil {
					finish(idx, true)
					continue
				}

				stats.waitingNs.Add(int64(time.Since(readyAt[idx])))

				mu.Unlock()
				err := w.runFamilyChunk(ctx, plan.chunks[idx], plan, stats)
				mu.Lock()

				if err != nil && firstErr == nil {
					firstErr = err
				}

				finish(idx, err != nil)
			}
		}()
	}

	wg.Wait()

	return firstErr
}

// runFamilyChunk writes one chunk, running its database transaction again when PostgreSQL stops
// it as a deadlock with another chunk or another writer. A deadlock never fails the list: the
// transaction rolled back whole, so the rerun starts from nothing.
func (w *nettedWrite) runFamilyChunk(ctx context.Context, ch *familyChunk, plan *chunkPlan, stats *familyStats) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	its := make([]*multiTx, len(ch.pos))
	for k, i := range ch.pos {
		its[k] = w.items[i]
	}

	// A transaction whose parent in an EARLIER chunk failed cannot be written. The chunks
	// holding those parents have finished, so their results are final.
	parentDead := make([]bool, len(its))

	for k, it := range its {
		for _, p := range it.parents {
			if plan.chunkOf[p] != ch.idx && w.items[p].dead() {
				parentDead[k] = true
				break
			}
		}
	}

	return withDeadlockRetry(ctx, "multi_chunk", func() error {
		err := w.familyAttempt(ctx, ch, its, parentDead, stats)
		if isDeadlock(err) {
			stats.deadlocks.Add(1)
		}

		return err
	})
}

// chunkRun is the state of one chunk across its rounds.
type chunkRun struct {
	ch  *familyChunk
	its []*multiTx
	pos map[int]int // list position -> index in its
	st  []chunkStatus
	res []utxo.SpendAndCreateMultiResult
	// spendFailed[k] is true once a round found a spend of its transaction refused: it stays
	// failed for the rest of this attempt, and a later round does not spend for it.
	spendFailed []bool
	// forcedExisted[k] is true once the create claim reported the transaction as stored, or a
	// refused spend turned out to be its own earlier spend.
	forcedExisted []bool
	parentDead    []bool
	// netted[k][vin] is true when input vin's parent is a live transaction of this same chunk.
	netted [][]bool
}

// familyAttempt writes one chunk in database transactions, one per round. A round that finds a
// transaction's spend refused rolls back whole, drops that transaction and its descendants in the
// chunk, and runs again: only a whole rollback lets a competing spend that waited on our delete
// see the row again, for the reason runSpendAndCreateBatch gives. The rounds end because each
// extra one removes a transaction. The common round is the only one, and commits.
func (w *nettedWrite) familyAttempt(ctx context.Context, ch *familyChunk, its []*multiTx, parentDead []bool,
	stats *familyStats) error {
	run := &chunkRun{
		ch: ch, its: its, pos: make(map[int]int, len(its)),
		st: make([]chunkStatus, len(its)), res: make([]utxo.SpendAndCreateMultiResult, len(its)),
		spendFailed: make([]bool, len(its)), forcedExisted: make([]bool, len(its)),
		parentDead: parentDead, netted: make([][]bool, len(its)),
	}

	for k, it := range its {
		run.pos[it.pos] = k
	}

	for {
		stats.rounds.Add(1)

		again, err := w.familyRound(ctx, run, stats)
		if err != nil {
			return err
		}

		if !again {
			return nil
		}
	}
}

// familyRound is one database transaction of a chunk. It returns true when it rolled back to run
// again with a smaller or better-informed set, and false once the chunk is settled.
//
// The plan of the records to write is built after the spends, not before: a spend decorates its
// input with the coin's value and script, and the record's fee and body are built from the
// decorated transaction, as the per-transaction write builds them.
func (w *nettedWrite) familyRound(ctx context.Context, run *chunkRun, stats *familyStats) (again bool, err error) {
	its := run.its

	for k, it := range its {
		it.restoreClaims()

		switch {
		case run.spendFailed[k]:
			run.st[k] = chunkFailed
		case run.parentDead[k]:
			it.spends = make([]*utxo.Spend, len(it.tx.Inputs))
			run.st[k] = chunkParentFailed
			run.res[k] = w.parentFailedResult(it)
		default:
			it.spends = make([]*utxo.Spend, len(it.tx.Inputs))
			run.st[k] = chunkLive
			run.res[k] = utxo.SpendAndCreateMultiResult{}
		}

		run.netted[k] = nil
	}

	// A chunk with nothing live, every transaction of it parent-failed, opens no transaction.
	if len(run.liveSet()) == 0 {
		w.settleChunk(run, nil, nil)

		return false, nil
	}

	dbTx, err := w.s.pool.Begin(ctx)
	if err != nil {
		return false, errors.NewStorageError("[utxoset][SpendAndCreateMulti] begin chunk", err)
	}

	committed := false

	defer func() {
		if !committed {
			_ = dbTx.Rollback(ctx)
		}
	}()

	t0 := time.Now()

	if err = w.s.lockTxids(ctx, dbTx, run.lockIDs(run.liveSet())); err != nil {
		return false, err
	}

	t1 := time.Now()
	stats.lockNs.Add(int64(t1.Sub(t0)))

	// A transaction an earlier round found stored is marked existing before anything is netted:
	// an output of one that exists is a real coin and its child must spend it for real.
	w.markExisting(run)

	if err = chunkFault(run.ch.idx, "locked"); err != nil {
		return false, err
	}

	t2 := time.Now()

	w.nettedInChunk(run)
	w.failChunkDescendants(run)

	failed, err := w.spendOutsideInChunk(ctx, dbTx, run)
	if err != nil {
		return false, err
	}

	stats.spendNs.Add(int64(time.Since(t2)))

	if failed {
		// Nothing of this round is kept. The next one leaves out what failed, and treats as
		// existing what the store turns out to hold.
		tProbe := time.Now()

		if err = w.reprobeRefused(ctx, dbTx, run); err != nil {
			return false, err
		}

		stats.probeNs.Add(int64(time.Since(tProbe)))

		return true, nil
	}

	if err = chunkFault(run.ch.idx, "spent"); err != nil {
		return false, err
	}

	t3 := time.Now()

	plan, planned, retry, err := w.createInChunk(ctx, dbTx, run, stats)
	if err != nil {
		return false, err
	}

	if retry {
		return true, nil
	}

	stats.createNs.Add(int64(time.Since(t3)))

	if err = chunkFault(run.ch.idx, "created"); err != nil {
		return false, err
	}

	if live := run.liveSet(); len(live) > 0 {
		if multiCreateBeforeCommit != nil {
			multiCreateBeforeCommit(ctx, run.txidsOf(live))
		}

		t4 := time.Now()

		// The commit runs on a context the caller cannot cancel; see multiCommitTimeout.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), multiCommitTimeout)
		err = dbTx.Commit(commitCtx)

		cancel()

		if err != nil {
			return false, errors.NewStorageError("[utxoset][SpendAndCreateMulti] commit chunk", err)
		}

		committed = true

		stats.commitNs.Add(int64(time.Since(t4)))
		stats.committed.Add(1)

		if multiCreateAfterCommit != nil {
			if err = multiCreateAfterCommit(); err != nil {
				return false, err
			}
		}

		if multiCreateCommitted != nil {
			multiCreateCommitted(len(live))
		}
	}

	w.settleChunk(run, planned, plan)

	return false, chunkFault(run.ch.idx, "committed")
}

// liveSet is the indexes of the chunk's transactions still to be written.
func (r *chunkRun) liveSet() []int {
	var out []int

	for k := range r.its {
		if r.st[k] == chunkLive {
			out = append(out, k)
		}
	}

	return out
}

// lockIDs is the lock keys, the txids as bytes, of the given transactions of the chunk.
func (r *chunkRun) lockIDs(ks []int) [][]byte {
	out := make([][]byte, len(ks))
	for i, k := range ks {
		out[i] = r.its[k].txid[:]
	}

	return out
}

// txidsOf is the txids of the given transactions of the chunk.
func (r *chunkRun) txidsOf(ks []int) []chainhash.Hash {
	out := make([]chainhash.Hash, len(ks))
	for i, k := range ks {
		out[i] = r.its[k].txid
	}

	return out
}

// parentFailedResult is the result of a transaction whose parent in the list failed.
func (w *nettedWrite) parentFailedResult(it *multiTx) utxo.SpendAndCreateMultiResult {
	return utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxParentFailed, Spends: it.spends,
		Err: errors.NewProcessingError("SpendAndCreateMulti: a parent of %s earlier in the list failed", it.txid.String())}
}

// probeExisting reads which of the given transactions of the chunk the store already holds,
// under their advisory locks.
func (w *nettedWrite) probeExisting(ctx context.Context, dbTx pgx.Tx, run *chunkRun, ks []int) (map[chainhash.Hash]struct{}, error) {
	leaves := make([]int16, len(ks))
	txids := make([][]byte, len(ks))
	lo := make([][16]byte, len(ks))
	hi := make([][16]byte, len(ks))

	for i, k := range ks {
		id := &run.its[k].txid
		leaves[i], txids[i] = LeafFor(id[:]), id[:]
		lo[i], hi[i] = Pack(id[:], 0), Pack(id[:], ^uint32(0))
	}

	out, err := dbTx.Query(ctx, existingTxidsSQL, leaves, txids, lo, hi, w.s.claimFloor())
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SpendAndCreateMulti] read existing", err)
	}

	held := make(map[chainhash.Hash]struct{})

	for out.Next() {
		var id []byte
		if err := out.Scan(&id); err != nil {
			out.Close()
			return nil, errors.NewStorageError("[utxoset][SpendAndCreateMulti] scan existing", err)
		}

		held[chainhash.Hash(id)] = struct{}{}
	}

	out.Close()

	if err := out.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][SpendAndCreateMulti] existing rows", err)
	}

	return held, nil
}

// markExisting marks the live transactions of the chunk that an earlier round found stored, by
// the create claim or by reprobeRefused, as existed.
func (w *nettedWrite) markExisting(run *chunkRun) {
	for _, k := range run.liveSet() {
		if run.forcedExisted[k] {
			run.st[k] = chunkExisted
		}
	}
}

// reprobeRefused is the check on a refused spend: a transaction whose spend was refused because
// it already exists, its coins long since spent by itself, is marked existing and no longer
// failed, for the next round. It reads only the refused transactions, so a chunk with none pays
// no existence read.
func (w *nettedWrite) reprobeRefused(ctx context.Context, dbTx pgx.Tx, run *chunkRun) error {
	var refused []int

	for k := range run.its {
		if run.spendFailed[k] {
			refused = append(refused, k)
		}
	}

	if len(refused) == 0 {
		return nil
	}

	held, err := w.probeExisting(ctx, dbTx, run, refused)
	if err != nil {
		return err
	}

	for _, k := range refused {
		if _, ok := held[run.its[k].txid]; ok {
			run.spendFailed[k] = false
			run.forcedExisted[k] = true
		}
	}

	return nil
}

// nettedInChunk decides, for every live transaction, which inputs are netted: those whose parent
// is a live transaction of the same chunk. It then does in memory what the spend statement does
// for an input whose parent is in the chunk: the output must be one the create stores as a UTXO,
// and the input's claimed value and script must match it. A parent created by this list carries
// no flags and no maturity, because canNet admits no flagged create and a coinbase is never in a
// list. The input is then decorated, as the statement's RETURNING decorates a spent input. A
// parent that is not live in this chunk, existing or failed or in another chunk, leaves the
// input outside, for the spend statement.
func (w *nettedWrite) nettedInChunk(run *chunkRun) {
	genesis := w.s.settings.ChainCfgParams.GenesisActivationHeight

	for k, it := range run.its {
		if run.st[k] != chunkLive {
			continue
		}

		failures := 0

		var first error

		for vin, in := range it.tx.Inputs {
			pp, inList := w.position[*in.PreviousTxIDChainHash()]
			if !inList {
				continue
			}

			pk, inChunk := run.pos[pp]
			if !inChunk || run.st[pk] != chunkLive {
				continue
			}

			if run.netted[k] == nil {
				run.netted[k] = make([]bool, len(it.tx.Inputs))
			}

			run.netted[k][vin] = true

			parent := run.its[pk]
			rec := &utxo.Spend{TxID: &parent.txid, Vout: in.PreviousTxOutIndex,
				SpendingData: &spendpkg.SpendingData{TxID: &it.txid, Vin: vin}}
			it.spends[vin] = rec

			out := parent.tx.Outputs[in.PreviousTxOutIndex]
			if out == nil || out.LockingScript == nil || !utxo.ShouldStoreOutputAsUTXO(out, w.blockHeight, genesis) {
				rec.Err = errors.NewTxNotFoundError("[utxoset][SpendAndCreateMulti] output %d of %s is not a spendable output", in.PreviousTxOutIndex, parent.txid.String())
			} else if err := claimMismatch(in, rec, int64(out.Satoshis), *out.LockingScript, nil, w.list.Options.IgnoreFlags.SkipUTXOHashCheck); err != nil { //nolint:gosec // satoshis fit int64
				rec.Err = err
			} else {
				decorateInput(in, int64(out.Satoshis), *out.LockingScript, nil) //nolint:gosec // satoshis fit int64
			}

			if rec.Err != nil {
				failures++

				if first == nil {
					first = rec.Err
				}
			}
		}

		if failures > 0 {
			run.st[k] = chunkFailed
			run.res[k] = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxFailed, Spends: it.spends,
				Err: errors.NewUtxoError("[utxoset][SpendAndCreate] %d of %d inputs could not be spent", failures, len(it.spends), first)}
		}
	}
}

// failChunkDescendants marks every live transaction with a failed parent in the chunk as
// ParentFailed, in list order, so a whole subtree goes with its root.
func (w *nettedWrite) failChunkDescendants(run *chunkRun) {
	for k, it := range run.its {
		if run.st[k] != chunkLive {
			continue
		}

		for _, p := range it.parents {
			pk, inChunk := run.pos[p]
			if !inChunk {
				continue
			}

			if s := run.st[pk]; s == chunkFailed || s == chunkParentFailed {
				run.st[k] = chunkParentFailed
				run.res[k] = w.parentFailedResult(it)

				break
			}
		}
	}
}

// spendOutsideInChunk deletes the outside coins of every live transaction and journals them, in
// the chunk's database transaction. It returns true when a spend was refused: that transaction is
// marked to stay failed, and the caller rolls the round back and runs again without it.
func (w *nettedWrite) spendOutsideInChunk(ctx context.Context, dbTx pgx.Tx, run *chunkRun) (bool, error) {
	var (
		idx   []int
		items []*spendItem
	)

	for k, it := range run.its {
		if run.st[k] != chunkLive {
			continue
		}

		idx = append(idx, k)
		items = append(items, &spendItem{tx: it.tx, blockHeight: w.blockHeight,
			ignoreFlags: w.list.Options.IgnoreFlags, skip: run.netted[k]})
	}

	plan := planSpends(items)

	if err := w.s.runSpendPlan(ctx, dbTx, plan); err != nil {
		return false, err
	}

	failed := false

	for i, k := range idx {
		it := run.its[k]

		var (
			failures int
			first    error
		)

		for vin, sp := range plan.perItem[i] {
			if run.netted[k] != nil && run.netted[k][vin] {
				continue
			}

			it.spends[vin] = sp

			if sp != nil && sp.Err != nil {
				failures++

				if first == nil {
					first = sp.Err
				}
			}
		}

		if failures == 0 {
			continue
		}

		failed = true
		run.spendFailed[k] = true
		run.st[k] = chunkFailed
		run.res[k] = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxFailed, Spends: it.spends,
			Err: errors.NewUtxoError("[utxoset][SpendAndCreate] %d of %d inputs could not be spent", failures, len(it.spends), first)}
	}

	return failed, nil
}

// createInChunk writes the live transactions: their records, bodies and every output not spent
// inside the chunk as a UTXO, and the spend-journal row of every output that is. It returns the
// plan it wrote and which chunk transactions its items are, and true when the create claim found
// a transaction already stored, which makes the caller roll back and run the round again with
// that transaction treated as existing.
func (w *nettedWrite) createInChunk(ctx context.Context, dbTx pgx.Tx, run *chunkRun, stats *familyStats) (*createPlan, []int, bool, error) {
	planned := run.liveSet()
	if len(planned) == 0 {
		return nil, nil, false, nil
	}

	tPlan := time.Now()

	items := make([]*createItem, len(planned))
	for i, k := range planned {
		it := run.its[k]
		items[i] = &createItem{tx: it.tx, blockHeight: w.blockHeight, options: w.list.Options.ItemOptions(it.pos)}
	}

	plan := w.s.planCreates(items)

	for i := range items {
		if plan.errs[i] != nil {
			return nil, nil, false, plan.errs[i]
		}
	}

	for _, b := range plan.bodies {
		stats.bodyBytes.Add(int64(len(b)))
	}

	stats.planNs.Add(int64(time.Since(tPlan)))

	spender := make(map[nettedKey]nettedSpender)

	for _, k := range planned {
		if run.netted[k] == nil {
			continue
		}

		it := run.its[k]

		for vin, in := range it.tx.Inputs {
			if !run.netted[k][vin] {
				continue
			}

			parent := in.PreviousTxIDChainHash()
			spender[newNettedKey(parent[:], Pack(parent[:], in.PreviousTxOutIndex))] =
				nettedSpender{txid: it.txid[:], vin: int32(vin)} //nolint:gosec // an input index fits int32
		}
	}

	journal := plan.takeNetted(spender)

	// Every netted spend must have found the output it spends among the plan's UTXOs. A
	// shortfall would be a netted input with no journal row, a spend that left no trace.
	if len(journal.ukeys) != len(spender) {
		return nil, nil, false, errors.NewProcessingError("[utxoset][SpendAndCreateMulti] %d netted spends of a chunk found %d outputs to journal", len(spender), len(journal.ukeys))
	}

	stats.nettedEdges.Add(int64(len(spender)))

	if err := w.s.runCreatePlan(ctx, dbTx, plan); err != nil {
		return nil, nil, false, err
	}

	retry := false

	for i, k := range planned {
		switch err := plan.errs[i]; {
		case err == nil:
		case errors.Is(err, errors.ErrTxExists):
			run.forcedExisted[k] = true
			retry = true
		default:
			return nil, nil, false, err
		}
	}

	if retry {
		return nil, nil, true, nil
	}

	return plan, planned, false, w.writeNettedJournal(ctx, dbTx, journal)
}

// settleChunk gives every transaction of a settled chunk its result. It runs after the commit,
// so a rolled-back chunk leaves none behind.
func (w *nettedWrite) settleChunk(run *chunkRun, planned []int, plan *createPlan) {
	at := make(map[int]int, len(planned))
	for i, k := range planned {
		at[k] = i
	}

	for k, it := range run.its {
		switch run.st[k] {
		case chunkLive:
			it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxCreated, Meta: plan.perItem[at[k]], Spends: it.spends}
		case chunkExisted:
			it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxExisted, Spends: existingSpends(it)}
		default:
			it.result = run.res[k]
		}
	}
}

// logFamiliesIfSlow writes one line for a list that took longer than multiSlowListLog, saying how
// big and how cut it was and where the time went. The chunk timings are summed across parallel
// chunks.
func (w *nettedWrite) logFamiliesIfSlow(start time.Time, st *familyStats) {
	if time.Since(start) < multiSlowListLog {
		return
	}

	w.s.logger.Infof("[utxoset][SpendAndCreateMulti] slow list at height %d: %d txs in %d families (largest %d), %d chunks, %d cut edges, %d rounds, %d deadlock retries, %.1f MB of bodies; wall %s (plan %s, lock %s, probe %s, spend %s, create %s, commit %s, queued for a worker %s)",
		w.blockHeight, len(w.items), st.families, st.largest, st.chunks, st.cutEdges, st.rounds.Load(), st.deadlocks.Load(),
		float64(st.bodyBytes.Load())/1e6, time.Since(start).Round(time.Millisecond),
		time.Duration(st.planNs.Load()).Round(time.Millisecond),
		time.Duration(st.lockNs.Load()).Round(time.Millisecond), time.Duration(st.probeNs.Load()).Round(time.Millisecond),
		time.Duration(st.spendNs.Load()).Round(time.Millisecond), time.Duration(st.createNs.Load()).Round(time.Millisecond),
		time.Duration(st.commitNs.Load()).Round(time.Millisecond), time.Duration(st.waitingNs.Load()).Round(time.Millisecond))
}

// existingSpends describes the spends of a transaction that already existed: each input spent by
// it, which is what its record says. The write made none of them and checked none, because the
// transaction's own writer spent them with its record.
func existingSpends(it *multiTx) []*utxo.Spend {
	out := make([]*utxo.Spend, len(it.tx.Inputs))

	for vin, in := range it.tx.Inputs {
		out[vin] = &utxo.Spend{TxID: in.PreviousTxIDChainHash(), Vout: in.PreviousTxOutIndex,
			SpendingData: &spendpkg.SpendingData{TxID: &it.txid, Vin: vin}}
	}

	return out
}
