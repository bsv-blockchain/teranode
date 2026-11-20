package bridge

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
)

// orphanValidateFunc validates one transaction at block height 0 — the same
// default IngestTx itself uses (ingest_tx.go, "passing in block height 0,
// which will default to utxo store block height in validator"). It is the
// shape orphanPool needs both to test a released orphan's promotion and to
// give an evicted orphan its final validation attempt (G2).
type orphanValidateFunc func(ctx context.Context, tx *bt.Tx) (*meta.Data, error)

// orphanEntry is the pool's stored value: legacy's own orphanTxAndParents
// (services/legacy/netsync/manager.go:277), relocated. parents is a plain
// map rather than legacy's txmap.SyncedMap: legacy's own comment says the
// synced map is "for faster lookups", but an entry's parent set is built
// once at insertion (add, below) and never mutated afterwards, and the pool
// as a whole already serializes access to any one entry through
// expiringmap's own mutex — there is no concurrent writer here the synced
// map would need to protect against.
//
// No addedAt field: legacy's own two readers of that timestamp
// (prometheusLegacyNetsyncOrphanTime, manager.go:1358-1359, and the
// orphan-count gauge goroutine at :3239+) are metrics, and neither is ported
// in this task — expiringmap's own itemWrapper already tracks insertion
// order internally for eviction purposes, so this type does not need its
// own copy of it for anything release or add actually do.
type orphanEntry struct {
	tx      *bt.Tx
	parents map[chainhash.Hash]struct{}

	// addedAt feeds orphan_time when the entry is released.
	addedAt time.Time
}

// ReleasedOrphan is one orphan the release walk promoted to accepted, in
// the shape the caller needs to feed Task 13's tx announcement seam
// (txAnnouncer.put in services/svp2p/txrelay.go) the same way it feeds the
// tx whose acceptance triggered the walk.
type ReleasedOrphan struct {
	TxHash chainhash.Hash
	Fee    uint64
	Size   uint64
}

// orphanEvictionQueueSize bounds the hand-off from an eviction (TTL sweep or
// cap trigger) to the pool's own final-validation worker (see onEvict and
// runEvictionWorker below, fix round 1 Issue I1). It does not bound the pool
// itself — legacy_maxOrphanTxs already does that — it only bounds how many
// evicted-but-not-yet-finally-validated entries can be in flight at once. A
// full queue means the hand-off is dropped, not blocked: see onEvict's own
// doc comment for why that is an acceptable, disclosed trade rather than a
// silent one.
//
// 256 is sized against the default cap, not independent of it: at
// legacy_maxOrphanTxs's default of 100, the largest possible single-sweep
// burst is 100 (every resident entry expiring in the same TTL window), so
// 256 has headroom for that whole burst plus a concurrent cap eviction or
// two without dropping anything — at the default cap, no attempt is ever
// dropped. It is fixed rather than scaled to legacy_maxOrphanTxs because a
// dropped final-validation attempt is an accepted best-effort loss either
// way (onEvict's own doc comment); this size is chosen so the default
// configuration never pays that cost, not to guarantee zero drops at every
// possible cap setting. See orphanPool's own doc comment for what this
// means for the disabled-cap case, where the queue is a net improvement
// over fix round 1's own starting point, not merely a new bound.
const orphanEvictionQueueSize = 256

// orphanPool is the relocated netsync orphan pool (spec §6, "SVNode
// semantics with the existing eviction settings"). SVNode's own COrphanTxns
// (src/orphan_txns.h in the reference checkout) keeps both a flat,
// hash-keyed map (mOrphanTxns, :128) and a secondary index by the parent
// outpoints an orphan waits on (mOrphanTxnsByPrev, :129) — so the real
// structural difference from this port is that secondary index, not the
// presence or absence of a flat map. This port keeps legacy's own
// expiringmap + orphanTxAndParents shape instead (single flat map, no
// secondary index — services/legacy/netsync/manager.go:277, :458, :3165),
// because that is the hardened, in-production behaviour this plan relocates
// (spec §1: "keeps the hardened Teranode-side ingestion pipeline"), not a
// fresh implementation against the C++ reference.
//
// Sizing and TTL come from the two settings G1 confirms are already wired:
// legacy_maxOrphanTxs (default 100, settings/legacy_settings.go:14) and
// legacy_orphanEvictionDuration (default 10m, :13), both read in
// settings/settings.go:662-663. Per-instance cost at the default cap is NOT
// just the resident pool: it is the resident entries (at most 100) PLUS
// whatever fix round 1's eviction-hand-off queue is holding (at most
// orphanEvictionQueueSize, 256 — see that const's own doc comment for why
// 256 against a cap of 100), because a queued entry is a full decoded
// transaction the pool has already evicted but not yet finally validated.
// Worst case at the default cap is therefore 100 + 256 = 356 resident
// transactions, each with a hash-keyed parent set sized to its input
// count — still in the low hundreds of KB to low single-digit MB total for
// ordinary transactions, not the "100 entries" the cap alone would suggest.
// legacy_maxOrphanTxs = 0 disables the cap (expiringmap.WithMaxSize's own
// "a value of 0 ... disables the cap" contract,
// util/expiringmap/expiringmap.go): the operator has explicitly asked for
// legacy's original unbounded behaviour, "not recommended on public
// networks" (the setting's own longdesc). Disabling the cap removes two
// independent bounds, not one: the memory bound the longdesc names
// directly, and the stack-depth bound release's own doc comment below
// covers — and release's per-pop Items() call (a full map copy,
// expiringmap.go's own Items) makes the walk's cost O(n^2) in the pool's
// size, so a disabled cap also turns that quadratic term peer-controlled.
// Legacy shares the identical O(n^2) shape (processOrphanTransactions'
// own Items() snapshot per recursive call), so this is fidelity-preserving,
// not a regression this port introduced — it is named here because the
// disabled-cap configuration is where it stops being negligible. One thing
// the disabled cap does NOT make worse, because of fix round 1's own I1
// fix: a TTL sweep at legacy_maxOrphanTxs = 0 now hands off at most
// orphanEvictionQueueSize (256) expired entries to the worker and drops
// the rest (counted, not silent — see onEvict), rather than validating
// every expired entry serially under expiringmap's write lock the way this
// port did before I1. The disabled cap still costs memory and quadratic
// time unboundedly; it no longer also costs an unbounded whole-node
// tx-ingest stall during a TTL sweep.
//
// Owned once per bridge instance, not per peer (mirroring legacy's own
// single SyncManager-wide sm.orphanTxs): the aggregate cost across a
// running node is this single pool's cost, not multiplied by peer count.
//
// Two background goroutines are owned by one pool instance: expiringmap's
// own TTL ticker (started inside expiringmap.New, stopped by stop() below
// calling p.m.Stop()) and this pool's eviction-validation worker
// (runEvictionWorker, started here, stopped by the same stop() cancelling
// ctx and joining wg). Both are release-round-1 fixes: the ticker's
// lifecycle was previously unowned (Issue I4) and the eviction path used to
// validate synchronously under expiringmap's own write lock (Issue I1).
type orphanPool struct {
	m        *expiringmap.ExpiringMap[chainhash.Hash, *orphanEntry]
	validate orphanValidateFunc
	logger   ulogger.Logger

	// ctx/cancel give the eviction worker's validate calls a cancellable
	// context tied to the pool's own lifetime — legacy's own equivalent is
	// sm.ctx, passed to its eviction closure's Validate call
	// (manager.go:3230). Fix round 1, Issue I4: this port previously passed
	// context.Background() there, an uncancellable regression against
	// legacy that let post-shutdown validate calls run forever. This ctx is
	// cancelled once, by stop(), and never derived from any per-request
	// context — an evicted orphan's final attempt is not any one peer's
	// request, the same way legacy's sm.ctx is not.
	ctx    context.Context
	cancel context.CancelFunc

	// evictions is the non-blocking hand-off from onEvict (running under
	// expiringmap's own write lock) to runEvictionWorker (running on its
	// own goroutine, outside any lock this pool or expiringmap holds). Fix
	// round 1, Issue I1.
	evictions chan *orphanEntry

	// dropped counts final-validation attempts lost because evictions was
	// full when onEvict tried to hand one off. Best-effort, not correctness
	// (see onEvict's own doc comment): read only for logging/diagnostics,
	// never for control flow.
	dropped atomic.Uint64

	// wg is released once runEvictionWorker returns, so stop() can join it
	// before reporting the pool fully quiesced.
	wg sync.WaitGroup
}

// newOrphanPool builds the pool from the two wired settings, with the
// eviction function that gives every evicted entry its final validation
// attempt (G2). One function covers both the TTL sweep and a
// cap-triggered eviction: expiringmap's own evictOldestLocked (the cap
// path, called from Set) and clean() (the TTL path, called from its own
// background ticker) both call the identical evictionFn
// (util/expiringmap/expiringmap.go). "Mirroring TTL-based eviction
// semantics" therefore falls out of using one eviction function for both,
// rather than needing to be implemented twice.
//
// The returned pool owns two background goroutines (expiringmap's TTL
// ticker and this pool's own eviction worker) that only stop() releases —
// see stop's own doc comment and the type's.
func newOrphanPool(tSettings *settings.Settings, logger ulogger.Logger, validate orphanValidateFunc) *orphanPool {
	// Idempotent (prometheusMetricsInitOnce): New (bridge.go) already calls
	// this, but newOrphanPool is also called directly by this package's own
	// tests, which never go through New, and onEvict's drop path (fix round
	// 2, Minor 2) needs prometheusSvp2pBridgeOrphanEvictionQueueDrops
	// registered before it can ever fire.
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())

	p := &orphanPool{
		validate:  validate,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
		evictions: make(chan *orphanEntry, orphanEvictionQueueSize),
	}

	p.m = expiringmap.New[chainhash.Hash, *orphanEntry](tSettings.Legacy.OrphanEvictionDuration).
		WithMaxSize(tSettings.Legacy.MaxOrphanTxs).
		WithEvictionFunction(p.onEvict)

	p.wg.Add(1)

	go p.runEvictionWorker()

	return p
}

// stop releases both background goroutines a pool owns (fix round 1, Issue
// I4): expiringmap's own TTL ticker, and this pool's eviction-validation
// worker. Cancelling ctx first asks any final-validation call already in
// flight inside runEvictionWorker to abandon rather than run to completion
// — but whether it actually does depends on the injected validate func
// honoring ctx cancellation, which is not guaranteed for every caller:
// validator.Client.Validate propagates ctx into its gRPC call directly in
// non-batched mode, but its batched mode (services/validator/Client.go —
// the base settings.conf context sets validator_sendBatchSize to 0
// (non-batched); only the operator and docker.m contexts set it to 1000,
// so batched is the default for real/operator deployments, not for every
// deployment) waits on the batch dispatcher with an uncancellable wait —
// completion.Group.Wait is called with context.Background(), not the
// item's own ctx, and go-batcher/v2's own Wait then blocks purely on the
// dispatcher with no timeout arm at all — and that dispatcher's own gRPC
// call underneath carries no deadline either. So wg.Wait() below can
// block, genuinely unbounded, until an in-flight batched validate
// naturally completes rather than being cut short — stop is not
// guaranteed to return promptly, only to return once no validate call
// from this pool is still running. Also idempotent: p.m.Stop is itself
// idempotent, cancelling an already-cancelled CancelFunc is a no-op, and
// a second wg.Wait simply returns immediately — though callers only need
// to call this once, and svp2pBridge.Stop does, at most once per bridge
// lifetime, the same contract Admission.Stop already has in this package.
func (p *orphanPool) stop() {
	p.cancel()
	p.m.Stop()
	p.wg.Wait()
}

// runEvictionWorker drains evictions and performs each entry's final
// validation attempt off expiringmap's own lock (fix round 1, Issue I1).
// It is the sole reader of evictions and the sole caller of finalValidate,
// so p.validate is never invoked concurrently with itself from this path —
// only add's and release's own, separate, synchronous validate calls run
// concurrently with it, which is unchanged from before this fix and is
// exactly what a peer-concurrent IngestTx already implies.
func (p *orphanPool) runEvictionWorker() {
	defer p.wg.Done()

	for {
		select {
		case entry, ok := <-p.evictions:
			if !ok {
				return
			}

			p.finalValidate(entry)
		case <-p.ctx.Done():
			return
		}
	}
}

// finalValidate performs one evicted orphan's final validation attempt,
// logging the outcome only — exactly as onEvict did before this fix, and
// exactly as legacy's own eviction closure does (manager.go:3226-3236,
// "try to process one last time"). Neither legacy nor this port feeds the
// tx into the announce path or the release walk from here: an orphan that
// resolves on its way out the door does not cascade to its own dependents
// (a known legacy quirk, carried rather than fixed, since fixing it is
// outside this task's brief).
func (p *orphanPool) finalValidate(entry *orphanEntry) {
	hash := *entry.tx.TxIDChainHash()

	if _, err := p.validate(p.ctx, entry.tx); err != nil {
		p.logger.Debugf("[orphanPool] failed to validate orphan transaction %s when evicting: %v", hash, err)
	} else {
		p.logger.Debugf("[orphanPool] evicted orphan transaction %s", hash)
	}
}

// onEvict is expiringmap's eviction callback for both the TTL sweep and a
// cap-triggered eviction — see newOrphanPool's own doc comment for why one
// function covers both. It always returns true: legacy's closure has no
// veto path either, so the cap stays honored unconditionally — a vetoed
// eviction would let a peer pin the cap by flooding just enough inserts to
// keep tripping WithMaxSize's eviction without an entry ever actually
// leaving.
//
// Fix round 1, Issue I1: this used to call p.validate directly, a blocking
// network round trip made while expiringmap holds its own internal write
// mutex (evictOldestLocked, the cap path called from Set; clean(), the TTL
// path called from expiringmap's own background ticker) — and release
// takes that same write lock on every accepted tx, so the block was on the
// whole node's tx-ingest path, not merely on orphan handling. A TTL sweep
// that evicts many entries in one pass validated every one of them
// serially under that lock, which could stall all peers' tx ingestion for
// the sweep's full duration. It also violated expiringmap's own stated
// invariant that eviction never blocks under its mutex
// (expiringmap.go:87-89, about its eviction-channel send — a blocking
// validate in evictionFn broke that guarantee for this consumer just the
// same).
//
// Now onEvict only hands the evicted entry to runEvictionWorker
// (non-blocking, via evictions) and returns immediately; the validate call
// itself happens on that worker's own goroutine, entirely outside
// expiringmap's lock. The trade this makes: the final-validation attempt
// becomes best-effort rather than guaranteed — a full evictions buffer
// drops the attempt rather than blocking the evicting caller — which is
// acceptable because legacy_maxOrphanTxs's own longdesc already frames the
// final attempt as a courtesy ("mirroring TTL-based eviction semantics"),
// not as a correctness requirement anything downstream depends on. Drops
// are counted in p.dropped rather than silent.
//
// Re-entrancy prohibition: onEvict itself must never call anything that
// re-enters this pool (add, release, Get, ...) on the calling goroutine —
// expiringmap already holds its own write lock when this runs, and any
// re-entrant call to that same lock self-deadlocks. Nothing in this pool
// does that today; this is a standing constraint for anything added here
// later, not a description of a bug that exists now.
func (p *orphanPool) onEvict(hash chainhash.Hash, entry *orphanEntry) bool {
	prometheusSvp2pBridgeOrphans.Dec()

	select {
	case p.evictions <- entry:
	default:
		dropped := p.dropped.Add(1)
		prometheusSvp2pBridgeOrphanEvictionQueueDrops.Inc()
		p.logger.Debugf("[orphanPool] final-validation queue full, dropping the attempt for evicted orphan %s (total dropped so far: %d)", hash, dropped)
	}

	return true
}

// add inserts tx into the pool if it is not already present, keyed by hash
// with its inputs' previous-outpoint hashes as its parent set — legacy's
// own duplicate check and parent-map construction
// (netsync/manager.go:1256-1273), ported field for field. A tx already in
// the pool is left untouched: the pool always keeps the version keyed by
// hash that it first saw, matching legacy's own "otherwise add it" branch.
func (p *orphanPool) add(tx *bt.Tx) {
	hash := *tx.TxIDChainHash()

	if _, exists := p.m.Get(hash); exists {
		return
	}

	parents := make(map[chainhash.Hash]struct{}, len(tx.Inputs))
	for _, in := range tx.Inputs {
		parents[*in.PreviousTxIDChainHash()] = struct{}{}
	}

	p.m.Set(hash, &orphanEntry{
		tx:      tx,
		parents: parents,
		addedAt: time.Now(),
	})

	// AN ORPHAN IS DELIBERATELY NOT ADDED TO THE RECENT-TRANSACTION INDEX.
	// It failed validation, so it lives only in this pool's memory and never
	// reaches the UTXO store — and RecentTxIndex.Open reads the store. A hash
	// the index names but cannot serve is strictly worse than one it does not
	// name at all: reconstruction matches it, marks the slot held, and so
	// leaves it OUT of the getblocktxn, then fails at that slot mid-assembly
	// and falls back to fetching the whole block. Left unnamed, the same slot
	// is a gap the getblocktxn fills and the reconstruction succeeds.
	//
	// SVNode can afford the opposite choice because vExtraTxnForCompact holds
	// the transaction BYTES (blockencodings.cpp:194-215), not just the hash, so
	// what it names it can serve. Carrying those bytes here is a possible
	// follow-up; naming what we cannot serve is not.
	prometheusSvp2pBridgeOrphans.Inc()
}

// release runs legacy's processOrphanTransactions
// (netsync/manager.go:1309-1330) as an iterative worklist instead of
// legacy's own recursion, and reports every orphan it promotes to accepted
// for the caller to feed the tx announcement relay the same way it feeds
// the tx that triggered the walk.
//
// G3 finding: legacy recurses once per newly-accepted orphan, and the only
// bound on that recursion's depth is the pool's own size —
// legacy_maxOrphanTxs, 100 by default — because each recursive call scans
// the whole pool (sm.orphanTxs.Items()) for children of the tx it was just
// given, so the recursion depth tracks the longest dependency chain
// currently sitting in the pool. That makes the cap load-bearing for stack
// safety, not merely for memory — worth stating at both sites, so it is
// stated here and on orphanPool's own doc comment above. When
// legacy_maxOrphanTxs is 0 (G2's disabled-cap case) that bound disappears
// entirely: a peer can chain as many orphans as memory allows and then
// submit the root, and legacy's own recursion depth would track the full
// chain length with no cap at all. Porting that recursion verbatim would
// reintroduce a peer-controlled, unbounded call-stack depth on top of the
// memory-exhaustion and quadratic-time risks the disabled cap already
// carries (see orphanPool's own doc comment) — a THIRD, silent hazard the
// operator did not sign up for by setting the cap to 0.
//
// An iterative worklist (a queue on the heap, not the call stack) is
// behaviour-preserving — the same deletions, the same validations, the
// same released set, and the same processing order a queue walks a
// dependency level at a time the same way each level of the recursion did
// — and it removes the stack-depth hazard entirely, independent of
// legacy_maxOrphanTxs. The memory-exhaustion and quadratic-time costs of
// the disabled cap remain and are exactly what the setting's own longdesc,
// and this file's own doc comments, already document as accepted,
// operator-chosen risk; this change does not touch that.
//
// Fix round 1, Issue I2: an orphan is deleted from the pool the moment it
// is promoted (immediately below, before it is appended to released), not
// deferred until it is popped off the queue. This is legacy's own
// semantics exactly — legacy deletes a released orphan at the head of the
// very next recursive call into it (:1318) — and it matters for a
// multi-parent orphan: with the deletion deferred (this port's original
// shape), an orphan with two resident parents both promoted in the same
// walk stayed visible to Items() for whichever parent was processed
// second, so it was validated, released and re-queued once per resident
// parent — always more than once for such an orphan, and always
// successfully both times, since this port's breadth-first order promotes
// an entire level (both parents) before descending into either. Legacy's
// own guarantee is only that it never RELEASES such an orphan more than
// once, not that it never validates it more than once: legacy's
// depth-first order can still hit it once with only one of its two parents
// actually promoted so far (a genuine ErrTxMissingParent miss) and once
// more after the other parent lands (the hit that releases it) — "legacy
// validates it once" is true of the lucky interleaving, not a guarantee.
// Deleting immediately makes this port's orphan disappear from Items()
// before any sibling parent's scan can find it again, so it is validated
// exactly once, unconditionally — strictly cheaper than legacy's own worst
// case, not merely matching legacy's best case — and it makes the
// pop-time p.m.Delete(parent) below a harmless no-op for every non-root
// entry, since by the time an entry is popped it has already been removed
// here.
func (p *orphanPool) release(ctx context.Context, root chainhash.Hash) []ReleasedOrphan {
	start := time.Now()
	defer func() {
		prometheusSvp2pBridgeProcessOrphanTransactions.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	var released []ReleasedOrphan

	queue := []chainhash.Hash{root}

	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]

		// Remove the just-accepted transaction from the pool: it is no
		// longer an orphan. A no-op for the root (never an orphan itself,
		// the same no-op legacy's own top-level delete is on the call from
		// handleTxMsg, netsync/manager.go:1318) and, after the Issue I2 fix
		// below, also a no-op for every non-root entry, since promotion
		// already removed it here.
		p.m.Delete(parent)

		for hash, entry := range p.m.Items() {
			if _, ok := entry.parents[parent]; !ok {
				continue
			}

			txMeta, err := p.validate(ctx, entry.tx)
			if err != nil {
				if errors.Is(err, errors.ErrTxMissingParent) || errors.Is(err, errors.ErrTxLocked) {
					// Still waiting on another parent, or still locked;
					// leave it in the pool (netsync/manager.go:1333-1337).
					continue
				}

				if errors.Is(err, errors.ErrTxConflicting) {
					// Double spend: drop it. Legacy's own closure deletes
					// *txHash here (netsync/manager.go:1341) — the
					// just-processed PARENT, already removed above,
					// making that delete a no-op and leaving the
					// conflicting orphan itself in the pool until its own
					// TTL/cap eviction. Fixed here, as the divergence
					// this task's fidelity rule (G4/G7) requires naming:
					// delete the conflicting entry's own hash, which
					// legacy's version of this loop could not do because
					// it discarded the map key when it built orphanTxs
					// from Items() (`for _, orphanTx := range orphanTxs`).
					p.m.Delete(hash)
					prometheusSvp2pBridgeOrphans.Dec()

					continue
				}

				// Any other rejection: log and leave every descendant of
				// this orphan unprocessed, exactly as legacy does by
				// simply not recursing into it
				// (netsync/manager.go:1344-1348). The orphan itself is
				// also left in the pool, matching legacy: it lingers
				// until its own TTL/cap eviction gives it a final
				// attempt. A second, smaller divergence of the same
				// family as the ErrTxConflicting one above: legacy's own
				// log statement here names txHash (:1346) — the parent,
				// already removed above, the same wrong variable the
				// conflicting branch used — where it means the orphan.
				// This port logs hash, the orphan's own key, which is
				// what the message actually needs.
				p.logger.Errorf("[orphanPool] failed to process orphan transaction %s: %v", hash, err)

				continue
			}

			// Delete before appending: see the Issue I2 doc comment above
			// for why this has to happen here, immediately, rather than
			// being deferred to this hash's own turn at the top of the
			// loop.
			p.m.Delete(hash)
			prometheusSvp2pBridgeOrphans.Dec()
			prometheusSvp2pBridgeOrphanTime.Observe(float64(time.Since(entry.addedAt).Microseconds()) / 1_000_000)

			released = append(released, ReleasedOrphan{
				TxHash: hash,
				Fee:    txMeta.Fee,
				Size:   txMeta.SizeInBytes,
			})

			queue = append(queue, hash)
		}
	}

	return released
}
