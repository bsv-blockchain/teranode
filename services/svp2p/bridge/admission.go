package bridge

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"golang.org/x/sync/semaphore"
)

// Admission is the bridge-boundary gate in front of IngestBlock (spec §6). It
// carries three netsync behaviours that used to live inside the legacy peer
// read-loop and sync manager, with no change of meaning:
//
//   - a byte-weighted prefetch budget, so blocks received ahead of processing
//     are bounded in total memory and an oversized block is admitted alone
//     (services/legacy/netsync/manager.go AcquireBlockPrefetch, PR 1190);
//   - a per-block transient-failure backoff, so a re-delivered block that just
//     failed on a struggling local store is skipped cheaply instead of
//     re-running the full decorate (manager.go recordBlockFailureBackoff and
//     the skip in handleBlockMsg, PR 1192);
//   - a bounded pre-admission phase, so a wedged local round-trip before
//     admission rotates the sync peer rather than parking the read-loop
//     (services/legacy/peer_server.go OnBlock, PR 1281).
//
// It is deliberately NOT a field of svp2pBridge and not part of the Bridge
// interface. Bridge.IngestBlock takes (header, txCount, txReader, peerAddr)
// and knows nothing about the block's byte size — only the transport does,
// from the declared payload length. Admission is therefore a separate exported
// entry that protocol composes around IngestBlock: acquire, ingest, release.
// That keeps the Bridge signature intact and keeps the one honest size input
// (the declared payload length) flowing from the only layer that has it.
//
// Admission owns a background eviction goroutine when the failure backoff is
// enabled, so its owner must call Stop.
type Admission struct {
	logger ulogger.Logger

	// budgetBytes is the configured ceiling; budget is nil when prefetch is
	// disabled (legacy_blockPrefetchBufferBytes = 0), which makes Acquire a
	// no-op and skips dedup — the synchronous one-block-in-flight behaviour.
	budgetBytes int64
	budget      *semaphore.Weighted

	// inFlight is the dedup half of the gate. A hash is inserted before the
	// (possibly blocking) budget acquire and deleted alongside the budget
	// release, so the two halves share exactly one lifetime and cannot drift.
	inFlight   map[chainhash.Hash]struct{}
	inFlightMu sync.Mutex

	// waiters counts callers currently parked on the budget. While it is
	// above zero the node is backpressuring its own reads, which protocol's
	// stall detector must not hold against the delivering peer.
	waiters atomic.Int64

	failureBase   time.Duration
	failureWindow time.Duration
	failures      *expiringmap.ExpiringMap[chainhash.Hash, *blockFailureState]

	preAdmitTimeout time.Duration
}

const (
	// minInFlightBlockWeight floors a block's admission weight so a flood of
	// tiny blocks cannot admit an unbounded number of concurrent ingests
	// within the byte budget. Carried from
	// services/legacy/netsync/manager.go:72 (PR 1190).
	minInFlightBlockWeight = 64 * 1024

	// blockFailureBackoffMaxTracked bounds the failure-tracking map, so only
	// currently-failing block hashes are held. Carried from
	// services/legacy/netsync/manager.go:103 (PR 1192).
	blockFailureBackoffMaxTracked = 1024

	// defaultPreAdmitTimeout is the fallback when legacy_peerProcessingTimeout
	// is unset, so an operator zeroing it cannot reintroduce an unbounded
	// pre-admission phase. Carried from services/legacy/peer_server.go
	// OnBlock (PR 1281).
	defaultPreAdmitTimeout = 3 * time.Minute
)

// ErrDuplicateBlockInFlight is returned by Acquire when a copy of the block
// hash is already admitted or parked waiting for budget. It is benign: the
// caller drops the duplicate without disconnecting the peer, because the first
// copy is still being processed. Carried from
// services/legacy/netsync/manager.go:159 (PR 1190).
var ErrDuplicateBlockInFlight = errors.NewServiceError("duplicate block already in flight")

// blockFailureState is the per-hash failure record behind the linear backoff
// ramp. Carried from services/legacy/netsync/manager.go (PR 1192).
type blockFailureState struct {
	attempts  int
	nextRetry time.Time
}

// NewAdmission builds the gate from the already-wired legacy_* settings. Both
// halves are independently switchable: a zero prefetch budget disables the
// byte gate and the dedup set, and a zero base or max window disables the
// failure backoff (and with it the tracking map and its eviction goroutine).
func NewAdmission(logger ulogger.Logger, tSettings *settings.Settings) *Admission {
	a := &Admission{
		logger:          logger,
		preAdmitTimeout: defaultPreAdmitTimeout,
	}

	if tSettings == nil {
		return a
	}

	if tSettings.Legacy.PeerProcessingTimeout > 0 {
		a.preAdmitTimeout = tSettings.Legacy.PeerProcessingTimeout
	}

	if budget := tSettings.Legacy.BlockPrefetchBufferBytes; budget > 0 {
		a.budgetBytes = budget
		a.budget = semaphore.NewWeighted(budget)
		a.inFlight = make(map[chainhash.Hash]struct{})
	}

	a.failureBase = tSettings.Legacy.BlockFailureBackoffBase
	a.failureWindow = tSettings.Legacy.BlockFailureBackoffMaxDuration
	a.failures = newBlockFailureBackoffMap(a.failureBase, a.failureWindow, tSettings.Legacy.PeerProcessingTimeout)

	return a
}

// newBlockFailureBackoffMap builds the per-block transient-failure map, or
// returns nil when either knob is at or below zero. The map TTL is
// deliberately decoupled from the backoff cap: it is window + maxAttempt, not
// window. The window caps retry SPACING, but the failure COUNT that drives the
// linear ramp only survives while the entry is live, and the gap between two
// consecutive failures is (retry spacing <= window) + (one full failing
// ingest attempt). A TTL of just window would expire the entry mid-attempt and
// reset the count to its base every time, defeating the ramp. Carried verbatim
// in meaning from services/legacy/netsync/manager.go:1443 (PR 1192).
func newBlockFailureBackoffMap(base, window, maxAttempt time.Duration) *expiringmap.ExpiringMap[chainhash.Hash, *blockFailureState] {
	if base <= 0 || window <= 0 {
		return nil
	}

	retention := window
	if maxAttempt > 0 {
		retention += maxAttempt
	}

	return expiringmap.New[chainhash.Hash, *blockFailureState](retention).WithMaxSize(blockFailureBackoffMaxTracked)
}

// Enabled reports whether the byte budget is active. When it is not, Acquire
// reserves nothing and the caller keeps the synchronous one-block-in-flight
// behaviour that full TCP backpressure already provides.
func (a *Admission) Enabled() bool { return a.budget != nil }

// BudgetBytes returns the configured byte ceiling, or 0 when prefetch is off.
func (a *Admission) BudgetBytes() int64 { return a.budgetBytes }

// Waiters reports how many callers are currently parked waiting for budget.
// Above zero means local processing, not the peer, is the limiting factor —
// protocol's stall detector must not rotate a peer for that.
func (a *Admission) Waiters() int64 { return a.waiters.Load() }

// Acquire reserves budget for a block of sizeBytes and returns the weight the
// caller MUST hand back to Release exactly once. sizeBytes is the block's
// declared payload length, taken from the transport's message envelope: that
// is the same number legacy weighted by (blockAdmissionWeight's payloadSize
// argument, services/legacy/peer_server.go:1293). The weight is floored at
// minInFlightBlockWeight and clamped to the whole budget, so a block larger
// than the entire budget waits for everything else to drain and then runs
// alone — the original one-block-at-a-time backpressure for huge blocks, and
// the reason Acquire can never deadlock on an oversized block.
//
// It returns ErrDuplicateBlockInFlight when a copy of blockHash is already
// admitted (nothing reserved), or ctx's error when the wait is cancelled
// (nothing reserved). quit, when non-nil, aborts a parked wait on peer
// teardown as well as on ctx cancellation.
func (a *Admission) Acquire(ctx context.Context, quit <-chan struct{}, blockHash chainhash.Hash, sizeBytes int64) (int64, error) {
	if a.budget == nil {
		return 0, nil
	}

	weight := sizeBytes
	if weight < minInFlightBlockWeight {
		weight = minInFlightBlockWeight
	}

	if weight > a.budgetBytes {
		weight = a.budgetBytes
	}

	// Reserve the hash BEFORE the possibly blocking acquire, so duplicates are
	// bounded even while a copy is parked: otherwise N copies of one
	// near-budget-sized block could each reserve budget and fill it.
	a.inFlightMu.Lock()

	if _, dup := a.inFlight[blockHash]; dup {
		a.inFlightMu.Unlock()

		return 0, ErrDuplicateBlockInFlight
	}

	a.inFlight[blockHash] = struct{}{}
	a.inFlightMu.Unlock()

	if a.budget.TryAcquire(weight) {
		return weight, nil
	}

	// Slow path: wait for in-flight blocks to drain, flagging that the wait is
	// our own backpressure and not a slow peer.
	a.waiters.Add(1)
	defer a.waiters.Add(-1)

	if quit != nil {
		var cancel context.CancelFunc

		ctx, cancel = context.WithCancel(ctx)
		defer cancel()

		go func() {
			select {
			case <-quit:
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	if err := a.budget.Acquire(ctx, weight); err != nil {
		// Nothing reserved, so the hash must not linger in the dedup set.
		a.inFlightMu.Lock()
		delete(a.inFlight, blockHash)
		a.inFlightMu.Unlock()

		return 0, err
	}

	return weight, nil
}

// Release returns budget reserved by Acquire and drops the hash from the dedup
// set. Both halves are released together, on block completion rather than peer
// lifetime, so the budget can never under-count and over-admit.
//
// Release ONLY what Acquire admitted. Pairing it with a failed Acquire is a
// dedup-corrupting bug: on the ErrDuplicateBlockInFlight path the hash in the
// set belongs to the copy still being ingested, so releasing it there would
// evict a live entry and let a third copy in against the same budget. Legacy
// relied on its single call site never doing that (netsync ReleaseBlockPrefetch
// is reached only from awaitBlockResult, which the duplicate path never spawns);
// this is an exported API any composer can call, so the weight guard below is
// hoisted above the delete to make the mistake harmless rather than silent.
// A zero weight means nothing was reserved, which is exactly the failed-Acquire
// case, so it now touches neither half of the gate.
func (a *Admission) Release(blockHash chainhash.Hash, weight int64) {
	if a.budget == nil || weight <= 0 {
		return
	}

	a.inFlightMu.Lock()
	delete(a.inFlight, blockHash)
	a.inFlightMu.Unlock()

	a.budget.Release(weight)
}

// RecordFailure records or extends the transient-failure backoff for a block
// hash and returns the window the block must now wait. The failure count rises
// by one per consecutive failure (resetting once the map TTL forgets the hash)
// and the window grows linearly, capped at the configured maximum. It is a
// no-op returning 0 when the backoff is disabled.
//
// Only TRANSIENT LOCAL failures belong here — service and storage errors,
// which errors.IsTransientLocalError classifies. A block that is genuinely
// invalid is the peer's fault and must reject and rotate, not back off.
func (a *Admission) RecordFailure(blockHash chainhash.Hash) time.Duration {
	if a.failures == nil {
		return 0
	}

	attempts := 1
	if fs, ok := a.failures.Get(blockHash); ok {
		attempts = fs.attempts + 1
	}

	backoff := time.Duration(attempts) * a.failureBase
	if backoff > a.failureWindow {
		backoff = a.failureWindow
	}

	a.failures.Set(blockHash, &blockFailureState{
		attempts:  attempts,
		nextRetry: time.Now().Add(backoff),
	})

	return backoff
}

// ClearFailure forgets a block's failure history, so a later failure starts a
// fresh count instead of inheriting a stale one. Call it after the block has
// been ingested successfully.
func (a *Admission) ClearFailure(blockHash chainhash.Hash) {
	if a.failures == nil {
		return
	}

	a.failures.Delete(blockHash)
}

// BackoffRemaining reports how long a block's backoff window still has to run,
// how many consecutive failures it has, and whether the caller must skip it.
func (a *Admission) BackoffRemaining(blockHash chainhash.Hash) (time.Duration, int, bool) {
	if a.failures == nil {
		return 0, 0, false
	}

	fs, ok := a.failures.Get(blockHash)
	if !ok {
		return 0, 0, false
	}

	remaining := time.Until(fs.nextRetry)
	if remaining <= 0 {
		return 0, fs.attempts, false
	}

	return remaining, fs.attempts, true
}

// SkipForBackoff returns the error the caller must surface instead of
// ingesting a block that is still inside its failure window, or nil when the
// block may proceed. The skip is non-blocking by design: the caller returns a
// retryable error rather than sleeping, so the block-processing path is never
// stalled and re-delivery is driven by the existing recovery plumbing.
//
// The error is a ServiceUnavailable error, which errors.IsTransientLocalError
// classifies as a local fault. That matters upstream: the delivering peer must
// NOT be rotated for it, because rotating in a fresh peer would only re-deliver
// the same still-backed-off block. Carried from the skip in
// services/legacy/netsync/manager.go handleBlockMsg (PR 1192).
//
// CALLER'S JOB: legacy also refreshed the delivering peer's lastBlockTime on
// this path (manager.go:1668-1670), because the peer did deliver a block and
// the fault is our local store. Admission holds no peer state, so protocol
// must do it: map errors.IsTransientLocalError to "refresh the stall timer, do
// not rotate". Without that refresh the backoff window itself looks like a
// stalled peer and rotates one for a fault it had no part in.
func (a *Admission) SkipForBackoff(blockHash chainhash.Hash) error {
	remaining, attempts, skip := a.BackoffRemaining(blockHash)
	if !skip {
		return nil
	}

	a.logger.Warnf("[svp2p:admission][%s] in backoff after %d transient failure(s), skipping for another %s",
		blockHash.String(), attempts, remaining)

	return errors.NewServiceUnavailableError("[svp2p:admission][%s] block in backoff after %d transient failure(s)",
		blockHash.String(), attempts)
}

// PreAdmitContext bounds the work a caller does before admitting a block —
// ban checks, existence lookups — so a wedged local round-trip cannot park the
// transport read loop. These calls are sub-millisecond on a healthy node and
// budget backpressure is strictly after them, so the deadline only ever fires
// on a genuine hang. The caller must always cancel the returned context.
func (a *Admission) PreAdmitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, a.preAdmitTimeout)
}

// PreAdmitTimedOut reports whether a failed pre-admission call should rotate
// the sync peer: true only when the pre-admission context hit its own
// deadline, which means a wedged local round-trip stranded a requested block.
// A parent cancellation (daemon shutdown, peer teardown) is not a timeout —
// there the block is simply dropped. Callers must consult this only after a
// pre-admission call has actually failed.
func PreAdmitTimedOut(preAdmitCtx context.Context) bool {
	return errors.Is(preAdmitCtx.Err(), context.DeadlineExceeded)
}

// Stop releases the failure map's background eviction goroutine. It is safe to
// call on an Admission whose backoff is disabled, and safe to call more than
// once only in the sense that the underlying map tolerates it; owners should
// call it exactly once at shutdown.
func (a *Admission) Stop() {
	if a.failures != nil {
		a.failures.Stop()
	}
}
