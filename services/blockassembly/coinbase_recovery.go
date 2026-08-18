package blockassembly

import (
	"context"
	"math"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// coinbaseRecoveryRetryBackoff is the pause between automatic recovery
// attempts. Retries exist to ride out a transient store or blockchain-client
// blip, and retrying instantly would burn the whole attempt budget inside the
// same blip. It is deliberately a constant rather than a setting: this runs
// once at startup with a small attempt budget, so the total added boot latency
// is bounded at (CoinbaseRecoveryMaxAttempts-1) * this value.
const coinbaseRecoveryRetryBackoff = 500 * time.Millisecond

// canonicalBlockAt fetches the canonical block at a height, tagging "the chain
// has no block there" as a block-not-found error so callers can tell it apart
// from the client being unable to answer (see canonicalBlockAbsent).
//
// The tag has to be applied here rather than inferred later. Different
// blockchain-client implementations report the missing case with different
// error codes, and once every failure is wrapped in the same processing error
// the distinction is only recoverable by matching on the wrapped code -- which
// then also matches a store failure that happens to carry the same code.
//
// A not-found from the client is not on its own proof that the chain stops
// short of this height, and must be corroborated before it is believed. The
// blockchain gRPC server relabels every store failure as block-not-found
// (services/blockchain/Server.go, GetBlockByHeight), and teranode's errors.Is
// matches *Error values by code, so a query failure, a dropped connection or a
// driver deadline all arrive here indistinguishable from a genuinely absent
// block. Block assembly runs that gRPC client in production while every test
// drives the local client, which does keep storage errors and not-found apart
// -- so believing the tag would pass the tests and fail on the real node.
//
// Believing it wrongly is the expensive direction: canonicalBlockAbsent short
// circuits withProbeRetry before the retry budget is touched, and the startup
// scan skips the height rather than abandoning the boot with a visible error,
// so a blockchain service that is down for the whole window would silently skip
// every height and report nothing. Corroborating against the chain height costs
// one extra call on a path that is rare on a healthy node, and an
// uncorroborated not-found is treated as possibly transient so the retry budget
// engages as intended.
//
// The text-fold that denial needs is confined to the error it is aimed at.
// Flattening a cause into a message is not a neutral reformatting: it discards
// the whole chain, and with it every classification a caller might test for --
// so applying it to all causes silently broke errors.IsContextError, which is
// what tells a shutdown apart from a fault in both callers. A cancellation is
// now returned untouched, an uncorroborated not-found is still folded, and
// anything else is wrapped normally. (Review finding ChiR20.)
func (b *BlockAssembler) canonicalBlockAt(ctx context.Context, height uint32) (*model.Block, error) {
	blk, err := b.blockchainClient.GetBlockByHeight(ctx, height)
	if err != nil {
		if errors.IsContextError(err) {
			// A shutdown, not a fault, and returned exactly as it arrived.
			// Everything below either asks the client a second question it can
			// no longer answer, or flattens the cause into text -- and a
			// flattened cause leaves errors.IsContextError nothing to inspect,
			// so an ordinary stop reads as a failure. That costs the
			// "aborted" outcome its meaning and fires MANUAL INTERVENTION
			// REQUIRED at an operator who only stopped the node.
			return nil, err
		}

		notFound := errors.Is(err, errors.ErrBlockNotFound) || errors.Is(err, errors.ErrNotFound)

		if notFound && b.chainStopsBelow(ctx, height) {
			return nil, errors.NewBlockNotFoundError("[coinbaseRecovery] no canonical block at height %d", height, err)
		}

		if notFound {
			// Only the uncorroborated not-found is folded in as text rather
			// than wrapped, and only because wrapping it would leave
			// canonicalBlockAbsent still answering true for it through the
			// chain -- the whole condition this branch exists to deny. The fold
			// is a targeted way to hide one error code, so it is applied to the
			// one error that needs hiding and to nothing else.
			return nil, errors.NewProcessingError("[coinbaseRecovery] cannot get canonical block at height %d: %s", height, err.Error())
		}

		// Not a not-found at all, so there is nothing to hide: wrap normally and
		// let every classification the cause carries survive for its callers.
		return nil, errors.NewProcessingError("[coinbaseRecovery] cannot get canonical block at height %d", height, err)
	}

	if blk == nil {
		return nil, errors.NewBlockNotFoundError("[coinbaseRecovery] no canonical block at height %d", height)
	}

	return blk, nil
}

// chainStopsBelow reports whether the canonical chain genuinely does not reach
// the given height, corroborating a not-found from the blockchain client
// against the chain's own best height (see canonicalBlockAt).
//
// It answers false when the corroborating lookup itself fails. That keeps the
// unprovable case on the retry path rather than the skip path: a client that
// cannot answer this question is exactly the client whose not-found should not
// be trusted.
func (b *BlockAssembler) chainStopsBelow(ctx context.Context, height uint32) bool {
	_, meta, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil || meta == nil {
		return false
	}

	return height > meta.Height
}

// canonicalCoinbaseAt reports whether block assembly's UTXO store holds the
// canonical coinbase transaction for the given height. It returns the
// canonical block itself so the repair path can reuse its CoinbaseTx without
// re-fetching from the blockchain client.
func (b *BlockAssembler) canonicalCoinbaseAt(ctx context.Context, height uint32) (present bool, canonicalBlock *model.Block, err error) {
	blk, err := b.canonicalBlockAt(ctx, height)
	if err != nil {
		return false, nil, err
	}

	if blk.CoinbaseTx == nil {
		return false, nil, errors.NewProcessingError("[coinbaseRecovery] canonical block at height %d has no coinbase", height)
	}

	txMeta, err := b.utxoStore.Get(ctx, blk.CoinbaseTx.TxIDChainHash(), fields.Tx)
	if err != nil {
		// Either code means the same thing here -- the coinbase is not in the
		// store -- and which one comes back depends on the store backend.
		if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) {
			return false, blk, nil
		}

		return false, blk, errors.NewProcessingError("[coinbaseRecovery] error checking coinbase at height %d", height, err)
	}

	if txMeta == nil || txMeta.Tx == nil {
		return false, blk, nil
	}

	return true, blk, nil
}

// coinbaseRecoveryProbeRetries is the number of probe retries the startup scan
// may spend in total, across every height it visits.
//
// It is a whole-scan budget rather than a per-height one on purpose. Detection
// runs only at startup, so a single transient failure must not be allowed to
// disable it for the entire boot -- but a store that flaps for the whole window
// must not be allowed to add window * retries * backoff to boot time either.
// Two retries caps the added latency at one second.
const coinbaseRecoveryProbeRetries = 2

// probeBudget is the retry allowance one startup scan may spend, shared by
// every lookup that scan makes.
type probeBudget struct {
	left int
}

// spend takes one retry from the budget, reporting whether there was one to
// take.
func (p *probeBudget) spend() bool {
	if p.left <= 0 {
		return false
	}

	p.left--

	return true
}

// canonicalBlockAbsent reports whether err means the canonical chain has no
// block at the probed height at all, as opposed to the client or store failing
// to answer. It pairs with the tag canonicalBlockAt applies.
//
// This is worth separating because it is not a health signal: it says the
// question was wrong, not that the answer is unavailable. It happens when block
// assembly's persisted tip height sits above the canonical chain height, and
// treating it like a store outage would abandon the whole scan on the very
// first probe.
func canonicalBlockAbsent(err error) bool {
	return errors.Is(err, errors.ErrBlockNotFound)
}

// withProbeRetry runs a startup probe, retrying transient failures out of the
// shared whole-scan budget.
//
// Without it a single transient store or blockchain-client failure abandons the
// only coinbase-divergence detector for the whole boot -- and because the
// incident this recovery exists for had a restart as its only repair
// opportunity, losing the check to one blip is a poor trade when an attempt
// budget is already available a level down.
//
// budget is shared by every probe in one scan. A missing canonical block is
// returned immediately without spending any of it: retrying cannot conjure a
// block the chain does not have.
func (b *BlockAssembler) withProbeRetry(ctx context.Context, height uint32, budget *probeBudget, probe func() error) error {
	for {
		err := probe()
		if err == nil || canonicalBlockAbsent(err) || !budget.spend() {
			return err
		}

		b.logger.Warnf("[coinbaseRecovery] startup probe at height %d failed, retrying (%d scan retries left): %v", height, budget.left, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(coinbaseRecoveryRetryBackoff):
		}
	}
}

// probeCanonicalCoinbase is canonicalCoinbaseAt with the shared retry budget
// applied, discarding the canonical block the detection path has no use for.
func (b *BlockAssembler) probeCanonicalCoinbase(ctx context.Context, height uint32, budget *probeBudget) (present bool, err error) {
	err = b.withProbeRetry(ctx, height, budget, func() error {
		var probeErr error

		present, _, probeErr = b.canonicalCoinbaseAt(ctx, height)

		return probeErr
	})

	return present, err
}

// coinbaseGapTooLargeError is the type of the errCoinbaseGapTooLarge sentinel.
//
// It is deliberately NOT built with errors.NewProcessingError. teranode's
// errors.Is matches two *Error values by their error *code*, not by identity,
// so a ProcessingError sentinel would compare equal to every other
// ProcessingError -- including the wrapped canonicalCoinbaseAt failures
// scopeCoinbaseGap also returns. recoverCoinbaseDivergence has to tell those
// two apart (structural gap = escalate now, transient store blip = retry), and
// with a ProcessingError sentinel that test would always say "too large".
//
// As a distinct type it takes the non-*Error path in errors.Is: identity
// comparison against this sentinel, and a message-substring test (which no
// canonicalCoinbaseAt error matches) against anything else.
type coinbaseGapTooLargeError struct{}

// Error implements the error interface.
func (coinbaseGapTooLargeError) Error() string {
	return "[coinbaseRecovery] gap exceeds max recoverable blocks"
}

// errCoinbaseGapTooLarge is returned by scopeCoinbaseGap when the walk-back
// from the trigger height exceeds CoinbaseRecoveryMaxGapBlocks without
// finding a proven-good floor. The orchestrator treats this as a non-local
// divergence and escalates immediately rather than spending retries on it.
var errCoinbaseGapTooLarge error = coinbaseGapTooLargeError{}

// coinbaseFloorNotProvenError is the type of the errCoinbaseFloorNotProven
// sentinel. Same reasoning as coinbaseGapTooLargeError for why it is not a
// teranode *Error.
type coinbaseFloorNotProvenError struct{}

// Error implements the error interface.
func (coinbaseFloorNotProvenError) Error() string {
	return "[coinbaseRecovery] walk-back reached its safety floor without proving a good floor beneath the gap"
}

// errCoinbaseFloorNotProven is returned by scopeCoinbaseGap when the walk-back
// runs out of safely-probeable heights while still inside a gap. Like
// errCoinbaseGapTooLarge it is structural, not transient.
var errCoinbaseFloorNotProven error = coinbaseFloorNotProvenError{}

// isUnscopableCoinbaseGap reports whether err is one of the structural scoping
// failures, for which retrying cannot change the answer, as opposed to a
// transient canonicalCoinbaseAt failure, which the attempt budget should
// absorb.
func isUnscopableCoinbaseGap(err error) bool {
	return errors.Is(err, errCoinbaseGapTooLarge) || errors.Is(err, errCoinbaseFloorNotProven)
}

// coinbaseRepairFloor returns the lowest height at which "no coinbase in the
// UTXO store" provably means "this coinbase was never created", so re-creating
// it cannot resurrect an output that was already legitimately spent.
//
// The distinction matters because absence is ambiguous. A coinbase that was
// created, matured, fully spent and then pruned by the DAH pruner leaves no
// record either -- utxoStore.Get returns ErrTxNotFound for it exactly as it
// does for one that was never written. Re-creating that one would put spent
// outputs back into the UTXO set as unspent: a corruption far worse than the
// wedge this recovery exists to fix.
//
// Coinbase maturity draws the line. A coinbase at height h cannot be spent
// until a block at height h+CoinbaseMaturity exists, and an unspent output is
// never given a delete-at height, so it is never pruned. Every height above
// tipHeight-CoinbaseMaturity is therefore still immature, cannot have been
// spent, cannot have been pruned, and absence there means exactly one thing.
//
// Below that line the walk stops rather than guessing, which is why a
// divergence deeper than the maturity window escalates to an operator instead
// of being auto-repaired.
//
// tipHeight must be a prune reference height (see pruneReferenceHeight), not
// block assembly's own tip: the floor is only sound if it is measured against
// the highest height the pruner could already have acted on.
//
// Where the proof cannot be made at all -- no chain parameters to read, or a
// maturity of zero, under which a coinbase is spendable the moment it is
// created and therefore no height is provably unspent -- the answer is to
// refuse every height, not to open the whole chain. Those are configurations
// no shipped network uses (all go-chaincfg networks set 100), but a floor
// whose degenerate branch is the widest possible window is the wrong way round.
func (b *BlockAssembler) coinbaseRepairFloor(tipHeight uint32) uint32 {
	if b.settings == nil || b.settings.ChainCfgParams == nil {
		// Nothing to measure maturity with, so nothing can be proven immature.
		//
		// Only the ChainCfgParams half of this is reachable today: both callers
		// read b.settings.BlockAssembly before they get here, so a nil settings
		// would already have panicked further up. It is kept because dropping it
		// would move the nil dereference into the line below rather than remove
		// it -- but it should not be read as evidence that a nil settings is
		// handled, because upstream of here it is not.
		return refuseEveryHeight(tipHeight)
	}

	maturity := uint32(b.settings.ChainCfgParams.CoinbaseMaturity)
	if maturity == 0 {
		// Coinbases are spendable immediately, so any of them could already
		// have been spent and pruned. Nothing is safe to re-create.
		return refuseEveryHeight(tipHeight)
	}

	if tipHeight <= maturity {
		// Chain shorter than the maturity window: nothing on it can have
		// matured yet, so every height down to 1 is safe to probe.
		return 1
	}

	return tipHeight - maturity + 1
}

// refuseEveryHeight returns a floor that sits above tipHeight, so every
// walk-back bounded by it collects nothing. Used for the configurations where
// coinbaseRepairFloor cannot prove any height unspent.
func refuseEveryHeight(tipHeight uint32) uint32 {
	if tipHeight == math.MaxUint32 {
		// Saturate rather than wrap: tipHeight+1 would be 0 here, which reads
		// as "no floor at all" and would open the entire chain to repair --
		// the exact opposite of what this function is for.
		return tipHeight
	}

	return tipHeight + 1
}

// pruneReferenceHeight returns the highest height the DAH pruner could already
// have acted on, which is what coinbaseRepairFloor has to be measured against.
//
// The pruner does not work off block assembly's tip pointer. Delete-at heights
// are stamped as the UTXO store's own height plus the retention window, and the
// store's height is fed from the chain best height by the store factory's
// blockchain subscription (stores/utxo/factory/utxo.go). Block assembly's
// pointer routinely lags that -- handleCatchUp exists for exactly that case --
// so deriving the floor from it would put the bottom of the scan window inside
// prunable territory once the lag exceeded roughly CoinbaseMaturity plus the
// retention window. Absence down there stops meaning "never created" and can
// equally mean "created, matured, fully spent, pruned", which is precisely the
// resurrection the floor exists to prevent.
//
// Taking the higher of the two can only raise the floor, never lower it, so the
// worst case is a scan window narrower than strictly necessary -- the safe
// direction. baHeight is kept as the lower bound so a store that has not yet
// been given a height (or is absent altogether, as in the store-less test
// paths) still yields a real floor rather than falling back to height 1.
func (b *BlockAssembler) pruneReferenceHeight(baHeight uint32) uint32 {
	if b.utxoStore == nil {
		return baHeight
	}

	if storeHeight := b.utxoStore.GetBlockHeight(); storeHeight > baHeight {
		return storeHeight
	}

	return baHeight
}

// scopeCoinbaseGap walks back from triggerHeight, collecting canonical
// blocks whose coinbase is absent from the UTXO store, until it has seen
// CoinbaseRecoveryConsecutiveGood *consecutive* present coinbases -- proving
// a good floor beneath the gap. The consecutive-good requirement (rather
// than stopping at the first present coinbase) exists because the
// fast-forward create loop is concurrent and can leave a present coinbase
// sitting above still-missing ones (a "hole"); stopping early there would
// under-scope the repair.
//
// The walk is bounded by CoinbaseRecoveryMaxGapBlocks: exceeding it returns
// errCoinbaseGapTooLarge so the caller can escalate instead of attempting to
// repair an unbounded (and likely non-local) divergence.
//
// The walk never descends below coinbaseRepairFloor. That floor is what keeps
// "absent" unambiguous: above it a coinbase is too young to have been spent,
// so it cannot have been pruned, so absence can only mean it was never
// created. It also subsumes the genesis case -- the floor is never below
// height 1, and the genesis coinbase is unspendable and never written to the
// UTXO store, so probing height 0 would always report it "missing" and pull
// genesis into the repair set, where processCoinbaseUtxos would write a bogus
// entry (block ID 0, maturity taken from the store's current height).
//
// Reaching the floor while still inside a gap returns errCoinbaseFloorNotProven:
// the repair set cannot be proven safe, so the caller escalates rather than
// re-creating coinbases that may have been legitimately spent and pruned.
//
// gapBlocks is returned in ascending height order. An empty slice with a nil
// error means no gap was found (the trigger height itself already satisfies
// the consecutive-good floor).
func (b *BlockAssembler) scopeCoinbaseGap(ctx context.Context, triggerHeight uint32) (gapBlocks []*model.Block, err error) {
	needConsecutive := b.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood
	if needConsecutive < 1 {
		// A non-positive setting would satisfy the floor test on the first
		// present coinbase, silently degrading the walk to the stop-at-first
		// behaviour this function exists to avoid.
		needConsecutive = 1
	}

	maxGap := b.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks
	if maxGap < 1 {
		// A non-positive setting would escalate on the very first missing
		// coinbase, making every divergence unrecoverable.
		maxGap = 1
	}

	// The maturity window is measured from the chain tip, not from
	// triggerHeight, since it is the tip that determines what has had the
	// chance to mature. triggerHeight is used as a lower bound on the tip so
	// that a direct call with no persisted tip still gets a real floor rather
	// than falling all the way back to height 1. pruneReferenceHeight then
	// lifts it to the UTXO store's height when block assembly is lagging the
	// chain, which is the height the pruner actually works off.
	_, tipHeight := b.CurrentBlock()
	if triggerHeight > tipHeight {
		tipHeight = triggerHeight
	}

	safetyFloor := b.coinbaseRepairFloor(b.pruneReferenceHeight(tipHeight))
	if safetyFloor > triggerHeight {
		// Every height at or below the trigger is old enough to have been
		// spent and pruned, so absence proves nothing down there and there is
		// no safe repair set to build. Report "no gap" rather than an unproven
		// floor: a block assembler lagging the chain would otherwise raise the
		// manual-intervention alarm on every restart during a normal catch-up.
		b.logger.Warnf("[coinbaseRecovery] not scoping a gap at height %d: the maturity floor is %d, so no height at or below the trigger is provably unpruned", triggerHeight, safetyFloor)

		return nil, nil
	}

	var gap []*model.Block

	consecutiveGood := 0
	scanned := 0
	gapReachedFloor := false

	for h := int64(triggerHeight); h >= int64(safetyFloor); h-- {
		present, blk, err := b.canonicalCoinbaseAt(ctx, uint32(h))
		if err != nil {
			return nil, err
		}

		if present {
			consecutiveGood++
			if consecutiveGood >= needConsecutive {
				break
			}

			continue
		}

		consecutiveGood = 0
		gap = append(gap, blk)
		scanned++

		if uint32(h) == safetyFloor { //nolint:gosec // h is bounded below by safetyFloor, a uint32
			// The lowest height that can be probed safely is itself missing, so
			// the divergence probably carries on below it -- into heights where
			// absence proves nothing. That is the case worth refusing.
			gapReachedFloor = true
		}

		if scanned > maxGap {
			return nil, errCoinbaseGapTooLarge
		}
	}

	if gapReachedFloor {
		// Refuse the whole walk rather than repairing its safe part. The blocks
		// collected are provably safe (see below), but the part of the
		// divergence that continues below the floor is not visible from here, so
		// a partial repair would report success while leaving the rest to wedge
		// the tip on its maturity spend. Escalating names a remedy that covers
		// all of it.
		return nil, errCoinbaseFloorNotProven
	}

	// Deliberately no "was the floor proven by consecutive-good runs?" test here.
	// Every height the loop visits is at or above safetyFloor, and the floor's
	// definition is that such a height is too young to have been spent and
	// therefore cannot have been pruned -- so absence at any collected height
	// already means "never created", whatever the consecutive-good run did.
	//
	// Conflating the two refused repairs that were provably safe, and did it in
	// the worst place: the bottom of the scan window is the set of coinbases
	// closest to maturity, i.e. the last chance to fix a hole before its
	// maturity spend wedges the tip. With the default ConsecutiveGood of 6, a
	// single miss two heights above the floor left consecutiveGood at 2 and
	// raised MANUAL INTERVENTION REQUIRED for a one-block repair that could not
	// have resurrected anything. The consecutive-good rule guards against
	// under-scoping a gap (a concurrent-create hole above still-missing
	// heights); it was never what made the repair safe.

	// gap was accumulated walking toward genesis (descending height);
	// reverse it so callers get a deterministic ascending-height repair order.
	for i, j := 0, len(gap)-1; i < j; i, j = i+1, j-1 {
		gap[i], gap[j] = gap[j], gap[i]
	}

	return gap, nil
}

// recoverCoinbaseDivergence is the staged orchestration for a detected
// coinbase divergence at triggerHeight. It is coinbase-only and O(blocks in
// the gap): it scopes the gap (scopeCoinbaseGap) and, when non-empty,
// attempts a coinbase-only repair (subtreeProcessor.ReconcileCoinbases),
// retrying up to CoinbaseRecoveryMaxAttempts times. It never inspects
// per-transaction state, which is what keeps its cost independent of mempool
// or subtree size.
//
// Boundary: this repairs missing canonical coinbases only. Reconciling
// ordinary-transaction double-spend/conflict state for a genuine competing-fork
// reorg is out of scope here -- that state is owned by block validation.
//
// Only a structural scoping failure escalates immediately (see
// isUnscopableCoinbaseGap): no number of retries will change the answer. Every
// other failure -- a canonicalCoinbaseAt error during the walk-back, or a
// ReconcileCoinbases error during the repair -- is treated as possibly
// transient and absorbed by the attempt budget, with
// coinbaseRecoveryRetryBackoff between attempts. A momentary store hiccup must
// not fire the loudest signal the system has: crying wolf on "escalated" would
// erode its value for real divergences.
//
// Exactly one outcome metric is recorded per call alongside "detected", so
// detected == repaired + no_gap + escalated + aborted:
//   - "repaired"  -- a non-empty gap was closed
//   - "no_gap"    -- scoping found nothing to repair (a concurrent writer, or
//     a retry, closed it first)
//   - "escalated" -- structural gap, or the attempt budget ran out
//   - "aborted"   -- the context was cancelled mid-recovery, i.e. the node is
//     shutting down. Nothing is known to be wrong, so this raises no alarm.
//
// On escalation it logs a single loud operator-facing line naming
// resetblockassembly and returns an error. "Escalate" means this recovery
// stops trying, not that the process stops: the sole caller
// (checkCoinbaseDivergenceOnStart) deliberately lets the node boot anyway --
// see the note there. The node therefore does keep advancing on the diverged
// tip, and an operator must intervene for it to be genuinely repaired.
func (b *BlockAssembler) recoverCoinbaseDivergence(ctx context.Context, triggerHeight uint32) error {
	prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected").Inc()

	maxAttempts := b.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts
	if maxAttempts < 1 {
		// A non-positive setting would skip the loop entirely and escalate
		// without ever trying to repair. Clamp to a single attempt, matching
		// the lower-bound clamps scopeCoinbaseGap applies to its own settings.
		maxAttempts = 1
	}

	var lastErr error

attempts:
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			// Shutdown (or a cancelled startup) while retrying: stop burning
			// attempts against a dead context and report the real reason.
			lastErr = ctx.Err()
			break
		}

		if attempt > 1 {
			select {
			case <-ctx.Done():
				lastErr = ctx.Err()
				break attempts
			case <-time.After(coinbaseRecoveryRetryBackoff):
			}
		}

		gap, err := b.scopeCoinbaseGap(ctx, triggerHeight)
		if err != nil {
			lastErr = err

			if isUnscopableCoinbaseGap(err) {
				// Structural: either the divergence is non-local, or the walk
				// ran out of heights where "absent" provably means "never
				// created". Retrying the same walk-back cannot change either.
				break
			}

			// A canonicalCoinbaseAt failure (store or blockchain-client blip).
			// Possibly transient, so spend an attempt on it rather than
			// raising the manual-intervention alarm on a network hiccup.
			b.logger.Warnf("[coinbaseRecovery] attempt %d/%d could not scope the gap below height %d: %v", attempt, maxAttempts, triggerHeight, err)

			continue
		}

		if len(gap) == 0 {
			// Nothing to repair. Recorded as its own outcome so the metric
			// balances rather than leaving a "detected" with no counterpart.
			prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("no_gap").Inc()

			return nil
		}

		if err := b.subtreeProcessor.ReconcileCoinbases(ctx, gap); err != nil {
			lastErr = err
			b.logger.Warnf("[coinbaseRecovery] attempt %d/%d failed to repair %d coinbase(s) below height %d: %v", attempt, maxAttempts, len(gap), triggerHeight, err)

			continue
		}

		b.logger.Infof("[coinbaseRecovery] repaired %d coinbase(s) up to height %d on attempt %d", len(gap), triggerHeight, attempt)
		prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired").Inc()

		return nil
	}

	// Defensive: every break above sets lastErr, but keep the error message
	// truthful rather than wrapping a nil if a future edit adds a path that
	// does not.
	if lastErr == nil {
		lastErr = errors.NewProcessingError("[coinbaseRecovery] no repair attempt completed after %d attempts", maxAttempts)
	}

	if errors.IsContextError(lastErr) {
		// The node is shutting down, or the startup context was cancelled.
		// Nothing is known to be wrong with the UTXO state, so this must not
		// fire the loudest signal the system has. Telling an operator who just
		// restarted a pod mid-repair that manual intervention is required is
		// the same crying-wolf failure the attempt budget exists to avoid.
		prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("aborted").Inc()
		b.logger.Warnf("[coinbaseRecovery] recovery at height %d abandoned before completing: %v", triggerHeight, lastErr)

		return lastErr
	}

	// Stage 2: stop retrying and raise a single loud operator signal.
	prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated").Inc()
	b.logger.Errorf("[coinbaseRecovery] MANUAL INTERVENTION REQUIRED: coinbase divergence at/below height %d could not be auto-repaired after %d attempts (%v); the node will keep running on the diverged tip -- run resetblockassembly", triggerHeight, maxAttempts, lastErr)

	return errors.NewProcessingError("[coinbaseRecovery] unrecoverable coinbase divergence at height %d", triggerHeight, lastErr)
}

// checkCoinbaseDivergenceOnStart looks for canonical coinbases missing from the
// UTXO store near the persisted tip and repairs what it finds. Called once
// during Start, before block-notification listeners begin, so a divergence
// created by a prior unclean shutdown is healed before the node advances.
//
// # Detection coverage
//
// It probes a bounded window of heights walking down from the tip. On the
// first (i.e. highest) missing coinbase it runs recovery from there --
// recoverCoinbaseDivergence scopes and repairs the gap beneath it -- and then
// carries on down the same window looking for further holes, because the
// walk-back only proves CoinbaseRecoveryConsecutiveGood present coinbases
// below the gap it closed and stops there. Only a refusal ends the scan, since
// a structural refusal would repeat identically at every lower height.
//
// Probing the tip alone is not enough. The fast-forward create loop in
// SubtreeProcessor.reset runs one goroutine per moveForward block, so a crash
// mid-loop can leave the tip's coinbase written while a lower one is still
// missing -- a "hole" under a healthy-looking tip. That is exactly the shape
// scopeCoinbaseGap's consecutive-good walk-back exists to repair, and a
// tip-only probe would return early and never invoke it, leaving the hole to
// wedge the node on the eventual maturity spend.
//
// The window is the smaller of CoinbaseRecoveryMaxGapBlocks heights and the
// coinbase-maturity window (see coinbaseRepairFloor, measured against
// pruneReferenceHeight). The first bound reuses what the repair itself honours
// -- there is no point detecting a divergence deeper than recovery will
// auto-repair. The second is a correctness bound: below the maturity floor a
// missing coinbase may simply have been spent and pruned, so probing there
// would report healthy blocks as diverged.
//
// A clean boot costs one blockchain-client lookup and one UTXO store read per
// height in the window, issued serially. Only the second of those is local: the
// first is GetBlockByHeight, which in a deployed node is a gRPC call to the
// blockchain service, so at the shipped defaults this adds on the order of a
// hundred sequential round-trips to startup. That is small on a healthy
// cluster, but it scales with the latency to another service rather than with
// local disk, which is worth knowing before this is blamed on the store.
//
// This is a bounded window, not whole-chain coverage. A hole further below the
// tip than the window stays invisible here; finding that cheaply is the job of
// the runtime point-of-pain detector, which is deliberately future work.
//
// Every lookup the scan makes draws on one shared retry budget
// (coinbaseRecoveryProbeRetries), so a transient blip costs a retry rather than
// the whole boot's detection, while a store that flaps for the entire window
// still cannot inflate boot time without bound.
//
// # Why it cannot fail Start
//
// It returns nothing, which is the whole contract rather than an omission. A
// recovery give-up is deliberately non-fatal: recoverCoinbaseDivergence has
// already logged the loud operator-facing alert and incremented the "escalated"
// metric internally before returning its error, so there is nothing left to
// surface here except the fact that startup could not repair it. Returning an
// error would fail Start and crash-loop the process, which does not help an
// operator -- a running node they can inspect is strictly better than one stuck
// restarting. The trade-off is explicit: the node boots and keeps building on a
// tip known to be diverged, so the escalation log is a call to action, not a
// description of a node that has stopped itself.
//
// Having no return value is what makes that contract enforceable. While this
// returned an error it was statically always nil, so the callers' handling of
// it -- Start discarding it, the tests requiring it to be nil -- asserted
// nothing at all, and a future edit that started returning a real error would
// have silently gained the power to abort the boot. Everything worth observing
// here is already on the logger and the coinbase-divergence metric, which is
// what the tests read.
func (b *BlockAssembler) checkCoinbaseDivergenceOnStart(ctx context.Context) {
	if b.blockchainClient == nil || b.utxoStore == nil {
		return // nothing to probe against; matches the guards on the other optional-store paths in Start
	}

	header, height := b.CurrentBlock()
	if header == nil || height == 0 {
		return // genesis / unset — nothing to check
	}

	// Only judge divergence when block assembly is actually on the canonical
	// chain at its own tip height. Every probe below resolves its height with
	// GetBlockByHeight, i.e. against the best-work chain, so a block assembler
	// still sitting on a branch the chain has since reorged away from would see
	// canonical coinbases it legitimately never created and report them as
	// diverged. The repair itself would be harmless -- processCoinbaseUtxos is
	// idempotent and moveForwardBlock would create those coinbases anyway --
	// but a fork deeper than CoinbaseRecoveryConsecutiveGood ends in an
	// unproven floor and a MANUAL INTERVENTION line for something the ordinary
	// reconcile handles on its own. Reorgs are shallow and rare on BSV, so this
	// is unlikely; the point is to keep that alarm meaningful when it does fire.
	//
	// This lookup draws on the same retry budget as the probes below, so it is
	// not itself a single point of failure for the whole scan.
	budget := &probeBudget{left: coinbaseRecoveryProbeRetries}

	var tipBlk *model.Block

	tipErr := b.withProbeRetry(ctx, height, budget, func() error {
		var err error

		tipBlk, err = b.canonicalBlockAt(ctx, height)

		return err
	})

	switch {
	case errors.IsContextError(tipErr):
		// Shutdown (or a cancelled startup) landed on the tip lookup. Saying the
		// canonical block could not be resolved would point an operator at the
		// blockchain client for something that is not a fault at all.
		b.logger.Infof("[coinbaseRecovery] startup divergence check stopped before scanning at tip height %d: shutting down", height)
		return

	case tipErr != nil && canonicalBlockAbsent(tipErr):
		// The canonical chain simply does not reach this height -- block
		// assembly's persisted tip sits above it. That is not a fault, so it must
		// not read like one. The branch below would blame the blockchain client
		// for a question it answered correctly.
		b.logger.Infof("[coinbaseRecovery] startup divergence check skipped: block assembly tip height %d is above the canonical chain height, so there is nothing to check there", height)
		return

	case tipErr != nil:
		b.logger.Warnf("[coinbaseRecovery] startup divergence check skipped: cannot resolve the canonical block at block assembly's tip height %d: %v", height, tipErr)
		return

	case tipBlk.Header == nil || !tipBlk.Header.Hash().IsEqual(header.Hash()):
		b.logger.Warnf("[coinbaseRecovery] startup divergence check skipped: block assembly tip %s at height %d is not the canonical block there, so the normal reconcile owns this", header.Hash().String(), height)
		return
	}

	window := b.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks
	if window < 1 {
		// A non-positive setting would scan nothing at all, silently disabling
		// startup detection. Match the lower-bound clamp scopeCoinbaseGap uses.
		window = 1
	}

	// Walk down from the tip, stopping at whichever bound bites first. The
	// safety floor is never below height 1, which also keeps the walk off
	// genesis: its coinbase is unspendable and never written to the UTXO
	// store, so probing height 0 would always report "missing". It is measured
	// against the prune reference height rather than block assembly's own tip,
	// so a lagging block assembler does not scan into already-prunable heights.
	scanFloor := b.coinbaseRepairFloor(b.pruneReferenceHeight(height))
	if scanFloor > height {
		// Block assembly is far enough behind the chain that its whole tip
		// window is already prunable, so no height here can be probed safely.
		// Say so rather than silently checking nothing.
		b.logger.Warnf("[coinbaseRecovery] startup divergence check skipped: block assembly tip %d is below the maturity floor %d, so no height in the window is provably unpruned", height, scanFloor)
		return
	}

	for h, scanned := height, 0; h >= scanFloor && scanned < window; h, scanned = h-1, scanned+1 {
		present, err := b.probeCanonicalCoinbase(ctx, h, budget)
		if err != nil {
			if canonicalBlockAbsent(err) {
				// The canonical chain simply has no block at this height. Skip
				// it rather than abandoning the scan: the heights below it are
				// still worth checking, and this is the one "error" that says
				// nothing at all about whether the client is healthy.
				b.logger.Warnf("[coinbaseRecovery] startup: no canonical block at height %d (tip %d); skipping that height", h, height)

				continue
			}

			if errors.IsContextError(err) {
				// The node is shutting down mid-probe, either in the lookup
				// itself or in withProbeRetry's backoff. Detection is not lost
				// for the run -- there is no run left -- so this must not claim
				// it is. Same reasoning as the recovery call site below.
				b.logger.Infof("[coinbaseRecovery] startup divergence check stopped at height %d: shutting down", h)

				return
			}

			// Non-fatal: log and continue booting; see the function-level
			// comment above. Abandoning the whole scan (rather than skipping
			// this height) is deliberate -- the probe has already spent its
			// retry budget, so if the blockchain client or store still cannot
			// answer, the remaining probes will not either.
			//
			// Errorf, not Warnf: detection is startup-only, so giving up here
			// means the node runs with no coinbase-divergence check at all
			// until the next restart. That lost coverage should be visible.
			b.logger.Errorf("[coinbaseRecovery] startup divergence check abandoned at height %d (tip %d): %v; no coinbase-divergence detection will run until the next restart", h, height, err)

			return
		}

		if present {
			continue
		}

		b.logger.Warnf("[coinbaseRecovery] startup: canonical coinbase missing at height %d (tip %d); running recovery", h, height)

		if err := b.recoverCoinbaseDivergence(ctx, h); err != nil {
			if errors.IsContextError(err) {
				// Shutdown, not a divergence we failed to fix.
				// recoverCoinbaseDivergence has already recorded the "aborted"
				// outcome and said so; do not restate it as a call to action.
				return
			}

			// Non-fatal: see the function-level comment above. recoverCoinbaseDivergence
			// has already logged the loud MANUAL INTERVENTION alert and incremented
			// the "escalated" metric; log that startup recovery failed and let the
			// node boot so an operator can inspect and act.
			//
			// Stop scanning here. A refusal is structural -- an over-large gap,
			// or a floor that cannot be proven -- so the same walk-back run from
			// a lower height would refuse for the same reason, adding nothing
			// but repeat alarms.
			b.logger.Errorf("[coinbaseRecovery] startup recovery failed at height %d (tip %d): %v; manual intervention required (run resetblockassembly)", h, height, err)

			return
		}

		// Keep scanning below the range just repaired, inside the same window
		// budget. The walk-back only proved CoinbaseRecoveryConsecutiveGood
		// present coinbases beneath the gap it closed, so a second hole
		// separated from the first by that many present heights is still
		// missing and has not been looked for. That shape is the norm, not an
		// exotic case: the fast-forward create loop runs one goroutine per
		// block, so a crash mid-loop leaves a scattered set of misses, and at a
		// low miss rate runs of several successes between them are expected.
		//
		// Re-probing the heights just repaired costs one store read each and
		// now reports them present, so they are skipped. h decrements every
		// iteration, so the existing window and floor bounds still terminate
		// the loop.
	}
}
