package blockvalidation

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// subtreeKeyMismatchKey marks an error whose cause is a locally stored subtree blob
// that does not answer to the key it is stored under. Two shapes qualify: a .subtree
// or .subtreeToCheck blob whose nodes do not hash to that key, and a .subtreeData
// blob whose transactions do not match that subtree's nodes. Both are the same fault
// — bytes on disk under a key they do not belong to — and both take the same
// disposition. The value is a []subtreeBlobRef naming the exact blobs, so the handler
// deletes what was actually read rather than re-resolving the file type and possibly
// picking a different sibling. The marker pattern is the one markCacheBypassRetryable
// uses.
const subtreeKeyMismatchKey = "subtree_key_mismatch"

// subtreeKeyMismatchUnquarantinedKey marks a key mismatch whose blob could NOT be
// confirmed removed. It exists so tryQuickValidation can abort instead of falling
// through to normal validation, whose loader checks only the .subtree header's
// claimed root and therefore cannot detect the blob this route just rejected.
const subtreeKeyMismatchUnquarantinedKey = "subtree_key_mismatch_unquarantined"

// markerChainDepth bounds the walk over a wrapped error chain, matching
// isCacheBypassRetryable.
const markerChainDepth = 32

// subtreeBlobRef names one exact blob in the subtree store: the hash it is keyed by
// and the file type it was resolved from in the read that rejected it.
type subtreeBlobRef struct {
	hash     chainhash.Hash
	fileType fileformat.FileType
}

// markSubtreeKeyMismatch tags err with the refs of every blob whose nodes did not
// hash to its key, and returns the result. Nil-safe.
//
// The refs replace rather than extend any already present, because the only caller
// that sets them twice is the whole-block pass, which sets the complete collected
// set over the single ref the errgroup's first error carried.
func markSubtreeKeyMismatch(err error, refs ...subtreeBlobRef) error {
	if err == nil || len(refs) == 0 {
		return err
	}

	var e *errors.Error
	if errors.As(err, &e) {
		e.SetData(subtreeKeyMismatchKey, refs)
		return err
	}

	// A foreign error type has nothing to call SetData on, so wrap it the way
	// markCacheBypassRetryable does rather than lose the marker.
	wrapped := errors.NewProcessingError("subtree key mismatch", err)
	wrapped.SetData(subtreeKeyMismatchKey, refs)

	return wrapped
}

// subtreeKeyMismatchRefs returns the blobs named by the first key-mismatch marker in
// err's chain, or nil when there is none. The walk mirrors isCacheBypassRetryable:
// the marker must be found mid-chain because every layer above the read wraps the
// error.
func subtreeKeyMismatchRefs(err error) []subtreeBlobRef {
	var e *errors.Error
	if !errors.As(err, &e) {
		return nil
	}

	for depth := 0; e != nil && depth < markerChainDepth; depth++ {
		if refs, ok := e.GetData(subtreeKeyMismatchKey).([]subtreeBlobRef); ok && len(refs) > 0 {
			return refs
		}

		next := e.WrappedErr()
		if next == nil {
			return nil
		}

		var wrapped *errors.Error
		if !errors.As(next, &wrapped) {
			return nil
		}

		e = wrapped
	}

	return nil
}

// markUnquarantinedLocalSubtree records that a key-mismatching blob is still on
// disk. Nil-safe.
func markUnquarantinedLocalSubtree(err error) error {
	if err == nil {
		return nil
	}

	var e *errors.Error
	if errors.As(err, &e) {
		e.SetData(subtreeKeyMismatchUnquarantinedKey, true)
		return err
	}

	wrapped := errors.NewProcessingError("unquarantined local subtree", err)
	wrapped.SetData(subtreeKeyMismatchUnquarantinedKey, true)

	return wrapped
}

// isUnquarantinedLocalSubtree reports whether err names a key-mismatching blob that
// could not be confirmed removed from the local store.
func isUnquarantinedLocalSubtree(err error) bool {
	var e *errors.Error
	if !errors.As(err, &e) {
		return false
	}

	for depth := 0; e != nil && depth < markerChainDepth; depth++ {
		if v, ok := e.GetData(subtreeKeyMismatchUnquarantinedKey).(bool); ok && v {
			return true
		}

		next := e.WrappedErr()
		if next == nil {
			return false
		}

		var wrapped *errors.Error
		if !errors.As(next, &wrapped) {
			return false
		}

		e = wrapped
	}

	return false
}

// bindSubtreeBodyToHeader proves the peer-supplied body hashes to the header BEFORE
// any block-id assignment or UTXO mutation. It reads subtree structures, never
// subtree_data. Its only write is the quarantine of a blob that does not match its
// own key, and that happens at the caller's error boundary, not here.
//
// The binding cannot be moved inside the batch pipeline without giving up streaming:
// AssignBlockID runs inside the batch loop, after the first batch's read, so any
// check that only happens at batch-read time lands after a durable block-id
// reservation for every batch past the first. This pass runs the same checks on the
// whole block first, and holds only node hashes — 48 bytes per transaction — never a
// transaction body (bitcoin-sv/teranode#4838).
//
// That figure is the whole cost on every attempt, including a retry. A promoted
// FileTypeSubtree blob is still read and anchored by readSubtreeStructure when one is
// present, because anchoring it before the pipeline is the only pre-mutation check for
// a forged promoted blob beside an honest subtree_to_check — but this pass never reads
// its node list, so it releases it immediately rather than retaining a second copy of
// the block's nodes for the duration.
//
// The checks run on a probe block rather than on the live one: the pipeline is still
// the single writer of block.SubtreeSlices, so nothing here can race the TTL cleaner
// that releases a cached block's subtree nodes.
func (u *BlockValidation) bindSubtreeBodyToHeader(ctx context.Context, block *model.Block) error {
	if len(block.Subtrees) == 0 {
		// A coinbase-only body is bound at the entry points by
		// CheckCoinbaseOnlyBodyBound; there is no subtree list to compose.
		return nil
	}

	slices := make([]*subtreepkg.Subtree, len(block.Subtrees))

	// Collected under a mutex rather than taken from the errgroup's single error: a
	// doctored body can name several mismatching blobs and every one of them has to
	// be quarantined, not just whichever read failed first.
	var (
		mismatchMu       sync.Mutex
		mismatches       []subtreeBlobRef
		firstMismatch    error
		anyUnquarantined bool
	)

	// Deliberately a plain errgroup.Group on the caller's context, NOT
	// errgroup.WithContext: the first failing read must not cancel its siblings.
	//
	// This pass is the only place that can name EVERY mismatching blob for the
	// quarantine, and a cancelled sibling returns a context error in place of its own
	// anchor verdict. One forged blob would then be deleted, the attempt would be
	// classified an ordinary local fault, and normal validation would be handed the
	// other one — whose loader checks only the .subtree header's claimed root and so
	// cannot detect it. That is the fall-through this pass exists to prevent
	// (bitcoin-sv/teranode#4838). The cost is reading every structure even once one is
	// known bad, bounded by the block's node hashes and never its transaction bodies.
	var g errgroup.Group
	util.SafeSetLimit(u.logger, &g, 128)

	for i := range block.Subtrees {
		idx := i
		hash := block.Subtrees[idx]

		g.Go(func() error {
			structure, err := u.readSubtreeStructure(ctx, block, hash)
			if err != nil {
				if refs := subtreeKeyMismatchRefs(err); len(refs) > 0 {
					mismatchMu.Lock()

					mismatches = append(mismatches, refs...)

					if firstMismatch == nil {
						firstMismatch = err
					}

					// The FAIL-CLOSED verdict is aggregated separately from the refs,
					// because only one error survives as firstMismatch and it may not be
					// the one that could not audit its sibling.
					if isUnquarantinedLocalSubtree(err) {
						anyUnquarantined = true
					}

					mismatchMu.Unlock()
				}

				return err
			}

			// The promoted blob has served its purpose the moment readSubtreeStructure
			// anchored it; nothing in this pass reads it. Released here rather than at
			// the deferred sweep below so the block never holds two node lists per
			// subtree at once.
			releaseSubtreeStructure(structure.fullSubtree)

			slices[idx] = structure.subtree

			return nil
		})
	}

	readErr := g.Wait()

	// Every structure read here is discarded once the composition has been checked;
	// the pipeline reads its own. Released after the checks below, which need them.
	defer func() {
		for _, subtree := range slices {
			releaseSubtreeStructure(subtree)
		}
	}()

	if readErr != nil {
		mismatchMu.Lock()
		refs := dedupeSubtreeBlobRefs(mismatches)
		mismatchErr := firstMismatch
		unquarantined := anyUnquarantined
		mismatchMu.Unlock()

		if mismatchErr != nil {
			// Prefer an anchor verdict over any sibling's read failure: it is the error
			// that carries the quarantine, and errgroup reports whichever failed first
			// rather than whichever matters.
			return combineSweepMismatchError(mismatchErr, refs, unquarantined)
		}

		return readErr
	}

	// A probe rather than the live block: block.SubtreeSlices is still set exactly
	// once, by the pipeline, as it is today.
	probe := &model.Block{
		Header:        block.Header,
		CoinbaseTx:    block.CoinbaseTx,
		Height:        block.Height,
		Subtrees:      block.Subtrees,
		SubtreeSlices: slices,
	}

	return checkSubtreeBodyBinding(ctx, probe, "bindSubtreeBodyToHeader")
}

// combineSweepMismatchError folds the whole-block sweep's collected verdicts into the
// one error it returns: every mismatching blob it named, and whether ANY of them could
// not be fully audited.
//
// Aggregating the refs alone is not enough (bitcoin-sv/teranode#4838). Only one of the
// failing reads survives as the error to return, and it may be an ordinary mismatch
// while a different hash was the one whose sibling could not be read. Carrying only
// that first error's own markers would drop the fail-closed verdict: the named blobs
// would delete cleanly, the boundary would report success, and the attempt would fall
// through to normal validation with an unaudited blob still on disk — which is exactly
// the outcome the marker exists to prevent.
func combineSweepMismatchError(firstMismatch error, refs []subtreeBlobRef, anyUnquarantined bool) error {
	combined := markSubtreeKeyMismatch(firstMismatch, refs...)

	if anyUnquarantined {
		combined = markUnquarantinedLocalSubtree(combined)
	}

	return combined
}

// dedupeSubtreeBlobRefs collapses repeated (hash, fileType) pairs so the handler
// does not delete the same blob twice.
func dedupeSubtreeBlobRefs(refs []subtreeBlobRef) []subtreeBlobRef {
	if len(refs) < 2 {
		return refs
	}

	seen := make(map[subtreeBlobRef]struct{}, len(refs))
	out := make([]subtreeBlobRef, 0, len(refs))

	for _, ref := range refs {
		if _, ok := seen[ref]; ok {
			continue
		}

		seen[ref] = struct{}{}

		out = append(out, ref)
	}

	return out
}

// quarantineSubtreeKeyMismatch is the ONE boundary for every typed key mismatch
// produced by any subtree read on this route — the whole-block pass and all three
// processing variants — because all of them return through the two quick-validation
// entry points, where this is installed as a deferred rewrite of the named error
// return rather than an explicit call on each path.
//
// By the time it runs no reader of a subtree blob is still live: the whole-block pass
// joins its own group, and each batch builder cancels AND joins its per-batch reader
// group before returning. The asynchronous write workers may still be in flight, but
// with the carried full subtree they no longer read any blob — a Del/Set interleaving
// for the same key is benign in both orders, because the worker's bytes are built from
// anchored transactions, so the blob ends up either correct on disk or absent and
// re-fetched.
//
// A blob that does not answer to its own key is a LOCAL fault, not a peer fault, and
// the classification is by elimination rather than by caution: every in-tree writer
// serializes a tree built with AddNode, whose root is therefore recomputed, and the
// fetch path checks what it is about to store against the hash it asked for —
// structures against the requested hash, and a subtree_data body against the nodes of
// the subtree it belongs to — striking the serving peer before anything is written.
// What can be read back mismatching is therefore a stale or damaged local file, so no
// ban score is applied here.
//
// Returns err untouched when there is no marker.
func (u *BlockValidation) quarantineSubtreeKeyMismatch(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	refs := subtreeKeyMismatchRefs(err)
	if len(refs) == 0 {
		return err
	}

	var unconfirmed int

	for _, ref := range refs {
		confirmed, attempted := u.deleteSubtreeBlobConfirmed(ctx, ref)
		if confirmed {
			u.logger.Warnf("[quarantineSubtreeKeyMismatch] removed local subtree blob %s (%s) that does not answer to the key it is stored under", ref.hash.String(), ref.fileType)
			continue
		}

		unconfirmed++

		// The two failures are reported apart because they say different things to
		// whoever reads the log. "Could not be removed" accuses the local storage; on a
		// cancelled shared catch-up context every attempt returns instantly and nothing
		// was ever asked of the store, so reporting it that way sends the reader after a
		// storage fault that does not exist. The VERDICT is the same either way — an
		// unremoved blob is unremoved whatever the reason, and the attempt must still
		// fail closed.
		if !attempted {
			u.logger.Warnf("[quarantineSubtreeKeyMismatch] context cancelled before local subtree blob %s (%s) could be removed; no deletion was attempted", ref.hash.String(), ref.fileType)
			continue
		}

		u.logger.Errorf("[quarantineSubtreeKeyMismatch] could not confirm removal of local subtree blob %s (%s) that does not answer to the key it is stored under", ref.hash.String(), ref.fileType)
	}

	if unconfirmed > 0 {
		return markUnquarantinedLocalSubtree(err)
	}

	return err
}

// quarantineDeleteAttempts bounds the delete/confirm retries per blob. Three is
// enough to ride out a transient store error without turning a genuinely
// undeletable blob into a long stall.
const quarantineDeleteAttempts = 3

// quarantineDeleteBackoff is the pause before each retry. Retrying a failing store
// with no pause at all spends all three attempts inside a few microseconds, which
// rides out nothing: the transient fault the retries exist for has not had time to
// clear. The last entry is reused if the attempt count ever grows.
var quarantineDeleteBackoff = []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}

// deleteSubtreeBlobConfirmed deletes one exact blob and PROVES it is gone, because a
// Del that reports success while the blob survives would let the attempt fall
// through to a loader that cannot detect the forgery. ErrNotFound from Del counts as
// success: the blob is absent, which is the property being established.
//
// It returns whether the absence was confirmed and whether the store was asked at all.
// The second value exists for the caller's log line only: a cancelled context makes
// every attempt return without touching the store, and reporting that as a blob that
// could not be removed points the reader at local storage when the cause was the
// caller's own cancellation. Both cases are failures and both fail closed.
func (u *BlockValidation) deleteSubtreeBlobConfirmed(ctx context.Context, ref subtreeBlobRef) (confirmed, attempted bool) {
	for attempt := 0; attempt < quarantineDeleteAttempts; attempt++ {
		if attempt > 0 {
			// Slept through a select rather than a bare sleep: the catch-up context is
			// shared, so a cancellation arriving mid-backoff must end the loop rather
			// than hold it for the rest of the pause.
			idx := attempt - 1
			if idx >= len(quarantineDeleteBackoff) {
				idx = len(quarantineDeleteBackoff) - 1
			}

			select {
			case <-time.After(quarantineDeleteBackoff[idx]):
			case <-ctx.Done():
				return false, attempted
			}
		}

		if ctx.Err() != nil {
			return false, attempted
		}

		attempted = true

		delErr := u.subtreeStore.Del(ctx, ref.hash[:], ref.fileType)
		if delErr != nil && !errors.Is(delErr, errors.ErrNotFound) {
			continue
		}

		exists, existsErr := u.subtreeStore.Exists(ctx, ref.hash[:], ref.fileType)
		if existsErr == nil && !exists {
			return true, attempted
		}
	}

	return false, attempted
}
