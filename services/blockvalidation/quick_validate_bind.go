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
// It walks the block ONE CHUNK OF SUBTREES AT A TIME, releasing each chunk before
// reading the next. It used to read every subtree in the block at once at a hard-coded
// fan-out of 128, which bypassed the per-batch residency cap the pipeline exists to
// enforce: a catch-up block of several hundred million-node subtrees held tens of
// gigabytes of node data at once, and with mmapDir set one live mapping and one temp
// file per subtree, where the pipeline would have held one batch. What survives a chunk
// is 32 bytes of root per subtree plus the running duplicate-transaction set.
//
// The chunk width is the pipeline's own SubtreeBatchSize, so the pass's node-list
// residency cannot exceed one pipeline batch; the running txid set is whole-block and
// is the one term that grows with the block.
//
// Handing these anchored structures forward to the pipeline instead — so it need not
// re-read them — was considered and rejected. It cannot fix the residency: it EXTENDS
// it, from "until this function returns" to "until the last batch leaves stage 3". The
// duplicate structure read the pipeline then performs is the deliberate price of
// binding the whole block before the first batch mutates anything; the two pull in
// opposite directions and this is the chosen side (bitcoin-sv/teranode#4838). What can
// be removed without moving any anchor is removed — see readSubtreeStructure's two
// modes.
func (u *BlockValidation) bindSubtreeBodyToHeader(ctx context.Context, block *model.Block) error {
	numSubtrees := len(block.Subtrees)
	if numSubtrees == 0 {
		// A coinbase-only body is bound at the entry points by
		// CheckCoinbaseOnlyBodyBound; there is no subtree list to compose.
		return nil
	}

	chunkWidth := u.settings.BlockValidation.SubtreeBatchSize
	if chunkWidth < 1 {
		chunkWidth = 1
	}

	// The bracketed site prefix, pre-rendered once, so the shared model helpers emit
	// "[bindSubtreeBodyToHeader][<hash>] …" exactly as the messages written here do.
	label := "bindSubtreeBodyToHeader][" + block.Hash().String()

	// Retained across the whole block: one root per subtree, and the txid set. Nothing
	// else. The node lists themselves live only for their own chunk.
	//
	// The deduper is created once the first subtree fixes targetLength, so it can be
	// sized for the whole block; see bindDeduperHintCap.
	roots := make([]chainhash.Hash, numSubtrees)

	var deduper *model.SubtreeTxDeduper

	// The first subtree fixes the body's target shape, and chunk 0 is read first, so
	// both are known before any later chunk needs them.
	var targetLength, targetHeight int

	// Accumulated across ALL chunks rather than returned from the first failing one.
	// This pass is the only place that can name EVERY mismatching blob for the
	// quarantine: stopping at the first would leave a second forged blob on disk, the
	// attempt would be classified an ordinary local fault, and normal validation would
	// be handed that blob — whose loader checks only the .subtree header's claimed root
	// and so cannot detect it. That fall-through is what this pass exists to prevent
	// (bitcoin-sv/teranode#4838). The cost is reading every structure even once one is
	// known bad, bounded by the block's node hashes and never its transaction bodies.
	//
	// firstCheckErr is accumulated the same way and for the same reason. A composed
	// check — shape, duplicate scan, root — can only fail once enough of the body has
	// been read, and returning it the moment it is found would stop the walk with later
	// chunks unread and their forged blobs unnamed. Before the chunking that could not
	// happen: every subtree was read before any check ran. A read error still wins over
	// a check error at the end, because it is the one that carries the quarantine.
	//
	// The collector is function-scoped rather than per-chunk so there is no question
	// about which lock guards it; chunks are walked strictly in sequence, so it is only
	// ever contended within one chunk.
	//
	// The read failures are classified by subtreeReadVerdicts, one slot per class; see
	// its doc for why a single first-error slot would silently drop a class.
	var (
		verdicts      subtreeReadVerdicts
		firstCheckErr error
	)

	for chunkStart := 0; chunkStart < numSubtrees; chunkStart += chunkWidth {
		chunkEnd := chunkStart + chunkWidth
		if chunkEnd > numSubtrees {
			chunkEnd = numSubtrees
		}

		chunk := make([]*subtreepkg.Subtree, chunkEnd-chunkStart)

		// Deliberately a plain errgroup.Group on the caller's context, NOT
		// errgroup.WithContext: the first failing read must not cancel its siblings,
		// because a cancelled sibling returns a context error in place of its own anchor
		// verdict and its blob would go unnamed.
		//
		// Fan-out is capped by the chunk itself: reading 128 at a time out of a chunk of
		// 16 would put the residency straight back.
		var g errgroup.Group

		limit := chunkWidth
		if limit > 128 {
			limit = 128
		}

		util.SafeSetLimit(u.logger, &g, limit)

		for i := chunkStart; i < chunkEnd; i++ {
			local := i - chunkStart
			hash := block.Subtrees[i]

			g.Go(func() error {
				// Anchor only: this pass never reads a promoted blob's node list, so
				// readSubtreeStructure anchors it and releases it rather than handing it
				// back. The anchor itself must stay — with both file types present
				// findLocalSubtreeFile prefers ToCheck, so a forged promoted blob beside an
				// honest one is caught only here, before the pipeline.
				structure, err := u.readSubtreeStructure(ctx, block, hash, subtreeReadAnchorOnly, "binding")
				if err != nil {
					// Collected rather than taken from the group's single error: a
					// doctored body can name several mismatching blobs and every one of
					// them has to be quarantined, not just whichever failed first.
					verdicts.record(err)

					return err
				}

				chunk[local] = structure.subtree

				return nil
			})
		}

		chunkErr := g.Wait()

		// The chunk's node lists die here, before the next chunk is read. That is the
		// whole point of the restructuring, so the release must not be skipped on any
		// path out of the loop body — hence a closure with its own defer rather than a
		// release at each exit.
		checkErr := func() error {
			defer func() {
				for _, subtree := range chunk {
					releaseSubtreeStructure(subtree)
				}
			}()

			// Once anything has failed, later chunks are read and anchored ONLY: their
			// roots can never be composed into a verdict, and feeding a partial body to
			// the shape rules or the duplicate scan would produce a second, misleading
			// error. Reading them is not wasted work — it is what names their blobs for
			// the quarantine.
			if chunkErr != nil || verdicts.failed() || firstCheckErr != nil {
				return nil
			}

			for local, subtree := range chunk {
				idx := chunkStart + local

				if subtree == nil {
					return errors.NewProcessingError("[bindSubtreeBodyToHeader][%s] subtree %d of %d was released during validation", block.Hash().String(), idx, numSubtrees)
				}

				if idx == 0 {
					if len(subtree.Nodes) == 0 {
						return errors.NewBlockCorruptError("[bindSubtreeBodyToHeader][%s] first subtree has no nodes", block.Hash().String())
					}

					// The dedup scan below skips slot [0][0] only when it holds the coinbase
					// placeholder, so "first node is the placeholder" is its unstated
					// precondition, and Block.Valid enforces it as its own step 7. Without it
					// a first subtree whose node 0 is a real txid — reachable on a retry that
					// reuses a locally present .subtree blob — passes the merkle check with
					// the body's true first transaction silently substituted, and the scan
					// would then treat that txid as an ordinary node.
					if !subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholder) {
						return errors.NewBlockCorruptError("[bindSubtreeBodyToHeader][%s] first transaction in first subtree is not a coinbase placeholder: %s", block.Hash().String(), subtree.Nodes[0].Hash.String())
					}

					targetLength = subtree.Length()
					targetHeight = subtree.Height

					deduper = model.NewSubtreeTxDeduper(label, bindDeduperHint(numSubtrees, targetLength))
				}

				// A ONE-SUBTREE BLOCK IS EXEMPT from the shape rules, exactly as
				// Block.CheckMerkleRoot is: it returns the single subtree's root directly
				// from its len(hashes) == 1 branch, before the power-of-two guard and the
				// non-final/final length rules. A single subtree with a non-power-of-two
				// leaf count is a legitimate body today, so applying the guard here would
				// reject genuine blocks that the normal route accepts.
				if numSubtrees > 1 {
					if err := model.CheckSubtreeShape(label, idx, subtree.Length(), targetLength, idx == numSubtrees-1); err != nil {
						return err
					}
				}

				// CVE-2012-2459, and it is whole-block: the merkle root CANNOT detect a
				// duplicated trailing transaction, because the duplicate-last-node-when-odd
				// rule makes the mutated body produce the SAME root and so the same block
				// hash as the honest one. Only the txid SET has to span the block, not the
				// node lists, which is what makes the scan survive chunking at all. The
				// GLOBAL index is passed, so the placeholder skip stays pinned to block
				// position [0][0].
				//
				// Chunks run in order and this loop is skipped once anything has failed,
				// so idx 0 has always created the deduper by here; the nil check is
				// defence only.
				if deduper == nil {
					return errors.NewProcessingError("[bindSubtreeBodyToHeader][%s] duplicate scan reached subtree %d before the first subtree", block.Hash().String(), idx)
				}

				if err := deduper.Add(idx, subtree); err != nil {
					return err
				}

				switch {
				case idx == 0:
					// The coinbase substituted for the placeholder, computed while the
					// object is still alive — after this chunk there is no subtree left to
					// compute it from.
					root, err := subtree.RootHashWithReplaceRootNode(block.CoinbaseTx.TxIDChainHash(), 0, uint64(block.CoinbaseTx.Size())) // nolint: gosec
					if err != nil {
						return errors.NewProcessingError("[bindSubtreeBodyToHeader][%s] error replacing root node in subtree", block.Hash().String(), err)
					}

					roots[idx] = *root

				case idx == numSubtrees-1 && subtree.Length() < targetLength:
					// A short final subtree contributes its root LIFTED to the first
					// subtree's height, so it occupies the slot of a same-capacity subtree
					// in the top-level tree. Chunks are walked in order, so targetHeight is
					// already known.
					lifted, err := subtree.RootHashPadded(targetHeight)
					if err != nil {
						return errors.NewProcessingError("[bindSubtreeBodyToHeader][%s] failed lifting final subtree", block.Hash().String(), err)
					}

					roots[idx] = *lifted

				default:
					root := subtree.RootHash()
					if root == nil {
						return errors.NewProcessingError("[bindSubtreeBodyToHeader][%s] subtree %d returned nil root hash", block.Hash().String(), idx)
					}

					roots[idx] = *root
				}
			}

			return nil
		}()

		// Recorded, not returned: the remaining chunks still have to be read so their
		// blobs can be named. Before the chunking, a composed check could not even run
		// until every subtree had been read, so returning here would be a genuine
		// regression in quarantine completeness rather than a change of message.
		if checkErr != nil && firstCheckErr == nil {
			firstCheckErr = checkErr
		}
	}

	// A read error still wins over a check error, because it is the one that carries
	// the quarantine.
	if verdicts.failed() {
		return verdicts.err()
	}

	if firstCheckErr != nil {
		return firstCheckErr
	}

	// Compose and compare. For one subtree the composed root IS that subtree's
	// coinbase-substituted root, which is Block.CheckMerkleRoot's len(hashes) == 1
	// branch reproduced exactly.
	composed := &roots[0]

	if numSubtrees > 1 {
		var err error

		composed, err = model.ComposeSubtreeRootsToMerkleRoot(label, roots)
		if err != nil {
			if errors.IsBlockCorrupt(err) {
				return err
			}

			return errors.NewProcessingError("[bindSubtreeBodyToHeader][%s] merkle root composition failed", block.Hash().String(), err)
		}
	}

	if !block.Header.HashMerkleRoot.IsEqual(composed) {
		// The received body's subtrees do not hash to the header's merkle root: the body
		// is not bound to the header, so this cannot condemn the hash — classify corrupt
		// and re-download, never invalid=true (bitcoin-sv/teranode#4692).
		return errors.NewBlockCorruptError("[bindSubtreeBodyToHeader][%s] merkle root does not match", block.Hash().String())
	}

	// The coinbase shape, last, because the composition above is what makes the header
	// commit to this exact transaction — which is why an unshaped coinbase is genuine
	// invalidity rather than a corrupt download.
	//
	// IsConsensusCoinbase rather than go-bt's Tx.IsCoinbase: the latter is a disjunction
	// that also accepts a 0xFFFFFFFF sequence number in place of a null prevout index,
	// admitting a transaction svnode rejects.
	if !model.IsConsensusCoinbase(block.CoinbaseTx) {
		return errors.NewBlockInvalidError("[bindSubtreeBodyToHeader][%s] block coinbase tx is not a valid coinbase tx", block.Hash().String())
	}

	return nil
}

// bindDeduperHintCap caps the binding pass's duplicate-set capacity hint. The hint is
// the subtree count times the first subtree's length, both taken from the peer-supplied
// body, so it is capped rather than trusted; an understated hint only costs growth.
const bindDeduperHintCap = 1 << 20

// bindDeduperHint returns min(numSubtrees*targetLength, bindDeduperHintCap), clamping
// before the multiplication so it cannot overflow.
func bindDeduperHint(numSubtrees, targetLength int) int {
	if numSubtrees <= 0 || targetLength <= 0 {
		return 0
	}

	if targetLength > bindDeduperHintCap/numSubtrees {
		return bindDeduperHintCap
	}

	return numSubtrees * targetLength
}

// subtreeReadVerdicts collects the failures of a set of concurrent subtree reads so
// that every mismatching blob is named, not only whichever read failed first. The
// binding pass, the batch collector and the subtree_data sweep all record into one.
//
// firstMismatch and firstOtherErr are one slot PER CLASS rather than a single
// first-error slot, and that is the whole point rather than tidiness. A single slot is
// order-dependent: whichever class lost the race is recorded nowhere, so a mismatch
// arriving before an ErrNotFound discards the ErrNotFound. With one slot per class, the
// join in err() fires whenever both classes occurred, in either order.
type subtreeReadVerdicts struct {
	mu               sync.Mutex
	mismatches       []subtreeBlobRef
	firstMismatch    error
	firstOtherErr    error
	anyUnquarantined bool
}

// record classifies one read failure. Nil-safe and safe for concurrent use.
func (v *subtreeReadVerdicts) record(err error) {
	if err == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if refs := subtreeKeyMismatchRefs(err); len(refs) > 0 {
		v.mismatches = append(v.mismatches, refs...)

		if v.firstMismatch == nil {
			v.firstMismatch = err
		}

		// The FAIL-CLOSED verdict is aggregated separately from the refs, because only
		// one error survives as firstMismatch and it may not be the one that could not
		// audit its sibling.
		if isUnquarantinedLocalSubtree(err) {
			v.anyUnquarantined = true
		}

		return
	}

	if v.firstOtherErr == nil {
		v.firstOtherErr = err
	}
}

// failed reports whether any read failure was recorded.
func (v *subtreeReadVerdicts) failed() bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	return v.firstMismatch != nil || v.firstOtherErr != nil
}

// err returns the one error the recorded failures fold into, or nil when none was
// recorded. An anchor verdict is preferred over any other read failure because it is
// the error that carries the quarantine; the other failure is joined in rather than
// discarded.
func (v *subtreeReadVerdicts) err() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.firstMismatch != nil {
		return combineSweepMismatchError(v.firstMismatch, dedupeSubtreeBlobRefs(v.mismatches), v.anyUnquarantined, v.firstOtherErr)
	}

	return v.firstOtherErr
}

// sweepSubtreeDataMismatches names every forged subtree_data blob in the block once one
// has been found, so the quarantine removes all of them rather than only those in the
// batch that failed.
//
// The batch collector aggregates within one batch only. When a batch fails every
// driver stops, so a forged body in a later batch is never read; the binding pass reads
// structures only, so nothing else would name it. The first quarantine would then
// succeed, the attempt would fall through to normal validation as an ordinary local
// fault, and the surviving blob would stay on disk for the asset service to serve
// (bitcoin-sv/teranode#4838).
//
// It runs only when err already names a FileTypeSubtreeData blob, so the success path
// and ordinary failures pay nothing. It walks the block one SubtreeBatchSize chunk at a
// time, anchor-only, releasing every structure before the next chunk, so its residency
// is one batch, as in the pipeline. Each failure is classified:
//
//   - a key mismatch is merged into err's quarantine refs;
//   - a context or storage error prevents a verdict, so err is marked unquarantined and
//     the run aborts instead of falling through with a body nobody could audit;
//   - ErrNotFound is ignored only once an Exists probe confirms the body is absent,
//     because readSubtree wraps every open failure of the body as NotFound;
//   - any other body fault is logged and left in place, the same disposition the batch
//     path gives it.
//
// A non-mismatch failure never replaces the original verdict.
func (u *BlockValidation) sweepSubtreeDataMismatches(ctx context.Context, block *model.Block, err error) error {
	refs := subtreeKeyMismatchRefs(err)

	named := make(map[chainhash.Hash]struct{}, len(refs))
	anyData := false

	for _, ref := range refs {
		named[ref.hash] = struct{}{}

		if ref.fileType == fileformat.FileTypeSubtreeData {
			anyData = true
		}
	}

	// Structure mismatches are already swept whole-block by the binding pass.
	if !anyData {
		return err
	}

	numSubtrees := len(block.Subtrees)

	chunkWidth := u.settings.BlockValidation.SubtreeBatchSize
	if chunkWidth < 1 {
		chunkWidth = 1
	}

	var (
		verdicts    subtreeReadVerdicts
		unauditedMu sync.Mutex
		unaudited   bool
	)

	for chunkStart := 0; chunkStart < numSubtrees; chunkStart += chunkWidth {
		chunkEnd := chunkStart + chunkWidth
		if chunkEnd > numSubtrees {
			chunkEnd = numSubtrees
		}

		// A plain errgroup.Group, as in the binding pass: a failing read must not cancel
		// its siblings, or they return a context error in place of their own verdict.
		var g errgroup.Group

		limit := chunkWidth
		if limit > 128 {
			limit = 128
		}

		util.SafeSetLimit(u.logger, &g, limit)

		for i := chunkStart; i < chunkEnd; i++ {
			idx := i
			hash := block.Subtrees[i]

			if _, ok := named[*hash]; ok {
				continue
			}

			g.Go(func() error {
				result := u.readSubtree(ctx, block, idx, hash, subtreeReadAnchorOnly, "sweep")
				if result.err == nil {
					releaseSubtreeStructure(result.subtree)
					releaseSubtreeStructure(result.fullSubtree)

					return nil
				}

				switch {
				case len(subtreeKeyMismatchRefs(result.err)) > 0:
					verdicts.record(result.err)

				case u.sweepReadPreventsVerdict(ctx, hash, result.err):
					u.logger.Errorf("[sweepSubtreeDataMismatches][%s] subtree %s could not be audited, aborting: %v", block.Hash().String(), hash.String(), result.err)

					unauditedMu.Lock()
					unaudited = true
					unauditedMu.Unlock()

				case errors.Is(result.err, errors.ErrNotFound):
					// Confirmed absent: nothing is on disk to serve.

				default:
					u.logger.Warnf("[sweepSubtreeDataMismatches][%s] subtree %s has a body fault that is not a forgery, left in place: %v", block.Hash().String(), hash.String(), result.err)
				}

				return nil
			})
		}

		_ = g.Wait()
	}

	if swept := verdicts.err(); swept != nil {
		all := dedupeSubtreeBlobRefs(append(append([]subtreeBlobRef{}, refs...), subtreeKeyMismatchRefs(swept)...))
		err = markSubtreeKeyMismatch(err, all...)

		if isUnquarantinedLocalSubtree(swept) {
			err = markUnquarantinedLocalSubtree(err)
		}
	}

	if unaudited {
		err = markUnquarantinedLocalSubtree(err)
	}

	return err
}

// sweepReadPreventsVerdict reports whether a sweep read failure leaves a subtree_data
// body that exists but could not be judged.
//
// The context and storage classes are checked before ErrNotFound on purpose: readSubtree
// wraps every failure to open the body as NotFound, and errors.Is walks the wrapped
// chain, so a storage error beneath that wrapper is still caught here. Any other
// NotFound is confirmed with an Exists probe rather than trusted, because the same
// wrapper also covers a raw store error that is not classified as storage.
func (u *BlockValidation) sweepReadPreventsVerdict(ctx context.Context, hash *chainhash.Hash, readErr error) bool {
	if ctx.Err() != nil || errors.IsContextError(readErr) || errors.Is(readErr, errors.ErrStorageError) {
		return true
	}

	if !errors.Is(readErr, errors.ErrNotFound) {
		return false
	}

	exists, existsErr := u.subtreeStore.Exists(ctx, hash[:], fileformat.FileTypeSubtreeData)

	return existsErr != nil || exists
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
//
// otherErr is whichever read failed FIRST, mismatch or not, and it is joined in rather
// than discarded. The two can differ: a subtree can be missing or unreadable in the
// same pass in which a different one fails its anchor, and the anchor verdict is the
// one preferred as the outer error because it is what carries the quarantine. Dropping
// the other left an ErrNotFound invisible to both the log and any errors.Is
// classification downstream, so an attempt that was partly an infrastructure failure
// read as a pure blob forgery. The mismatch stays outermost, so nothing about the
// existing routing changes; the other becomes a reachable cause.
func combineSweepMismatchError(firstMismatch error, refs []subtreeBlobRef, anyUnquarantined bool, otherErr error) error {
	combined := markSubtreeKeyMismatch(firstMismatch, refs...)

	// Identity, not errors.Is. The collector now classifies its two error slots, so it
	// can no longer pass the same value twice; this stays as a guard so a future caller
	// that reintroduces a single first-error slot degrades to a no-op join rather than
	// wrapping an error in itself.
	if otherErr != nil && otherErr != firstMismatch {
		combined = errors.NewProcessingError("subtree key mismatch alongside an unrelated read failure", errors.Join(combined, otherErr))

		// Re-applied to the new outer error: the markers live in the wrapper's data, and
		// the walk that reads them stops at the first link that has them, so the
		// re-wrapped chain must carry them at the top.
		combined = markSubtreeKeyMismatch(combined, refs...)
	}

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
