package netsync

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
)

// subtreeAdoptions keeps a converting block's cleanup from removing a subtree file that another
// block relies on.
//
// Subtree files are content-addressed, so two blocks that carry an identical run of
// transactions name the same file: the first to publish it is its creator, and every later
// writer finds it there and adopts it. Conversions of different blocks run at the same time
// (the admission budget is MaxBlocksInTransitPerPeer times an allowance, not one), so a
// creator's conversion can fail after another block adopted the file, even after that block
// committed and the block persister kept the file. The creator's cleanup used to delete it
// regardless, leaving the adopter's record, or a committed block, naming subtree data that was
// gone.
//
// Only a creator deletes a subtree file (one exception, the record-read fallback, is set out
// below), and only while its own conversion is in flight:
// subtreeWriter.DeleteAll runs inside pipelineBlockSink, and pipelineBlockDelete runs in the
// same wire read, straight after the sink. So a creator holds its files "live" from the moment
// it publishes them until its writer finishes, and an adoption needs protecting only while some
// writer of the file could still delete it. Three counts per file say so, all under mu:
//
//   - pending: writers trying to write the file right now. Each writer counts itself before it
//     tries, so a delete and an attempt are always ordered: either the delete is decided first,
//     the attempt waits for it to end (deleting), and the writer finds no file and creates its
//     own, or the attempt is counted first and the delete is skipped.
//   - live: creators whose conversion has not finished.
//   - held: writers that adopted the file while another writer of it was pending or live.
//
// The invariant, held under mu: a writer that adopts a file takes a hold whenever any other
// writer of it is unresolved (pending, so possibly about to be told it created the file, or
// live), and every hold on a file stands until no writer of it is pending or live. A delete is
// skipped while pending or held is above zero.
//
// Per writer and file, the transitions are:
//
//	attempt   none -> pending                         (before the write)
//	created   pending -> live                         (its publish succeeded)
//	adopted   pending -> holding, if pending or live is still above zero once it leaves
//	          pending -> none, otherwise
//	abandoned pending -> none                         (its write failed)
//	finished  live -> none                            (its conversion is over, no delete follows)
//	release   holding -> none                         (it failed, its record is never read)
//
// The creator's publish makes the file visible before created runs, and an adopter can meet the
// file in that gap. The creator is still pending then, which is why pending counts as a writer
// that could delete. A hold that counted only live creators missed that gap, and the creator's
// failure cleanup deleted a file the adopter's record named (external review of e4698b219).
//
// When no writer of a file is pending or live, nobody is left who could delete it, so its
// entry, holds included, is dropped; tidyLocked is the only place that ends a hold other than
// release. A later write of the same file starts a new entry, and a hold remembers the entry it
// was taken on (subtreeHold), so a release that arrives after its entry was dropped is a no-op
// rather than taking a hold someone else took on the new entry.
//
// The registry is in memory and dies with the process. Restart recovery needs nothing from it:
// after a restart no conversion is in flight, so no creator exists who could delete, and the
// files are left to their delete-at-height. The one delete that is not a creator's is the
// record-read fallback in pipelineBlockDelete, taken only when no writer waits for the copy
// (settleStale finished it after settleBackstop); it skips pending and held files like the
// rest, but an adopter of a finished creator holds nothing, so it can remove a file another
// block adopted. The wire layer settles a copy in the same read that converted it, so that path
// needs a verdict delayed past a minute.
//
// The registry therefore holds only the files of conversions in flight, however many copies of
// a block convert and whatever happens to them afterwards.
type subtreeAdoptions struct {
	mu    sync.Mutex
	files map[writtenSubtree]*subtreeFileCounts
}

type subtreeFileCounts struct {
	pending int
	live    int
	held    int

	// deleting is closed when a delete of the file in flight outside mu ends; nil when none
	// is. An attempt waits for it, so a writer never finds the file in the unlink window and
	// adopts a file that is about to go (deleteUnlessAdopted).
	deleting chan struct{}
}

// subtreeHold is one hold a writer took as an adopter: the file, and the entry the hold is
// counted on.
type subtreeHold struct {
	key   writtenSubtree
	entry *subtreeFileCounts
}

func newSubtreeAdoptions() *subtreeAdoptions {
	return &subtreeAdoptions{files: make(map[writtenSubtree]*subtreeFileCounts)}
}

// countsLocked returns key's counts, creating them. The caller holds mu.
func (a *subtreeAdoptions) countsLocked(key writtenSubtree) *subtreeFileCounts {
	c, ok := a.files[key]
	if !ok {
		c = &subtreeFileCounts{}
		a.files[key] = c
	}

	return c
}

// tidyLocked drops key's entry, and with it every hold on it, once no writer of the file is
// pending or live: nobody is left who could delete it. The caller holds mu.
func (a *subtreeAdoptions) tidyLocked(key writtenSubtree, c *subtreeFileCounts) {
	if c.pending == 0 && c.live == 0 && c.deleting == nil {
		delete(a.files, key)
	}
}

// attempt records that a writer is about to try to write key. While a delete of key is in
// flight it waits for the delete to end first, so the write and the delete stay ordered as they
// were when the delete ran under mu.
func (a *subtreeAdoptions) attempt(key writtenSubtree) {
	for {
		a.mu.Lock()

		c := a.countsLocked(key)
		if c.deleting == nil {
			c.pending++
			a.mu.Unlock()

			return
		}

		done := c.deleting
		a.mu.Unlock()

		<-done
	}
}

// abandoned ends an attempt that neither created nor adopted key: the write failed.
func (a *subtreeAdoptions) abandoned(key writtenSubtree) {
	a.mu.Lock()
	defer a.mu.Unlock()

	c := a.countsLocked(key)
	c.pending--
	a.tidyLocked(key, c)
}

// created ends an attempt that published key: the writer is now a live creator of it.
func (a *subtreeAdoptions) created(key writtenSubtree) {
	a.mu.Lock()
	defer a.mu.Unlock()

	c := a.countsLocked(key)
	c.pending--
	c.live++
}

// adopted ends an attempt that found key already published, and reports whether the writer now
// holds it and on which entry. It holds whenever another writer of key is still pending or
// live, because either could yet delete it: a live creator in its cleanup, and a pending writer
// may be the creator whose publish succeeded and which has not said so yet.
func (a *subtreeAdoptions) adopted(key writtenSubtree) (*subtreeFileCounts, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	c := a.countsLocked(key)
	c.pending--

	if c.pending > 0 || c.live > 0 {
		c.held++

		return c, true
	}

	a.tidyLocked(key, c)

	return nil, false
}

// release gives back a hold a failed adopter took on entry. A hold whose entry has since been
// dropped is already gone, and releasing it is a no-op, even if the file has a new entry now.
func (a *subtreeAdoptions) release(key writtenSubtree, entry *subtreeFileCounts) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if c, ok := a.files[key]; ok && c == entry && c.held > 0 {
		c.held--
	}
}

// finished closes one creator's window on key.
func (a *subtreeAdoptions) finished(key writtenSubtree) {
	a.mu.Lock()
	defer a.mu.Unlock()

	c, ok := a.files[key]
	if !ok {
		return
	}

	if c.live > 0 {
		c.live--
	}

	a.tidyLocked(key, c)
}

// deleteUnlessAdopted deletes key from store unless a writer is trying to write it or holds it,
// and reports whether it was left.
//
// The check is made under mu and the delete runs outside it. store.Del can wait up to 25 s for a
// file-store write permit, and holding mu across that stalled every writer of every file, and
// through finish every read loop's settle (review of 76be891d6). The file is marked deleting
// under mu instead: attempt waits for the mark, so no writer can start on the file, find it
// still there and adopt it, until the delete has ended. A writer counted before the check keeps
// the file, as before. A second delete of the same file waits for the first and reports it
// deleted.
func (a *subtreeAdoptions) deleteUnlessAdopted(ctx context.Context, store blob.Store, key writtenSubtree) (bool, error) {
	a.mu.Lock()

	c := a.countsLocked(key)

	if c.pending > 0 || c.held > 0 {
		a.mu.Unlock()

		return true, nil
	}

	if c.deleting != nil {
		done := c.deleting
		a.mu.Unlock()

		<-done

		return false, nil
	}

	done := make(chan struct{})
	c.deleting = done
	a.mu.Unlock()

	// Deferred so a Del that panics still ends the delete: left marked deleting, the
	// key holds every later attempt on it for good.
	defer func() {
		a.mu.Lock()
		c.deleting = nil
		close(done)
		a.tidyLocked(key, c)
		a.mu.Unlock()
	}()

	return false, store.Del(ctx, key.Hash[:], key.FileType)
}

// size is the number of files the registry is tracking, for tests.
func (a *subtreeAdoptions) size() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return len(a.files)
}

// settleBackstop is how long a converted copy's writer may wait to be settled before the park
// sweep settles it. The wire layer settles a copy in the same read that converted it, so any
// writer still waiting this long was settled by a message that never arrived.
const settleBackstop = time.Minute

// settlingWriter is a converted copy's writer waiting for the wire layer's verdict on the copy.
type settlingWriter struct {
	writer *subtreeWriter
	since  time.Time
}

// holdUntilSettled keeps a converted copy's writer, and so its window as a creator, open past
// the sink. The wire layer can still refuse the copy after the sink succeeded, and its
// cleanup, pipelineBlockDelete, must then delete what this copy created while holds other
// blocks took on those files still stand.
//
// Writers wait in a queue per block hash, because neither verdict carries anything that names
// the delivery: pipelineBlockDelete gets the hash, QueueBlockOnDisk the hash and the peer. One
// writer is the usual case. A second copy of the same block can convert as soon as the first
// copy's sink has returned and before its verdict arrives (endConversion runs at the end of
// the sink, and the gate admits the hash until the park takes the block), so two can wait at
// once. A verdict then cannot be told apart from the other copy's, and the two settle paths
// treat that as such (takeSettling).
func (sm *SyncManager) holdUntilSettled(hash chainhash.Hash, w *subtreeWriter) {
	sm.settlingMu.Lock()
	defer sm.settlingMu.Unlock()

	if sm.settling == nil {
		sm.settling = make(map[chainhash.Hash][]settlingWriter)
	}

	sm.settling[hash] = append(sm.settling[hash], settlingWriter{writer: w, since: time.Now()})
}

// takeSettling removes and returns the writer waiting under hash when there is exactly one.
// With more than one waiting it removes nothing and reports the hash ambiguous: a refusal
// cannot know which copy it is about.
func (sm *SyncManager) takeSettling(hash chainhash.Hash) (w *subtreeWriter, ambiguous bool) {
	sm.settlingMu.Lock()
	defer sm.settlingMu.Unlock()

	waiting := sm.settling[hash]

	switch len(waiting) {
	case 0:
		return nil, false
	case 1:
		delete(sm.settling, hash)

		return waiting[0].writer, false
	default:
		return nil, true
	}
}

// settleAccepted closes the window of a copy the wire layer accepted. With more than one copy
// waiting it closes the oldest: verdicts on one peer's copies come in the order they were read,
// and if it is the other copy's window that closes early, the cost is only that copy's
// refusal, which RequireEnd keeps from happening, losing the holds it would have kept.
//
// queued says the accepted copy is about to be sent to the consumer. Until handleBlockOnDiskMsg
// has run, the park does not know the block, yet the record under the hash is the accepted
// copy's, so a refusal of another copy that lands in between must not delete it. The mark is
// taken in the same critical section that takes the writer, so no refusal can fall between the
// two. It is cleared by acceptanceHandled and by nothing else: the consumer handles every
// queued message, so a mark outlives its message only while the manager is stopping, when
// nothing deletes anyway.
func (sm *SyncManager) settleAccepted(hash chainhash.Hash, queued bool) {
	sm.settlingMu.Lock()

	if queued {
		if sm.accepting == nil {
			sm.accepting = make(map[chainhash.Hash]int)
		}

		sm.accepting[hash]++
	}

	waiting := sm.settling[hash]

	var oldest *subtreeWriter

	switch len(waiting) {
	case 0:
	case 1:
		oldest = waiting[0].writer

		delete(sm.settling, hash)
	default:
		oldest = waiting[0].writer
		sm.settling[hash] = waiting[1:]
	}

	sm.settlingMu.Unlock()

	if oldest != nil {
		oldest.finish()
	}
}

// acceptanceHandled clears one settleAccepted mark once handleBlockOnDiskMsg is done with it.
func (sm *SyncManager) acceptanceHandled(hash chainhash.Hash) {
	sm.settlingMu.Lock()
	defer sm.settlingMu.Unlock()

	if n := sm.accepting[hash]; n > 1 {
		sm.accepting[hash] = n - 1
	} else {
		delete(sm.accepting, hash)
	}
}

// acceptancePending reports whether an accepted copy of hash is queued and not yet handled.
func (sm *SyncManager) acceptancePending(hash chainhash.Hash) bool {
	sm.settlingMu.Lock()
	defer sm.settlingMu.Unlock()

	return sm.accepting[hash] > 0
}

// settleStale finishes every writer that has waited longer than settleBackstop.
//
// The writers are taken out under settlingMu and finished after it is released, as
// settleAccepted does: finish takes the adoption registry's lock, and holding settlingMu across
// it made every read loop's settle wait on whatever held the registry (review of 76be891d6).
func (sm *SyncManager) settleStale(now time.Time) {
	var stale []*subtreeWriter

	sm.settlingMu.Lock()

	for hash, waiting := range sm.settling {
		kept := waiting[:0]

		for _, s := range waiting {
			if now.Sub(s.since) >= settleBackstop {
				stale = append(stale, s.writer)
			} else {
				kept = append(kept, s)
			}
		}

		if len(kept) == 0 {
			delete(sm.settling, hash)
		} else {
			sm.settling[hash] = kept
		}
	}

	sm.settlingMu.Unlock()

	for _, w := range stale {
		w.finish()
	}
}

// subtreeAdoptionsFor returns the manager's registry, creating it on first use. Lazy so a
// SyncManager built as a literal, as most tests do, still shares one registry across every
// writer it makes.
func (sm *SyncManager) subtreeAdoptionsFor() *subtreeAdoptions {
	if a := sm.adoptions.Load(); a != nil {
		return a
	}

	sm.adoptions.CompareAndSwap(nil, newSubtreeAdoptions())

	return sm.adoptions.Load()
}

// deleteSubtreeFileUnlessAdopted is pipelineBlockDelete's delete of one subtree file: a file a
// writer is writing or holds is left to its delete-at-height. A file that is not there counts
// as deleted.
func (sm *SyncManager) deleteSubtreeFileUnlessAdopted(root chainhash.Hash, ft fileformat.FileType) error {
	_, err := sm.subtreeAdoptionsFor().deleteUnlessAdopted(sm.ctx, sm.subtreeStore, writtenSubtree{Hash: root, FileType: ft})
	if err != nil && errors.Is(err, errors.ErrNotFound) {
		return nil
	}

	return err
}
