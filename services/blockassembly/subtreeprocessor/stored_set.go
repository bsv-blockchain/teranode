package subtreeprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// StoredSet is the set of subtree root hashes whose files are in the subtree store (and the meta
// file, when one is written), shared between the storage side, which adds a root when a store ends
// with the subtree stored, and the subtree processor, which cuts a mining template at the first
// chained subtree that is not in it (blockassembly_storedPrefixTemplate, issue 2269).
//
// It is keyed by root hash, so a late completion for a subtree that has left the chain matches
// nothing and a rechain's new roots are simply not in it yet. Workers write it directly: nothing
// goes through the processor's select loop, where a blocking completion message could deadlock.
type StoredSet struct {
	mu       sync.RWMutex
	roots    map[chainhash.Hash]struct{}
	failed   map[chainhash.Hash]struct{} // stores that ended failed for good, until Retain or a later success
	added    uint64                      // roots ever added: the stored count the progress rule watches
	retrying map[chainhash.Hash]int      // per root: requests with a write in the retry processor, not yet settled

	// version changes whenever a root is added or dropped, so a reader can tell that a prefix it
	// computed is still current without taking the lock.
	version atomic.Uint64
}

// NewStoredSet returns an empty set.
func NewStoredSet() *StoredSet {
	return &StoredSet{roots: make(map[chainhash.Hash]struct{}), failed: make(map[chainhash.Hash]struct{}), retrying: make(map[chainhash.Hash]int)}
}

// Add records root as stored.
func (s *StoredSet) Add(root chainhash.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.addLocked(root)
}

func (s *StoredSet) addLocked(root chainhash.Hash) {
	if _, ok := s.roots[root]; !ok {
		s.roots[root] = struct{}{}
		s.added++

		s.version.Add(1)
	}

	delete(s.failed, root)
}

// Has reports whether root is stored.
func (s *StoredSet) Has(root chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.roots[root]

	return ok
}

// PrefixLen is the number of leading subtrees that are all stored, under one lock.
func (s *StoredSet) PrefixLen(subtrees []*subtreepkg.Subtree) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i, st := range subtrees {
		if _, ok := s.roots[*st.RootHash()]; !ok {
			return i
		}
	}

	return len(subtrees)
}

// RetryStarted notes that a request for root handed a write to the retry processor. Settle must
// follow exactly once, through Finish with retried true.
func (s *StoredSet) RetryStarted(root chainhash.Hash) {
	s.mu.Lock()
	s.retrying[root]++
	s.mu.Unlock()
}

// Finish settles one request: its root is added when the result says everything the store has to
// write is stored, and noted as failed when it ended failed (not cancelled).
func (s *StoredSet) Finish(root chainhash.Hash, res StoreResult, retried bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if retried {
		if s.retrying[root]--; s.retrying[root] <= 0 {
			delete(s.retrying, root)
		}
	}

	switch {
	case res.OK():
		s.addLocked(root)
	case res.Err != nil && !res.Cancelled:
		s.failed[root] = struct{}{}
	}
}

// Added is the number of roots ever added.
func (s *StoredSet) Added() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.added
}

// Retrying reports whether a request for root has a write in the retry processor.
func (s *StoredSet) Retrying(root chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.retrying[root] > 0
}

// ClearFailed forgets that root's store failed, when it is sent again: a new failure notes it again.
func (s *StoredSet) ClearFailed(root chainhash.Hash) {
	s.mu.Lock()
	delete(s.failed, root)
	s.mu.Unlock()
}

// HasFailed reports whether root's store ended failed for good and has not been stored since.
func (s *StoredSet) HasFailed(root chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.failed[root]

	return ok
}

// Retain drops every root and failure note not in keep: the roots of the chain, so the set cannot
// outgrow it.
func (s *StoredSet) Retain(keep map[chainhash.Hash]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for root := range s.roots {
		if _, ok := keep[root]; !ok {
			delete(s.roots, root)
			s.version.Add(1)
		}
	}

	for root := range s.failed {
		if _, ok := keep[root]; !ok {
			delete(s.failed, root)
		}
	}
}

// Version changes whenever a root is added or dropped.
func (s *StoredSet) Version() uint64 { return s.version.Load() }
