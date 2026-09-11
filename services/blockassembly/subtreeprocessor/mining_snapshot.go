package subtreeprocessor

import (
	"sync"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// MiningSnapshotLease keeps mmap storage alive while a candidate or submission
// reads it. Release must be called when the owner finishes using the snapshot.
// A nil lease represents heap-backed data and needs no cleanup.
type MiningSnapshotLease struct {
	storage  *miningSnapshotStorage
	subtrees []*subtreepkg.Subtree
	released bool // guarded by storage.mu
}

type miningSnapshotStorage struct {
	mu             sync.Mutex
	refs           map[*subtreepkg.Subtree]int
	retired        map[*subtreepkg.Subtree]bool
	storagePending int
	storageDone    chan struct{}
}

func (s *miningSnapshotStorage) retainLocked(subtrees []*subtreepkg.Subtree) *MiningSnapshotLease {
	var mapped []*subtreepkg.Subtree
	for _, st := range subtrees {
		if st.IsMmapBacked() {
			mapped = append(mapped, st)
		}
	}
	if len(mapped) == 0 {
		return nil
	}
	if s.refs == nil {
		s.refs = make(map[*subtreepkg.Subtree]int)
	}
	for _, st := range mapped {
		s.refs[st]++
	}
	return &MiningSnapshotLease{storage: s, subtrees: mapped}
}

// Retain acquires independent ownership for an in-flight request. It fails if
// eviction already released this lease; callers must not read the snapshot then.
func (l *MiningSnapshotLease) Retain() (*MiningSnapshotLease, bool) {
	if l == nil {
		return nil, true
	}
	l.storage.mu.Lock()
	defer l.storage.mu.Unlock()
	if l.released {
		return nil, false
	}
	return l.storage.retainLocked(l.subtrees), true
}

// Release is idempotent. A retired mapping is closed after its final reader exits.
func (l *MiningSnapshotLease) Release() {
	if l == nil {
		return
	}
	s := l.storage
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	for _, st := range l.subtrees {
		s.refs[st]--
		if s.refs[st] == 0 {
			delete(s.refs, st)
			if s.retired[st] {
				delete(s.retired, st)
				_ = st.Close()
			}
		}
	}
	l.subtrees = nil
}

// closeMiningSubtree removes the processor's ownership. Publication and lease
// acquisition share this lock so a reader cannot acquire an unmapped snapshot.
func (stp *SubtreeProcessor) closeMiningSubtree(st *subtreepkg.Subtree) {
	s := &stp.miningSnapshots
	s.mu.Lock()
	defer s.mu.Unlock()
	if data := stp.precomputedMiningData.Load(); data != nil {
		for _, published := range data.Subtrees {
			if published == st {
				stp.precomputedMiningData.Store(nil)
				break
			}
		}
	}
	if s.refs[st] > 0 {
		if s.retired == nil {
			s.retired = make(map[*subtreepkg.Subtree]bool)
		}
		s.retired[st] = true
		return
	}
	_ = st.Close()
}
