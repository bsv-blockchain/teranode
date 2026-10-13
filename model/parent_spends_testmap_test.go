package model

import (
	"sync"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// testParentSpendsMap is a plain mutex-guarded Go map implementing
// ParentSpendsMap: the reference model the streaming check is tested against.
type testParentSpendsMap struct {
	mu sync.Mutex
	m  map[subtreepkg.Inpoint]struct{}
}

var _ ParentSpendsMap = (*testParentSpendsMap)(nil)

// newTestParentSpendsMap ignores its arguments; they only keep the call sites
// of the tests that used a bucketed map readable.
func newTestParentSpendsMap(_ uint16, _ ...uint64) *testParentSpendsMap {
	return &testParentSpendsMap{m: make(map[subtreepkg.Inpoint]struct{})}
}

func (t *testParentSpendsMap) SetIfNotExists(inpoint subtreepkg.Inpoint) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.m[inpoint]; ok {
		return false, nil
	}

	t.m[inpoint] = struct{}{}

	return true, nil
}
