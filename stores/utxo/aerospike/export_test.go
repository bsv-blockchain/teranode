package aerospike

// ConflictingIndexVerdict reports whether the conflicting index self-test has
// finished and, if it has, whether it trusted the index.
func (s *Store) ConflictingIndexVerdict() (done, usable bool) {
	switch s.conflictingIndexState.Load() {
	case conflictingIndexUsable:
		return true, true
	case conflictingIndexUnusable:
		return true, false
	default:
		return false, false
	}
}

// StopConflictingIndexSelfTest stops the background self-test, for tests that
// never close their store.
func (s *Store) StopConflictingIndexSelfTest() {
	s.stopConflictingIndexSelfTest()
}
