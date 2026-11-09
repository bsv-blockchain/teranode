package utxo

// LanePositionSource hands out lane positions (util/lane), one per record. Next reports false when
// it has none to give, and the record is then created without one. *lane.Allocator implements it.
type LanePositionSource interface {
	Next() (uint64, bool)

	// Prime does the one wait of a process, for its first positions, so a later Next cannot. A store
	// that takes positions with a database transaction open primes before it opens one.
	Prime()
}

// LanePositionTaker is a store that gives each record it creates a position from a source. A
// store's create path calls this once per record, so taking a position costs a create one call to
// Next. The store also writes the position, and that cost is the store's. There is no wrapper
// around the store, because a wrapper has to scan and extend the options on every create, and it
// hides what the wrapped store can do.
//
// Find it with As, since the store may sit inside a logging wrapper. Set the source before the
// first create, because it is not guarded against a concurrent create.
type LanePositionTaker interface {
	SetLanePositionSource(source LanePositionSource)
}

// LanePositions is the part of a store that implements LanePositionTaker. With no source set, which
// is every store with lane positions off, a create pays one nil check.
type LanePositions struct {
	source LanePositionSource
}

// SetLanePositionSource implements LanePositionTaker.
func (l *LanePositions) SetLanePositionSource(source LanePositionSource) {
	l.source = source
}

// PrimeLanePositions lets the source do its one wait now, before the caller holds anything.
func (l *LanePositions) PrimeLanePositions() {
	if l.source != nil {
		l.source.Prime()
	}
}

// Take returns the position for the record that o describes: the caller's own if it named one,
// otherwise the source's next, otherwise zero, which the store writes as no lane position.
func (l *LanePositions) Take(o *CreateOptions) uint64 {
	if o.LanePosition != 0 || l.source == nil {
		return o.LanePosition
	}

	position, _ := l.source.Next()

	return position
}
