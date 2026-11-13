//go:build race

package txmetacache

// raceEnabled reports whether the race detector is on; it adds allocations of
// its own, so allocation-count tests skip under it.
const raceEnabled = true
