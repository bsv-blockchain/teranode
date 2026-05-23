// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

import "time"

// clock supplies wall time to code paths that need a deterministic substitute
// in tests. The production implementation is realClock; tests install a fake
// to drive timestamps without touching wall time.
type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
