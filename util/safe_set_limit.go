package util

import "golang.org/x/sync/errgroup"

// SafeSetLimit safely sets the limit on an errgroup.Group, ensuring the limit
// is not zero to prevent a panic. It panics if the limit is 0, which would
// indicate a programming error as errgroup.SetLimit(0) panics.
//
// Parameters:
//   - g: The errgroup.Group to set the limit on
//   - limit: The maximum number of goroutines that can be active at once (must be > 0)
//
// Panics:
//   - If limit is 0, as this would cause errgroup.SetLimit to panic
func SafeSetLimit(g *errgroup.Group, limit int) {
	if limit == 0 {
		panic("limit cannot be 0")
	}

	g.SetLimit(limit)
}
