package util

import "golang.org/x/sync/errgroup"

func SafeSetLimit(g *errgroup.Group, limit int) {
	if limit == 0 {
		panic("limit cannot be 0")
	}

	g.SetLimit(limit)
}
