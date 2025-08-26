package util

import (
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestSafeSetLimit(t *testing.T) {
	t.Run("valid positive limit", func(t *testing.T) {
		g := &errgroup.Group{}

		// Should not panic
		SafeSetLimit(g, 1)
		SafeSetLimit(g, 10)
		SafeSetLimit(g, 100)
	})

	t.Run("zero limit panics", func(t *testing.T) {
		g := &errgroup.Group{}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("SafeSetLimit(g, 0) did not panic")
			} else if r != "limit cannot be 0" {
				t.Errorf("SafeSetLimit(g, 0) panicked with wrong message: %v", r)
			}
		}()

		SafeSetLimit(g, 0)
	})

	t.Run("negative limit", func(t *testing.T) {
		g := &errgroup.Group{}

		// Should not panic from SafeSetLimit, but errgroup.SetLimit might
		// We'll let errgroup handle negative values as per its own behavior
		SafeSetLimit(g, -1)
	})
}
