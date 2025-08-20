package testhelpers

import (
	"context"
	"testing"
	"time"
)

// CreateTestContext creates a context with timeout for tests
func CreateTestContext(t *testing.T, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(func() {
		cancel()
	})
	return ctx, cancel
}
