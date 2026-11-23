package soak

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if a test leaves a goroutine running, which here means the Sampler's background loop
// outliving Stop, or the in-process leak test failing to release its deliberately blocked goroutines.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
