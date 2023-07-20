package util

import (
	"os"
	"testing"
)

func SkipLongTests(t *testing.T) {
	if os.Getenv("LONG_TESTS") == "" {
		t.Skip("Skipping long running tests. Set LONG_TESTS=1 to run them.")
	}
}
