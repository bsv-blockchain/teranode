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

func SkipVeryLongTests(t *testing.T) {
	if os.Getenv("VERY_LONG_TESTS") == "" {
		t.Skip("Skipping very long running tests. Set LONG_TESTS=1 to run them.")
	}
}
