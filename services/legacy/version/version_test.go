package version

import (
	"testing"

	"github.com/ordishs/gocore"
)

func TestString(t *testing.T) {
	testCases := []struct {
		name     string
		version  string
		expected string
	}{
		{
			name:     "git tag version",
			version:  "v1.2.3",
			expected: "1.2.3",
		},
		{
			name:     "dev version with timestamp",
			version:  "v0.0.0-20260407-abc1234",
			expected: "0.0.0-20260407-abc1234",
		},
		{
			name:     "no v prefix",
			version:  "1.0.0",
			expected: "1.0.0",
		},
		{
			name:     "empty version",
			version:  "",
			expected: "",
		},
	}

	for _, tc := range testCases {
		gocore.SetInfo("test", tc.version, "deadbeef")

		v := String()
		if v != tc.expected {
			t.Fatalf("%s: expected %q, got %q", tc.name, tc.expected, v)
		}
	}
}
