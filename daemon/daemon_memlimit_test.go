package daemon

import (
	"os"
	"runtime/debug"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

func TestParseCgroupMemoryValue(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    uint64
		wantErr bool
	}{
		{name: "valid limit", input: "10737418240", want: 10737418240},
		{name: "small limit", input: "1048576", want: 1048576},
		{name: "max means no limit", input: "max", wantErr: true},
		{name: "empty means no limit", input: "", wantErr: true},
		{name: "v1 no limit", input: "9223372036854771712", wantErr: true}, // page-aligned max int64
		{name: "invalid number", input: "abc", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCgroupMemoryValue(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{input: 1073741824, want: "1.0GiB"},
		{input: 10737418240, want: "10.0GiB"},
		{input: 536870912, want: "512MiB"},
		{input: 1048576, want: "1MiB"},
		{input: 1024, want: "1024B"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			require.Equal(t, tt.want, formatBytes(tt.input))
		})
	}
}

func TestComputeGOMEMLIMIT(t *testing.T) {
	const (
		ratio      = 0.9
		cache1GiB  = 1024 * bytesPerMiB
		txmetaHost = cache1GiB + offHeapHeadroomBytes // settings.conf's txMetaCacheMaxMB=1024
		gib        = float64(bytesPerGiB)
	)

	// The issue-2176 host's cap, which the startup log printed as 8.6GiB.
	capGiB := 8.6
	cap86GiB := uint64(capGiB * gib)

	tests := []struct {
		name        string
		limit       uint64
		reserve     uint64
		wantGiB     float64
		wantFloored bool
	}{
		// A process without the txmeta cache keeps ratio * limit.
		{name: "no reserve, 8.6GiB", limit: cap86GiB, reserve: 0, wantGiB: 7.74},
		// The caps quickstart and the issue-2176 host use, with a 1024MiB cache.
		{name: "3.2GB cap is floored", limit: 3_200_000_000, reserve: txmetaHost, wantGiB: 1.34, wantFloored: true},
		{name: "6.4GB cap", limit: 6_400_000_000, reserve: txmetaHost, wantGiB: 3.11},
		{name: "8.6GiB cap", limit: cap86GiB, reserve: txmetaHost, wantGiB: 5.49},
		{name: "25.6GB cap", limit: 25_600_000_000, reserve: txmetaHost, wantGiB: 19.21},
		// A reservation larger than the limit must not go negative.
		{name: "reserve above limit is floored", limit: 2 * bytesPerGiB, reserve: txmetaHost, wantGiB: 0.9, wantFloored: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, floored := computeGOMEMLIMIT(tt.limit, ratio, tt.reserve)
			require.InDelta(t, tt.wantGiB, got/gib, 0.005)
			require.Equal(t, tt.wantFloored, floored)
			require.Positive(t, got)
		})
	}
}

func TestOffHeapReserve(t *testing.T) {
	newSettings := func(enabled bool, bucketType string) *settings.Settings {
		return &settings.Settings{SubtreeValidation: settings.SubtreeValidationSettings{
			TxMetaCacheEnabled:    enabled,
			TxMetaCacheMaxMB:      1024,
			TxMetaCacheBucketType: bucketType,
		}}
	}

	withCache := uint64(1024*bytesPerMiB + offHeapHeadroomBytes)

	tests := []struct {
		name     string
		settings *settings.Settings
		args     []string
		want     uint64
	}{
		{name: "subtreevalidation with the default bucket type", settings: newSettings(true, "unallocated"), args: []string{"-subtreevalidation=1"}, want: withCache},
		{name: "subtreevalidation with the empty bucket type", settings: newSettings(true, ""), args: []string{"-subtreevalidation=1"}, want: withCache},
		{name: "lockfree is off-heap too", settings: newSettings(true, "lockfree"), args: []string{"-subtreevalidation=1"}, want: withCache},
		{name: "all-in-one process", settings: newSettings(true, "unallocated"), args: []string{"-subtreevalidation=1", "-validator=1", "-blockvalidation=1"}, want: withCache},
		{name: "subtreevalidation not started here", settings: newSettings(true, "unallocated"), args: []string{"-subtreevalidation=0"}, want: 0},
		{name: "only other services started here", settings: newSettings(true, "unallocated"), args: []string{"-all=0", "-blockvalidation=1"}, want: 0},
		{name: "cache disabled", settings: newSettings(false, "unallocated"), args: []string{"-subtreevalidation=1"}, want: 0},
		{name: "pointer cache lives on the Go heap", settings: newSettings(true, " Pointer "), args: []string{"-subtreevalidation=1"}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := offHeapReserve(tt.settings, tt.args)
			require.Equal(t, tt.want, got)

			if tt.want == 0 {
				require.Empty(t, reason)
			} else {
				require.Equal(t, "txmeta cache 1.0GiB + off-heap headroom 1.5GiB", reason)
			}
		})
	}
}

func TestConfigureGOMEMLIMIT(t *testing.T) {
	const limit = 8 * bytesPerGiB

	ratio := 0.9

	prevDetect := detectMemoryLimit
	detectMemoryLimit = func() (uint64, error) { return limit, nil }

	prevLimit := debug.SetMemoryLimit(-1)

	t.Cleanup(func() {
		detectMemoryLimit = prevDetect
		debug.SetMemoryLimit(prevLimit)
	})

	unsetGOMEMLIMIT := func(t *testing.T) {
		t.Helper()
		t.Setenv("GOMEMLIMIT", "") // restores the original value after the test
		require.NoError(t, os.Unsetenv("GOMEMLIMIT"))
	}

	t.Run("reserve is taken off before the ratio", func(t *testing.T) {
		unsetGOMEMLIMIT(t)

		configureGOMEMLIMIT(ratio, 2*bytesPerGiB, "test")
		require.Equal(t, int64(ratio*float64(6*bytesPerGiB)), debug.SetMemoryLimit(-1))
	})

	t.Run("no reserve keeps ratio times limit", func(t *testing.T) {
		unsetGOMEMLIMIT(t)

		configureGOMEMLIMIT(ratio, 0, "")
		require.Equal(t, int64(ratio*float64(limit)), debug.SetMemoryLimit(-1))
	})

	t.Run("GOMEMLIMIT from the environment wins", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "3GiB")

		before := debug.SetMemoryLimit(123 * bytesPerMiB)
		t.Cleanup(func() { debug.SetMemoryLimit(before) })

		configureGOMEMLIMIT(ratio, 2*bytesPerGiB, "test")
		require.Equal(t, int64(123*bytesPerMiB), debug.SetMemoryLimit(-1))
	})
}
