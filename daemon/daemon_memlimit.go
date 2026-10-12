package daemon

import (
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
)

// configureGCTuning sets GOMEMLIMIT and GOGC based on the container's cgroup
// memory limit and the GCTuning settings. This should be called early in
// startup, before services begin allocating significant memory.
//
// When enabled, it:
//  1. Detects the cgroup memory limit (v2 then v1 fallback)
//  2. Sets GOMEMLIMIT to ratio * limit (default 90%)
//  3. Sets GOGC to the configured target (default 100)
//
// If GOMEMLIMIT or GOGC are already set via environment variables, those
// values are respected and not overridden.
func ConfigureGCTuning(gcSettings settings.GCTuningSettings) {
	if !gcSettings.Enabled {
		fmt.Println("GC tuning: disabled by setting gc_tuning_enabled=false")
		return
	}

	// Validate ratio
	if gcSettings.Ratio <= 0 || gcSettings.Ratio > 1.0 {
		fmt.Printf("GC tuning: invalid gc_tuning_ratio=%f (must be in (0.0, 1.0]), skipping\n", gcSettings.Ratio)
		return
	}

	configureGOGC(gcSettings.GCTarget)
	configureGOMEMLIMIT(gcSettings.Ratio)
}

// configureGOGC sets the GOGC value if not already set via environment variable.
func configureGOGC(target int) {
	if val, ok := os.LookupEnv("GOGC"); ok {
		fmt.Printf("GC tuning: GOGC already set by environment to %s, not overriding\n", val)
		return
	}

	if target < 0 {
		fmt.Printf("GC tuning: invalid gc_tuning_gogc=%d (must be >= 0), using default 100\n", target)
		target = 100
	}

	prev := debug.SetGCPercent(target)
	fmt.Printf("GC tuning: GOGC set to %d (was %d)\n", target, prev)
}

// configureGOMEMLIMIT detects the cgroup memory limit and sets GOMEMLIMIT.
func configureGOMEMLIMIT(ratio float64) {
	if val, ok := os.LookupEnv("GOMEMLIMIT"); ok {
		fmt.Printf("GC tuning: GOMEMLIMIT already set by environment to %s, not overriding\n", val)
		return
	}

	limit, err := detectCgroupMemoryLimit()
	if err != nil {
		fmt.Printf("GC tuning: could not detect cgroup memory limit: %v, GOMEMLIMIT not set\n", err)
		return
	}

	memlimitFloat := float64(limit) * ratio
	if memlimitFloat <= 0 || memlimitFloat > float64(math.MaxInt64) {
		fmt.Printf("GC tuning: computed GOMEMLIMIT %.0f is out of range, skipping\n", memlimitFloat)
		return
	}

	memlimit := int64(memlimitFloat)
	prev := debug.SetMemoryLimit(memlimit)
	fmt.Printf("GC tuning: GOMEMLIMIT set to %s (%.0f%% of %s cgroup limit, was %s)\n",
		formatBytes(uint64(memlimit)), ratio*100, formatBytes(limit), formatBytes(uint64(prev)))
}

// detectCgroupMemoryLimit reads the memory limit from cgroup v2 or v1.
func detectCgroupMemoryLimit() (uint64, error) {
	// Try cgroup v2 first
	limit, err := readCgroupV2MemoryLimit()
	if err == nil {
		return limit, nil
	}

	// Fall back to cgroup v1
	limit, err = readCgroupV1MemoryLimit()
	if err == nil {
		return limit, nil
	}

	return 0, errors.NewConfigurationError("no cgroup memory limit found (tried v2 and v1)")
}

// readCgroupV2MemoryLimit reads memory.max from cgroup v2.
func readCgroupV2MemoryLimit() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0, errors.NewConfigurationError("cgroup v2: " + err.Error())
	}

	return parseCgroupMemoryValue(strings.TrimSpace(string(data)))
}

// readCgroupV1MemoryLimit reads memory.limit_in_bytes from cgroup v1.
func readCgroupV1MemoryLimit() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0, errors.NewConfigurationError("cgroup v1: " + err.Error())
	}

	return parseCgroupMemoryValue(strings.TrimSpace(string(data)))
}

// parseCgroupMemoryValue parses a cgroup memory limit value.
// Returns an error for "max" or values that indicate no limit.
func parseCgroupMemoryValue(s string) (uint64, error) {
	if s == "max" || s == "" {
		return 0, errors.NewConfigurationError("memory is not limited")
	}

	limit, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, errors.NewConfigurationError("failed to parse memory limit %q: %s", s, err.Error())
	}

	// Values near max int64 (page-aligned) indicate no limit in cgroup v1
	pageSize := uint64(os.Getpagesize())
	noLimitV1 := uint64(math.MaxInt64) / pageSize * pageSize
	if limit >= noLimitV1 {
		return 0, errors.NewConfigurationError("memory is not limited")
	}

	return limit, nil
}

// formatBytes formats a byte count as a human-readable string.
func formatBytes(b uint64) string {
	const (
		mib = 1024 * 1024
		gib = 1024 * mib
	)

	switch {
	case b >= gib:
		return fmt.Sprintf("%.1fGiB", float64(b)/float64(gib))
	case b >= mib:
		return fmt.Sprintf("%.0fMiB", float64(b)/float64(mib))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
