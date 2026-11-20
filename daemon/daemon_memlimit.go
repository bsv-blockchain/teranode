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

const (
	bytesPerMiB = 1024 * 1024
	bytesPerGiB = 1024 * bytesPerMiB

	// offHeapHeadroomBytes is what a process hosting the txmeta cache keeps out of
	// GOMEMLIMIT for off-heap memory other than the cache itself (issue 2176). On a
	// subtreevalidation container capped at 8.6GiB with a 1024MiB cache, RSS ran
	// 1.3-2.9GB above what the Go runtime held, so up to ~1.7GiB beyond the cache;
	// its source is not established. With 1.5GiB here and the default ratio of 0.9,
	// that cap leaves 3.1GiB outside GOMEMLIMIT, which covers the measured peak,
	// while GOMEMLIMIT (5.5GiB) stays above the 3.3-3.8GB live heap of those blocks.
	offHeapHeadroomBytes = 1536 * bytesPerMiB

	// gomemlimitFloorFraction bounds how far the off-heap reservation can push
	// GOMEMLIMIT down: never below this fraction of ratio * limit. Without it a
	// small cap (a 3GiB container with a 1GiB cache) would get a GOMEMLIMIT of a
	// few hundred MiB, or a negative one, and the GC would run continuously.
	gomemlimitFloorFraction = 0.5
)

// detectMemoryLimit is detectCgroupMemoryLimit, held in a variable so a test can
// supply a limit on a host without cgroups.
var detectMemoryLimit = detectCgroupMemoryLimit

// configureGCTuning sets GOMEMLIMIT and GOGC based on the container's cgroup
// memory limit and the GCTuning settings. This should be called early in
// startup, before services begin allocating significant memory.
//
// When enabled, it:
//  1. Detects the cgroup memory limit (v2 then v1 fallback)
//  2. Sets GOMEMLIMIT to ratio * limit (default 90%). In a process that hosts
//     the off-heap txmeta cache, it is ratio * (limit - cache - headroom)
//     instead, floored at half of ratio * limit; see offHeapReserve.
//  3. Sets GOGC to the configured target (default 100)
//
// args are the daemon's command-line arguments, used to tell which services this
// process will start.
//
// If GOMEMLIMIT or GOGC are already set via environment variables, those
// values are respected and not overridden.
func ConfigureGCTuning(tSettings *settings.Settings, args []string) {
	gcSettings := tSettings.GCTuning

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

	reserve, reason := offHeapReserve(tSettings, args)
	configureGOMEMLIMIT(gcSettings.Ratio, reserve, reason)
}

// offHeapReserve returns the bytes to take off the cgroup limit before applying
// gc_tuning_ratio, and a description of them for the startup log.
//
// The reservation applies only to a process that hosts the txmeta cache: the
// subtreevalidation service runs here (on its own or all-in-one), the cache is
// enabled, and its bucket type keeps the cache off the Go heap. Every bucket type
// except pointer allocates its memory with mmap, which GOMEMLIMIT cannot see; the
// pointer cache lives on the Go heap and is already counted. subtreevalidation is
// the only service that builds the cache (services/subtreevalidation/Server.go).
// Every other process gets 0, so its GOMEMLIMIT is unchanged.
func offHeapReserve(tSettings *settings.Settings, args []string) (uint64, string) {
	if !serviceEnabled(serviceSubtreeValidationFormal, args) {
		return 0, ""
	}

	sv := tSettings.SubtreeValidation

	if !sv.TxMetaCacheEnabled || strings.EqualFold(strings.TrimSpace(sv.TxMetaCacheBucketType), "pointer") {
		return 0, ""
	}

	cacheBytes := uint64(max(sv.TxMetaCacheMaxMB, 0)) * bytesPerMiB

	return cacheBytes + offHeapHeadroomBytes, fmt.Sprintf("txmeta cache %s + off-heap headroom %s",
		formatBytes(cacheBytes), formatBytes(offHeapHeadroomBytes))
}

// computeGOMEMLIMIT returns the GOMEMLIMIT for a cgroup limit, the GC ratio and
// the bytes reserved for off-heap memory: ratio * (limit - reserve), but never
// less than gomemlimitFloorFraction of ratio * limit. floored reports whether the
// floor was used. With no reservation it is ratio * limit, as before issue 2176.
func computeGOMEMLIMIT(limit uint64, ratio float64, reserve uint64) (memlimit float64, floored bool) {
	untuned := float64(limit) * ratio
	if reserve == 0 {
		return untuned, false
	}

	floor := untuned * gomemlimitFloorFraction

	reserved := (float64(limit) - float64(reserve)) * ratio
	if reserved < floor {
		return floor, true
	}

	return reserved, false
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

// configureGOMEMLIMIT detects the cgroup memory limit and sets GOMEMLIMIT,
// keeping reserve bytes (described by reason) out of it for off-heap memory.
func configureGOMEMLIMIT(ratio float64, reserve uint64, reason string) {
	if val, ok := os.LookupEnv("GOMEMLIMIT"); ok {
		fmt.Printf("GC tuning: GOMEMLIMIT already set by environment to %s, not overriding\n", val)
		return
	}

	limit, err := detectMemoryLimit()
	if err != nil {
		fmt.Printf("GC tuning: could not detect cgroup memory limit: %v, GOMEMLIMIT not set\n", err)
		return
	}

	memlimitFloat, floored := computeGOMEMLIMIT(limit, ratio, reserve)
	if memlimitFloat <= 0 || memlimitFloat > float64(math.MaxInt64) {
		fmt.Printf("GC tuning: computed GOMEMLIMIT %.0f is out of range, skipping\n", memlimitFloat)
		return
	}

	memlimit := int64(memlimitFloat)
	prev := debug.SetMemoryLimit(memlimit)

	switch {
	case floored:
		fmt.Printf("GC tuning: WARNING: the %s cgroup limit is too small to reserve %s (%s) for off-heap memory; GOMEMLIMIT set to its floor of %s (half of %.0f%% of the limit, was %s), which leaves off-heap memory uncovered, so lower txMetaCacheMaxMB or raise the memory limit\n",
			formatBytes(limit), formatBytes(reserve), reason, formatBytes(uint64(memlimit)), ratio*100, formatBytes(uint64(prev)))
	case reserve > 0:
		fmt.Printf("GC tuning: GOMEMLIMIT set to %s (%.0f%% of %s cgroup limit minus %s reserved for %s, was %s)\n",
			formatBytes(uint64(memlimit)), ratio*100, formatBytes(limit), formatBytes(reserve), reason, formatBytes(uint64(prev)))
	default:
		fmt.Printf("GC tuning: GOMEMLIMIT set to %s (%.0f%% of %s cgroup limit, was %s)\n",
			formatBytes(uint64(memlimit)), ratio*100, formatBytes(limit), formatBytes(uint64(prev)))
	}
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
