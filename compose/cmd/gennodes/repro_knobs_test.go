package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// generateIntoWith is generateInto plus the two repro knobs (-limits and
// -retention). Kept separate from generateInto so the existing tests keep
// exercising the plain default path.
func generateIntoWith(t *testing.T, n int, allInOne bool, outDir, limits string, profile retentionProfile) (composeYAML, settingsConf string) {
	t.Helper()
	keys, err := loadKeys(n)
	if err != nil {
		t.Fatalf("loadKeys: %v", err)
	}
	s := buildSpec(keys)
	s.AllInOne = allInOne
	s.Retention = profile
	if err := applyLimits(s.SplitServices, limits); err != nil {
		t.Fatalf("applyLimits(%q): %v", limits, err)
	}
	if err := writeAll(outDir, s); err != nil {
		t.Fatalf("writeAll: %v", err)
	}
	composeBytes, err := os.ReadFile(filepath.Join(outDir, "docker-compose-multinode.yml"))
	if err != nil {
		t.Fatalf("read compose file: %v", err)
	}
	settingsBytes, err := os.ReadFile(filepath.Join(outDir, "settings_multinode.conf"))
	if err != nil {
		t.Fatalf("read settings file: %v", err)
	}
	return string(composeBytes), string(settingsBytes)
}

// TestSplitMetricsPortsAreUniqueAndComplete guards the per-service metrics
// mapping. Every split service serves :9091, but only one can own the
// derived HostBase+1091, so each gets an explicit offset. If this breaks,
// block-assembly metrics silently vanish and a repro scenario passes
// vacuously — hence the belt-and-braces check on both presence and
// uniqueness.
func TestSplitMetricsPortsAreUniqueAndComplete(t *testing.T) {
	dir := t.TempDir()
	compose, _ := generateInto(t, 3, false, dir)

	svcs := buildSplitServices()

	// Every service of every node publishes :9091 on its own host port.
	for node := 1; node <= 3; node++ {
		base := hostBaseFloor + (node-1)*nodeStride
		for i, svc := range svcs {
			want := fmt.Sprintf("%d:9091", base+metricsHostOffset+i)
			block := serviceBlock(t, compose, fmt.Sprintf("teranode%d-%s", node, svc.Name))
			if !strings.Contains(block, want) {
				t.Errorf("teranode%d-%s: missing metrics mapping %q", node, svc.Name, want)
			}
		}
	}

	// No host port may appear twice anywhere in the file.
	re := regexp.MustCompile(`"(\d+):(\d+)"`)
	seen := map[int]string{}
	for _, m := range re.FindAllStringSubmatch(compose, -1) {
		host, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatalf("unparseable host port %q", m[1])
		}
		if prev, dup := seen[host]; dup {
			t.Errorf("host port %d published twice (%s and %s)", host, prev, m[0])
		}
		seen[host] = m[0]
		if host >= 65536 {
			t.Errorf("host port %d out of range", host)
		}
	}

	// The offsets must stay inside the per-node stride, or node N's metrics
	// port would land in node N+1's range.
	if got := metricsHostOffset + len(svcs) - 1; got >= nodeStride {
		t.Errorf("metrics offsets reach %d, which is >= nodeStride %d", got, nodeStride)
	}
}

// TestLimitsApplyOnlyToNamedServices pins that -limits caps exactly the
// services named and leaves every other container unconstrained.
func TestLimitsApplyOnlyToNamedServices(t *testing.T) {
	dir := t.TempDir()
	compose, _ := generateIntoWith(t, 3, false, dir, "blockassembly=2:8g,validator=4", productionRetention)

	for node := 1; node <= 3; node++ {
		ba := serviceBlock(t, compose, fmt.Sprintf("teranode%d-blockassembly", node))
		if !strings.Contains(ba, "cpus: 2") {
			t.Errorf("teranode%d-blockassembly: missing 'cpus: 2'", node)
		}
		if !strings.Contains(ba, "mem_limit: 8g") {
			t.Errorf("teranode%d-blockassembly: missing 'mem_limit: 8g'", node)
		}

		// validator got cpus only — no mem_limit should be emitted.
		val := serviceBlock(t, compose, fmt.Sprintf("teranode%d-validator", node))
		if !strings.Contains(val, "cpus: 4") {
			t.Errorf("teranode%d-validator: missing 'cpus: 4'", node)
		}
		if strings.Contains(val, "mem_limit:") {
			t.Errorf("teranode%d-validator: unexpected mem_limit", node)
		}

		// An unnamed service must stay unconstrained.
		p2p := serviceBlock(t, compose, fmt.Sprintf("teranode%d-p2p", node))
		if strings.Contains(p2p, "cpus:") || strings.Contains(p2p, "mem_limit:") {
			t.Errorf("teranode%d-p2p: should be unconstrained, got:\n%s", node, p2p)
		}
	}

	// 3 nodes × 2 named services.
	mustMatchCount(t, compose, regexp.MustCompile(`(?m)^    cpus:`), 6)
	mustMatchCount(t, compose, regexp.MustCompile(`(?m)^    mem_limit: 8g`), 3)
}

// TestLimitsRejectsBadSpecs keeps a typo from silently producing an
// unconstrained container, which would look like "the repro didn't fire".
func TestLimitsRejectsBadSpecs(t *testing.T) {
	for _, spec := range []string{
		"blockassembler=2", // typo'd service name
		"blockassembly",    // no '='
		"blockassembly=",   // neither cpus nor mem
		"blockassembly=:",  // same, with separator
	} {
		if err := applyLimits(buildSplitServices(), spec); err == nil {
			t.Errorf("applyLimits(%q) should have failed", spec)
		}
	}

	// Empty and whitespace specs are a no-op, not an error.
	for _, spec := range []string{"", "   "} {
		svcs := buildSplitServices()
		if err := applyLimits(svcs, spec); err != nil {
			t.Errorf("applyLimits(%q) should be a no-op, got %v", spec, err)
		}
		for _, s := range svcs {
			if s.Limits != (resourceLimits{}) {
				t.Errorf("applyLimits(%q) set limits on %s", spec, s.Name)
			}
		}
	}
}

// TestRetentionProfiles pins that production emits nothing and aggressive
// emits the full override set per node, and that the documented invariant
// is actually enforced.
func TestRetentionProfiles(t *testing.T) {
	prodDir := t.TempDir()
	_, prodSettings := generateIntoWith(t, 3, false, prodDir, "", productionRetention)
	for _, key := range []string{
		"global_blockHeightRetention",
		"utxostore_unminedTxRetention",
		"pruner_block_trigger",
	} {
		if strings.Contains(prodSettings, key) {
			t.Errorf("production profile should emit no overrides, but found %q", key)
		}
	}

	aggrDir := t.TempDir()
	_, aggrSettings := generateIntoWith(t, 3, false, aggrDir, "", aggressiveRetention)
	for node := 1; node <= 3; node++ {
		for key, want := range map[string]string{
			"global_blockHeightRetention":            "20",
			"utxostore_unminedTxRetention":           "10",
			"utxostore_parentPreservationBlocks":     "40",
			"pruner_block_trigger":                   "OnBlockPersisted",
			"pruner_blockAssemblyWaitTimeout":        "60s",
			"pruner_skipProcessExpiredPreservations": "false",
			"startPruner":                            "true",
		} {
			re := regexp.MustCompile(fmt.Sprintf(`(?m)^%s\.docker\.teranode%d\s*=\s*%s\s*$`,
				regexp.QuoteMeta(key), node, regexp.QuoteMeta(want)))
			if !re.MatchString(aggrSettings) {
				t.Errorf("aggressive profile: missing %s.docker.teranode%d = %s", key, node, want)
			}
		}
	}

	// The invariant must be enforced, not merely documented.
	if err := aggressiveRetention.validate(); err != nil {
		t.Errorf("aggressiveRetention should be valid: %v", err)
	}
	if err := productionRetention.validate(); err != nil {
		t.Errorf("productionRetention (emits nothing) should be valid: %v", err)
	}
	bad := aggressiveRetention
	bad.UnminedTxRetention = bad.BlockHeightRetention + 1
	if err := bad.validate(); err == nil {
		t.Error("a profile violating unminedTxRetention < blockHeightRetention should fail validate()")
	}
}
