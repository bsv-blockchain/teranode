package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// generateInto runs the same code path as main() but writes into a caller-
// supplied directory and returns the loaded compose + settings files. It
// avoids invoking the binary so tests stay hermetic and fast (no go run).
func generateInto(t *testing.T, n int, allInOne bool, outDir string) (composeYAML, settingsConf string) {
	t.Helper()
	keys, err := loadKeys(n)
	if err != nil {
		t.Fatalf("loadKeys: %v", err)
	}
	s := buildSpec(keys)
	s.AllInOne = allInOne
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

// TestGenAllInOneTopology pins the shape of the default (all-in-one)
// generator output: one teranodeN service per node and no per-microservice
// container names. This is the regression guard — if anyone refactors the
// template selection wrong the all-in-one output will start emitting split
// containers and this test will catch it.
func TestGenAllInOneTopology(t *testing.T) {
	dir := t.TempDir()
	compose, settings := generateInto(t, 3, true, dir)

	// Each node renders exactly one container, named teranode<N>.
	mustMatchCount(t, compose, regexp.MustCompile(`(?m)^  teranode[0-9]+:$`), 3)

	// No split-mode container names leak through.
	if strings.Contains(compose, "-blockchain:") || strings.Contains(compose, "-validator:") {
		t.Fatalf("all-in-one compose contains split-mode service blocks; got:\n%s", compose)
	}

	// All-in-one settings overlay does NOT carry the per-service grpcAddress
	// overrides — those would force the in-process gRPC clients to dial the
	// docker DNS instead of localhost and serve no purpose in this mode.
	for _, key := range []string{
		"blockchain_grpcAddress.docker.teranode1",
		"blockassembly_grpcAddress.docker.teranode1",
		"validator_grpcAddress.docker.teranode1",
	} {
		if strings.Contains(settings, key) {
			t.Fatalf("all-in-one settings unexpectedly contains %q", key)
		}
	}
}

// TestGenSplitTopology asserts the split-per-service generator output:
// 9 service containers per node, correct entrypoint flags, and the
// inter-service gRPC address overrides emitted under the per-node
// settings context.
func TestGenSplitTopology(t *testing.T) {
	dir := t.TempDir()
	compose, settings := generateInto(t, 3, false, dir)

	expectedServices := []string{
		"blockchain", "blockassembly", "blockvalidation",
		"subtreevalidation", "validator", "propagation",
		"p2p", "asset", "core",
	}
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		for _, svc := range expectedServices {
			pat := regexp.MustCompile(`(?m)^  teranode` + strconv.Itoa(nodeIdx) + `-` + svc + `:$`)
			if !pat.MatchString(compose) {
				t.Errorf("expected service teranode%d-%s missing from compose output", nodeIdx, svc)
			}
		}
	}

	// 9 services x 3 nodes = 27 service-block headers. The character class
	// allows digits because one service (p2p) carries one.
	mustMatchCount(t, compose, regexp.MustCompile(`(?m)^  teranode[0-9]+-[a-z0-9]+:$`), 27)

	// Entrypoint flags use the daemon's -all=0 -<svc>=1 pattern.
	// Pick a couple of representative services to confirm the wiring.
	for _, want := range []string{
		`"-all=0", "-blockchain=1"`,
		`"-all=0", "-validator=1"`,
		`"-all=0", "-p2p=1"`,
	} {
		if !strings.Contains(compose, want) {
			t.Errorf("compose missing entrypoint fragment %q", want)
		}
	}

	// Core sidecar bundles the leftover services (rpc, alert, etc.) so RPC
	// stays reachable and ancillary services aren't stranded.
	for _, want := range []string{
		`"-rpc=1"`,
		`"-alert=1"`,
		`"-blockpersister=1"`,
		`"-utxopersister=1"`,
		`"-pruner=1"`,
		`"-legacy=1"`,
	} {
		if !strings.Contains(compose, want) {
			t.Errorf("core sidecar missing flag %q", want)
		}
	}

	// Only the -p2p container gets NET_ADMIN; the netns chaos primitives
	// (iptables / tc) need it. The other containers must NOT have it (lower
	// capability surface).
	netAdminBlocks := regexp.MustCompile(`(?m)^    cap_add:\n(?:[ \t]+- NET_ADMIN[ \t]*\n)`).FindAllString(compose, -1)
	if len(netAdminBlocks) != 3 {
		t.Errorf("expected NET_ADMIN on exactly 3 containers (one -p2p per node), got %d", len(netAdminBlocks))
	}

	// Per-node split settings: each node gets the full set of gRPC and
	// HTTP address overrides routing to sibling DNS names.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		nodeStr := strconv.Itoa(nodeIdx)
		for _, want := range []string{
			"blockchain_grpcAddress.docker.teranode" + nodeStr + "             = teranode" + nodeStr + "-blockchain:${BLOCKCHAIN_GRPC_PORT}",
			"blockassembly_grpcAddress.docker.teranode" + nodeStr + "          = teranode" + nodeStr + "-blockassembly:${BLOCKASSEMBLY_GRPC_PORT}",
			"validator_grpcAddress.docker.teranode" + nodeStr + "              = teranode" + nodeStr + "-validator:${VALIDATOR_GRPC_PORT}",
			"propagation_grpcAddresses.docker.teranode" + nodeStr + "          = teranode" + nodeStr + "-propagation:${PROPAGATION_GRPC_PORT}",
			"p2p_grpcAddress.docker.teranode" + nodeStr + "                    = teranode" + nodeStr + "-p2p:${P2P_GRPC_PORT}",
		} {
			if !strings.Contains(settings, want) {
				t.Errorf("split settings missing line:\n  %q\nfull settings:\n%s", want, settings)
			}
		}
	}

	// Host port arithmetic: validator on host 20081/22081/24081, etc.
	// Spot-check node 2's blockassembly: HostBase=22000, port 8085 -> 22085.
	if !strings.Contains(compose, `"22085:8085"`) {
		t.Errorf("expected node2 blockassembly host mapping 22085:8085 in compose output")
	}
	// Spot-check node 3's p2p TCP port: HostBase=24000, port 9905 -> 25905.
	if !strings.Contains(compose, `"25905:9905"`) {
		t.Errorf("expected node3 p2p TCP host mapping 25905:9905 in compose output")
	}

	assertServiceMounts(t, compose)
	assertHealthchecks(t, compose)
}

// assertHealthchecks pins that each split service gets a healthcheck on its
// primary listener port and that the 8 services depending on blockchain
// gate on `service_healthy` (not `service_started`). This removes the
// startup-time connection-refused log noise from sibling services dialling
// blockchain's gRPC port before it's actually accepting connections.
func assertHealthchecks(t *testing.T, compose string) {
	t.Helper()
	expectedPort := map[string]int{
		"blockchain":        8087,
		"blockassembly":     8085,
		"blockvalidation":   8088,
		"subtreevalidation": 8089,
		"validator":         8081,
		"propagation":       8084,
		"p2p":               9904,
		"asset":             8090,
		"core":              9292,
	}
	for svc, port := range expectedPort {
		block := serviceBlock(t, compose, "teranode1-"+svc)
		want := "nc -z localhost " + strconv.Itoa(port)
		if !strings.Contains(block, want) {
			t.Errorf("service teranode1-%s missing healthcheck %q\nblock:\n%s", svc, want, block)
		}
	}

	// Across all 3 nodes: 8 dependents × 3 = 24 blockchain-gate lines, all
	// at condition: service_healthy. If any leak through as service_started
	// we'll see fewer than 24 _healthy matches, or a _started false negative.
	healthy := strings.Count(compose, "teranode1-blockchain:\n        condition: service_healthy")
	healthy += strings.Count(compose, "teranode2-blockchain:\n        condition: service_healthy")
	healthy += strings.Count(compose, "teranode3-blockchain:\n        condition: service_healthy")
	if healthy != 24 {
		t.Errorf("expected 24 blockchain `service_healthy` gates (8 deps × 3 nodes); got %d", healthy)
	}
	if strings.Contains(compose, "teranode1-blockchain:\n        condition: service_started") {
		t.Errorf("found a `service_started` gate on teranode1-blockchain — should be `service_healthy`")
	}
}

// assertServiceMounts pins the per-service volume-mount scoping. Each entry
// states the EXACT set of data subdirs that should appear in that service's
// volumes block. Pinning both the "should mount" set and the absence of
// out-of-scope mounts means accidental over-sharing (or accidental
// under-sharing that breaks a service) trips the test loudly.
//
// Source of truth: daemon/daemon_services.go's start*Service functions —
// each Get*Store call adds the corresponding subdir. See the package-level
// docstring on buildSplitServices for the audit.
func assertServiceMounts(t *testing.T, compose string) {
	t.Helper()
	allMounts := []string{"txstore", "subtreestore", "blockstore", "subtree_quorum", "external"}
	expected := map[string][]string{
		"blockchain":        nil,
		"blockassembly":     {"txstore", "subtreestore", "external"},
		"blockvalidation":   {"txstore", "subtreestore", "external"},
		"subtreevalidation": {"txstore", "subtreestore", "subtree_quorum", "external"},
		"validator":         {"external"},
		"propagation":       {"txstore"},
		"p2p":               nil,
		"asset":             {"txstore", "subtreestore", "external"},
		"core":              {"txstore", "subtreestore", "blockstore", "external"},
	}
	for svc, want := range expected {
		block := serviceBlock(t, compose, "teranode1-"+svc)
		wantSet := map[string]bool{}
		for _, m := range want {
			wantSet[m] = true
		}
		for _, m := range allMounts {
			line := "/" + m + ":/app/data/" + m
			has := strings.Contains(block, line)
			switch {
			case wantSet[m] && !has:
				t.Errorf("service teranode1-%s is missing required mount %q\nblock:\n%s", svc, m, block)
			case !wantSet[m] && has:
				t.Errorf("service teranode1-%s has out-of-scope mount %q (should not appear)\nblock:\n%s", svc, m, block)
			}
		}
	}
}

// serviceBlock extracts the YAML block for a named compose service
// (everything from the service header up to the next service header at
// the same indentation level). Used by mount-scoping assertions so each
// service is checked against its own volumes block, not the whole file.
func serviceBlock(t *testing.T, compose, svcName string) string {
	t.Helper()
	startMarker := "\n  " + svcName + ":\n"
	startIdx := strings.Index(compose, startMarker)
	if startIdx < 0 {
		t.Fatalf("service %q not found in compose", svcName)
	}
	// Walk forward from the header until we hit the next service header
	// (any "\n  <name>:\n" pattern) or EOF.
	rest := compose[startIdx+1:]
	nextMatch := regexp.MustCompile(`(?m)^  [a-zA-Z][a-zA-Z0-9_-]*:$`).FindStringIndex(rest[len("  "+svcName+":"):])
	if nextMatch == nil {
		return rest
	}
	return rest[:len("  "+svcName+":")+nextMatch[0]]
}

// mustMatchCount fails the test if the regex doesn't match exactly want times.
func mustMatchCount(t *testing.T, s string, re *regexp.Regexp, want int) {
	t.Helper()
	got := len(re.FindAllString(s, -1))
	if got != want {
		t.Errorf("regex %q: want %d matches, got %d", re.String(), want, got)
	}
}

