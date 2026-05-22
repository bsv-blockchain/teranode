//go:build network_chaos

package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// Environment variables controlling harness behaviour. Defaults chosen so that
// running `make network-chaos-test` on a fresh checkout Just Works.
const (
	// envAllowTakeover lets a test run against a pre-existing multinode stack
	// instead of refusing. Useful for CI hosts where test setup pre-stages the
	// stack for speed.
	envAllowTakeover = "MULTINODE_ALLOW_TAKEOVER"
	// envBYOS ("bring your own stack") skips Provision/Teardown entirely.
	// The harness still talks to whatever containers are running. Useful for
	// iterating locally against a long-running stack.
	envBYOS = "MULTINODE_BYOS"
	// envUpTimeout overrides the readiness-wait timeout (seconds).
	envUpTimeout = "MULTINODE_UP_TIMEOUT"
)

// progressLogger is the subset of *testing.T the harness needs for progress
// reporting and fatal aborts. *testing.T satisfies it directly; a small
// log-package adapter handles Provision/Teardown which run inside TestMain.
type progressLogger interface {
	Logf(format string, args ...any)
	Fatalf(format string, args ...any)
	Helper()
}

type stdLogger struct{}

func (stdLogger) Logf(format string, args ...any)   { log.Printf(format, args...) }
func (stdLogger) Fatalf(format string, args ...any) { log.Fatalf(format, args...) }
func (stdLogger) Helper()                           {}

// Stack wraps a compose/multinode.sh-managed teranode stack. The shared-stack
// testing model is:
//
//  1. Provision (from TestMain) brings up a single stack for the whole package.
//  2. Each scenario calls Reset(t) to clear any chaos state the previous
//     scenario left behind (iptables rules, tc netem qdiscs, killed or paused
//     containers), wait for the p2p mesh, and establish its own baseline tip.
//  3. Teardown (from TestMain) drops the stack.
//
// This amortises the multi-minute cold-start cost across scenarios and avoids
// repeatedly hitting the Aerospike secondary-index startup race.
type Stack struct {
	NodeCount int
	RepoRoot  string
	Script    string // absolute path to compose/multinode.sh
	byos      bool
}

// Provision creates and starts a Stack intended to live for the whole test
// package. Intended for use from TestMain: it uses log.Fatalf on setup
// failure because TestMain has no *testing.T. Honours MULTINODE_BYOS=1 by
// pointing at an already-running stack instead of starting a new one, and
// aborts (rather than skipping) if a stack is already running without
// MULTINODE_ALLOW_TAKEOVER=1.
func Provision(nodeCount int) *Stack {
	l := stdLogger{}

	root, err := repoRoot()
	if err != nil {
		l.Fatalf("resolve repo root: %v", err)
	}
	script := filepath.Join(root, "compose", "multinode.sh")
	if _, err := os.Stat(script); err != nil {
		l.Fatalf("multinode.sh not found at %s: %v", script, err)
	}

	s := &Stack{
		NodeCount: nodeCount,
		RepoRoot:  root,
		Script:    script,
		byos:      os.Getenv(envBYOS) == "1",
	}

	if !s.byos {
		running, err := stackRunning(root)
		if err != nil {
			l.Fatalf("probe for existing stack: %v", err)
		}
		if running && os.Getenv(envAllowTakeover) != "1" {
			l.Fatalf("multinode stack already running; tear it down with 'compose/multinode.sh down' or set %s=1", envAllowTakeover)
		}

		if err := s.wipePersistedState(); err != nil {
			l.Logf("warning: wipe of data/multinode state failed: %v", err)
		}

		l.Logf("bringing up %d-node multinode stack...", nodeCount)
		out, err := s.runCombined(context.Background(), "up", fmt.Sprintf("%d", nodeCount))
		if err != nil {
			l.Fatalf("multinode.sh up %d failed: %v\n%s", nodeCount, err, out)
		}
	} else {
		l.Logf("MULTINODE_BYOS=1: using already-running stack (%d nodes assumed)", nodeCount)
	}

	timeout := 4 * time.Minute
	if v := os.Getenv(envUpTimeout); v != "" {
		if d, err := time.ParseDuration(v + "s"); err == nil {
			timeout = d
		}
	}
	s.waitReady(l, timeout)

	if nodeCount > 1 {
		l.Logf("waiting for p2p mesh (%d peers per node)...", nodeCount-1)
		waitForMesh(l, s.Nodes(), nodeCount-1, 5*time.Minute)
		l.Logf("mesh ready")
	}

	return s
}

// Teardown drops a stack provisioned via Provision. Safe to call from
// TestMain after m.Run().
func (s *Stack) Teardown() {
	l := stdLogger{}
	if s.byos {
		l.Logf("MULTINODE_BYOS=1: leaving stack running")
		return
	}

	_, _ = s.runCombined(context.Background(), "chaos", "heal")

	out, err := s.runCombined(context.Background(), "down")
	if err != nil {
		l.Logf("multinode.sh down failed: %v\n%s", err, out)
	}
}

// Reset prepares the stack for a new scenario. It:
//   - heals any iptables isolation rules,
//   - removes any tc netem latency,
//   - restarts any containers the previous scenario killed or paused,
//   - waits for every node's RPC to answer,
//   - waits for the p2p mesh to re-establish,
//   - waits for all nodes to converge on a single tip.
//
// On failure it calls t.Fatalf so a flaky stack surfaces as a clear test
// failure at the boundary rather than a confusing mid-scenario timeout.
func (s *Stack) Reset(t *testing.T) {
	t.Helper()

	// Best-effort chaos reset. Each command is idempotent / no-op when the
	// condition it undoes is absent.
	_, _ = s.runCombined(context.Background(), "chaos", "heal")
	for n := 1; n <= s.NodeCount; n++ {
		// Unpause anything that was frozen.
		_ = exec.Command("docker", "unpause", ctrName(n)).Run()
		// Remove any tc netem qdisc; errors are fine (none installed).
		_, _ = s.runCombined(context.Background(), "chaos", "unslow", fmt.Sprintf("%d", n))
		// Start anything that was killed.
		if s.containerExited(n) {
			t.Logf("reset: starting teranode%d (was exited)", n)
			if err := s.restartContainer(n); err != nil {
				t.Fatalf("reset: restart teranode%d: %v", n, err)
			}
		}
	}

	s.waitReady(t, 3*time.Minute)

	if s.NodeCount > 1 {
		waitForMesh(t, s.Nodes(), s.NodeCount-1, 3*time.Minute)
	}

	// Converge on some tip, whatever it is. Caller can use this as its
	// baseline.
	_ = WaitForConverged(t, s.Nodes(), 60*time.Second)
}

// Node returns an RPC client for node n (1-indexed).
func (s *Stack) Node(n int) *RPCClient {
	return newRPCClient(n)
}

// Nodes returns RPC clients for every node in the stack.
func (s *Stack) Nodes() []*RPCClient {
	clients := make([]*RPCClient, s.NodeCount)
	for i := 0; i < s.NodeCount; i++ {
		clients[i] = newRPCClient(i + 1)
	}
	return clients
}

// MustRun shells out to multinode.sh, fails t on non-zero exit, and returns
// combined stdout+stderr for logging.
func (s *Stack) MustRun(t *testing.T, args ...string) string {
	t.Helper()
	out, err := s.runCombined(context.Background(), args...)
	if err != nil {
		t.Fatalf("multinode.sh %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// Run is the non-fatal variant of MustRun.
func (s *Stack) Run(ctx context.Context, args ...string) (string, error) {
	return s.runCombined(ctx, args...)
}

// RequireSudo skips the current test if passwordless sudo is not available.
// Scenarios that use chaos isolate/heal/slow/unslow must call this before
// invoking those verbs.
func (s *Stack) RequireSudo(t *testing.T) {
	t.Helper()
	cmd := exec.Command("sudo", "-n", "true")
	if err := cmd.Run(); err != nil {
		t.Skipf("passwordless sudo not available (needed for chaos iptables/tc): %v", err)
	}
}

// waitReady blocks until every node's RPC answers getinfo or timeout expires.
// Nodes are polled in parallel so one slow starter doesn't starve the others
// of the shared timeout budget.
//
// Teranode currently has a known startup race with Aerospike: the block
// assembler queries a secondary index before it finishes building, retries
// three times, and exits with code 0. If we spot an exited container during
// the wait we restart it a bounded number of times; persistent crashes will
// still surface as a timeout with diagnostic output.
func (s *Stack) waitReady(l progressLogger, timeout time.Duration) {
	l.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	l.Logf("waiting up to %s for %d teranode RPCs to answer...", timeout, s.NodeCount)

	type result struct {
		node int
		err  error
	}
	results := make(chan result, s.NodeCount)

	var wg sync.WaitGroup
	for n := 1; n <= s.NodeCount; n++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			err := s.waitNodeReady(ctx, l, node)
			results <- result{node: node, err: err}
		}(n)
	}
	wg.Wait()
	close(results)

	var failures []int
	for r := range results {
		if r.err != nil {
			failures = append(failures, r.node)
		}
	}
	if len(failures) > 0 {
		for _, n := range failures {
			s.dumpDiagnostics(l, n)
		}
		l.Fatalf("%d teranode(s) did not become ready within %s: nodes=%v", len(failures), timeout, failures)
	}
}

// waitNodeReady polls a single node's RPC until it answers, restarting the
// container up to maxRestarts times if it enters an exited state.
func (s *Stack) waitNodeReady(ctx context.Context, l progressLogger, node int) error {
	l.Helper()
	client := newRPCClient(node)
	const (
		interval        = 2 * time.Second
		postRestartWait = 5 * time.Second
		maxRestarts     = 3
	)
	restarts := 0

	for {
		rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
		err := client.Ready(rctx)
		rcancel()
		if err == nil {
			l.Logf("teranode%d RPC ready", node)
			return nil
		}

		if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
			return err
		}

		if restarts < maxRestarts && s.containerExited(node) {
			l.Logf("teranode%d container exited; restarting (attempt %d/%d)", node, restarts+1, maxRestarts)
			if rerr := s.restartContainer(node); rerr != nil {
				l.Logf("restart teranode%d failed: %v", node, rerr)
			}
			restarts++
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(postRestartWait):
			}
			continue
		}

		select {
		case <-ctx.Done():
			return err
		case <-time.After(interval):
		}
	}
}

// containerExited reports whether teranode<node>-multinode is in the Exited
// state right now. Errors (including container-not-found) are treated as
// not-exited so we don't infinite-loop restarting something that never
// existed.
func (s *Stack) containerExited(node int) bool {
	cmd := exec.Command("docker", "inspect", "--format", "{{.State.Status}}", ctrName(node))
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) == "exited"
}

// restartContainer runs `docker start` on the named teranode container.
func (s *Stack) restartContainer(node int) error {
	cmd := exec.Command("docker", "start", ctrName(node))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// dumpDiagnostics logs container status and recent logs for node n.
func (s *Stack) dumpDiagnostics(l progressLogger, node int) {
	l.Helper()
	if out, err := s.runCombined(context.Background(), "status"); err == nil {
		l.Logf("--- multinode.sh status ---\n%s", out)
	} else {
		l.Logf("status failed: %v\n%s", err, out)
	}
	cmd := exec.Command("docker", "logs", "--tail", "60", ctrName(node))
	out, err := cmd.CombinedOutput()
	if err != nil {
		l.Logf("docker logs %s failed: %v\n%s", ctrName(node), err, out)
		return
	}
	l.Logf("--- docker logs %s (tail 60) ---\n%s", ctrName(node), out)
}

// runCombined execs multinode.sh with args and returns combined output.
func (s *Stack) runCombined(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, s.Script, args...)
	cmd.Dir = s.RepoRoot
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	return buf.String(), err
}

// wipePersistedState removes aerospike and teranode bind-mount data under
// data/multinode/. Uses a throwaway root-privileged container because docker
// created those dirs as root. Safe to call when the directory doesn't exist.
func (s *Stack) wipePersistedState() error {
	stateDir := filepath.Join(s.RepoRoot, "data", "multinode")
	if _, err := os.Stat(stateDir); err != nil {
		return nil
	}
	cmd := exec.Command("docker", "run", "--rm", "-v", stateDir+":/data", "alpine",
		"sh", "-c", "rm -rf /data/aerospike* /data/teranode* 2>/dev/null || true")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// ctrName returns the docker container name for teranode n.
func ctrName(n int) string { return fmt.Sprintf("teranode%d-multinode", n) }

// repoRoot walks up from this file's location (via runtime.Caller) until it
// finds a go.mod. This avoids brittle dependencies on the test's cwd.
func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod above %s", file)
		}
		dir = parent
	}
}

// stackRunning returns true iff at least one teranode*-multinode container
// is currently running.
func stackRunning(root string) (bool, error) {
	cmd := exec.Command("docker", "ps", "--filter", "name=-multinode", "--format", "{{.Names}}")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.HasPrefix(line, "teranode") {
			return true, nil
		}
	}
	return false, nil
}
