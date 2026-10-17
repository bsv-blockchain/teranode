//go:build network_chaos

package harness

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/failpoint"
)

// Failpoint helpers drive util/failpoint seams in an all-in-one stack whose
// teranode:latest image was built with the "failpoints" tag
// (make network-chaos-failpoint-test). The compose template maps the host
// env var TERANODE<N>_FAILPOINTS onto the container's TERANODE_FAILPOINTS, so
// arming or disarming a node means recreating its container with a different
// value; `docker start` would reuse the old environment. Data directories are
// bind mounts and Aerospike/Postgres are separate containers, so all store
// state survives the recreate.

// containerName returns the all-in-one container name for node n.
func containerName(node int) string {
	return fmt.Sprintf("teranode%d-multinode", node)
}

// RecreateNode recreates node n's container with failpoints (comma-separated
// util/failpoint seam names) armed, or disarmed when failpoints is empty, and
// waits for its RPC to answer. Unlike Reset it never restarts an exited
// container, so a crash during startup fails the test instead of looping.
func (s *Stack) RecreateNode(t *testing.T, node int, failpoints string) {
	t.Helper()

	if s.splitMode {
		t.Fatalf("RecreateNode supports all-in-one stacks only")
	}

	composeFile := filepath.Join(s.RepoRoot, "compose", "generated", "docker-compose-multinode.yml")
	service := fmt.Sprintf("teranode%d", node)

	t.Logf("recreating %s with %s=%q", service, failpoint.EnvVar, failpoints)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile, "up", "-d", "--no-deps", "--force-recreate", service)
	cmd.Dir = s.RepoRoot
	cmd.Env = append(os.Environ(), fmt.Sprintf("TERANODE%d_FAILPOINTS=%s", node, failpoints))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("recreate %s: %v\n%s", service, err, out)
	}

	client := s.Node(node)
	WaitForCondition(t, service+" RPC ready after recreate", 3*time.Minute, 2*time.Second,
		func(ctx context.Context) (bool, error) {
			if status, _ := containerState(node); status == "exited" {
				return false, errors.NewProcessingError("%s exited during startup", service)
			}
			rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
			defer rcancel()
			return client.Ready(rctx) == nil, nil
		})
}

// WaitForExit polls node n's container until it has exited and returns its
// exit code. Call it before Reset or any readiness wait: those restart exited
// containers.
func (s *Stack) WaitForExit(t *testing.T, node int, timeout time.Duration) int {
	t.Helper()

	exitCode := -1
	WaitForCondition(t, containerName(node)+" to exit", timeout, time.Second,
		func(context.Context) (bool, error) {
			status, code := containerState(node)
			if status != "exited" {
				return false, nil
			}
			exitCode = code
			return true, nil
		})
	return exitCode
}

// RequireFailpointHit fails t unless node n's container logs contain the line
// util/failpoint writes when seam name fires, proving the process died at that
// exact seam rather than somewhere else.
func (s *Stack) RequireFailpointHit(t *testing.T, node int, name string) {
	t.Helper()

	out, err := exec.Command("docker", "logs", "--tail", "500", containerName(node)).CombinedOutput()
	if err != nil {
		t.Fatalf("docker logs %s: %v\n%s", containerName(node), err, out)
	}
	if !strings.Contains(string(out), failpoint.HitMarker+name) {
		t.Fatalf("%s logs do not contain %q; tail:\n%s", containerName(node), failpoint.HitMarker+name, lastLines(string(out), 40))
	}
}

// containerState returns node n's container status and exit code. Errors are
// reported as an empty status.
func containerState(node int) (string, int) {
	out, err := exec.Command("docker", "inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", containerName(node)).Output()
	if err != nil {
		return "", -1
	}
	fields := strings.Fields(string(out))
	if len(fields) != 2 {
		return "", -1
	}
	code, err := strconv.Atoi(fields[1])
	if err != nil {
		return fields[0], -1
	}
	return fields[0], code
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
