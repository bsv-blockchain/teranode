//go:build network_chaos

// Package multinodesplit contains network-chaos end-to-end scenarios that
// exercise the split-per-service topology (`compose/multinode.sh up <N>
// -allinone=0`). Each node is nine sibling containers, one per microservice,
// so scenarios can kill or pause individual services without taking down
// the whole node — the feature these tests are written to showcase.
//
// These tests are gated behind the `network_chaos` build tag so they do not
// run under `go test ./...`, `make smoketest`, or `make sequentialtest`.
// Use `make network-chaos-test` (with the appropriate split-mode entry
// point, when added) to run them.
//
// Lifecycle mirrors the all-in-one suite in test/multinode/: TestMain
// brings up a single 3-node split stack, scenarios share it, each scenario
// calls stack.Reset(t) on entry. 3 nodes (not 5) because a split stack runs
// 9 containers per node — three nodes is 27 service containers plus infra,
// which is already heavy on a developer laptop.
package multinodesplit

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
)

// sharedStack is the package-level stack used by every scenario. Populated
// by TestMain, consumed via stack().
var sharedStack *harness.Stack

// stack returns the package-level stack. Scenarios call this rather than
// creating their own; use stack().Reset(t) on entry to get a clean state.
func stack() *harness.Stack { return sharedStack }

// sharedNodeCount is fixed at 3 — the smallest mesh that lets a scenario
// isolate one node while two survivors keep mining and propagating. A
// 3-node split stack is already ~32 containers (3 × 9 service + infra);
// scaling further hits diminishing returns vs. the laptop-friendliness
// the whole local-network harness was designed for.
const sharedNodeCount = 3

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		// -short skips the whole suite without bringing up any containers.
		os.Exit(0)
	}

	// -test.list just enumerates tests; don't spin up a docker stack for it.
	if f := flag.Lookup("test.list"); f != nil && f.Value.String() != "" {
		os.Exit(m.Run())
	}

	if _, err := exec.LookPath("docker"); err != nil {
		fmt.Fprintln(os.Stderr, "network_chaos: docker not found in PATH")
		os.Exit(1)
	}

	sharedStack = harness.ProvisionSplit(sharedNodeCount)

	code := m.Run()

	sharedStack.Teardown()
	os.Exit(code)
}
