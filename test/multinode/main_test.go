//go:build network_chaos

// Package multinode contains network-chaos end-to-end scenarios that drive
// compose/multinode.sh (docker, iptables, tc) to exercise P2P-layer behaviour
// that in-process TestDaemon tests cannot reach.
//
// These tests are gated behind the `network_chaos` build tag so they do not
// run under `go test ./...`, `make smoketest`, or `make sequentialtest`. Use
// `make network-chaos-test` to run them.
//
// Lifecycle: TestMain brings up a single 5-node stack, shared by every
// scenario in the package. Each scenario calls stack.Reset(t) on entry to
// clear any chaos state the previous scenario may have left behind and
// establish its own baseline tip. One cold-start amortised across all
// scenarios is much less racy than a fresh up/down per test.
package multinode

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

// sharedNodeCount is fixed at 5 so all scenarios have plenty of nodes
// available. Scenarios that only need 3 operate on the first 3; the rest
// just sit there healthy.
const sharedNodeCount = 5

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

	sharedStack = harness.Provision(sharedNodeCount)

	code := m.Run()

	sharedStack.Teardown()
	os.Exit(code)
}
