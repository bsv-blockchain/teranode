//go:build network_chaos

package harness

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// All the Wait* functions below accept a progressLogger rather than *testing.T
// so they can be shared between per-test scenarios (passing their t) and
// package-level setup code in Provision (passing a log.Printf adapter).
// *testing.T satisfies progressLogger directly.

// WaitForCondition polls cond every interval until it returns (true, nil) or
// timeout elapses. The description goes into the failure message and the
// per-attempt logs to make flakes easier to read.
func WaitForCondition(l progressLogger, desc string, timeout, interval time.Duration, cond func(ctx context.Context) (bool, error)) {
	l.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		ok, err := cond(ctx)
		if ok {
			return
		}
		lastErr = err
		if time.Now().After(deadline) {
			if lastErr != nil {
				l.Fatalf("timeout after %s waiting for %s; last error: %v", timeout, desc, lastErr)
			}
			l.Fatalf("timeout after %s waiting for %s", timeout, desc)
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				l.Fatalf("context done waiting for %s: %v (last: %v)", desc, ctx.Err(), lastErr)
			}
			l.Fatalf("context done waiting for %s: %v", desc, ctx.Err())
		case <-time.After(interval):
		}
	}
}

// WaitForHeight blocks until node c reports a best-chain height of at least
// target. Useful when you need "node N has seen all my blocks."
func WaitForHeight(l progressLogger, c *RPCClient, target int64, timeout time.Duration) {
	l.Helper()
	WaitForCondition(l, fmt.Sprintf("teranode%d at height>=%d", c.NodeIndex, target), timeout, 1*time.Second,
		func(ctx context.Context) (bool, error) {
			info, err := c.GetBlockchainInfo(ctx)
			if err != nil {
				return false, err
			}
			return info.Blocks >= target, nil
		})
}

// WaitForHeightOn blocks until every client reports height >= target.
func WaitForHeightOn(l progressLogger, clients []*RPCClient, target int64, timeout time.Duration) {
	l.Helper()
	for _, c := range clients {
		WaitForHeight(l, c, target, timeout)
	}
}

// WaitForTip blocks until node c's active tip hash equals want.
func WaitForTip(l progressLogger, c *RPCClient, want string, timeout time.Duration) {
	l.Helper()
	WaitForCondition(l, fmt.Sprintf("teranode%d at tip %s", c.NodeIndex, short(want)), timeout, 1*time.Second,
		func(ctx context.Context) (bool, error) {
			tip, err := c.BestTip(ctx)
			if err != nil {
				return false, err
			}
			return tip.Hash == want, nil
		})
}

// waitForMesh verifies that the p2p mesh can actually propagate blocks. It
// mines a single block on the first client and polls every node's
// getblockchaininfo until they all see it, retrying the probe if needed.
//
// This is a direct test of the property we care about (block gossip works).
// Earlier versions counted peers via getpeerinfo, but teranode's
// getpeerinfo handler can hang for >10s during startup, making it a
// useless readiness signal.
//
// Cost: each mesh check adds one block to the chain. Scenarios using
// relative heights don't care.
//
// Cold-start reality: libp2p mesh formation on a fresh compose stack
// takes considerably longer than RPC readiness, so this function gets a
// generous default timeout. Callers can override.
func waitForMesh(l progressLogger, clients []*RPCClient, _ int, timeout time.Duration) {
	l.Helper()
	if len(clients) < 2 {
		return
	}

	src := clients[0]
	deadline := time.Now().Add(timeout)

	var target int64
	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		info, err := src.GetBlockchainInfo(ctx)
		cancel()
		if err != nil {
			l.Fatalf("waitForMesh: baseline height on teranode%d: %v", src.NodeIndex, err)
		}
		target = info.Blocks + 1
	}

	// Mine one probe block. Block assembly may still be warming up.
	mineProbe := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := src.Generate(ctx, 1)
		return err
	}
	for i := 0; i < 5; i++ {
		if err := mineProbe(); err == nil {
			break
		} else {
			l.Logf("waitForMesh: probe mine attempt %d: %v", i+1, err)
			if time.Now().After(deadline) {
				l.Fatalf("waitForMesh: probe mine timed out")
			}
			time.Sleep(3 * time.Second)
		}
	}

	// Poll each node every 2s. Log heights periodically so a failure shows
	// which nodes were stuck vs caught-up.
	l.Logf("waitForMesh: probe block at height %d mined on teranode%d; waiting for others to see it...",
		target, src.NodeIndex)
	lastLog := time.Now()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		heights := make(map[int]int64, len(clients))
		ready := true
		for _, c := range clients {
			info, err := c.GetBlockchainInfo(ctx)
			if err != nil {
				heights[c.NodeIndex] = -1
				ready = false
				continue
			}
			heights[c.NodeIndex] = info.Blocks
			if info.Blocks < target {
				ready = false
			}
		}
		cancel()

		if ready {
			l.Logf("waitForMesh: all %d nodes at height >= %d", len(clients), target)
			return
		}

		if time.Since(lastLog) > 20*time.Second {
			l.Logf("waitForMesh: heights=%v (need %d on all)", heights, target)
			lastLog = time.Now()
		}

		if time.Now().After(deadline) {
			// Dump logs from any node still behind target to help diagnosis.
			for _, c := range clients {
				if h, ok := heights[c.NodeIndex]; !ok || h < target {
					dumpNodeLogs(l, c.NodeIndex)
				}
			}
			l.Fatalf("waitForMesh: probe block (height %d) did not propagate within %s; last heights=%v",
				target, timeout, heights)
		}
		time.Sleep(2 * time.Second)
	}
}

// dumpNodeLogs tails docker logs for every container belonging to node n —
// the single teranodeN-multinode in all-in-one mode, or all nine
// teranodeN-<svc>-multinode siblings in split mode. Resolved at call time
// so it works under either topology without the caller plumbing a Stack
// reference through.
func dumpNodeLogs(l progressLogger, node int) {
	l.Helper()
	pattern := fmt.Sprintf("^teranode%d(-[a-z0-9]+)?-multinode$", node)
	psOut, err := exec.Command("docker", "ps", "-a", "--format", "{{.Names}}").Output()
	if err != nil {
		l.Logf("docker ps failed while collecting teranode%d logs: %v", node, err)
		return
	}
	re := regexp.MustCompile(pattern)
	var names []string
	for _, line := range strings.Split(strings.TrimSpace(string(psOut)), "\n") {
		if re.MatchString(line) {
			names = append(names, line)
		}
	}
	if len(names) == 0 {
		l.Logf("no containers found matching %s", pattern)
		return
	}
	for _, ctr := range names {
		cmd := exec.Command("docker", "logs", "--tail", "40", ctr)
		out, err := cmd.CombinedOutput()
		if err != nil {
			l.Logf("docker logs %s failed: %v", ctr, err)
			continue
		}
		l.Logf("--- docker logs %s (tail 40) ---\n%s", ctr, out)
	}
}

// WaitForMesh is the exported form of waitForMesh, usable from scenarios.
func WaitForMesh(l progressLogger, clients []*RPCClient, minPeers int, timeout time.Duration) {
	l.Helper()
	waitForMesh(l, clients, minPeers, timeout)
}

// WaitForConverged polls every client until all agree on the same active tip
// hash. Returns the converged hash.
func WaitForConverged(l progressLogger, clients []*RPCClient, timeout time.Duration) string {
	l.Helper()
	var converged string
	WaitForCondition(l, fmt.Sprintf("%d nodes converged on one active tip", len(clients)), timeout, 2*time.Second,
		func(ctx context.Context) (bool, error) {
			tips := make([]ChainTip, len(clients))
			for i, c := range clients {
				tip, err := c.BestTip(ctx)
				if err != nil {
					return false, fmt.Errorf("teranode%d best tip: %w", c.NodeIndex, err)
				}
				tips[i] = tip
			}
			h := tips[0].Hash
			for i := 1; i < len(tips); i++ {
				if tips[i].Hash != h {
					var diff string
					for i2, t2 := range tips {
						diff += fmt.Sprintf("\n  teranode%d: height=%d hash=%s", clients[i2].NodeIndex, t2.Height, short(t2.Hash))
					}
					return false, fmt.Errorf("tips still diverge:%s", diff)
				}
			}
			converged = h
			return true, nil
		})
	return converged
}

func short(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:8] + "..." + hash[len(hash)-4:]
}
