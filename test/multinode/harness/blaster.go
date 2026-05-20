//go:build network_chaos

package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Blaster is a handle to a running coinbase blaster process launched via
// `compose/multinode.sh blast`.
type Blaster struct {
	cmd    *exec.Cmd
	cancel context.CancelFunc
	stdout bytes.Buffer
	stderr bytes.Buffer
	done   chan error
}

// StartBlaster launches the blaster in the background targeting the given
// nodes. Additional args (headless mode, max-tps, duration) are passed after
// `--` to blast.
//
// The call returns once the process has been started. Use Stop to terminate
// it cleanly; the cancellable context is automatically cancelled on t.Cleanup
// so a forgotten Stop still tears the process down.
func (s *Stack) StartBlaster(t *testing.T, nodes []int, args ...string) *Blaster {
	t.Helper()

	if len(nodes) == 0 {
		t.Fatal("StartBlaster requires at least one node")
	}
	list := make([]string, len(nodes))
	for i, n := range nodes {
		list[i] = fmt.Sprintf("%d", n)
	}

	cliArgs := []string{"blast", strings.Join(list, ",")}
	if len(args) > 0 {
		cliArgs = append(cliArgs, "--")
		cliArgs = append(cliArgs, args...)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, s.Script, cliArgs...)
	cmd.Dir = s.RepoRoot
	// Put the blaster in its own process group so a Stop can target the whole
	// subtree (blaster + any background miner spawned by --auto-mine).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	b := &Blaster{cmd: cmd, cancel: cancel, done: make(chan error, 1)}
	cmd.Stdout = &b.stdout
	cmd.Stderr = &b.stderr

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start blaster: %v", err)
	}
	t.Logf("blaster started: pid=%d args=%v", cmd.Process.Pid, cliArgs)

	go func() { b.done <- cmd.Wait() }()

	t.Cleanup(func() {
		// Idempotent: Stop after first call is a no-op.
		if b.cmd == nil || b.cmd.ProcessState != nil {
			return
		}
		b.Stop(t)
	})

	return b
}

// Stop signals the blaster's process group with SIGINT and waits up to 10s
// for exit. Falls back to SIGKILL on timeout.
func (b *Blaster) Stop(t *testing.T) {
	t.Helper()
	if b.cmd == nil || b.cmd.Process == nil {
		return
	}
	if b.cmd.ProcessState != nil {
		return
	}

	pgid, err := syscall.Getpgid(b.cmd.Process.Pid)
	if err != nil {
		// Fall back to the process itself if we can't see the group.
		pgid = b.cmd.Process.Pid
	}
	_ = syscall.Kill(-pgid, syscall.SIGINT)

	select {
	case err := <-b.done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("blaster exited with error: %v\n--- stderr ---\n%s", err, b.stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Log("blaster did not exit within 10s of SIGINT, sending SIGKILL")
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		<-b.done
	}
	b.cancel()
	b.cmd = nil
}

// Stdout returns accumulated blaster stdout (useful for assertions on the
// preamble and tx/s stats in headless mode).
func (b *Blaster) Stdout() string { return b.stdout.String() }

// Stderr returns accumulated blaster stderr.
func (b *Blaster) Stderr() string { return b.stderr.String() }
