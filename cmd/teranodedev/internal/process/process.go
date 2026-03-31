package process

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/config"
)

const pidFile = ".teranode-dev.pid"

// Start starts the teranode daemon in the background with log rotation.
func Start(projectRoot string, cfg *config.Config) error {
	// Check if already running
	if pid, running := isRunning(projectRoot); running {
		return fmt.Errorf("teranode is already running (PID %d)", pid)
	}

	binary := filepath.Join(projectRoot, "teranode.run")
	if _, err := os.Stat(binary); os.IsNotExist(err) {
		return fmt.Errorf("teranode.run not found - run 'teranode-dev init' to build it")
	}

	// Create logs directory
	logDir := filepath.Join(projectRoot, "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create logs directory: %w", err)
	}

	logFile := filepath.Join(logDir, "teranode.log")

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	settingsContext := "dev." + cfg.DevName

	cmd := exec.Command(binary)
	cmd.Dir = projectRoot
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Env = append(os.Environ(), "SETTINGS_CONTEXT="+settingsContext)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		f.Close()

		return fmt.Errorf("failed to start teranode: %w", err)
	}

	f.Close()

	// Write PID file
	pidPath := filepath.Join(projectRoot, pidFile)
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(cmd.Process.Pid)), 0644); err != nil {
		return fmt.Errorf("failed to write PID file: %w", err)
	}

	fmt.Printf("  teranode started (PID %d)\n", cmd.Process.Pid)
	fmt.Printf("  SETTINGS_CONTEXT=%s\n", settingsContext)
	fmt.Printf("  Logs: %s\n", logFile)

	// Detach - don't wait for the process
	go func() {
		_ = cmd.Wait()
	}()

	return nil
}

// Stop stops the teranode daemon.
func Stop(projectRoot string) error {
	pid, running := isRunning(projectRoot)
	if !running {
		fmt.Println("  teranode is not running.")
		return nil
	}

	fmt.Printf("  Stopping teranode (PID %d)...\n", pid)

	// Send SIGTERM
	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("failed to find process %d: %w", pid, err)
	}

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		// Process might already be gone
		cleanupPID(projectRoot)
		fmt.Println("  Process already stopped.")

		return nil
	}

	// Wait up to 5 seconds for graceful shutdown
	for range 10 {
		time.Sleep(500 * time.Millisecond)

		if err := proc.Signal(syscall.Signal(0)); err != nil {
			cleanupPID(projectRoot)
			fmt.Println("  teranode stopped.")

			return nil
		}
	}

	// Force kill
	fmt.Println("  Graceful shutdown timed out, sending SIGKILL...")

	if err := proc.Signal(syscall.SIGKILL); err != nil {
		cleanupPID(projectRoot)

		return nil
	}

	cleanupPID(projectRoot)
	fmt.Println("  teranode killed.")

	return nil
}

// Status prints whether teranode is running.
func Status(projectRoot string) {
	pid, running := isRunning(projectRoot)
	if running {
		fmt.Printf("\nteranode: running (PID %d)\n", pid)
	} else {
		fmt.Println("\nteranode: stopped")
	}
}

func isRunning(projectRoot string) (int, bool) {
	pidPath := filepath.Join(projectRoot, pidFile)

	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, false
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, false
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return 0, false
	}

	// Check if process is alive
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		return 0, false
	}

	return pid, true
}

func cleanupPID(projectRoot string) {
	pidPath := filepath.Join(projectRoot, pidFile)
	_ = os.Remove(pidPath)

	// Clean up gocore socket files
	socketDir := "/tmp/gocore"

	entries, err := os.ReadDir(socketDir)
	if err != nil {
		return
	}

	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "teranode") {
			_ = os.Remove(filepath.Join(socketDir, e.Name()))
		}
	}
}
