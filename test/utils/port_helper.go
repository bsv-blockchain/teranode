package utils

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
)

// WaitForPortsToBeAvailable waits until all specified ports are available (not in use)
// Returns nil if all ports become available within the timeout, error otherwise
func WaitForPortsToBeAvailable(ctx context.Context, ports []int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return errors.NewProcessingError("timeout waiting for ports to be available")
		}

		allAvailable := true

		for _, port := range ports {
			if !isPortAvailable(port) {
				allAvailable = false
				break
			}
		}

		if allAvailable {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// isPortAvailable checks if a specific port is available by attempting to bind to it
func isPortAvailable(port int) bool {
	addr := fmt.Sprintf(":%d", port)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}

	l.Close()

	return true
}

// WaitForDirRemoval polls to verify that a directory has been completely removed
// Returns nil when directory is confirmed gone, or error if timeout occurs
func WaitForDirRemoval(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			return nil // Directory is confirmed gone
		}

		time.Sleep(10 * time.Millisecond)
	}

	return errors.NewProcessingError("timeout waiting for directory removal: %s", path)
}
