package wait

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
)

// ForPortsReady polls a list of TCP ports on a given host until they become available
// or the context times out.
// It returns an error if the context times out before all ports are ready.
func ForPortsReady(ctx context.Context, host string, ports []int, maxWait time.Duration, checkInterval time.Duration) error {
	if len(ports) == 0 {
		return nil // Nothing to wait for
	}

	// log.Printf("Waiting up to %v for ports %v on host '%s' to become available...", maxWait, ports, host)

	ctxWait, cancelWait := context.WithTimeout(ctx, maxWait)
	defer cancelWait()

	readyPorts := make(map[int]bool)
	portsStillWaiting := make([]string, 0, len(ports))

	for _, p := range ports {
		readyPorts[p] = false

		portsStillWaiting = append(portsStillWaiting, net.JoinHostPort(host, strconv.Itoa(p)))
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	allReady := false
	for !allReady {
		select {
		case <-ctxWait.Done():
			if !errors.Is(ctxWait.Err(), context.DeadlineExceeded) {
				return nil
			}

			return errors.NewServiceError("timeout (%v) waiting for ports. Ports still unavailable: [%s].",
				maxWait, strings.Join(portsStillWaiting, ", "), ctxWait.Err())
		case <-ticker.C:
			numReady := 0
			nextPortsStillWaiting := []string{} // Build the list for the next iteration/logging

			for port, isReady := range readyPorts {
				if !isReady {
					// Use a short timeout for the dial attempt
					address := net.JoinHostPort(host, strconv.Itoa(port))

					conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
					if err == nil {
						conn.Close()

						readyPorts[port] = true

						// log.Printf("Port %d on host '%s' is ready.", port, host)
					} else {
						// Not ready yet, add to the list for next check
						nextPortsStillWaiting = append(nextPortsStillWaiting, address)
					}
				}
				// Count how many are ready *now*
				if readyPorts[port] {
					numReady++
				}
			}

			portsStillWaiting = nextPortsStillWaiting // Update the list for the next potential timeout message

			// Check if all ports passed in the 'ports' slice are now ready
			if numReady == len(ports) {
				allReady = true

				// log.Printf("All required ports (%v) on host '%s' are ready.", ports, host)
				// } else if len(portsStillWaiting) > 0 {
				// Optional: Log periodically which ports are still being waited on
				// log.Printf("Still waiting for ports: %v", portsStillWaiting)
			}
		}
	}

	return nil // All ports are ready
}

// ForPortsFree polls a list of TCP ports on a given host until they become unavailable
// (connection attempts fail), indicating they are likely free.
// It returns an error if the context times out before all ports are free.
func ForPortsFree(ctx context.Context, host string, ports []int, maxWait time.Duration, checkInterval time.Duration) error {
	if len(ports) == 0 {
		return nil // Nothing to wait for
	}

	// log.Printf("Waiting up to %v for ports [%v] on host '%s' to become free...", maxWait, ports, host)

	ctxWait, cancelWait := context.WithTimeout(ctx, maxWait)
	defer cancelWait()

	portsStillBound := make(map[int]bool)

	for _, p := range ports {
		portsStillBound[p] = true // Assume bound initially
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	allFree := false
	for !allFree {
		select {
		case <-ctxWait.Done():
			if !errors.Is(ctxWait.Err(), context.DeadlineExceeded) {
				// no timeout occurred, so we can return nil
				return nil
			}

			boundPortsList := []string{}

			for port, isBound := range portsStillBound {
				if isBound {
					address := net.JoinHostPort(host, strconv.Itoa(port))
					boundPortsList = append(boundPortsList, address)
				}
			}

			return errors.NewServiceError("timeout (%v) waiting for ports to become free. Ports still bound: [%s].",
				maxWait, strings.Join(boundPortsList, ", "))
		case <-ticker.C:
			numStillBound := 0
			nextPortsStillBoundLog := []string{}

			for port, isBound := range portsStillBound {
				if isBound {
					address := net.JoinHostPort(host, strconv.Itoa(port))

					conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond) // Use a very short timeout
					if err != nil {
						// Error dialing means the port is likely free
						portsStillBound[port] = false

						// log.Printf("Port %d on host '%s' is now free.", port, host)
					} else {
						// Connection succeeded, port is still bound
						conn.Close() // Close the successful connection immediately

						numStillBound++

						nextPortsStillBoundLog = append(nextPortsStillBoundLog, address)
					}
				}
			}

			if numStillBound == 0 {
				allFree = true

				// log.Printf("All specified ports %v on host '%s' are now free.", ports, host)
				// } else {
				// log.Printf("Still waiting for ports to become free on host '%s': %v", host, nextPortsStillBoundLog)
			}
		}
	}

	return nil
}

// ForDockerComposeProjectDown polls docker to check if any containers
// belonging to the specified compose project name are still running or exist.
// It waits until no containers are found or the timeout is reached.
func ForDockerComposeProjectDown(ctx context.Context, projectName string, timeout time.Duration, checkInterval time.Duration) error {
	// log.Printf("Waiting up to %v for Docker Compose project '%s' containers to disappear...", timeout, projectName)

	startTime := time.Now()

	ticker := time.NewTicker(checkInterval)

	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Original context cancelled
			return errors.NewServiceError("context cancelled while waiting for project '%s' containers to disappear", projectName, ctx.Err())
		case <-time.After(timeout):
			// Overall timeout for this wait function
			// Check one last time before timing out
			remainingContainers, err := getComposeProjectContainers(projectName)
			if err != nil {
				log.Printf("Error checking container status on timeout for project %s: %v", projectName, err)
			}

			if len(remainingContainers) == 0 {
				log.Printf("Confirmed project '%s' containers disappeared just before timeout.", projectName)
				return nil // Success right at the end
			}

			return errors.NewServiceError("timeout (%v) waiting for Docker Compose project '%s' containers to disappear. Still present: %s",
				timeout, projectName, strings.Join(remainingContainers, ", "))

		case <-ticker.C:
			// Time to check container status
			remainingContainers, err := getComposeProjectContainers(projectName)
			if err != nil {
				// Log error but keep trying
				log.Printf("Error checking container status for project %s (will retry): %v", projectName, err)
				continue
			}

			if len(remainingContainers) == 0 {
				log.Printf("All containers for Docker Compose project '%s' confirmed gone after %v.", projectName, time.Since(startTime))
				return nil // Success
			}
			// Log periodically which containers are still present
			log.Printf("Waiting for project '%s' containers to disappear. Still present: %s", projectName, strings.Join(remainingContainers, ", "))
		}
	}
}

// getComposeProjectContainers executes docker ps to find containers for a project.
func getComposeProjectContainers(projectName string) ([]string, error) {
	projectFilter := fmt.Sprintf("label=com.docker.compose.project=%s", projectName)
	// List containers (all states -a) matching the project label, output only names (--format {{.Names}})
	cmd := exec.Command("docker", "ps", "-a", "--filter", projectFilter, "--format", "{{.Names}}")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		// If docker command itself fails
		return nil, errors.NewServiceError("failed to run 'docker ps' for project %s, stderr: %s", projectName, err, stderr.String(), err)
	}

	output := strings.TrimSpace(stdout.String())
	if output == "" {
		return []string{}, nil // No containers found
	}

	// Split names by newline
	containerNames := strings.Split(output, "\n")
	// Filter out any potential empty strings from splitting
	actualNames := make([]string, 0, len(containerNames))

	for _, name := range containerNames {
		trimmedName := strings.TrimSpace(name)
		if trimmedName != "" {
			actualNames = append(actualNames, trimmedName)
		}
	}

	return actualNames, nil
}
