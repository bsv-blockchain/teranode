package docker

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/config"
	"github.com/bsv-blockchain/teranode/errors"
)

type service struct {
	name        string
	composeDir  string
	composeFile string // empty = docker-compose.yml
	dataDirs    []string
	healthPort  int
	isDocker    bool // false for jaeger which uses docker run directly
}

func services(cfg *config.Config) []service {
	var svcs []service

	if cfg.UTXOBackend == "postgres" {
		svcs = append(svcs, service{
			name:       "postgres",
			composeDir: "deploy/docker/postgres",
			dataDirs:   []string{"postgres"},
			healthPort: 5432,
			isDocker:   true,
		})
	}

	if cfg.UTXOBackend == "aerospike" {
		svcs = append(svcs, service{
			name:        "aerospike",
			composeDir:  "deploy/docker/aerospike",
			composeFile: "docker-compose-ee.yml",
			dataDirs:    []string{"aerospike/data", "aerospike/smd"},
			healthPort:  3000,
			isDocker:    true,
		})
	}

	if cfg.UseKafka {
		svcs = append(svcs, service{
			name:       "kafka",
			composeDir: "deploy/docker/kafka",
			dataDirs:   nil, // ephemeral
			healthPort: 9092,
			isDocker:   true,
		})
	}

	if cfg.EnableMonitoring {
		svcs = append(svcs, service{
			name:       "monitoring",
			composeDir: "deploy/docker/monitoring",
			dataDirs:   []string{"prometheus", "grafana"},
			healthPort: 3005,
			isDocker:   true,
		})
	}

	if cfg.EnableTracing {
		svcs = append(svcs, service{
			name:       "jaeger",
			isDocker:   false,
			healthPort: 16686,
		})
	}

	return svcs
}

// CreateDataDirs creates the necessary data directories.
func CreateDataDirs(projectRoot string, cfg *config.Config) error {
	for _, svc := range services(cfg) {
		for _, dir := range svc.dataDirs {
			path := filepath.Join(projectRoot, "data", dir)
			if err := os.MkdirAll(path, 0755); err != nil {
				return errors.NewProcessingError("failed to create %s", path, err)
			}
		}
	}

	return nil
}

// Up starts all configured Docker services.
func Up(projectRoot string, cfg *config.Config) error {
	dataPath, err := filepath.Abs(filepath.Join(projectRoot, "data"))
	if err != nil {
		return err
	}

	for _, svc := range services(cfg) {
		// Skip if the service is already reachable
		if svc.healthPort > 0 && isPortOpen(svc.healthPort) {
			fmt.Printf("  %s already running on port %d, skipping.\n", svc.name, svc.healthPort)
			continue
		}

		fmt.Printf("  Starting %s...\n", svc.name)

		if !svc.isDocker {
			if err := startJaeger(); err != nil {
				return errors.NewProcessingError("failed to start %s", svc.name, err)
			}
		} else {
			if err := composeUp(projectRoot, svc, dataPath); err != nil {
				return errors.NewProcessingError("failed to start %s", svc.name, err)
			}
		}

		// Wait for health
		if svc.healthPort > 0 {
			fmt.Printf("  Waiting for %s (port %d)...\n", svc.name, svc.healthPort)
			if err := waitForPort(svc.healthPort, 60*time.Second); err != nil {
				return errors.NewProcessingError("%s did not become healthy", svc.name, err)
			}

			fmt.Printf("  %s is ready.\n", svc.name)
		}
	}

	return nil
}

// Down stops all configured Docker services.
func Down(projectRoot string, cfg *config.Config) error {
	dataPath, err := filepath.Abs(filepath.Join(projectRoot, "data"))
	if err != nil {
		return err
	}

	for _, svc := range services(cfg) {
		fmt.Printf("  Stopping %s...\n", svc.name)

		if !svc.isDocker {
			stopJaeger()
		} else {
			composeDown(projectRoot, svc, dataPath)
		}
	}

	fmt.Println("  All services stopped.")

	return nil
}

// Status prints the status of all configured services.
func Status(projectRoot string, cfg *config.Config) {
	fmt.Println("\nService status:")

	for _, svc := range services(cfg) {
		port := svc.healthPort
		running := isPortOpen(port)

		status := "stopped"
		if running {
			status = "running"
		}

		fmt.Printf("  %-15s port %-5d %s\n", svc.name, port, status)
	}
}

// CheckPorts checks availability of ports for configured services.
func CheckPorts(cfg *config.Config) {
	for _, svc := range services(cfg) {
		if svc.healthPort == 0 {
			continue
		}

		open := isPortOpen(svc.healthPort)

		status := "available"
		if open {
			status = "in use"
		}

		fmt.Printf("  %-15s port %-5d %s\n", svc.name, svc.healthPort, status)
	}
}

// Clean removes data directories after confirmation.
func Clean(projectRoot string, cfg *config.Config) error {
	dataDir := filepath.Join(projectRoot, "data")

	// Check if any containers are running
	for _, svc := range services(cfg) {
		if svc.healthPort > 0 && isPortOpen(svc.healthPort) {
			return errors.NewProcessingError("%s is still running on port %d - run 'teranode-dev down' first", svc.name, svc.healthPort)
		}
	}

	fmt.Printf("This will delete: %s\n", dataDir)
	fmt.Print("Type 'yes' to confirm: ")

	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() || strings.TrimSpace(scanner.Text()) != "yes" {
		fmt.Println("Cancelled.")
		return nil
	}

	if err := os.RemoveAll(dataDir); err != nil {
		return errors.NewProcessingError("failed to remove data directory", err)
	}

	fmt.Println("Data directory removed.")

	return nil
}

func composeUp(projectRoot string, svc service, dataPath string) error {
	dir := filepath.Join(projectRoot, svc.composeDir)
	args := []string{"compose"}

	if svc.composeFile != "" {
		args = append(args, "-f", svc.composeFile)
	}

	args = append(args, "up", "-d")

	cmd := exec.Command("docker", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "DATA_PATH="+dataPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func composeDown(projectRoot string, svc service, dataPath string) {
	dir := filepath.Join(projectRoot, svc.composeDir)
	args := []string{"compose"}

	if svc.composeFile != "" {
		args = append(args, "-f", svc.composeFile)
	}

	args = append(args, "down")

	cmd := exec.Command("docker", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "DATA_PATH="+dataPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
}

func startJaeger() error {
	// Check if already running
	out, err := exec.Command("docker", "ps", "-q", "-f", "name=jaeger").Output()
	if err == nil && strings.TrimSpace(string(out)) != "" {
		return nil // already running
	}

	// Remove stopped container if it exists
	_ = exec.Command("docker", "rm", "-f", "jaeger").Run()

	// Ensure the shared network exists
	_ = exec.Command("docker", "network", "create", "my-teranode-network").Run()

	cmd := exec.Command("docker", "run", "-d", "--name", "jaeger",
		"--network", "my-teranode-network",
		"-p", "127.0.0.1:16686:16686",
		"-p", "127.0.0.1:4317:4317",
		"-p", "127.0.0.1:4318:4318",
		"-p", "127.0.0.1:5778:5778",
		"-p", "127.0.0.1:9411:9411",
		"cr.jaegertracing.io/jaegertracing/jaeger:2.8.0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func stopJaeger() {
	_ = exec.Command("docker", "stop", "jaeger").Run()
	_ = exec.Command("docker", "rm", "jaeger").Run()
}

func waitForPort(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if isPortOpen(port) {
			return nil
		}

		time.Sleep(2 * time.Second)
	}

	return errors.NewProcessingError("timeout waiting for port %d", port)
}

func isPortOpen(port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		return false
	}

	conn.Close()

	return true
}
