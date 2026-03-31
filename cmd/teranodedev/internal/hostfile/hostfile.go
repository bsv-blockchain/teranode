package hostfile

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const hostsFile = "/etc/hosts"
const kafkaEntry = "127.0.0.1\tkafka-shared"

// EnsureKafkaEntry checks /etc/hosts for the kafka-shared entry and adds it if missing.
func EnsureKafkaEntry() error {
	data, err := os.ReadFile(hostsFile)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", hostsFile, err)
	}

	if strings.Contains(string(data), "kafka-shared") {
		fmt.Println("  /etc/hosts already has kafka-shared entry.")
		return nil
	}

	fmt.Println("  Adding kafka-shared to /etc/hosts (requires sudo)...")

	cmd := exec.Command("sudo", "sh", "-c", fmt.Sprintf("echo '%s' >> %s", kafkaEntry, hostsFile))
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to update /etc/hosts: %w", err)
	}

	fmt.Println("  Added successfully.")

	return nil
}
