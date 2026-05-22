package hostfile

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

const hostsFile = "/etc/hosts"
const kafkaEntry = "127.0.0.1\tkafka-shared"

// EnsureKafkaEntry checks /etc/hosts for the kafka-shared entry and adds it if missing.
func EnsureKafkaEntry() error {
	data, err := os.ReadFile(hostsFile)
	if err != nil {
		return errors.NewProcessingError("failed to read %s", hostsFile, err)
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
		return errors.NewProcessingError("failed to update /etc/hosts", err)
	}

	fmt.Println("  Added successfully.")

	return nil
}
