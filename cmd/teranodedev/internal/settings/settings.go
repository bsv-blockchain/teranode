package settings

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/config"
	"github.com/bsv-blockchain/teranode/errors"
)

const settingsFile = "settings_local.conf"

func markerStart(devName string) string {
	return fmt.Sprintf("# --- teranode-dev auto-generated for dev.%s ---", devName)
}

func markerEnd(devName string) string {
	return fmt.Sprintf("# --- end teranode-dev for dev.%s ---", devName)
}

// Generate writes developer-specific settings into settings_local.conf.
// It replaces any existing auto-generated block for this developer, or appends if none exists.
func Generate(projectRoot string, cfg *config.Config) error {
	path := filepath.Join(projectRoot, settingsFile)

	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return errors.NewProcessingError("failed to read %s", settingsFile, err)
	}

	block := generateBlock(cfg)
	content := string(existing)

	start := markerStart(cfg.DevName)
	end := markerEnd(cfg.DevName)

	startIdx := strings.Index(content, start)
	endIdx := strings.Index(content, end)

	if startIdx >= 0 && endIdx >= 0 {
		// Replace existing block
		content = content[:startIdx] + block + content[endIdx+len(end):]
	} else {
		// Append new block
		if content != "" && !strings.HasSuffix(content, "\n") {
			content += "\n"
		}

		content += "\n" + block + "\n"
	}

	return os.WriteFile(path, []byte(content), 0644)
}

// HasEntries checks if settings_local.conf has auto-generated entries for the given developer.
func HasEntries(projectRoot, devName string) bool {
	path := filepath.Join(projectRoot, settingsFile)

	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}

	return strings.Contains(string(data), markerStart(devName))
}

func generateBlock(cfg *config.Config) string {
	ctx := "dev." + cfg.DevName

	// Capitalize first letter for clientName
	displayName := cfg.DevName
	if len(displayName) > 0 {
		displayName = strings.ToUpper(displayName[:1]) + displayName[1:]
	}

	lines := []string{
		markerStart(cfg.DevName),
		fmt.Sprintf("clientName.%s = %s", ctx, displayName),
		fmt.Sprintf("network.%s = %s", ctx, cfg.Network),
		fmt.Sprintf("utxostore.%s = %s", ctx, utxoConnectionString(cfg)),
	}

	if cfg.EnableTracing {
		lines = append(lines, fmt.Sprintf("tracing_enabled.%s = true", ctx))
	} else {
		lines = append(lines, fmt.Sprintf("tracing_enabled.%s = false", ctx))
	}

	if cfg.UseKafka {
		lines = append(lines, fmt.Sprintf("KAFKA_SCHEMA.%s = kafka", ctx))
	} else {
		lines = append(lines, fmt.Sprintf("KAFKA_SCHEMA.%s = memory", ctx))
	}

	lines = append(lines, fmt.Sprintf("local_test_start_from_state.%s = RUNNING", ctx))
	lines = append(lines, markerEnd(cfg.DevName))

	return strings.Join(lines, "\n")
}

func utxoConnectionString(cfg *config.Config) string {
	switch cfg.UTXOBackend {
	case "postgres":
		return "postgres://teranode:teranode@localhost:5432/teranode"
	case "aerospike":
		return "aerospike://localhost:3000/utxo-store?set=utxo&externalStore=file://${DATADIR}/external"
	default:
		return "sqlite:///utxostore"
	}
}
