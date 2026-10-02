package settings

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/config"
	"github.com/bsv-blockchain/teranode/errors"
)

// rpcPassChars are the characters used to build a generated dev RPC password.
// URL-safe so the value can be used unescaped in settings files and URLs.
const rpcPassChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

// rpcPassLength is the length of a generated dev RPC password.
const rpcPassLength = 32

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

	content := string(existing)

	start := markerStart(cfg.DevName)
	end := markerEnd(cfg.DevName)

	startIdx := strings.Index(content, start)
	endIdx := strings.Index(content, end)

	var existingBlock string
	if startIdx >= 0 && endIdx >= 0 {
		existingBlock = content[startIdx : endIdx+len(end)]
	}

	block, err := generateBlock(cfg, existingBlock)
	if err != nil {
		return err
	}

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

func generateBlock(cfg *config.Config, existingBlock string) (string, error) {
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

	rpcPass := existingRPCPass(existingBlock, ctx)
	if rpcPass == "" {
		var err error

		rpcPass, err = randomRPCPass()
		if err != nil {
			return "", err
		}
	}

	lines = append(lines,
		fmt.Sprintf("rpc_user.%s = %s", ctx, cfg.DevName),
		fmt.Sprintf("rpc_pass.%s = %s", ctx, rpcPass),
	)

	lines = append(lines, markerEnd(cfg.DevName))

	return strings.Join(lines, "\n"), nil
}

// existingRPCPass extracts the rpc_pass value for ctx from a previously generated
// block, so re-running init doesn't rotate the developer's RPC password.
func existingRPCPass(block, ctx string) string {
	if block == "" {
		return ""
	}

	prefix := fmt.Sprintf("rpc_pass.%s = ", ctx)

	for _, line := range strings.Split(block, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}

	return ""
}

// randomRPCPass generates a random URL-safe RPC password for a fresh dev context.
func randomRPCPass() (string, error) {
	buf := make([]byte, rpcPassLength)

	if _, err := rand.Read(buf); err != nil {
		return "", errors.NewProcessingError("failed to generate dev RPC password", err)
	}

	for i, b := range buf {
		buf[i] = rpcPassChars[int(b)%len(rpcPassChars)]
	}

	return string(buf), nil
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
