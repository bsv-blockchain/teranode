package wizard

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/config"
)

var validName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9.]*$`)

// Run runs the interactive setup wizard and returns the resulting config.
func Run() (*config.Config, error) {
	scanner := bufio.NewScanner(os.Stdin)
	cfg := &config.Config{}

	// Developer name
	name, err := askString(scanner,
		"What is your developer name? (used for SETTINGS_CONTEXT=dev.<name>)",
		"",
	)
	if err != nil {
		return nil, err
	}

	if !validName.MatchString(name) {
		return nil, fmt.Errorf("invalid name %q - use letters, numbers, and dots only, starting with a letter", name)
	}

	cfg.DevName = name

	// UTXO backend
	backend, err := askChoice(scanner,
		"UTXO storage backend:",
		[]string{
			"sqlite    - simplest, no containers needed (default)",
			"postgres  - realistic, needs Docker",
			"aerospike - high-performance, needs Docker + build tag",
		},
		0,
	)
	if err != nil {
		return nil, err
	}

	cfg.UTXOBackend = []string{"sqlite", "postgres", "aerospike"}[backend]

	// Network
	network, err := askChoice(scanner,
		"Network:",
		[]string{
			"regtest - default for local dev (default)",
			"testnet",
			"mainnet",
		},
		0,
	)
	if err != nil {
		return nil, err
	}

	cfg.Network = []string{"regtest", "testnet", "mainnet"}[network]

	// Kafka
	cfg.UseKafka, err = askYesNo(scanner, "Use Docker-based Kafka? (default: no, uses in-memory)", false)
	if err != nil {
		return nil, err
	}

	// Monitoring
	cfg.EnableMonitoring, err = askYesNo(scanner, "Enable monitoring (Grafana + Prometheus)?", false)
	if err != nil {
		return nil, err
	}

	// Tracing
	cfg.EnableTracing, err = askYesNo(scanner, "Enable tracing (Jaeger)?", false)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func askString(scanner *bufio.Scanner, prompt, defaultVal string) (string, error) {
	if defaultVal != "" {
		fmt.Printf("%s [%s]\n> ", prompt, defaultVal)
	} else {
		fmt.Printf("%s\n> ", prompt)
	}

	if !scanner.Scan() {
		return "", fmt.Errorf("no input")
	}

	val := strings.TrimSpace(scanner.Text())
	if val == "" {
		return defaultVal, nil
	}

	return val, nil
}

func askChoice(scanner *bufio.Scanner, prompt string, options []string, defaultIdx int) (int, error) {
	fmt.Println(prompt)

	for i, opt := range options {
		fmt.Printf("  [%d] %s\n", i+1, opt)
	}

	fmt.Print("> ")

	if !scanner.Scan() {
		return defaultIdx, nil
	}

	val := strings.TrimSpace(scanner.Text())
	if val == "" {
		return defaultIdx, nil
	}

	for i := range options {
		if val == fmt.Sprintf("%d", i+1) {
			return i, nil
		}
	}

	return 0, fmt.Errorf("invalid choice %q - enter a number 1-%d", val, len(options))
}

func askYesNo(scanner *bufio.Scanner, prompt string, defaultVal bool) (bool, error) {
	defStr := "y/N"
	if defaultVal {
		defStr = "Y/n"
	}

	fmt.Printf("%s [%s]\n> ", prompt, defStr)

	if !scanner.Scan() {
		return defaultVal, nil
	}

	val := strings.TrimSpace(strings.ToLower(scanner.Text()))
	if val == "" {
		return defaultVal, nil
	}

	switch val {
	case "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return defaultVal, nil
	}
}
