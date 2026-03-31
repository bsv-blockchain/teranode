package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/cmd/diagnose"
	"github.com/bsv-blockchain/teranode/cmd/logs"
	"github.com/bsv-blockchain/teranode/cmd/monitor"
	cmdSettings "github.com/bsv-blockchain/teranode/cmd/settings"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/build"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/config"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/docker"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/hostfile"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/prereq"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/process"
	devsettings "github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/settings"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/wizard"
	"github.com/bsv-blockchain/teranode/errors"
	teranodeSettings "github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	cli "github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:  "teranode-dev",
		Usage: "Interactive local development setup for teranode",
		Commands: []*cli.Command{
			initCmd(),
			upCmd(),
			downCmd(),
			statusCmd(),
			doctorCmd(),
			cleanCmd(),
			startCmd(),
			stopCmd(),
			monitorCmd(),
			logsCmd(),
			settingsCmd(),
			diagnoseCmd(),
			generateCmd(),
			rpcCmd(),
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func initCmd() *cli.Command {
	return &cli.Command{
		Name:  "init",
		Usage: "Interactive setup wizard for local development",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "non-interactive",
				Usage: "Run without prompts (requires all flags to be set)",
			},
			&cli.StringFlag{
				Name:  "name",
				Usage: "Developer name (for SETTINGS_CONTEXT=dev.<name>)",
			},
			&cli.StringFlag{
				Name:  "utxo",
				Usage: "UTXO backend: sqlite, postgres, aerospike",
			},
			&cli.StringFlag{
				Name:  "network",
				Usage: "Network: regtest, testnet, mainnet",
			},
			&cli.BoolFlag{
				Name:  "kafka",
				Usage: "Use Docker-based Kafka instead of in-memory",
			},
			&cli.BoolFlag{
				Name:  "monitoring",
				Usage: "Enable Grafana + Prometheus",
			},
			&cli.BoolFlag{
				Name:  "tracing",
				Usage: "Enable Jaeger tracing",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			projectRoot, err := config.FindProjectRoot()
			if err != nil {
				return err
			}

			// Load existing config for defaults (if re-running init)
			existing, _ := config.Load(projectRoot)

			var cfg *config.Config

			if cmd.Bool("non-interactive") {
				cfg = &config.Config{
					DevName:          cmd.String("name"),
					UTXOBackend:      cmd.String("utxo"),
					Network:          cmd.String("network"),
					UseKafka:         cmd.Bool("kafka"),
					EnableMonitoring: cmd.Bool("monitoring"),
					EnableTracing:    cmd.Bool("tracing"),
				}

				if cfg.DevName == "" || cfg.UTXOBackend == "" || cfg.Network == "" {
					return errors.NewProcessingError("--name, --utxo, and --network are required in non-interactive mode")
				}
			} else {
				cfg, err = wizard.Run(existing)
				if err != nil {
					return err
				}
			}

			cfg.ProjectRoot = projectRoot
			cfg.DataDir = "./data"

			// Check prerequisites
			fmt.Println("\nChecking prerequisites...")
			results := prereq.CheckAll()
			prereq.PrintResults(results)

			if prereq.HasFailures(results) {
				return errors.NewProcessingError("prerequisite checks failed, fix the issues above and try again")
			}

			// Save config early so choices persist even if later steps fail
			if err := config.Save(projectRoot, cfg); err != nil {
				return errors.NewProcessingError("failed to save config", err)
			}

			// Generate settings_local.conf
			fmt.Println("\nGenerating settings_local.conf...")
			if err := devsettings.Generate(projectRoot, cfg); err != nil {
				return errors.NewProcessingError("failed to generate settings", err)
			}

			fmt.Println("  Done.")

			// Create data directories
			fmt.Println("\nCreating data directories...")
			if err := docker.CreateDataDirs(projectRoot, cfg); err != nil {
				return errors.NewProcessingError("failed to create data directories", err)
			}

			fmt.Println("  Done.")

			// Handle /etc/hosts for Kafka
			if cfg.UseKafka {
				fmt.Println("\nChecking /etc/hosts for kafka-shared...")
				if err := hostfile.EnsureKafkaEntry(); err != nil {
					fmt.Printf("  Warning: %v\n", err)
					fmt.Println("  You may need to manually add '127.0.0.1 kafka-shared' to /etc/hosts")
				}
			}

			// Start Docker containers
			fmt.Println("\nStarting Docker containers...")
			if err := docker.Up(projectRoot, cfg); err != nil {
				return errors.NewProcessingError("failed to start containers", err)
			}

			// Build teranode
			fmt.Println("\nBuilding teranode...")
			if err := build.Build(projectRoot, cfg); err != nil {
				return errors.NewProcessingError("failed to build teranode", err)
			}

			// Print summary
			fmt.Println("\n" + "=" + repeatStr("=", 59))
			fmt.Println("  Setup complete!")
			fmt.Println(repeatStr("=", 60))
			fmt.Printf("\n  Add this to your shell profile (~/.zshrc or ~/.bashrc):\n")
			fmt.Printf("    export SETTINGS_CONTEXT=dev.%s\n", cfg.DevName)
			fmt.Printf("\n  Then start teranode:\n")
			fmt.Printf("    teranode-dev start\n")
			fmt.Printf("\n  Or run directly:\n")
			fmt.Printf("    SETTINGS_CONTEXT=dev.%s ./teranode.run\n", cfg.DevName)
			fmt.Println()

			return nil
		},
	}
}

func upCmd() *cli.Command {
	return &cli.Command{
		Name:  "up",
		Usage: "Start infrastructure containers",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			projectRoot, cfg := loadConfigOrHint()
			if cfg == nil {
				return nil
			}

			return docker.Up(projectRoot, cfg)
		},
	}
}

func downCmd() *cli.Command {
	return &cli.Command{
		Name:  "down",
		Usage: "Stop infrastructure containers",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			projectRoot, cfg := loadConfigOrHint()
			if cfg == nil {
				return nil
			}

			return docker.Down(projectRoot, cfg)
		},
	}
}

func statusCmd() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "Show running services, ports, and health",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			projectRoot, cfg := loadConfigOrHint()
			if projectRoot == "" {
				return nil
			}

			if cfg != nil {
				docker.Status(projectRoot, cfg)
			}

			process.Status(projectRoot, cfg)

			return nil
		},
	}
}

func doctorCmd() *cli.Command {
	return &cli.Command{
		Name:  "doctor",
		Usage: "Check prerequisites and configuration",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			fmt.Println("Checking prerequisites...")
			fmt.Println()

			results := prereq.CheckAll()
			prereq.PrintResults(results)

			// Also check config if it exists
			projectRoot, err := config.FindProjectRoot()
			if err != nil {
				fmt.Println("\nProject root: NOT FOUND")
				return nil
			}

			fmt.Printf("\nProject root: %s\n", projectRoot)

			cfg, err := config.Load(projectRoot)
			if err != nil {
				fmt.Println("Config (.teranode-dev.yaml): NOT FOUND - run 'teranode-dev init'")
				return nil
			}

			fmt.Println("Config (.teranode-dev.yaml): OK")
			fmt.Printf("  Developer: %s\n", cfg.DevName)
			fmt.Printf("  UTXO backend: %s\n", cfg.UTXOBackend)
			fmt.Printf("  Network: %s\n", cfg.Network)
			fmt.Printf("  Kafka: %v\n", cfg.UseKafka)
			fmt.Printf("  Monitoring: %v\n", cfg.EnableMonitoring)
			fmt.Printf("  Tracing: %v\n", cfg.EnableTracing)

			// Check settings_local.conf
			if devsettings.HasEntries(projectRoot, cfg.DevName) {
				fmt.Println("\nsettings_local.conf: OK (has entries for dev." + cfg.DevName + ")")
			} else {
				fmt.Println("\nsettings_local.conf: MISSING entries for dev." + cfg.DevName)
			}

			// Check SETTINGS_CONTEXT
			sc := os.Getenv("SETTINGS_CONTEXT")
			expected := "dev." + cfg.DevName

			if sc == expected {
				fmt.Printf("\nSETTINGS_CONTEXT: OK (%s)\n", sc)
			} else if sc == "" {
				fmt.Println("\nSETTINGS_CONTEXT: NOT SET")
				fmt.Printf("  Add to your shell profile: export SETTINGS_CONTEXT=%s\n", expected)
			} else {
				fmt.Printf("\nSETTINGS_CONTEXT: %s (expected %s)\n", sc, expected)
			}

			// Check ports
			fmt.Println("\nPort availability:")
			docker.CheckPorts(cfg)

			// Check chain consistency
			fmt.Println("\nChain consistency:")
			storeURL, dataFolder := loadBlockchainSettings(cfg)
			chainResult := prereq.CheckChain(cfg.Network, storeURL, dataFolder)
			if chainResult.NoDatabase {
				fmt.Println("  No blockchain database found (fresh setup)")
			} else if chainResult.OK {
				fmt.Printf("  OK - stored genesis matches configured network (%s)\n", cfg.Network)
			} else {
				handleChainMismatch(projectRoot, cfg, storeURL, dataFolder, chainResult)
			}

			return nil
		},
	}
}

func cleanCmd() *cli.Command {
	return &cli.Command{
		Name:  "clean",
		Usage: "Wipe data directory (with confirmation)",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			projectRoot, cfg := loadConfigOrHint()
			if cfg == nil {
				return nil
			}

			return docker.Clean(projectRoot, cfg)
		},
	}
}

func startCmd() *cli.Command {
	return &cli.Command{
		Name:  "start",
		Usage: "Start teranode daemon with log rotation",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			projectRoot, cfg := loadConfigOrHint()
			if cfg == nil {
				return nil
			}

			// Check chain consistency before starting
			storeURL, dataFolder := loadBlockchainSettings(cfg)
			chainResult := prereq.CheckChain(cfg.Network, storeURL, dataFolder)
			if !chainResult.OK && !chainResult.NoDatabase {
				fmt.Println("Chain consistency check failed:")
				handleChainMismatch(projectRoot, cfg, storeURL, dataFolder, chainResult)

				// Re-check after potential fix
				chainResult = prereq.CheckChain(cfg.Network, storeURL, dataFolder)
				if !chainResult.OK && !chainResult.NoDatabase {
					return errors.NewProcessingError("chain mismatch not resolved, cannot start")
				}
			}

			return process.Start(projectRoot, cfg)
		},
	}
}

func stopCmd() *cli.Command {
	return &cli.Command{
		Name:  "stop",
		Usage: "Stop teranode daemon",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			projectRoot, _ := loadConfigOrHint()
			if projectRoot == "" {
				return nil
			}

			return process.Stop(projectRoot)
		},
	}
}

func generateCmd() *cli.Command {
	return &cli.Command{
		Name:      "generate",
		Usage:     "Generate blocks (regtest only)",
		ArgsUsage: "[numblocks]",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			_, cfg := loadConfigOrHint()
			if cfg == nil {
				return nil
			}

			// Check if network supports generation
			params, err := chaincfg.GetChainParams(cfg.Network)
			if err != nil {
				return errors.NewProcessingError("unknown network: %s", cfg.Network)
			}

			if !params.GenerateSupported {
				return errors.NewProcessingError("block generation is not supported on %s", cfg.Network)
			}

			numBlocks := 1
			if cmd.Args().Len() > 0 {
				n, err := strconv.Atoi(cmd.Args().First())
				if err != nil || n <= 0 {
					return errors.NewProcessingError("invalid number of blocks: %s", cmd.Args().First())
				}

				numBlocks = n
			}

			// Load settings for RPC connection
			tSettings := teranodeSettings.NewSettings("dev." + cfg.DevName)

			rpcURL := "http://localhost:9292"
			if tSettings.RPC.RPCListenerURL != nil {
				rpcURL = tSettings.RPC.RPCListenerURL.String()
			}

			rpcUser := tSettings.RPC.RPCUser
			rpcPass := tSettings.RPC.RPCPass

			// Call the generate RPC
			payload := fmt.Sprintf(`{"jsonrpc":"1.0","id":"teranode-dev","method":"generate","params":[%d]}`, numBlocks)

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcURL, strings.NewReader(payload))
			if err != nil {
				return err
			}

			req.Header.Set("Content-Type", "application/json")
			req.SetBasicAuth(rpcUser, rpcPass)

			client := &http.Client{Timeout: 120 * time.Second}

			resp, err := client.Do(req)
			if err != nil {
				return errors.NewProcessingError("RPC request failed", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return errors.NewProcessingError("failed to read response", err)
			}

			// Parse response to extract block hashes or error
			var rpcResp struct {
				Result []string `json:"result"`
				Error  *struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}

			if err := json.Unmarshal(body, &rpcResp); err != nil {
				return errors.NewProcessingError("failed to parse response: %s", string(body))
			}

			if rpcResp.Error != nil {
				return errors.NewProcessingError("RPC error: %s", rpcResp.Error.Message)
			}

			fmt.Printf("Generated %d block(s):\n", len(rpcResp.Result))

			for _, hash := range rpcResp.Result {
				fmt.Printf("  %s\n", hash)
			}

			return nil
		},
	}
}

func rpcCmd() *cli.Command {
	return &cli.Command{
		Name:      "rpc",
		Usage:     "Call Bitcoin JSON-RPC methods",
		ArgsUsage: "[method] [params...]",
		Description: `With no arguments, lists all available RPC commands.
With a method name, calls that RPC method with optional parameters.

Examples:
  teranode-dev rpc                          List all commands
  teranode-dev rpc help getblock            Detailed help for getblock
  teranode-dev rpc getblockchaininfo        Call with no params
  teranode-dev rpc getblockhash 0           Call with params
  teranode-dev rpc getblock <hash> 2        Multiple params`,
		Action: func(ctx context.Context, cmd *cli.Command) error {
			_, cfg := loadConfigOrHint()
			if cfg == nil {
				return nil
			}

			tSettings := teranodeSettings.NewSettings("dev." + cfg.DevName)

			rpcURL := "http://localhost:9292"
			if tSettings.RPC.RPCListenerURL != nil {
				rpcURL = tSettings.RPC.RPCListenerURL.String()
			}

			// Determine method and params
			method := "help"
			var params []json.RawMessage

			if cmd.Args().Len() > 0 {
				method = cmd.Args().First()

				for i := 1; i < cmd.Args().Len(); i++ {
					arg := cmd.Args().Get(i)
					// Try parsing as JSON (handles numbers, booleans, objects, arrays)
					if json.Valid([]byte(arg)) {
						params = append(params, json.RawMessage(arg))
					} else {
						// Treat as string
						quoted, _ := json.Marshal(arg)
						params = append(params, json.RawMessage(quoted))
					}
				}
			}

			// Build JSON-RPC request
			rpcReq := struct {
				JSONRPC string            `json:"jsonrpc"`
				ID      string            `json:"id"`
				Method  string            `json:"method"`
				Params  []json.RawMessage `json:"params"`
			}{
				JSONRPC: "1.0",
				ID:      "teranode-dev",
				Method:  method,
				Params:  params,
			}

			reqBody, err := json.Marshal(rpcReq)
			if err != nil {
				return err
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcURL, strings.NewReader(string(reqBody)))
			if err != nil {
				return err
			}

			req.Header.Set("Content-Type", "application/json")
			req.SetBasicAuth(tSettings.RPC.RPCUser, tSettings.RPC.RPCPass)

			client := &http.Client{Timeout: 120 * time.Second}

			resp, err := client.Do(req)
			if err != nil {
				return errors.NewProcessingError("RPC request failed (is teranode running?)", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return errors.NewProcessingError("failed to read response", err)
			}

			// Parse response
			var rpcResp struct {
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}

			if err := json.Unmarshal(body, &rpcResp); err != nil {
				return errors.NewProcessingError("failed to parse response: %s", string(body))
			}

			if rpcResp.Error != nil {
				return errors.NewProcessingError("RPC error (%d): %s", rpcResp.Error.Code, rpcResp.Error.Message)
			}

			// Pretty-print result
			var result string
			if err := json.Unmarshal(rpcResp.Result, &result); err == nil {
				// String result (e.g. help text) - print directly
				fmt.Println(result)
			} else {
				// JSON result - pretty-print
				var pretty bytes.Buffer
				if err := json.Indent(&pretty, rpcResp.Result, "", "  "); err == nil {
					fmt.Println(pretty.String())
				} else {
					fmt.Println(string(rpcResp.Result))
				}
			}

			return nil
		},
	}
}

func monitorCmd() *cli.Command {
	return &cli.Command{
		Name:  "monitor",
		Usage: "Live TUI dashboard for monitoring node status",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			_, cfg := loadConfigOrHint()
			if cfg == nil {
				return nil
			}

			tSettings := teranodeSettings.NewSettings("dev." + cfg.DevName)
			logger := ulogger.New("monitor", ulogger.WithLevel("ERROR"))

			return monitor.Run(logger, tSettings)
		},
	}
}

func logsCmd() *cli.Command {
	return &cli.Command{
		Name:  "logs",
		Usage: "Interactive log viewer with filtering and search",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "file",
				Usage: "Path to log file",
				Value: "./logs/teranode.log",
			},
			&cli.IntFlag{
				Name:  "buffer",
				Usage: "Number of log entries to keep in memory",
				Value: 10000,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return logs.Run(cmd.String("file"), int(cmd.Int("buffer")))
		},
	}
}

func settingsCmd() *cli.Command {
	return &cli.Command{
		Name:  "settings",
		Usage: "Print resolved settings as JSON",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			_, cfg := loadConfigOrHint()
			if cfg == nil {
				return nil
			}

			tSettings := teranodeSettings.NewSettings("dev." + cfg.DevName)
			logger := ulogger.New("settings", ulogger.WithLevel("ERROR"))
			cmdSettings.PrintSettings(logger, tSettings, "", "")

			return nil
		},
	}
}

func diagnoseCmd() *cli.Command {
	return &cli.Command{
		Name:  "diagnose",
		Usage: "Run health checks and configuration validation",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "config",
				Usage: "Run configuration checks",
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "Output as JSON",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			_, cfg := loadConfigOrHint()
			if cfg == nil {
				return nil
			}

			tSettings := teranodeSettings.NewSettings("dev." + cfg.DevName)
			logger := ulogger.New("diagnose", ulogger.WithLevel("ERROR"))

			checkMode := !cmd.Bool("config") // default to health checks
			configMode := cmd.Bool("config")
			diagnose.Run(logger, tSettings, checkMode, configMode, cmd.Bool("json"))

			return nil
		},
	}
}

// loadConfigOrHint loads config, printing a hint if missing. Returns nil cfg if not found.
func loadConfigOrHint() (string, *config.Config) {
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		fmt.Println("Could not find teranode project root.")
		return "", nil
	}

	cfg, err := config.Load(projectRoot)
	if err != nil {
		fmt.Println("No configuration found. Run 'teranode-dev init' to set up your environment.")
		return projectRoot, nil
	}

	return projectRoot, cfg
}

// loadBlockchainSettings uses gocore to resolve the actual blockchain_store setting.
func loadBlockchainSettings(cfg *config.Config) (*url.URL, string) {
	tSettings := teranodeSettings.NewSettings("dev." + cfg.DevName)
	return tSettings.BlockChain.StoreURL, tSettings.DataFolder
}

func handleChainMismatch(projectRoot string, cfg *config.Config, storeURL *url.URL, dataFolder string, result *prereq.ChainCheckResult) { //nolint:unparam
	storedDesc := result.StoredNet
	if storedDesc == "unknown" {
		storedDesc = "unknown network"
	}

	fmt.Printf("  [FAIL] Configured network is %q but stored blockchain data is from %q\n", result.ConfiguredNet, storedDesc)
	fmt.Printf("         Store:            %s\n", result.StoreURL)
	fmt.Printf("         Stored genesis:   %s\n", result.StoredHash)
	fmt.Printf("         Expected genesis: %s\n", result.ExpectedHash)
	fmt.Println()
	fmt.Println("  How would you like to fix this?")
	fmt.Printf("  [1] Delete stored data and start fresh with %s\n", result.ConfiguredNet)

	if result.StoredNet != "unknown" {
		fmt.Printf("  [2] Change network setting to %s to match stored data\n", result.StoredNet)
	}

	fmt.Println("  [3] Skip - I'll handle it manually")
	fmt.Print("  > ")

	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return
	}

	choice := strings.TrimSpace(scanner.Text())

	switch choice {
	case "1":
		fmt.Println()
		if err := prereq.DeleteChainData(storeURL, dataFolder); err != nil {
			fmt.Printf("  Error: %v\n", err)
			return
		}
		fmt.Println("  Chain data deleted. Teranode will create a fresh genesis on next start.")

	case "2":
		if result.StoredNet == "unknown" {
			fmt.Println("  Cannot change to unknown network.")
			return
		}

		cfg.Network = result.StoredNet
		if err := config.Save(projectRoot, cfg); err != nil {
			fmt.Printf("  Error saving config: %v\n", err)
			return
		}

		if err := devsettings.Generate(projectRoot, cfg); err != nil {
			fmt.Printf("  Error updating settings: %v\n", err)
			return
		}

		fmt.Printf("  Network changed to %s in config and settings_local.conf.\n", result.StoredNet)

	default:
		fmt.Println("  Skipped.")
	}
}

func repeatStr(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}

	return result
}
