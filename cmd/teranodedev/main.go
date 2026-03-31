package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/build"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/config"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/docker"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/hostfile"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/prereq"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/process"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/settings"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/wizard"
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
					return fmt.Errorf("--name, --utxo, and --network are required in non-interactive mode")
				}
			} else {
				cfg, err = wizard.Run()
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
				return fmt.Errorf("prerequisite checks failed, fix the issues above and try again")
			}

			// Generate settings_local.conf
			fmt.Println("\nGenerating settings_local.conf...")
			if err := settings.Generate(projectRoot, cfg); err != nil {
				return fmt.Errorf("failed to generate settings: %w", err)
			}

			fmt.Println("  Done.")

			// Create data directories
			fmt.Println("\nCreating data directories...")
			if err := docker.CreateDataDirs(projectRoot, cfg); err != nil {
				return fmt.Errorf("failed to create data directories: %w", err)
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
				return fmt.Errorf("failed to start containers: %w", err)
			}

			// Build teranode
			fmt.Println("\nBuilding teranode...")
			if err := build.Build(projectRoot, cfg); err != nil {
				return fmt.Errorf("failed to build teranode: %w", err)
			}

			// Save config
			if err := config.Save(projectRoot, cfg); err != nil {
				return fmt.Errorf("failed to save config: %w", err)
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

			process.Status(projectRoot)

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
			if settings.HasEntries(projectRoot, cfg.DevName) {
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
			chainResult := prereq.CheckChain(projectRoot, cfg)
			if chainResult.NoDatabase {
				fmt.Println("  No blockchain database found (fresh setup)")
			} else if chainResult.OK {
				fmt.Printf("  OK - stored genesis matches configured network (%s)\n", cfg.Network)
			} else {
				handleChainMismatch(projectRoot, cfg, chainResult)
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
			chainResult := prereq.CheckChain(projectRoot, cfg)
			if !chainResult.OK && !chainResult.NoDatabase {
				fmt.Println("Chain consistency check failed:")
				handleChainMismatch(projectRoot, cfg, chainResult)

				// Re-check after potential fix
				chainResult = prereq.CheckChain(projectRoot, cfg)
				if !chainResult.OK && !chainResult.NoDatabase {
					return fmt.Errorf("chain mismatch not resolved, cannot start")
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

func handleChainMismatch(projectRoot string, cfg *config.Config, result *prereq.ChainCheckResult) {
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
		if err := prereq.DeleteChainData(projectRoot, cfg); err != nil {
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

		if err := settings.Generate(projectRoot, cfg); err != nil {
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
