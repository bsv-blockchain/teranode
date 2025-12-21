package teranodecli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/bsv-blockchain/teranode/cmd/aerospikekafkaconnector"
	"github.com/bsv-blockchain/teranode/cmd/aerospikereader"
	"github.com/bsv-blockchain/teranode/cmd/bitcointoutxoset"
	"github.com/bsv-blockchain/teranode/cmd/checkblock"
	"github.com/bsv-blockchain/teranode/cmd/checkblocktemplate"
	"github.com/bsv-blockchain/teranode/cmd/filereader"
	"github.com/bsv-blockchain/teranode/cmd/getfsmstate"
	"github.com/bsv-blockchain/teranode/cmd/logs"
	"github.com/bsv-blockchain/teranode/cmd/monitor"
	"github.com/bsv-blockchain/teranode/cmd/resetblockassembly"
	"github.com/bsv-blockchain/teranode/cmd/seeder"
	"github.com/bsv-blockchain/teranode/cmd/setfsmstate"
	cmdSettings "github.com/bsv-blockchain/teranode/cmd/settings"
	"github.com/bsv-blockchain/teranode/cmd/utxopersister"
	"github.com/bsv-blockchain/teranode/cmd/utxovalidator"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// commandHelp stores the command descriptions
var commandHelp = map[string]string{
	"filereader":              "File Reader",
	"aerospikereader":         "Aerospike Reader",
	"aerospikekafkaconnector": "Read Aerospike CDC from Kafka and filter by txID bin",
	"bitcointoutxoset":        "Bitcoin to Utxoset",
	"seeder":                  "Seeder",
	"utxopersister":           "Utxo Persister",
	"getfsmstate":             "Get the current FSM State",
	"setfsmstate":             "Set the FSM State",
	"settings":                "Settings",
	"export-blocks":           "Export blockchain to CSV",
	"import-blocks":           "Import blockchain from CSV",
	"checkblocktemplate":      "Check block template",
	"checkblock":              "Check block - fetches a block and validates it using the block validation service",
	"resetblockassembly":      "Reset block assembly state",
	"fix-chainwork":           "Fix incorrect chainwork values in blockchain database",
	"validate-utxo-set":       "Validate UTXO set file",
	"monitor":                 "Live TUI dashboard for monitoring node status",
	"logs":                    "Interactive log viewer with filtering and search",
}

var dangerousCommands = map[string]bool{}

// Command represents a CLI command configuration
type Command struct {
	Name        string
	Description string
	FlagSet     *flag.FlagSet
	Execute     func(args []string) error
}

// setupCommand creates a new command with its flag set
func setupCommand(name string) *Command {
	cmd := &Command{
		Name:        name,
		Description: commandHelp[name],
		FlagSet:     flag.NewFlagSet(name, flag.ExitOnError),
	}

	// Add common help flag to all commands
	cmd.FlagSet.Bool("help", false, "Show help for this command")

	return cmd
}

// printUsage prints all available commands and their descriptions
func printUsage() {
	fmt.Println("Usage: teranode-cli <command> [options]")
	fmt.Println("\nAvailable Commands:")

	commands := make([]string, 0, len(commandHelp))
	for cmd := range commandHelp {
		commands = append(commands, cmd)
	}

	// Sort the help guide alphabetically
	sort.Strings(commands)

	for _, cmd := range commands {
		fmt.Printf("  %-20s %s\n", cmd, commandHelp[cmd])
	}

	fmt.Println("\nUse 'teranode-cli <command> --help' for more information about a command")
}

// confirmDangerousAction asks the user to confirm a dangerous action by typing the command name
func confirmDangerousAction(command string) bool {
	fmt.Printf("\n⚠️  WARNING: You are about to perform a dangerous action: %s\n", command)
	fmt.Printf("To confirm, please type the command name: %s\n", command)
	fmt.Print("> ")

	var input string

	_, err := fmt.Scanln(&input)
	if err != nil {
		fmt.Println("Error reading input. Action cancelled.")
		return false
	}

	if input != command {
		fmt.Printf("Input '%s' does not match '%s'. Action cancelled.\n", input, command)
		return false
	}

	return true
}

func Start(args []string, version, commit string) {
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]

	// Check if the command is dangerous
	if dangerousCommands[command] {
		if !confirmDangerousAction(command) {
			fmt.Println("Command cancelled by user")
			os.Exit(1)
		}
	}

	cmd := setupCommand(command)
	tSettings := settings.NewSettings()

	logger := ulogger.InitLogger("teranode-cli", tSettings)

	util.InitGRPCResolver(logger, tSettings.GRPCResolver)

	switch command {
	case "filereader":
		verbose := cmd.FlagSet.Bool("verbose", false, "verbose output")
		checkHeights := cmd.FlagSet.Bool("checkHeights", false, "check heights in utxo headers")
		useStore := cmd.FlagSet.Bool("useStore", false, "use store")

		cmd.Execute = func(args []string) error {
			var path string
			if len(args) == 1 {
				path = args[0]
			}

			filereader.ReadAndProcessFile(logger, tSettings, *verbose, *checkHeights, *useStore, path)

			return nil
		}
	case "aerospikereader":
		cmd.Execute = func(args []string) error {
			if len(args) != 1 {
				return errors.NewProcessingError("Usage: aerospikereader <txid>")
			}

			if len(args[0]) != 64 {
				return errors.NewProcessingError("Invalid txid: %s", args[0])
			}

			aerospikereader.ReadAerospike(logger, tSettings, args[0])

			return nil
		}
	case "aerospikekafkaconnector":
		kafkaURL := cmd.FlagSet.String("kafka-url", "", "Kafka broker URL (required, e.g., kafka://localhost:9092/aerospike-cdc)")
		txid := cmd.FlagSet.String("txid", "", "Filter by 64-char hex transaction ID (optional)")
		namespace := cmd.FlagSet.String("namespace", "", "Filter by Aerospike namespace (optional)")
		set := cmd.FlagSet.String("set", "txmeta", "Filter by Aerospike set")
		statsInterval := cmd.FlagSet.Int("stats-interval", 30, "Statistics logging interval in seconds")

		cmd.Execute = func(args []string) error {
			if *kafkaURL == "" {
				return errors.NewProcessingError("--kafka-url is required")
			}

			if *txid != "" && len(*txid) != 64 {
				return errors.NewProcessingError("Invalid txid: must be 64 hex characters")
			}

			return aerospikekafkaconnector.ReadAerospikeKafka(
				logger, tSettings, *kafkaURL, *txid, *namespace, *set, *statsInterval)
		}
	case "utxopersister":
		cmd.Execute = func(args []string) error {
			utxopersister.RunUtxoPersister(logger, tSettings)
			return nil
		}
	case "seeder":
		inputDir := cmd.FlagSet.String("inputDir", "", "Input directory for UTXO set and headers.")
		hash := cmd.FlagSet.String("hash", "", "Hash of the UTXO set / headers to process.")
		skipHeaders := cmd.FlagSet.Bool("skipHeaders", false, "Skip processing headers.")
		skipUTXOs := cmd.FlagSet.Bool("skipUTXOs", false, "Skip processing UTXOs.")
		cmd.Execute = func(args []string) error {
			if *inputDir == "" {
				return errors.NewProcessingError("Please provide an inputDir")
			}

			if *hash == "" {
				return errors.NewProcessingError("Please provide a hash")
			}

			seeder.Seeder(logger, tSettings, *inputDir, *hash, *skipHeaders, *skipUTXOs)

			return nil
		}
	case "bitcointoutxoset":
		blockchainDir := cmd.FlagSet.String("bitcoinDir", "", "Location of bitcoin data")
		outputDir := cmd.FlagSet.String("outputDir", "", "Output directory for UTXO set.")
		skipHeaders := cmd.FlagSet.Bool("skipHeaders", false, "Skip processing headers")
		skipUTXOs := cmd.FlagSet.Bool("skipUTXOs", false, "Skip processing UTXOs")
		blockHashStr := cmd.FlagSet.String("blockHash", "", "Block hash to start from")
		previousBlockHashStr := cmd.FlagSet.String("previousBlockHash", "", "Previous block hash")
		blockHeightUint := cmd.FlagSet.Uint("blockHeight", 0, "Block height to start from")
		dumpRecords := cmd.FlagSet.Int("dumpRecords", 0, "Dump records from index")
		cmd.Execute = func(args []string) error {
			if *blockchainDir == "" {
				return errors.NewProcessingError("the 'bitcoinDir' flag is mandatory.")
			}

			// Check the bitcoinDir exists
			if _, err := os.Stat(*blockchainDir); os.IsNotExist(err) {
				return errors.NewProcessingError("couldn't find %s", *blockchainDir)
			}

			if *outputDir == "" {
				return errors.NewProcessingError("the 'outputDir' flag is mandatory.")
			}

			// Run the conversion
			bitcointoutxoset.ConvertBitcoinToUtxoSet(logger, tSettings, *blockchainDir, *outputDir, *skipHeaders,
				*skipUTXOs, *blockHashStr, *previousBlockHashStr, *blockHeightUint, *dumpRecords)

			return nil
		}
	case "getfsmstate":
		cmd.Execute = func(args []string) error {
			getfsmstate.FetchFSMState(logger, tSettings)
			return nil
		}
	case "monitor":
		cmd.Execute = func(args []string) error {
			return monitor.Run(logger, tSettings)
		}
	case "logs":
		logFile := cmd.FlagSet.String("file", "./logs/teranode.log", "Path to log file")
		bufferSize := cmd.FlagSet.Int("buffer", 10000, "Number of log entries to keep in memory")

		cmd.Execute = func(args []string) error {
			return logs.Run(*logFile, *bufferSize)
		}
	case "setfsmstate":
		targetFsmState := cmd.FlagSet.String("fsmstate", "", "target fsm state (accepted values: running, idle, catchingblocks, legacysyncing)")

		cmd.Execute = func(args []string) error {
			if *targetFsmState == "" {
				return errors.NewProcessingError("target fsm state is required")
			}

			setfsmstate.UpdateFSMState(logger, tSettings, *targetFsmState)

			return nil
		}
	case "settings":
		cmd.Execute = func(args []string) error {
			cmdSettings.PrintSettings(logger, tSettings, version, commit)
			return nil
		}
	case "export-blocks":
		filePath := cmd.FlagSet.String("file", "", "CSV file path to export")
		cmd.Execute = func(args []string) error {
			if *filePath == "" {
				return errors.NewProcessingError("Usage: export-blocks --file <path>")
			}

			u := tSettings.BlockChain.StoreURL
			if u == nil {
				return errors.NewProcessingError("Store URL not configured in settings")
			}

			s, err := sql.New(logger, u, tSettings)
			if err != nil {
				return err
			}

			if err := s.ExportBlockchainCSV(context.Background(), *filePath); err != nil {
				return err
			}

			fmt.Printf("Exported blockchain to %s\n", *filePath)

			return nil
		}
	case "import-blocks":
		filePath := cmd.FlagSet.String("file", "", "CSV file path to import")
		cmd.Execute = func(args []string) error {
			if *filePath == "" {
				return errors.NewProcessingError("Usage: import-blocks --file <path>")
			}

			u := tSettings.BlockChain.StoreURL
			if u == nil {
				return errors.NewProcessingError("Store URL not configured in settings")
			}

			s, err := sql.New(logger, u, tSettings)
			if err != nil {
				return err
			}

			if err := s.ImportBlockchainCSV(context.Background(), *filePath); err != nil {
				return err
			}

			fmt.Printf("Imported blockchain from %s\n", *filePath)

			return nil
		}
	case "checkblocktemplate":
		cmd.Execute = func(args []string) error {
			blockTemplate, err := checkblocktemplate.ValidateBlockTemplate(logger, tSettings)
			if err != nil {
				return errors.NewProcessingError("Failed to check block template", err)
			}

			fmt.Printf("Checked block template successfully: %s\n", blockTemplate.String())

			return nil
		}
	case "checkblock":
		cmd.Execute = func(args []string) error {
			blockTemplate, err := checkblock.CheckBlock(logger, tSettings, args[0])
			if err != nil {
				return errors.NewProcessingError("Failed to check block", err)
			}

			fmt.Printf("Checked block successfully: %s\n", blockTemplate.String())

			return nil
		}
	case "resetblockassembly":
		fullReset := cmd.FlagSet.Bool("full-reset", false, "Perform a full reset, including clearing mempool and unmined transactions")

		cmd.Execute = func(args []string) error {
			err := resetblockassembly.ResetBlockAssembly(logger, tSettings, *fullReset)
			if err != nil {
				return errors.NewProcessingError("Failed to reset block assembly", err)
			}

			return nil
		}
	case "fix-chainwork":
		dbURL := cmd.FlagSet.String("db-url", "", "Database URL (postgres://... or sqlite://...)")
		dryRun := cmd.FlagSet.Bool("dry-run", true, "Preview changes without updating database")
		batchSize := cmd.FlagSet.Int("batch-size", 1000, "Number of updates to batch in a transaction")
		startHeight := cmd.FlagSet.Uint("start-height", 650286, "Starting block height")
		endHeight := cmd.FlagSet.Uint("end-height", 0, "Ending block height (0 for current tip)")

		cmd.Execute = func(args []string) error {
			if *dbURL == "" {
				return errors.NewProcessingError("Please provide a database URL with --db-url")
			}

			return fixChainwork(*dbURL, *dryRun, *batchSize, uint32(*startHeight), uint32(*endHeight))
		}
	case "validate-utxo-set":
		verbose := cmd.FlagSet.Bool("verbose", false, "verbose output showing individual UTXOs")

		cmd.Execute = func(args []string) error {
			if len(args) != 1 {
				return errors.NewProcessingError("Usage: validate-utxo-set [--verbose] <utxo-set-file-path>")
			}

			utxoFilePath := args[0]

			// Validate the UTXO file
			result, err := utxovalidator.ValidateUTXOFile(context.Background(), utxoFilePath, logger, tSettings, *verbose)
			if err != nil {
				return errors.NewProcessingError("Failed to validate UTXO-set file", err)
			}

			// Print results
			fmt.Printf("\n")
			fmt.Printf("UTXO Set Validation Results:\n")
			fmt.Printf("============================\n")
			fmt.Printf("Block Height:      %d\n", result.BlockHeight)
			fmt.Printf("Block Hash:        %s\n", result.BlockHash.String())
			fmt.Printf("Previous Hash:     %s\n", result.PreviousHash.String())
			fmt.Printf("UTXO Count:        %d\n", result.UTXOCount)
			fmt.Printf("Actual Satoshis:   %s\n", formatSatoshis(result.ActualSatoshis))
			fmt.Printf("Expected Satoshis: %s\n", formatSatoshis(result.ExpectedSatoshis))

			if result.IsValid {
				fmt.Printf("Status:            ✓ VALID - Satoshi amounts match\n")
			} else {
				fmt.Printf("Status:            ✗ INVALID - Satoshi mismatch!\n")
				diff := int64(result.ActualSatoshis) - int64(result.ExpectedSatoshis)
				if diff > 0 {
					fmt.Printf("Difference:        +%s satoshis (excess)\n", formatSatoshis(uint64(diff)))
				} else {
					fmt.Printf("Difference:        -%s satoshis (deficit)\n", formatSatoshis(uint64(-diff)))
				}
			}
			fmt.Printf("\n")

			// Exit with non-zero code if validation failed
			if !result.IsValid {
				os.Exit(1)
			}

			return nil
		}
	default:
		fmt.Printf("Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}

	// Parse flags
	if err := cmd.FlagSet.Parse(args[1:]); err != nil {
		fmt.Printf("Error parsing arguments: %v\n", err)
		os.Exit(1)
	}

	// Check for help flag
	if help := cmd.FlagSet.Lookup("help"); help != nil && help.Value.String() == "true" {
		fmt.Printf("Usage of %s:\n", cmd.Name)
		cmd.FlagSet.PrintDefaults()
		os.Exit(0)
	}

	// Execute the command
	if err := cmd.Execute(cmd.FlagSet.Args()); err != nil {
		fmt.Printf("Error executing command: %v\n", err)
		os.Exit(1)
	}
}

// formatSatoshis formats a satoshi amount with thousand separators for better readability.
func formatSatoshis(satoshis uint64) string {
	str := fmt.Sprintf("%d", satoshis)

	// Add thousand separators
	n := len(str)
	if n <= 3 {
		return str
	}

	// Calculate how many commas we need
	commas := (n - 1) / 3
	result := make([]byte, n+commas)

	// Fill from right to left
	resultPos := len(result) - 1
	strPos := n - 1
	digitCount := 0

	for strPos >= 0 {
		if digitCount == 3 {
			result[resultPos] = ','
			resultPos--
			digitCount = 0
		}

		result[resultPos] = str[strPos]
		resultPos--
		strPos--
		digitCount++
	}

	return string(result)
}
