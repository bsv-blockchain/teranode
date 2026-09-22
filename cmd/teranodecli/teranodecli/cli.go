package teranodecli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/bsv-blockchain/teranode/cmd/aerospikekafkaconnector"
	"github.com/bsv-blockchain/teranode/cmd/aerospikereader"
	"github.com/bsv-blockchain/teranode/cmd/bitcointoutxoset"
	"github.com/bsv-blockchain/teranode/cmd/checkblock"
	"github.com/bsv-blockchain/teranode/cmd/checkblockassembly"
	"github.com/bsv-blockchain/teranode/cmd/checkblocktemplate"
	"github.com/bsv-blockchain/teranode/cmd/diagnose"
	"github.com/bsv-blockchain/teranode/cmd/filereader"
	"github.com/bsv-blockchain/teranode/cmd/getfsmstate"
	"github.com/bsv-blockchain/teranode/cmd/logs"
	"github.com/bsv-blockchain/teranode/cmd/monitor"
	"github.com/bsv-blockchain/teranode/cmd/reconsiderblock"
	"github.com/bsv-blockchain/teranode/cmd/resetblockassembly"
	"github.com/bsv-blockchain/teranode/cmd/rewindblockchain/rewindblockchain"
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

const (
	flagCPUProfile        = "cpu-profile"
	flagMemProfile        = "mem-profile"
	usageCPUProfileOutput = "CPU profile output"
	usageMemProfileOutput = "Memory profile output"
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
	"reconsiderblock":         "Reconsider a block that was previously marked as invalid",
	"resetblockassembly":      "Reset block assembly state",
	"checkblockassembly":      "Check block assembly state by validating unmined transaction inputs (read-only)",
	"fix-chainwork":           "Fix incorrect chainwork values in blockchain database",
	"rewindblockchain":        "Rewind blockchain DB, UTXO store and subtree blobs to Block Assembly's persisted height (DESTRUCTIVE, node must be stopped)",
	"validate-utxo-set":       "Validate UTXO set file",
	"subtreebench":            "Benchmark SubtreeProcessor throughput with CPU and memory profiling",
	"loadunminedbench":        "Benchmark loadUnminedTransactions with CPU and memory profiling",
	"txmapbench":              "Benchmark CreateTransactionMap with CPU and memory profiling",
	"remainderbench":          "Benchmark processRemainderTransactionsAndDequeue with CPU and memory profiling",
	"monitor":                 "Live TUI dashboard for monitoring node status",
	"logs":                    "Interactive log viewer with filtering and search",
	"diagnose":                "Diagnose node health and validate configuration",
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

	// Add common help and printSettings flag to all commands
	cmd.FlagSet.Bool("help", false, "Show help for this command")
	cmd.FlagSet.Bool("printSettings", false, "Print settings")

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
		endHeight := uint32Flag(cmd.FlagSet, "end-height", 0,
			"Build the UTXO set up to this block height and exit (one-shot). 0 = run the service.")
		startHeight := uint32Flag(cmd.FlagSet, "start-height", 0,
			"Start the one-shot build on top of the existing utxo-set at this height (0 = genesis). Requires --end-height.")
		updateLastProcessed := cmd.FlagSet.Bool("update-last-processed", false,
			"After a one-shot build, write lastProcessed.dat = end-height so a later service start resumes there.")

		cmd.Execute = func(args []string) error {
			if *endHeight > 0 {
				if *startHeight >= *endHeight {
					return errors.NewProcessingError("--start-height (%d) must be less than --end-height (%d)", *startHeight, *endHeight)
				}

				return utxopersister.RunUtxoPersisterToHeight(logger, tSettings,
					*startHeight, *endHeight, *updateLastProcessed)
			}

			if *startHeight > 0 || *updateLastProcessed {
				return errors.NewProcessingError("--start-height / --update-last-processed require --end-height")
			}

			utxopersister.RunUtxoPersister(logger, tSettings)

			return nil
		}
	case "seeder":
		inputDir := cmd.FlagSet.String("inputDir", "", "Input directory for UTXO set and headers.")
		hash := cmd.FlagSet.String("hash", "", "Hash of the UTXO set / headers to process.")
		skipHeaders := cmd.FlagSet.Bool("skipHeaders", false, "Skip processing headers.")
		skipUTXOs := cmd.FlagSet.Bool("skipUTXOs", false, "Skip processing UTXOs.")
		force := cmd.FlagSet.Bool("force", false, "Force processing even if lastProcessed.dat or BlockAssembler state already exists.")
		cmd.Execute = func(args []string) error {
			if *inputDir == "" {
				return errors.NewProcessingError("Please provide an inputDir")
			}

			if *hash == "" {
				return errors.NewProcessingError("Please provide a hash")
			}

			return seeder.Seeder(logger, tSettings, *inputDir, *hash, *skipHeaders, *skipUTXOs, *force)
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
	case "diagnose":
		checkMode := cmd.FlagSet.Bool("check", false, "Run service health checks (default if no mode specified)")
		configMode := cmd.FlagSet.Bool("config", false, "Validate configuration without running services")
		jsonOutput := cmd.FlagSet.Bool("json", false, "Output results as JSON")

		cmd.Execute = func(args []string) error {
			exitCode := diagnose.Run(logger, tSettings, *checkMode, *configMode, *jsonOutput)
			if exitCode != 0 {
				os.Exit(exitCode)
			}

			return nil
		}
	case "setfsmstate":
		targetFsmState := cmd.FlagSet.String("fsmstate", "", "target fsm state (accepted values: running, idle, catchingblocks)")

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
	case "reconsiderblock":
		cmd.Execute = func(args []string) error {
			if len(args) != 1 {
				return errors.NewProcessingError("Usage: reconsiderblock <blockhash>")
			}

			return reconsiderblock.ReconsiderBlock(logger, tSettings, args[0])
		}
	case "resetblockassembly":
		fullReset := cmd.FlagSet.Bool("full-reset", false, "Perform a full reset, including clearing mempool and unmined transactions")
		validateInputs := cmd.FlagSet.Bool("validate-inputs", false, "Validate that each unmined tx's inputs are still spent by this tx (marks invalid ones as conflicting)")

		cmd.Execute = func(args []string) error {
			err := resetblockassembly.ResetBlockAssembly(logger, tSettings, *fullReset, *validateInputs)
			if err != nil {
				return errors.NewProcessingError("Failed to reset block assembly", err)
			}

			return nil
		}
	case "checkblockassembly":
		cmd.Execute = func(args []string) error {
			if err := checkblockassembly.CheckBlockAssembly(logger, tSettings); err != nil {
				return errors.NewProcessingError("Failed to check block assembly", err)
			}
			fmt.Println("Block assembly validation passed: all unmined transactions have valid inputs")
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
	case "rewindblockchain":
		cmd.Execute = rewindExecute(logger, tSettings, registerRewindFlags(cmd.FlagSet))
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
	case "subtreebench":
		subtreeSize := cmd.FlagSet.Int("subtree-size", 1_048_576, "Size of subtree")
		producers := cmd.FlagSet.Int("producers", 16, "Number of producer goroutines")
		iterations := cmd.FlagSet.Int("iterations", 10_000_000, "Number of transactions to process")
		cpuProfile := cmd.FlagSet.String(flagCPUProfile, "cpu.prof", "Output file for CPU profile")
		memProfile := cmd.FlagSet.String(flagMemProfile, "mem.prof", "Output file for memory profile")
		duration := cmd.FlagSet.Int("duration", 0, "Duration to run benchmark in seconds (0 for iteration-based, processes all items)")

		cmd.Execute = func(args []string) error {
			return runSubtreeBenchmark(*subtreeSize, *producers, *iterations, *duration, *cpuProfile, *memProfile)
		}
	case "loadunminedbench":
		txCount := cmd.FlagSet.Int("tx-count", 1_000_000, "Number of transactions")
		cpuProfile := cmd.FlagSet.String(flagCPUProfile, "loadunmined_cpu.prof", usageCPUProfileOutput)
		memProfile := cmd.FlagSet.String(flagMemProfile, "loadunmined_mem.prof", usageMemProfileOutput)
		aerospikeURL := cmd.FlagSet.String("aerospike-url", "", "Aerospike URL (empty=testcontainer)")

		cmd.Execute = func(args []string) error {
			return runLoadUnminedBenchmark(*txCount, *cpuProfile, *memProfile, *aerospikeURL)
		}
	case "txmapbench":
		numSubtrees := cmd.FlagSet.Int("subtrees", 100, "Number of subtrees")
		txsPerSubtree := cmd.FlagSet.Int("txs-per-subtree", 1_048_576, "Transactions per subtree")
		cpuProfile := cmd.FlagSet.String(flagCPUProfile, "createtransactionmap_cpu.prof", usageCPUProfileOutput)
		memProfile := cmd.FlagSet.String(flagMemProfile, "createtransactionmap_mem.prof", usageMemProfileOutput)

		cmd.Execute = func(args []string) error {
			return runCreateTxMapBenchmark(*numSubtrees, *txsPerSubtree, *cpuProfile, *memProfile)
		}
	case "remainderbench":
		numSubtrees := cmd.FlagSet.Int("subtrees", 100, "Number of subtrees")
		txsPerSubtree := cmd.FlagSet.Int("txs-per-subtree", 1_048_576, "Transactions per subtree")
		cpuProfile := cmd.FlagSet.String(flagCPUProfile, "processremaindertxanddequeue_cpu.prof", usageCPUProfileOutput)
		memProfile := cmd.FlagSet.String(flagMemProfile, "processremaindertxanddequeue_mem.prof", usageMemProfileOutput)

		cmd.Execute = func(args []string) error {
			return runProcessRemainderBenchmark(*numSubtrees, *txsPerSubtree, *cpuProfile, *memProfile)
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

	if printSettings := cmd.FlagSet.Lookup("printSettings"); printSettings != nil && printSettings.Value.String() == "true" {
		cmdSettings.PrintSettings(logger, tSettings, version, commit)
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

// uint32Value is a flag.Value that parses into a uint32, rejecting values
// outside the uint32 range at parse time rather than silently truncating.
type uint32Value uint32

func (u *uint32Value) Set(s string) error {
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return err
	}

	*u = uint32Value(n)

	return nil
}

func (u *uint32Value) String() string {
	return strconv.FormatUint(uint64(*u), 10)
}

// uint32Flag registers a uint32-typed flag on fs and returns a pointer to its value.
func uint32Flag(fs *flag.FlagSet, name string, value uint32, usage string) *uint32 {
	p := new(uint32)
	*p = value
	fs.Var((*uint32Value)(p), name, usage)

	return p
}

// rewindFlags holds the parsed rewindblockchain flag values. Registration lives
// here rather than inline in the dispatch switch so tests can exercise the flag
// surface without executing a rewind, which opens real stores.
type rewindFlags struct {
	targetHeight *int64
	dryRun       *bool
	assumeYes    *bool
	forceNotIdle *bool
	forceDeep    *bool
	verify       *bool
	concurrency  *int
}

// registerRewindFlags registers the rewindblockchain flags on fs. Names and
// defaults are copied verbatim from cmd/rewindblockchain/main.go and are
// documented in docs/howto/miners/minersHowToTeranodeCLI.md; do not rename them.
func registerRewindFlags(fs *flag.FlagSet) *rewindFlags {
	return &rewindFlags{
		targetHeight: fs.Int64("target-height", -1, "Target height to rewind to (default: read state[\"BlockAssembler\"])"),
		dryRun:       fs.Bool("dry-run", false, "Log actions but do not modify any store"),
		assumeYes:    fs.Bool("assume-yes", false, "Skip interactive confirmation prompt"),
		forceNotIdle: fs.Bool("force-not-idle", false, "Proceed even if FSM is not IDLE (DANGEROUS)"),
		forceDeep:    fs.Bool("force-deep", false, "Allow rewind deeper than 100 blocks (coinbase-maturity risk)"),
		verify:       fs.Bool("verify", false, "Run post-rewind consistency checks"),
		concurrency:  fs.Int("concurrency", 0, "Subtree-load concurrency (0 = settings.BlockAssembly.MoveBackBlockConcurrency or 4)"),
	}
}

// rewindExecute builds the Execute closure for the rewindblockchain command.
// Split out of the dispatch switch so the positional-argument guard below is
// reachable from a test: the guard returns before Rewind is called, so a test
// can drive it without opening any store.
//
// The guard matters because Go's flag package stops parsing at the first
// non-flag argument. Without it, `rewindblockchain --assume-yes 1749330
// --force-deep` parses only --assume-yes and discards the rest, leaving
// TargetHeight at -1 so resolveTarget silently falls back to
// state["BlockAssembler"] — an irreversible rewind to an unasked-for height,
// with the confirmation prompt skipped.
func rewindExecute(logger ulogger.Logger, tSettings *settings.Settings, rewind *rewindFlags) func(args []string) error {
	return func(args []string) error {
		if len(args) > 0 {
			return errors.NewProcessingError("rewindblockchain takes no positional arguments (got %v); use --target-height", args)
		}

		_, err := rewindblockchain.Rewind(context.Background(), logger, tSettings, rewind.options())

		return err
	}
}

// options converts the parsed flags into rewindblockchain.Options, wiring the
// process stdin/stdout so the destructive-action confirmation prompt works
// under `kubectl exec -it` / `docker compose run`.
func (f *rewindFlags) options() rewindblockchain.Options {
	return rewindblockchain.Options{
		TargetHeight: *f.targetHeight,
		DryRun:       *f.dryRun,
		AssumeYes:    *f.assumeYes,
		ForceNotIdle: *f.forceNotIdle,
		ForceDeep:    *f.forceDeep,
		Verify:       *f.verify,
		Concurrency:  *f.concurrency,
		Stdin:        os.Stdin,
		Stdout:       os.Stdout,
	}
}
