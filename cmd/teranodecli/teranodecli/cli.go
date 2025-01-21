package teranodecli

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/bitcoin-sv/teranode/cmd/aerospike_reader"
	"github.com/bitcoin-sv/teranode/cmd/bare"
	"github.com/bitcoin-sv/teranode/cmd/bitcoin2utxoset"
	"github.com/bitcoin-sv/teranode/cmd/blockassembly_blaster"
	"github.com/bitcoin-sv/teranode/cmd/blockchainstatus"
	"github.com/bitcoin-sv/teranode/cmd/chainintegrity"
	"github.com/bitcoin-sv/teranode/cmd/filereader"
	"github.com/bitcoin-sv/teranode/cmd/propagation_blaster"
	"github.com/bitcoin-sv/teranode/cmd/recovertx"
	"github.com/bitcoin-sv/teranode/cmd/s3_blaster"
	"github.com/bitcoin-sv/teranode/cmd/s3inventoryintegrity"
	"github.com/bitcoin-sv/teranode/cmd/seeder"
	cmdSettings "github.com/bitcoin-sv/teranode/cmd/settings"
	"github.com/bitcoin-sv/teranode/cmd/txblockidcheck"
	"github.com/bitcoin-sv/teranode/cmd/unspend"
	"github.com/bitcoin-sv/teranode/cmd/utxopersister"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
)

// commandHelp stores the command descriptions
var commandHelp = map[string]string{
	"unspend":              "Unspend a transaction by its TXID",
	"recover_tx":           "Recover one or more transactions by TXID and block height",
	"s3_blaster":           "Blaster for S3",
	"propagation_blaster":  "Propagation Blaster",
	"chainintegrity":       "Chain Integrity",
	"txblockidcheck":       "Transaction Block ID Check",
	"blockchainstatus":     "Blockchain Status",
	"assemblyblaster":      "Assembly Blaster",
	"bare":                 "Bare",
	"filereader":           "File Reader",
	"s3inventoryintegrity": "S3 Inventory Integrity",
	"aerospikereader":      "Aerospike Reader",
	"utxopersister":        "Utxo Persister",
	"seeder":               "Seeder",
	"bitcoin2utxoset":      "Bitcoin 2 Utxoset",
	"settings":             "Settings",
}

var dangerousCommands = map[string]bool{
	"unspend":    true,
	"recover_tx": true,
}

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

	for cmd, desc := range commandHelp {
		fmt.Printf("  %-12s %s\n", cmd, desc)
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

	switch command {
	case "unspend":
		txid := cmd.FlagSet.String("txid", "", "Transaction ID to unspend")
		cmd.Execute = func(args []string) error {
			if *txid == "" {
				return errors.NewProcessingError("--txid is required")
			}

			unspend.Unspend(*txid)

			return nil
		}
	case "recover_tx":
		simulate := cmd.FlagSet.Bool("simulate", false, "Simulate the recovery")
		cmd.Execute = func(args []string) error {
			if len(args) < 2 {
				return errors.NewProcessingError("Usage: teranodecli recover_tx <txID> <blockHeight> [comma separated list of spending tx ids]")
			}

			txIDHex := args[0]

			blockHeightStr := args[1]

			blockHeight64, err := strconv.ParseUint(blockHeightStr, 10, 32)
			if err != nil {
				return errors.NewProcessingError("Error: invalid block height: %s", err.Error())
			}

			var spendingTxIDsStr string

			// check if args[3] is provided
			if len(args) > 2 {
				spendingTxIDsStr = args[2]
			}

			recovertx.RecoverTransaction(txIDHex, blockHeight64, *simulate, spendingTxIDsStr)

			return nil
		}
	case "s3_blaster":
		workers := cmd.FlagSet.Int("workers", 1, "Set worker count")
		usePrefix := cmd.FlagSet.Bool("usePrefix", false, "Use a prefix for the S3 key")
		cmd.Execute = func(args []string) error {
			s3_blaster.Init()
			s3_blaster.Blast(*workers, *usePrefix)

			return nil
		}
	case "propagation_blaster":
		workers := cmd.FlagSet.Int("workers", 1, "Set worker count")
		target := cmd.FlagSet.String("target", "", "Target for the propagation blaster")
		cmd.Execute = func(args []string) error {
			if *target == "" {
				return errors.NewProcessingError("--target is required")
			}

			propagation_blaster.Init()
			propagation_blaster.Blast(*workers, *target)

			return nil
		}
	case "chainintegrity":
		checkInterval := cmd.FlagSet.Int("interval", 10, "Set check interval in seconds")
		alertThreshold := cmd.FlagSet.Int("threshold", 5, "Set alert threshold")
		debug := cmd.FlagSet.Bool("debug", false, "Enable debug logging")
		logfile := cmd.FlagSet.String("logfile", "chainintegrity.log", "Path to logfile")
		cmd.Execute = func(args []string) error {
			chainintegrity.ChainIntegrity(*checkInterval, *alertThreshold, *debug, *logfile)

			return nil
		}
	case "txblockidcheck":
		utxoStore := cmd.FlagSet.String("utxostore", "", "UTXO store URL")
		blockchainStore := cmd.FlagSet.String("blockchainstore", "", "Blockchain store URL")
		subtreeStore := cmd.FlagSet.String("subtreestore", "", "Subtree store URL")
		txHash := cmd.FlagSet.String("txhash", "", "Transaction hash")
		cmd.Execute = func(args []string) error {
			if *txHash == "" {
				return errors.NewProcessingError("--txhash is required")
			}

			txblockidcheck.TxBlockIDCheck(*utxoStore, *blockchainStore, *subtreeStore, *txHash)

			return nil
		}
	case "blockchainstatus":
		miners := cmd.FlagSet.String("miners", "", "Blockchain miners to watch (comma-separated)")
		refresh := cmd.FlagSet.Int("refresh", 5, "Refresh rate in seconds")
		cmd.Execute = func(args []string) error {
			if *miners == "" {
				return errors.NewProcessingError("--miners is required")
			}

			blockchainstatus.Init()
			blockchainstatus.BlockchainStatus(*miners, *refresh)

			return nil
		}
	case "assemblyblaster":
		workers := cmd.FlagSet.Int("workers", 1, "Set worker count")
		broadcast := cmd.FlagSet.String("broadcast", "grpc", "Broadcast to blockassembly server using (disabled|grpc|frpc|http)")
		batchSize := cmd.FlagSet.Int("batch_size", 0, "Batch size [0 for no batching]")
		cmd.Execute = func(args []string) error {
			blockassembly_blaster.Init(tSettings)
			blockassembly_blaster.AssemblyBlaster(*workers, *broadcast, *batchSize)

			return nil
		}
	case "bare":
		cmd.Execute = func(args []string) error {
			bare.Bare()
			return nil
		}
	case "filereader":
		verbose := cmd.FlagSet.Bool("verbose", false, "verbose output")
		checkHeights := cmd.FlagSet.Bool("checkHeights", false, "check heights in utxo headers")
		useStore := cmd.FlagSet.Bool("useStore", false, "use store")

		var path string
		if len(args) == 1 {
			path = args[0]
		}

		cmd.Execute = func(args []string) error {
			filereader.FileReader(*verbose, *checkHeights, *useStore, path)
			return nil
		}
	case "s3inventoryintegrity":
		verbose := cmd.FlagSet.Bool("verbose", false, "enable verbose logging")
		quick := cmd.FlagSet.Bool("quick", false, "skip checking file exists in S3 storage (very slow)")
		blockchainStoreURL := cmd.FlagSet.String("d", "", "blockchain store URL")
		filename := cmd.FlagSet.String("f", "", "CSV filename")
		cmd.Execute = func(args []string) error {
			if *blockchainStoreURL == "" {
				return errors.NewProcessingError("blockchain store URL required")
			}

			if *filename == "" {
				return errors.NewProcessingError("filename required")
			}

			s3inventoryintegrity.S3InventoryIntegrity(*verbose, *quick, *blockchainStoreURL, *filename)

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

			aerospike_reader.AerospikeReader(args[0])

			return nil
		}
	case "utxopersister":
		cmd.Execute = func(args []string) error {
			utxopersister.UtxoPersister()
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

			seeder.Seeder(*inputDir, *hash, *skipHeaders, *skipUTXOs)

			return nil
		}
	case "bitcoin2utxoset":
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
				return errors.NewProcessingError("The 'bitcoinDir' flag is mandatory.")
			}

			// Check the bitcoinDir exists
			if _, err := os.Stat(*blockchainDir); os.IsNotExist(err) {
				return errors.NewProcessingError("Couldn't find %s", *blockchainDir)
			}

			if *outputDir == "" {
				return errors.NewProcessingError("The 'outputDir' flag is mandatory.")
			}

			bitcoin2utxoset.Bitcoin2Utxoset(*blockchainDir, *outputDir, *skipHeaders, *skipUTXOs,
				*blockHashStr, *previousBlockHashStr, *blockHeightUint, *dumpRecords)

			return nil
		}
	case "settings":
		cmd.Execute = func(args []string) error {
			cmdSettings.CmdSettings(version, commit)
			return nil
		}

		return
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
