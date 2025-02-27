package teranodecli

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/bitcoin-sv/teranode/cmd/aerospike_reader"
	"github.com/bitcoin-sv/teranode/cmd/bitcoin2utxoset"
	"github.com/bitcoin-sv/teranode/cmd/filereader"
	"github.com/bitcoin-sv/teranode/cmd/getfsmstate"
	"github.com/bitcoin-sv/teranode/cmd/seeder"
	"github.com/bitcoin-sv/teranode/cmd/setfsmstate"
	cmdSettings "github.com/bitcoin-sv/teranode/cmd/settings"
	"github.com/bitcoin-sv/teranode/cmd/utxopersister"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
)

// commandHelp stores the command descriptions
var commandHelp = map[string]string{
	"filereader":      "File Reader",
	"aerospikereader": "Aerospike Reader",
	"seeder":          "Seeder",
	"getfsmstate":     "Get the current FSM State",
	"setfsmstate":     "Set the FSM State",
	"settings":        "Settings",
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

		var path string
		if len(args) == 1 {
			path = args[0]
		}

		cmd.Execute = func(args []string) error {
			filereader.FileReader(logger, tSettings, *verbose, *checkHeights, *useStore, path)
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

			aerospike_reader.AerospikeReader(logger, tSettings, args[0])

			return nil
		}
	case "utxopersister":
		cmd.Execute = func(args []string) error {
			utxopersister.UtxoPersister(logger, tSettings)
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

			bitcoin2utxoset.Bitcoin2Utxoset(logger, tSettings, *blockchainDir, *outputDir, *skipHeaders,
				*skipUTXOs, *blockHashStr, *previousBlockHashStr, *blockHeightUint, *dumpRecords)

			return nil
		}
	case "getfsmstate":
		cmd.Execute = func(args []string) error {
			getfsmstate.GetFSMState(logger, tSettings)
			return nil
		}
	case "setfsmstate":
		targetFsmState := cmd.FlagSet.String("fsmstate", "", "target fsm state (accepted values: running, idle, catchingblocks, legacysyncing)")

		cmd.Execute = func(args []string) error {
			if *targetFsmState == "" {
				return errors.NewProcessingError("target fsm state is required")
			}

			setfsmstate.SetFSMState(logger, tSettings, *targetFsmState)

			return nil
		}
	case "settings":
		cmd.Execute = func(args []string) error {
			cmdSettings.CmdSettings(logger, tSettings, version, commit)
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
