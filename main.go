package main

import (
	_ "net/http/pprof" //nolint:gosec // Import for pprof, only enabled via CLI flag
	"os"
	"path"

	"github.com/bitcoin-sv/teranode/cmd/aerospike_reader/aerospike_reader"
	"github.com/bitcoin-sv/teranode/cmd/bare/bare"
	"github.com/bitcoin-sv/teranode/cmd/bitcoin2utxoset/bitcoin2utxoset"
	"github.com/bitcoin-sv/teranode/cmd/blockassembly_blaster/blockassembly_blaster"
	"github.com/bitcoin-sv/teranode/cmd/blockchainstatus/blockchainstatus"
	"github.com/bitcoin-sv/teranode/cmd/chainintegrity/chainintegrity"
	"github.com/bitcoin-sv/teranode/cmd/filereader/filereader"
	"github.com/bitcoin-sv/teranode/cmd/miner/miner"
	"github.com/bitcoin-sv/teranode/cmd/propagation_blaster/propagation_blaster"
	"github.com/bitcoin-sv/teranode/cmd/recovertx/recovertx"
	"github.com/bitcoin-sv/teranode/cmd/s3_blaster/s3_blaster"
	"github.com/bitcoin-sv/teranode/cmd/s3inventoryintegrity/s3inventoryintegrity"
	"github.com/bitcoin-sv/teranode/cmd/seeder/seeder"
	cmdsettings "github.com/bitcoin-sv/teranode/cmd/settings/settings"
	"github.com/bitcoin-sv/teranode/cmd/txblaster/txblaster"
	"github.com/bitcoin-sv/teranode/cmd/txblockidcheck/txblockidcheck"
	"github.com/bitcoin-sv/teranode/cmd/unspend/unspend"
	utxopersister_cmd "github.com/bitcoin-sv/teranode/cmd/utxopersister/utxopersister"
	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/rs/zerolog"
	"golang.org/x/term"
)

// Name used by build script for the binaries. (Please keep on single line)
const progname = "teranode"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

func init() {
	gocore.SetInfo(progname, version, commit)

	// Call the gocore.Log function to initialize the logger and start the Unix domain socket that allows us to configure settings at runtime.
	gocore.Log(progname)

	gocore.AddAppPayloadFn("CONFIG", func() interface{} {
		return gocore.Config().GetAll()
	})
}

func main() {
	tSettings := settings.NewSettings()

	switch path.Base(os.Args[0]) {
	case "bare.run":
		// bare.Init()
		bare.Start()
		return
	case "blockassemblyblaster.run":
		blockassembly_blaster.Init(tSettings)
		blockassembly_blaster.Start()
		return
	case "chainintegrity.run":
		// chainintegrity.Init()
		chainintegrity.Start()
		return
	case "propagationblaster.run":
		propagation_blaster.Init()
		propagation_blaster.Start()
		return
	case "s3blaster.run":
		s3_blaster.Init()
		s3_blaster.Start()
		return
	case "blockchainstatus.run":
		blockchainstatus.Init()
		blockchainstatus.Start()
		return
	case "blaster.run":
		// txblaster.Init()
		txblaster.Start()
		return
	case "filereader.run":
		// filereader.Init()
		filereader.Start()
		return
	case "s3inventoryintegrity.run":
		s3inventoryintegrity.Start()
		return
	case "txblockidcheck.run":
		txblockidcheck.Start()
		return
	case "aerospike_reader.run":
		aerospike_reader.Start()
		return
	case "utxopersister.run":
		utxopersister_cmd.Start()
		return
	case "seeder.run":
		seeder.Start()
		return
	case "bitcoin2utxoset.run":
		bitcoin2utxoset.Start()
		return
	case "settings.run":
		cmdsettings.Start(version, commit)
		return
	case "unspend.run":
		unspend.Start()
		return
	case "recovertx.run":
		recovertx.Start()
		return
	case "miner.run":
		miner.Start()
		return
	}

	logger := initLogger(progname)

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	daemon.New().Start(logger, os.Args[1:], tSettings)
}

func initLogger(progname string) ulogger.Logger {
	logLevel, _ := gocore.Config().Get("logLevel", "info")
	logOptions := []ulogger.Option{
		ulogger.WithLevel(logLevel),
	}

	isTerminal := term.IsTerminal(int(os.Stdout.Fd()))

	output := zerolog.ConsoleWriter{
		Out:     os.Stdout,
		NoColor: !isTerminal, // Disable color if output is not a terminal
	}

	logOptions = append(logOptions, ulogger.WithWriter(output))

	useLogger, ok := gocore.Config().Get("logger")
	if ok && useLogger != "" {
		logOptions = append(logOptions, ulogger.WithLoggerType(useLogger))
	}

	logger := ulogger.New(progname, logOptions...)

	return logger
}
