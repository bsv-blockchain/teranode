package teranode

import (
	"fmt"
	_ "net/http/pprof" //nolint:gosec // Import for pprof, only enabled via CLI flag
	"os"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/debugflags"
	"github.com/ordishs/gocore"
)

// RunDaemon starts the teranode daemon with all necessary initialization
func RunDaemon(progname, version, commit string) {
	// Initialize gocore with version info
	gocore.SetInfo(progname, version, commit)

	// Call the gocore.Log function to initialize the logger and start the Unix domain socket that allows us to configure settings at runtime.
	gocore.Log(progname)

	gocore.AddAppPayloadFn("CONFIG", func() interface{} {
		return gocore.Config().GetAll()
	})

	// Initialize settings
	tSettings := settings.NewSettings()

	// Configure GC tuning (GOMEMLIMIT + GOGC) based on cgroup memory limits.
	// Must happen early, before services allocate significant memory.
	daemon.ConfigureGCTuning(tSettings.GCTuning)

	debugflags.Init(debugflags.Flags{
		All:       tSettings.Debug.All,
		File:      tSettings.Debug.File,
		Blobstore: tSettings.Debug.Blobstore,
		UTXOStore: tSettings.Debug.UTXOStore,
	})

	logger := ulogger.InitLogger(progname, tSettings)

	readLimit := tSettings.Block.FileStoreReadConcurrency
	writeLimit := tSettings.Block.FileStoreWriteConcurrency

	// CRITICAL: Initialize file store semaphores BEFORE any file operations begin.
	// This MUST happen before daemon.Start() creates any file stores or starts any
	// services that use file stores. The InitSemaphores function replaces global
	// channel variables and is not safe to call after file operations have started.
	// See file.go for detailed documentation on the race condition risk.
	//
	// It runs after the logger exists so that a reduced concurrency reaches the
	// operator's log rather than only stdout. Nothing between here and
	// daemon.Start() opens a file store.
	//
	// Note for anyone tempted to reinstate a shell-out here: the previous version
	// read "ulimit -u", which is the max-user-processes limit rather than the
	// open-file limit, and scaled concurrency UP from it. The open-file limit is
	// now read directly, in util/fdlimit.
	applied, err := file.InitSemaphores(readLimit, writeLimit, tSettings.Block.FileStoreUseSystemLimits)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize file store semaphores: %v", err))
	}

	if applied.Clamped {
		// Deliberately a warning and not fatal. The semaphores are a ceiling on
		// concurrent operations, not a reservation of descriptors, so the node runs
		// correctly here — just with less file concurrency than was configured.
		// Refusing to start would turn a bounded, already-handled condition into
		// total unavailability from boot (issue 1431).
		logger.Warnf("File store concurrency reduced to fit the open-file limit: read=%d, write=%d (configured read=%d, write=%d). Raise the hard limit (ulimit -Hn, systemd LimitNOFILE, or macOS kern.maxfilesperproc) to use the configured concurrency.",
			applied.Read, applied.Write, readLimit, writeLimit)
	} else {
		logger.Infof("File store semaphores initialized: read=%d, write=%d", applied.Read, applied.Write)
	}

	util.InitGRPCResolver(logger, tSettings.GRPCResolver)

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	daemon.New(daemon.WithLoggerFactory(func(serviceName string) ulogger.Logger {
		return ulogger.New(serviceName, ulogger.WithLevel(tSettings.LogLevel))
	})).Start(logger, os.Args[1:], tSettings)
}
