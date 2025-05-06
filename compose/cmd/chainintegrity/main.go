package main

import (
	"flag"

	"github.com/bitcoin-sv/teranode/compose/chainintegrity"
)

func main() {
	checkInterval := flag.Int("interval", 10, "Set check interval in seconds")
	alertThreshold := flag.Int("threshold", 5, "Set alert threshold")
	debug := flag.Bool("debug", false, "Enable debug logging")
	logfile := flag.String("logfile", "chainintegrity.log", "Path to logfile")
	flag.Parse()

	// chainintegrity.ChainIntegrity(*checkInterval, *alertThreshold, *debug, *logfile)
	chainintegrity.ChainIntegrityBaseline(*checkInterval, *alertThreshold, *debug, *logfile)
}
