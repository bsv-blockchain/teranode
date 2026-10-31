// Command coveragegate fails when a listed package's statement coverage in a Go
// cover profile is below its minimum.
//
// Usage:
//
//	go run ./cmd/coveragegate -profile coverage.out -thresholds .github/consensus-coverage.txt
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/bsv-blockchain/teranode/errors"
)

func main() {
	profilePath := flag.String("profile", "coverage.out", "Go cover profile, as written by make test")
	thresholdsPath := flag.String("thresholds", ".github/consensus-coverage.txt", "file of \"<package> <min percent>\" lines")
	modulePath := flag.String("module", "github.com/bsv-blockchain/teranode", "module path that package names are relative to")
	flag.Parse()

	if err := run(*profilePath, *thresholdsPath, *modulePath); err != nil {
		fmt.Fprintln(os.Stderr, "coveragegate:", err)
		os.Exit(1)
	}
}

func run(profilePath, thresholdsPath, modulePath string) error {
	thresholdsFile, err := os.Open(thresholdsPath)
	if err != nil {
		return err
	}
	defer thresholdsFile.Close()

	thresholds, err := readThresholds(thresholdsFile)
	if err != nil {
		return errors.NewConfigurationError("%s", thresholdsPath, err)
	}

	pkgs := make(map[string]bool, len(thresholds))
	for _, t := range thresholds {
		pkgs[t.pkg] = true
	}

	profile, err := os.Open(profilePath)
	if err != nil {
		return err
	}
	defer profile.Close()

	coverage, err := packageCoverage(profile, modulePath, pkgs)
	if err != nil {
		return errors.NewProcessingError("%s", profilePath, err)
	}

	lines, failed := evaluate(thresholds, coverage)
	for _, line := range lines {
		fmt.Println(line)
	}

	if failed {
		return errors.NewProcessingError("statement coverage is below its minimum for at least one package in %s. If this pull request removed no tests, first check the make test output for \"Skipping\" lines: tests that need a container (Aerospike, PostgreSQL) skip when it cannot start, for example when an image pull is rate-limited, and their coverage is then missing. Otherwise add tests or, if the drop is intended, lower the minimum in the same pull request and say why", thresholdsPath)
	}

	return nil
}
