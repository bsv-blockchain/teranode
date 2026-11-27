// Command consensusdiff gates consensus-library version bumps. Teranode's
// accept/reject decisions live largely in pinned libraries (the script engine,
// transaction parsing, subtrees, legacy wire format), so a bump of one of them
// can change consensus. When a pull request changes one of those pins, the
// consensus vectors run on the base commit and on the pull request, and the
// gate fails when an outcome differs.
//
// Usage:
//
//	consensusdiff changed -modules .github/consensus-modules.txt -base base.go.mod -head go.mod
//	consensusdiff compare -base base.json -head head.json
//	consensusdiff coverage -profile head.cover <module>...
//
// changed prints one "<module>\t<base>\t<head>" line per listed module whose
// pin (version or replace directive) differs. compare reads two `go test -json`
// runs, prints a Markdown report, and exits 3 when a test or package that the
// base run reported ends differently in the head run, or is missing from it. A
// skip is an outcome like any other. Tests only the head run has are listed but
// do not fail the gate. coverage prints "<module>\t<executed>\t<statements>"
// for each module from a cover profile written with -coverpkg, so a bump the
// vectors never execute is not passed as checked.
//
// scripts/consensus_lib_gate.sh drives both; CI runs it in
// .github/workflows/consensus-lib-gate.yaml.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/mod/modfile"
)

// exitDifferent is the exit code for "compare found a difference", kept apart
// from 1 (the tool failed) and 2 (usage).
const exitDifferent = 3

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	different, err := run(os.Args[1], os.Args[2:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "consensusdiff:", err)
		os.Exit(1)
	}

	if different {
		os.Exit(exitDifferent)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: consensusdiff changed|compare|coverage [flags]")
	os.Exit(2)
}

// run executes one subcommand. different reports that compare found an
// outcome that differs.
func run(cmd string, args []string, stdout io.Writer) (different bool, err error) {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	modulesPath := fs.String("modules", ".github/consensus-modules.txt", "changed: file listing the consensus module paths, one per line")
	basePath := fs.String("base", "", "changed: the base go.mod; compare: the base go test -json output")
	headPath := fs.String("head", "go.mod", "changed: the pull request go.mod; compare: the pull request go test -json output")
	limit := fs.Int("limit", 200, "compare: most changes to list")
	profilePath := fs.String("profile", "", "coverage: cover profile written with -coverpkg naming the modules")

	if err := fs.Parse(args); err != nil {
		return false, err
	}

	if cmd == "coverage" {
		return false, coverage(*profilePath, fs.Args(), stdout)
	}

	if *basePath == "" {
		return false, errors.NewInvalidArgumentError("-base is required")
	}

	switch cmd {
	case "changed":
		return false, changed(*modulesPath, *basePath, *headPath, stdout)
	case "compare":
		return compare(*basePath, *headPath, *limit, stdout)
	default:
		usage()
		return false, nil
	}
}

func changed(modulesPath, basePath, headPath string, stdout io.Writer) error {
	modulesFile, err := os.Open(modulesPath)
	if err != nil {
		return err
	}
	defer modulesFile.Close()

	modules, err := readModules(modulesFile)
	if err != nil {
		return errors.NewConfigurationError("%s", modulesPath, err)
	}

	base, err := readGoMod(basePath)
	if err != nil {
		return err
	}

	head, err := readGoMod(headPath)
	if err != nil {
		return err
	}

	for _, c := range changedPins(base, head, modules) {
		fmt.Fprintf(stdout, "%s\t%s\t%s\n", c.path, c.base, c.head)
	}

	return nil
}

func coverage(profilePath string, modules []string, stdout io.Writer) error {
	if profilePath == "" || len(modules) == 0 {
		return errors.NewInvalidArgumentError("-profile and at least one module are required")
	}

	f, err := os.Open(profilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	byModule, err := readModuleCoverage(f, modules)
	if err != nil {
		return errors.NewProcessingError("%s", profilePath, err)
	}

	for _, m := range modules {
		fmt.Fprintf(stdout, "%s\t%d\t%d\n", m, byModule[m].covered, byModule[m].total)
	}

	return nil
}

func readGoMod(path string) (*modfile.File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return parseGoMod(path, data)
}

func compare(basePath, headPath string, limit int, stdout io.Writer) (bool, error) {
	base, err := readResultsFile(basePath)
	if err != nil {
		return false, err
	}

	head, err := readResultsFile(headPath)
	if err != nil {
		return false, err
	}

	changes, added := diffResults(base, head)
	writeReport(stdout, base, head, changes, added, limit)

	return len(changes) > 0, nil
}

func readResultsFile(path string) (map[string]outcome, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	results, err := readResults(f)
	if err != nil {
		return nil, errors.NewProcessingError("%s", path, err)
	}

	if len(results) == 0 {
		return nil, errors.NewProcessingError("%s has no test results; did go test run?", path)
	}

	return results, nil
}
