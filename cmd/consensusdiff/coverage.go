package main

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

// moduleCoverage is how many of a module's statements a test run executed.
type moduleCoverage struct {
	covered int
	total   int
}

// readModuleCoverage reads a Go cover profile (written with -coverpkg naming the
// modules) and counts each module's statements and those that ran at least
// once. A block listed more than once, as happens when several test binaries
// write one profile, counts once.
func readModuleCoverage(r io.Reader, modules []string) (map[string]moduleCoverage, error) {
	type block struct {
		statements int
		ran        bool
	}

	blocks := map[string]block{}
	scanner := bufio.NewScanner(r)

	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := scanner.Text()
		if lineNo == 1 && strings.HasPrefix(line, "mode:") {
			continue
		}

		if line == "" {
			continue
		}

		// <file>:<start>,<end> <statements> <count>
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, errors.NewProcessingError("cover profile line %d: want \"<block> <statements> <count>\", got %q", lineNo, line)
		}

		statements, err1 := strconv.Atoi(fields[1])
		count, err2 := strconv.Atoi(fields[2])

		if err1 != nil || err2 != nil {
			return nil, errors.NewProcessingError("cover profile line %d: bad numbers in %q", lineNo, line)
		}

		b := blocks[fields[0]]
		b.statements = statements
		b.ran = b.ran || count > 0
		blocks[fields[0]] = b
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	coverage := make(map[string]moduleCoverage, len(modules))
	for _, m := range modules {
		coverage[m] = moduleCoverage{}
	}

	for name, b := range blocks {
		m := owningModule(name, modules)
		if m == "" {
			continue
		}

		c := coverage[m]
		c.total += b.statements

		if b.ran {
			c.covered += b.statements
		}

		coverage[m] = c
	}

	return coverage, nil
}

// owningModule returns the module in modules whose path is the longest prefix
// of a cover profile block's file, or "" if none is.
func owningModule(block string, modules []string) string {
	best := ""

	for _, m := range modules {
		if strings.HasPrefix(block, m+"/") && len(m) > len(best) {
			best = m
		}
	}

	return best
}
