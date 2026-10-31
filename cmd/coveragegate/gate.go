package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

// threshold is the minimum statement coverage, in percent, for one package.
type threshold struct {
	pkg string
	min float64
}

// counts is the number of covered and total statements in one package.
type counts struct {
	covered int
	total   int
}

func (c counts) percent() float64 {
	if c.total == 0 {
		return 0
	}

	return 100 * float64(c.covered) / float64(c.total)
}

// readThresholds parses lines of "<package> <min percent>". A package is a path
// relative to the module root. Text after '#' is a comment.
func readThresholds(r io.Reader) ([]threshold, error) {
	var thresholds []threshold

	seen := map[string]bool{}
	scanner := bufio.NewScanner(r)

	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := scanner.Text()
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}

		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		if len(fields) != 2 {
			return nil, errors.NewConfigurationError("line %d: want \"<package> <min percent>\", got %q", lineNo, scanner.Text())
		}

		minPercent, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || minPercent < 0 || minPercent > 100 {
			return nil, errors.NewConfigurationError("line %d: %q is not a percentage from 0 to 100", lineNo, fields[1])
		}

		if seen[fields[0]] {
			return nil, errors.NewConfigurationError("line %d: %s is listed twice", lineNo, fields[0])
		}

		seen[fields[0]] = true

		thresholds = append(thresholds, threshold{pkg: fields[0], min: minPercent})
	}

	return thresholds, scanner.Err()
}

type block struct {
	pkg      string
	numStmts int
	covered  bool
}

// packageCoverage reads a Go cover profile and returns the statement coverage of
// each package in pkgs. Packages are paths relative to modulePath; subpackages
// are not included in their parent. A block that appears more than once, once
// per test binary under -coverpkg, counts as covered when any of its lines has
// a non-zero count.
func packageCoverage(r io.Reader, modulePath string, pkgs map[string]bool) (map[string]counts, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "mode: ") {
		if err := scanner.Err(); err != nil {
			return nil, err
		}

		return nil, errors.NewProcessingError("not a cover profile: the first line is not \"mode: ...\"")
	}

	prefix := modulePath + "/"
	blocks := map[string]*block{}

	for lineNo := 2; scanner.Scan(); lineNo++ {
		line := scanner.Text()
		if !strings.HasPrefix(line, prefix) {
			continue
		}

		// A line is "<file>:<block> <statements> <count>".
		countSep := strings.LastIndexByte(line, ' ')
		stmtSep := -1
		colon := -1

		if countSep > 0 {
			stmtSep = strings.LastIndexByte(line[:countSep], ' ')
		}

		if stmtSep > 0 {
			colon = strings.LastIndexByte(line[:stmtSep], ':')
		}

		if colon < len(prefix) {
			return nil, errors.NewProcessingError("line %d: malformed profile line %q", lineNo, line)
		}

		file := line[len(prefix):colon]

		slash := strings.LastIndexByte(file, '/')
		if slash < 0 || !pkgs[file[:slash]] {
			continue
		}

		numStmts, err := strconv.Atoi(line[stmtSep+1 : countSep])
		if err != nil {
			return nil, errors.NewProcessingError("line %d: malformed statement count in %q", lineNo, line)
		}

		count, err := strconv.ParseUint(line[countSep+1:], 10, 64)
		if err != nil {
			return nil, errors.NewProcessingError("line %d: malformed hit count in %q", lineNo, line)
		}

		key := line[:stmtSep]

		b, ok := blocks[key]
		if !ok {
			b = &block{pkg: file[:slash], numStmts: numStmts}
			blocks[key] = b
		}

		if count > 0 {
			b.covered = true
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	coverage := map[string]counts{}

	for _, b := range blocks {
		c := coverage[b.pkg]
		c.total += b.numStmts

		if b.covered {
			c.covered += b.numStmts
		}

		coverage[b.pkg] = c
	}

	return coverage, nil
}

// evaluate returns one report line per threshold, and whether any package is
// below its threshold. A package with no statements in the profile fails, so a
// renamed or untested package cannot pass unmeasured.
func evaluate(thresholds []threshold, coverage map[string]counts) ([]string, bool) {
	lines := make([]string, 0, len(thresholds))
	failed := false

	for _, t := range thresholds {
		c := coverage[t.pkg]
		percent := c.percent()

		// Compare at the precision the report prints, so a package shown at the
		// threshold passes.
		ok := c.total > 0 && math.Round(percent*100) >= math.Round(t.min*100)

		status := "ok"
		if !ok {
			status = "FAIL"
			failed = true
		}

		lines = append(lines, fmt.Sprintf("%-4s %-45s %6.2f%% (min %.2f%%, %d/%d statements)", status, t.pkg, percent, t.min, c.covered, c.total))
	}

	return lines, failed
}
