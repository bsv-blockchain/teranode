package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

// absent is the outcome of a test that one run did not report at all.
const absent = "absent"

// outcome is how one test or package ended in a `go test -json` run, with the
// last line it printed, which usually holds the skip reason or the failure.
type outcome struct {
	action string
	detail string
}

// testEvent is one line of `go test -json` output.
type testEvent struct {
	Action      string
	Package     string
	Test        string
	Output      string
	FailedBuild string
}

// readResults reads `go test -json` output and returns the final outcome of
// every test, keyed "<package> <test>", and of every package, keyed
// "<package> (package)". A package that fails to build is reported as a
// package failure.
func readResults(r io.Reader) (map[string]outcome, error) {
	results := map[string]outcome{}
	details := map[string]string{}
	errorLines := map[string]string{}
	messageLines := map[string]string{}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 || line[0] != '{' {
			continue // go test may print non-JSON lines, e.g. linker warnings
		}

		var e testEvent
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, errors.NewProcessingError("decoding go test -json line %q", string(line), err)
		}

		if e.Package == "" {
			continue // build-output events carry ImportPath, not Package
		}

		if e.Test == "" {
			// A package's own output is the test binary's log, not a reason.
			if e.Action == "pass" || e.Action == "fail" || e.Action == "skip" {
				o := outcome{action: e.Action}
				if e.FailedBuild != "" {
					o.detail = "build failed: " + e.FailedBuild
				}

				results[e.Package+" (package)"] = o
			}

			continue
		}

		key := e.Package + " " + e.Test

		switch e.Action {
		case "output":
			d := meaningfulOutput(e.Output)
			if d == "" {
				continue
			}

			details[key] = d

			// testify prints "Error Trace:", "Error:", "Test:"; the Error line
			// says what failed, the last line only names the test.
			if _, seen := errorLines[key]; !seen && strings.HasPrefix(d, "Error:") {
				errorLines[key] = d
			}

			// When the assertion carries a message, it says more than "Should be true".
			if _, seen := messageLines[key]; !seen && strings.HasPrefix(d, "Messages:") {
				messageLines[key] = d
			}
		case "pass", "fail", "skip":
			detail := details[key]

			switch {
			case e.Action != "fail":
			case messageLines[key] != "":
				detail = messageLines[key]
			case errorLines[key] != "":
				detail = errorLines[key]
			}

			results[key] = outcome{action: e.Action, detail: detail}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// meaningfulOutput trims a test output line and drops go test's own framing
// lines, which carry no reason.
func meaningfulOutput(s string) string {
	s = strings.TrimSpace(s)

	for _, prefix := range []string{"=== ", "--- ", "PASS", "FAIL", "ok ", "?"} {
		if strings.HasPrefix(s, prefix) {
			return ""
		}
	}

	return s
}

// change is a test or package whose outcome differs between the two runs.
type change struct {
	key  string
	base outcome
	head outcome
}

// diffResults compares the runs. changes holds every key the base run reported
// whose outcome differs in the head run, including keys the head run lacks.
// added holds the keys only the head run reported: new tests say nothing about
// whether the libraries changed behaviour, so they are listed, not failed. Both
// are sorted.
func diffResults(base, head map[string]outcome) (changes []change, added []string) {
	for k, b := range base {
		h, ok := head[k]
		if !ok {
			h = outcome{action: absent}
		}

		if b.action != h.action {
			changes = append(changes, change{key: k, base: b, head: h})
		}
	}

	for k := range head {
		if _, ok := base[k]; !ok {
			added = append(added, k)
		}
	}

	sort.Slice(changes, func(i, j int) bool { return changes[i].key < changes[j].key })
	sort.Strings(added)

	return changes, added
}

// counts returns how many results ended in each action.
func counts(results map[string]outcome) map[string]int {
	c := map[string]int{}
	for _, o := range results {
		c[o.action]++
	}

	return c
}

// writeReport writes a Markdown summary of the comparison. At most limit
// changes and limit added tests are listed; the rest are counted, never
// dropped silently.
func writeReport(w io.Writer, base, head map[string]outcome, changes []change, added []string, limit int) {
	bc, hc := counts(base), counts(head)

	fmt.Fprintf(w, "| Run | pass | fail | skip |\n|---|---|---|---|\n")
	fmt.Fprintf(w, "| base | %d | %d | %d |\n", bc["pass"], bc["fail"], bc["skip"])
	fmt.Fprintf(w, "| pull request | %d | %d | %d |\n\n", hc["pass"], hc["fail"], hc["skip"])

	if len(changes) == 0 {
		fmt.Fprintln(w, "Every test and package the base run reported has the same outcome in the pull request.")
	} else {
		fmt.Fprintf(w, "%d tests or packages have a different outcome:\n\n", len(changes))
		fmt.Fprintln(w, "| Test | Base | Pull request |\n|---|---|---|")

		for i, c := range changes {
			if i == limit {
				fmt.Fprintf(w, "\n%d more not listed; see the job log.\n", len(changes)-limit)
				break
			}

			fmt.Fprintf(w, "| `%s` | %s | %s |\n", c.key, cell(c.base), cell(c.head))
		}
	}

	if len(added) == 0 {
		return
	}

	fmt.Fprintf(w, "\n%d tests are new in the pull request (not compared):\n\n", len(added))

	for i, k := range added {
		if i == limit {
			fmt.Fprintf(w, "\n%d more not listed.\n", len(added)-limit)
			break
		}

		fmt.Fprintf(w, "- `%s`: %s\n", k, cell(head[k]))
	}
}

// cell renders an outcome for a Markdown table cell.
func cell(o outcome) string {
	if o.detail == "" {
		return o.action
	}

	d := o.detail
	if len(d) > 160 {
		d = d[:160] + "…"
	}

	d = strings.NewReplacer("|", `\|`, "`", "'").Replace(d)

	return o.action + ": " + d
}
