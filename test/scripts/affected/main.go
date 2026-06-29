// Command affected computes the minimal set of tests a change needs to run.
//
// It reads a newline-separated list of changed files on stdin (repo-relative
// paths, as produced by `git diff --name-only`), builds the Go package import
// graph via `go list`, and emits the reverse-dependency closure of the changed
// packages — i.e. every package that is changed or transitively imports a
// changed package. The closure is then classified per CI suite.
//
// See plans/ci-test-scoping.md for the rules this implements.
//
// Output is GITHUB_OUTPUT-style `key=value` lines (or JSON with -json):
//
//	full=<bool>                  run the complete suite; ignore the scoped lists
//	reason=<string>              human-readable explanation
//	run_unit=<bool>              run scoped unit tests
//	unit_pkgs=<space-separated>  import paths for `make test TEST_PKGS=...`
//	run_smoke=<bool>             closure touches test/e2e/daemon/ready
//	run_chainintegrity=<bool>    closure touches test/e2e/chainintegrity
//	run_seq=<bool>               run scoped sequential tests
//	seq_pkgs=<space-separated>   repo-relative dirs for run_tests_sequentially.sh
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const modulePath = "github.com/bsv-blockchain/teranode"

// buildTags is the union of every build tag any in-scope CI suite compiles with.
// Over-including tags only enlarges the package graph (more import edges visible),
// which can only widen the closure — never scope a real change out. A MISSING tag
// hides edges and is unsafe, so this list must be a superset of all CI tag sets:
//   - unit (make test):        testtxmetacache
//   - sequential:              aerospike,native,functional,test_sequentially,test_all,memory,postgres,sqlite
//   - smoke (e2e/daemon/ready): (none beyond default)
const buildTags = "testtxmetacache,aerospike,native,functional,test_sequentially,test_all,memory,postgres,sqlite"

// Suite anchor packages (import paths). A suite runs when the closure contains
// its anchor. smoke/pruner/legacy all live in the same e2e/daemon/ready package.
const (
	pkgE2EReady       = modulePath + "/test/e2e/daemon/ready"
	pkgChainIntegrity = modulePath + "/test/e2e/chainintegrity"
	prefixSequential  = modulePath + "/test/sequentialtest/"
	prefixTest        = modulePath + "/test/"
)

// Pkg is the subset of `go list -json` output we need.
//
// Deps is the transitive non-test dependency set. Test imports are reported
// separately (and only as DIRECT imports) in TestImports/XTestImports — so for a
// package whose production dependency on X exists only through its _test.go files,
// X appears in TestImports but NOT in Deps. The closure must consult both, or a
// test-only package would never be flagged affected by a change to what it tests.
type Pkg struct {
	ImportPath   string   `json:"ImportPath"`
	Dir          string   `json:"Dir"`
	Deps         []string `json:"Deps"`         // transitive non-test deps
	TestImports  []string `json:"TestImports"`  // direct imports of in-package _test.go
	XTestImports []string `json:"XTestImports"` // direct imports of external _test package
}

// Result is the scoping decision.
type Result struct {
	Full              bool     `json:"full"`
	AnyGo             bool     `json:"any_go"` // any .go file changed → run linters
	Reason            string   `json:"reason"`
	RunUnit           bool     `json:"run_unit"`
	UnitPkgs          []string `json:"unit_pkgs"`
	RunSmoke          bool     `json:"run_smoke"`
	RunChainIntegrity bool     `json:"run_chainintegrity"`
	RunSeq            bool     `json:"run_seq"`
	SeqPkgs           []string `json:"seq_pkgs"`
}

// isGlobalInput reports whether a changed file is a Tier-0 global input whose
// blast radius the import graph cannot capture, forcing a full run.
func isGlobalInput(f string) bool {
	switch f {
	case "go.mod", "go.sum", "Makefile":
		return true
	}
	switch {
	case strings.HasPrefix(f, ".github/workflows/"),
		strings.HasPrefix(f, ".github/actions/"),
		strings.HasPrefix(f, "settings/"),
		strings.HasPrefix(f, "test/scripts/affected/"),
		f == "test/scripts/run_tests_sequentially.sh",
		// Test/CI build & runtime infra the import graph cannot see: the compose
		// stacks that wire e2e/smoke/chainintegrity nodes together, and the lint
		// config (any encoding) that gates the linter jobs.
		strings.HasPrefix(f, "test/docker-compose"),
		strings.HasPrefix(f, "compose/docker-compose"),
		strings.HasSuffix(f, ".golangci.yml"),
		strings.HasSuffix(f, ".golangci.yaml"),
		strings.HasSuffix(f, ".golangci.json"),
		strings.HasSuffix(f, ".golangci.toml"):
		return true
	}
	base := filepath.Base(f)
	// Any Dockerfile (root image + test/utils helper images) builds something the
	// e2e/smoke suites run against; a change there can break tests no Go edge links.
	if strings.HasPrefix(base, "Dockerfile") {
		return true
	}
	return strings.HasPrefix(base, "settings") && strings.HasSuffix(base, ".conf")
}

// compute is the pure scoping logic: given the changed files and the package
// graph (repo-relative dirs), it returns the suite-classified closure. Kept free
// of go/exec and filesystem access so it can be unit-tested with fixtures.
//
// pkgs must have Dir set to a REPO-RELATIVE path ("." for the module root).
func compute(changedFiles []string, pkgs []Pkg) (res Result) {
	// any_go gates linters; set it on whatever Result we return (full implies it).
	anyGo := false
	for _, f := range changedFiles {
		if strings.HasSuffix(f, ".go") {
			anyGo = true
			break
		}
	}
	defer func() { res.AnyGo = anyGo || res.Full }()

	// Tier 0: any global input forces a full run.
	for _, f := range changedFiles {
		if isGlobalInput(f) {
			return Result{Full: true, Reason: "global input changed: " + f}
		}
	}

	// Index packages by repo-relative dir for longest-prefix file mapping.
	type pkgRef struct {
		importPath string
		dir        string
	}
	refs := make([]pkgRef, 0, len(pkgs))
	depsOf := make(map[string][]string, len(pkgs))
	for _, p := range pkgs {
		refs = append(refs, pkgRef{importPath: p.ImportPath, dir: p.Dir})
		depsOf[p.ImportPath] = p.Deps
	}

	// Map each changed file to its owning package (longest matching dir prefix).
	// A changed .go file that maps to no known package (e.g. behind an unselected
	// build tag) is treated conservatively as a full run.
	changed := map[string]bool{}
	for _, f := range changedFiles {
		best := ""
		bestLen := -1
		for _, r := range refs {
			if r.dir == "." {
				// Root package owns only root-level .go files — not everything
				// under the tree (".") and not repo-meta files (README, LICENSE).
				if !strings.Contains(f, "/") && strings.HasSuffix(f, ".go") && bestLen < 0 {
					best, bestLen = r.importPath, 0
				}
				continue
			}
			if f == r.dir || strings.HasPrefix(f, r.dir+"/") {
				if len(r.dir) > bestLen {
					best, bestLen = r.importPath, len(r.dir)
				}
			}
		}
		if best == "" {
			if strings.HasSuffix(f, ".go") {
				return Result{Full: true, Reason: "changed .go file maps to no known package: " + f}
			}
			continue // non-Go file outside any package — no test impact
		}
		changed[best] = true
	}

	// Tier 1: nothing mapped to a package — docs/assets only.
	if len(changed) == 0 {
		return Result{Reason: "no Go packages affected"}
	}

	// Reverse-dependency closure: a package is affected if it is changed, or it
	// (transitively) imports a changed package — through either its production
	// deps or its test imports. Test imports are direct only, so each is expanded
	// through the changed package's transitive non-test deps (depsOf).
	affected := map[string]bool{}
	dependsOnChanged := func(imp string) bool {
		if changed[imp] {
			return true
		}
		for _, d := range depsOf[imp] {
			if changed[d] {
				return true
			}
		}
		return false
	}
	for _, p := range pkgs {
		if changed[p.ImportPath] {
			affected[p.ImportPath] = true
			continue
		}
		hit := false
		for _, d := range p.Deps {
			if changed[d] {
				hit = true
				break
			}
		}
		if !hit {
			for _, ti := range p.TestImports {
				if dependsOnChanged(ti) {
					hit = true
					break
				}
			}
		}
		if !hit {
			for _, ti := range p.XTestImports {
				if dependsOnChanged(ti) {
					hit = true
					break
				}
			}
		}
		if hit {
			affected[p.ImportPath] = true
		}
	}

	res = Result{Reason: fmt.Sprintf("%d changed package(s), %d affected", len(changed), len(affected))}
	var unit, seq []string
	for ip := range affected {
		switch {
		case ip == pkgE2EReady:
			res.RunSmoke = true
		case ip == pkgChainIntegrity:
			res.RunChainIntegrity = true
		case strings.HasPrefix(ip, prefixSequential):
			seq = append(seq, strings.TrimPrefix(ip, modulePath+"/"))
		case strings.HasPrefix(ip, prefixTest):
			// other test/ suites (tna, tec, longtest…) — out of scope here
		default:
			unit = append(unit, ip)
		}
	}
	sort.Strings(unit)
	sort.Strings(seq)
	res.UnitPkgs, res.SeqPkgs = unit, seq
	res.RunUnit = len(unit) > 0
	res.RunSeq = len(seq) > 0
	return res
}

// goList runs `go list` with the union build tags and returns packages with
// repo-relative Dir paths.
func goList(root string) ([]Pkg, error) {
	cmd := exec.Command("go", "list", "-json", "-tags", buildTags, "./...")
	cmd.Dir = root
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w", err)
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	var pkgs []Pkg
	dec := json.NewDecoder(strings.NewReader(string(out)))
	for dec.More() {
		var p Pkg
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("decode go list output: %w", err)
		}
		if rel, err := filepath.Rel(absRoot, p.Dir); err == nil {
			p.Dir = rel
		}
		pkgs = append(pkgs, p)
	}
	return pkgs, nil
}

// readChangedFiles reads the newline-separated changed-file list from r. A read
// error (anything other than clean EOF) is returned rather than swallowed: a
// truncated list would silently under-select the test set.
func readChangedFiles(r *bufio.Scanner) ([]string, error) {
	var files []string
	for r.Scan() {
		line := strings.TrimSpace(r.Text())
		if line != "" {
			files = append(files, line)
		}
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	return files, nil
}

// emit writes the result. The whole key=value block is buffered and written
// once so a short write surfaces as an error the caller fails on, rather than a
// truncated output set that would silently skip CI jobs.
func emit(res Result, asJSON bool, w *os.File) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	// reason is free-form and can echo an attacker-influenced filename; strip
	// CR/LF so it can never inject extra key=value lines into $GITHUB_OUTPUT.
	reason := strings.NewReplacer("\r", " ", "\n", " ").Replace(res.Reason)
	var b strings.Builder
	fmt.Fprintf(&b, "full=%t\n", res.Full)
	fmt.Fprintf(&b, "any_go=%t\n", res.AnyGo)
	fmt.Fprintf(&b, "reason=%s\n", reason)
	fmt.Fprintf(&b, "run_unit=%t\n", res.RunUnit)
	fmt.Fprintf(&b, "unit_pkgs=%s\n", strings.Join(res.UnitPkgs, " "))
	fmt.Fprintf(&b, "run_smoke=%t\n", res.RunSmoke)
	fmt.Fprintf(&b, "run_chainintegrity=%t\n", res.RunChainIntegrity)
	fmt.Fprintf(&b, "run_seq=%t\n", res.RunSeq)
	fmt.Fprintf(&b, "seq_pkgs=%s\n", strings.Join(res.SeqPkgs, " "))
	_, err := w.WriteString(b.String())
	return err
}

func main() {
	root := flag.String("root", ".", "repository root to run `go list` from")
	asJSON := flag.Bool("json", false, "emit JSON instead of key=value lines")
	flag.Parse()

	changed, err := readChangedFiles(bufio.NewScanner(os.Stdin))
	if err != nil {
		fmt.Fprintln(os.Stderr, "affected: read stdin:", err)
		os.Exit(1)
	}

	// Fast path: a global input change is full regardless of the graph, so skip
	// the (relatively expensive) `go list` entirely.
	for _, f := range changed {
		if isGlobalInput(f) {
			if err := emit(Result{Full: true, AnyGo: true, Reason: "global input changed: " + f}, *asJSON, os.Stdout); err != nil {
				fmt.Fprintln(os.Stderr, "affected: emit:", err)
				os.Exit(1)
			}
			return
		}
	}

	pkgs, err := goList(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "affected:", err)
		os.Exit(1)
	}
	if err := emit(compute(changed, pkgs), *asJSON, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "affected: emit:", err)
		os.Exit(1)
	}
}
