package compat

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// legacyRoot is the package tree that owns the fenced packages below. It,
// and everything under it, may import the fenced packages freely: they are
// its own implementation details.
const legacyRoot = "github.com/bsv-blockchain/teranode/services/legacy"

// fencedPaths are the legacy script/address import paths new code must not
// import directly. Consumers must use the compat/ replacements instead, so
// services/legacy/{txscript,bsvutil,bsvec} stay deletable at cutover.
//
// bsvutil is fenced as a whole subtree: a match on this path also matches
// its subpackages (bech32, bloom, hdkeychain, merkleblock, txsort) via
// prefix, see matchesFenced. Verified 2026-08-18: bloom, hdkeychain,
// merkleblock, and txsort have no importers outside services/legacy today,
// so they need no allowlist rows of their own; a new external import of any
// of them is caught by the "bsvutil" row below.
var fencedPaths = []string{
	legacyRoot + "/txscript",
	legacyRoot + "/bsvutil",
	legacyRoot + "/bsvec",
}

// allowlistEntry names one module package still permitted to import a
// fenced package directly, and why. Every entry must carry a reason and,
// where it applies, the cutover ticket that removes the need for it.
type allowlistEntry struct {
	pkgPath  string // production PkgPath of the allowed package
	testOnly bool   // true: only that package's own tests may import; its production code may not
	reason   string
}

// allowlist holds every consumer outside services/legacy/ verified today to
// still import a fenced package directly.
//
// services/legacy/ itself needs no row here: everything under legacyRoot is
// allowed unconditionally (see isUnderLegacyRoot), because the fenced
// packages are implementation details of that tree.
//
// Verified 2026-08-18: daemon/daemon_services.go, cmd/peercli/{main.go,
// peer_handlers.go, peer_handlers_test.go}, and services/alert/{server.go,
// node.go} import only services/legacy (root) and services/legacy/peer or
// services/legacy/peer_api — none of the three fencedPaths. They need no
// row here; moving them off services/legacy entirely is handled by the
// peer_api relocation at cutover, out of this fence's scope.
var allowlist = []allowlistEntry{
	{
		pkgPath:  "github.com/bsv-blockchain/teranode/model",
		testOnly: true,
		reason: "model/Block_test.go calls bsvutil.NewBlockFromReader/Block; " +
			"compat/bsvutil has no Block type, so nothing was ported for it. " +
			"Must gain a compat equivalent, or be dropped, when " +
			"services/legacy is deleted at cutover (spec §8 follow-up).",
	},
}

// TestBoundary fails if any package outside services/legacy/ (and not on
// the allowlist above) imports one of fencedPaths, directly, from either
// production or test files. It loads the whole module's package graph once
// (including test variants) and checks all three fenced paths against it.
func TestBoundary(t *testing.T) {
	pkgs := loadModulePackages(t)

	for _, fencedPath := range fencedPaths {
		fencedPath := fencedPath
		name := strings.TrimPrefix(fencedPath, legacyRoot+"/")
		t.Run(name, func(t *testing.T) {
			violations := findViolations(pkgs, fencedPath)
			if len(violations) == 0 {
				return
			}

			var b strings.Builder
			fmt.Fprintf(&b, "found %d new consumer(s) importing %s directly (or a subpackage of it):\n", len(violations), fencedPath)
			for _, v := range violations {
				fmt.Fprintf(&b, "  %s imports %s\n", v.pkgID, v.importPath)
			}
			b.WriteString("Use the compat/ replacement instead, or add a reviewed allowlist entry in compat/boundary_test.go naming why.\n")
			t.Fatal(b.String())
		})
	}
}

type violation struct {
	pkgID      string
	importPath string
}

// findViolations reports every loaded package, outside legacyRoot and the
// allowlist, whose direct imports include fencedPath or one of its
// subpackages.
func findViolations(pkgs []*packages.Package, fencedPath string) []violation {
	var out []violation

	for _, p := range pkgs {
		base := basePkgPath(p)
		if isUnderLegacyRoot(base) {
			continue
		}
		if isAllowed(base, isTestVariant(p)) {
			continue
		}

		for imp := range p.Imports {
			if matchesFenced(imp, fencedPath) {
				out = append(out, violation{pkgID: p.ID, importPath: imp})
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].pkgID != out[j].pkgID {
			return out[i].pkgID < out[j].pkgID
		}
		return out[i].importPath < out[j].importPath
	})

	return out
}

func matchesFenced(importPath, fencedPath string) bool {
	return importPath == fencedPath || strings.HasPrefix(importPath, fencedPath+"/")
}

func isUnderLegacyRoot(pkgPath string) bool {
	return pkgPath == legacyRoot || strings.HasPrefix(pkgPath, legacyRoot+"/")
}

func isAllowed(base string, testVariant bool) bool {
	for _, e := range allowlist {
		if e.pkgPath != base {
			continue
		}
		return !e.testOnly || testVariant
	}
	return false
}

// isTestVariant reports whether p is a test-only variant of a package
// (golang.org/x/tools/go/packages gives these an ID like "X [X.test]" or
// "X_test [X.test]", distinct from the production package whose ID is
// simply its PkgPath).
func isTestVariant(p *packages.Package) bool {
	return strings.Contains(p.ID, " [")
}

// basePkgPath returns the production PkgPath a package variant belongs to,
// so an external test package (PkgPath "foo_test") is matched against the
// same allowlist row as its production package "foo".
func basePkgPath(p *packages.Package) string {
	path := p.PkgPath
	if isTestVariant(p) && strings.HasSuffix(path, "_test") {
		return strings.TrimSuffix(path, "_test")
	}
	return path
}

// loadModulePackages loads every package in the module, including test
// variants, in a single golang.org/x/tools/go/packages call. It fails the
// test on any load error rather than silently trusting a partial graph.
func loadModulePackages(t *testing.T) []*packages.Package {
	t.Helper()

	cfg := &packages.Config{
		Mode:  packages.NeedName | packages.NeedImports,
		Tests: true,
		Dir:   repoRoot(t),
	}

	// The fence loads with default build tags only, so consumers behind
	// build tags (e.g., test_txscript, aerospike) would evade it.
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}

	var loadErrs []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			loadErrs = append(loadErrs, fmt.Sprintf("%s: %v", p.ID, e))
		}
	})

	if len(loadErrs) > 0 {
		sort.Strings(loadErrs)
		t.Fatalf("go/packages reported load errors; the fence cannot trust an incomplete import graph:\n%s", strings.Join(loadErrs, "\n"))
	}

	return pkgs
}

// repoRoot locates the module root from this test file's own location
// (compat/boundary_test.go), so the load works regardless of the process's
// current working directory.
func repoRoot(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed; cannot locate repo root")
	}

	return filepath.Dir(filepath.Dir(thisFile))
}
