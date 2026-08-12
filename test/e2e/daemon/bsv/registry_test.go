package bsv

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// registryFile is the hand-maintained source of truth for the bitcoin-sv
// functional-test porting exercise. See PORTING.md for the workflow.
const registryFile = "registry.yaml"

// Entry is one upstream bitcoin-sv functional test and its porting state.
type Entry struct {
	// Name is the upstream script filename, e.g. "invalidblockrequest.py".
	Name string `yaml:"name"`
	// Bucket is the triage class: A (portable), B (blocked on test-hook RPCs),
	// C (not applicable to Teranode's architecture).
	Bucket string `yaml:"bucket"`
	// Status is the porting state; see validStatuses.
	Status string `yaml:"status"`
	// Needs lists harness prerequisites, e.g. "wirepeer", "funding-shim".
	Needs []string `yaml:"needs"`
	// UpstreamHooks lists bitcoin-sv test-only RPCs the original relies on.
	UpstreamHooks []string `yaml:"upstream_hooks"`
	// Reason explains a blocked or not-applicable classification.
	Reason string `yaml:"reason"`
	// PortedTo names the Go test function in this package that carries the port.
	PortedTo string `yaml:"ported_to"`
	// UpstreamAssertions enumerates what the original test actually asserts.
	// Every entry must be covered by the port or listed in WaivedAssertions.
	UpstreamAssertions []string `yaml:"upstream_assertions"`
	// WaivedAssertions records upstream assertions the port deliberately does
	// not make, each as "assertion: reason".
	WaivedAssertions []string `yaml:"waived_assertions"`
}

// Gap is a known obstacle holding up a set of otherwise-portable tests. Gaps are
// tracked in the registry rather than in prose so the number of tests each one
// blocks is derived from the data, not restated by hand.
type Gap struct {
	// ID is a stable kebab-case identifier, referenced from PORTING.md.
	ID string `yaml:"id"`
	// Title states the obstacle in one line.
	Title string `yaml:"title"`
	// Kind classifies it; see validGapKinds.
	Kind string `yaml:"kind"`
	// Status is the gap's lifecycle state; see validGapStatuses.
	Status string `yaml:"status"`
	// Impact says what cannot be ported, and what the consequence is beyond
	// testing if any.
	Impact string `yaml:"impact"`
	// Plan says what happens next, and explicitly whether the next step is
	// diagnosis or implementation.
	Plan string `yaml:"plan"`
	// Blocks names upstream scripts this gap holds up directly.
	Blocks []string `yaml:"blocks"`
	// BlocksHooks names upstream_hooks whose entries this gap holds up, for gaps
	// too broad to enumerate script by script.
	BlocksHooks []string `yaml:"blocks_hooks"`
	// FoundWhilePorting names the script whose port surfaced this gap, for gaps
	// that block nothing. Porting turns up defects that cost nothing in coverage
	// - a slow RPC, a wrong log line - and those are worth recording even though
	// no script is held up. Setting this is how a gap earns an empty blocks list,
	// so an empty one still cannot pass unexplained.
	FoundWhilePorting string `yaml:"found_while_porting"`
}

type registry struct {
	Gaps    []Gap   `yaml:"gaps"`
	Entries []Entry `yaml:"entries"`
}

var validBuckets = map[string]bool{"A": true, "B": true, "C": true}

// validStatuses maps each status to whether it requires a ported_to target.
var validStatuses = map[string]bool{
	"todo":           false, // triaged as portable, not started
	"in-progress":    false, // being ported now
	"ported":         true,  // ported with full assertion fidelity
	"ported-partial": true,  // ported, but some upstream assertions waived
	"blocked":        false, // portable in principle, prerequisite missing
	"not-applicable": false, // no Teranode counterpart, by architecture
}

// validNeeds is the closed set of harness prerequisites an entry may declare.
// Closed on purpose: `needs` drives the bucket-A breakdown in TestRegistrySummary,
// so a typo would not fail anything - it would quietly invent a prerequisite,
// report "1 need frozentxo" next to "4 need frozen-txo", and make the numbers
// people plan from wrong. Adding a genuinely new prerequisite means adding it
// here and describing it in PORTING.md, which is the point.
var validNeeds = map[string]string{
	"wirepeer":       "the Go mininode analogue over go-wire; built",
	"funding-shim":   "wallet-shaped funding the tests assume; not built",
	"frozen-txo":     "a way to freeze/unfreeze a UTXO from a test; not built",
	"test-hook-rpcs": "bitcoin-sv RPCs that exist only for testing; see bucket B",
}

// validGapKinds records what sort of thing a gap is, because the right next step
// differs: a test-config gap is ours to fix, a defect belongs in the issue
// tracker, and a product-decision is not ours to make at all.
var validGapKinds = map[string]bool{
	"test-config":      true, // the test harness, not the node, is at fault
	"defect":           true, // confirmed node bug; escalate separately
	"product-decision": true, // needs a deliberate decision outside this exercise
	"unknown":          true, // not yet diagnosed; must not stay here indefinitely
}

var validGapStatuses = map[string]bool{
	"open":          true, // known, not being worked on
	"investigating": true, // diagnosis in progress
	"deferred":      true, // deliberately not being worked on; plan must say why
	"resolved":      true, // closed; plan must record the outcome
}

func loadRegistry(t *testing.T) registry {
	t.Helper()

	data, err := os.ReadFile(registryFile)
	require.NoError(t, err, "read %s", registryFile)

	var reg registry
	require.NoError(t, yaml.Unmarshal(data, &reg), "parse %s", registryFile)
	require.NotEmpty(t, reg.Entries, "%s has no entries", registryFile)

	return reg
}

// goTestFuncs returns the names of every Test function declared in this package.
func goTestFuncs(t *testing.T) map[string]bool {
	t.Helper()

	files, err := filepath.Glob("*_test.go")
	require.NoError(t, err)

	found := make(map[string]bool)

	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		require.NoError(t, err, "parse %s", file)

		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Test") {
				found[fn.Name.Name] = true
			}
		}
	}

	return found
}

// TestRegistryIsWellFormed guards the schema so the tracker cannot rot into
// free-form notes: legal buckets and statuses, no duplicates, and the
// bucket/status combinations that would be self-contradictory.
func TestRegistryIsWellFormed(t *testing.T) {
	reg := loadRegistry(t)

	seen := make(map[string]bool, len(reg.Entries))

	for _, e := range reg.Entries {
		require.True(t, strings.HasSuffix(e.Name, ".py"), "entry name %q must be an upstream .py script", e.Name)
		require.False(t, seen[e.Name], "duplicate registry entry for %s", e.Name)
		seen[e.Name] = true

		require.True(t, validBuckets[e.Bucket], "%s: unknown bucket %q", e.Name, e.Bucket)

		_, ok := validStatuses[e.Status]
		require.True(t, ok, "%s: unknown status %q", e.Name, e.Status)

		for _, need := range e.Needs {
			_, known := validNeeds[need]
			require.True(t, known,
				"%s: unknown prerequisite %q - add it to validNeeds and document it in PORTING.md, "+
					"or fix the typo", e.Name, need)
		}

		switch e.Bucket {
		case "C":
			require.Equal(t, "not-applicable", e.Status,
				"%s: bucket C must have status not-applicable", e.Name)
		case "B":
			require.NotEmpty(t, e.UpstreamHooks,
				"%s: bucket B must list the upstream_hooks it depends on", e.Name)
		case "A":
			require.NotEqual(t, "not-applicable", e.Status,
				"%s: bucket A cannot be not-applicable; move it to bucket C with a reason", e.Name)
		}

		if e.Status == "not-applicable" || e.Status == "blocked" {
			require.NotEmpty(t, strings.TrimSpace(e.Reason),
				"%s: status %q requires a reason", e.Name, e.Status)
		}
	}
}

// TestRegistryPortedEntriesExist is the anti-drift check: a status of ported or
// ported-partial must name a Go test function that actually exists here, and no
// ported Go test may be missing from the registry.
func TestRegistryPortedEntriesExist(t *testing.T) {
	reg := loadRegistry(t)
	funcs := goTestFuncs(t)

	claimed := make(map[string]string)

	for _, e := range reg.Entries {
		needsTarget := validStatuses[e.Status]

		if !needsTarget {
			require.Empty(t, e.PortedTo,
				"%s: status %q must not name a ported_to target", e.Name, e.Status)

			continue
		}

		require.NotEmpty(t, e.PortedTo, "%s: status %q requires ported_to", e.Name, e.Status)
		require.True(t, funcs[e.PortedTo],
			"%s: ported_to names %s, which does not exist in this package", e.Name, e.PortedTo)

		prev, dup := claimed[e.PortedTo]
		require.False(t, dup, "%s and %s both claim Go test %s", prev, e.Name, e.PortedTo)
		claimed[e.PortedTo] = e.Name
	}

	// Every Test function in this package must be accounted for, so a port can
	// never be added without a registry row. Registry's own tests and the
	// package entry point are exempt.
	for name := range funcs {
		if strings.HasPrefix(name, "TestRegistry") || name == "TestMain" {
			continue
		}

		_, ok := claimed[name]
		require.True(t, ok,
			"Go test %s is not referenced by any registry entry; add a ported_to row in %s",
			name, registryFile)
	}
}

// TestRegistryFidelityContract enforces the rule that makes the count
// meaningful: a port claiming full fidelity must enumerate the upstream
// assertions it reproduces, and a partial port must say what it dropped and why.
func TestRegistryFidelityContract(t *testing.T) {
	reg := loadRegistry(t)

	for _, e := range reg.Entries {
		switch e.Status {
		case "ported":
			require.NotEmpty(t, e.UpstreamAssertions,
				"%s: status ported requires upstream_assertions listing what the original asserts", e.Name)
			require.Empty(t, e.WaivedAssertions,
				"%s: status ported cannot waive assertions; use ported-partial", e.Name)
		case "ported-partial":
			require.NotEmpty(t, e.UpstreamAssertions,
				"%s: status ported-partial requires upstream_assertions", e.Name)
			require.NotEmpty(t, e.WaivedAssertions,
				"%s: status ported-partial requires waived_assertions; use ported if nothing was dropped", e.Name)

			for _, w := range e.WaivedAssertions {
				require.Contains(t, w, ":",
					"%s: waived assertion %q must be formatted \"assertion: reason\"", e.Name, w)
			}
		}
	}
}

// TestRegistryGaps keeps the gap register honest: a gap must say what it holds
// up and what happens next, and the tests it claims to block must actually
// exist. A gap that names a script that was renamed away, or a hook no entry
// depends on, is stale documentation pretending to be a plan.
func TestRegistryGaps(t *testing.T) {
	reg := loadRegistry(t)
	require.NotEmpty(t, reg.Gaps, "%s has no gaps section", registryFile)

	entries := make(map[string]bool, len(reg.Entries))
	hooks := make(map[string]bool)

	for _, e := range reg.Entries {
		entries[e.Name] = true

		for _, h := range e.UpstreamHooks {
			hooks[h] = true
		}
	}

	seen := make(map[string]bool, len(reg.Gaps))

	for _, g := range reg.Gaps {
		require.NotEmpty(t, g.ID, "every gap needs an id")
		require.False(t, seen[g.ID], "duplicate gap id %q", g.ID)
		seen[g.ID] = true

		require.True(t, validGapKinds[g.Kind], "%s: unknown gap kind %q", g.ID, g.Kind)
		require.True(t, validGapStatuses[g.Status], "%s: unknown gap status %q", g.ID, g.Status)

		require.NotEmpty(t, strings.TrimSpace(g.Title), "%s: gap needs a title", g.ID)
		require.NotEmpty(t, strings.TrimSpace(g.Impact), "%s: gap needs an impact", g.ID)
		require.NotEmpty(t, strings.TrimSpace(g.Plan), "%s: gap needs a plan", g.ID)

		if len(g.Blocks) == 0 && len(g.BlocksHooks) == 0 {
			require.NotEmpty(t, g.FoundWhilePorting,
				"%s: a gap must either block something, via blocks or blocks_hooks, or name the port "+
					"that found it, via found_while_porting", g.ID)
			require.True(t, entries[g.FoundWhilePorting],
				"%s: found_while_porting names %s, which is not a tracked upstream script",
				g.ID, g.FoundWhilePorting)
		}

		for _, name := range g.Blocks {
			require.True(t, entries[name],
				"%s: blocks %s, which is not a tracked upstream script", g.ID, name)
		}

		for _, h := range g.BlocksHooks {
			require.True(t, hooks[h],
				"%s: blocks_hooks names %q, which no registry entry depends on", g.ID, h)
		}
	}
}

// TestRegistryGapReport prints every gap in full: what it is, who has to act,
// what it holds up, and what happens next. It asserts nothing - TestRegistryGaps
// does the checking - and exists so `make bsvportinggaps` answers "what is in
// the way, and why" without anyone reading YAML.
//
// Gaps are printed worst-first by the number of scripts they hold up, because
// that is the order in which fixing them buys the most.
func TestRegistryGapReport(t *testing.T) {
	reg := loadRegistry(t)

	gaps := slices.Clone(reg.Gaps)
	slices.SortStableFunc(gaps, func(a, b Gap) int {
		return gapBlockedCount(b, reg.Entries) - gapBlockedCount(a, reg.Entries)
	})

	for _, g := range gaps {
		blocked := make([]string, 0, len(g.Blocks))
		blocked = append(blocked, g.Blocks...)

		for _, h := range g.BlocksHooks {
			blocked = append(blocked, "(all tests needing "+h+")")
		}

		// A gap that blocks nothing is still worth acting on - it just cannot be
		// ranked by coverage - so say what it is rather than printing "0 tests".
		headline := fmt.Sprintf("holds up %d test(s)", gapBlockedCount(g, reg.Entries))
		if len(blocked) == 0 {
			headline = "blocks no test, found while porting " + g.FoundWhilePorting
			blocked = append(blocked, "nothing - recorded for its own sake")
		}

		t.Logf("\n%s [%s / %s] %s\n  %s\n\n  Impact: %s\n\n  Plan:   %s\n\n  Blocks: %s\n",
			g.ID, g.Kind, g.Status, headline, g.Title,
			unwrap(g.Impact), unwrap(g.Plan), strings.Join(blocked, ", "))
	}

	t.Logf("%d gap(s). kind=defect belongs in the issue tracker; kind=test-config is ours to fix; "+
		"kind=product-decision is not ours to make; kind=unknown needs diagnosis before anything else.",
		len(gaps))
}

// unwrap collapses a YAML folded block back onto one logical paragraph so the
// test log does not double-wrap it.
func unwrap(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// TestRegistrySummary prints the live progress table. It asserts nothing about
// progress itself; it exists so `go test -run TestRegistrySummary -v` is the
// single command that answers "where is the porting exercise at?".
func TestRegistrySummary(t *testing.T) {
	reg := loadRegistry(t)

	byStatus := make(map[string]int)
	byNeed := make(map[string]int)
	byHook := make(map[string]int)

	for _, e := range reg.Entries {
		byStatus[e.Status]++

		if e.Bucket == "A" {
			if len(e.Needs) == 0 {
				byNeed["(none)"]++
			}

			for _, n := range e.Needs {
				byNeed[n]++
			}
		}

		for _, h := range e.UpstreamHooks {
			byHook[h]++
		}
	}

	readyNow, gapHeld := countReadyToPort(reg)

	t.Logf("upstream scripts tracked: %d", len(reg.Entries))

	logCounts(t, "status", byStatus)
	logCounts(t, "bucket-A prerequisite", byNeed)
	logCounts(t, "bucket-B upstream hook", byHook)

	for _, g := range reg.Gaps {
		t.Logf("  gap=%-28s %s/%s, holds up %d test(s)",
			g.ID, g.Kind, g.Status, gapBlockedCount(g, reg.Entries))
	}

	done := byStatus["ported"] + byStatus["ported-partial"]
	portable := done + byStatus["todo"] + byStatus["in-progress"]

	t.Logf("progress: %d/%d portable ported (%d full, %d partial)",
		done, portable, byStatus["ported"], byStatus["ported-partial"])
	t.Logf("startable today: %d bucket-A entries need no prerequisite and no gap; "+
		"a further %d need no prerequisite but are held up by a gap", readyNow, gapHeld)
}

// countReadyToPort splits the bucket-A entries that declare no prerequisite into
// the ones someone could pick up right now and the ones a gap already holds up.
//
// The distinction matters because a gap-blocked entry keeps status todo by
// convention - the gap records the obstacle, not the entry - so the raw todo count
// reads as available work when part of it is not. Reporting both keeps the honest
// number beside the flattering one.
func countReadyToPort(reg registry) (readyNow, gapHeld int) {
	blockedByGap := make(map[string]bool)

	for _, g := range reg.Gaps {
		for _, name := range g.Blocks {
			blockedByGap[name] = true
		}
	}

	for _, e := range reg.Entries {
		if e.Bucket != "A" || e.Status != "todo" || len(e.Needs) > 0 {
			continue
		}

		if blockedByGap[e.Name] {
			gapHeld++
		} else {
			readyNow++
		}
	}

	return readyNow, gapHeld
}

// gapBlockedCount is how many upstream scripts a gap holds up: the ones it names
// directly, plus every entry depending on a hook it names.
func gapBlockedCount(g Gap, entries []Entry) int {
	blocked := len(g.Blocks)

	for _, e := range entries {
		for _, h := range e.UpstreamHooks {
			if slices.Contains(g.BlocksHooks, h) {
				blocked++

				break
			}
		}
	}

	return blocked
}

func logCounts(t *testing.T, label string, counts map[string]int) {
	t.Helper()

	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}

		return keys[i] < keys[j]
	})

	for _, k := range keys {
		t.Logf("  %-40s %d", fmt.Sprintf("%s=%s", label, k), counts[k])
	}
}
