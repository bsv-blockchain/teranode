package bsv

import (
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// gapsDocFile is the browsable rendering of registry.yaml's gaps: block. The
// register is data so that it can be machine-checked, but data in an 88KB YAML
// file is not something anyone finds or links to from an issue. This is the same
// content as a document.
const gapsDocFile = "GAPS.md"

// updateGapsDoc rewrites GAPS.md instead of checking it. Wired to
// `make bsvportinggapsdoc`.
var updateGapsDoc = flag.Bool("update-gaps-doc", false, "rewrite "+gapsDocFile+" from "+registryFile)

// gapKindMeaning says who has to act on each kind, restating validGapKinds where
// the reader is - in the document - rather than in a Go comment they will not see.
var gapKindMeaning = map[string]string{
	"test-config":      "ours to fix, in this repository",
	"defect":           "a confirmed Teranode bug; belongs in the issue tracker",
	"product-decision": "not ours to make; needs a deliberate decision elsewhere",
	"unknown":          "needs diagnosis before anything else can be decided",
}

// TestRegistryGapsDoc keeps GAPS.md byte-identical to what registry.yaml implies,
// so the document cannot drift from the data the way a hand-maintained table
// does. It runs in `make test` via bsvportingcheck, so a gap edited without
// regenerating fails the build rather than quietly publishing a stale claim.
func TestRegistryGapsDoc(t *testing.T) {
	reg := loadRegistry(t)
	want := renderGapsDoc(reg)

	if *updateGapsDoc {
		require.NoError(t, os.WriteFile(gapsDocFile, []byte(want), 0o600), "write %s", gapsDocFile)
		t.Logf("wrote %s (%d gaps)", gapsDocFile, len(reg.Gaps))

		return
	}

	got, err := os.ReadFile(gapsDocFile)
	require.NoError(t, err, "%s is missing - run `make bsvportinggapsdoc`", gapsDocFile)

	require.Equal(t, want, string(got),
		"%s no longer matches the gaps: block in %s - run `make bsvportinggapsdoc` to regenerate it",
		gapsDocFile, registryFile)
}

// renderGapsDoc builds the whole document. It takes no clock and no environment,
// so the same registry always renders the same bytes - which is what lets the
// check above be an equality assertion rather than a fuzzy one.
func renderGapsDoc(reg registry) string {
	gaps := slices.Clone(reg.Gaps)
	slices.SortStableFunc(gaps, func(a, b Gap) int {
		return gapBlockedCount(b, reg.Entries) - gapBlockedCount(a, reg.Entries)
	})

	var b strings.Builder

	b.WriteString("# bitcoin-sv porting: gap register\n\n")
	b.WriteString("<!-- Generated from registry.yaml by TestRegistryGapsDoc. Do not edit by hand. -->\n")
	b.WriteString("<!-- Regenerate with: make bsvportinggapsdoc -->\n\n")
	b.WriteString("Every obstacle standing between Teranode's current bitcoin-sv port coverage and a\n")
	b.WriteString("larger one. Each entry here is generated from the `gaps:` block at the top of\n")
	b.WriteString("[`registry.yaml`](registry.yaml), which is the source of truth; editing this file\n")
	b.WriteString("by hand will fail `make test`. For how the register fits into the wider exercise,\n")
	b.WriteString("see [`PORTING.md`](PORTING.md).\n\n")
	b.WriteString("Gaps are ordered by how many upstream scripts they hold up, worst first, because\n")
	b.WriteString("that is the order in which fixing them buys the most coverage. A gap that holds up\n")
	b.WriteString("nothing is not therefore unimportant - it is a defect that happens to cost no\n")
	b.WriteString("coverage, recorded because porting found it.\n\n")
	b.WriteString("**`kind` decides who acts:**\n\n")

	for _, kind := range []string{"defect", "test-config", "product-decision", "unknown"} {
		fmt.Fprintf(&b, "- **`%s`** - %s\n", kind, gapKindMeaning[kind])
	}

	b.WriteString("\n## Summary\n\n")
	b.WriteString("| Gap | Kind | Status | Holds up |\n")
	b.WriteString("|-----|------|--------|----------|\n")

	for _, g := range gaps {
		fmt.Fprintf(&b, "| [`%s`](#%s) | %s | %s | %s |\n",
			g.ID, g.ID, g.Kind, g.Status, holdsUp(g, reg.Entries))
	}

	fmt.Fprintf(&b, "\n%s\n", countsSentence(gaps))

	for _, g := range gaps {
		fmt.Fprintf(&b, "\n## %s\n\n", g.ID)
		fmt.Fprintf(&b, "**%s**\n\n", g.Title)
		fmt.Fprintf(&b, "- **Kind:** `%s` - %s\n", g.Kind, gapKindMeaning[g.Kind])
		fmt.Fprintf(&b, "- **Status:** `%s`\n", g.Status)
		fmt.Fprintf(&b, "- **Holds up:** %s\n", holdsUp(g, reg.Entries))

		if blocked := blockedList(g); blocked != "" {
			fmt.Fprintf(&b, "- **Blocks:** %s\n", blocked)
		}

		if g.FoundWhilePorting != "" {
			fmt.Fprintf(&b, "- **Found while porting:** `%s`\n", g.FoundWhilePorting)
		}

		fmt.Fprintf(&b, "\n### Impact\n\n%s\n", unwrap(g.Impact))
		fmt.Fprintf(&b, "\n### Plan\n\n%s\n", unwrap(g.Plan))
	}

	return b.String()
}

// holdsUp phrases the blocked count for a reader rather than a counter, so that
// "0" reads as a deliberate statement instead of a missing value.
func holdsUp(g Gap, entries []Entry) string {
	n := gapBlockedCount(g, entries)

	switch {
	case n == 0 && g.FoundWhilePorting != "":
		return fmt.Sprintf("no tests - found while porting `%s`", g.FoundWhilePorting)
	case n == 0:
		return "no tests"
	case n == 1:
		return "1 upstream script"
	default:
		return fmt.Sprintf("%d upstream scripts", n)
	}
}

// blockedList names the scripts, or the hook clusters where a gap is too broad to
// enumerate script by script.
func blockedList(g Gap) string {
	parts := make([]string, 0, len(g.Blocks)+len(g.BlocksHooks))

	for _, name := range g.Blocks {
		parts = append(parts, "`"+name+"`")
	}

	for _, hook := range g.BlocksHooks {
		parts = append(parts, "every script needing `"+hook+"`")
	}

	return strings.Join(parts, ", ")
}

// countsSentence summarises the register by kind, so the reader learns how much of
// it is actionable here before reading any of it.
func countsSentence(gaps []Gap) string {
	byKind := map[string]int{}
	for _, g := range gaps {
		byKind[g.Kind]++
	}

	kinds := make([]string, 0, len(byKind))
	for kind := range byKind {
		kinds = append(kinds, kind)
	}

	slices.Sort(kinds)

	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, fmt.Sprintf("%d `%s`", byKind[kind], kind))
	}

	return fmt.Sprintf("%d open gaps: %s.", len(gaps), strings.Join(parts, ", "))
}
