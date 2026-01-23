package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

type benchmarkResult struct {
	Name        string `json:"name"`
	NsPerOp     int64  `json:"ns_per_op"`
	BytesPerOp  int64  `json:"bytes_per_op"`
	AllocsPerOp int64  `json:"allocs_per_op"`
	Iterations  int64  `json:"iterations"`
}

type benchmarkRun struct {
	Benchmarks []benchmarkResult `json:"benchmarks"`
	Git        map[string]string `json:"git"`
	Timestamp  string            `json:"timestamp"`
	Version    string            `json:"version"`
}

type comparison struct {
	Name                string
	BaselineNsPerOp     int64
	CurrentNsPerOp      int64
	PercentChange       float64
	BaselineAllocsPerOp int64
	CurrentAllocsPerOp  int64
	AllocsChange        float64
	Degraded            bool
	Improved            bool
}

func main() {
	var (
		currentFile  = flag.String("current", "", "Current benchmark JSON file (required)")
		baselineFile = flag.String("baseline", "", "Baseline benchmark JSON file (required)")
		outputFile   = flag.String("output", "comparison-report.md", "Output markdown file")
		threshold    = flag.Float64("threshold", 5.0, "Degradation threshold percentage")
	)

	flag.Parse()

	// Validate required flags
	if *currentFile == "" || *baselineFile == "" {
		fmt.Println("Usage: compare-benchmarks -current <file> -baseline <file> [-output <file>] [-threshold <percent>]")
		os.Exit(1)
	}

	// Load benchmark runs
	baseline, err := loadBenchmarkRun(*baselineFile)
	if err != nil {
		log.Fatalf("Failed to load baseline: %v", err)
	}

	current, err := loadBenchmarkRun(*currentFile)
	if err != nil {
		log.Fatalf("Failed to load current: %v", err)
	}

	fmt.Printf("Baseline: %d benchmarks (branch: %s)\n", len(baseline.Benchmarks), baseline.Git["branch"])
	fmt.Printf("Current:  %d benchmarks (branch: %s)\n", len(current.Benchmarks), current.Git["branch"])

	// Compare
	comparisons := compare(baseline, current, *threshold)

	// Generate report
	report := generateReport(baseline, current, comparisons, *threshold)

	// Write report
	if err := os.WriteFile(*outputFile, []byte(report), 0o600); err != nil {
		log.Fatalf("Failed to write report: %v", err)
	}

	fmt.Printf("Report written to: %s\n", *outputFile)
	fmt.Println("\n=== Summary ===")
	printSummary(comparisons)

	// Exit with error if regressions found
	hasRegressions := false
	for _, c := range comparisons {
		if c.Degraded {
			hasRegressions = true
			break
		}
	}

	if hasRegressions {
		os.Exit(1)
	}
}

// loadBenchmarkRun loads a benchmark run from JSON file
func loadBenchmarkRun(filename string) (*benchmarkRun, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var run benchmarkRun
	if err := json.Unmarshal(data, &run); err != nil {
		return nil, err
	}

	return &run, nil
}

// compare generates comparisons between baseline and current benchmarks
func compare(baseline, current *benchmarkRun, threshold float64) []comparison {
	baselineMap := make(map[string]benchmarkResult)
	for _, b := range baseline.Benchmarks {
		baselineMap[b.Name] = b
	}

	comparisons := make([]comparison, 0, len(current.Benchmarks))
	for _, curr := range current.Benchmarks {
		base, exists := baselineMap[curr.Name]
		if !exists {
			// New benchmark
			comparisons = append(comparisons, comparison{
				Name:           curr.Name,
				CurrentNsPerOp: curr.NsPerOp,
				Degraded:       false,
			})
			continue
		}

		// Calculate percent change
		percentChange := 0.0
		if base.NsPerOp > 0 {
			percentChange = float64(curr.NsPerOp-base.NsPerOp) / float64(base.NsPerOp) * 100
		}

		allocsChange := 0.0
		if base.AllocsPerOp > 0 {
			allocsChange = float64(curr.AllocsPerOp-base.AllocsPerOp) / float64(base.AllocsPerOp) * 100
		}

		degraded := percentChange > threshold
		improved := percentChange < -threshold

		comparisons = append(comparisons, comparison{
			Name:                curr.Name,
			BaselineNsPerOp:     base.NsPerOp,
			CurrentNsPerOp:      curr.NsPerOp,
			PercentChange:       percentChange,
			BaselineAllocsPerOp: base.AllocsPerOp,
			CurrentAllocsPerOp:  curr.AllocsPerOp,
			AllocsChange:        allocsChange,
			Degraded:            degraded,
			Improved:            improved,
		})
	}

	// Sort by percent change (worst first)
	sort.Slice(comparisons, func(i, j int) bool {
		return comparisons[i].PercentChange > comparisons[j].PercentChange
	})

	return comparisons
}

// generateReport creates a markdown report
func generateReport(baseline, current *benchmarkRun, comparisons []comparison, threshold float64) string {
	var sb strings.Builder

	// Header
	sb.WriteString("## 📊 Benchmark Comparison Report\n\n")

	// Branch info
	baselineBranch := baseline.Git["branch"]
	currentBranch := current.Git["branch"]
	baselineCommit := baseline.Git["commit"]
	if len(baselineCommit) > 8 {
		baselineCommit = baselineCommit[:8]
	}
	currentCommit := current.Git["commit"]
	if len(currentCommit) > 8 {
		currentCommit = currentCommit[:8]
	}

	sb.WriteString(fmt.Sprintf("**Baseline:** `%s` (%s)\n\n", baselineBranch, baselineCommit))
	sb.WriteString(fmt.Sprintf("**Current:** `%s` (%s)\n\n", currentBranch, currentCommit))

	// Summary statistics
	regressions := 0
	improvements := 0
	unchanged := 0

	for _, c := range comparisons {
		if c.Degraded {
			regressions++
		} else if c.Improved {
			improvements++
		} else {
			unchanged++
		}
	}

	sb.WriteString("### Summary\n\n")
	sb.WriteString(fmt.Sprintf("- **Regressions (>%.1f%%):** %d ❌\n", threshold, regressions))
	sb.WriteString(fmt.Sprintf("- **Improvements (>%.1f%%):** %d ✅\n", threshold, improvements))
	sb.WriteString(fmt.Sprintf("- **Unchanged:** %d ✓\n\n", unchanged))

	if regressions > 0 {
		sb.WriteString("### ⚠️ REGRESSION DETECTED\n\n")
		sb.WriteString(fmt.Sprintf("**%d benchmark(s) degraded by more than %.1f%%**\n\n", regressions, threshold))
	}

	// Detailed results table
	sb.WriteString("### Detailed Results\n\n")
	sb.WriteString("| Benchmark | Baseline | Current | Change | Allocs | Status |\n")
	sb.WriteString("|-----------|----------|---------|--------|--------|--------|\n")

	for _, c := range comparisons {
		status := "✓"
		if c.Degraded {
			status = "❌ REGRESSED"
		} else if c.Improved {
			status = "✅ IMPROVED"
		}

		name := formatBenchmarkName(c.Name)

		if c.BaselineNsPerOp == 0 {
			// New benchmark
			sb.WriteString(fmt.Sprintf("| %s | NEW | %d ns/op | - | %d | %s |\n",
				name, c.CurrentNsPerOp, c.CurrentAllocsPerOp, status))
		} else {
			changeStr := fmt.Sprintf("%+.1f%%", c.PercentChange)
			allocsStr := fmt.Sprintf("%+.1f%%", c.AllocsChange)

			sb.WriteString(fmt.Sprintf("| %s | %d ns/op | %d ns/op | %s | %s | %s |\n",
				name, c.BaselineNsPerOp, c.CurrentNsPerOp, changeStr, allocsStr, status))
		}
	}

	sb.WriteString("\n")

	// Detailed regressions section
	if regressions > 0 {
		sb.WriteString("### ❌ Regressions\n\n")
		for _, c := range comparisons {
			if !c.Degraded {
				continue
			}
			name := formatBenchmarkName(c.Name)
			sb.WriteString(fmt.Sprintf("- **%s**\n", name))
			sb.WriteString(fmt.Sprintf("  - Baseline: %d ns/op\n", c.BaselineNsPerOp))
			sb.WriteString(fmt.Sprintf("  - Current: %d ns/op\n", c.CurrentNsPerOp))
			sb.WriteString(fmt.Sprintf("  - Change: **%+.1f%%**\n\n", c.PercentChange))
		}
	}

	// Detailed improvements section
	if improvements > 0 {
		sb.WriteString("### ✅ Improvements\n\n")
		for _, c := range comparisons {
			if !c.Improved {
				continue
			}
			name := formatBenchmarkName(c.Name)
			sb.WriteString(fmt.Sprintf("- **%s**\n", name))
			sb.WriteString(fmt.Sprintf("  - Baseline: %d ns/op\n", c.BaselineNsPerOp))
			sb.WriteString(fmt.Sprintf("  - Current: %d ns/op\n", c.CurrentNsPerOp))
			sb.WriteString(fmt.Sprintf("  - Change: **%+.1f%%** 🎉\n\n", c.PercentChange))
		}
	}

	// Footer
	sb.WriteString("\n---\n")
	sb.WriteString(fmt.Sprintf("*Threshold: %.1f%% | Generated at %s*\n", threshold, baseline.Timestamp))

	return sb.String()
}

// formatBenchmarkName shortens benchmark names for display
func formatBenchmarkName(name string) string {
	// Remove "Benchmark" prefix for cleaner display
	name = strings.TrimPrefix(name, "Benchmark")

	if len(name) > 60 {
		return name[:57] + "..."
	}
	return name
}

// printSummary prints a summary to stdout
func printSummary(comparisons []comparison) {
	regressions := 0
	improvements := 0

	for _, c := range comparisons {
		if c.Degraded {
			regressions++
			fmt.Printf("❌ %s: %+.1f%%\n", c.Name, c.PercentChange)
		} else if c.Improved {
			improvements++
		}
	}

	fmt.Printf("\nTotal: %d improvements, %d regressions\n", improvements, regressions)
}
