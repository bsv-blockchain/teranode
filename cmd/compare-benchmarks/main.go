package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type comparison struct {
	Name          string
	BaselineValue string
	CurrentValue  string
	PercentChange float64
	PValue        float64
	HasPValue     bool
	Degraded      bool
	Improved      bool
	Metric        string // "sec/op", "B/op", "allocs/op"
}

func main() {
	var (
		baselineFile = flag.String("baseline", "", "Baseline benchmark output file (required)")
		currentFile  = flag.String("current", "", "Current benchmark output file (required)")
		outputFile   = flag.String("output", "benchmark-report.md", "Output markdown file")
		threshold    = flag.Float64("threshold", 10.0, "Degradation threshold percentage (only used when p-value unavailable)")
		alpha        = flag.Float64("alpha", 0.05, "P-value significance level (default 0.05)")
		baselineSHA  = flag.String("baseline-sha", "", "Baseline commit SHA")
		currentSHA   = flag.String("current-sha", "", "Current commit SHA")
		baselineRef  = flag.String("baseline-ref", "main", "Baseline branch/ref name")
		currentRef   = flag.String("current-ref", "PR", "Current branch/ref name")
	)

	flag.Parse()

	if *baselineFile == "" || *currentFile == "" {
		fmt.Println("Usage: compare-benchmarks -baseline <file> -current <file> [-output <file>]")
		os.Exit(1)
	}

	// Run benchstat
	benchstatOut, err := runBenchstat(*baselineFile, *currentFile)
	if err != nil {
		log.Fatalf("Failed to run benchstat: %v", err)
	}

	// Parse benchstat output
	comparisons := parseBenchstat(benchstatOut, *threshold, *alpha)

	fmt.Printf("Parsed %d comparisons from benchstat\n", len(comparisons))

	// Generate report
	report := generateReport(comparisons, *threshold, *alpha, *baselineRef, *baselineSHA, *currentRef, *currentSHA)

	if err := os.WriteFile(*outputFile, []byte(report), 0o600); err != nil {
		log.Fatalf("Failed to write report: %v", err)
	}

	fmt.Printf("Report written to: %s\n", *outputFile)
	fmt.Println("\n=== Summary ===")

	hasRegressions := false
	for _, c := range comparisons {
		if c.Degraded {
			hasRegressions = true
			fmt.Printf("  REGRESSION: %s %s -> %s (%+.1f%%", c.Name, c.BaselineValue, c.CurrentValue, c.PercentChange)
			if c.HasPValue {
				fmt.Printf(", p=%.3f", c.PValue)
			}
			fmt.Println(")")
		}
	}

	if !hasRegressions {
		fmt.Println("  No statistically significant regressions detected")
	}

	if hasRegressions {
		os.Exit(1)
	}
}

func runBenchstat(baselineFile, currentFile string) (string, error) {
	cmd := exec.Command("benchstat", baselineFile, currentFile)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// benchstat exits non-zero sometimes but still produces output
		if len(out) > 0 {
			return string(out), nil
		}
		return "", fmt.Errorf("benchstat failed: %w\n%s", err, string(out))
	}
	return string(out), nil
}

// parseBenchstat parses benchstat output format:
//
//	                          │  baseline   │               current               │
//	                          │   sec/op    │   sec/op     vs base                │
//	ErrorAs/shallow_chain-4     2.159n ± 0%   2.130n ± 1%  -1.34% (p=0.043 n=3)
//	ErrorAs/deep_chain-4        2.115n ± 0%   2.200n ± 2%       ~ (p=0.200 n=3)
func parseBenchstat(output string, threshold, alpha float64) []comparison {
	var comparisons []comparison

	// Match lines with benchmark results
	// Format: Name  value ± x%  value ± x%  change (p=x.xxx n=N)  or  ~ (p=x.xxx n=N)
	resultRe := regexp.MustCompile(
		`^(\S+)\s+` + // benchmark name
			`(\S+)\s+±\s+\S+\s+` + // baseline value ± variance
			`(\S+)\s+±\s+\S+\s+` + // current value ± variance
			`(.+)$`, // change + p-value section
	)

	// Match percentage change with p-value: +1.34% (p=0.043 n=3) or -2.50% (p=0.001 n=3)
	changeWithPRe := regexp.MustCompile(`([+-]?\d+\.?\d*)%\s+\(p=(\d+\.?\d*)\s+n=\d+\)`)
	// Match "~" (not significant): ~ (p=0.200 n=3)
	noChangeRe := regexp.MustCompile(`~\s+\(p=(\d+\.?\d*)\s+n=\d+\)`)

	scanner := bufio.NewScanner(strings.NewReader(output))
	currentMetric := "sec/op"

	for scanner.Scan() {
		line := scanner.Text()

		// Detect metric headers (sec/op, B/op, allocs/op)
		if strings.Contains(line, "sec/op") && strings.Contains(line, "vs base") {
			currentMetric = "sec/op"
			continue
		}
		if strings.Contains(line, "B/op") && strings.Contains(line, "vs base") {
			currentMetric = "B/op"
			continue
		}
		if strings.Contains(line, "allocs/op") && strings.Contains(line, "vs base") {
			currentMetric = "allocs/op"
			continue
		}

		// Skip non-data lines
		matches := resultRe.FindStringSubmatch(line)
		if matches == nil {
			continue
		}

		name := matches[1]
		baseVal := matches[2]
		currVal := matches[3]
		changePart := matches[4]

		comp := comparison{
			Name:          name,
			BaselineValue: baseVal,
			CurrentValue:  currVal,
			Metric:        currentMetric,
		}

		// Parse change
		if m := changeWithPRe.FindStringSubmatch(changePart); m != nil {
			comp.PercentChange, _ = strconv.ParseFloat(m[1], 64)
			comp.PValue, _ = strconv.ParseFloat(m[2], 64)
			comp.HasPValue = true

			// Only flag as regression if statistically significant AND above threshold
			if currentMetric == "sec/op" {
				comp.Degraded = comp.PValue < alpha && comp.PercentChange > threshold
				comp.Improved = comp.PValue < alpha && comp.PercentChange < -threshold
			}
		} else if m := noChangeRe.FindStringSubmatch(changePart); m != nil {
			comp.PValue, _ = strconv.ParseFloat(m[1], 64)
			comp.HasPValue = true
			comp.PercentChange = 0
		} else {
			// No p-value available (e.g., count=1), fall back to threshold only
			pctRe := regexp.MustCompile(`([+-]?\d+\.?\d*)%`)
			if m := pctRe.FindStringSubmatch(changePart); m != nil {
				comp.PercentChange, _ = strconv.ParseFloat(m[1], 64)
				if currentMetric == "sec/op" {
					comp.Degraded = math.Abs(comp.PercentChange) > threshold && comp.PercentChange > 0
					comp.Improved = math.Abs(comp.PercentChange) > threshold && comp.PercentChange < 0
				}
			}
		}

		// Only track sec/op for regression detection (allocs/B are informational)
		comparisons = append(comparisons, comp)
	}

	// Sort: regressions first (worst first), then improvements, then unchanged
	sort.Slice(comparisons, func(i, j int) bool {
		if comparisons[i].Degraded != comparisons[j].Degraded {
			return comparisons[i].Degraded
		}
		if comparisons[i].Improved != comparisons[j].Improved {
			return comparisons[i].Improved
		}
		return comparisons[i].PercentChange > comparisons[j].PercentChange
	})

	return comparisons
}

func generateReport(comparisons []comparison, threshold, alpha float64, baselineRef, baselineSHA, currentRef, currentSHA string) string {
	var sb strings.Builder

	sb.WriteString("## Benchmark Comparison Report\n\n")

	if baselineSHA != "" {
		shortSHA := baselineSHA
		if len(shortSHA) > 8 {
			shortSHA = shortSHA[:8]
		}
		sb.WriteString(fmt.Sprintf("**Baseline:** `%s` (%s)\n\n", baselineRef, shortSHA))
	}
	if currentSHA != "" {
		shortSHA := currentSHA
		if len(shortSHA) > 8 {
			shortSHA = shortSHA[:8]
		}
		sb.WriteString(fmt.Sprintf("**Current:** `%s` (%s)\n\n", currentRef, shortSHA))
	}

	// Filter to sec/op only for summary
	var secComps []comparison
	for _, c := range comparisons {
		if c.Metric == "sec/op" {
			secComps = append(secComps, c)
		}
	}

	regressions, improvements, unchanged := 0, 0, 0
	for _, c := range secComps {
		if c.Degraded {
			regressions++
		} else if c.Improved {
			improvements++
		} else {
			unchanged++
		}
	}

	sb.WriteString("### Summary\n\n")
	sb.WriteString(fmt.Sprintf("- **Regressions:** %d\n", regressions))
	sb.WriteString(fmt.Sprintf("- **Improvements:** %d\n", improvements))
	sb.WriteString(fmt.Sprintf("- **Unchanged:** %d\n", unchanged))
	sb.WriteString(fmt.Sprintf("- **Significance level:** p < %.2f\n\n", alpha))

	if regressions > 0 {
		sb.WriteString("### Regressions\n\n")
		sb.WriteString("| Benchmark | Baseline | Current | Change | p-value | Status |\n")
		sb.WriteString("|-----------|----------|---------|--------|---------|--------|\n")
		for _, c := range secComps {
			if !c.Degraded {
				continue
			}
			pStr := "n/a"
			if c.HasPValue {
				pStr = fmt.Sprintf("%.3f", c.PValue)
			}
			sb.WriteString(fmt.Sprintf("| %s | %s | %s | %+.1f%% | %s | REGRESSED |\n",
				formatName(c.Name), c.BaselineValue, c.CurrentValue, c.PercentChange, pStr))
		}
		sb.WriteString("\n")
	}

	if improvements > 0 {
		sb.WriteString("### Improvements\n\n")
		sb.WriteString("| Benchmark | Baseline | Current | Change | p-value |\n")
		sb.WriteString("|-----------|----------|---------|--------|--------|\n")
		for _, c := range secComps {
			if !c.Improved {
				continue
			}
			pStr := "n/a"
			if c.HasPValue {
				pStr = fmt.Sprintf("%.3f", c.PValue)
			}
			sb.WriteString(fmt.Sprintf("| %s | %s | %s | %+.1f%% | %s |\n",
				formatName(c.Name), c.BaselineValue, c.CurrentValue, c.PercentChange, pStr))
		}
		sb.WriteString("\n")
	}

	// All results (sec/op only, collapsed)
	sb.WriteString("<details>\n<summary>All benchmark results (sec/op)</summary>\n\n")
	sb.WriteString("| Benchmark | Baseline | Current | Change | p-value |\n")
	sb.WriteString("|-----------|----------|---------|--------|--------|\n")
	for _, c := range secComps {
		pStr := "n/a"
		if c.HasPValue {
			pStr = fmt.Sprintf("%.3f", c.PValue)
		}
		changeStr := "~"
		if c.PercentChange != 0 {
			changeStr = fmt.Sprintf("%+.1f%%", c.PercentChange)
		}
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s |\n",
			formatName(c.Name), c.BaselineValue, c.CurrentValue, changeStr, pStr))
	}
	sb.WriteString("\n</details>\n\n")

	sb.WriteString("---\n")
	sb.WriteString(fmt.Sprintf("*Threshold: >%.0f%% with p < %.2f | Generated: %s*\n",
		threshold, alpha, time.Now().UTC().Format("2006-01-02 15:04 UTC")))

	return sb.String()
}

func formatName(name string) string {
	name = strings.TrimPrefix(name, "Benchmark")
	if len(name) > 60 {
		return name[:57] + "..."
	}
	return name
}
