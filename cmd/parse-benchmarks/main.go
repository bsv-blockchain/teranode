package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
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

type gitInfo struct {
	Commit string `json:"commit"`
	Branch string `json:"branch"`
	PR     string `json:"pr,omitempty"`
}

func main() {
	var (
		inputFile  = flag.String("input", "", "Input file with benchmark output (required)")
		outputFile = flag.String("output", "", "Output JSON file (required)")
		commit     = flag.String("commit", "", "Git commit hash")
		branch     = flag.String("branch", "", "Git branch name")
		pr         = flag.String("pr", "", "PR number (optional)")
	)

	flag.Parse()

	// Validate required flags
	if *inputFile == "" || *outputFile == "" {
		fmt.Println("Usage: parse-benchmarks -input <file> -output <file> -commit <hash> -branch <name> [-pr <number>]")
		os.Exit(1)
	}

	// Read input file
	content, err := os.ReadFile(*inputFile)
	if err != nil {
		log.Fatalf("Failed to read input file: %v", err)
	}

	// Parse benchmarks
	benchmarks := parseBenchmarks(string(content))
	if len(benchmarks) == 0 {
		log.Fatalf("No benchmarks found in output")
	}

	fmt.Printf("Parsed %d benchmarks\n", len(benchmarks))

	// Create benchmark run
	run := &benchmarkRun{
		Version:   "1.0",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Git: map[string]string{
			"commit": *commit,
			"branch": *branch,
			"pr":     *pr,
		},
		Benchmarks: benchmarks,
	}

	// Write output
	data, err := json.MarshalIndent(run, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	if err := os.WriteFile(*outputFile, data, 0o600); err != nil {
		log.Fatalf("Failed to write output file: %v", err)
	}

	fmt.Printf("Wrote %d benchmarks to %s\n", len(benchmarks), *outputFile)
}

// parseBenchmarks extracts benchmarks from go test output
func parseBenchmarks(output string) []benchmarkResult {
	results := make([]benchmarkResult, 0, len(strings.Split(output, "\n")))

	// Pattern matches lines like:
	// BenchmarkDemoFastOperation-12        \t  583326\t      3920 ns/op\t       0 B/op\t       0 allocs/op
	// BenchmarkGetSubtree_1M_Binary-8        10   1234567 ns/op    512 B/op    2 allocs/op
	pattern := regexp.MustCompile(
		`^Benchmark(\S+?)\s+\d+\s+(.+)\s+ns/op\s+(.+)\s+B/op\s+(.+)\s+allocs/op$`,
	)

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}

		matches := pattern.FindStringSubmatch(line)
		if matches == nil {
			continue
		}

		// matches[0] = full line
		// matches[1] = name
		// matches[2] = ns/op
		// matches[3] = bytes/op
		// matches[4] = allocs/op

		result := benchmarkResult{
			Name:        "Benchmark" + matches[1],
			NsPerOp:     parseInt64(matches[2]),
			BytesPerOp:  parseInt64(matches[3]),
			AllocsPerOp: parseInt64(matches[4]),
		}

		results = append(results, result)
	}

	return results
}

// parseInt64 safely parses a string to int64
func parseInt64(s string) int64 {
	val, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return val
}
