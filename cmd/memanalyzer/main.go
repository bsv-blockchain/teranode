// Package main provides a CLI tool for analyzing Teranode process memory.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

type memoryRegion struct {
	addressRange string
	permissions  string
	name         string
	size         int64
	rss          int64
	pss          int64
	privateDirty int64
	privateClean int64
	sharedDirty  int64
	sharedClean  int64
}

func main() {
	pid := flag.Int("pid", 0, "Process ID to analyze (0 for self)")
	topN := flag.Int("top", 20, "Number of top regions to show")
	showAll := flag.Bool("all", false, "Show all regions")
	flag.Parse()

	smapsPath := "/proc/self/smaps"
	if *pid > 0 {
		smapsPath = fmt.Sprintf("/proc/%d/smaps", *pid)
	}

	regions, err := parseSmaps(smapsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing smaps: %v\n", err)
		os.Exit(1)
	}

	// Calculate breakdown
	breakdown := categorizeRegions(regions)

	// Print breakdown
	printBreakdown(breakdown)

	// Print top regions
	if *showAll {
		printAllRegions(regions)
	} else {
		printTopRegions(regions, *topN)
	}
}

type breakdown struct {
	goHeap        int64
	goStack       int64
	goMetadata    int64
	namedRegions  map[string]int64
	fileBacked    map[string]int64
	anonReadWrite int64
	anonReadOnly  int64
	anonExecute   int64
	other         int64
	totalVirtual  int64
	totalRss      int64
	totalPss      int64
}

func parseSmaps(path string) ([]memoryRegion, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var regions []memoryRegion
	var current *memoryRegion

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// New region
		if strings.Contains(line, "-") && (strings.Contains(line, "rw") || strings.Contains(line, "r-") || strings.Contains(line, "r-x")) {
			if current != nil {
				regions = append(regions, *current)
			}

			parts := strings.Fields(line)
			current = &memoryRegion{
				addressRange: parts[0],
				permissions:  parts[1],
			}

			if len(parts) > 5 {
				current.name = strings.Join(parts[5:], " ")
			} else {
				current.name = "[anonymous]"
			}
			continue
		}

		if current == nil {
			continue
		}

		var value int64
		if strings.HasPrefix(line, "Size:") {
			_, _ = fmt.Sscanf(line, "Size: %d kB", &value)
			current.size = value * 1024
		} else if strings.HasPrefix(line, "Rss:") {
			_, _ = fmt.Sscanf(line, "Rss: %d kB", &value)
			current.rss = value * 1024
		} else if strings.HasPrefix(line, "Pss:") {
			_, _ = fmt.Sscanf(line, "Pss: %d kB", &value)
			current.pss = value * 1024
		} else if strings.HasPrefix(line, "Private_Dirty:") {
			_, _ = fmt.Sscanf(line, "Private_Dirty: %d kB", &value)
			current.privateDirty = value * 1024
		} else if strings.HasPrefix(line, "Private_Clean:") {
			_, _ = fmt.Sscanf(line, "Private_Clean: %d kB", &value)
			current.privateClean = value * 1024
		} else if strings.HasPrefix(line, "Shared_Dirty:") {
			_, _ = fmt.Sscanf(line, "Shared_Dirty: %d kB", &value)
			current.sharedDirty = value * 1024
		} else if strings.HasPrefix(line, "Shared_Clean:") {
			_, _ = fmt.Sscanf(line, "Shared_Clean: %d kB", &value)
			current.sharedClean = value * 1024
		}
	}

	if current != nil {
		regions = append(regions, *current)
	}

	return regions, scanner.Err()
}

func categorizeRegions(regions []memoryRegion) *breakdown {
	b := &breakdown{
		namedRegions: make(map[string]int64),
		fileBacked:   make(map[string]int64),
	}

	for _, region := range regions {
		b.totalVirtual += region.size
		b.totalRss += region.rss
		b.totalPss += region.pss

		// Named anonymous regions
		if strings.HasPrefix(region.name, "[anon:") {
			name := strings.TrimPrefix(region.name, "[anon:")
			name = strings.TrimSuffix(name, "]")

			if strings.Contains(name, "Go:") {
				if strings.Contains(name, "metadata") {
					b.goMetadata += region.rss
				} else if strings.Contains(name, "stack") {
					b.goStack += region.rss
				} else {
					b.goHeap += region.rss
				}
			} else {
				b.namedRegions[name] += region.rss
			}
			continue
		}

		// File-backed
		if strings.HasPrefix(region.name, "/") {
			if strings.Contains(region.name, ".so") || strings.Contains(region.name, "lib") {
				b.fileBacked["Shared Libraries"] += region.rss
			} else if strings.Contains(region.name, "teranode") {
				b.fileBacked["Teranode Binary"] += region.rss
			} else {
				b.fileBacked["Other Files"] += region.rss
			}
			continue
		}

		// Stack
		if strings.HasPrefix(region.name, "[stack") {
			b.goStack += region.rss
			continue
		}

		// Heap
		if region.name == "[heap]" {
			b.goHeap += region.rss
			continue
		}

		// Anonymous
		if region.name == "[anonymous]" || region.name == "" {
			if strings.HasPrefix(region.permissions, "rw") {
				b.anonReadWrite += region.rss
			} else if strings.HasPrefix(region.permissions, "r--") {
				b.anonReadOnly += region.rss
			} else if strings.HasPrefix(region.permissions, "r-x") {
				b.anonExecute += region.rss
			}
			continue
		}

		b.other += region.rss
	}

	return b
}

func printBreakdown(b *breakdown) {
	fmt.Println("=== Complete Memory Breakdown ===")

	fmt.Println("Go Runtime:")
	fmt.Printf("  Heap:         %10d MB\n", b.goHeap/1024/1024)
	fmt.Printf("  Stack:        %10d MB\n", b.goStack/1024/1024)
	fmt.Printf("  Metadata:     %10d MB\n", b.goMetadata/1024/1024)
	fmt.Printf("  Subtotal:     %10d MB\n\n", (b.goHeap+b.goStack+b.goMetadata)/1024/1024)

	if len(b.namedRegions) > 0 {
		fmt.Println("Named Memory Regions (mmap):")

		type kv struct {
			key   string
			value int64
		}
		var sorted []kv
		for k, v := range b.namedRegions {
			sorted = append(sorted, kv{k, v})
		}
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].value > sorted[j].value
		})

		total := int64(0)
		for _, item := range sorted {
			fmt.Printf("  %-40s %10d MB\n", item.key, item.value/1024/1024)
			total += item.value
		}
		fmt.Printf("  Subtotal:     %10d MB\n\n", total/1024/1024)
	}

	if len(b.fileBacked) > 0 {
		fmt.Println("File-Backed Memory:")
		total := int64(0)
		for name, size := range b.fileBacked {
			fmt.Printf("  %-40s %10d MB\n", name, size/1024/1024)
			total += size
		}
		fmt.Printf("  Subtotal:     %10d MB\n\n", total/1024/1024)
	}

	fmt.Println("Anonymous Memory:")
	fmt.Printf("  Read-Write:   %10d MB\n", b.anonReadWrite/1024/1024)
	fmt.Printf("  Read-Only:    %10d MB\n", b.anonReadOnly/1024/1024)
	fmt.Printf("  Executable:   %10d MB\n", b.anonExecute/1024/1024)
	fmt.Printf("  Subtotal:     %10d MB\n\n", (b.anonReadWrite+b.anonReadOnly+b.anonExecute)/1024/1024)

	if b.other > 0 {
		fmt.Printf("Other:          %10d MB\n\n", b.other/1024/1024)
	}

	fmt.Println("=== Totals ===")
	fmt.Printf("Virtual Memory: %10d MB\n", b.totalVirtual/1024/1024)
	fmt.Printf("RSS (Resident): %10d MB\n", b.totalRss/1024/1024)
	fmt.Printf("PSS (Proportional): %10d MB\n", b.totalPss/1024/1024)
}

func printTopRegions(regions []memoryRegion, n int) {
	sort.Slice(regions, func(i, j int) bool {
		return regions[i].rss > regions[j].rss
	})

	if n > len(regions) {
		n = len(regions)
	}

	fmt.Printf("\n=== Top %d Memory Regions ===\n\n", n)
	fmt.Printf("%-18s %-6s %-10s %-10s %-10s %s\n",
		"ADDRESS", "PERMS", "RSS", "PRIVATE", "SHARED", "NAME")
	fmt.Println(strings.Repeat("-", 120))

	for i := 0; i < n && i < len(regions); i++ {
		region := regions[i]
		if region.rss == 0 {
			continue
		}

		privateMB := (region.privateClean + region.privateDirty) / 1024 / 1024
		sharedMB := (region.sharedClean + region.sharedDirty) / 1024 / 1024
		rssMB := region.rss / 1024 / 1024

		name := region.name
		if len(name) > 60 {
			name = name[:57] + "..."
		}

		addrShort := region.addressRange
		if len(addrShort) > 18 {
			addrShort = addrShort[:12] + "..."
		}

		fmt.Printf("%-18s %-6s %8d MB %8d MB %8d MB %s\n",
			addrShort,
			region.permissions,
			rssMB,
			privateMB,
			sharedMB,
			name)
	}
}

func printAllRegions(regions []memoryRegion) {
	sort.Slice(regions, func(i, j int) bool {
		return regions[i].rss > regions[j].rss
	})

	fmt.Println("\n=== All Memory Regions ===")
	fmt.Printf("%-18s %-6s %-10s %-10s %-10s %s\n",
		"ADDRESS", "PERMS", "RSS", "PRIVATE", "SHARED", "NAME")
	fmt.Println(strings.Repeat("-", 120))

	for _, region := range regions {
		if region.rss == 0 {
			continue
		}

		privateMB := (region.privateClean + region.privateDirty) / 1024 / 1024
		sharedMB := (region.sharedClean + region.sharedDirty) / 1024 / 1024
		rssMB := region.rss / 1024 / 1024

		name := region.name
		if len(name) > 60 {
			name = name[:57] + "..."
		}

		addrShort := region.addressRange
		if len(addrShort) > 18 {
			addrShort = addrShort[:12] + "..."
		}

		fmt.Printf("%-18s %-6s %8d MB %8d MB %8d MB %s\n",
			addrShort,
			region.permissions,
			rssMB,
			privateMB,
			sharedMB,
			name)
	}
}
