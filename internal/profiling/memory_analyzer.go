// Package profiling provides memory profiling and analysis tools for Teranode.
package profiling

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
)

// MemoryRegion represents a single memory region from /proc/[pid]/smaps.
type MemoryRegion struct {
	AddressRange string
	Permissions  string
	Name         string
	Size         int64 // in bytes
	Rss          int64 // Resident Set Size
	Pss          int64 // Proportional Set Size
	SharedClean  int64
	SharedDirty  int64
	PrivateClean int64
	PrivateDirty int64
	Referenced   int64
	Anonymous    int64
	Swap         int64
}

// MemoryBreakdown provides a categorized view of all process memory.
type MemoryBreakdown struct {
	// Go-specific memory
	GoHeap     int64
	GoStack    int64
	GoMetadata int64

	// Named regions (like txmetacache mmaps)
	NamedRegions map[string]int64

	// Categorized anonymous memory
	AnonReadWrite int64 // rw-p anonymous
	AnonReadOnly  int64 // r--p anonymous
	AnonExecute   int64 // r-xp anonymous

	// File-backed memory
	FileBacked map[string]int64

	// Network and other
	NetworkBuffers int64
	Other          int64

	// Totals
	TotalVirtual int64
	TotalRss     int64
	TotalPss     int64

	// Detailed regions for analysis
	Regions []MemoryRegion
}

// GetCompleteMemoryProfile analyzes all memory used by the current process.
func GetCompleteMemoryProfile() (*MemoryBreakdown, error) {
	breakdown := &MemoryBreakdown{
		NamedRegions: make(map[string]int64),
		FileBacked:   make(map[string]int64),
		Regions:      make([]MemoryRegion, 0),
	}

	// Get Go runtime stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	breakdown.GoHeap = int64(m.Alloc)
	breakdown.GoStack = int64(m.StackInuse)

	// Parse /proc/self/smaps
	if err := parseSmaps(breakdown); err != nil {
		return nil, err
	}

	return breakdown, nil
}

// parseSmaps reads and parses /proc/self/smaps to categorize memory regions.
func parseSmaps(breakdown *MemoryBreakdown) error {
	f, err := os.Open("/proc/self/smaps")
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var currentRegion *MemoryRegion

	for scanner.Scan() {
		line := scanner.Text()

		// New memory region line
		if strings.Contains(line, "-") && (strings.Contains(line, "rw") || strings.Contains(line, "r-") || strings.Contains(line, "r-x")) {
			if currentRegion != nil {
				categorizeRegion(breakdown, currentRegion)
				breakdown.Regions = append(breakdown.Regions, *currentRegion)
			}

			parts := strings.Fields(line)
			currentRegion = &MemoryRegion{
				AddressRange: parts[0],
				Permissions:  parts[1],
			}

			// Extract region name
			if len(parts) > 5 {
				currentRegion.Name = strings.Join(parts[5:], " ")
			} else {
				currentRegion.Name = "[anonymous]"
			}
			continue
		}

		if currentRegion == nil {
			continue
		}

		// Parse memory statistics for current region
		var value int64
		if strings.HasPrefix(line, "Size:") {
			_, _ = fmt.Sscanf(line, "Size: %d kB", &value)
			currentRegion.Size = value * 1024
			breakdown.TotalVirtual += value * 1024
		} else if strings.HasPrefix(line, "Rss:") {
			_, _ = fmt.Sscanf(line, "Rss: %d kB", &value)
			currentRegion.Rss = value * 1024
			breakdown.TotalRss += value * 1024
		} else if strings.HasPrefix(line, "Pss:") {
			_, _ = fmt.Sscanf(line, "Pss: %d kB", &value)
			currentRegion.Pss = value * 1024
			breakdown.TotalPss += value * 1024
		} else if strings.HasPrefix(line, "Shared_Clean:") {
			_, _ = fmt.Sscanf(line, "Shared_Clean: %d kB", &value)
			currentRegion.SharedClean = value * 1024
		} else if strings.HasPrefix(line, "Shared_Dirty:") {
			_, _ = fmt.Sscanf(line, "Shared_Dirty: %d kB", &value)
			currentRegion.SharedDirty = value * 1024
		} else if strings.HasPrefix(line, "Private_Clean:") {
			_, _ = fmt.Sscanf(line, "Private_Clean: %d kB", &value)
			currentRegion.PrivateClean = value * 1024
		} else if strings.HasPrefix(line, "Private_Dirty:") {
			_, _ = fmt.Sscanf(line, "Private_Dirty: %d kB", &value)
			currentRegion.PrivateDirty = value * 1024
		} else if strings.HasPrefix(line, "Referenced:") {
			_, _ = fmt.Sscanf(line, "Referenced: %d kB", &value)
			currentRegion.Referenced = value * 1024
		} else if strings.HasPrefix(line, "Anonymous:") {
			_, _ = fmt.Sscanf(line, "Anonymous: %d kB", &value)
			currentRegion.Anonymous = value * 1024
		} else if strings.HasPrefix(line, "Swap:") {
			_, _ = fmt.Sscanf(line, "Swap: %d kB", &value)
			currentRegion.Swap = value * 1024
		}
	}

	// Don't forget the last region
	if currentRegion != nil {
		categorizeRegion(breakdown, currentRegion)
		breakdown.Regions = append(breakdown.Regions, *currentRegion)
	}

	return scanner.Err()
}

// categorizeRegion places a memory region into the appropriate category.
func categorizeRegion(breakdown *MemoryBreakdown, region *MemoryRegion) {
	rss := region.Rss

	// Named anonymous regions (like our mmaps)
	if strings.HasPrefix(region.Name, "[anon:") {
		name := strings.TrimPrefix(region.Name, "[anon:")
		name = strings.TrimSuffix(name, "]")

		// Categorize Go runtime regions
		if strings.Contains(name, "Go:") {
			if strings.Contains(name, "metadata") {
				breakdown.GoMetadata += rss
			} else if strings.Contains(name, "stack") {
				breakdown.GoStack += rss
			} else {
				breakdown.GoHeap += rss
			}
		} else {
			// Custom named regions (like txmetacache)
			breakdown.NamedRegions[name] += rss
		}
		return
	}

	// File-backed regions
	if strings.HasPrefix(region.Name, "/") {
		// Categorize by file type
		if strings.Contains(region.Name, ".so") || strings.Contains(region.Name, "lib") {
			breakdown.FileBacked["Shared Libraries"] += rss
		} else if strings.Contains(region.Name, "teranode") {
			breakdown.FileBacked["Teranode Binary"] += rss
		} else {
			breakdown.FileBacked["Other Files"] += rss
		}
		return
	}

	// Stack regions
	if strings.HasPrefix(region.Name, "[stack") {
		breakdown.GoStack += rss
		return
	}

	// Heap regions
	if region.Name == "[heap]" {
		breakdown.GoHeap += rss
		return
	}

	// Anonymous memory by permission
	if region.Name == "[anonymous]" || region.Name == "" {
		if strings.HasPrefix(region.Permissions, "rw") {
			breakdown.AnonReadWrite += rss
		} else if strings.HasPrefix(region.Permissions, "r--") {
			breakdown.AnonReadOnly += rss
		} else if strings.HasPrefix(region.Permissions, "r-x") {
			breakdown.AnonExecute += rss
		}
		return
	}

	// Everything else
	breakdown.Other += rss
}

// FormatBreakdown returns a human-readable string representation of the memory breakdown.
func (b *MemoryBreakdown) FormatBreakdown() string {
	var sb strings.Builder

	_, _ = sb.WriteString("=== Complete Memory Breakdown ===\n\n")

	// Go memory
	_, _ = sb.WriteString("Go Runtime:\n")
	_, _ = sb.WriteString(fmt.Sprintf("  Heap:         %10d MB\n", b.GoHeap/1024/1024))
	_, _ = sb.WriteString(fmt.Sprintf("  Stack:        %10d MB\n", b.GoStack/1024/1024))
	_, _ = sb.WriteString(fmt.Sprintf("  Metadata:     %10d MB\n", b.GoMetadata/1024/1024))
	_, _ = sb.WriteString(fmt.Sprintf("  Subtotal:     %10d MB\n\n", (b.GoHeap+b.GoStack+b.GoMetadata)/1024/1024))

	// Named regions (our mmaps)
	if len(b.NamedRegions) > 0 {
		_, _ = sb.WriteString("Named Memory Regions (mmap):\n")

		// Sort by size
		type kv struct {
			Key   string
			Value int64
		}
		var sorted []kv
		for k, v := range b.NamedRegions {
			sorted = append(sorted, kv{k, v})
		}
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Value > sorted[j].Value
		})

		total := int64(0)
		for _, item := range sorted {
			_, _ = sb.WriteString(fmt.Sprintf("  %-40s %10d MB\n", item.Key, item.Value/1024/1024))
			total += item.Value
		}
		_, _ = sb.WriteString(fmt.Sprintf("  Subtotal:     %10d MB\n\n", total/1024/1024))
	}

	// File-backed memory
	if len(b.FileBacked) > 0 {
		_, _ = sb.WriteString("File-Backed Memory:\n")
		total := int64(0)
		for name, size := range b.FileBacked {
			_, _ = sb.WriteString(fmt.Sprintf("  %-40s %10d MB\n", name, size/1024/1024))
			total += size
		}
		_, _ = sb.WriteString(fmt.Sprintf("  Subtotal:     %10d MB\n\n", total/1024/1024))
	}

	// Anonymous memory
	_, _ = sb.WriteString("Anonymous Memory:\n")
	_, _ = sb.WriteString(fmt.Sprintf("  Read-Write:   %10d MB\n", b.AnonReadWrite/1024/1024))
	_, _ = sb.WriteString(fmt.Sprintf("  Read-Only:    %10d MB\n", b.AnonReadOnly/1024/1024))
	_, _ = sb.WriteString(fmt.Sprintf("  Executable:   %10d MB\n", b.AnonExecute/1024/1024))
	_, _ = sb.WriteString(fmt.Sprintf("  Subtotal:     %10d MB\n\n", (b.AnonReadWrite+b.AnonReadOnly+b.AnonExecute)/1024/1024))

	// Other
	if b.Other > 0 {
		_, _ = sb.WriteString(fmt.Sprintf("Other:          %10d MB\n\n", b.Other/1024/1024))
	}

	// Totals
	_, _ = sb.WriteString("=== Totals ===\n")
	_, _ = sb.WriteString(fmt.Sprintf("Virtual Memory: %10d MB\n", b.TotalVirtual/1024/1024))
	_, _ = sb.WriteString(fmt.Sprintf("RSS (Resident): %10d MB\n", b.TotalRss/1024/1024))
	_, _ = sb.WriteString(fmt.Sprintf("PSS (Proportional): %10d MB\n", b.TotalPss/1024/1024))

	return sb.String()
}

// GetTopRegions returns the N largest memory regions by RSS.
func (b *MemoryBreakdown) GetTopRegions(n int) []MemoryRegion {
	// Sort regions by RSS
	sorted := make([]MemoryRegion, len(b.Regions))
	copy(sorted, b.Regions)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Rss > sorted[j].Rss
	})

	if n > len(sorted) {
		n = len(sorted)
	}

	return sorted[:n]
}

// FormatTopRegions returns a formatted string of the top N memory regions.
func (b *MemoryBreakdown) FormatTopRegions(n int) string {
	var sb strings.Builder

	_, _ = sb.WriteString(fmt.Sprintf("\n=== Top %d Memory Regions ===\n\n", n))
	_, _ = sb.WriteString(fmt.Sprintf("%-18s %-6s %-10s %-10s %-10s %s\n",
		"ADDRESS", "PERMS", "RSS", "PRIVATE", "SHARED", "NAME"))
	_, _ = sb.WriteString(strings.Repeat("-", 120) + "\n")

	for _, region := range b.GetTopRegions(n) {
		if region.Rss == 0 {
			continue
		}

		privateMB := (region.PrivateClean + region.PrivateDirty) / 1024 / 1024
		sharedMB := (region.SharedClean + region.SharedDirty) / 1024 / 1024
		rssMB := region.Rss / 1024 / 1024

		// Truncate long names
		name := region.Name
		if len(name) > 60 {
			name = name[:57] + "..."
		}

		_, _ = sb.WriteString(fmt.Sprintf("%-18s %-6s %8d MB %8d MB %8d MB %s\n",
			region.AddressRange[:12]+"...",
			region.Permissions,
			rssMB,
			privateMB,
			sharedMB,
			name))
	}

	return sb.String()
}
