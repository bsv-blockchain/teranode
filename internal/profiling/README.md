# Memory Profiling and Analysis

This package provides comprehensive memory analysis tools for Teranode that go beyond Go's standard heap profiler.

## Problem

Go's heap profiler (`pprof`) only shows memory allocated through Go's memory allocator. It **does not include**:

- Memory-mapped files (mmap)
- CGO allocations
- Shared memory
- OS page cache
- Network buffers
- Go runtime metadata

This can lead to significant discrepancies between reported heap usage and actual RSS (Resident Set Size).

## Solution

This package parses `/proc/[pid]/smaps` to categorize **all** memory used by the process, including:

### Memory Categories

1. **Go Runtime Memory**
   - Heap: Go allocator-managed memory
   - Stack: Goroutine stacks
   - Metadata: Type info, GC structures, etc.

2. **Named Memory Regions** (Custom mmaps)
   - TXMetaCache mmaps (tracked with custom names)
   - Any other named anonymous memory regions

3. **File-Backed Memory**
   - Shared libraries (.so files)
   - Binary executable
   - Other mapped files

4. **Anonymous Memory**
   - Read-Write: Typically heap/stack
   - Read-Only: Typically constants
   - Executable: JIT code or trampolines

5. **Other**
   - Network buffers
   - Kernel structures
   - Everything else

## Usage

### 1. As a Library (HTTP Handler)

Add the handler to your HTTP server:

```go
import "github.com/bsv-blockchain/teranode/internal/profiling"

// In your HTTP server setup
http.HandleFunc("/debug/memory", profiling.MemoryProfileHandler)
```

Then access it:

```bash
# Get complete memory breakdown
curl http://localhost:8090/debug/memory

# Get top 50 regions
curl http://localhost:8090/debug/memory?top=50
```

### 2. As a CLI Tool

Build the memory analyzer:

```bash
go build -o memanalyzer ./cmd/memanalyzer
```

Analyze the current process:

```bash
./memanalyzer
```

Analyze a specific process:

```bash
./memanalyzer -pid 12345
```

Show top 50 regions:

```bash
./memanalyzer -top 50
```

Show all regions:

```bash
./memanalyzer -all
```

### 3. Programmatically

```go
import "github.com/bsv-blockchain/teranode/internal/profiling"

breakdown, err := profiling.GetCompleteMemoryProfile()
if err != nil {
    log.Fatal(err)
}

// Print formatted breakdown
fmt.Println(breakdown.FormatBreakdown())

// Get top 10 regions
fmt.Println(breakdown.FormatTopRegions(10))

// Access specific categories
fmt.Printf("TXMetaCache usage: %d MB\n",
    breakdown.NamedRegions["TXMetaCache"] / 1024 / 1024)
```

## Analysis Findings

After analyzing the Teranode codebase:

### External Memory Sources

1. **TXMetaCache mmap** ✅ Already tracked
   - Uses `unix.Mmap()` for cache storage
   - Located in `stores/txmetacache/`
   - Now properly named and tracked

2. **No CGO** ✅
   - No `import "C"` found
   - No C library allocations

3. **No Shared Memory** ✅
   - No `shm_open` or `shmget` calls found

4. **Pure Go Libraries** ✅
   - IBM/sarama (Kafka): Pure Go
   - Aerospike client: Pure Go
   - gRPC: Pure Go
   - PostgreSQL: Pure Go
   - All allocations go through Go heap

5. **File I/O** ✅
   - Blob store uses regular file I/O
   - Uses OS page cache (not counted in RSS)

### Expected Memory Categories

For a typical Teranode process, you should see:

1. **Go Heap** (30-40%): Business logic, data structures
2. **TXMetaCache mmap** (30-50%): Transaction metadata cache
3. **Go Metadata** (5-10%): Runtime structures
4. **Stack** (1-5%): Goroutine stacks
5. **Anonymous RW** (5-10%): Misc allocations
6. **File-backed** (5-10%): Shared libraries, binary

### What Uses External Memory?

- **TXMetaCache**: Explicitly uses mmap for large cache
- **gRPC**: Connection buffers (Go heap)
- **Kafka**: Message buffers (Go heap)
- **Network**: TCP/UDP buffers (kernel, not in RSS)
- **File I/O**: OS page cache (not in RSS)

## Example Output

```text
=== Complete Memory Breakdown ===

Go Runtime:
  Heap:              2048 MB
  Stack:              128 MB
  Metadata:           256 MB
  Subtotal:          2432 MB

Named Memory Regions (mmap):
  TXMetaCache                              4096 MB
  Subtotal:          4096 MB

File-Backed Memory:
  Shared Libraries                           64 MB
  Teranode Binary                           181 MB
  Subtotal:           245 MB

Anonymous Memory:
  Read-Write:         512 MB
  Read-Only:           32 MB
  Executable:          16 MB
  Subtotal:           560 MB

=== Totals ===
Virtual Memory:    12288 MB
RSS (Resident):    7333 MB
PSS (Proportional): 7333 MB

=== Top 20 Memory Regions ===

ADDRESS            PERMS  RSS        PRIVATE    SHARED     NAME
--------------------------------------------------
7f8a40000000...    rw-p   4096 MB    4096 MB         0 MB [anon: TXMetaCache]
7f8a30000000...    rw-p   2048 MB    2048 MB         0 MB [heap]
7f8a20000000...    r-xp    181 MB     181 MB         0 MB /app/teranode.run
...
```

## Performance

- Parsing `/proc/self/smaps` takes ~1-5ms
- Memory overhead: <1MB
- Safe to call frequently (e.g., every 10 seconds)

## Limitations

- Linux only (requires `/proc/[pid]/smaps`)
- Requires read access to `/proc/[pid]/smaps`
- For other processes, may need root or same user

## Future Enhancements

- [ ] Export metrics to Prometheus
- [ ] Track memory growth over time
- [ ] Alert on abnormal memory patterns
- [ ] Integration with pprof for combined view
- [ ] Support for other platforms (macOS, Windows)
