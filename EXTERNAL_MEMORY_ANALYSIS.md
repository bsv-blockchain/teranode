# External Memory Analysis for Teranode

## Summary

This document identifies all sources of external memory (non-Go-heap) in Teranode and provides tools to track and analyze them.

## What is External Memory?

External memory is memory allocated outside of Go's heap allocator. This includes:

- Memory-mapped files (mmap)
- CGO/C library allocations
- Shared memory regions
- OS kernel buffers
- Go runtime metadata

Go's heap profiler (`pprof`) **does not track this memory**, which can cause confusion when RSS is much larger than reported heap usage.

## Analysis Results

### ✅ TXMetaCache mmap (Already Tracked)

- **Location**: `stores/txmetacache/`
- **Method**: `unix.Mmap()`
- **Status**: Now instrumented with naming via `PR_SET_VMA_ANON_NAME`
- **Impact**: Can be 30-50% of total RSS
- **Notes**: This is the largest source of external memory

### ✅ No CGO Usage

- **Finding**: No `import "C"` statements found
- **Impact**: Zero C library allocations
- **Status**: No action needed

### ✅ No Shared Memory

- **Finding**: No `shm_open`, `shmget`, or `MAP_SHARED` usage
- **Impact**: Zero shared memory allocations
- **Status**: No action needed

### ✅ Pure Go External Libraries
All external dependencies use pure Go implementations:

| Library | Usage | Memory Allocation |
|---------|-------|-------------------|
| IBM/sarama | Kafka client | Go heap |
| Aerospike client | UTXO store | Go heap |
| gRPC | RPC framework | Go heap |
| PostgreSQL (GORM) | Blockchain store | Go heap |

**Impact**: All library allocations are tracked by Go heap profiler

### ✅ File-Backed Memory

- **Blob Store** (`stores/blob/file/`): Uses standard file I/O
- **Impact**: Minimal - uses OS page cache (not counted in RSS)
- **Status**: No action needed

### ✅ Network Buffers

- **TCP/UDP sockets**: Managed by kernel
- **Impact**: Minimal - kernel buffers not counted in process RSS
- **Status**: No action needed

## Memory Breakdown (Expected)

For a typical Teranode process:

```text
Category                   Percentage    Notes
-----------------------------------------------------------
Go Heap                    30-40%        Business logic, data structures
TXMetaCache mmap           30-50%        Transaction metadata cache
Go Runtime Metadata         5-10%        Type info, GC structures
Stack                       1-5%         Goroutine stacks
Anonymous RW                5-10%        Misc allocations
File-backed                 5-10%        Shared libraries, binary
-----------------------------------------------------------
Total RSS                  100%
```

## Tools Provided

### 1. Memory Analyzer Library

**Location**: `internal/profiling/`

Provides programmatic access to complete memory breakdown:

```go
breakdown, err := profiling.GetCompleteMemoryProfile()
fmt.Println(breakdown.FormatBreakdown())
fmt.Println(breakdown.FormatTopRegions(20))
```

### 2. HTTP Handler

Add to any service for runtime inspection:

```go
http.HandleFunc("/debug/memory", profiling.MemoryProfileHandler)
```

Access via:

```bash
curl http://localhost:8090/debug/memory?top=50
```

### 3. CLI Tool

**Location**: `cmd/memanalyzer/`

Analyze any running Teranode process:

```bash
# Build
go build -o memanalyzer ./cmd/memanalyzer

# Analyze a process
./memanalyzer -pid 12345 -top 50

# Show all regions
./memanalyzer -pid 12345 -all
```

**Note**: Linux only (requires `/proc/[pid]/smaps`)

## Recommendations

### For Development

1. Use `/debug/memory` endpoint in local development
2. Compare Go heap profile with RSS breakdown
3. Monitor TXMetaCache mmap size vs configuration

### For Production

1. Add memory breakdown to monitoring dashboards
2. Set up alerts if anonymous memory grows unexpectedly
3. Track RSS growth trends by category

### For Debugging Memory Issues

1. Get heap profile: `curl http://localhost:6060/debug/pprof/heap > heap.prof`
2. Get complete breakdown: `curl http://localhost:8090/debug/memory > memory.txt`
3. Compare:
   - If RSS >> heap, check TXMetaCache and anonymous memory
   - If heap ≈ RSS, issue is in Go allocations
   - If TXMetaCache is large, check cache configuration

## Example Analysis Session

```bash
# 1. Check current memory usage
kubectl exec -it subtree-validator-pod -- cat /proc/1/smaps | grep -E "^Rss:|^[0-9a-f]+-" | head -40

# 2. Identify large anonymous regions
kubectl exec -it subtree-validator-pod -- cat /proc/1/smaps | awk '/^[0-9a-f]/{region=$0} /^Rss:/{print $2, region}' | sort -rn | head -20

# 3. Check named regions (our mmaps)
kubectl exec -it subtree-validator-pod -- cat /proc/1/maps | grep anon:

# 4. Use our tool
go build -o memanalyzer ./cmd/memanalyzer
kubectl cp memanalyzer subtree-validator-pod:/tmp/
kubectl exec -it subtree-validator-pod -- /tmp/memanalyzer -top 30
```

## Interpreting Results

### Normal Patterns

- TXMetaCache mmap: Large (GB), stable
- Go Heap: Large (GB), fluctuates with GC
- Go Stack: Small-Medium (MB), grows with goroutines
- Anonymous RW: Small-Medium (MB), relatively stable

### Potential Issues

- **Anonymous RW growing**: Possible memory leak outside Go heap
- **Go Metadata very large**: Too many types/functions being loaded
- **Multiple large anonymous regions**: Untracked mmaps
- **File-backed memory growing**: Memory-mapped files not being unmapped

## Conclusion

Teranode's external memory sources have been thoroughly analyzed:

1. **TXMetaCache mmap** is the only significant external memory source (30-50% of RSS)
2. **No CGO or C libraries** means no hidden allocations
3. **Pure Go dependencies** keep everything in Go heap
4. **Tools provided** give complete visibility into all memory

The provided tools enable:

- Real-time memory breakdown
- Identification of memory growth sources
- Debugging of RSS vs heap discrepancies
- Production monitoring and alerting

For questions about unexplained memory usage, first check:

1. TXMetaCache configuration and actual usage
2. Go heap profile for business logic leaks
3. Anonymous RW regions for unexpected allocations
