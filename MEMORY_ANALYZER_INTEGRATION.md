# Memory Analyzer Integration

## Overview

The memory analyzer is integrated into Teranode and available on all services that have the profiler enabled **on a loopback-bound address**.

## Where It's Used

The memory analyzer handler is registered in `daemon/daemon_services.go` in `newProfilerMux`, and only when `profilerAddr` is loopback-bound:

```go
// memory analyzer support (includes mmap, non-heap memory) - loopback only
if util.IsLoopbackListenAddress(profilerAddr) {
    logger.Infof("Memory analyzer available on http://%s/debug/memory", profilerAddr)
    mux.HandleFunc("/debug/memory", profiling.MemoryProfileHandler)
}
```

### Why loopback only

The analyzer output lists the exact virtual-address range of every large mapping in the
process (the Teranode binary, shared libraries, heap and stack regions, named mmaps).
That defeats ASLR, which is the prerequisite for turning any later memory-corruption bug
into a reliable exploit. The default non-dev `profilerAddr` is `:${PROFILE_PORT}` (all
interfaces, so Prometheus can scrape `/metrics`), and on that bind `/debug/memory`
answers 404. The `.dev` context binds `localhost:${PROFILE_PORT}` and keeps the route.

## How to Access

### Requirements

The memory analyzer is available when:

1. `ProfilerAddr` is set in settings to a loopback address (e.g., `localhost:6060`); on `:6060` or any interface bind the route is not registered
2. The service has started successfully
3. You're running on Linux (requires `/proc/[pid]/smaps`)

### Endpoints

For any Teranode service with profiler enabled:

```bash
# Get complete memory breakdown with top 20 regions (default)
curl http://localhost:6060/debug/memory

# Get breakdown with top 50 regions
curl http://localhost:6060/debug/memory?top=50

# Get breakdown with top 100 regions
curl http://localhost:6060/debug/memory?top=100
```

### Example: Accessing from Kubernetes

Set `profilerAddr` to `localhost:6060` on the pod (port-forward reaches loopback inside the
pod's network namespace), or run `cmd/memanalyzer` via `kubectl exec` instead.

```bash
# Port-forward to a pod
kubectl port-forward subtree-validator-pod 6060:6060

# In another terminal, get memory analysis
curl http://localhost:6060/debug/memory > memory_analysis.txt
```

### Example: Accessing from Docker Compose

```bash
# Only when the service's profilerAddr is loopback-bound and reached from inside
# the container; a published port on a wildcard bind answers 404.
docker compose exec <service> curl -s http://localhost:6060/debug/memory
```

## Which Services Have It

The memory analyzer is available on **all services** that have the profiler enabled, including:

- Blockchain
- Block Assembly
- Block Validation
- Subtree Validation
- Validator
- Propagation
- Asset Server
- RPC
- P2P
- Legacy
- Alert
- Pruner
- Block Persister
- UTXO Persister

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
...
```

## Integration Details

### Files Added

1. **`internal/profiling/memory_analyzer.go`**
   - Core memory analysis logic
   - Parses `/proc/self/smaps`
   - Categorizes memory by type

2. **`internal/profiling/memory_handler.go`**
   - HTTP handler for web access
   - Query parameter support

3. **`internal/profiling/README.md`**
   - Detailed usage documentation

4. **`cmd/memanalyzer/main.go`**
   - Standalone CLI tool
   - Can analyze any process by PID

### Files Modified

1. **`daemon/daemon_services.go`**
   - Added import: `"github.com/bsv-blockchain/teranode/internal/profiling"`
   - Registered handler in `startProfilerAndMetrics` function

## Other Debug Endpoints

The memory analyzer joins other debugging endpoints available on the same port:

- `/debug/pprof/` - Go profiler (CPU, memory, goroutines, etc.)
- `/debug/pprof/profile` - CPU profile
- `/debug/pprof/heap` - Heap profile
- `/debug/pprof/goroutine` - Goroutine dump
- `/debug/fgprof` - Full goroutine profiler
- `/debug/memory` - **Complete memory analyzer (NEW)**
- `/metrics` - Prometheus metrics (if enabled)

## Typical Workflow

### Investigating High Memory Usage

1. **Check overall RSS:**

   ```bash
   kubectl top pod subtree-validator-pod
   ```

2. **Get complete breakdown:**

   ```bash
   curl http://localhost:6060/debug/memory > memory.txt
   ```

3. **Compare with heap profile:**

   ```bash
   curl http://localhost:6060/debug/pprof/heap > heap.prof
   go tool pprof -http=:8080 heap.prof
   ```

4. **Analyze the difference:**
   - If RSS >> heap: Check TXMetaCache and anonymous memory
   - If RSS ≈ heap: Issue is in Go allocations
   - If TXMetaCache is large: Check configuration

### Monitoring in Production

Add to monitoring dashboards:

```bash
# Cron job or monitoring script
while true; do
  curl -s http://localhost:6060/debug/memory | \
    grep "RSS (Resident)" | \
    awk '{print $3}' | \
    send_to_metrics_system
  sleep 60
done
```

## Platform Notes

- **Linux**: Fully supported (requires `/proc/[pid]/smaps`)
- **macOS**: Not supported (no `/proc` filesystem)
- **Windows**: Not supported

For non-Linux platforms, the handler will return an error message.

## Performance Impact

- Parsing `/proc/self/smaps`: ~1-5ms
- Memory overhead: <1MB
- Safe to call frequently (e.g., every 10 seconds)
- No GC pressure

## Troubleshooting

### "Failed to open /proc/self/smaps"

This means:

1. Running on non-Linux platform
2. `/proc` not mounted (container misconfiguration)
3. Insufficient permissions

### Handler Not Found (404)

This means:

1. Profiler not enabled (check `ProfilerAddr` setting)
2. Service hasn't started yet
3. Wrong port or service

### Empty Named Regions

This means:

1. TXMetaCache hasn't been initialized yet
2. Not using the mmap tracking code
3. Running on non-Linux (regions won't have names)

## Future Enhancements

Planned improvements:

- [ ] Export metrics to Prometheus format
- [ ] Alert on abnormal memory growth
- [ ] Historical tracking and trends
- [ ] Integration with pprof UI
- [ ] Support for other platforms (macOS, Windows)
