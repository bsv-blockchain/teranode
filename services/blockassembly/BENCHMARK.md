# LoadUnminedTransactions Benchmark

This benchmark suite tests the performance of the `loadUnminedTransactions` function with a real Aerospike container.

## Benchmarks

### BenchmarkLoadUnminedTransactions
Tests the function with varying transaction counts (1,000, 10,000, and 50,000) and both scan modes:
- **Index-based scan** (`fullScan=false`) - Uses the `unminedSinceIndex` for faster lookups
- **Full scan** (`fullScan=true`) - Scans all transactions

### BenchmarkLoadUnminedTransactions_MixedStates
Simulates real-world conditions with 10,000 transactions in mixed states:
- 50% unmined transactions
- 25% already mined in main chain
- 25% locked transactions

## Running the Benchmarks

### Basic Run
```bash
cd services/blockassembly
go test -bench=BenchmarkLoadUnminedTransactions -benchmem -benchtime=3x -timeout=30m
```

### Run Specific Benchmark
```bash
go test -bench=BenchmarkLoadUnminedTransactions/txCount=10000/fullScan=false \
  -benchmem -benchtime=3x -timeout=30m
```

### Run Mixed States Benchmark
```bash
go test -bench=BenchmarkLoadUnminedTransactions_MixedStates \
  -benchmem -benchtime=3x -timeout=30m
```

## Profiling for Hotspot Analysis

### CPU Profiling
Identify which functions consume the most CPU time:

```bash
go test -bench=BenchmarkLoadUnminedTransactions/txCount=10000/fullScan=false \
  -benchmem -benchtime=3x -cpuprofile=cpu.prof -timeout=30m

# View the profile in your browser
go tool pprof -http=:8080 cpu.prof
```

In the pprof UI:
- **Graph** view shows call relationships
- **Top** view lists functions by CPU usage
- **Flame Graph** shows execution hierarchy
- **Source** view shows line-by-line profiling

### Memory Profiling
Identify memory allocation hotspots:

```bash
go test -bench=BenchmarkLoadUnminedTransactions/txCount=10000/fullScan=false \
  -benchmem -benchtime=3x -memprofile=mem.prof -timeout=30m

# View the profile
go tool pprof -http=:8080 mem.prof
```

### Trace Profiling
See detailed execution timeline:

```bash
go test -bench=BenchmarkLoadUnminedTransactions/txCount=10000/fullScan=false \
  -benchmem -benchtime=1x -trace=trace.out -timeout=30m

# View the trace
go tool trace trace.out
```

## Understanding the Output

Example output:
```
BenchmarkLoadUnminedTransactions/txCount=10000/fullScan=false-8
    3    1234567890 ns/op    123.45 tx/sec    12345 ns/tx    5000000 B/op    100000 allocs/op
```

Metrics:
- **3** - Number of iterations run
- **1234567890 ns/op** - Nanoseconds per operation
- **123.45 tx/sec** - Transactions processed per second (custom metric)
- **12345 ns/tx** - Nanoseconds per transaction (custom metric)
- **5000000 B/op** - Bytes allocated per operation
- **100000 allocs/op** - Number of allocations per operation

## Optimization Targets

Based on the profiling, look for optimization opportunities in:

1. **Iterator Performance**
   - GetUnminedTxIterator creation time
   - Iterator.Next() call frequency and duration

2. **Data Processing**
   - Sorting of unmined transactions
   - Parent chain validation
   - Block ID lookups in map

3. **Memory Allocations**
   - Slice reallocations during append
   - Temporary object creation
   - String/byte conversions

4. **Subtree Processing**
   - AddDirectly calls
   - Lock contention in concurrent operations

## Comparing Results

To compare before/after optimization:

```bash
# Before optimization
go test -bench=BenchmarkLoadUnminedTransactions/txCount=10000/fullScan=false \
  -benchmem -benchtime=10x -count=5 > before.txt

# Make your changes...

# After optimization
go test -bench=BenchmarkLoadUnminedTransactions/txCount=10000/fullScan=false \
  -benchmem -benchtime=10x -count=5 > after.txt

# Compare with benchstat
go install golang.org/x/perf/cmd/benchstat@latest
benchstat before.txt after.txt
```

## Notes

- Benchmarks use a real Aerospike container via testcontainers
- Setup time (container initialization, data population) is excluded from measurements
- The subtree processor is reset between iterations to ensure consistent state
- Index wait time is included in setup but not in benchmark timing
