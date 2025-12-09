# Aerospike Store Settings

**Related Topic**: [UTXO Store](../../../topics/stores/utxo.md)

Aerospike is a high-performance NoSQL database used as a backend for Teranode's UTXO store. These settings control Aerospike connection, policies, and performance tuning.

## Configuration Settings

### Connection Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| Host | string | "localhost" | aerospike_host | **CRITICAL** - Aerospike server hostname or IP |
| Port | int | 3000 | aerospike_port | Aerospike server port |

### Policy Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| BatchPolicyURL | *url.URL | "defaultBatchPolicy" | aerospike_batchPolicy | **CRITICAL** - Batch operation policy URL |
| ReadPolicyURL | *url.URL | "defaultReadPolicy" | aerospike_readPolicy | **CRITICAL** - Read operation policy URL |
| WritePolicyURL | *url.URL | "defaultWritePolicy" | aerospike_writePolicy | **CRITICAL** - Write operation policy URL |
| QueryPolicyURL | *url.URL | "defaultQueryPolicy" | aerospike_queryPolicy | **CRITICAL** - Query operation policy URL |
| UseDefaultBasePolicies | bool | false | aerospike_useDefaultBasePolicies | Use Aerospike default base policies |
| UseDefaultPolicies | bool | false | aerospike_useDefaultPolicies | Use Aerospike default policies |

### Performance and Debugging

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| WarmUp | bool | true | aerospike_warmUp | **CRITICAL** - Enable connection pool warm-up |
| StoreBatcherDuration | time.Duration | 10ms | aerospike_storeBatcherDuration | Store batcher flush duration |
| StatsRefreshDuration | time.Duration | 5s | aerospike_statsRefresh | Statistics refresh interval |
| Debug | bool | false | aerospike_debug | Enable Aerospike debug logging |

## Configuration Dependencies

### Policy URL Format

Policy URLs follow the `aerospike:///` scheme with query parameters:

```text
aerospike:///?MaxRetries=5&SleepBetweenRetries=500ms&TotalTimeout=1s&SocketTimeout=1s
```

**Supported Policy Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| MaxRetries | int | Maximum retry attempts |
| SleepBetweenRetries | duration | Delay between retry attempts |
| SleepMultiplier | int | Backoff multiplier for retries |
| TotalTimeout | duration | Total operation timeout |
| SocketTimeout | duration | Socket-level timeout |
| ConcurrentNodes | int | Number of concurrent node connections |

### Policy Types

**Batch Policy** (for bulk operations):

- Higher timeout (default: 64s total, 10s socket)
- Used for batch read/write operations
- ConcurrentNodes parameter controls parallelism

**Read Policy** (for single record reads):

- Lower timeout (default: 1s total, 1s socket)
- Optimized for low-latency reads
- Used for UTXO lookups

**Write Policy** (for single record writes):

- Lower timeout (default: 1s total, 1s socket)
- Controls write behavior (create, update, replace)
- Used for UTXO creation and updates

**Query Policy** (for query operations):

- Timeout configuration for query operations
- Used for database queries

### Policy Configuration Behavior

- When `UseDefaultPolicies = true`:

    - Aerospike client default policies used
    - Policy URLs are ignored

- When `UseDefaultPolicies = false` (recommended):

    - Custom policy URLs are parsed and applied
    - Allows fine-tuned timeout and retry behavior

- When `UseDefaultBasePolicies = true`:

    - Aerospike default base policies used as foundation
    - Custom parameters applied on top

### Connection Pool Warm-Up

- When `WarmUp = true`:

    - Pre-establishes connections to Aerospike cluster
    - Reduces initial request latency
    - Recommended for production deployments

- When `WarmUp = false`:

    - Connections established on-demand
    - May experience higher latency on first requests

### Batcher Configuration

- `StoreBatcherDuration` controls flush frequency:

    - Lower values (1-10ms): Lower latency, more frequent writes
    - Higher values (50-100ms): Better batching, higher throughput
    - Trade-off between latency and throughput

- Works in conjunction with UTXO store batcher settings

## UTXO Store Integration

When using Aerospike as the UTXO store backend, set the store URL:

```text
utxostore = aerospike://${aerospike_host}:${aerospike_port}/namespace?set=utxo&param=value
```

**UTXO Store URL Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| set | string | "utxo" | Aerospike set name |
| WarmUp | int | 0 | Warm-up connection count |
| ConnectionQueueSize | int | 256 | Connection queue size |
| LimitConnectionsToQueueSize | bool | false | Limit connections to queue size |
| MinConnectionsPerNode | int | 0 | Minimum connections per node |
| externalStore | string | "" | External transaction cache store URL |

## Validation Rules

| Setting | Validation | Impact |
|---------|------------|--------|
| Host | Must be valid hostname/IP | Connection failure if invalid |
| Port | Must be valid port number (1-65535) | Connection failure if invalid |
| BatchPolicyURL | Must be valid URL with aerospike:// scheme | Policy parsing failure |
| ReadPolicyURL | Must be valid URL with aerospike:// scheme | Policy parsing failure |
| WritePolicyURL | Must be valid URL with aerospike:// scheme | Policy parsing failure |
| QueryPolicyURL | Must be valid URL with aerospike:// scheme | Policy parsing failure |
| StatsRefreshDuration | Must be positive duration | Statistics update frequency |

## Configuration Examples

### Basic Configuration

```text
aerospike_host = localhost
aerospike_port = 3000
aerospike_useDefaultPolicies = false
aerospike_warmUp = true
```

### Production Configuration with Custom Policies

```text
aerospike_host = aerospike-cluster.example.com
aerospike_port = 3000
aerospike_debug = false
aerospike_warmUp = true
aerospike_useDefaultPolicies = false
aerospike_batchPolicy = aerospike:///?MaxRetries=5&SleepBetweenRetries=500ms&TotalTimeout=64s&SocketTimeout=10s&ConcurrentNodes=4
aerospike_readPolicy = aerospike:///?MaxRetries=3&SleepBetweenRetries=250ms&TotalTimeout=2s&SocketTimeout=1s
aerospike_writePolicy = aerospike:///?MaxRetries=3&SleepBetweenRetries=250ms&TotalTimeout=2s&SocketTimeout=1s
aerospike_storeBatcherDuration = 10ms
aerospike_statsRefresh = 5s
```

### High-Performance Configuration

```text
aerospike_warmUp = true
aerospike_storeBatcherDuration = 5ms
aerospike_batchPolicy = aerospike:///?MaxRetries=5&TotalTimeout=30s&SocketTimeout=5s&ConcurrentNodes=8
aerospike_statsRefresh = 10s
```

### Development/Debug Configuration

```text
aerospike_host = localhost
aerospike_port = 3000
aerospike_debug = true
aerospike_useDefaultPolicies = true
aerospike_warmUp = false
```

### Multi-Node Cluster Configuration

```text
# Node-specific hosts using settings context
aerospike_host.docker.teranode1 = aerospike-1
aerospike_host.docker.teranode2 = aerospike-2
aerospike_host.docker.teranode3 = aerospike-3

aerospike_port.docker.teranode1 = 3100
aerospike_port.docker.teranode2 = 3200
aerospike_port.docker.teranode3 = 3300
```

### Complete UTXO Store with Aerospike

```text
utxostore = aerospike://${aerospike_host}:${aerospike_port}/utxo-store?set=utxo&WarmUp=32&ConnectionQueueSize=256&MinConnectionsPerNode=8&externalStore=file://${DATADIR}/${clientName}/external

aerospike_host = aerospike-cluster
aerospike_port = 3000
aerospike_warmUp = true
aerospike_useDefaultPolicies = false
aerospike_batchPolicy = aerospike:///?MaxRetries=5&TotalTimeout=64s&SocketTimeout=10s
aerospike_readPolicy = aerospike:///?MaxRetries=3&TotalTimeout=1s&SocketTimeout=1s
aerospike_writePolicy = aerospike:///?MaxRetries=3&TotalTimeout=1s&SocketTimeout=1s
aerospike_queryPolicy = aerospike:///?MaxRetries=3&TotalTimeout=2s&SocketTimeout=1s
```

## Performance Tuning Guidelines

### Latency-Optimized Configuration

- Lower `StoreBatcherDuration` (5-10ms)
- Lower policy timeouts (1-2s total)
- Higher `MinConnectionsPerNode` (8-16)
- Enable `WarmUp`

### Throughput-Optimized Configuration

- Higher `StoreBatcherDuration` (50-100ms)
- Higher policy timeouts (5-10s total)
- Higher `ConnectionQueueSize` (512-1024)
- Higher `ConcurrentNodes` in batch policy (8-16)

### Memory-Constrained Configuration

- Lower `ConnectionQueueSize` (64-128)
- Lower `MinConnectionsPerNode` (2-4)
- Higher `StoreBatcherDuration` (reduce write frequency)
- Lower `ConcurrentNodes` (1-2)

## Monitoring and Debugging

- Set `Debug = true` for detailed Aerospike client logging
- Monitor `StatsRefreshDuration` for performance statistics updates
- Check Aerospike server logs for connection and operation errors
- Use Aerospike Management Console (AMC) for cluster monitoring

## Related Documentation

- [UTXO Store](../../../topics/stores/utxo.md)
- [UTXO Store Settings](utxo_settings.md)
