# Global Settings

**Related Topics**: [System Architecture](../../topics/architecture/teranode-microservices-overview.md), [Prometheus Metrics](../prometheusMetrics.md)

Global settings control system-wide behavior across all Teranode services. These settings affect tracing, logging, security, health checks, and profiling for the entire node.

## Configuration Settings

### Service Identification

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| ServiceName | string | "teranode" | SERVICE_NAME | Service identification for monitoring |
| ClientName | string | "defaultClientName" | clientName | Client/node identification |
| DataFolder | string | "data" | dataFolder | Data storage directory |
| Context | string | (from SETTINGS_CONTEXT) | SETTINGS_CONTEXT | **CRITICAL** - Settings context selector |

### Tracing Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| TracingEnabled | bool | false | tracing_enabled | **CRITICAL** - Enable distributed tracing |
| TracingSampleRate | float64 | 0.01 | tracing_SampleRate | Tracing sample rate (1% default) |
| TracingCollectorURL | *url.URL | <http://localhost:4318> | tracing_collector_url | Jaeger/OTLP collector endpoint |

### Logging Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| Logger | string | "" | logger | Logger implementation selection |
| LogLevel | string | "INFO" | logLevel | **CRITICAL** - Logging verbosity level |
| PrettyLogs | bool | true | prettyLogs | Human-readable log formatting |
| JSONLogging | bool | false | jsonLogging | JSON-structured log output |

### Debug Toggles

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| Debug.All | bool | false | debug_all | Enable every subsystem debug log |
| Debug.File | bool | false | debug_file | Low-level filesystem operation logs |
| Debug.Blobstore | bool | false | debug_blobstore | High-level blob store API call logs with stack traces |
| Debug.UTXOStore | bool | false | debug_utxostore | UTXO store file operation logs |

#### Debug Toggle Details

**Prerequisites**: All debug toggles require `logLevel = DEBUG` to emit logs. Setting a debug flag without DEBUG log level has no effect.

**`debug_file`** - Low-level filesystem operations:
- Detailed logs of file system I/O operations
- File paths, byte counts, checksums, DAH cleanup operations
- Use when debugging: disk space issues, file I/O problems, filesystem-level errors
- Example logs: `"[File] SetFromReader start key=... type=... size=..."`, `"[File] removing expired file: ..."`

**`debug_blobstore`** - High-level blob store API tracing:
- Logs all blob store API calls (Get, Set, Exists, Del, etc.) with 5-level deep call stacks
- Shows which code is calling blob store operations and why
- Use when debugging: application-level blob access patterns, API usage tracking
- Example logs: `"[BlobStore][logger][Get] key ... fileType ... : called from ..."`
- Note: Only active when `?logger=true` is set in blob store URL configuration

**`debug_utxostore`** - UTXO store file operations:
- Similar to `debug_file` but specific to UTXO store file operations
- Use when debugging UTXO-specific storage issues

**`debug_all`** - Enable all subsystems:
- Shortcut to enable all debug flags at once
- Equivalent to setting all individual flags to true
- Use for comprehensive debugging or when you're unsure which subsystem is involved

#### Hierarchical Relationships

Debug flags have hierarchical dependencies:
- `debug_file = true` → enables both File logs AND Blobstore logs (when `?logger=true` is set)
- `debug_blobstore = true` → enables only Blobstore logs
- `debug_all = true` → enables ALL debug logs across all subsystems

This hierarchy exists because file-level operations are the foundation of blob store operations. When debugging filesystem issues, you typically want to see both layers.

### HTTP Security Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| SecurityLevelHTTP | int | 0 | securityLevelHTTP | **CRITICAL** - HTTP (0) vs HTTPS (non-zero) |
| ServerCertFile | string | "" | server_certFile | **Required for HTTPS** - TLS certificate file |
| ServerKeyFile | string | "" | server_keyFile | **Required for HTTPS** - TLS private key file |

### gRPC Global Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| GRPCResolver | string | "" | grpc_resolver | gRPC name resolver configuration |
| GRPCMaxRetries | int | 40 | grpc_max_retries | **CRITICAL** - Maximum gRPC retry attempts |
| GRPCRetryBackoff | time.Duration | 250ms | grpc_retry_backoff | Retry backoff duration |
| SecurityLevelGRPC | int | 0 | security_level_grpc | gRPC security level |
| UsePrometheusGRPCMetrics | bool | true | use_prometheus_grpc_metrics | Enable gRPC Prometheus metrics |
| GRPCAdminAPIKey | string | "" | grpc_admin_api_key | Admin API authentication key |

### Monitoring and Profiling

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| StatsPrefix | string | "gocore" | stats_prefix | Statistics metric prefix |
| PrometheusEndpoint | string | "" | prometheusEndpoint | Prometheus metrics endpoint |
| HealthCheckHTTPListenAddress | string | ":8000" | health_check_httpListenAddress | **CRITICAL** - Health check server binding |
| ProfilerAddr | string | "" | profilerAddr | Go pprof profiler address |
| UseDatadogProfiler | bool | false | use_datadog_profiler | Enable Datadog profiler integration |

### Database and Storage

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| PostgresCheckAddress | string | "localhost:5432" | postgres_check_address | PostgreSQL connection check address |
| GlobalBlockHeightRetention | uint32 | 288 | global_blockHeightRetention | **CRITICAL** - Block height retention (2 days default) |

### Performance and Optimization

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| UseCgoVerifier | bool | true | use_cgo_verifier | **CRITICAL** - Use CGO-based signature verification |
| LocalTestStartFromState | string | "" | local_test_start_from_state | **TESTING ONLY** - Initial test state |

## Configuration Dependencies

### Settings Context System

- `Context` is set via `SETTINGS_CONTEXT` environment variable
- Controls which settings overrides are applied from settings.conf
- Common contexts: `dev`, `test`, `docker`, `operator`, `mainnet`, `teratestnet`
- Example: `SETTINGS_CONTEXT=dev` applies settings with `.dev` suffix

### Tracing Configuration

- When `TracingEnabled = true`:

    - Requires valid `TracingCollectorURL` (Jaeger/OTLP endpoint)
    - `TracingSampleRate` controls sampling (0.01 = 1% of traces)
    - Integrates with OpenTelemetry for distributed tracing

### Logging Configuration

- `LogLevel` values: "DEBUG", "INFO", "WARN", "ERROR", "FATAL"
- `PrettyLogs = true`: Human-readable colored output (development)
- `JSONLogging = true`: Structured JSON logs (production)
- Cannot enable both `PrettyLogs` and `JSONLogging` simultaneously
- Subsystem debug toggles (`debug_*`) still require `logLevel = DEBUG`

### HTTPS Configuration

- When `SecurityLevelHTTP != 0`:

    - Requires valid `ServerCertFile` (PEM format)
    - Requires valid `ServerKeyFile` (PEM format)
    - Applies to HTTP servers across all services

### gRPC Configuration

- `GRPCMaxRetries` controls retry behavior for all gRPC clients
- `GRPCRetryBackoff` determines delay between retries
- `UsePrometheusGRPCMetrics` enables gRPC method-level metrics
- `GRPCAdminAPIKey` used for administrative gRPC endpoints

### Health Check System

- `HealthCheckHTTPListenAddress` starts global health check server
- Aggregates health status from all services
- Returns 200 OK when all services healthy, 503 otherwise
- Critical for Kubernetes liveness/readiness probes

### Performance Optimization

- `UseCgoVerifier = true`: Uses secp256k1 C library (faster)
- `UseCgoVerifier = false`: Uses pure Go implementation (portable)
- CGO version provides 10-20x performance improvement for signature verification

## Validation Rules

| Setting | Validation | Impact |
|---------|------------|--------|
| SecurityLevelHTTP | 0 = HTTP, non-zero = HTTPS | Service startup |
| ServerCertFile | Required when HTTPS enabled | TLS configuration |
| ServerKeyFile | Required when HTTPS enabled | TLS configuration |
| TracingCollectorURL | Must be valid URL when tracing enabled | Tracing functionality |
| LogLevel | Must be valid level | Logging behavior |
| HealthCheckHTTPListenAddress | Must be valid address | Health check availability |

## Configuration Examples

### Development Configuration

```text
SETTINGS_CONTEXT = dev
logLevel = DEBUG
prettyLogs = true
jsonLogging = false
tracing_enabled = false
health_check_httpListenAddress = :8000
```

### Production Configuration

```text
SETTINGS_CONTEXT = operator
logLevel = INFO
prettyLogs = false
jsonLogging = true
tracing_enabled = true
tracing_SampleRate = 0.05
tracing_collector_url = http://jaeger:4318
securityLevelHTTP = 1
server_certFile = /certs/server.crt
server_keyFile = /certs/server.key
use_datadog_profiler = true
prometheusEndpoint = :9090
```

### HTTPS-Enabled Configuration

```text
securityLevelHTTP = 1
server_certFile = /path/to/cert.pem
server_keyFile = /path/to/key.pem
```

### High-Performance Configuration

```text
use_cgo_verifier = true
grpc_max_retries = 60
grpc_retry_backoff = 100ms
use_prometheus_grpc_metrics = true
```

### Testing Configuration

```text
SETTINGS_CONTEXT = test
logLevel = DEBUG
local_test_start_from_state = IDLE
postgres_check_address = localhost:15432
```

## Settings Context Hierarchy

Settings are loaded in the following priority order (highest to lowest):

1. **Environment Variables**: Direct OS environment variables
2. **Context-Specific Settings**: `setting.{context}` from settings.conf
3. **Multi-Level Contexts**: `setting.{context1}.{context2}` for nested contexts
4. **Default Settings**: Base `setting` value from settings.conf
5. **Code Defaults**: Default values defined in settings.go

Example with `SETTINGS_CONTEXT=docker.host.teranode1`:

```text
clientName                       = teranode           # Priority 4
clientName.docker.host.teranode1 = teranode1          # Priority 2 (wins)
```

## Related Documentation

- [Developer Setup](../../tutorials/developers/developerSetup.md)
