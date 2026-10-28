# Probe Service Settings

**Related Topic**: [Probe Service](../../../topics/services/probe.md)

## Configuration Settings

Settings are organized under the `Probe` struct in `settings.Settings`.

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| HTTPListenAddress | string | "localhost:8100" | probe_httpListenAddress | Bind address of the dashboard and the API |
| AuthToken | string | "" | probe_authToken | **CRITICAL** - Bearer token for every request. Required when the probe is on; an empty value stops the node at startup |
| SampleInterval | time.Duration | 2s | probe_sampleInterval | Interval between samples of service metrics, Blockchain and P2P |
| SlowSampleInterval | time.Duration | 15s | probe_slowSampleInterval | Interval between samples of Aerospike, Kafka and Postgres |
| BaselineWarmup | time.Duration | 30m | probe_baselineWarmup | Minimum learning time before a baseline is ready |
| SnoozeDefault | time.Duration | 15m | probe_snoozeDefault | Snooze length of one alarm when the request gives no duration |
| AlarmLogRetention | time.Duration | 168h | probe_alarmLogRetention | Age after which alarm log entries are removed |
| MetricsTargets | []string | [] | probe_metricsTargets | Pipe-separated `host:port`, `host:port/path` or URL entries. Empty reads the registry of the probe process |
| StatePath | string | "" | probe_statePath | State file path. Empty means `probe_state.json` in the working directory |
| AerospikeURL | *url.URL | "" | probe_aerospikeURL | Aerospike URL. Empty uses the UTXO store URL when it is Aerospike |
| PostgresURL | *url.URL | "" | probe_postgresURL | Postgres URL. Empty uses the blockchain store URL when it is Postgres |
| PostgresDiskCapacityGB | float64 | 0 | probe_postgresDiskCapacityGB | Postgres volume size in GB for the disk fill reading; 0 means unknown |
| Floors | []string | [] | probe_floors | Pipe-separated `metric=value` floors. Unknown keys stop the node at startup |

### Service Control

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| startProbe | bool | false (settings.conf ships `false`) | startProbe | Enable or disable the Probe service. The `-probe=1` flag does the same |

`startProbe` is read directly by `daemon.shouldStart()`, one per service, and is not part of the
settings package - it has no struct tag and does not appear in `ExportMetadata()`.

## Configuration Dependencies

### Credentials

When `probe_aerospikeURL` or `probe_postgresURL` is empty, the probe connects with the node's own
store URL and therefore with the node's read-write credentials. The probe sends only read
requests (Aerospike info commands and `READ ONLY` Postgres transactions). To give the probe
read-only credentials, set the two `probe_*URL` settings to a read-only user.

### Valid Floor Keys

`cache.hitRate`, `stores.aerospike.readP99Sec`, `validator.latencyP99Sec`, `validator.queueFill`,
`assembly.queueHeadAgeSec`, `assembly.tipLagBlocks`, `ingress.subtreeQueueOldestSec`,
`chain.persisterBehind`.
