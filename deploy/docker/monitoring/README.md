# Teranode Monitoring Setup

This directory contains the Docker Compose configuration for running Prometheus and Grafana to monitor Teranode instances.

## Quick Start

```bash
# Start monitoring services
./scripts/monitoring.sh

# Stop monitoring services
./scripts/monitoring.sh down

# Restart monitoring services
./scripts/monitoring.sh restart
```

## Services

Once started, the following services will be available:

- **Grafana**: <http://localhost:3005> - Visualization and dashboards
- **Prometheus**: <http://localhost:9090> - Metrics collection and storage

## Configuration

### Target Configuration

By default, Prometheus is configured to scrape metrics from:

- **Teranode**: `host.docker.internal:9091` - The main Teranode metrics endpoint
- **Aerospike**: `aerospike-exporter:10145` - Aerospike database metrics

This configuration is designed for "all in one" development deployments where Teranode runs on the host machine.

### Alert Rules

The block-assembler alert rules are loaded from the canonical
[`../base/blockassembly.rules.yml`](../base/blockassembly.rules.yml), referenced via `rule_files` in `prometheus.yml` and bind-mounted into the container. This stack runs no Alertmanager, so firing alerts are only visible in the Prometheus UI at <http://localhost:9090/alerts>.

### host.docker.internal Compatibility

The configuration uses `host.docker.internal:9091` to allow Prometheus (running in Docker) to scrape metrics from Teranode running on the host machine.

**Compatibility:**

- ✅ Docker Desktop (Mac/Windows) - Works out of the box
- ✅ Linux with Docker 20.10+ - Requires adding `--add-host=host.docker.internal:host-gateway` when starting containers

### Linux Setup

If you're running on Linux, you need to add extra host mapping. You have two options:

#### Option 1: Add to docker-compose.yml

Add the following to the `prometheus` service in `docker-compose.yml`:

```yaml
prometheus:
  # ... existing config ...
  extra_hosts:
    - "host.docker.internal:host-gateway"
```

#### Option 2: Add to docker compose command

Run the compose command with the additional flag:

```bash
docker compose --add-host=host.docker.internal:host-gateway up
```

## Data Persistence

Monitoring data is stored in the directory specified by the `DATADIR` environment variable (defaults to `./data`):

```text
${DATADIR}/
├── grafana/
│   └── grafana.db/    # Grafana dashboards and settings
└── prometheus/        # Prometheus time-series data
```

## Custom Data Directory

You can specify a custom data directory by setting the `DATADIR` environment variable:

```bash
DATADIR=/custom/path ./scripts/monitoring.sh
```

## Adding Additional Aerospike Clusters

To monitor a dedicated Aerospike cluster, edit `prometheus.yml` and add your cluster nodes under the aerospike job:

```yaml
- job_name: 'aerospike'
  static_configs:
    # Docker-based exporter (default)
    - targets: ['aerospike-exporter:10145']
    # Add your dedicated cluster nodes
    - targets: ['172.x.x.x:9145', '10.x.x.x:9145']
```

See [Aerospike monitoring docs](https://aerospike.com/docs/monitorstack/install/linux) for installation instructions.

## Troubleshooting

### Prometheus can't reach Teranode metrics

If Prometheus shows the target as "down":

1. Verify Teranode is running and exposing metrics on port 9091:

   ```bash
   curl http://localhost:9091/metrics
   ```

2. On Linux, ensure you've added the `extra_hosts` configuration (see Linux Setup above)

3. Check Prometheus logs:

   ```bash
   docker logs prometheus
   ```

### Permission issues with data directories

The monitoring script creates directories with proper permissions, but if you encounter issues:

```bash
# Ensure the data directory is writable
chmod -R 755 ${DATADIR}
```
