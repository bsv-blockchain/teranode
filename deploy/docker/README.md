# deploy/docker

Docker Compose files for the infrastructure a Teranode developer runs next to a
natively built node: Aerospike, PostgreSQL, Kafka and the monitoring stack.
Nothing here runs Teranode itself.

> To run a Teranode node with Docker, use
> [teranode-quickstart](https://github.com/bsv-blockchain/teranode-quickstart).

## Layout

| Directory | What it runs | Started by |
| --- | --- | --- |
| `aerospike/` | Aerospike UTXO store: `docker-compose-ee.yml` (Enterprise evaluation), `docker-compose-tn.yml` (TN edition), `docker-compose-cluster.yml` (3 nodes) | `scripts/aerospike.sh`, `scripts/aerospike-tn.sh`, `scripts/aerospike-cluster.sh`; `teranode-dev up` with the `aerospike` UTXO backend |
| `postgres/` | PostgreSQL | `scripts/postgres.sh`; `teranode-dev up` with the `postgres` UTXO backend |
| `kafka/` | Redpanda (`kafka-shared`) and Redpanda Console | `scripts/kafka.sh`; `teranode-dev up` with Docker Kafka enabled |
| `monitoring/` | Prometheus and Grafana scraping a node on the host (see [`monitoring/README.md`](monitoring/README.md)) | `scripts/monitoring.sh`; `teranode-dev up` with monitoring enabled |
| `base/` | Shared service definitions and the config files they mount | see below |

The compose files in `aerospike/`, `postgres/`, `kafka/` and `monitoring/`
`extends` services from `base/docker-services.yml`. Data lands under
`${DATA_PATH}`, which the scripts and `teranode-dev` set to a local `data/`
directory.

## base/

- `docker-services.yml`: service definitions (postgres, kafka-shared,
  kafka-console-shared, aerospike*, aerospike-exporter, prometheus,
  alertmanager, grafana, asset-cache) and the configs they mount:
  `aerospike*.conf`, `aerospike-asmt-wrapper.sh`, `kafka-console-config.yml`,
  `prometheus.yml`, `alertmanager.yml`, `grafana_datasource.yaml`,
  `grafana_dashboards/`, `asset-cache-nginx.conf`.
- `asset-cache-nginx.conf`: also used by `scripts/nginx-cache.sh`, which runs
  the nginx asset cache in front of a local asset service.
- `blockassembly.rules.yml`: the canonical block-assembly alert rules. Mounted
  by `monitoring/`, the `compose/` multi-node stacks and
  `test/docker-compose-host.yml`. `blockassembly.rules_test.yml` holds their
  unit tests; `.github/workflows/prometheus_rules.yaml` runs both through
  `promtool`.
- `grafana_dashboards/`: dashboards provisioned by the Grafana service (see
  [`grafana_dashboards/README.md`](base/grafana_dashboards/README.md)).
