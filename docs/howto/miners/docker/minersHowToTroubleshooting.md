# How to Troubleshoot Teranode (Docker Compose)

Last modified: 22-January-2025

## Index

- [Introduction](#introduction)
- [Troubleshooting](#troubleshooting)
    - [Health Checks and System Monitoring](#health-checks-and-system-monitoring)
        - [Service Status](#service-status)
        - [Detailed Container/Pod Health](#detailed-containerpod-health)
        - [Monitoring System Resources](#monitoring-system-resources)
    - [Recovery Procedures](#recovery-procedures)
        - [Third Party Component Failure](#third-party-component-failure)
        - [Container Name Conflicts When Switching Networks](#container-name-conflicts-when-switching-networks)

## Introduction

This guide provides troubleshooting steps for common issues encountered while running Teranode with Docker Compose.

## Troubleshooting

### Health Checks and System Monitoring

#### Service Status

```bash
docker compose ps
```

This command lists all services defined in your docker-compose.yml file, along with their current status (Up, Exit, etc.) and health state if health checks are configured.

#### Detailed Container/Pod Health

```bash
docker inspect --format='{{json .State.Health}}' container_name
```

Replace `container_name` with the name of your specific Teranode service container. For example:

```bash
docker inspect --format='{{json .State.Health}}' asset
```

#### Monitoring System Resources

- Use `docker stats` to monitor CPU, memory, and I/O usage of containers:

  ```bash
  docker stats
  ```

- Consider using Prometheus and Grafana for comprehensive monitoring.
- Look for services consuming unusually high resources.

##### Viewing Global Logs

```bash
docker compose logs
docker compose logs -f  # Follow logs in real-time
docker compose logs --tail=100  # View only the most recent logs
```

##### Viewing Logs for Specific Microservices

```bash
docker compose logs [service_name]
```

##### Useful Options for Log Viewing

- Show timestamps:

  ```bash
  docker compose logs -t
  ```

- Limit output:

  ```bash
  docker compose logs --tail=50 [service_name]
  ```

- Since time:

  ```bash
  docker compose logs --since 2025-01-01T00:00:00 [service_name]
  ```

##### Checking Logs for Specific Teranode Microservices

```bash
docker compose logs [service_name]
```

For Docker Compose, replace `[service_name]` with the appropriate service or pod name:

- Propagation Service (service name: `propagation`)
- Blockchain Service (service name: `blockchain`)
- Asset Service (service name: `asset`)
- Block Validation Service (service name: `blockvalidation`)
- P2P Service (service name: `p2p`)
- Block Assembly Service (service name: `blockassembly`)
- Subtree Validation Service (service name: `subtreevalidation`)
- RPC Server (service name: `rpc`)
- Postgres Database (service name: `postgres`)          [*Only in Docker*]
- Aerospike Database (service name: `aerospike`)        [*Only in Docker*]
- Kafka   (service name: `kafka-shared`)                [*Only in Docker*]
- Kafka Console (service name: `kafka-console-shared`)  [*Only in Docker*]
- Prometheus (service name: `prometheus`)               [*Only in Docker*]
- Grafana  (service name: `grafana`)                    [*Only in Docker*]

##### Redirecting Logs to a File

```bash
docker compose logs > teranode_logs.txt
docker compose logs [service_name] > [service_name]_logs.txt
```

Remember to replace placeholders like `[service_name]`, and label selectors with the appropriate values for your Teranode setup.

#### **Check Services Dashboard**

Check your Grafana `TERANODE Service Overview` dashboard:

- Check that there's no blocks in the queue (`Queued Blocks in Block Validation`). We expect little or no queueing, and not creeping up. 3 blocks queued up are already a concern.

- Check that the propagation instances are handling around the same load to make sure the load is equally distributed among all the propagation servers. See the `Propagation Processed Transactions per Instance` diagram.

- Check that the cache is at a sustainable pattern rather than "exponentially" growing (see both the `Tx Meta Cache in Block Validation` and `Tx Meta Cache Size in Block Validation` diagrams).

- Check that go routines (`Goroutines` graph) are not creeping up or reaching excessive levels.

### Recovery Procedures

#### Third Party Component Failure

Teranode is highly dependent on its third party dependencies. Postgres, Kafka and Aerospike are critical for Teranode operations, and the node cannot work without them.

If a third party service fails, you must restore its functionality. Once it is back, please restart Teranode cleanly following the instructions in the [How to Start and Stop Teranode in Docker](minersHowToStopStartDockerTeranode.md) guide.

#### Container Name Conflicts When Switching Networks

**Problem:** When attempting to start a different Teranode network (e.g., switching from testnet to mainnet, or testnet to teratestnet) on the same machine, Docker Compose may fail with errors indicating that containers with the same names already exist.

**Root Cause:** All Teranode Docker Compose configurations (testnet, mainnet, teratestnet) use the same hardcoded container names for their services. Docker requires container names to be unique across all running containers on the host machine, regardless of which Docker Compose project they belong to.

**Important Note:** Running multiple Teranode network instances simultaneously on the same machine is **not recommended**. Beyond container name conflicts, you will also encounter port conflicts as all networks attempt to bind to the same host ports. This configuration is not supported for production or testing environments.

##### Solution: Proper Cleanup Before Switching Networks

Before starting a different Teranode network, run these commands from your current network directory:

```bash
# Stop and remove all containers
docker compose down

# Verify containers are removed
docker ps -a | grep teranode
```

If any containers remain, remove them with:

```bash
docker container prune
```

Then start your new network from its directory:

```bash
cd deploy/docker/[new-network]  # e.g., testnet, mainnet
docker compose up -d
```

------

Should you encounter a bug, please report it following the instructions in the [Bug Reporting](../../bugReporting.md) section.
