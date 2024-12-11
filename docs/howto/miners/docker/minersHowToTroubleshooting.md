# How to Troubleshoot Teranode (Docker Compose)


## Index

- [Introduction](#introduction)
- [Troubleshooting](#troubleshooting)
    - [Health Checks and System Monitoring](#health-checks-and-system-monitoring)
        - [Service Status](#service-status)
        - [Detailed Container/Pod Health](#detailed-containerpod-health)
        - [Viewing Health Check Logs](#viewing-health-check-logs)
        - [Monitoring System Resources](#monitoring-system-resources)
    - [Recovery Procedures](#recovery-procedures)
        - [Third Party Component Failure](#third-party-component-failure)

## Introduction

This guide provides troubleshooting steps for common issues encountered while running Teranode with Docker Compose.



## Troubleshooting



### Health Checks and System Monitoring



#### Service Status


```bash
docker-compose ps
```
This command lists all services defined in your docker-compose.yml file, along with their current status (Up, Exit, etc.) and health state if health checks are configured.



#### Detailed Container/Pod Health



```bash
docker inspect --format='{{json .State.Health}}' container_name
```
Replace `container_name` with the name of your specific Teranode service container.



##### Configuring Health Checks




In your docker-compose.yml file:

```yaml
services:
  ubsv-blockchain:
    ...
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8087/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
```




#### Viewing Health Check Logs





```bash
docker inspect --format='{{json .State.Health}}' container_name | jq
```



#### Monitoring System Resources





* Use `docker stats` to monitor CPU, memory, and I/O usage of containers:
  ```bash
  docker stats
  ```


* Consider using Prometheus and Grafana for comprehensive monitoring.
* Look for services consuming unusually high resources.



##### Viewing Global Logs





```bash
docker-compose logs
docker-compose logs -f  # Follow logs in real-time
docker-compose logs --tail=100  # View only the most recent logs
```




##### Viewing Logs for Specific Microservices





```bash
docker-compose logs [service_name]
```



##### Useful Options for Log Viewing





* Show timestamps:
  ```bash
  docker-compose logs -t
  ```
* Limit output:
  ```bash
  docker-compose logs --tail=50 [service_name]
  ```
* Since time:
  ```bash
  docker-compose logs --since 2023-07-01T00:00:00 [service_name]
  ```




##### Checking Logs for Specific Teranode Microservices



For Docker Compose, replace `[service_name]` with the appropriate service or pod name:

* Propagation Service
* Blockchain Service
* Asset Service
* Block Validation Service
* P2P Service
* Block Assembly Service
* Subtree Validation Service
* Miner Service
* RPC Service
* Block Persister Service
* UTXO Persister Service
* Postgres Database    [*Only in Docker*]
* Aerospike Database  [*Only in Docker*]
* Kafka                            [*Only in Docker*]
* Kafka Console            [*Only in Docker*]
* Prometheus               [*Only in Docker*]
* Grafana                       [*Only in Docker*]



##### Redirecting Logs to a File





```bash
docker-compose logs > teranode_logs.txt
docker-compose logs [service_name] > [service_name]_logs.txt
```



Remember to replace placeholders like `[service_name]`, and label selectors with the appropriate values for your Teranode setup.



#### **Check Services Dashboard**



Check your Grafana `UBSV Service Overview` dashboard:



- Check that there's no blocks in the queue (`Queued Blocks in Block Validation`). We expect little or no queueing, and not creeping up. 3 blocks queued up are already a concern.



- Check that the propagation instances are handling around the same load to make sure the load is equally distributed among all the propagation servers. See the `Propagation Processed Transactions per Instance` diagram.



- Check that the cache is at a sustainable pattern rather than "exponentially" growing (see both the `Tx Meta Cache in Block Validation` and `Tx Meta Cache Size in Block Validation` diagrams).



- Check that go routines (`Goroutines` graph) are not creeping up or reaching excessive levels.



### Recovery Procedures



#### Third Party Component Failure



Teranode is highly dependent on its third party dependencies. Postgres, Kafka and Aerospike are critical for Teranode operations, and the node cannot work without them.



If a third party service fails, you must restore its functionality. Once it is back, please restart Teranode cleanly, as per the instructions in the Section 6 of this document.



------



Should you encounter a bug, please report it following the instructions in the [Bug Reporting](../../bugReporting.md) section.
