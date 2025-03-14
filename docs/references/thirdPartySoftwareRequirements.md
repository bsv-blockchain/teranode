# Teranode Third Party Software Requirements

## Index

- [Introduction](#introduction)
- [Apache Kafka](#apache-kafka)
    - [What is Kafka?](#what-is-kafka)
    - [Kafka in BSV Teranode](#kafka-in-bsv-teranode)
- [PostgreSQL](#postgresql)
    - [What is PostgreSQL?](#what-is-postgresql)
    - [PostgreSQL in Teranode](#postgresql-in-teranode)
- [Aerospike](#aerospike)
    - [What is Aerospike?](#what-is-aerospike)
    - [How Aerospike is Used in Teranode](#how-aerospike-is-used-in-teranode)
- [Shared Storage](#shared-storage)
- [Grafana and Prometheus](#grafana-and-prometheus)
    - [What is Grafana?](#what-is-grafana)
    - [What is Prometheus?](#what-is-prometheus)
    - [Grafana and Prometheus in Teranode](#grafana-and-prometheus-in-teranode)

## Introduction

Teranode relies on a number of third-party software dependencies, some of which can be sourced from different vendors.

BSV provides both a `docker compose` that initialises all dependencies within a single server node, and a `Kubernetes operator` that provides a production-live multi-node setup.


This section will outline the various vendors in use in Teranode.


## Apache Kafka



### What is Kafka?

Apache Kafka is an open-source platform for handling real-time data feeds, designed to provide high-throughput, low-latency data pipelines and stream processing. It supports publish-subscribe messaging, fault-tolerant storage, and real-time stream processing.



To know more, please refer to the Kafka official site: https://kafka.apache.org/.



### Kafka in BSV Teranode

In BSV Teranode, Kafka is used to manage and process large volumes of transaction data efficiently. Kafka serves as a data pipeline, ingesting high volumes of transaction data from multiple sources in real-time. Kafka’s distributed architecture ensures data consistency and fault tolerance across the network.





## PostgreSQL




### What is PostgreSQL?

Postgres, short for PostgreSQL, is an advanced, open-source relational database management system (RDBMS). It is known for its robustness, extensibility, and standards compliance. Postgres supports SQL for querying and managing data, and it also provides features like:

- **ACID Compliance:** Ensures reliable transactions.
- **Complex Queries:** Supports joins, subqueries, and complex operations.
- **Extensibility:** Allows custom functions, data types, operators, and more.
- **Concurrency:** Handles multiple transactions simultaneously with high efficiency.
- **Data Integrity:** Supports constraints, triggers, and foreign keys to maintain data accuracy.



To know more, please refer to the PostgreSQL official site: https://www.postgresql.org/



### PostgreSQL in Teranode

In Teranode, PostgreSQL is used for **Blockchain Storage**. Postgres stores the blockchain data, providing a reliable and efficient database solution for handling large volumes of block data.



## Aerospike



### What is Aerospike?

Aerospike is an open-source, high-performance, NoSQL database designed for real-time analytics and high-speed transaction processing. It is known for its ability to handle large-scale data with low latency and high reliability. Key features include:

- **High Performance:** Optimized for fast read and write operations.
- **Scalability:** Easily scales horizontally to handle massive data loads.
- **Consistency and Reliability:** Ensures data consistency and supports ACID transactions.
- **Hybrid Memory Architecture:** Utilizes both RAM and flash storage for efficient data management.



### How Aerospike is Used in Teranode

In the context of Teranode, Aerospike is utilized for **UTXO (Unspent Transaction Output) storage**, which requires of a robust high performance and reliable solution.



## Shared Storage



Teranode requires a robust and scalable shared storage solution to efficiently manage its critical **Subtree** and **Transaction** data. This shared storage is accessed and modified by various microservices within the Teranode ecosystem.



In a full production deployment, Teranode is designed to use Lustre (or a similar high-performance shared filesystem) for optimal performance and scalability. Lustre is a clustered, high-availability, low-latency shared filesystem provided by AWS FSx for Lustre service.



Benefits of Using Lustre with Teranode:



- **High Availability:** Ensures continuous access to shared data.
- **Low Latency:** Provides sub-millisecond latency for consistent filesystem state.
- **Automatic Data Archiving:** Facilitates seamless data transfer to S3 for long-term storage.
- **Scalability:** Supports scalable data sharing between various services.



Note - if using Docker Compose, the shared Docker storage, which is automatically managed by `docker compose`, is used instead. This approach provides a more accessible testing environment while still allowing for essential functionality and performance evaluation.


## Grafana and Prometheus



### What is Grafana?

**Grafana** is an open-source platform used for monitoring, visualization, and analysis of data. It allows users to create and share interactive dashboards to visualize real-time data from various sources.



### What is Prometheus?

**Prometheus** is an open-source systems monitoring and alerting toolkit. It is particularly well-suited for monitoring dynamic and cloud-native environments.



Grafana and Prometheus are used together to provide a comprehensive monitoring and visualization solution. Prometheus scrapes metrics from configured targets at regular intervals, and stores them in a its time series database. Grafana then connects to Prometheus as a data source.


Users can create dashboards in Grafana to visualize the metrics collected by Prometheus. Additionally, both Prometheus and Grafana support alerting.


### Grafana and Prometheus in Teranode

In the context of Teranode, Grafana and Prometheus are used to provide comprehensive monitoring and visualization of the blockchain node’s performance, health, and various metrics.
