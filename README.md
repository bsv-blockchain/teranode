# 🚀 UBSV
> Unbounded Bitcoin Satoshi Vision

## Index

- [_Introduction_](#introduction)
- [_Getting Started_](#getting-started)
  - [Pre-requisites and Installation](#pre-requisites-and-installation)
  - [Running the node locally in development](#running-the-node-locally-in-development)
    - [Running the node in native mode](#running-the-node-in-native-mode)
    - [Running the node with Aerospike support](#running-the-node-with-aerospike-support)
    - [Running the node with custom settings](#running-the-node-with-custom-settings)
  - [Running specific services locally in development](#running-specific-services-locally-in-development)
  - [Running specific commands locally in development](#running-specific-commands-locally-in-development)
  - [Running UI Dashboard locally:](#running-ui-dashboard-locally)
- [_Advanced Usage_  [ TO-DO ]](#advanced-usage---to-do-)
  - [Settings](#settings)
  - [Makefile](#makefile)
    - [Proto buffers (protoc)](#proto-buffers-protoc)
    - [Running Tests](#running-tests)
  - [gRPC Logging](#grpc-logging)
- [_Architecture_  [ TO-DO ]](#architecture---to-do-)
- [_Technology_  [ TO-DO ]](#technology---to-do-)
- [_Project Structure and Coding Conventions_](#project-structure-and-coding-conventions)
  - [Project Structure](#project-structure)
  - [Directory Structure and Descriptions:](#directory-structure-and-descriptions)
  - [Coding Conventions](#coding-conventions)
  - [TODO ERROR HANDLING, LOGGING, ...???????](#todo---error-handling-logging-)
  - [Testing Conventions [TODO]](#testing-conventions---todo)
- [_License_](#license)

## _Introduction_

---

The Bitcoin (BTC) _scalability issue_ refers to the challenge faced by the historical Bitcoin network in processing a large number of transactions efficiently. Originally, the Bitcoin block size, where transactions are recorded, was limited to 1 megabyte. This limitation meant that the network could only handle an average of **3.3 to 7 transactions per second**. As Bitcoin's popularity grew, this has led to delayed transaction processing and higher fees.

**UBSV** is BSV’s solution to the challenges of vertical scaling by instead spreading the workload across multiple machines. This horizontal scaling approach enables network capacity to grow with increasing demand through the addition of cluster nodes, allowing for BSV scaling to be truly unbounded.

UBSV provides a robust node processing system for BSV that can consistently handle over **1M transactions per second**, white strictly adhering to the Bitcoin whitepaper.
The node has been designed as a collection of microservices, each handling specific functionalities of the BSV network.

---

## _Getting Started_

---

### Pre-requisites and Installation

For installation instructions please check the [Installation Guide](docs/installation.md).

### Running the node locally in development

You can run all services in 1 terminal window, using the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

_Note - Please make sure you have created the relevant settings for your username ([YOUR_USERNAME]) as part of the installation steps. If you have not done so, please review the Installation Guide link above.

_Note2 - If you use badger or sqlite as the datastore, you need to delete the data directory before running._

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

#### Running the node in native mode

In standard mode, the node will use the Go secp256k1 capabilities. However, you can enable the "native" mode, which uses the significantly faster native C secp256k1 library. Notice that this is not required or has any advantage in development mode.

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native .
```

#### Running the node with Aerospike support

If you need support for Aerospike, you need to add "aerospike" to the tags:

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native,aerospike .
```

#### Running the node with custom settings


You can start the node with custom settings by specifying which components of the node to run.

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native,aerospike . [OPTIONS]
```

Where `[OPTIONS]` are the desired components you want to start.

###### Components Options

Each component can be activated by setting its value to `1`, or disabled with `0`. Here's a table summarizing the available components:

| Component       | Option          | Description                                         |
|-----------------|-----------------|-----------------------------------------------------|
| Blockchain      | `-Blockchain=1`     | Start the Blockchain component.                      |
| Block Assembly  | `-BlockAssembly=1`  | Start the Block Assembly process.                    |
| Block Validation| `-BlockValidation=1`| Begin the Block Validation process.                  |
| Validator       | `-Validator=1`      | Activate the Validator.                              |
| Utxo Store      | `-UtxoStore=1`      | Initiate the UTXO Store.                             |
| Tx Meta Store   | `-TxMetaStore=1`    | Start the Transaction Meta Store.                    |
| Propagation     | `-Propagation=1`    | Begin the Propagation process.                       |
| Seeder          | `-Seeder=1`         | Activate the Seeder component.                       |
| Miner           | `-Miner=1`          | Start the Miner component.                           |
| Blob Server     | `-BlobServer=1`     | Initiate the Blob Server.                            |
| Coinbase        | `-Coinbase=1`       | Activate the Coinbase component.                     |
| Bootstrap       | `-Bootstrap=1`      | Start the Bootstrap process.                         |
| P2P             | `-P2P=1`            | Begin the P2P communication process.                 |
| Help            | `-help=1`           | Display the help information.                        |


###### Example:

To start the node with only `Validator`, `UtxoStore`, `Propagation`, and `Seeder` components:

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native,aerospike . -Validator=1 -UtxoStore=1 -Propagation=1 -Seeder=1
```

Note - the variable names are not case-sensitive, and can be inputted in any case. For example, `-validator=1` is the same as `-Validator=1`.


### Running specific services locally in development

Although you can run specific services using the command above, you can also run each service individually.

To do this, go to a specific service location (any directory under _service/_). Example:

```shell
cd services/validator
```

and run the service using the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

Details on each specific service can be found in their relevant documentation (see sections below  -- **LINK TO RELEVANT SECTIONS ONCE THEY EXIST** --).

### Running specific commands locally in development

Besides services, there are a number of commands that can be executed to perform specific tasks. These commands are located under _cmd/_. Example:

```shell
cd cmd/txblaster
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

Details on each specific command can be found in their relevant documentation (see sections below  -- **LINK TO RELEVANT SECTIONS ONCE THEY EXIST** --).

### Running UI Dashboard locally:

To run the UI Dashboard locally, run the following command:

```shell
make dev-dashboard
```

---

## _Advanced Usage_  [ TO-DO ]

### Settings

All services accept settings allowing local and remote servers to have their own specific configuration.

For more information on how to create and use settings, please check the [Settings Documentation](docs/settings.md).

### Makefile

The Makefile facilitates a variety of development and build tasks for the UBSV project.

Check the [Makefile Documentation](docs/makefile.md) for detailed documentation. Some use cases will be highlighted here:

#### Proto buffers (protoc)

You can generate the protobuf files by running the following command:

```shell
make gen
```

You can read more about proto buffers in the Technology section.

For additional make commands, please check the [Makefile Documentation](docs/makefile.md).

#### Running Tests

There are 2 commands to run tests:

```shell
make test  # Executes Go tests excluding the playground and PoC directories.
```

```shell
make testall  # Executes Go tests excluding the playground and PoC directories.
```

### gRPC Logging

Additional logs can be produced when the node is run with the following environment variables set: `GRPC_VERBOSITY=debug GRPC_TRACE=client_channel,round_robin`


---

## _Architecture_  [ TO-DO ]

---

Diagram...

...List of services and link to specific README.md per service

....List of commands

-- UI dashboard


---

## _Technology_  [ TO-DO ]

---


* Go
* grpc
  -- gRPC vs IPV6 multicast
  -- — https://grpc.io/docs/what-is-grpc/introduction/
* protobuf
* Stores (options)
* Docker
* Kubernetes
  * [Kubernetes Resolver for gRPC](k8sresolver/README.md)


---

## _Project Structure and Coding Conventions_

---

### Project Structure

Documenting a set of directories in Markdown can be efficiently done using a combination of nested lists and descriptions. Here's a structure that many developers find readable and straightforward:

### Directory Structure and Descriptions:

```
ubsv/
│
├── main.go                       # Start the services.
│
├── main_native.go                # Start the services in native secp256k1 mode.
│
├── Makefile                      # This Makefile facilitates a variety of development and build tasks for our project.
│
├── settings.conf                 # Global settings
│
├── settings_local.conf           # Local overridden settings
│
├── certs/                        # Project dev self-signed and ca certificates
│
├── cmd/                          # Directory containing all different Commands
│   ├── chainintegrity/           # Utility to verify the integrity of the blockchain.
│   ├── propagation_blaster/      # Utility to load test the Propagation service
│   ├── s3_blaster/               # Utility to load test the S3 service
│   ├── seeder_blaster/           # Utility to load test the Seeder service
│   ├── sutos_blaster/            # Utility to load test the SUTOS service
│   ├── txblaster_blaster/        # Utility to load test the TxBlaster service
│   └── utxostore_blaster/        # Utility to load test the UTXO Store service
│
├── data/                         # Local node data directory, as required by local databases
│
├── deploy/                       # Deployment scripts for the project (Docker, k8s, Kafka, others)
│
├── docs/                         # Documentation for the project
│
├── k8sresolver/                  # Kubernetes resolver for gRPC.
│
├── model/                        # Key model definitions for the project
│
├── native/                       # Native signature implementation for secp256k1
│
├── scripts/                      # Various scripts
│
├── services/                     # Directory containing all different Services
│   ├── blobserver/               # Blob Server Service
│   ├── blockassembly/            # Block Assembly Service
│   ├── blockchain/               # Blockchain Service
│   ├── blockvalidation/          # Block Validation Service
│   ├── bootstrap/                # Bootstrap Service
│   ├── coinbase/                 # Coinbase Service
│   ├── miner/                    # Miner Service
│   ├── p2p/                      # P2P Service
│   ├── propagation/              # Propagation Service
│   ├── seeder/                   # Seeder Service
│   ├── txmeta/                   # TXMeta Service
│   ├── utxo/                     # UTXO Service
│   └── validator/                # Validator Service
│
├── stores/                       # This directory contains the different stores used by the node.
│   ├── blob/                     # A collection of supported or experimental stores for the Blob service.
│   ├── blockchain/               # A collection of supported or experimental stores for the Blockchain service.
│   ├── txmeta/                   # A collection of supported or experimental stores for the TXMeta service.
│   └── utxo/                     # A collection of supported or experimental stores for the UTXO service.
│
├── tracing/                      # Tracing, Stats and Metric utilities
│
├── ui/
│   └── dashboard/                # Teranode Dashboard UI
│
└── util/                         # Utilities

```


### Coding Conventions

For naming conventions please check the [Naming Conventions](docs/guidelines/namingConventions.md).

### TODO - ERROR HANDLING, LOGGING, ...

xxx

### Testing Conventions - [TODO]

xxxx

---


## _License_

---
**Copyright © 2023 BSV Blockchain Org. All rights reserved.**

No part of this software may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the author.

_Unauthorized duplication, distribution, or modification of this software, in whole or in part, is strictly prohibited._

---
