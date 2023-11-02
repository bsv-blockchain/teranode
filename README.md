# 🚀 UBSV
> Unbounded Bitcoin Satoshi Vision

## Index

- [_Introduction_](#introduction)
- [_Getting Started_](#getting-started)
  - [Pre-requisites and Installation](#pre-requisites-and-installation)
  - [Running the node locally in development](#running-the-node-locally-in-development)
    - [Running the node in native mode ](#running-the-node-in-native-mode-)
    - [Running the node with Aerospike support](#running-the-node-with-aerospike-support)
    - [Running the node with custom settings](#running-the-node-with-custom-settings)
  - [Running specific services locally in development](#running-specific-services-locally-in-development)
  - [Running specific commands locally in development](#running-specific-commands-locally-in-development)
- [_Advanced usage (protoc, settings (SETTINGS_CONTEXT),  flags, etc)_  [ TO-DO ]](#advanced-usage-protoc-settings-settingscontext--flags-etc---to-do-)
  - [Makefile](#makefile)
- [_Architecture_  [ TO-DO ]](#architecture---to-do-)
- [_Technology (gRPC, Go,...)_  [ TO-DO ]](#technology-grpc-go---to-do-)
- [_Project Structure and coding conventions_  [ TO-DO ]](#project-structure-and-coding-conventions---to-do-)
  - [Coding Conventions](#coding-conventions)
- [_License_](#license)

## _Introduction_

---

The Bitcoin (BTC) _scalability issue_ refers to the challenge faced by the historical Bitcoin network in processing a large number of transactions efficiently. Originally, the Bitcoin block size, where transactions are recorded, was limited to 1 megabyte. This limitation meant that the network could only handle an average of **3.3 to 7 transactions per second**. As Bitcoin's popularity grew, this has led to delayed transaction processing and higher fees.

**UBSV** is BSV’s solution to the challenges of vertical scaling by instead spreading the workload across multiple machines. This horizontal scaling approach enables network capacity to grow with increasing demand through the addition of cluster nodes, allowing for BSV scaling to be truly unbounded.

UBSV provides a robust node processing system for BSV that can consistently handle over **1M transactions per second**.
The node has been designed as a collection of microservices, each handling specific functionalities of the BSV network.

---

## _Getting Started_

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

## _Advanced usage (protoc, settings (SETTINGS_CONTEXT),  flags, etc)_  [ TO-DO ]


#### Makefile
To make the proto files run

```make gen```

...
...
Additional detail - logs produced when run with the following environment variables set: GRPC_VERBOSITY=debug GRPC_TRACE=client_channel,round_robin


---

## _Architecture_  [ TO-DO ]

---

Diagram...

...List of services and link to specific README.md per service

....List of commands


---

## _Technology (gRPC, Go,...)_  [ TO-DO ]

---



---

## _Project Structure and coding conventions_  [ TO-DO ]

---


### Coding Conventions

For naming conventions please check the [Naming Conventions](docs/guidelines/namingConventions.md).

---


## _License_

---
**Copyright © 2023 BSV Blockchain Org. All rights reserved.**

No part of this software may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the author.

_Unauthorized duplication, distribution, or modification of this software, in whole or in part, is strictly prohibited._

---
