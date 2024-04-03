#  🔊 TX Blaster

## Index

1. [Introduction](#1-introduction)
2. [Architecture](#2-architecture)
3. [Functionality](#3-functionality)
- [3.1. TX Blaster init and workers management](#31-tx-blaster-init-and-workers-management)
- [3.2. Worker Logic](#32-worker-logic)
4. [Technology](#4-technology)
5. [Directory Structure and Main Files](#5-directory-structure-and-main-files)
6. [How to run](#6-how-to-run)


## 1. Introduction

The TX Blaster service is a utility service designed to simulate a high volume of transactions on the **test** Teranode network. The utility generates transactions and sends them to the network for processing.

This is intended for use in the test and staging networks, and it is not allowed in production (_mainnet_).

## 2. Architecture

![TX_Blaster_Container_Diagram.png](img%2FTX_Blaster_Container_Diagram.png)

The TX Blaster interacts with three (3) services, namely:
1. **Coinbase**: For acquiring initial funds for transactions.
2. **Propogation**: For transaction propagation across the network.
3. **P2P**: For listening and tracking rejected transactions.

![TX_Blaster_Component_Diagram.png](img%2FTX_Blaster_Component_Diagram.png)


The service is composed of two main differentiated components: the worker package for processing transactions and a main application (TX Blaster) that orchestrates these workers.

The TX Blaster (main) configures global settings, initializes logging, and parses command-line flags to determine operation modes, such as the number of workers, rate limiting, and Kafka settings.
* Additionally, it initializes and starts a specified number of worker goroutines for transaction processing, each in its own context to ensure isolated error handling and graceful termination.
* The workers are staggered to start at different times to distribute the load on external services evenly.
* If configured, the application sets up connections to Kafka for message passing and configures IPv6 multicast for network broadcasting.
* It integrates with Coinbase for initial fund acquisition for transactions and utilizes a custom distribution service for propagating transactions across the network.

The workers are the core of the transaction processing logic:
* Each worker is responsible for generating and sending transactions to the network.
* They manage connections to blockchain services (like Coinbase and a P2P network for transaction propagation) and Kafka producers for messaging.


## 3. Functionality

## 3.1. TX Blaster init and workers management

The TX Blaster service starts by initializing the global settings, logging, and parsing command-line flags to determine operation modes, such as the number of workers, rate limiting, and Kafka settings.

![tx_blaster_main.svg](img%2Fplantuml%2Ftx_blaster_main.svg)

This diagram breaks down the process into four main sections:

1. **Initialization**: `txblaster` starts by parsing command-line arguments and loading configurations. It then sets up connections to external services (particularly the P2P networks), and prepares for graceful shutdowns by setting up signal handling.

2. **Worker Setup and Start**: Based on the loaded configurations and command-line arguments, `txblaster` initializes the specified number of worker goroutines. Each worker establishes connections and subscribes to necessary external services (propagation service for submission of transactions, p2p for reception of rejected transaction and coinbase for reception of newly minted coinbase txs).

3. **Running**: `txblaster` manages the workers, which involves monitoring their health and restarting them if necessary.

4. **Shutdown Signal**: Upon receiving a shutdown signal (SIGINT/SIGTERM), `txblaster` notifies all running workers to initiate a graceful shutdown process. This includes disconnecting from external services, cleaning up resources, and finally, the main application exits.

## 3.2. Worker Logic

The Workers task is to generate and send transactions to the network. To do this, they first acquire funds from Coinbase (out of the existing spendable UTXOs), then generate transactions out of the output UTXOs, in a loop. i.e. for each placed transaction, the resulting output UTXOs will be used as seed for the next iteration of transactions to send, in an endless loop.

![tx_blaster_worker.svg](img%2Fplantuml%2Ftx_blaster_worker.svg)

To illustrate the initialization and start function of a worker within the `txblaster` system using PlantUML, we focus on detailing the sequence of actions from the creation of a worker instance to the processing of transactions, including error handling and interaction with external services for transaction distribution.

1. **Initialization (`Init`)**: The worker is instantiated with specific configurations, including rate limits and connections to services such as the Coinbase for initial funding. The `Init` function is responsible for setting up the worker, requesting initial funds (UTXO) from the Coinbase service, and preparing this UTXO for use in generating a first new transaction.

2. **Start (Transaction Processing)**: The worker enters a loop where the worker generates new transactions from the available UTXOs, sends these transactions to the propagation service for propagation across the blockchain, and handles acknowledgments. The sent Txs output UTXOs are used as seed for the next iteration of transaction processing, in an endless loop. If rate limiting is enabled, the worker will pace the transaction generation accordingly.

3. **Graceful Shutdown**: Upon receiving a signal for a graceful shutdown, the worker stops processing new transactions and updates metrics one last time before shutting down.

Additionally, although not illustrated in the diagram above, the worker will listen for P2P notifications about rejected Txs, in order to monitor the achieved throughput.

## 4. Technology

The entire codebase is written in Go.

The application uses gRPC or Quic for communication with the propagation service, depending on the configuration.


## 5. Directory Structure and Main Files

```
.
├── main.go               # Main entry point for the application; initializes and starts the txblaster service.
├── main_native.go        # Alternative entry point optimized for native execution, potentially for specific deployment environments.
├── txblaster
│   └── Start.go          # Contains the Start function; orchestrates the initialization and startup logic of the txblaster application.
└── worker
    ├── rollingCache.go       # Implements a rolling cache; used by workers to manage and access recent transactions efficiently.
    ├── rollingCache_test.go  # Unit tests for rollingCache.go; ensures the rolling cache behaves as expected under various scenarios.
    └── worker.go             # Defines the worker logic for processing transactions, including creation, distribution, and interaction with blockchain networks.
```

## 6. How to run

To run the TX Blaster service locally, you can execute the following command:

```shell
cd cmd/txblaster
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -P2P=1 -workers=10 -print=0 -profile=:9092 -log=0 -limit=1000 --quic=false
```
