#  🔊 Propagation Blaster

## Index


1. [Introduction](#1-introduction)
2. [Architecture](#2-architecture)
3. [Functionality](#3-functionality)
- [3.1. Propagation Blaster Initialization and Configuration](#31-propagation-blaster-initialization-and-configuration)
- [3.2. Worker Operation](#32-worker-operation)
4. [Technology](#4-technology)
5. [Directory Structure and Main Files](#5-directory-structure-and-main-files)
6. [How to run](#6-how-to-run)


## 1. Introduction

The `PropagationBlaster` service is designed to stress-test Teranode propagation service by generating, signing, and broadcasting transactions. This service aims to evaluate the performance and reliability of different propagation methods (HTTP, gRPC, streaming) under load.


## 2. Architecture

![Propagation_Blaster_Component_Diagram.png](img%2FPropagation_Blaster_Component_Diagram.png)

- **Transaction Broadcast Modes**:

    - **HTTP**: Transactions are POSTed to a configured HTTP endpoint. This mode constructs an HTTP request for each transaction and sends it to the specified propagation service URL.

    - **gRPC**: Utilizes gRPC for transaction broadcast, establishing a connection to a gRPC server based on configured addresses and sending transactions through this channel.

    - **Disabled**: A mode where transactions are generated and processed but not actually broadcasted. This mode is useful for testing transaction generation performance without network overhead.

The service includes a Go's built-in profiler accessible via an HTTP endpoint, allowing for runtime performance analysis.

## 3. Functionality

### 3.1. Propagation Blaster Initialization and Configuration

![propagation_blaster_init.svg](img%2Fplantuml%2Fpropagation_blaster_init.svg)

1. **Initialization Phase**:
  - The service starts by loading necessary configurations, such as the profiler address and Prometheus endpoint.
  - It initializes Prometheus metrics for tracking the number of workers and the number of processed transactions.
  - An HTTP server is set up for exposing Prometheus metrics and Go's built-in profiler (`pprof`).

2. **Start Function**:
  - Parses command-line arguments to configure operational parameters like the number of workers, the broadcast protocol, and the buffer size for streaming clients.
  - Depending on the chosen broadcast protocol (`http` or `grpc`), it retrieves the corresponding service addresses from the configuration and initializes the appropriate client (HTTP or gRPC).
  - Launches a specified number of worker goroutines, each responsible for generating, signing, and broadcasting transactions.

3. **Worker Routine**:
  - Each worker continuously generates new transactions, signs them, and broadcasts them to the Propagation service using the configured protocol.

### 3.2. Worker Operation

![propagation_blaster_worker.svg](img%2Fplantuml%2Fpropagation_blaster_worker.svg)


- **Transaction Generation and Signing**:
  - Each worker starts a loop to continuously generate and process transactions. First, it generates a new, incomplete transaction. Then, it uses a transaction signer to sign the transaction, making it ready for broadcast.

- **Decision on Broadcast Protocol**:
  - The worker checks the configured broadcast protocol. Depending on the protocol setting (`http`, `grpc`, or `stream`), it follows different paths for processing the transaction.

- **HTTP Broadcast**:
  - If the broadcast protocol is set to HTTP, the worker uses an HTTP client to send the transaction to a specified endpoint. Upon receiving the HTTP response, it increments the Prometheus counter for processed transactions.

- **gRPC Broadcast**:
  - For the gRPC broadcast protocol, the worker interacts with a gRPC client to broadcast the transaction. After receiving the gRPC response, it similarly increments the Prometheus metric for processed transactions.

- **Streaming Broadcast**:
  - In this case, it uses a streaming client to send transactions.

- **Metrics Recording**:
  - Regardless of the broadcast protocol, after each transaction is processed (broadcasted or skipped), the worker increments a Prometheus counter to track the number of transactions processed.


## 4. Technology

The `PropagationBlaster` service is designed to benchmark and test the performance and reliability of transaction propagation services within the Bitcoin SV (BSV) ecosystem. It simulates the generation, signing, and broadcasting of transactions to a BSV network, providing insights into how efficiently and reliably different propagation methods handle transaction traffic. The service leverages a range of technologies and architectural patterns to achieve its goals:


- **Go (Golang)**: The service is written in Go, a statically typed programming language known for its simplicity, efficiency, and excellent support for concurrency and networking.

- **gRPC and HTTP**: For transaction broadcasting, the service supports multiple protocols, including gRPC and HTTP. gRPC is used for its high performance and efficiency in communication between microservices, utilizing HTTP/2 features like multiplexing and server push. HTTP broadcasting is supported for scenarios where gRPC may not be applicable or for compatibility with simpler network interfaces.

- **Prometheus**: Monitoring and metrics collection are handled by Prometheus, an open-source monitoring solution that provides powerful query capabilities and real-time alerting. `PropagationBlaster` uses Prometheus to track metrics such as the number of transactions processed, allowing for detailed performance analysis.

- **Bitcoin SV (BSV) Libraries**:
  - **`libsv/go-bt`**: A Go library for building and serializing Bitcoin SV transactions. It's utilized for creating the raw transactions that the service broadcasts.
  - **`libsv/go-bk/wif`**: Used for handling Wallet Import Format (WIF) keys, enabling the service to sign transactions before broadcasting them to ensure they are valid on the network.

## 5. Directory Structure and Main Files

```
cmd/propagation_blaster/
├── main.go               # The primary entry point for the PropagationBlaster service; initializes the service and starts the HTTP server for metrics and profiling.
├── main_native.go        # An alternative entry point designed for native deployment scenarios, potentially with optimizations or configurations specific to native environments.
└── propagation_blaster
└── Start.go          # Contains the core logic for initiating the PropagationBlaster service, including parsing command-line arguments, setting up propagation clients (HTTP, gRPC), and launching worker routines for transaction broadcasting.
```

## 6. How to run

To run the Propagation Blaster service locally, you can execute the following command:

```shell
cd cmd/propagation_blaster
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -broadcast=grpc -workers=10 -buffer_size=0
```

where broadcast is a value in:
* grpc
* http
* stream
