# 🌐 Propagation Service

## Index


## 1. Description

The `Propagation Service` is designed to handle the propagation of transactions across a peer-to-peer UBSV network.

At a glance, the Propagation service:
1. Receives new transactions through various communication methods.
2. Stores transactions in the tx store.
3. Sends the transaction to the Validator service for further processing.


![Propagation_Service_Container_Diagram.png](img%2FPropagation_Service_Container_Diagram.png)


The service implements multiple experimental alternative communication methods (e.g. DRPC, fRPC, QUIC) for transaction propagation, as well as UDP listeners over IPv6. At the time of writing, the gRPC protocol is the primary communication method.

- `StartUDP6Listeners`, `quicServer`, `drpcServer`, `frpcServer`, `StartHTTPServer`: These functions are designed to start various network listeners for different protocols like UDP, QUIC, DRPC, fRPC, and HTTP. Each function configures and starts a server to listen for incoming connections and requests on specific network addresses and ports.

A node can start multiple parallel instances of the Propagation service. This translates into multiple pods within a Kubernetes cluster. Each instance will have its own gRPC server, and will be able to receive and propagate transactions independently. GRPC load balancing allows to distribute the load across the multiple instances.

The Notice how fRPC and DRPC do not allow for load balancing.

![Propagation_Service_Component_Diagram.png](img%2FPropagation_Service_Component_Diagram.png)

## 2. Functionality

### 2.1. Starting the Propagation Service

![propagation_startup.svg](img%2Fplantuml%2Fpropagation%2Fpropagation_startup.svg)

Upon startup, the Propagation service starts the relevant communication channels, as configured via settings.

### 2.2. Propagating Transactions

All communication channels receive txs and delegate them to the `ProcessTransaction()` function. The main communication channels are shown below.

**HTTP:**

![propagation_http.svg](img%2Fplantuml%2Fpropagation%2Fpropagation_http.svg)


**gRPC:**

![propagation_grpc.svg](img%2Fplantuml%2Fpropagation%2Fpropagation_grpc.svg)


**UDP IPv6:**

![propagation_udp_ipv6.svg](img%2Fplantuml%2Fpropagation%2Fpropagation_udp_ipv6.svg)

## 3. Data Model

The Propagation Service deals with the extended transaction format, as seen below:

| Field           | Description                                                                                            | Size                                              |
|-----------------|--------------------------------------------------------------------------------------------------------|---------------------------------------------------|
| Version no      | currently 2                                                                                            | 4 bytes                                           |
| **EF marker**   | **marker for extended format**                                                                         | **0000000000EF**                                  |
| In-counter      | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of inputs  | **Extended Format** transaction Input Structure                                                        | <in-counter> qty with variable length per input   |
| Out-counter     | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of outputs | Transaction Output Structure                                                                           | <out-counter> qty with variable length per output |
| nLocktime       | if non-zero and sequence numbers are < 0xFFFFFFFF: block height or timestamp when transaction is final | 4 bytes                                           |

More information on the extended tx structure and purpose can be found in the [Architecture Documentation](docs/architecture/architecture.md).

## 4. Technology

Main technologies involved:

1. **Go Programming Language (Golang)**:
  - The entire service is written in Go.

2. **Peer-to-Peer (P2P) Networking**:
  - The service is designed for a P2P network environment, where nodes (computers) in the network communicate directly with each other without central coordination.
  - `libsv/go-p2p/wire` is used for P2P transaction propagation in the UBSV network.

3. **Networking Protocols (UDP, HTTP, QUIC, DRPC, fRPC)**:
  - The service uses various networking protocols for communication:
    - **UDP (User Datagram Protocol)**: A lightweight, connectionless protocol used for low-latency and loss-tolerating connections.
    - **HTTP (Hypertext Transfer Protocol)**.
    - **QUIC (Quick UDP Internet Connections)**: A transport layer network protocol designed by Google to improve the performance of connection-oriented web applications.
    - **DRPC**: DRPC is a lightweight, drop-in, protocol buffer-based gRPC replacement for Go.
    - **fRPC**: fRPC-go is a lightweight, fast, and secure RPC framework implemented for Go.

4. **Cryptography**:
  - The use of `crypto` packages for RSA key generation and TLS (Transport Layer Security) configuration for secure communication.

5. **gRPC and Protocol Buffers**:
  - gRPC, indicated by the use of `google.golang.org/grpc`, is a high-performance, open-source universal RPC framework. It uses Protocol Buffers as its interface definition language.


## 5. Directory Structure and Main Files

```
./services/propagation
│
├── Client.go                    - Contains the client-side logic for interacting with the propagation service.
├── Server.go                    - Contains the main server-side implementation for the propagation service.
├── StreamingClient.go           - Implementation of a client capable of handling streaming data, for real-time data processing or communication.
├── StreamingClient_test.go      - Unit tests for the `StreamingClient.go` functionality.
├── frpc.go                      - Related to the fRPC framework implementation for the propagation service.
├── metrics.go                   - Metrics collection and monitoring of the propagation service.
└── propagation_api              - Directory containing various files related to the API definition and implementation of the propagation service.
    ├── propagation_api.frpc.go         - Specific implementation file for the fRPC framework for the propagation API.
    ├── propagation_api.pb.go           - Auto-generated file from protobuf definitions, containing Go bindings for the API.
    ├── propagation_api.proto           - Protocol Buffers definition file for the propagation API.
    ├── propagation_api_drpc.pb.go      - DRPC (Distributed RPC) specific implementation file for the propagation API.
    └── propagation_api_grpc.pb.go      - gRPC (Google's RPC framework) specific implementation file for the propagation API.

```

## 6. How to run

To run the Propagation Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -Propagation=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Bootstrap Service locally.


## 7. Configuration options (settings flags)

The Propagation service uses the following configuration options:

1. **`utxostore_grpcAddress`**:
  - Used in the `Enabled()` function to check if the gRPC address for the UTXO store is set.

2. **`ipv6_addresses`**:
  - Used in the `Start()` function to retrieve IPv6 addresses for setting up UDP listeners.

3. **`propagation_httpAddress`**:
  - Used in the `Start()` function to set the HTTP address for the propagation server.

4. **`propagation_drpcListenAddress`**:
  - Used in the `Start()` function to set the address for the DRPC (Distributed RPC) server.

5. **`propagation_frpcListenAddress`**:
  - Used in the `Start()` function to set the address for the fRPC server.

6. **`propagation_quicListenAddress`**:
  - Used in the `Start()` function to set the address for the QUIC server.

7. **`ipv6_interface`**:
  - Used in the `StartUDP6Listeners()` function to find the network interface name for setting up IPv6 listeners.

8. **`propagation_frpcConcurrency`**:
  - Used in the `frpcServer()` function to set the concurrency level for the fRPC server.
