# 🌐 Propagation Service

## Index


1. [Description](#1-description)
2. [Functionality](#2-functionality)
    - [2.1. Starting the Propagation Service](#21-starting-the-propagation-service)
    - [2.1.1 Validator Integration](#211-validator-integration)
    - [2.2. Propagating Transactions](#22-propagating-transactions)
3. [gRPC Protobuf Definitions](#3-grpc-protobuf-definitions)
4. [Data Model](#4-data-model)
5. [Technology](#5-technology)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)
9. [Other Resources](#9-other-resources)


## 1. Description

The `Propagation Service` is designed to handle the propagation of transactions across a peer-to-peer Teranode network.

At a glance, the Propagation service:

1. Receives new transactions through various communication methods.
2. Stores transactions in the tx store.
3. Sends the transaction to the Validator service for further processing.


![Propagation_Service_Container_Diagram.png](img/Propagation_Service_Container_Diagram.png)


The gRPC protocol is the primary communication method, although HTTP is also accepted.

-  `StartHTTPServer`: This function is designed to start a network listener for the HTTP protocol. Each function configures and starts a server to listen for incoming connections and requests on specific network addresses and ports.

A node can start multiple parallel instances of the Propagation service. This translates into multiple pods within a Kubernetes cluster. Each instance will have its own gRPC server, and will be able to receive and propagate transactions independently. GRPC load balancing allows to distribute the load across the multiple instances.

![Propagation_Service_Component_Diagram.png](img/Propagation_Service_Component_Diagram.png)

Notice that the Validator, as shown in the diagram above, can be either a local validator or a remote validator service, depending on the Node configuration. To know more, please refer to the Transaction Validator documentation.

Also, note how the Blockchain client is used in order to wait for the node State to change to `RUNNING` state. For more information on this, please refer to the [State Management](../architecture/stateManagement.md)  documentation.

## 2. Functionality

### 2.1. Starting the Propagation Service

![propagation_startup.svg](img/plantuml/propagation/propagation_startup.svg)

Upon startup, the Propagation service starts the relevant communication channels, as configured via settings.

### 2.1.1 Validator Integration

The Propagation service can work with the Validator in two different configurations:

1. **Local Validator**:
   - When `validator.useLocalValidator=true` (recommended for production)
   - The Validator is instantiated directly within the Propagation service
   - Direct method calls are used without network overhead
   - This provides the best performance and lowest latency

2. **Remote Validator Service**:
   - When `validator.useLocalValidator=false`
   - The Propagation service connects to a separate Validator service via gRPC
   - Useful for development, testing, or specialized deployment scenarios
   - Has higher latency due to additional network calls

This configuration is controlled by the settings passed to `GetValidatorClient()` in daemon.go.

> **Note**: For detailed information about how services are initialized and connected during daemon startup, see the [Teranode Daemon Reference](../../references/teranodeDaemonReference.md#service-initialization-flow).

### 2.2. Propagating Transactions

All communication channels receive txs and delegate them to the `ProcessTransaction()` function. The main communication channels are shown below.

**HTTP:**

![propagation_http.svg](img/plantuml/propagation/propagation_http.svg)


**gRPC:**

![propagation_grpc.svg](img/plantuml/propagation/propagation_grpc.svg)



## 3. gRPC Protobuf Definitions

The Propagation Service uses gRPC for communication between nodes. The protobuf definitions used for defining the service methods and message formats can be seen [here](../../references/protobuf_docs/propagationProto.md).

## 4. Data Model

The Propagation Service deals with the extended transaction format, as seen below:

- [Extended Transaction Data Model](../datamodel/transaction_data_model.md): Include additional metadata to facilitate processing.

## 5. Technology

Main technologies involved:

1. **Go Programming Language (Golang)**:
    - The entire service is written in Go.

2. **Peer-to-Peer (P2P) Networking**:
    - The service is designed for a P2P network environment, where nodes (computers) in the network communicate directly with each other without central coordination.
    - `libsv/go-p2p/wire` is used for P2P transaction propagation in the Teranode BSV network.

3. **Networking Protocols (HTTP)**

4. **Cryptography**:
    - The use of `crypto` packages for RSA key generation and TLS (Transport Layer Security) configuration for secure communication.

5. **gRPC and Protocol Buffers**:
    - gRPC, indicated by the use of `google.golang.org/grpc`, is a high-performance, open-source universal RPC framework. It uses Protocol Buffers as its interface definition language.


## 6. Directory Structure and Main Files

```
./services/propagation
│
├── Client.go                    - Contains the client-side logic for interacting with the propagation service.
├── Server.go                    - Contains the main server-side implementation for the propagation service.
├── Server_test.go               - Unit tests for the Server.go functionality.
├── StreamingClient.go           - Implementation of a client capable of handling streaming data, for real-time data processing or communication.
├── StreamingClient_test.go      - Unit tests for the StreamingClient.go functionality.
├── metrics.go                   - Metrics collection and monitoring of the propagation service.
└── propagation_api              - Directory containing various files related to the API definition and implementation of the propagation service.
    ├── propagation_api.pb.go           - Auto-generated file from protobuf definitions, containing Go bindings for the API.
    ├── propagation_api.proto           - Protocol Buffers definition file for the propagation API.
    └── propagation_api_grpc.pb.go      - gRPC (Google's RPC framework) specific implementation file for the propagation API.
```

## 7. How to run

To run the Propagation Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -Propagation=1
```

Please refer to the [Locally Running Services Documentation](../../howto/locallyRunningServices.md) document for more information on running the Propagation Service locally.


## 8. Configuration options (settings flags)

The Propagation service uses the following configuration options:

### Daemon Level Settings

1. **`validator.useLocalValidator`**: Controls the validator deployment model used by the Propagation service.
   - When `true`: Uses a local validator instance embedded within the service (recommended for production).
   - When `false`: Connects to a remote validator service via gRPC.
   - This setting affects performance and deployment architecture across multiple services.
   - Default: `false`

### Propagation Service Settings

- **`propagation_sendBatchSize`**: Defines the batch size for sending transactions (default: 100).
- **`propagation_sendBatchTimeout`**: Sets the timeout for sending batches of transactions, likely in seconds (default: 5).
- **`grpc_resolver`**: Determines the gRPC resolver to use for client connections, supporting Kubernetes ("k8s" or "kubernetes") and potentially other resolvers.
- **`propagation_grpcAddresses`**: Lists the gRPC server addresses for the propagation service, used by the client to connect and process transactions.
- **`propagation_httpListenAddress`**: Specifies the HTTP listen address for the propagation service.
- **`fsm_state_restore`**: A boolean flag to determine if the Finite State Machine (FSM) should be restored to a previous state (default: false).
- **`kafka_validatortxsConfig`**: URL configuration for Kafka, used for validator transactions.
- **`validator_kafkaWorkers`**: Number of Kafka workers for the validator service (default: 100).
- **`propagation_grpcMaxConnectionAge`**: Maximum age of gRPC connections before they are closed and reopened (default: 90 seconds).


## 9. Other Resources

[Propagation Service Reference](../../references/services/propagation_reference.md)
