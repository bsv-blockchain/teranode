# 🔍 TX Validator

## Index

1. [Description](#1-description)
2. [Functionality](#2-functionality)
- [2.1. Starting the Validator as a Service](#21-starting-the-validator-service)
- [2.2. Receiving Transaction Validation Requests](#22-receiving-transaction-validation-requests)
- [2.3. Validating the Transaction](#23-validating-the-transaction)
- [2.4. Post-validation: Updating stores and propagating the transaction](#24-post-validation-updating-stores-and-propagating-the-transaction)
3. [gRPC Protobuf Definitions](#3-grpc-protobuf-definitions)
4. [Data Model](#4-data-model)
5. [Technology](#5-technology)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)


## 1. Description

The `Validator` (also called `Transaction Validator` or `Tx Validator`) is a go component responsible for:
1. Receiving new transactions from the `Propagation`service,
2. Validating them,
3. Persisting the data into the utxo store,
4. Propagating the transactions to the `Subtree Validation` and `Block Assembly` service (if the Tx is passed), or notify the `P2P` service (if the tx is rejected).

The Validator, as a component, is instantiated as part of any service requiring to validate transactions.
However, the Validator can also be started as a service, allowing to interact with it via gRPC, fRPC or Kafka. This setup is not recommended, given its performance overhead.

![Tx_Validator_Container_Diagram.png](img%2FTx_Validator_Container_Diagram.png)

The `Validator` receives notifications about new Txs.

Also, the `Validator` will accept subscriptions from the P2P Service, where rejected tx notifications are pushed to.

![Tx_Validator_Component_Diagram.png](img%2FTx_Validator_Component_Diagram.png)

The Validator notifies the Block Assembly service of new transactions through two channels: gRPC and Kafka. The gRPC channel is used for direct communication, while the Kafka channel is used for broadcasting the transaction to the `blockassembly_kafkaBrokers` topic. Either channel can be enabled or disabled through the configuration settings.

A node can start multiple parallel instances of the TX Validator. This translates into multiple pods within a Kubernetes cluster. Each instance / pod will have its own gRPC server, and will be able to receive and process transactions independently. GRPC load balancing allows to distribute the load across the multiple instances.

## 2. Functionality

### 2.1. Starting the Validator as a service

Should the node require to start the validator as an independent service, the `services/validator/Server.go` will be instantiated as follows:

![tx_validator_init.svg](img/plantuml/validator/tx_validator_init.svg)

* **`NewServer` Function**:
1. Initialize a new `Server` instance.
2. Create channels for new and dead subscriptions.
3. Initialize a map for subscribers.
4. Create a context for subscriptions and a corresponding cancellation function.
5. Return the newly created `Server` instance.

* **`Init` Function**:
1. Create a new validator instance within the server.
2. Handle any errors during the creation of the validator.
3. Return the outcome (success or error).

* **`Start` Function**:
1. Optionally, set up an fRPC server if the configuration exists.
2. Start a goroutine to manage new and dead subscriptions.
3. Start the gRPC server and handle any errors.

### 2.2. Receiving Transaction Validation Requests

The Propagation and Subtree Validation modules invoke the validator process in order to have new or previously missed Txs validated. The Propagation service is responsible for processing new Txs, while the Block Validation service is responsible for identifying missed Txs while processing blocks.

![tx_validation_request_process.svg](img/plantuml/validator/tx_validation_request_process.svg)

BlockValidation and Propagation invoke the validator process with and without batching. Batching is settings controlled, and improves the processing performance.

1. **Transaction Propagation**:
    - The Propagation module `ProcessTransaction()` function invokes `Validate()` on the Validator client.
    - The Validator validates the transaction.

2. **Subtree Validation**:
    - The SubtreeValidation module `blessMissingTransaction()` function invokes `Validate()` on the Validator client.
   - The Validator validates the transaction.

### 2.3. Validating the Transaction

For every transaction received, Teranode must validate:
 - All inputs against the existing UTXO-set, verifying if the input(s) can be spent,
   - Notice that if Teranode detects a double-spend, the transaction that was received first must be considered the valid transaction.
 - Bitcoin consensus rules,
 - Local policies (if any),
 - whether the script execution returns `true.

Teranode will consider a transaction that passes consensus rules, local policies and script validation as fully validated and fit to be included in the next possible block.

New Txs are validated by the `ValidateTransaction()` function. To ensure the validity of the extended Tx, this is delegated to a BSV library: `github.com/TAAL-GmbH/arc/validator/default` (the default validator).


We can see the exact steps being executed as part of the validation process below:

```go

func (v *DefaultValidator) ValidateTransaction(tx *bt.Tx) error { //nolint:funlen - mostly comments
	//
	// Each node will verify every transaction against a long checklist of criteria:
	//
	txSize := tx.Size()

	// fmt.Println(hex.EncodeToString(tx.ExtendedBytes()))

	// 0) Check whether we have a complete transaction in extended format, with all input information
	//    we cannot check the satoshi input, OP_RETURN is allowed 0 satoshis
	if !v.IsExtended(tx) {
		return validator.NewError(fmt.Errorf("transaction is not in extended format"), api.ErrStatusTxFormat)
	}

	// 1) Neither lists of inputs or outputs are empty
	if len(tx.Inputs) == 0 || len(tx.Outputs) == 0 {
		return validator.NewError(fmt.Errorf("transaction has no inputs or outputs"), api.ErrStatusInputs)
	}

	// 2) The transaction size in bytes is less than maxtxsizepolicy.
	if err := checkTxSize(txSize, v.policy); err != nil {
		return validator.NewError(err, api.ErrStatusTxFormat)
	}

	// 3) check that each input value, as well as the sum, are in the allowed range of values (less than 21m coins)
	// 5) None of the inputs have hash=0, N=–1 (coinbase transactions should not be relayed)
	if err := checkInputs(tx); err != nil {
		return validator.NewError(err, api.ErrStatusInputs)
	}

	// 4) Each output value, as well as the total, must be within the allowed range of values (less than 21m coins,
	//    more than the dust threshold if 1 unless it's OP_RETURN, which is allowed to be 0)
	if err := checkOutputs(tx); err != nil {
		return validator.NewError(err, api.ErrStatusOutputs)
	}

	// 7) The transaction size in bytes is greater than or equal to 100
	if txSize < 100 {
		return validator.NewError(fmt.Errorf("transaction size in bytes is less than 100 bytes"), api.ErrStatusMalformed)
	}

	// 8) The number of signature operations (SIGOPS) contained in the transaction is less than the signature operation limit
	if err := sigOpsCheck(tx, v.policy); err != nil {
		return validator.NewError(err, api.ErrStatusMalformed)
	}

	// 9) The unlocking script (scriptSig) can only push numbers on the stack
	if err := pushDataCheck(tx); err != nil {
		return validator.NewError(err, api.ErrStatusMalformed)
	}

	// 10) Reject if the sum of input values is less than sum of output values
	// 11) Reject if transaction fee would be too low (minRelayTxFee) to get into an empty block.
	if err := checkFees(tx, api.FeesToBtFeeQuote(v.policy.MinMiningTxFee)); err != nil {
		return validator.NewError(err, api.ErrStatusFees)
	}

	// 12) The unlocking scripts for each input must validate against the corresponding output locking scripts
	if err := checkScripts(tx); err != nil {
		return validator.NewError(err, api.ErrStatusUnlockingScripts)
	}

	// everything checks out
	return nil
}
```

The above represents an implementation of the core Teranode validation rules:

- All transactions must exist and be unspent (does not apply to Coinbase transactions).

- All transaction inputs must have been present in either a transaction in an ancestor block, or in a transaction in the same block that is located before the transaction being validated.

- All transaction inputs must not have been spent by any transaction in ancestor blocks, or by any transaction in the same block that is not located after the transaction being validated

- The length of the script (scriptSig) in the Coinbase transaction must be between 2 and (including) 100 bytes.

- The transaction must be syntactically valid:

- A transaction must have at least one input

- A transaction must have at least one output

- The amount of satoshis in all outputs must be less than or equal to the amount of satoshis in the inputs, to avoid new BSV being introduced to the network

- The amount in all outputs must be between 0 and 21,000,000 BSV (2.1 * 10^15 satoshi)

- The amount of all inputs must be between 0 and 21,000,000 BSV. The sum of the amount over all inputs must not be larger than 21,000,000 BSV.

- A transaction must be final, meaning that either of the following conditions is met:

  - The sequence number in all inputs is equal to 0xffffffff, or

  - The lock time is:

    - Equal to zero, or

    - <500000000 and smaller than block height, or >=500000000 and SMALLER THAN TIMESTAMP

    - Note: This means that Teranode will deem non-final transactions invalid and REJECT these transactions. It is up to the user to create proper non-final transactions to ensure that Teranode is aware of them. For clarity, if a transaction has a locktime in the future, the Tx Validator will reject it.

  - No output must be Pay-to-Script-Hash (P2SH)

  - A new transaction must not have any output which includes P2SH, as creation of new P2SH transactions is not allowed.

  - Historical P2SH transactions (if any) must still be supported by Teranode, allowing these transactions to be spent.

  - A transaction must not spend frozen UTXOs (see 3.13 – Integration with Alert System)

  - A node must not be able to spend a confiscated (re-assigned) transaction until 1,000 blocks after the transaction was re-assigned (confiscation maturity). The difference between block height and height at which the transaction was re-assigned must not be less than one thousand.



### 2.4. Post-validation: Updating stores and propagating the transaction

Once a Tx is validated, the Validator will update the UTXO store with the new Tx data. Then, it will notify the Block Assembly service and any P2P subscribers about the new Tx.

![tx_validation_post_process.svg](img/plantuml/validator/tx_validation_post_process.svg)


- The Server receives a validation request and calls the `Validate` method on the Validator struct.
- If the transaction is valid:
   - The Validator marks the transaction's input UTXOs as spent in the UTXO Store.
   - The Validator registers the new transaction in the UTXO Store.
   - The Validator sends the transaction to the Subtree Validation Service, either via Kafka or gRPC batches.
   - The Validator sends the transaction to the Block Assembly Service for inclusion in a block.
   - The Validator stores the new UTXOs generated by the transaction in the UTXO Store.

- If the transaction is invalid:
   - The Server sends invalid transaction notifications to all P2P Service subscribers.
   - The rejected Tx is not stored or tracked in any store, and it is simply discarded.


We can see the submission to the Subtree Validation Service here:

![tx_validation_subtree_validation.svg](img/plantuml/validator/tx_validation_subtree_validation.svg)

We can dive deeper into the submission to the Block Assembly:

![tx_validation_block_assembly.svg](img/plantuml/validator/tx_validation_block_assembly.svg)

Depending on the configuration settings, the TX Validator can notify the Block Assembly service of new transactions in one of two ways:
1. Directly, by calling the `Store()` method on the Block Assembly client.
2. Through a Kafka topic, by sending the transaction to the `tx` topic.

When the TX Validator Service is running in the node, the P2P Service will subscribe to it. We can see this in more detail here:

![tx_validation_p2p_subscribers.svg](img/plantuml/validator/tx_validation_p2p_subscribers.svg)

1. **Establish gRPC Subscription**:
   - The P2P Service starts and, if the `useLocalValidator` is false, it calls its `validatorSubscriptionListener` function.
   - The Listener requests a gRPC subscription from the Validator Client.
   - The Validator updates its subscribers map and confirms the subscription establishment back to the Listener and P2P Service.

2. **Send Failed Transaction Notification**:
   - Upon encountering a failed transaction, the Validator calls the `sendInvalidTxNotification` function.
   - It loops through all the subscribers (P2P Subscribers) in its list.
   - A gRPC stream notification about the failed transaction is sent to each subscriber.



## 3. gRPC Protobuf Definitions

The Validator, when run as a service, uses gRPC for communication between nodes. The protobuf definitions used for defining the service methods and message formats can be seen [here](../references/protobuf_docs/validatorProto.md).

## 4. Data Model

The Validation Service deals with the extended transaction format, as seen below:

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


## 5. Technology

The code snippet you've provided utilizes a variety of technologies and libraries, each serving a specific purpose within the context of a Bitcoin SV (BSV) blockchain-related application. Here's a breakdown of these technologies:

1. **Go (Golang)**: The programming language used for the entire codebase.

2. **gRPC**: Google's Remote Procedure Call system, used here for server-client communication. It enables the server to expose specific methods that clients can call remotely.

3. **fRPC**: These are alternative RPC frameworks to gRPC. fRPC is a framework for creating RPC servers and clients.

4. **Kafka (by Apache)**: A distributed streaming platform (optionally) used here for message handling. Kafka is used for distributing transaction validation data to the block assembly.

5. **Sarama**: A Go library for Apache Kafka.

6. **Go-Bitcoin**: A Go library that provides utilities and tools for working with Bitcoin, including transaction parsing and manipulation.

7. **LibSV**: Another Go library for Bitcoin SV, used for transaction-related operations.

8. **Other Utilities and Libraries**:
  - `sync/atomic`, `strings`, `strconv`, `time`, `io`, `net/url`, `os`, `bytes`, and other standard Go packages for various utility functions.
  - `github.com/ordishs/gocore` and `github.com/ordishs/go-utils/batcher`: Utility libraries, used for handling core functionalities and batch processing.
  - `github.com/opentracing/opentracing-go`: Used for distributed tracing.


## 6. Directory Structure and Main Files

```
./services/validator
│
├── Client.go
│   └── Contains client-side logic for interacting with the Validator, including functions for connecting and utilizing its services.
│
├── Interface.go
│   └── Defines interfaces for the Validator, outlining the structure and methods any implementation of the validator should adhere to.
│
├── Mock.go
│   └── Provides mock implementations of the Validator, primarily used for testing and simulation purposes.
│
├── Server.go
│   └── Implements the server-side logic of the Validator, detailing the core functionalities as exposed to clients.
│
├── Server_test.go
│   └── Contains tests for the server-side logic implemented in Server.go, ensuring expected behavior and functionality.
│
├── Validator.go
│   └── Contains the main logic for validator functionalities, including the business logic for transaction validation.
│
├── Validator_test.go
│   └── Includes unit tests for the Validator.go code, ensuring correctness of the validator logic.
│
├── frpc.go
│   └── Implements functionalities related to fRPC (Fast Remote Procedure Call), including server setup and request handling.
│
├── metrics.go
│   └── Contains code for metrics collection within the Validator, covering performance data, usage statistics, etc.
│
└── validator_api
    │
    ├── validator_api.frpc.go
    │   └── Contains Fast RPC specific code, auto-generated from the validator_api.proto file.
    │
    ├── validator_api.pb.go
    │   └── Auto-generated Go code from validator_api.proto, defining structs and functions for gRPC requests and responses.
    │
    ├── validator_api.proto
    │   └── The Protocol Buffers definition file for the validator API, outlining data structures and available RPC methods.
    │
    ├── validator_api_drpc.pb.go
    │   └── Contains DRPC (Decentralized RPC) specific code, also generated from the validator_api.proto file.
    │
    └── validator_api_grpc.pb.go
        └── Auto-generated gRPC specific code from the validator_api.proto file, detailing the gRPC server and client interfaces.

```


## 7. How to run


To run the Validator locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -Validator=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Validator locally.


## 8. Configuration options (settings flags)

1. **`validator_grpcAddress`**: Specifies the address for the validator's gRPC server to connect to.
2. **`use_open_tracing`**: Enables OpenTracing for gRPC client connections.
3. **`use_prometheus_grpc_metrics`**: Enables Prometheus metrics for gRPC client connections.
4. **`blockassembly_disabled`**: Indicates whether the block assembly feature is disabled.
5. **`kafka_txsConfig`**: URL for the Kafka configuration for transaction messages.
6. **`blockassembly_kafkaWorkers`**: Number of workers for Kafka related to block assembly.
7. **`kafka_txmetaConfig`**: URL for the Kafka configuration for transaction metadata.
8. **`blockvalidation_kafkaWorkers`**: Number of workers for Kafka related to block validation.
9. **`grpc_resolver`**: Determines the gRPC resolver to use, supporting Kubernetes with "k8s" or "kubernetes" options for service discovery.
10. **`validator_sendBatchSize`**: Specifies the size of batches for sending validation requests to the validator gRPC server.
11. **`validator_sendBatchTimeout`**: Sets the timeout in milliseconds for batching validation requests before sending them to the validator.
12. **`validator_sendBatchWorkers`**: Configures the number of workers for processing batches of validation requests.
