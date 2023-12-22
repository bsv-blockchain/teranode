# 🔍 TX Validator Service

## Index


## 1. Description

The `Validator` service is responsible for:
1. Receiving new transactions from the `Propagation`service,
2. Validating them,
3. Persisting the data into the tx meta store and utxo store,
4. Propagating the transactions to the `Block Assembly` service (if the Tx is passed), or notify the `P2P` service (if the tx is rejected).


![Tx_Validator_Service_Container_Diagram.png](img%2FTx_Validator_Service_Container_Diagram.png)


The `Validator` service receives notifications about new Txs through various channels - gRPC, fRPC and dRPC (where fRPC and dRPC are considered experimental).

Also, the `Validator` service will accept gRPC subscriptions from the P2P Service, where rejected tx notifications are pushed to.


![Tx_Validator_Service_Component_Diagram.png](img%2FTx_Validator_Service_Component_Diagram.png)

The Validator service notifies the Block Assembly service of new transactions through two channels: gRPC and Kafka. The gRPC channel is used for direct communication, while the Kafka channel is used for broadcasting the transaction to the `blockassembly_kafkaBrokers` topic. Either channel can be enabled or disabled through the configuration settings.

A node can start multiple parallel instances of the TX Validator service. This translates into multiple pods within a Kubernetes cluster. Each instance / pod will have its own gRPC server, and will be able to receive and process transactions independently. GRPC load balancing allows to distribute the load across the multiple instances.

## 2. Functionality

### 2.1. Starting the Validator Service

We can see here the steps in the validator (`services/validator/Server.go`)  `NewServer`, `Init`, and `Start` functions:

![tx_validator_init.svg](img%2Fplantuml%2Fvalidator%2Ftx_validator_init.svg)

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
1. Optionally, set up a DRPC server if the configuration exists.
2. Optionally, set up an fRPC server if the configuration exists.
3. Start a goroutine to manage new and dead subscriptions.
4. Start the gRPC server and handle any errors.

### 2.2. Receiving Transaction Validation Requests

The Propagation and Block Validation modules invoke the validator process in order to have new or previously missed Txs validated. The Propagation service is responsible for processing new Txs, while the Block Validation service is responsible for identifying missed Txs while processing blocks.

![tx_validation_request_process.svg](img%2Fplantuml%2Fvalidator%2Ftx_validation_request_process.svg)

BlockValidation and Propagation invoke the validator process with and without batching. Batching is settings controlled, and improves the processing performance.

1. **Transaction Propagation**:
    - The Propagation module `ProcessTransaction()` function invokes `Validate()` on the gRPC Validator client.
    - The gRPC Validator client forwards this request to the Validator Server.
    - The Validator Server, based on whether batching is enabled or disabled, either processes the transaction through a batch worker or directly validates the transaction.


2. **Block Validation**:
    - The BlockValidation module `blessMissingTransaction()` function invokes `Validate()` on the gRPC Validator client.
    - The gRPC Validator client forwards this request to the Validator Server.
    - The Validator Server follows a similar logic as above, deciding between batch processing and direct transaction validation based on the batching configuration.

It must be noted that the Block Validation and Propagation services can communicate with the Tx Validation service through other channels as well, such as fRPC and dRPC. Those altenative communication channels are considered experimental and will not be covered in detail.
Note that, as of the time of writing, fRPC and dRPC does not support batch processing. Also, they do not support load balancing (meaning that only a single transaction validator instance will be possible within each node).

### 2.3. Validating the Transaction

Once a Tx is received by the Validator Server, it is validated by the `ValidateTransaction()` function. To ensure the validity of the extended Tx, this is delegated to a BSV library: `github.com/TAAL-GmbH/arc/validator/default` (the default validator).

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

	// 6) nLocktime is equal to INT_MAX, or nLocktime and nSequence values are satisfied according to MedianTimePast
	//    => checked by the node, we do not want to have to know the current block height

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

### 2.4. Post-validation: Updating stores and propagating the transaction

Once a Tx is validated, the Validator Server will update the Tx Meta and UTXO store with the new Tx data. Then, it will notify the Block Assembly service and any P2P subscribers about the new Tx.

![tx_validation_post_process.svg](img%2Fplantuml%2Fvalidator%2Ftx_validation_post_process.svg)


- The Server receives a validation request and calls the `Validate` method on the Validator struct.
- If the transaction is valid:
   - The Validator marks the transaction's input UTXOs as spent in the UTXO Store.
   - The Validator registers the new transaction in the TX Meta Store.
   - The Validator sends the transaction to the Block Assembly Service for inclusion in a block.
   - The Validator stores the new UTXOs generated by the transaction in the UTXO Store.

- If the transaction is invalid:
   - The Server sends invalid transaction notifications to all P2P Service subscribers.
   - The rejected Tx is not stored or tracked in any store, and it is simply discarded.

We can dive deeper into the submission to the Block Assembly:

![tx_validation_block_assembly.svg](img%2Fplantuml%2Fvalidator%2Ftx_validation_block_assembly.svg)

Depending on the configuration settings, the TX Validator service can notify the Block Assembly service of new transactions in one of two ways:
1. Directly, by calling the `Store()` method on the Block Assembly client.
2. Through a Kafka topic, by sending the transaction to the `tx` topic.

Equally, we can see the submission to the P2P Service in more detail:

![tx_validation_p2p_subscribers.svg](img%2Fplantuml%2Fvalidator%2Ftx_validation_p2p_subscribers.svg)

1. **Establish gRPC Subscription**:
   - The P2P Service starts and calls its `validatorSubscriptionListener` function.
   - The Listener requests a gRPC subscription from the Validator Client.
   - The Validator Server updates its subscribers map and confirms the subscription establishment back to the Listener and P2P Service.

2. **Send Failed Transaction Notification**:
   - Upon encountering a failed transaction, the Validator Server calls the `sendInvalidTxNotification` function.
   - It loops through all the subscribers (P2P Subscribers) in its list.
   - A gRPC stream notification about the failed transaction is sent to each subscriber.


## 3. Data Model

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


## 4. Technology

The code snippet you've provided utilizes a variety of technologies and libraries, each serving a specific purpose within the context of a Bitcoin SV (BSV) blockchain-related application. Here's a breakdown of these technologies:

1. **Go (Golang)**: The programming language used for the entire codebase.

2. **gRPC**: Google's Remote Procedure Call system, used here for server-client communication. It enables the server to expose specific methods that clients can call remotely.

3. **DRPC and fRPC**: These are alternative RPC frameworks to gRPC. DRPC (developed by Storj) is designed to be simpler and more efficient than gRPC, while fRPC is a framework for creating RPC servers and clients.

4. **Kafka (by Apache)**: A distributed streaming platform (optionally) used here for message handling. Kafka is used for distributing transaction validation data to the block assembly.

5. **Sarama**: A Go library for Apache Kafka.

6. **Go-Bitcoin**: A Go library that provides utilities and tools for working with Bitcoin, including transaction parsing and manipulation.

7. **LibSV**: Another Go library for Bitcoin SV, used for transaction-related operations.

8. **Other Utilities and Libraries**:
  - `sync/atomic`, `strings`, `strconv`, `time`, `io`, `net/url`, `os`, `bytes`, and other standard Go packages for various utility functions.
  - `github.com/ordishs/gocore` and `github.com/ordishs/go-utils/batcher`: Utility libraries, used for handling core functionalities and batch processing.
  - `github.com/opentracing/opentracing-go`: Used for distributed tracing.


## 5. Directory Structure and Main Files

```
./services/validator
│
├── Client.go
│   └── Contains client-side logic for interacting with the validator service, including functions for connecting and utilizing its services.
│
├── Interface.go
│   └── Defines interfaces for the validator service, outlining the structure and methods any implementation of the validator should adhere to.
│
├── Mock.go
│   └── Provides mock implementations of the validator service, primarily used for testing and simulation purposes.
│
├── Server.go
│   └── Implements the server-side logic of the validator service, detailing the core functionalities as exposed to clients.
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
│   └── Contains code for metrics collection within the validator service, covering performance data, usage statistics, etc.
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


## 6. How to run


To run the Validator Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -Validator=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Bootstrap Service locally.


## 7. Configuration options (settings flags)

1. **validator_grpcListenAddress**: Specifies the listening address for the gRPC server.

2. **validator_kafkaBrokers**: Defines the address of the Kafka brokers. This is crucial for the application to connect to a Kafka cluster for message streaming, particularly for transaction data.

3. **validator_kafkaWorkers**: Indicates the number of workers to be used for Kafka consumers. This setting controls the level of parallelism or concurrency in processing Kafka messages.

4. **validator_drpcListenAddress**: The listening address for the DRPC server.

5. **validator_frpcListenAddress**: The listening address for the fRPC server.

6. **validator_frpcConcurrency**: Sets the concurrency level for the fRPC server.

7. **blockvalidation_txMetaCacheBatcherEnabled**: Determines whether the batcher for transaction metadata caching in the block validation process is enabled.

8. **blockvalidation_txMetaCacheBatchSize**: Specifies the batch size for transaction metadata caching in the block validation process.

9. **blockvalidation_txMetaCacheBatchTimeoutMillis**: Indicates the timeout (in milliseconds) for the batch operation in transaction metadata caching.

10. **blockassembly_disabled**: A boolean setting that indicates whether the block assembly feature is disabled.

11. **blockassembly_kafkaBrokers**: Defines the Kafka brokers' address for the block assembly process.
