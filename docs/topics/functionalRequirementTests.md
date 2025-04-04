# QA Guide & Instructions for Functional Requirement Tests

## Scope

This document explains how tests are structured within the TERANODE codebase and how they are organized within the test folder in relation to the Functional Requirements for Teranode document.

The Functional Requirements for Teranode document describes the responsibilities and features of Teranode. Accordingly, the tests are organized to verify that these responsibilities and features are correctly implemented in the code.

## Structure

The structure of both the document and the tests is as follows:

- Node responsibilities (TNA)
- Receive and validate transactions (TNB)
- Assemble blocks (TNC)
- Propagate blocks to the network (TND)
- Receive and validate blocks (TNE)
- Keep track of the longest honest chain (TNF)
- Store the blockchain (TNG)
- Store the UTXO set (TNH)
- Exchange information with mining pool software (TNI)
- Adherence to Standard Policies (Consensus Rules) (TNJ)
- Ability to define Local Policies (TNK)
- Alert System Functionality (TNL)
- Joining the Network (TNM)

Within the test folder, there is a subfolder for each of these feature areas. Each area contains its own tests, following a specific naming convention. For example, within the Node Responsibilities tests (TNA), there will be tests named TNA-1, TNA-2, and so on. Sometimes, to cover a single test case, more than one test is required. In such cases, the naming convention within the feature area will be TNC-1-TC-01, TNC-1-TC-02, and so forth.

## Test Framework

The test framework is defined in the `test_framework.go` file. This file contains the main structs and functions that the QA team uses to set up, run, and control the execution of the test suites.

## Test Suites and Setup

A custom setup for each test suite can be achieved through the structs and functions defined in `test/setup/setup.go`. In this file, the Suite object, obtained via the commonly used Go library testify, defines the `settings_local.conf` file parameters and specifies which Docker Compose setup to use when running a test suite.

## Test Execution


Prerequisites:

- Docker must be running
- Docker compose must be installed



To execute a given test suite (e.g., TNA, TNC, TNJ), use the following standard command template from the terminal:

```bash
cd /teranode/test/<test-suite-selected>
    SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -tags <tnXtests>
```

Where X should be replaced with the letter corresponding to the desired suite. For example:

```bash
go test -v -tags tnjtests
```

This command will execute all TNJ tests.

To execute a single test, use:

```bash
go test -v -run "^<TestSuiteName>$/<TestName>$"
```

For example:

```bash
go test -v -run "^TestTNC1TestSuite$/TestCoinbaseTXAmount$"
```

`<TestSuiteName>` is available at the end of each `tn<X>_test.go` file.

## TNA

As outlined in the Functional Requirements for Teranode reference document, the tests in the `/test/tna` folder verify the essential conditions that a node must follow in order to be part of the BSV network.

The naming convention for files in this folder is as follows:

`tna_<number>.go` corresponds to the TNA-<number> test in the Functional Requirements for Teranode document.

For example:

- `tna1_test.go` covers TNA-1: `Teranode must broadcast new transactions to all nodes (although new transaction broadcasts do not necessarily need to reach all nodes)`.
    - **Implementation**: The test starts three distinct Teranode instances in Docker containers. It sends 35 transactions to the first node and verifies transaction propagation by checking that at least one of these transactions appears in subtree notifications received from the network, confirming the broadcast functionality.

- `tna2_test.go` covers TNA-2: `Teranode collects new transactions into a block`.
    - **Implementation**: The test starts three Teranode instances in Docker containers and runs three sub-tests to verify transaction collection: (1) single transaction propagation - sending one transaction and verifying it appears in all nodes' block assembly, (2) multiple transaction propagation - sending 5 sequential transactions and confirming all appear in all nodes' block assembly, and (3) concurrent transaction propagation - sending 10 simultaneous transactions and verifying their presence in block assembly on all nodes.

- `tna4_test.go` covers TNA-4: `Teranode must broadcast the block to all nodes when it finds a proof-of-work`.
    - **Implementation**: The test configures three Teranode nodes and sets up notification listeners on two receiving nodes. It sends transactions to the first node, mines a block containing these transactions, and then verifies that all receiving nodes get block notifications with the correct block hash. It also confirms that each node successfully stores the block in its blockchain database, proving complete block propagation across the network.

- TNA-5: `Teranode must only accept the block if all transactions in it are valid and not already spent`
    - **Implementation**: While not explicitly implemented as a standalone test in the TNA suite, this requirement is covered by two complementary test areas:
      1. The double spend tests in `/test/double_spend/double_spend_test.go`, which verify that Teranode properly handles and rejects transactions that attempt to spend already spent outputs through multiple scenarios including:
         - Single transaction with one conflicting transaction
         - Multiple conflicting transactions in different blocks
         - Conflicting transaction chains
         - Double spend fork resolution
      2. The BDK (Block Development Kit) tests, which verify transaction validity checks
    - These tests collectively ensure that Teranode only accepts blocks containing valid, non-double-spent transactions, fulfilling the TNA-5 requirement.

- `tna6_test.go` covers TNA-6: `Teranode must express its acceptance of a block by working on creating the next block in the chain, using the hash of the accepted block as the previous hash`.
    - **Implementation**: The test instantiates three Teranode nodes, mines a block on the first node, and then verifies that the block is accepted by checking that it becomes the best block. It then retrieves a mining candidate from each node and confirms that they all use the accepted block's hash as their previous hash, demonstrating that all nodes are building on top of the accepted block.

### Running TNA Tests

#### Running All TNA Tests
```bash
cd /teranode/test/tna
go test -v -tags test_tna
```

#### Running Specific TNA Tests

```bash
cd /teranode/test/tna
go test -v -run "^TestTNA1TestSuite$/TestTNA1$"
```

Examples for each TNA test:

- For TNA-1 (transaction broadcasting):
  ```bash
  cd /teranode/test/tna
  go test -v -run "^TestTNA1TestSuite$/TestBroadcastNewTxAllNodes$" -tags test_tna
  ```

- For TNA-2 (transaction collection):
  ```bash
  cd /teranode/test/tna
  go test -v -run "^TestTNA2TestSuite$/TestTxsReceivedAllNodes$" -tags test_tna
  ```

- For TNA-4 (block broadcasting):
  ```bash
  cd /teranode/test/tna
  go test -v -run "^TestTNA4TestSuite$/TestBlockBroadcast$" -tags test_tna
  ```

- For TNA-6 (block acceptance):
  ```bash
  cd /teranode/test/tna
  go test -v -run "^TestTNA6TestSuite$/TestAcceptanceNextBlock$" -tags test_tna
  ```


## TNC

As outlined in the Functional Requirements for Teranode reference document, the tests in the `/test/tnc` folder check that transactions are correctly assembled into Merkle tree structures and consequently organized into valid blocks to propagate to mining pool software.

The naming convention for files in this folder is as follows:

`tnc_<number>.go` corresponds to the TNC-<number> test in the Functional Requirements for Teranode document.

### TNC1 Test Description

`tnc1_test.go` covers TNC-1.1, TNC-1.2, TNC-1.3: Teranode must build a proposed candidate block including all valid transactions that it is aware of.

- TNC-1.1: Teranode must calculate the Merkle root of all the transactions included in the candidate block.
- TNC-1.2: Teranode must refer to the hash of the previous block upon which the candidate block is being built.
- TNC-1.3: Teranode must add a Coinbase transaction in the block, the amount of this transaction corresponding to the sum of the block reward and the sum of all transaction fees.
- TNC-1.3-TC-01: checks balance between mining candidate coinbase value and the TotalOutputSatoshis of the coinbaseTx.
- TNC-1.3-TC-02: checks that fees are correctly calculated.

## TNJ

Section to be completed.

## TEC

As outlined in the Functional Requirements for Teranode reference document, the tests in the `/test/tec` folder check  Teranode's ability to recover from multiple types of errors, from incorrect settings to communication errors on the message channels between microservices.

The naming convention for files in this folder is as follows:

`tec_blk_<number>_test.go` corresponds to the TEC-<number> test in the Functional Requirements for Teranode document.

- TEC-BLK-1: Blockchain Service Reliability and Recoverability - Blockchain Service - Blockchain Store Failure
- TEC-BLK-2: Blockchain Service Reliability and Recoverability - Blockchain Service - UTXO Store Failure
- TEC-BLK-3: Blockchain Service Reliability and Recoverability - Blockchain Service - Block Assembly Failure
- TEC-BLK-4: Blockchain Service Reliability and Recoverability - Blockchain Service - Block Validation Failure
- TEC-BLK-5: Blockchain Service Reliability and Recoverability - Blockchain Service - P2P Service Failure
- TEC-BLK-6: Blockchain Service Reliability and Recoverability - Blockchain Service - Asset Server Failure
- TEC-BLK-7: Blockchain Service Reliability and Recoverability - Blockchain Service - Kafka Failure
