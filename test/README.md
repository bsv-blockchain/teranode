# QA Guide & Instructions for Functional Requirement Tests

**Reference document:** Functional Requirement Teranode

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

To execute a given test suite (e.g., TNA, TNC, TNJ), use the following standard command template from the terminal:

```bash
cd /teranode/test/<test-suite-selected>
    SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -tags <tnXtests>
```

Where X should be replaced with the letter corresponding to the desired suite. For example:

```bash
SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -tags tnjtests
```

This command will execute all TNJ tests.

To execute a single test, use:

```bash
SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^<TestSuiteName>$/<TestName>$"
```

For example:

```bash
SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^TestTNC1TestSuite$/TestCoinbaseTXAmount$"
```

`<TestSuiteName>` is available at the end of each `tn<X>_test.go` file.

## TNA

As outlined in the Functional Requirements for Teranode reference document, the tests in the `/test/tna` folder verify the essential conditions for a node to be part of the BSV network.

The naming convention for files in this folder is as follows:

`tna_<number>.go` corresponds to the TNA-<number> test in the Functional Requirements for Teranode document.

For example:

- `tna1_test.go` covers TNA-1: Teranode must broadcast new transactions to all nodes (although new transaction broadcasts do not necessarily need to reach all nodes).
- `tna2_test.go` covers TNA-2: Teranode collects new transactions into a block.
- `tna4_test.go` covers TNA-4: Teranode must broadcast the block to all nodes when it finds a proof-of-work.

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
