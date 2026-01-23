# QA Guide & Instructions for Functional Requirement Tests

## Scope

This document explains how tests are structured within the TERANODE codebase and how they are organized within the test folder in relation to the Functional Requirements for Teranode document.

The Functional Requirements for Teranode document describes the responsibilities and features of Teranode. Accordingly, the tests are organized to verify that these responsibilities and features are correctly implemented in the code.

## Structure

The functional requirements are organized into the following categories:

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
- Technical Errors (TEC)

## Test Organization

Tests are organized in two main locations:

1. **Dedicated test folders** (`/test/[category]/`):

    - `/test/tna/` - Node responsibilities tests
    - `/test/tnb/` - Transaction validation tests
    - `/test/tnd/` - Block propagation tests
    - `/test/tnf/` - Longest chain tracking tests
    - `/test/tnj/` - Consensus rules tests
    - `/test/tec/` - Error handling tests

2. **Integration tests** (`/test/e2e/daemon/`):

    - `tnc*_test.go` - Block assembly tests
    - `tne*_test.go` - Block validation tests
    - Additional integration tests for various requirements

Test files follow a naming convention: `tn[x][number]_test.go` where x is the category letter and number corresponds to the specific requirement.

## Test Framework

The test framework is built on the testify suite package and is defined primarily in `test/utils/arrange.go` and `test/utils/testenv.go`. These files contain the main structs and functions used to set up, run, and control the execution of the test suites.

## Test Suites and Setup

The test framework provides two main components:

- **TeranodeTestSuite** (`test/utils/arrange.go`): Base test suite that embeds testify's suite.Suite and provides common test setup/teardown
- **TeranodeTestEnv** (`test/utils/testenv.go`): Manages the test environment including Docker Compose stacks, node clients, and service connections

Each test suite can customize its configuration through the TConfig system, which defines settings contexts and Docker Compose files to use when running tests.

## Test Execution

Prerequisites:

- Docker must be running
- Docker compose must be installed

To execute a given test suite (e.g., TNA, TNB, TNJ), use the following standard command template from the terminal:

```bash
cd /teranode/test/<test-suite-folder>
go test -v -tags test_tn[x]
```

Where [x] should be replaced with the letter corresponding to the desired suite. For example:

```bash
# For TNA tests
cd /teranode/test/tna
go test -v -tags test_tna

# For TNJ tests
cd /teranode/test/tnj
go test -v -tags test_tnj
```

**Note**: Some tests don't require build tags and can be run directly:

```bash
# For TNC tests in e2e/daemon
cd /teranode/test/e2e/daemon
go test -v -run "^TestCoinbaseTXAmount$"
```

To execute a single test, use:

```bash
# For tests using testify suites
go test -v -run "^<TestSuiteName>$/^<TestName>$" -tags <build_tag>

# For standalone tests
go test -v -run "^<TestName>$"
```

For example:

```bash
# Suite-based test (TNA)
go test -v -run "^TestTNA1TestSuite$/TestBroadcastNewTxAllNodes$" -tags test_tna ./test/tna/tna1_test.go

# Standalone test (TNC in e2e/daemon)
go test -v -run "^TestCoinbaseTXAmount$" ./test/e2e/daemon/tnc1_3_test.go
```

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
    - **Implementation**: This requirement is covered extensively by the `test/sequentialtest/double_spend/double_spend_test.go` file, which contains comprehensive tests for double-spend detection and handling across multiple storage backends (SQLite, Postgres, and Aerospike). These tests verify:
        1. **Single Double-Spend Detection**: Tests that Teranode can detect and properly handle simple double-spend attempts.
        2. **Multiple Conflicting Transactions**: Verifies detection of multiple transactions conflicting with each other across different blocks.
        3. **Transaction Chain Conflicts**: Tests handling of entire chains of transactions that conflict with each other.
        4. **Double-Spend Forks**: Tests complex scenarios with competing chain forks containing conflicting transactions.
        5. **Chain Reorganization**: Verifies proper handling of transaction status during chain reorganizations.
        6. **Triple-Forked Chains**: Tests advanced scenarios with three competing chain forks.
        7. **Nested Transaction Conflicts**: Ensures correct handling of nested transaction dependencies in double-spend scenarios.
        8. **Frozen Transaction Handling**: Tests interaction of double-spends with frozen transactions.

        The tests confirm that Teranode correctly identifies double-spend attempts, properly marks conflicting transactions, and maintains UTXO integrity throughout chain reorganizations. Additional transaction validity checks are provided by the Block Development Kit (BDK) tests.
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

## TNB

As outlined in the Functional Requirements for Teranode reference document, the tests in the `/test/tnb` folder verify that transactions are correctly validated and processed within Teranode. These tests ensure that transaction validation rules are correctly enforced and that the UTXO set is properly maintained.

The naming convention for files in this folder is as follows:

`tnb<number>_test.go` corresponds to the TNB-<number> test in the Functional Requirements for Teranode document.

### TNB1 Test Description

#### TNB-1: Transaction Processing

`tnb1_test.go` covers TNB-1: Teranode must receive transactions sent by any node and add them to its mempool if they are valid.

- **Implementation**: The test, `TestSendTxsInBatch`, creates and sends a batch of transactions concurrently to a Teranode node. It then subscribes to blockchain notifications to receive subtree notifications, retrieves transaction hashes from the subtree, and verifies that all sent transactions are included in the subtree. The test ensures that transactions are correctly received, validated, and included in mining candidates, demonstrating that Teranode properly processes transactions in batches and maintains them for block assembly.

### TNB2 Test Description

#### TNB-2: Transaction Validation

`tnb2_test.go` covers TNB-2: Teranode must validate transactions to ensure they follow consensus rules.

- **Implementation**: The test includes multiple test cases:

    1. **TestUTXOValidation**: Verifies that Teranode correctly validates transaction inputs against the UTXO set by creating a transaction using coinbase funds, sending it to a node, mining a block to confirm the transaction, and then attempting to double-spend the same UTXO. The test ensures that the double-spend is rejected, confirming that Teranode properly tracks and validates UTXO spending.
    2. **TestScriptValidation**: Validates that Teranode correctly verifies transaction scripts by creating both valid and invalid signature scenarios. It creates a transaction with an invalid signature (using a different private key than the one that controls the UTXO) and verifies that Teranode rejects it, ensuring proper script execution and validation.

##### Additional Network Policy and Integration Tests

In addition to the functional test suite, there are two important test files in the `services/validator` directory that provide comprehensive testing of transaction validation:

1. **TxValidator_test.go**: Tests transaction validation network policies:

    - **MaxTxSizePolicy**: Tests that transactions exceeding the maximum size are rejected.
    - **MaxOpsPerScriptPolicy**: Verifies enforcement of operation count limits in scripts.
    - **MaxScriptSizePolicy**: Ensures scripts exceeding the maximum size are rejected.
    - **MaxTxSigopsCountsPolicy**: Tests enforcement of signature operation count limits.
    - **MinFeePolicy**: Validates that transactions with insufficient fees are rejected based on their size and the presence of OP_RETURN data.

2. **Validator_test.go**: Tests integration of the validator with other Teranode components:

    - **TestValidate_CoinbaseTransaction**: Verifies correct handling of coinbase transactions.
    - **TestValidate_BlockAssemblyAndTxMetaChannels**: Tests the integration between transaction validation and block assembly.
    - **TestValidate_RejectedTransactionChannel**: Ensures rejected transactions are properly handled and reported.
    - **TestValidate_BlockAssemblyError**: Verifies proper error handling during the block assembly process.
    - **Transaction-specific tests**: Contains tests for several specific real-world transactions to ensure they validate correctly, including edge cases like non-zero OP_RETURN outputs and complex script execution.

Together, these tests ensure that Teranode maintains proper network health by enforcing transaction validation rules and correctly integrating with the block assembly process, UTXO management, and Kafka messaging. They provide critical protection against potential denial-of-service vectors and ensure that transactions are properly processed throughout the entire validation pipeline.

### TNB6 Test Description

#### TNB-6: UTXO Set Management

`tnb6_test.go` covers TNB-6: All outputs of validated transactions must be added to Teranode's unspent transaction output (UTXO) set.

- **Implementation**: The `TestUnspentTransactionOutputs` test case verifies proper UTXO set management by creating a transaction with multiple outputs, sending it to a node, and then verifying that all outputs are correctly added to the UTXO set. The test checks UTXO metadata (amounts, scripts) and verifies the spending status of each output, ensuring that the UTXO store correctly maintains all transaction outputs for future spending.

### TNB7 Test Description

#### TNB-7: Transaction Input Spending

`tnb7_test.go` covers TNB-7: All inputs of validated transactions must be marked as spent in Teranode's UTXO set.

- **Implementation**: The `TestValidatedTxShouldSpendInputs` test case creates a transaction that spends a coinbase output, sends it to a node, and then verifies that the input is correctly marked as spent in the UTXO store. It checks the spending status and confirms that the spending transaction ID is correctly recorded, ensuring that Teranode properly updates the UTXO set when transactions are processed.

### Running TNB Tests

#### Running All TNB Tests

```bash
cd /teranode/test/tnb
go test -v -tags test_tnb
```

#### Running Specific TNB Tests

```bash
cd /teranode/test/tnb
go test -v -run "^TestTNB1TestSuite$/TestSendTxsInBatch$" -tags test_tnb
```

Examples for each TNB test:

- For TNB-1 (transaction processing):

  ```bash
  cd /teranode/test/tnb
  go test -v -run "^TestTNB1TestSuite$/TestSendTxsInBatch$" -tags test_tnb
  ```

- For TNB-2 (transaction validation):

  ```bash
  cd /teranode/test/tnb
  go test -v -run "^TestTNB2TestSuite$/TestUTXOValidation$" -tags test_tnb
  ```

- For TNB-6 (UTXO set management):

  ```bash
  cd /teranode/test/tnb
  go test -v -run "^TestTNB6TestSuite$/TestUnspentTransactionOutputs$" -tags test_tnb
  ```

- For TNB-7 (transaction input spending):

  ```bash
  cd /teranode/test/tnb
  go test -v -run "^TestTNB7TestSuite$/TestValidatedTxShouldSpendInputs$" -tags test_tnb
  ```

## TNC

As outlined in the Functional Requirements for Teranode reference document, the TNC tests verify that transactions are correctly assembled into Merkle tree structures and consequently organized into valid blocks to propagate to mining pool software.

**Note**: TNC tests are located in `/test/e2e/daemon/` rather than a dedicated `/test/tnc/` folder, as they require full daemon integration.

The naming convention for these test files is:

`tnc<number>_<subnumber>_test.go` corresponds to the TNC-<number>.<subnumber> test in the Functional Requirements for Teranode document.

### TNC1 Test Description

#### TNC-1.1: Merkle Root Calculation

`tnc1_1_test.go` covers TNC-1.1: Teranode must calculate the Merkle root of all the transactions included in the candidate block.

- **Implementation**: The test verifies the Merkle root calculation by first obtaining a mining candidate with no additional transactions. It then mines a block and retrieves the best block. The test validates that the block properly validates its subtrees and that the Merkle root is correctly calculated. This ensures that Teranode is capable of properly calculating the Merkle root for blocks, even in the simplest case of a coinbase-only block.

#### TNC-1.2: Previous Block Hash Reference

`tnc1_2_test.go` covers TNC-1.2: Teranode must refer to the hash of the previous block upon which the candidate block is being built.

- **Implementation**: The test consists of two test cases:

    1. **TestCheckPrevBlockHash**: This test sends transactions, mines a block, and then verifies that the mining candidate's previous hash reference matches the current best block hash. This ensures that new blocks are properly built on top of the current chain.
    2. **TestPrevBlockHashAfterReorg**: This test creates a chain reorganization scenario by mining blocks on different nodes to create a longer chain. It then verifies that the mining candidate from the first node correctly updates to reference the tip of the longer chain. This confirms that Teranode properly handles chain reorganizations when building candidate blocks.

#### TNC-1.3: Coinbase Transaction

`tnc1_3_test.go` covers TNC-1.3: Teranode must add a Coinbase transaction in the block, the amount of this transaction corresponding to the sum of the block reward and the sum of all transaction fees.

- **Implementation**: The test contains multiple test cases:

    1. **TestCandidateContainsAllTxs**: This test subscribes to blockchain notifications, sends multiple transactions, and verifies that the mining candidate contains all these transactions by checking subtree notifications and comparing Merkle proofs across different nodes.
    2. **TestCheckHashPrevBlockCandidate**: A test that verifies the previous block hash is correctly referenced in the candidate block.
    3. **TestCoinbaseTXAmount (TNC-1.3-TC-01)**: This test case specifically verifies the balance between the mining candidate coinbase value and the total output satoshis of the coinbase transaction, ensuring that the block reward is correctly calculated.
    4. **TestCoinbaseTXAmount2 (TNC-1.3-TC-02)**: This test case focuses on fee calculation, ensuring that the fees from all transactions are correctly included in the coinbase amount.

### TNC2 Test Description

#### TNC-2.1: Unique Candidate Identifiers

`tnc2_1_test.go` covers TNC-2.1: Each candidate block must have a unique identifier.

- **Implementation**: The test contains two test cases:

    1. **TestUniqueCandidateIdentifiers**: This test obtains multiple mining candidates and verifies they have unique identifiers, both within the same height and across different block heights after mining. This ensures that each candidate has a proper identifier for tracking.
    2. **TestConcurrentCandidateIdentifiers**: This test performs concurrent requests for mining candidates and verifies that all returned candidates have unique identifiers even under load. It creates 10 concurrent requests and checks that all returned IDs are unique, ensuring the candidate generation process maintains uniqueness under concurrent conditions.

### Additional Block Assembly System Tests

In addition to the functional test suite, `services/blockassembly/blockassembly_system_test.go` provides comprehensive system-level testing that covers aspects of both TNC and TNA requirements. These tests examine the block assembly process in an integrated system environment:

- **Test_CoinbaseSubsidyHeight**: Verifies correct coinbase subsidy calculation at different block heights, ensuring proper reward halving and fee handling.

- **TestDifficultyAdjustment**: Tests the difficulty adjustment mechanism to ensure blocks maintain the proper difficulty target.

- **TestShouldFollowLongerChain**: Verifies chain selection logic, ensuring the node correctly follows the chain with the most proof-of-work.

- **TestShouldFollowChainWithMoreChainwork**: Explicitly tests TNA-3 (proof-of-work) by verifying that the node properly calculates and follows the chain with the most accumulated work.

- **TestShouldAddSubtreesToLongerChain**: Tests both TNA-3 (proof-of-work) and TNA-6 (block acceptance), verifying that subtrees are correctly added to the chain and that blocks reference the proper previous hash.

- **TestShouldHandleReorg** and **TestShouldHandleReorgWithLongerChain**: Verify blockchain reorganization handling, ensuring the node can properly reorganize its chain when a better chain appears.

- **TestShouldFailCoinbaseArbitraryTextTooLong**: Tests validation of coinbase size policy, ensuring blocks with oversized coinbase transactions are rejected.

- **TestReset**: Verifies that the block assembler can properly reset its state and continue operation after a system event.

These system tests provide a more integrated view of how block assembly functions within the complete Teranode system, complementing the more focused functional tests in the TNC directory.

### Additional Unit Tests for Block Assembly

In addition to the TNC functional tests and system tests, `services/blockassembly/BlockAssembler_test.go` provides unit-level testing for the core block assembly component, covering key TNC requirements:

- **TestBlockAssembly_GetMiningCandidate**: Tests the creation of mining candidates with proper previous block hash references (TNC-1.2), correct Merkle proof/root calculation (TNC-1.1), and accurate coinbase value calculation including transaction fees (TNC-1.3).

- **TestBlockAssembly_ShouldNotAllowMoreThanOneCoinbaseTx**: Verifies that the block assembler properly enforces the rule that only one coinbase transaction is allowed per block.

- **TestBlockAssembly_GetMiningCandidate_MaxBlockSize**: Tests block size constraints and transaction selection when assembling candidate blocks.

These unit tests provide additional verification of the core block assembly functionality at a lower level than the functional tests, ensuring that the individual components work correctly before they're integrated into the complete system.

## TND

As outlined in the Functional Requirements for Teranode reference document, the tests in the `/test/tnd` folder verify the block propagation functionality between nodes in the network.

The naming convention for files in this folder is as follows:

`tnd<number>_<subnumber>_test.go` corresponds to the TND-<number>.<subnumber> test in the Functional Requirements for Teranode document.

### TND1 Test Description

#### TND-1.1: Block Propagation

`tnd1_1_test.go` covers TND-1.1: Teranode must propagate blocks to all connected nodes after they are mined.

- **Implementation**: The test establishes a three-node Teranode network and verifies block propagation functionality. It first mines a block on one node and then checks that the block hash is properly propagated to all connected nodes through the network. The test confirms this by retrieving the best block header from each node and verifying they all have the same block hash, proving that block propagation functions correctly across the network.

### Running TND Tests

#### Running All TND Tests

```bash
cd /teranode/test/tnd
go test -v -tags test_tnd
```

#### Running Specific TND Tests

```bash
cd /teranode/test/tnd
go test -v -run "^TestTND1TestSuite$/TestTND1_1$" -tags test_tnd
```

## TNE

As outlined in the Functional Requirements for Teranode reference document, the TNE tests verify that nodes correctly validate blocks received from the network and optimize verification processes.

**Note**: TNE tests are located in `/test/e2e/daemon/` rather than a dedicated `/test/tne/` folder, as they require full daemon integration.

The naming convention for these test files is:

`tne<number>_<subnumber>_test.go` corresponds to the TNE-<number>.<subnumber> test in the Functional Requirements for Teranode document.

### TNE1 Test Description

#### TNE-1.1: Optimized Transaction Verification

`tne1_1_test.go` covers TNE-1.1: Teranode must not re-verify transactions that it has already verified.

- **Implementation**: The test establishes a Teranode network and verifies that transactions are not re-verified once they've been processed. It sends a set of transactions to a node, confirms they are processed, and then mines blocks containing these transactions. The test then verifies that when these transactions appear in blocks, they aren't re-verified by examining internal metrics and logs. This ensures efficiency in block processing, particularly when handling blocks that contain transactions already present in the node's mempool.

### Running TNE Tests

#### Running TNE Tests

```bash
cd /teranode/test/e2e/daemon
# Run specific TNE test
go test -v -run "^TestTNE1_1$" tne1_1_test.go
```

**Note**: TNE tests are integration tests in the e2e/daemon directory and typically don't require build tags.

## TNF

As outlined in the Functional Requirements for Teranode reference document, the tests in the `/test/tnf` folder verify that Teranode correctly tracks and maintains the longest honest chain, including handling chain reorganizations and block invalidation.

The naming convention for files in this folder is as follows:

`tnf<number>_test.go` corresponds to the TNF-<number> test in the Functional Requirements for Teranode document.

### TNF6 Test Description

#### TNF-6: Block Invalidation

`tnf6_test.go` covers TNF-6: Teranode must allow for manual invalidation of blocks to handle potentially malicious or invalid chains.

- **Implementation**: The test establishes a three-node Teranode network to verify block invalidation functionality. It first ensures all nodes are synchronized on the same chain, then restarts one node with modified settings to trigger a specific blockchain state. The test then identifies blocks mined by a specific miner and uses the `InvalidateBlock` API to manually invalidate those blocks across all nodes. After invalidation, it verifies that the best block on all nodes has changed and no longer references the invalidated miner, confirming that Teranode properly handles manual block invalidation. This capability is crucial for network resilience against potentially malicious chains.

### Additional Chain Reorganization Tests

In addition to the dedicated TNF tests, the `test/e2e/daemon/reorg_test.go` file provides more comprehensive e2e tests for blockchain reorganization handling, which is a key aspect of keeping track of the longest honest chain:

- **TestMoveUp**: Tests block propagation between nodes and verifies that a newly generated block is properly propagated through the network.

- **TestMoveDownMoveUp**: Tests chain reorganization when a node with a shorter chain (200 blocks) connects to a node with a longer chain (300 blocks). This verifies that the node correctly abandons its shorter chain and reorganizes to follow the longer chain, fulfilling a core TNF requirement.

- **TestTDRestart**: Tests persistence of the blockchain after a node restart, ensuring that chain state is properly maintained across restarts.

These smoke tests provide additional verification of the chain reorganization functionality in more realistic network scenarios with actual running nodes, complementing the more focused TNF unit tests.

### Running TNF Tests

#### Running All TNF Tests

```bash
cd /teranode/test/tnf
go test -v -tags test_tnf
```

#### Running Specific TNF Tests

```bash
cd /teranode/test/tnf
go test -v -run "^TestTNFTestSuite$/TestInvalidateBlock$" -tags test_tnf
```

## TNJ

As outlined in the Functional Requirements for Teranode reference document, the tests in the `/test/tnj` folder verify that Teranode correctly implements and enforces the standard consensus rules of the Bitcoin SV protocol.

The naming convention for files in this folder is descriptive, using names like `locktime_test.go` that indicate the specific consensus rule being tested. Some TNJ tests may also be found in other directories (e.g., `test/e2e/daemon/`) when they require full daemon integration.

### Consensus Rules Test Description

#### TNJ-4: Coinbase Transaction Maturity

`block_subsidy_test.go` (located in `test/e2e/daemon/`) covers TNJ-4: Coinbase transactions must not be spent until they have matured (reached 100 blocks of confirmation).

- **Implementation**: The test establishes a multi-node Teranode network to verify coinbase transaction maturity rules. It creates a block with a coinbase transaction, then attempts to spend the outputs from that coinbase transaction immediately. The test verifies that this transaction is not included in subsequent blocks, demonstrating that Teranode correctly enforces the coinbase maturity rule. After generating 100 blocks to ensure maturity, it then confirms that the transaction spending from the mature coinbase can now be included in a block. This test ensures that Teranode enforces one of the fundamental consensus rules regarding coinbase outputs.

#### Transaction Locktime Rules

`locktime_test.go` covers the consensus rules regarding transaction locktime enforcement:

- **Implementation**: The test runs multiple locktime scenarios to verify Teranode's enforcement of transaction locktimes. It tests four specific scenarios:

    1. **Future Height Non-Final**: Transactions with a locktime set to a future block height and non-final sequence number, verifying they are not included in blocks.
    2. **Future Height Final**: Transactions with a locktime set to a future block height but with a final sequence number (0xFFFFFFFF), confirming they are included in blocks despite the locktime.
    3. **Future Timestamp Non-Final**: Transactions with a locktime set to a future timestamp and non-final sequence number, verifying they are not included in blocks.
    4. **Future Timestamp Final**: Transactions with a locktime set to a future timestamp but with a final sequence number, confirming they are included in blocks despite the locktime.

    These tests verify that Teranode correctly implements Bitcoin's locktime functionality, which allows transactions to be time-locked until a specific block height or time is reached, unless overridden by setting all input sequence numbers to 0xFFFFFFFF.

### Running TNJ Tests

#### Running All TNJ Tests

```bash
cd /teranode/test/tnj
go test -v -tags test_tnj
```

#### Running Specific TNJ Tests

```bash
cd /teranode/test/e2e/daemon
go test -v -run "^TestBlockSubsidy$"
```

```bash
cd /teranode/test/tnj
go test -v -run "^TestTNJLockTimeTestSuite$/TestLocktimeScenarios$" -tags test_tnj
```

## TEC

The tests in the `/test/tec` folder check  Teranode's ability to recover from multiple types of errors, from incorrect settings to communication errors on the message channels between microservices.

The naming convention for files in this folder is as follows:

`tec_blk_<number>_test.go` corresponds to the TEC-<number> test in the Functional Requirements for Teranode document.

- TEC-BLK-1: Blockchain Service Reliability and Recoverability - Blockchain Service - Blockchain Store Failure
- TEC-BLK-2: Blockchain Service Reliability and Recoverability - Blockchain Service - UTXO Store Failure
- TEC-BLK-3: Blockchain Service Reliability and Recoverability - Blockchain Service - Block Assembly Failure
- TEC-BLK-4: Blockchain Service Reliability and Recoverability - Blockchain Service - Block Validation Failure
- TEC-BLK-5: Blockchain Service Reliability and Recoverability - Blockchain Service - P2P Service Failure
- TEC-BLK-6: Blockchain Service Reliability and Recoverability - Blockchain Service - Asset Server Failure
- TEC-BLK-7: Blockchain Service Reliability and Recoverability - Blockchain Service - Kafka Failure
