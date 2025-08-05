# Bitcoin-SV Test Suite Port

This package contains the ported test suites from bitcoin-sv for validating transaction and script validation in native Go.

## Current Status

### ✅ Completed
1. **Test Infrastructure**
   - Created test vector loaders for JSON test data
   - Downloaded bitcoin-sv test vectors (script_tests.json, tx_valid.json, tx_invalid.json, sighash.json)
   - Built basic test framework structure
   - Created helper functions for transaction creation

2. **Test Analysis**
   - Analyzed 1,439 script tests from bitcoin-sv
   - Found mixed format: hex values, opcodes, and numeric values
   - Statistics:
     - 982 mixed format tests (opcodes + hex)
     - 449 numeric only tests
     - 7 "pure hex" tests (but actually contain 0x prefixes)
     - 858 tests expect OK result
     - 581 tests expect various errors

### 🚧 In Progress
1. **Script Parser Implementation**
   - Need to parse mixed format scripts (e.g., "0x51 ADD 0x60 EQUAL")
   - Handle opcode names (ADD, DUP, HASH160, etc.)
   - Parse hex values with 0x prefix
   - Handle numeric literals

2. **Validator Integration**
   - Hook up BDK validator for reference
   - Integrate go-bt validator
   - Integrate go-sdk validator
   - Build differential testing framework

### 📋 TODO
1. **Complete Script Parser**
   - Map opcode names to byte values
   - Handle push data operations
   - Parse complex scripts

2. **Port Remaining Tests**
   - Transaction validation tests (tx_valid.json, tx_invalid.json)
   - Signature hash tests (sighash.json)
   - Consensus rule tests

3. **Native Go Implementation**
   - Implement script interpreter in pure Go
   - Port consensus rules from C++
   - Ensure exact compatibility with bitcoin-sv

## Test Data Format

### Script Tests (script_tests.json)
Format: `[scriptSig, scriptPubKey, flags, expected_result, optional_comment]`

Example:
```json
["0x51", "0x5f ADD 0x60 EQUAL", "P2SH,STRICTENC", "OK", "0x51 through 0x60 push 1 through 16 onto stack"]
```

### Transaction Tests (tx_valid.json, tx_invalid.json)
Format: `[[inputs], serialized_tx_hex, flags]`

### Signature Hash Tests (sighash.json)
Format: `[raw_tx, script, input_index, hash_type, expected_hash]`

## Usage

```bash
# Run test statistics
go test -v ./test/consensus -run TestScriptTestStats

# Debug script formats
go test -v ./test/consensus -run TestDebugScriptFormats

# Run basic tests (currently skipped due to missing parser)
go test -v ./test/consensus -run TestBasicScriptTests
```

## Next Steps

1. Implement a proper script parser that can handle the mixed format
2. Create mapping of opcode names to their byte values
3. Hook up actual validators for testing
4. Begin differential testing between implementations
5. Start implementing native Go validation logic