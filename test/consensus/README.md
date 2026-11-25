# Bitcoin-SV Test Suite Port

This package contains the ported test suites from bitcoin-sv for validating transaction and script validation in native Go.

## Current Status

### ✅ Completed
1. **Test Infrastructure**
   - Created test vector loaders for JSON test data
   - bitcoin-sv v1.2.3 test vectors (script_tests.json, tx_valid.json, tx_invalid.json, sighash.json),
     the release gobdk 1.4.0 is built from. When gobdk moves to a new bitcoin-sv release, copy the
     four files from that release's `src/test/data/`.
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
   - Consensus rule tests
   - The hand-written TestBuilder tests (`TestScriptTests`, BIP66, locktime and others) still use
     the heuristic `ScriptParser`, derive a block height from the flags, and hold the remaining skips

3. **Native Go Implementation**
   - Implement script interpreter in pure Go
   - Port consensus rules from C++
   - Ensure exact compatibility with bitcoin-sv

## Test Data Format

### Script Tests (script_tests.json)
Format: `[[amount]?, tx_version, scriptSig, scriptPubKey, flags, expected_error, optional_comment]`

Example:
```json
["1", "0x51", "0x5f ADD 0x60 EQUAL", "P2SH,STRICTENC", "OK", "0x51 through 0x60 push 1 through 16 onto stack"]
```

`TestScriptVectors` runs every vector as bitcoin-sv's `script_tests.cpp` (`script_json_test`) does:
a crediting transaction pays the amount to scriptPubKey, a spending transaction of the vector's
version spends it with scriptSig, and gobdk verifies it in consensus mode with exactly the
vector's flags. The script error must match by name. Scripts are parsed with
`ParseScriptBitcoinSV` and v1.2.3's opcode names (`bitcoinSVOpNames`). Vectors where Teranode
deliberately differs go in `scriptVectorDeviations`.

### Transaction Tests (tx_valid.json, tx_invalid.json)
Format: `[[[prevout hash, prevout index, prevout scriptPubKey, optional amount], ...], serialized_tx_hex, flags]`,
where flags is one comma-separated flag set or an array of them.

`TestTransactionValidation` runs every vector as bitcoin-sv's `transaction_tests.cpp` does:
context-free checks with pre-Genesis limits, then each input's script through gobdk's
`VerifyScriptWithCustomFlags` with each flag set. Prevout scripts are parsed with
`ParseScriptBitcoinSV`, a port of bitcoin-sv's `ParseScript`. Vectors where Teranode
deliberately differs go in `txVectorDeviations`.

### Signature Hash Tests (sighash.json)
Format: `[raw_tx, script, input_index, hash_type, signature_hash, signature_hash_original]`

`TestSigHashVectors` checks go-bt's signature hashes against both columns, as bitcoin-sv's
`sighash_tests.cpp` does. Two go-bt signing deviations are pinned: its legacy preimage keeps
OP_CODESEPARATOR, which bitcoin-sv drops, and it uses BIP143 for FORKID|CHRONICLE hash types,
where bitcoin-sv v1.2.3 uses the original algorithm.

## Usage

```bash
# bitcoin-sv's script, transaction and signature hash vectors
go test -v ./test/consensus -run 'TestScriptVectors|TestTransactionValidation|TestSigHashVectors'

# Everything in the package, as CI runs it
make consensustest
```

## Next Steps

1. Implement a proper script parser that can handle the mixed format
2. Create mapping of opcode names to their byte values
3. Hook up actual validators for testing
4. Begin differential testing between implementations
5. Start implementing native Go validation logic