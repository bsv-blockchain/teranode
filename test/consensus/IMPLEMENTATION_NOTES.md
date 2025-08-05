# Bitcoin Script Testing Framework Implementation Notes

## Context
This document captures the implementation journey of porting Bitcoin Core/SV C++ script tests to Go for the Teranode project.

## Initial Problem
The tests in `script_simple_test.go` were failing because `ParseHexScript` couldn't handle mixed format scripts containing both opcodes and hex values (e.g., "0x01 0x0b" or "PUSHDATA1 0x00").

## Solution Overview

### 1. Script Parser Implementation
Created a new `ScriptParser` that can handle:
- Mixed opcodes and hex values in script strings
- Proper PUSHDATA operations (PUSHDATA1/2/4)
- Edge cases like zero-length PUSHDATA operations

### 2. Comprehensive Test Framework
Implemented the following components to match C++ testing approach:

#### Error Types (`script_errors.go`)
- Defined 97 script error types matching Bitcoin Core
- Created error mapping functions to translate validator errors

#### Test Builder (`test_builder.go`)
- Ported the C++ TestBuilder pattern
- Implemented signature generation with specific R/S lengths
- Added support for high S values (lenS=33) for BIP66 tests

#### Test Files Created
- `bip66_test.go` - Strict DER signature tests
- `locktime_test.go` - CHECKLOCKTIMEVERIFY and CHECKSEQUENCEVERIFY tests
- `script_comprehensive_test.go` - P2PK, P2PKH, multisig, and operation tests
- `error_mapping.go` - Maps validator errors to specific script errors

### 3. Key Technical Challenges Resolved

#### Signature Generation Hanging
**Problem**: Tests were hanging when trying to generate signatures with specific R/S lengths.

**Root Cause**: The signature variation algorithm wasn't producing enough entropy.

**Solution**: 
```go
// Mix iteration into the hash to create variation
variedHash := make([]byte, 32)
for i := 0; i < 32; i++ {
    if i < len(hash) {
        variedHash[i] = hash[i] ^ byte(iteration>>(8*(i%4)))
    } else {
        variedHash[i] = byte(iteration >> (8 * (i % 4)))
    }
}
```

#### High S Value Generation
**Problem**: BIP66 tests require generating signatures with high S values (S > order/2).

**Root Cause**: Needed to understand the C++ logic: when lenS=33, it means we want a high S value.

**Solution**: 
- Implemented proper S value negation using big.Int arithmetic
- Added logic to check DER encoding length and negate S when needed
- Fixed the MakeSig function to properly handle the C++ pattern

#### Error Mapping
**Problem**: Validators were returning generic "UNKNOWN_ERROR" instead of specific script errors.

**Solution**: Enhanced error mapping to recognize patterns like:
- "false stack entry at end of script execution" → SCRIPT_ERR_EVAL_FALSE
- "index X is invalid for stack size Y" → SCRIPT_ERR_INVALID_STACK_OPERATION

## Current Status

### Working
- Script parsing for mixed format inputs
- Basic script validation tests
- Signature generation with specific R/S lengths
- Error mapping for common validation failures
- Test framework structure matches C++ implementation

### Known Limitations
1. **Validator Flag Support**: The underlying validators (go-bt, go-sdk) don't fully enforce:
   - LOW_S flag (high S values should be rejected)
   - STRICTENC flag (non-canonical encodings should be rejected)
   - DERSIG flag (non-DER signatures should be rejected)

2. **Locktime Validation**: CHECKLOCKTIMEVERIFY and CHECKSEQUENCEVERIFY aren't properly validated

3. **Multisig Edge Cases**: Some multisig validation scenarios fail

## Implementation Timeline
1. Started with fixing ParseHexScript for mixed format scripts
2. User requested full C++ test parity: "I want to mimic the testing that the c++ codebase has done"
3. Implemented comprehensive test framework with all Bitcoin script error types
4. Fixed signature generation hanging issues
5. Resolved high S value generation for BIP66 tests
6. Enhanced error mapping for better test diagnostics

## Code Structure
```
test/consensus/
├── script_parser.go         # Handles mixed format script parsing
├── script_errors.go         # 97 script error types
├── test_builder.go          # Test building framework
├── error_mapping.go         # Maps validator errors to script errors
├── validator_integration.go # Integrates different validators
├── testutil.go             # Test utilities and key data
└── *_test.go               # Various test files
```

## Future Work
To achieve full C++ parity, the underlying validator libraries need enhancement:
1. Implement full script flag validation (LOW_S, STRICTENC, DERSIG)
2. Add proper locktime validation
3. Fix multisig edge cases
4. Consider implementing a native Go script interpreter that matches C++ behavior exactly

## Key Lessons Learned
1. The C++ test framework uses lenS=33 to signal that a high S value is desired
2. DER encoding can add padding bytes when the high bit is set
3. Validator error messages need careful mapping to specific script errors
4. The test framework architecture (TestBuilder pattern) is crucial for maintainable tests