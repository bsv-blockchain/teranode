# Testnet Headers for Emergency Difficulty Testing

## Files

- `testnet_headers_1602530_1602710.bin` - Reduced headers file (14KB) containing blocks 1602530-1602710
- `1600001_1610000_headers.bin` - Original full headers file (781KB) containing blocks 1600001-1610000

## Purpose

These header files are used to test the emergency difficulty adjustment on Bitcoin SV testnet, specifically:

- Block 1602682: Normal difficulty calculation (0x1a33c833)
- Block 1602683: Emergency difficulty triggered (0x1d00ffff) - mined 1208 seconds (20+ minutes) after previous block

## Emergency Difficulty Rule

On testnet, if a block takes more than 20 minutes (2 * target spacing of 10 minutes) to mine, the difficulty drops to the minimum (0x1d00ffff) to prevent the network from stalling.

## Data Source

Original headers downloaded from WhatsOnChain API:

```bash
https://api.whatsonchain.com/v1/bsv/test/block/headers/1600001_1610000_headers.bin
```

## File Reduction

The original 10,000 block file was reduced to 181 blocks containing:

- The target blocks (1602682-1602683)
- 144 blocks before for difficulty adjustment window calculation
- Some additional buffer blocks

This reduces file size by 98% (781KB → 14KB) and speeds up test execution.

## Test Usage

The test `TestGetNextWorkRequiredTestnet` in `test/blockchain_difficulty_testnet_test.go` uses these headers to:

1. Load headers into a SQLite database
2. Start a standalone blockchain service
3. Test GetNextWorkRequired gRPC calls
4. Verify both normal and emergency difficulty calculations
