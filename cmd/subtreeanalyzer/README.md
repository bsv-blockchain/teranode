# Subtree Analyzer

A command-line tool for analyzing scheduled blob deletions and determining which blocks contain the subtrees scheduled for deletion.

## Purpose

This tool helps you understand the relationship between subtrees stored in the blockchain database and their scheduled deletion times. It queries the `scheduled_blob_deletions` table and matches each subtree hash with the blocks that contain it.

## Usage

```bash
# Build the tool
go build -o subtreeanalyzer ./cmd/subtreeanalyzer/

# Run with default settings
./subtreeanalyzer

# The tool will automatically read settings from the standard configuration
```

## Output

The tool produces a block-centric report showing:
- Blocks ordered by height
- For each block:
  - Block height and hash
  - List of subtrees that have scheduled deletions
  - For each subtree:
    - Subtree hash
    - Deletion height range (min-max if multiple deletion schedules exist)
    - File type (transaction or subtree)
    - Retry count (highest retry count if multiple schedules exist)

Example output:
```
=== Blocks with Scheduled Subtree Deletions ===
Found 2 blocks containing subtrees scheduled for deletion

Block Height: 750
  Block Hash: 000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd
  Subtrees with Scheduled Deletions (2):
    - 0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206 (Delete at: 1000, Type: subtree, Retries: 0)
    - a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2 (Delete at: 950-1200, Type: subtree, Retries: 2)

Block Height: 800
  Block Hash: 00000000b873e79784647a6c82962c70d228557d24a747ea4d1b8bbe878e1206
  Subtrees with Scheduled Deletions (1):
    - 0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206 (Delete at: 1000, Type: subtree, Retries: 0)
```

## How It Works

1. Connects to the blockchain database using the configured store URL from settings
2. Queries all entries from the `scheduled_blob_deletions` table
3. Builds a map of subtree hashes with their deletion info:
   - Aggregates multiple deletion schedules for the same subtree
   - Tracks min/max deletion heights
   - Keeps the highest retry count
4. Scans all blocks in height order:
   - Parses the binary-encoded subtrees column in each block
   - Matches subtrees against the deletion map
   - Collects blocks that contain scheduled subtrees
5. Outputs results grouped by block in ascending height order

## Database Schema

The tool works with two main tables:

### scheduled_blob_deletions
- `id` - Unique identifier
- `blob_key` - Subtree hash (32 bytes)
- `file_type` - Type of blob (e.g., "subtree", "transaction")
- `store_type` - Storage backend identifier
- `delete_at_height` - Block height at which to delete
- `retry_count` - Number of deletion attempts
- `last_retry_at` - Timestamp of last retry

### blocks
- Contains block headers and metadata
- `subtrees` - Binary-encoded array of subtree hashes
  - Format: VarInt(count) + 32-byte hash + 32-byte hash + ...
- `height` - Block height
- `hash` - Block hash

## Use Cases

- Debugging blob deletion issues
- Understanding which blocks will be affected by pruning
- Verifying subtree references before deletion
- Auditing scheduled deletions
