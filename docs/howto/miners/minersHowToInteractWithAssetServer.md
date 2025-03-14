# How to Interact with the Asset Server


There are 2 primary ways to interact with the node, using the RPC Server, and using the Asset Server. This document will focus on the Asset Server. The Asset Server provides an HTTP API for interacting with the node. Below is a list of implemented endpoints.


## Teranode Asset Server HTTP API



The Teranode Asset Server provides the following HTTP endpoints. Unless otherwise specified, all endpoints are GET requests.



Base URL: `/api/v1` (configurable)

Port: `8090` (configurable)



**Health and Status Endpoints**

- GET `/alive`
    - Returns the service status and uptime

- GET `/health`
    - Performs a health check on the service



**Transaction Endpoints**

- GET `/tx/:hash`
    - Retrieves a transaction in binary stream format
- GET `/tx/:hash/hex`
    - Retrieves a transaction in hexadecimal format
- GET `/tx/:hash/json`
    - Retrieves a transaction in JSON format

- POST `/txs`
    - Retrieves multiple transactions (binary stream format)
    - The request body is a concatenated series of 32-byte transaction hashes:
[32-byte hash][32-byte hash][32-byte hash]...

    - Example (in hex):
123456789abcdef123456789abcdef123456789abcdef123456789abcdef1234
567890abcdef123456789abcdef123456789abcdef123456789abcdef123456
...
    - Each hash is exactly 32 bytes (256 bits)
    - No separators between hashes
    - The body can contain multiple hashes
    - The server reads until it reaches the end of the body (EOF)

- GET `/txmeta/:hash/json`
    - Retrieves transaction metadata in JSON format

- GET `/txmeta_raw/:hash`
    - Retrieves raw transaction metadata in binary stream format
- GET `/txmeta_raw/:hash/hex`
    - Retrieves raw transaction metadata in hexadecimal format
- GET `/txmeta_raw/:hash/json`
    - Retrieves raw transaction metadata in JSON format



**Subtree Endpoints**

- GET `/subtree/:hash`
    - Retrieves a subtree in binary stream format
- GET `/subtree/:hash/hex`
    - Retrieves a subtree in hexadecimal format
- GET `/subtree/:hash/json`
    - Retrieves a subtree in JSON format

- GET `/subtree/:hash/txs/json`
    - Retrieves transactions in a subtree in JSON format



**Block and Header Endpoints**

- GET `/headers/:hash`
    - Retrieves block headers in binary stream format
- GET `/headers/:hash/hex`
    - Retrieves block headers in hexadecimal format
- GET `/headers/:hash/json`
    - Retrieves block headers in JSON format
- GET `/header/:hash`
    - Retrieves a single block header in binary stream format
- GET `/header/:hash/hex`
    - Retrieves a single block header in hexadecimal format
- GET `/header/:hash/json`
    - Retrieves a single block header in JSON format
- GET `/blocks`
    - Retrieves multiple blocks
- GET `/blocks/:hash`
    - Retrieves N blocks starting from a specific hash in binary stream format
- GET `/blocks/:hash/hex`
    - Retrieves N blocks starting from a specific hash in hexadecimal format
- GET `/blocks/:hash/json`
    - Retrieves N blocks starting from a specific hash in JSON format
- GET `/block_legacy/:hash`
    - Retrieves a block in legacy format (binary stream)
- GET `/block/:hash`
    - Retrieves a block by hash in binary stream format
- GET `/rest/block/:hash.bin`
    - (Deprecated). Retrieves a block by hash in binary stream format.

- GET `/block/:hash/hex`
    - Retrieves a block by hash in hexadecimal format
- GET `/block/:hash/json`
    - Retrieves a block by hash in JSON format
- GET `/block/:hash/forks`
    - Retrieves fork information for a specific block
- GET `/block/:hash/subtrees/json`
    - Retrieves subtrees of a block in JSON format
- GET `/lastblocks`
    - Retrieves the last N blocks
- GET `/bestblockheader`
    - Retrieves the best block header in binary stream format
- GET `/bestblockheader/hex`
    - Retrieves the best block header in hexadecimal format
- GET `/bestblockheader/json`
    - Retrieves the best block header in JSON format



**UTXO and Balance Endpoints**

- GET `/utxo/:hash`
    - Retrieves a UTXO in binary stream format
- GET `/utxo/:hash/hex`
    - Retrieves a UTXO in hexadecimal format
- GET `/utxo/:hash/json`
    - Retrieves a UTXO in JSON format

- GET `/utxos/:hash/json`
    - Retrieves UTXOs by transaction ID in JSON format

- GET `/balance`
    - Retrieves balance information



**Miscellaneous Endpoints**

- GET `/search?q=:hash`
    - Performs a search

- GET `/blockstats`
    - Retrieves block statistics

- GET `/blockgraphdata/:period`
    - Retrieves block graph data for a specified period


For more information on the Asset Server, see the [Asset Server Reference](../../references/services/asset_reference.md).
