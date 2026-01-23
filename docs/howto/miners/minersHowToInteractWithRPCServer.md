# How to Interact with the RPC Server

There are 2 primary ways to interact with the node, using the RPC Server, and using the Asset Server. This document will focus on the RPC Server. The RPC server provides a JSON-RPC interface for interacting with the node. Below is a list of implemented RPC methods with their parameters and return values.

## Teranode RPC HTTP API

The Teranode RPC server provides a JSON-RPC interface for interacting with the node. Below is a list of implemented RPC methods:

### Block-related Methods

1. `getbestblockhash`: Returns the hash of the best (tip) block
2. `getblock`: Retrieves a block by its hash
3. `getblockbyheight`: Retrieves a block by its height
4. `getblockhash`: Returns the hash of a block at specified height
5. `getblockheader`: Returns information about a block's header
6. `getblockchaininfo`: Returns information about the blockchain (available via RPC only, see example below)
7. `invalidateblock`: Marks a block as invalid
8. `reconsiderblock`: Removes invalidity status of a block (performs full block validation)

> **Note:** The `getblockchaininfo` RPC method is accessed via JSON-RPC, not via `teranode-cli`. For a visual view of blockchain state, use the blockchain viewer at <http://localhost:8090/viewer>

### Mining-related Methods

1. `getdifficulty`: Returns the current network difficulty
    - Parameters:
        None

    - Returns: Current difficulty as a floating point number

2. `getmininginfo`: Returns mining-related information
    - Parameters:
        None

    - Returns: Object containing block height, current block size, current block transaction count, current difficulty, estimated network hashrate, error messages, and chain name

3. `getminingcandidate`: Obtain a mining candidate
    - Parameters:

        - Optional object containing:

            - `provideCoinbaseTx` (boolean, optional, default=false): If true, includes the coinbase transaction in the response
            - `verbosity` (numeric, optional, default=0): 0 for standard response, 1 to include subtree hashes

    - Returns: Object containing candidate ID, previous block hash, coinbase value, and merkle proof
    - Example Request:

        ```json
        {
            "jsonrpc": "1.0",
            "id": "mining",
            "method": "getminingcandidate",
            "params": []
        }
        ```

    - Example Request with coinbase transaction and subtree hashes:

        ```json
        {
            "jsonrpc": "1.0",
            "id": "mining",
            "method": "getminingcandidate",
            "params": [{"provideCoinbaseTx": true, "verbosity": 1}]
        }
        ```

    - Example Response:

        ```json
        {
            "result": {
                "id": "00000000000000000000000000000000...",
                "prevhash": "000000000000000004a1b6d6fdfa0d0a...",
                "coinbaseValue": 5000000000,
                "version": 536870912,
                "nBits": "180d60e3",
                "time": 1621500000,
                "height": 700001,
                "num_tx": 5620,
                "sizeWithoutCoinbase": 2300000,
                "merkleProof": [...]
            },
            "error": null,
            "id": "mining"
        }
        ```

4. `submitminingsolution`: Submits a new mining solution
    - Parameters:

        - `id` (string, required): Mining candidate ID
        - `nonce` (hexadecimal string, required): Nonce value found
        - `coinbase` (hexadecimal string, required): Complete coinbase transaction
        - `time` (numeric, required): Block time
        - `version` (numeric, optional): Block version

    - Returns: Boolean `true` if block accepted, error if rejected
    - Validation process: The solution is validated for proof-of-work correctness, block structure, and consensus rules before being accepted and propagated to the network
    - Example Request:

        ```json
        {
            "jsonrpc": "1.0",
            "id": "mining",
            "method": "submitminingsolution",
            "params": [{
                "id": "00000000000000000000000000000000...",
                "nonce": "17aab479321b85",
                "coinbase": "01000000010000000000000000000000000000...",
                "time": 1621500004
            }]
        }
        ```

5. `generate`: Generates new blocks (for testing only)
    - Parameters:

        - `nblocks` (numeric, required): Number of blocks to generate
        - `maxtries` (numeric, optional): Maximum iterations to try

    - Returns: Array of block hashes generated

6. `generatetoaddress`: Generates new blocks with rewards going to a specified address (for testing only)
    - Parameters:

        - `nblocks` (numeric, required): Number of blocks to generate
        - `address` (string, required): Bitcoin address to receive the rewards
        - `maxtries` (numeric, optional): Maximum iterations to try

    - Returns: Array of block hashes generated

### Transaction-related Methods

1. `createrawtransaction`: Creates a raw transaction
    - Parameters:

        - `inputs` (array, required): Array of transaction inputs
            - Each input is an object with:

                - `txid` (string, required): The transaction id
                - `vout` (numeric, required): The output number
        - `outputs` (object, required): JSON object with outputs as key-value pairs
            - Key is the Bitcoin address
            - Value is the amount in BTC

    - Returns: Hex-encoded raw transaction
    - Note: The transaction is not signed and cannot be submitted until signed

2. `getrawtransaction`: Gets a raw transaction
    - Parameters:

        - `txid` (string, required): Transaction ID
        - `verbose` (boolean, optional): If true, returns detailed information

    - Returns:

        - If verbose=false: Hex-encoded transaction data
        - If verbose=true: Detailed transaction object with txid, hash, size, version, locktime, and transaction inputs/outputs

3. `sendrawtransaction`: Sends a raw transaction
    - Parameters:

        - `hexstring` (string, required): Hex-encoded raw transaction
        - `allowhighfees` (boolean, optional): Allow high fees

    - Returns: Transaction hash (txid) if successful
    - Validation process: Transaction is validated for correct format, script correctness, and fee policy before being accepted and propagated to the network
    - Example Request:

      ```json
      {
          "jsonrpc": "1.0",
          "id": "curltext",
          "method": "sendrawtransaction",
          "params": ["0100000001bd2b5ba3d4a3a05c8ef31e8b6f8ab3e73b1f9ff5c617130cdf55e150d97a06ef000000006b483045022100c23a6432950e1ca96e438c95ce51bda58500ffa3a7a9941495a838bc7d3aee10022072ed0da7d7879f9ac7308a41c0e8ec7823e1b7932e211cf13a83a3ada10dacb141210386536695a23ba3ed37a18d542990f9b1df30a13952659d2820df3f47be78dcd3ffffffff01801a0600000000001976a914c5c25b16fa949402a8712e8e5fb3568eb87aee7288ac00000000"]
      }
      ```

4. `freeze`: Freezes a specific UTXO, preventing it from being spent
    - Parameters:

        - `txid` (string, required): The transaction ID of the UTXO
        - `vout` (numeric, required): The output index

    - Returns: Boolean `true` if successful
    - Note: Frozen UTXOs remain frozen until explicitly unfrozen

5. `unfreeze`: Unfreezes a previously frozen UTXO, allowing it to be spent
    - Parameters:

        - `txid` (string, required): The transaction ID of the frozen UTXO
        - `vout` (numeric, required): The output index

    - Returns: Boolean `true` if successful

6. `reassign`: Reassigns ownership of a specific UTXO to a new Bitcoin address
    - Parameters:

        - `txid` (string, required): The transaction ID of the UTXO
        - `vout` (numeric, required): The output index
        - `destination` (string, required): The Bitcoin address to reassign to

    - Returns: Boolean `true` if successful
    - Note: The UTXO must be frozen before it can be reassigned

7. `getrawmempool`: Returns transaction IDs being processed for block assembly
    - Parameters:

        - `verbose` (boolean, optional): If true, returns detailed information about pending transactions
    - Returns: 

        - If verbose=false: Array of transaction IDs being prepared for the next block
        - If verbose=true: Object with detailed information about transactions in the block assembly process
    - Note: In Teranode's architecture, this returns transactions from the subtree-based block assembly system, not a traditional mempool. The method name is kept for Bitcoin Core compatibility.

### Network-related Methods

1. `getinfo`: Returns information about the node
    - Parameters:
        None

    - Returns: Object containing node information including:

        - `version`: Server version
        - `protocolversion`: Protocol version
        - `blocks`: Current block count
        - `connections`: Current connection count
        - `difficulty`: Current network difficulty
        - `errors`: Current error messages
        - `testnet`: Whether running on testnet
        - `timeoffset`: Time offset in seconds

2. `getchaintips`: Returns information about all known chain tips
    - Parameters:
        None

    - Returns:

        - `height`: Height of the chain tip
        - `hash`: Block hash of the chain tip
        - `branchlen`: Zero for main chain, otherwise length of branch
        - `status`: Status of the chain tip ("active" for main chain, "valid-fork", etc.)

3. `getpeerinfo`: Returns information about connected peers
    - Parameters:
        None

    - Returns: Array of objects with detailed information about each connected peer, including:

        - `id`: Peer index
        - `addr`: IP address and port
        - `addrlocal`: Local address
        - `services`: Services provided by the peer
        - `lastsend`: Time since last message sent to this peer
        - `lastrecv`: Time since last message received from this peer
        - `bytessent`: Total bytes sent to this peer
        - `bytesrecv`: Total bytes received from this peer
        - `conntime`: Connection time in seconds
        - `pingtime`: Ping time in seconds
        - `version`: Peer protocol version
        - `subver`: Peer user agent
        - `inbound`: Whether connection is inbound
        - `startingheight`: Starting height of the peer
        - `banscore`: Ban score (for misbehavior)

4. `setban`: Manages banned IP addresses/subnets
    - Parameters:

        - `subnet` (string, required): The IP/Subnet to ban (e.g. 192.168.0.0/24)
        - `command` (string, required): 'add' to add to banlist, 'remove' to remove from banlist
        - `bantime` (numeric, optional): Time in seconds how long to ban (0 = permanently)
        - `absolute` (boolean, optional): If set to true, the bantime is interpreted as an absolute timestamp

    - Returns: null on success
    - Note: Successfully executes across both P2P and legacy peer services. The underlying GRPC operations require API key authentication, which is handled automatically by the RPC server.

5. `isbanned`: Checks if a network address is currently banned
    - Parameters:

        - `subnet` (string, required): The IP/Subnet to check

    - Returns: Boolean `true` if the address is banned, `false` otherwise
    - Note: This command accesses GRPC ban status methods which require API key authentication when accessed directly. The RPC command handles this authentication automatically.

6. `listbanned`: Returns list of all banned IP addresses/subnets
    - Parameters:
        None

    - Returns: Array of objects containing banned addresses with:

        - `address`: The banned IP/subnet
        - `banned_until`: The timestamp when the ban expires
        - `ban_created`: The timestamp when the ban was created
        - `ban_reason`: The reason for the ban (if provided)

7. `clearbanned`: Removes all IP address bans
    - Parameters:
        None

    - Returns: Boolean `true` on success

### Server Control Methods

1. `stop`: Stops the Teranode server
    - Parameters:
        None

    - Returns: String 'bsvd stopping.' when successful

2. `version`: Returns version information about the node
    - Parameters:
        None

    - Returns: Object containing version information including:

        - `version`: The server version
        - `subversion`: The server subversion string
        - `protocolversion`: The protocol version
        - `localservices`: The services supported by this node
        - `localrelay`: Whether transaction relay is active
        - `timeoffset`: The time offset
        - `buildinfo`: Additional build information (compiler, OS, etc.)

### Error Handling

When an error occurs during RPC method execution, the server returns a standardized error response in the following format:

```json
{
    "result": null,
    "error": {
        "code": -32601,
        "message": "Method not found"
    },
    "id": "1"
}
```

Common error codes include:

- `-32600`: Invalid request
- `-32601`: Method not found
- `-32602`: Invalid parameters
- `-32603`: Internal error
- `-1`: Misc error
- `-2`: Request rejected (e.g., node not synced)
- `-5`: Invalid address or key
- `-8`: Out of memory
- `-32`: Server error (e.g., shutting down)

**Note:** Teranode implements a subset of Bitcoin Core's RPC methods. If you encounter a "Command unimplemented" error, that method is not currently supported. See the [RPC reference documentation](../../references/services/rpc_reference.md) for the complete list of supported methods.

## Authentication

The RPC server uses HTTP Basic Authentication. Credentials are configured in the settings (see the section 4.1 for details). There are two levels of access:

1. Admin access: Full access to all RPC methods.
2. Limited access: Access to a subset of RPC methods defined in `rpcLimited`.

### GRPC API Key Authentication

For direct GRPC service access, certain administrative operations require additional API key authentication:

- **Protected Operations**: `BanPeer` and `UnbanPeer` methods in both P2P and Legacy GRPC services
- **Usage**: API key must be included in GRPC requests as metadata with the key `x-api-key`

**Note**: When using RPC commands like `setban` and `isbanned`, the API key authentication is handled automatically by the RPC server. Direct GRPC access requires manual API key inclusion.

## Request Format

Requests should be sent as HTTP POST requests with a JSON-RPC 1.0 or 2.0 formatted body. For example:

```json
{
    "jsonrpc": "1.0",
    "id": "1",
    "method": "getbestblockhash",
    "params": []
}
```

## Response Format

Responses are JSON objects containing the following fields:

- `result`: The result of the method call (if successful).
- `error`: Error information (if an error occurred).
- `id`: The id of the request.

## Example Request

The default credentials are `bitcoin:bitcoin`. The default credentials can be changed via settings.

```bash
curl --user bitcoin:bitcoin --data-binary '{"jsonrpc":"1.0","id":"curltext","method":"version","params":[]}' -H 'content-type: text/plain;' http://localhost:9292/
```

For detailed information on each method's parameters and return values, refer to the Bitcoin SV protocol documentation or the specific Teranode [RPC Reference](../../references/services/rpc_reference.md).
