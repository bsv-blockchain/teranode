# How to Interact with the RPC Server

Last Modified: 26-March-2025

There are 2 primary ways to interact with the node, using the RPC Server, and using the Asset Server. This document will focus on the RPC Server. The RPC server provides a JSON-RPC interface for interacting with the node. Below is a list of implemented RPC methods.


## Teranode RPC HTTP API

The Teranode RPC server provides a JSON-RPC interface for interacting with the node. Below is a list of implemented RPC methods:

### Block-related Methods
1. `getbestblockhash`: Returns the hash of the best (tip) block
2. `getblock`: Retrieves a block by its hash
3. `getblockbyheight`: Retrieves a block by its height
4. `getblockhash`: Returns the hash of a block at specified height
5. `getblockheader`: Returns information about a block's header
6. `getblockchaininfo`: Returns information about the blockchain
7. `invalidateblock`: Marks a block as invalid
8. `reconsiderblock`: Removes invalidity status of a block

### Mining-related Methods
1. `getdifficulty`: Returns the current network difficulty
2. `getmininginfo`: Returns mining-related information
3. `getminingcandidate`: Obtain a mining candidate
4. `submitminingsolution`: Submits a new mining solution
5. `generate`: Generates new blocks

### Transaction-related Methods
1. `createrawtransaction`: Creates a raw transaction
2. `sendrawtransaction`: Submits a raw transaction to the network
3. `freeze`: Freezes a specific UTXO, preventing it from being spent
4. `unfreeze`: Unfreezes a previously frozen UTXO, allowing it to be spent
5. `reassign`: Reassigns ownership of a specific UTXO to a new Bitcoin address

### Network-related Methods
1. `getinfo`: Returns information about the node
2. `getpeerinfo`: Returns information about connected peers
3. `setban`: Manages banned IP addresses/subnets
4. `isbanned`: Checks if a network address is currently banned

### Server Control Methods
1. `stop`: Stops the Teranode server
2. `version`: Returns version information


**Authentication**

The RPC server uses HTTP Basic Authentication. Credentials are configured in the settings (see the section 4.1 for details). There are two levels of access:

1. Admin access: Full access to all RPC methods.
2. Limited access: Access to a subset of RPC methods defined in `rpcLimited`.



**Request Format**

Requests should be sent as HTTP POST requests with a JSON-RPC 1.0 or 2.0 formatted body. For example:

```json
{
"jsonrpc": "1.0",
"id": "1",
"method": "getbestblockhash",
"params": []
}
```



**Response Format**

Responses are JSON objects containing the following fields:

- `result`: The result of the method call (if successful).
- `error`: Error information (if an error occurred).
- `id`: The id of the request.



**Error Handling**

Errors are returned as JSON-RPC error objects with a code and message. Common error codes include:

- `-32600`: Invalid Request
- `-32601`: Method not found
- `-32602`: Invalid params
- `-32603`: Internal error
- `-32700`: Parse error


**Example Request**

The default credentials are `bitcoin:bitcoin`. The default credentials can be changed via settings.

`curl --user bitcoin:bitcoin --data-binary '{"jsonrpc":"1.0","id":"curltext","method":"version","params":[]}' -H 'content-type: text/plain;' http://localhost:9292/`


------

For detailed information on each method's parameters and return values, refer to the Bitcoin SV protocol documentation or the specific Teranode [RPC Reference](../../references/services/rpc_reference.md).
