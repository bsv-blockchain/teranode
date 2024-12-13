# How to Interact with the RPC Server

Last Modified: 13-December-2024

There are 2 primary ways to interact with the node, using the RPC Server, and using the Asset Server. This document will focus on the RPC Server. The RPC server provides a JSON-RPC interface for interacting with the node. Below is a list of implemented RPC methods.


## Teranode RPC HTTP API



The Teranode RPC server provides a JSON-RPC interface for interacting with the node. Below is a list of implemented RPC methods:



**Supported Methods**

1. `createrawtransaction`: Creates a raw transaction.
2. `generate`: Generates new blocks.
3. `getbestblockhash`: Returns the hash of the best (tip) block in the longest blockchain.
4. `getblock`: Retrieves a block by its hash.
5. `getminingcandidate`: Obtain a mining candidate from the node.
6. `sendrawtransaction`: Submits a raw transaction to the network.
7. `submitminingsolution`: Submits a new mining solution.
8. `stop`: Stops the Teranode server.
9. `version`: Returns version information about the RPC server.



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
