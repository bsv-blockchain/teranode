# RPC Service: OpenRPC Specification for JSON-RPC API

## Why OpenRPC and not Swagger?

Teranode has two HTTP services with fundamentally different API styles:

| Service | Style | Spec tool | PR |
|---------|-------|-----------|-----|
| **Asset service** | REST API (Echo, many routes like `GET /api/v1/block/{hash}`) | **go-swagger** (Swagger/OpenAPI) | PR1 (`gokhan/swagger-asset`) |
| **RPC service** | JSON-RPC (single `POST /`, method name in JSON body) | **OpenRPC** | This PR (`gokhan/swagger-rpc`) |

Swagger/OpenAPI maps one spec entry per URL path — it was designed for REST. The RPC service has only one URL (`POST /`) and dispatches 30+ methods via `{"method": "getblock", "params": [...]}` in the request body. Swagger cannot describe this.

**OpenRPC** is the JSON-RPC equivalent of OpenAPI. It's the standard used by Ethereum, MetaMask, Chainlink, and other blockchain projects for documenting JSON-RPC node APIs. It describes methods, parameters, and result schemas — exactly what a JSON-RPC API needs.

## Summary

Adds an [OpenRPC 1.2.6](https://open-rpc.org/) specification documenting all 31 implemented JSON-RPC methods in the Teranode RPC service.

## What changed

### New files

| File | Purpose |
|------|---------|
| `openapi/rpc_openrpc.json` | OpenRPC 1.2.6 spec documenting all 31 implemented JSON-RPC methods with parameter and result schemas |
| `openapi/CHANGES.md` | This file |

## Methods documented (31)

### Blockchain queries
- `getinfo` — General node information
- `getbestblockhash` — Hash of the chain tip
- `getblock` — Block data by hash (hex/JSON/verbose)
- `getblockbyheight` — Block data by height (BSV extension)
- `getblockhash` — Block hash at a given height
- `getblockheader` — Block header by hash (hex/JSON)
- `getblockchaininfo` — Chain state information
- `getchaintips` — All known chain tips
- `getdifficulty` — Current mining difficulty

### Transactions
- `getrawtransaction` — Raw transaction by txid (hex/JSON)
- `sendrawtransaction` — Submit a raw transaction for broadcast
- `createrawtransaction` — Create an unsigned raw transaction
- `getrawmempool` — Mempool transaction IDs

### Mining
- `getmininginfo` — Mining-related information
- `getminingcandidate` — Mining candidate for block construction (BSV extension)
- `submitminingsolution` — Submit a solved block (BSV extension)
- `generate` — Mine blocks immediately (regtest)
- `generatetoaddress` — Mine blocks to an address (regtest)

### Network / Peers
- `getpeerinfo` — Connected peer information
- `setban` — Ban/unban an IP or subnet
- `isbanned` — Check if an IP is banned
- `listbanned` — List all bans
- `clearbanned` — Clear all bans

### Block validation
- `invalidateblock` — Mark a block as invalid
- `reconsiderblock` — Reconsider a previously invalidated block

### Alert system (BSV extension)
- `freeze` — Freeze a UTXO
- `unfreeze` — Unfreeze a UTXO
- `reassign` — Reassign a frozen UTXO to a new output

### Utility
- `help` — List commands or get help for a specific command
- `stop` — Shut down the node
- `version` — JSON-RPC API version information

## Bitcoin compatibility

All method names, parameter names, and response field names match the Bitcoin/BSV RPC specification. The `bsvjson` package (forked from btcsuite) defines the canonical type definitions. BSV-specific extensions (`getblockbyheight`, `getminingcandidate`, `submitminingsolution`, `freeze`, `unfreeze`, `reassign`) are clearly marked in their descriptions.

## Not documented (unimplemented methods)

The following methods are registered in `rpcHandlersBeforeInit` but map to `handleUnimplemented` and are excluded from the spec: `addnode`, `debuglevel`, `decoderawtransaction`, `decodescript`, `estimatefee`, `getaddednodeinfo`, `getbestblock`, `getblockcount`, `getblocktemplate`, `getcfilter`, `getcfilterheader`, `getconnectioncount`, `getcurrentnet`, `getgenerate`, `gethashespersec`, `getheaders`, `getmempoolinfo`, `getnettotals`, `getnetworkhashps`, `gettxout`, `gettxoutproof`, `node`, `ping`, `searchrawtransactions`, `setgenerate`, `submitblock`, `uptime`, `validateaddress`, `verifychain`, `verifymessage`, `verifytxoutproof`.

## Test plan

- [x] JSON is valid and parseable
- [x] All 31 implemented methods are documented with params and result schemas
- [ ] Verify spec renders correctly in an OpenRPC playground (https://playground.open-rpc.org/)
