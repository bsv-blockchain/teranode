# RPC Service

The RPC service provides a Bitcoin-compatible JSON-RPC interface for interacting with Teranode. It is the primary interface for wallets, miners, and tooling that communicate using the standard Bitcoin RPC protocol.

## Default Port

`9292` (configured via `rpc_listener_url` in settings, or `TERANODE_RPC_PORT` environment variable)

## Authentication

The service uses HTTP Basic Authentication. The default credentials are `bitcoin:bitcoin`. Pass them with `--user bitcoin:bitcoin` in curl, or configure them via settings.

## Running the Service

```bash
go run . -all=0 -rpc=1 -blockchain=1
```

## API Methods

All requests use HTTP POST with a JSON body containing `method` and optional `params`.

### getbestblockhash

Returns the hash of the best (tip) block.

```bash
curl --user bitcoin:bitcoin -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "getbestblockhash"}'
```

### getblock

Returns block data for a given block hash.

```bash
curl --user bitcoin:bitcoin -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "getblock", "params": ["000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", 1]}'
```

### getblockbyheight

Returns block data for a given block height.

```bash
curl --user bitcoin:bitcoin -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "getblockbyheight", "params": ["1", 1]}'
```

### getrawtransaction

Returns raw transaction data for a given transaction ID.

```bash
curl --user bitcoin:bitcoin -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "getrawtransaction", "params": ["<txid>", 1]}'
```

### sendrawtransaction

Broadcasts a raw transaction (hex-encoded) to the network.

```bash
curl --user bitcoin:bitcoin -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "sendrawtransaction", "params": ["<hex-encoded-tx>"]}'
```

### createrawtransaction

Creates a raw transaction from inputs and outputs.

```bash
curl --user bitcoin:bitcoin -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "createrawtransaction", "params": [[{"txid":"<txid>","vout":0}],{"<address>":12.5}]}'
```

### generate

Mines a given number of blocks (regtest/dev mode only).

```bash
curl --user bitcoin:bitcoin -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "generate", "params": [101]}'
```

### submitminingsolution

Submits a mining solution.

```bash
curl --user bitcoin:bitcoin -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "submitminingsolution", "params": ["{\"id\": \"<id>\",\"nonce\": 1804358173, \"coinbase\": \"<coinbase>\",\"time\": 1528925410,\"version\": 536870912}"]}'
```

### setban

Adds or removes a ban on an IP address or subnet.

```bash
curl --user bitcoin:bitcoin -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "setban", "params": ["172.108.0.0/24", "add", 86400, false]}'
```

### getpeerinfo

Returns information about connected peers.

```bash
curl --user bitcoin:bitcoin -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "getpeerinfo", "params": []}'
```

## Multi-Node Environments

In multi-node test setups, each node's RPC port is prefixed with the node index (e.g., node 1 uses `19292`, node 2 uses `29292`). These are not the default port — they are environment-specific overrides used in docker-compose test configurations.

## Related Configuration

- `rpc_listener_url`: Full URL the RPC server listens on (e.g., `localhost:9292`)
- `rpc_maxRequestSize`: Maximum request body size (default: 10MB)

For full configuration options, see the [Settings Reference](../../docs/references/settings_reference.md).
