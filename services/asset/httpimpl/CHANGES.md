# Asset Service: Auto-generated OpenAPI (Swagger 2.0) Specification

## Summary

Adds go-swagger annotations to the Asset service HTTP handlers and generates a Swagger 2.0 specification covering all 71 REST API operations. This adds a new auto-generated spec derived directly from the source in `services/asset/httpimpl/swagger.json`, while `ui/dashboard/docs/api/openapi.yaml` remains in the repository.

## What changed

### New files

| File | Purpose |
|------|---------|
| `services/asset/httpimpl/doc.go` | `swagger:meta` entry point — defines API title, version, base path (`/api/v1`), and supported content types |
| `services/asset/httpimpl/swagger_routes.go` | All `swagger:route` annotations (71 operations) organized by resource type — documents every endpoint's HTTP method, path, tags, description, and response types |
| `services/asset/httpimpl/swagger_types.go` | Swagger parameter (`swagger:parameters`) and response (`swagger:response`) wrapper types that connect route annotations to Go structs |
| `services/asset/httpimpl/swagger.json` | Generated Swagger 2.0 specification — 70 paths, 71 operations, 64 model definitions, validated |

### Modified files

| File | Change |
|------|--------|
| `services/asset/httpimpl/http.go` | Added `go:generate` directive for spec regeneration |
| `services/asset/httpimpl/sendError.go` | `swagger:model` on `errorResponse` |
| `services/asset/httpimpl/helpers.go` | `swagger:model` on `Pagination`, `ExtendedResponse` |
| `services/asset/httpimpl/block_header_response.go` | `swagger:model` on `blockHeaderResponse` |
| `services/asset/httpimpl/get_policy.go` | `swagger:model` on `FeeAmount`, `Policy`, `PolicyResponse` |
| `services/asset/httpimpl/GetChainParams.go` | `swagger:model` on `ChainParamsResponse` |
| `services/asset/httpimpl/GetBlock.go` | `swagger:model` on `BlockExtended` |
| `services/asset/httpimpl/GetBlockForks.go` | `swagger:model` on `forks` |
| `services/asset/httpimpl/GetNearestForkHeights.go` | `swagger:model` on `nearestForkInfo`, `nearestForksResponse` |
| `services/asset/httpimpl/GetBlockSubtrees.go` | `swagger:model` on `SubtreeMeta` |
| `services/asset/httpimpl/GetSubtreeTxs.go` | `swagger:model` on `SubtreeTx` |
| `services/asset/httpimpl/GetUTXOsByTXID.go` | `swagger:model` on `UTXOItem` |
| `services/asset/httpimpl/GetMerkleProof.go` | `swagger:model` on `LegacyMerkleProofResponse` |
| `services/asset/httpimpl/Search.go` | `swagger:model` on `res` (search result) |
| `services/asset/httpimpl/get_peers.go` | `swagger:model` on `PeerInfoResponse`, `PeersResponse` |
| `services/asset/httpimpl/reset_reputation.go` | `swagger:model` on `ResetReputationRequest`, `ResetReputationResponse` |
| `services/asset/httpimpl/settings_handler.go` | `swagger:model` on `SettingsResponse` |
| `services/asset/httpimpl/GetTxMetaByTXID.go` | `swagger:model` on `aerospikeRecord` |
| `Makefile` | Added `swagger-asset` and `swagger-validate` targets |

## API coverage

The generated spec documents **all** Asset service endpoints:

- **Transactions:** GET tx (binary/hex/json), POST tx/txs (propagation proxy)
- **Transaction metadata:** GET txmeta, txmeta_raw (binary/hex/json)
- **Blocks:** GET block by hash (binary/hex/json), blocks list, N blocks, legacy block, last N blocks, block stats, block graph data, block locator
- **Block headers:** GET header (binary/hex/json), headers list, headers to/from common ancestor, best block header
- **Subtrees:** GET subtree (binary/hex/json), subtree data, subtree txs, block subtrees
- **UTXOs:** GET utxo by hash+vout (binary/hex/json), utxos by txid
- **Merkle proofs:** GET merkle proof in BUMP format (binary/hex/json)
- **Search:** GET search by hash or block height
- **Chain info:** GET chain params, GET policy (ARC-compatible)
- **Block forks:** GET forks tree, nearest fork heights
- **Admin:** POST invalidate/revalidate block, GET invalid blocks
- **FSM:** GET state/events/states, POST send event
- **Peers:** GET peers, POST reset reputation
- **Status:** GET catchup status, service heights, settings, health, alive

## How to regenerate

After modifying handlers or adding new endpoints:

```bash
# Regenerate the spec
make swagger-asset

# Validate the spec
make swagger-validate

# Or via go generate
cd services/asset/httpimpl && go generate
```

Requires `swagger` CLI: `go install github.com/go-swagger/go-swagger/cmd/swagger@latest`

## Test plan

- [x] `swagger generate spec` completes without errors
- [x] `swagger validate` reports spec is valid against Swagger 2.0
- [ ] Verify spec renders correctly in Swagger UI or Redoc
- [ ] Confirm no existing tests are broken by the annotation comments
