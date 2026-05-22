package httpimpl

// This file contains all swagger:route annotations for the Asset service API.
// go-swagger scans these comments to generate the OpenAPI spec.
// Routes are organized by resource type.

// ========== Health ==========

// swagger:route GET /alive health alive
// Check if the Asset service is running.
// Returns uptime information.
// responses:
//   200: aliveResponse

// swagger:route GET /health health healthCheck
// Check the health of the Asset service and its dependencies.
// responses:
//   200: healthResponse
//   500: healthResponse

// ========== Transactions ==========

// swagger:route GET /api/v1/tx/{hash} transaction getTransaction
// Get a transaction by hash in binary format.
// Returns the raw transaction bytes.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/tx/{hash}/hex transaction getTransactionHex
// Get a transaction by hash in hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/tx/{hash}/json transaction getTransactionJSON
// Get a transaction by hash in JSON format.
// responses:
//   200:
//   400: errorResponse
//   404: errorResponse

// swagger:route POST /api/v1/tx transaction submitTransaction
// Submit a raw transaction for propagation.
// Proxies to the propagation service.
// responses:
//   200:
//   400: errorResponse

// swagger:route POST /api/v1/txs transaction submitTransactions
// Submit multiple raw transactions for propagation.
// Proxies to the propagation service.
// responses:
//   200:
//   400: errorResponse

// ========== Transaction Metadata ==========

// swagger:route GET /api/v1/txmeta/{hash}/json txmeta getTransactionMeta
// Get enriched transaction metadata in JSON format.
// Returns transaction metadata with block and subtree context.
// responses:
//   200: transactionMetaResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/txmeta_raw/{hash} txmeta getTxMetaByTxID
// Get raw transaction metadata in binary format.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/txmeta_raw/{hash}/hex txmeta getTxMetaByTxIDHex
// Get raw transaction metadata in hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/txmeta_raw/{hash}/json txmeta getTxMetaByTxIDJSON
// Get raw transaction metadata in JSON format.
// responses:
//   200:
//   400: errorResponse
//   404: errorResponse

// ========== Blocks ==========

// swagger:route GET /api/v1/block/{hash} block getBlockByHash
// Get a block by hash in binary format.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/block/{hash}/hex block getBlockByHashHex
// Get a block by hash in hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/block/{hash}/json block getBlockByHashJSON
// Get a block by hash in JSON format.
// Returns block data with the next block hash.
// responses:
//   200: blockExtendedResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/blocks block getBlocks
// Get a paginated list of blocks.
// Returns blocks with pagination metadata.
// responses:
//   200: extendedResponse
//   400: errorResponse
//   500: errorResponse

// swagger:route GET /api/v1/blocks/{hash} block getNBlocks
// Get N blocks starting from hash in binary format.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/blocks/{hash}/hex block getNBlocksHex
// Get N blocks starting from hash in hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/blocks/{hash}/json block getNBlocksJSON
// Get N blocks starting from hash in JSON format.
// responses:
//   200: extendedResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/block_legacy/{hash} block getLegacyBlock
// Get a legacy format block by hash in binary format.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /rest/block/{hash}.bin block getRestLegacyBlock
// Get a legacy format block by hash in binary (REST interface).
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/lastblocks block getLastNBlocks
// Get the last N blocks.
// Returns recent blocks ordered by height descending.
// responses:
//   200: lastNBlocksResponse
//   400: errorResponse
//   500: errorResponse

// swagger:route GET /api/v1/blockstats block getBlockStats
// Get aggregate blockchain statistics.
// Returns block count, transaction count, average sizes, and timestamps.
// responses:
//   200: blockStatsResponse
//   500: errorResponse

// swagger:route GET /api/v1/blockgraphdata/{period} block getBlockGraphData
// Get block data points for charting.
// Period can be: hour, day, week, month.
// responses:
//   200: blockGraphDataResponse
//   400: errorResponse
//   500: errorResponse

// swagger:route GET /api/v1/block_locator block getBlockLocator
// Get block locator hashes for chain synchronization.
// responses:
//   200: blockLocatorResponse
//   400: errorResponse
//   500: errorResponse

// ========== Block Headers ==========

// swagger:route GET /api/v1/header/{hash} header getBlockHeader
// Get a single block header by hash in binary format.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/header/{hash}/hex header getBlockHeaderHex
// Get a single block header by hash in hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/header/{hash}/json header getBlockHeaderJSON
// Get a single block header by hash in JSON format.
// responses:
//   200: blockHeaderResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/headers/{hash} header getBlockHeaders
// Get block headers starting from hash in binary format.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/headers/{hash}/hex header getBlockHeadersHex
// Get block headers starting from hash in hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/headers/{hash}/json header getBlockHeadersJSON
// Get block headers starting from hash in JSON format.
// responses:
//   200: blockHeaderListResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/headers_to_common_ancestor/{hash} header getBlockHeadersToCommonAncestor
// Get block headers to the common ancestor in binary format.
// Used for chain synchronization.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/headers_to_common_ancestor/{hash}/hex header getBlockHeadersToCommonAncestorHex
// Get block headers to the common ancestor in hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/headers_to_common_ancestor/{hash}/json header getBlockHeadersToCommonAncestorJSON
// Get block headers to the common ancestor in JSON format.
// responses:
//   200: blockHeaderListResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/headers_from_common_ancestor/{hash} header getBlockHeadersFromCommonAncestor
// Get block headers from the common ancestor in binary format.
// Used for chain synchronization.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/headers_from_common_ancestor/{hash}/hex header getBlockHeadersFromCommonAncestorHex
// Get block headers from the common ancestor in hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/headers_from_common_ancestor/{hash}/json header getBlockHeadersFromCommonAncestorJSON
// Get block headers from the common ancestor in JSON format.
// responses:
//   200: blockHeaderListResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/bestblockheader header getBestBlockHeader
// Get the best (tip) block header in binary format.
// responses:
//   200: binaryResponse
//   500: errorResponse

// swagger:route GET /api/v1/bestblockheader/hex header getBestBlockHeaderHex
// Get the best (tip) block header in hexadecimal format.
// responses:
//   200: hexResponse
//   500: errorResponse

// swagger:route GET /api/v1/bestblockheader/json header getBestBlockHeaderJSON
// Get the best (tip) block header in JSON format.
// responses:
//   200: blockHeaderResponse
//   500: errorResponse

// ========== Subtrees ==========

// swagger:route GET /api/v1/subtree/{hash} subtree getSubtree
// Get a subtree by hash in binary format.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/subtree/{hash}/hex subtree getSubtreeHex
// Get a subtree by hash in hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/subtree/{hash}/json subtree getSubtreeJSON
// Get a subtree by hash in JSON format with pagination.
// responses:
//   200: extendedResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/subtree_data/{hash} subtree getSubtreeData
// Get raw subtree data as a binary stream.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route POST /api/v1/subtree/{hash}/txs subtree getTransactions
// Get transactions from a subtree by posting a list of transaction hashes.
// Returns concatenated raw transaction bytes.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/subtree/{hash}/txs/json subtree getSubtreeTxs
// Get paginated transaction details from a subtree in JSON format.
// responses:
//   200: extendedResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/block/{hash}/subtrees/json subtree getBlockSubtrees
// Get paginated subtree metadata for a block in JSON format.
// responses:
//   200: extendedResponse
//   400: errorResponse
//   404: errorResponse

// ========== UTXOs ==========

// swagger:route GET /api/v1/utxo/{hash} utxo getUTXO
// Get a UTXO by transaction hash and output index in binary format.
// Requires vout query parameter.
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/utxo/{hash}/hex utxo getUTXOHex
// Get a UTXO by transaction hash and output index in hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/utxo/{hash}/json utxo getUTXOJSON
// Get a UTXO by transaction hash and output index in JSON format.
// responses:
//   200:
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/utxos/{hash}/json utxo getUTXOsByTxID
// Get all UTXOs for a transaction in JSON format.
// Returns detailed UTXO information for each output.
// responses:
//   200: utxosByTxIDResponse
//   400: errorResponse
//   404: errorResponse

// ========== Merkle Proofs ==========

// swagger:route GET /api/v1/merkle_proof/{hash} proof getMerkleProof
// Get a merkle proof for a transaction in BUMP binary format (BRC-74).
// responses:
//   200: binaryResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/merkle_proof/{hash}/hex proof getMerkleProofHex
// Get a merkle proof for a transaction in BUMP hexadecimal format.
// responses:
//   200: hexResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/merkle_proof/{hash}/json proof getMerkleProofJSON
// Get a merkle proof for a transaction in BUMP JSON format.
// responses:
//   200: merkleProofResponse
//   400: errorResponse
//   404: errorResponse

// ========== Search ==========

// swagger:route GET /api/v1/search search search
// Search for a blockchain entity by hash or block height.
// Finds blocks, transactions, and subtrees.
// responses:
//   200: searchResponse
//   400: errorResponse
//   404: errorResponse

// ========== Chain Info ==========

// swagger:route GET /api/v1/chainparams chain getChainParams
// Get blockchain network parameters.
// responses:
//   200: chainParamsResponse
//   500: errorResponse

// swagger:route GET /v1/policy policy getPolicy
// Get the node's mining policy in ARC-compatible format.
// responses:
//   200: policyResponse
//   500: errorResponse

// ========== Block Forks ==========

// swagger:route GET /api/v1/block/{hash}/forks block getBlockForks
// Get the fork tree for a block.
// Returns a hierarchical tree of blocks showing forks.
// responses:
//   200: blockForksResponse
//   400: errorResponse
//   404: errorResponse

// swagger:route GET /api/v1/block/{hash}/nearestforks block getNearestForkHeights
// Get the nearest fork heights relative to a block.
// Finds the closest blocks with multiple children in each direction.
// responses:
//   200: nearestForksResponse
//   400: errorResponse
//   404: errorResponse

// ========== Block Admin (requires auth) ==========

// swagger:route POST /api/v1/block/invalidate admin invalidateBlock
// Mark a block as invalid (requires authentication).
// responses:
//   200: blockOperationResponse
//   400: errorResponse
//   401:
//   500: errorResponse

// swagger:route POST /api/v1/block/revalidate admin revalidateBlock
// Revalidate a previously invalidated block (requires authentication).
// responses:
//   200: blockOperationResponse
//   400: errorResponse
//   401:
//   500: errorResponse

// swagger:route GET /api/v1/blocks/invalid admin getLastNInvalidBlocks
// Get a list of recently invalidated blocks.
// responses:
//   200: invalidBlocksResponse
//   500: errorResponse

// ========== FSM (Finite State Machine) ==========

// swagger:route GET /api/v1/fsm/state fsm getFSMState
// Get the current FSM state of the node.
// responses:
//   200: fsmStateResponse
//   500: errorResponse

// swagger:route GET /api/v1/fsm/events fsm getFSMEvents
// Get available FSM events for the current state.
// responses:
//   200: fsmStateResponse
//   500: errorResponse

// swagger:route GET /api/v1/fsm/states fsm getFSMStates
// Get all possible FSM states.
// responses:
//   200: fsmStateResponse
//   500: errorResponse

// swagger:route POST /api/v1/fsm/state fsm sendFSMEvent
// Send an FSM event to transition state (requires authentication).
// responses:
//   200: fsmStateResponse
//   400: errorResponse
//   401:
//   500: errorResponse

// ========== Peers ==========

// swagger:route GET /api/v1/peers peers getPeers
// Get the current peer registry.
// Returns connection status, catchup metrics, and reputation scores.
// responses:
//   200: peersResponse
//   500: errorResponse
//   503: errorResponse

// swagger:route POST /api/p2p/reset-reputation peers resetReputation
// Reset reputation metrics for a peer or all peers (requires authentication).
// responses:
//   200: resetReputationResponse
//   400: errorResponse
//   401:
//   500: errorResponse
//   503: errorResponse

// ========== Catchup & Service Status ==========

// swagger:route GET /api/v1/catchup/status status getCatchupStatus
// Get the current catchup synchronization status.
// responses:
//   200: catchupStatusResponse
//   500: errorResponse

// swagger:route GET /api/v1/service/heights status getServiceHeights
// Get the current block heights reported by each service.
// responses:
//   200: serviceHeightsResponse
//   500: errorResponse

// ========== Settings (requires auth) ==========

// swagger:route GET /api/v1/settings settings getSettings
// Get all node settings with sensitive values redacted (requires authentication).
// responses:
//   200: settingsResponse
//   401:
//   500: errorResponse

// swagger:route GET /api/v1/settings/categories settings getSettingsCategories
// Get the list of settings categories (requires authentication).
// responses:
//   200: settingsCategoriesResponse
//   401:
//   500: errorResponse
