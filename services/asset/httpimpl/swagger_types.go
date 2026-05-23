package httpimpl

import "github.com/bsv-blockchain/teranode/model"

// --- Swagger parameter types ---

// swagger:parameters getBlockByHash getBlockByHashHex getBlockByHashJSON getBlockHeader getBlockHeaderHex getBlockHeaderJSON getBlockHeaders getBlockHeadersHex getBlockHeadersJSON getBlockHeadersToCommonAncestor getBlockHeadersToCommonAncestorHex getBlockHeadersToCommonAncestorJSON getBlockHeadersFromCommonAncestor getBlockHeadersFromCommonAncestorHex getBlockHeadersFromCommonAncestorJSON getNBlocks getNBlocksHex getNBlocksJSON getLegacyBlock getRestLegacyBlock getTransaction getTransactionHex getTransactionJSON getTransactionMeta getSubtree getSubtreeHex getSubtreeJSON getSubtreeData getSubtreeTxs getBlockSubtrees getBlockForks getNearestForkHeights getTxMetaByTxID getTxMetaByTxIDHex getTxMetaByTxIDJSON getMerkleProof getMerkleProofHex getMerkleProofJSON
type hashPathParam struct {
	// Block, transaction, or subtree hash in hexadecimal format
	// in: path
	// required: true
	Hash string `json:"hash"`
}

// swagger:parameters getUTXO getUTXOHex getUTXOJSON
type utxoParams struct {
	// Transaction hash in hexadecimal format
	// in: path
	// required: true
	Hash string `json:"hash"`
	// Output index
	// in: query
	// required: true
	Vout int `json:"vout"`
}

// swagger:parameters getUTXOsByTxID
type utxosByTxIDParams struct {
	// Transaction hash in hexadecimal format
	// in: path
	// required: true
	Hash string `json:"hash"`
}

// swagger:parameters search
type searchParams struct {
	// Search query: 64-character hex hash or numeric block height
	// in: query
	// required: true
	Q string `json:"q"`
}

// swagger:parameters getBlocks getLastNBlocks getSubtreeTxs
type paginationParams struct {
	// Number of items to skip
	// in: query
	Offset int `json:"offset"`
	// Maximum number of items to return (max 100)
	// in: query
	Limit int `json:"limit"`
}

// swagger:parameters getBlockLocator
type blockLocatorParams struct {
	// Starting block hash
	// in: query
	Hash string `json:"hash"`
	// Starting block height
	// in: query
	Height int `json:"height"`
}

// swagger:parameters getBlockGraphData
type blockGraphDataParams struct {
	// Time period: hour, day, week, month
	// in: path
	// required: true
	Period string `json:"period"`
}

// swagger:parameters getTransactions
type getTransactionsParams struct {
	// Subtree hash
	// in: path
	// required: true
	Hash string `json:"hash"`
}

// swagger:parameters invalidateBlock revalidateBlock
type blockRequestParams struct {
	// in: body
	// required: true
	Body struct {
		// Block hash in hexadecimal format
		BlockHash string `json:"blockHash"`
	}
}

// swagger:parameters sendFSMEvent
type fsmEventParams struct {
	// in: body
	// required: true
	Body struct {
		// FSM event name
		Event string `json:"event"`
	}
}

// swagger:parameters resetReputation
type resetReputationParams struct {
	// in: body
	Body ResetReputationRequest
}

// swagger:parameters submitTransaction submitTransactions
type submitTxParams struct {
	// in: body
	// required: true
	Body struct{}
}

// --- Swagger response types ---

// Error response
// swagger:response errorResponse
type swaggerErrorResponse struct {
	// in: body
	Body errorResponse
}

// Block header in JSON format
// swagger:response blockHeaderResponse
type swaggerBlockHeaderResponse struct {
	// in: body
	Body blockHeaderResponse
}

// Block with next block hash
// swagger:response blockExtendedResponse
type swaggerBlockExtendedResponse struct {
	// in: body
	Body BlockExtended
}

// Policy response (ARC-compatible)
// swagger:response policyResponse
type swaggerPolicyResponse struct {
	// in: body
	Body PolicyResponse
}

// Chain parameters
// swagger:response chainParamsResponse
type swaggerChainParamsResponse struct {
	// in: body
	Body ChainParamsResponse
}

// Search result
// swagger:response searchResponse
type swaggerSearchResponse struct {
	// in: body
	Body res
}

// Paginated response with data and pagination metadata
// swagger:response extendedResponse
type swaggerExtendedResponse struct {
	// in: body
	Body ExtendedResponse
}

// Block statistics
// swagger:response blockStatsResponse
type swaggerBlockStatsResponse struct {
	// in: body
	Body model.BlockStats
}

// Block graph data points
// swagger:response blockGraphDataResponse
type swaggerBlockGraphDataResponse struct {
	// in: body
	Body model.BlockDataPoints
}

// Peers list
// swagger:response peersResponse
type swaggerPeersResponse struct {
	// in: body
	Body PeersResponse
}

// Reputation reset result
// swagger:response resetReputationResponse
type swaggerResetReputationResponse struct {
	// in: body
	Body ResetReputationResponse
}

// Block locator hashes
// swagger:response blockLocatorResponse
type swaggerBlockLocatorResponse struct {
	// in: body
	Body []string
}

// Block header list in JSON format
// swagger:response blockHeaderListResponse
type swaggerBlockHeaderListResponse struct {
	// in: body
	Body []blockHeaderResponse
}

// Last N blocks response
// swagger:response lastNBlocksResponse
type swaggerLastNBlocksResponse struct {
	// in: body
	Body []model.BlockInfo
}

// Invalid blocks list
// swagger:response invalidBlocksResponse
type swaggerInvalidBlocksResponse struct {
	// in: body
	Body map[string]interface{}
}

// FSM state
// swagger:response fsmStateResponse
type swaggerFSMStateResponse struct {
	// in: body
	Body map[string]interface{}
}

// Settings response
// swagger:response settingsResponse
type swaggerSettingsResponse struct {
	// in: body
	Body SettingsResponse
}

// Settings categories
// swagger:response settingsCategoriesResponse
type swaggerSettingsCategoriesResponse struct {
	// in: body
	Body []string
}

// Catchup status
// swagger:response catchupStatusResponse
type swaggerCatchupStatusResponse struct {
	// in: body
	Body map[string]interface{}
}

// Service heights
// swagger:response serviceHeightsResponse
type swaggerServiceHeightsResponse struct {
	// in: body
	Body map[string]interface{}
}

// UTXO list by transaction
// swagger:response utxosByTxIDResponse
type swaggerUTXOsByTxIDResponse struct {
	// in: body
	Body []UTXOItem
}

// Subtree metadata list
// swagger:response blockSubtreesResponse
type swaggerBlockSubtreesResponse struct {
	// in: body
	Body []SubtreeMeta
}

// Block forks tree
// swagger:response blockForksResponse
type swaggerBlockForksResponse struct {
	// in: body
	Body forks
}

// Nearest fork heights
// swagger:response nearestForksResponse
type swaggerNearestForksResponse struct {
	// in: body
	Body nearestForksResponse
}

// Binary data response
// swagger:response binaryResponse
type swaggerBinaryResponse struct {
	// in: body
	Body []byte
}

// Hex string response
// swagger:response hexResponse
type swaggerHexResponse struct {
	// in: body
	Body string
}

// Block operation success
// swagger:response blockOperationResponse
type swaggerBlockOperationResponse struct {
	// in: body
	Body map[string]interface{}
}

// Health check
// swagger:response healthResponse
type swaggerHealthResponse struct {
	// in: body
	Body string
}

// Alive check
// swagger:response aliveResponse
type swaggerAliveResponse struct {
	// in: body
	Body string
}

// Transaction meta enriched
// swagger:response transactionMetaResponse
type swaggerTransactionMetaResponse struct {
	// in: body
	Body map[string]interface{}
}

// Merkle proof response
// swagger:response merkleProofResponse
type swaggerMerkleProofResponse struct {
	// in: body
	Body interface{}
}
