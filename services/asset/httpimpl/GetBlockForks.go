// Package httpimpl provides HTTP handlers for blockchain data retrieval,
// including fork visualization and analysis.
package httpimpl

import (
	"net/http"
	"strconv"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

/*
{
  "tree" : {
    "nodeName" : "NODE NAME 1",
    "name" : "NODE NAME 1",
    "type" : "type3",
    "code" : "N1",
    "label" : "Node name 1",
    "version" : "v1.0",
    "link" : {
      "name" : "Link NODE NAME 1",
      "nodeName" : "NODE NAME 1",
      "direction" : "ASYN"
    },
    "children" : [{
...
*/

// forks represents the root structure for the block fork tree response.
// It encapsulates the entire fork hierarchy starting from a given block.
type forks struct {
	Tree forksTree `json:"tree"` // Root of the fork tree
}

// forksTree represents a node in the block fork tree structure.
// It contains block information and maintains parent-child relationships.
type forksTree struct {
	ID        uint32      `json:"id"`         // Unique identifier for the block
	Hash      string      `json:"hash"`       // Block hash in string format
	Miner     string      `json:"miner"`      // Miner information if available
	Height    uint32      `json:"height"`     // Block height in the chain
	TxCount   uint64      `json:"tx_count"`   // Number of transactions in the block
	Size      uint64      `json:"size"`       // Block size in bytes
	BlockTime uint32      `json:"block_time"` // Block timestamp
	Timestamp uint32      `json:"timestamp"`  // Block creation timestamp
	Link      forksLink   `json:"link"`       // Link information for visualization
	Children  []forksTree `json:"children"`   // Child blocks in the fork tree

	hash chainhash.Hash `json:"-"` // Internal hash representation (not serialized)
}

// forksLink represents connection information between blocks in the fork tree.
// Used for visual representation of block relationships.
type forksLink struct {
	Name      string `json:"name"`      // Link identifier
	NodeName  string `json:"nodeName"`  // Name of the connected node
	Direction string `json:"direction"` // Direction of the connection (SYNC/ASYN)
}

// GetBlockForks retrieves a tree structure showing the blockchain's fork history
// starting from a specified block. It maps parent-child relationships between blocks
// and supports pagination.
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// URL Parameters:
//   - hash: Starting block hash (hex string)
//
// Query Parameters:
//   - limit: Maximum number of blocks to include in the tree (default: 20, max: 100)
//     Example: ?limit=50
//
// Returns:
//   - error: Any error encountered during processing
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/json
//	Body: Tree structure of blocks:
//	  {
//	    "tree": {
//	      "id": <uint32>,              // Internal block ID
//	      "hash": "<string>",          // Block hash
//	      "miner": "<string>",         // Block miner information
//	      "height": <uint32>,          // Block height
//	      "tx_count": <uint32>,        // Number of transactions
//	      "size": <uint32>,            // Block size in bytes
//	      "block_time": <uint32>,      // Block timestamp
//	      "timestamp": <uint32>,       // Block creation timestamp
//	      "link": {
//	        "name": "Link <block_hash>",
//	        "nodeName": "<block_hash>",
//	        "direction": "SYNC"|"ASYN"  // Connection type between blocks
//	      },
//	      "children": [
//	        // Recursive array of blocks with same structure
//	      ]
//	    }
//	  }
//
// Error Responses:
//
//   - 400 Bad Request:
//
//   - Invalid limit parameter
//     Example: {"message": "strconv.Atoi: parsing \"invalid\": invalid syntax"}
//
//   - 500 Internal Server Error:
//
//   - Invalid block hash format
//
//   - Block header retrieval failure
//     Example: {"message": "internal server error"}
//
// Monitoring:
//   - Execution time recorded in "GetBlockForks_http" statistic
//
// Example Usage:
//
//	# Get fork tree starting from a block with default limit
//	GET /block/forks/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
//
//	# Get fork tree with custom limit
//	GET /block/forks/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f?limit=50
//
// Notes:
//   - The tree structure shows fork relationships between blocks
//   - The root block is the one specified by the hash parameter
//   - Each block can have multiple children (forks)
//   - Link direction indicates the relationship type between blocks:
//   - SYNC: Synchronous connection (main chain)
//   - ASYN: Asynchronous connection (fork point)
func (h *HTTP) GetBlockForks(c echo.Context) (err error) {
	hashStr := c.Param("hash")

	ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetBlockForks_http",
		tracing.WithParentStat(AssetStat),
		tracing.WithDebugLogMessage(h.logger, "[Asset] GetBlockForks_http for %s", hashStr),
	)

	defer deferFn()

	limit := 20
	limitStr := c.QueryParam("limit")

	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid limit parameter").Error())
		}
	}

	if limit > 100 {
		limit = 100
	}

	if len(hashStr) != 64 {
		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash length").Error())
	}

	blockHash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash format", err).Error())
	}

	_, meta, err := h.repository.GetBlockHeader(ctx, blockHash)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	limitUint32, err := safeconversion.IntToUint32(limit)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid limit parameter", err).Error())
	}

	blockHeaders, metas, err := h.repository.GetBlockHeadersFromHeight(ctx, meta.Height, limitUint32)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	// reorganize the block headers into a map of parent / child relationships
	blockHeadersParentChild := make(map[chainhash.Hash][]*model.BlockHeader)
	blockHeadersMap := make(map[chainhash.Hash]*model.BlockHeader)
	metasMap := make(map[chainhash.Hash]*model.BlockHeaderMeta)

	for idx, blockHeader := range blockHeaders {
		blockHeadersParentChild[*blockHeader.HashPrevBlock] = append(blockHeadersParentChild[*blockHeader.HashPrevBlock], blockHeader)
		blockHeadersMap[*blockHeader.Hash()] = blockHeader
		metasMap[*blockHeader.Hash()] = metas[idx]
	}

	// add the root block to the forks
	blockForks := forks{
		Tree: forksTree{
			Hash:      blockHash.String(),
			ID:        meta.ID,
			Miner:     meta.Miner,
			Height:    meta.Height,
			TxCount:   meta.TxCount,
			Size:      meta.SizeInBytes,
			BlockTime: meta.BlockTime,
			Timestamp: meta.Timestamp,
			Link: forksLink{
				Name:      "Link " + blockHash.String(),
				NodeName:  blockHash.String(),
				Direction: "ASYN",
			},
			hash: *blockHash,
		},
	}

	// recursively add the children to the forks
	addChildrenToBlockForks(&blockForks.Tree, blockHeadersParentChild, blockHeadersMap, metasMap)

	return c.JSONPretty(200, blockForks, "  ")
}

// addChildrenToBlockForks recursively builds the fork tree structure by adding child blocks
// to their respective parents. It processes the block relationships and creates the
// hierarchical structure required for the API response.
//
// Parameters:
//   - tree: Current node in the fork tree being processed
//   - blockHeadersParentChild: Map of parent hash to child block headers
//   - blockHeadersMap: Map of block hash to block header for quick lookups
//   - metasMap: Map of block hash to block metadata
//
// The function builds the tree by:
//  1. Finding all children of the current block
//  2. Creating a new tree node for each child
//  3. Recursively processing each child's children
//  4. Adding the complete child subtree to the parent's children array
//
// Internal Structure:
//   - Parent-child relationships are maintained through the blockHeadersParentChild map
//   - Block data is accessed through the blockHeadersMap and metasMap
//   - Each node includes block details and link information for visualization
func addChildrenToBlockForks(tree *forksTree, blockHeadersParentChild map[chainhash.Hash][]*model.BlockHeader,
	blockHeadersMap map[chainhash.Hash]*model.BlockHeader, metasMap map[chainhash.Hash]*model.BlockHeaderMeta) {
	children := blockHeadersParentChild[tree.hash]
	for _, child := range children {
		childTree := forksTree{
			Hash:      child.Hash().String(),
			ID:        metasMap[*child.Hash()].ID,
			Miner:     metasMap[*child.Hash()].Miner,
			Height:    metasMap[*child.Hash()].Height,
			TxCount:   metasMap[*child.Hash()].TxCount,
			Size:      metasMap[*child.Hash()].SizeInBytes,
			BlockTime: metasMap[*child.Hash()].BlockTime,
			Timestamp: metasMap[*child.Hash()].Timestamp,
			Link: forksLink{
				Name:      "Link " + child.Hash().String(),
				NodeName:  child.Hash().String(),
				Direction: "SYNC",
			},
			hash: *child.Hash(),
		}

		addChildrenToBlockForks(&childTree, blockHeadersParentChild, blockHeadersMap, metasMap)

		tree.Children = append(tree.Children, childTree)
	}
}
