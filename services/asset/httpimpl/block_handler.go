package httpimpl

import (
	"fmt"
	"net/http"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

// BlockHandler handles block-related operations API endpoints
type BlockHandler struct {
	blockchainClient blockchain.ClientI
	logger           ulogger.Logger
}

// blockOperation represents a function that performs an operation on a block
type blockOperation func(ctx echo.Context, blockHash *chainhash.Hash) error

// blockRequest represents the common request structure for block operations
type blockRequest struct {
	BlockHash string `json:"blockHash"`
}

// NewBlockHandler creates a new block operations handler
func NewBlockHandler(blockchainClient blockchain.ClientI, logger ulogger.Logger) *BlockHandler {
	return &BlockHandler{
		blockchainClient: blockchainClient,
		logger:           logger,
	}
}

// handleBlockOperation is a helper function that handles common block operation logic
func (h *BlockHandler) handleBlockOperation(c echo.Context, operationName string, operation blockOperation) error {
	// Parse the block hash from the request body
	var request blockRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request body: "+err.Error())
	}

	if request.BlockHash == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "BlockHash is required")
	}

	// Convert the string hash to chainhash.Hash
	blockHash, err := chainhash.NewHashFromStr(request.BlockHash)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid block hash format: "+err.Error())
	}

	ctx := c.Request().Context()

	h.logger.Infof("%s block: %s", operationName, blockHash)

	// First check if the block exists
	exists, err := h.blockchainClient.GetBlockExists(ctx, blockHash)
	if err != nil {
		h.logger.Errorf("Error checking if block %s exists: %v", blockHash, err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Error checking if block exists: "+err.Error())
	}

	if !exists {
		h.logger.Warnf("Block not found for %s: %s", operationName, blockHash)
		return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("Block with hash %s not found", blockHash))
	}

	// Call the blockchain service to perform the operation
	if err := operation(c, blockHash); err != nil {
		h.logger.Errorf("Failed to %s block %s: %v", operationName, blockHash, err)
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to %s block: %s", operationName, err.Error()))
	}

	h.logger.Infof("Block %s successfully: %s", operationName, blockHash)

	// Return success response
	return c.JSON(http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Block %s successfully", operationName),
	})
}

// InvalidateBlock handles HTTP requests to invalidate a block
func (h *BlockHandler) InvalidateBlock(c echo.Context) error {
	return h.handleBlockOperation(c, "invalidate", func(ctx echo.Context, blockHash *chainhash.Hash) error {
		return h.blockchainClient.InvalidateBlock(ctx.Request().Context(), blockHash)
	})
}

// RevalidateBlock handles HTTP requests to revalidate a previously invalidated block
func (h *BlockHandler) RevalidateBlock(c echo.Context) error {
	return h.handleBlockOperation(c, "revalidate", func(ctx echo.Context, blockHash *chainhash.Hash) error {
		return h.blockchainClient.RevalidateBlock(ctx.Request().Context(), blockHash)
	})
}

// GetLastNInvalidBlocks handles HTTP requests to retrieve the last N invalid blocks
func (h *BlockHandler) GetLastNInvalidBlocks(c echo.Context) error {
	// Parse the count parameter from the query string
	countStr := c.QueryParam("count")
	if countStr == "" {
		countStr = "10" // Default to 10 if not specified
	}

	// Convert count to int64
	var count int64

	_, err := fmt.Sscanf(countStr, "%d", &count)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid count parameter: "+err.Error())
	}

	// Validate count is positive
	if count <= 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "Count must be a positive number")
	}

	ctx := c.Request().Context()

	h.logger.Infof("Retrieving last %d invalid blocks", count)

	// Call the blockchain service to get the invalid blocks
	blocks, err := h.blockchainClient.GetLastNInvalidBlocks(ctx, count)
	if err != nil {
		h.logger.Errorf("Failed to retrieve invalid blocks: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve invalid blocks: "+err.Error())
	}

	h.logger.Infof("Successfully retrieved %d invalid blocks", len(blocks))

	// Return the blocks
	return c.JSON(http.StatusOK, map[string]interface{}{
		"success": true,
		"count":   len(blocks),
		"blocks":  blocks,
	})
}
