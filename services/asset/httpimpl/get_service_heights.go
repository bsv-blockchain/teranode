package httpimpl

import (
	"context"
	"encoding/binary"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
)

// GetServiceHeights returns the current heights of various services
func (h *HTTP) GetServiceHeights(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Second)
	defer cancel()

	// Get blockchain client from repository
	blockchainClient := h.repository.GetBlockchainClient()
	if blockchainClient == nil {
		h.logger.Errorf("[GetServiceHeights] Blockchain client not available")
		return c.JSON(http.StatusServiceUnavailable, map[string]interface{}{
			"error": "Blockchain service not available",
		})
	}

	response := make(map[string]interface{})

	// Get Block Persister height
	persisterHeightBytes, err := blockchainClient.GetState(ctx, "BlockPersisterHeight")
	if err == nil && len(persisterHeightBytes) >= 4 {
		persisterHeight := binary.LittleEndian.Uint32(persisterHeightBytes)
		response["block_persister_height"] = persisterHeight
	} else {
		response["block_persister_height"] = nil
		if err != nil {
			h.logger.Debugf("[GetServiceHeights] Failed to get BlockPersisterHeight: %v", err)
		}
	}

	// Get Block Assembly height
	assemblyStateBytes, err := blockchainClient.GetState(ctx, "BlockAssembler")
	if err == nil && len(assemblyStateBytes) >= 4 {
		assemblyHeight := binary.LittleEndian.Uint32(assemblyStateBytes)
		response["block_assembly_height"] = assemblyHeight
	} else {
		response["block_assembly_height"] = nil
		if err != nil {
			h.logger.Debugf("[GetServiceHeights] Failed to get BlockAssembler height: %v", err)
		}
	}

	return c.JSON(http.StatusOK, response)
}
