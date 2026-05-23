package httpimpl

import (
	"net/http"
	"strconv"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// swagger:model nearestForkInfo
type nearestForkInfo struct {
	Height     uint32 `json:"height"`
	ParentHash string `json:"parent_hash"`
}

// swagger:model nearestForksResponse
type nearestForksResponse struct {
	CurrentHeight uint32           `json:"current_height"`
	PrevFork      *nearestForkInfo `json:"prev_fork"`
	NextFork      *nearestForkInfo `json:"next_fork"`
}

// GetNearestForkHeights finds the nearest block heights with forks (multiple blocks
// sharing the same parent) relative to the specified block hash.
//
// The forks page displays a tree starting from a given block. A fork is visible
// when the block has multiple children at the next height. "Next fork" skips the
// currently visible fork and finds the next one. "Prev fork" searches backwards.
//
// URL Parameters:
//   - hash: Block hash to search from (hex string)
//
// Query Parameters:
//   - range: Number of heights to scan in each direction (default: 500, max: 5000)
func (h *HTTP) GetNearestForkHeights(c echo.Context) error {
	hashStr := c.Param("hash")

	ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetNearestForkHeights_http",
		tracing.WithParentStat(AssetStat),
		tracing.WithDebugLogMessage(h.logger, "[Asset] GetNearestForkHeights_http for %s", hashStr),
	)

	defer deferFn()

	searchRange := uint32(500)
	rangeStr := c.QueryParam("range")

	if rangeStr != "" {
		r, err := strconv.Atoi(rangeStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid range parameter").Error())
		}

		searchRange = uint32(r)
	}

	if searchRange > 5000 {
		searchRange = 5000
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

	currentHeight := meta.Height

	startHeight := uint32(0)
	if currentHeight > searchRange {
		startHeight = currentHeight - searchRange
	}

	endHeight := currentHeight + searchRange
	totalRange := endHeight - startHeight

	headers, metas, err := h.repository.GetBlockHeadersFromHeight(ctx, startHeight, totalRange)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	// Group blocks at each height by their parent hash.
	// A fork exists at height H when a parent has >1 children there.
	heightParents := make(map[uint32]map[chainhash.Hash]int)

	for i, m := range metas {
		if heightParents[m.Height] == nil {
			heightParents[m.Height] = make(map[chainhash.Hash]int)
		}

		prev := *headers[i].HashPrevBlock
		heightParents[m.Height][prev]++
	}

	// Identify fork heights and record the shared parent hash for navigation.
	forkHeights := make(map[uint32]bool)
	forkParentHash := make(map[uint32]*chainhash.Hash)

	for height, parents := range heightParents {
		for parentHash, count := range parents {
			if count > 1 {
				forkHeights[height] = true
				ph := parentHash
				forkParentHash[height] = &ph

				break
			}
		}
	}

	// Check if the current block's children form a visible fork.
	// If so, "next" should skip that fork and find the one after it.
	visibleFork := forkHeights[currentHeight+1] && forkParentHash[currentHeight+1] != nil && *forkParentHash[currentHeight+1] == *blockHash

	resp := nearestForksResponse{
		CurrentHeight: currentHeight,
	}

	// Prev: search from currentHeight downward.
	// Use height+1 as loop variable to avoid uint32 underflow when height==0.
	for h := currentHeight + 1; h > startHeight; h-- {
		height := h - 1
		if forkHeights[height] {
			resp.PrevFork = &nearestForkInfo{
				Height:     height,
				ParentHash: forkParentHash[height].String(),
			}

			break
		}
	}

	// Next: skip the visible fork at currentHeight+1 if present
	nextStart := currentHeight + 1
	if visibleFork {
		nextStart = currentHeight + 2
	}

	for height := nextStart; height < endHeight; height++ {
		if forkHeights[height] {
			resp.NextFork = &nearestForkInfo{
				Height:     height,
				ParentHash: forkParentHash[height].String(),
			}

			break
		}
	}

	return c.JSON(http.StatusOK, resp)
}
