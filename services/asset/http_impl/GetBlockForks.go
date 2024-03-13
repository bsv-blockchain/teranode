package http_impl

import (
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"net/http"
	"strconv"
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

type forks struct {
	Tree forksTree `json:"tree"`
}
type forksTree struct {
	ID        uint32      `json:"id"`
	Hash      string      `json:"hash"`
	Miner     string      `json:"miner"`
	Height    uint32      `json:"height"`
	TxCount   uint32      `json:"tx_count"`
	Size      uint32      `json:"size"`
	BlockTime uint32      `json:"block_time"`
	Timestamp uint32      `json:"timestamp"`
	Link      forksLink   `json:"link"`
	Children  []forksTree `json:"children"`

	hash chainhash.Hash `json:"-"`
}
type forksLink struct {
	Name      string `json:"name"`
	NodeName  string `json:"nodeName"`
	Direction string `json:"direction"`
}

func (h *HTTP) GetBlockForks(c echo.Context) (err error) {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBlockForks_http").AddTime(start)
	}()

	limit := 20
	limitStr := c.QueryParam("limit")
	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	}
	if limit > 100 {
		limit = 100
	}

	blockHash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	_, meta, err := h.repository.GetBlockHeader(c.Request().Context(), blockHash)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	blockHeaders, metas, err := h.repository.GetBlockHeadersFromHeight(c.Request().Context(), meta.Height, uint32(limit))
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
			ID:        metasMap[*blockHash].ID,
			Miner:     metasMap[*blockHash].Miner,
			Height:    metasMap[*blockHash].Height,
			TxCount:   uint32(metasMap[*blockHash].TxCount),
			Size:      uint32(metasMap[*blockHash].SizeInBytes),
			BlockTime: metasMap[*blockHash].BlockTime,
			Timestamp: metasMap[*blockHash].Timestamp,
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

func addChildrenToBlockForks(tree *forksTree, blockHeadersParentChild map[chainhash.Hash][]*model.BlockHeader,
	blockHeadersMap map[chainhash.Hash]*model.BlockHeader, metasMap map[chainhash.Hash]*model.BlockHeaderMeta) {

	children := blockHeadersParentChild[tree.hash]
	for _, child := range children {
		childTree := forksTree{
			Hash:      child.Hash().String(),
			ID:        metasMap[*child.Hash()].ID,
			Miner:     metasMap[*child.Hash()].Miner,
			Height:    metasMap[*child.Hash()].Height,
			TxCount:   uint32(metasMap[*child.Hash()].TxCount),
			Size:      uint32(metasMap[*child.Hash()].SizeInBytes),
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
