package http_impl

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type res struct {
	Type string `json:"type"`
	Hash string `json:"hash"`
}

func (h *HTTP) Search(c echo.Context) error {
	start := gocore.CurrentTime()
	stat := AssetStat.NewStat("Search")
	defer func() {
		stat.AddTime(start)
	}()

	q := c.QueryParam("q")

	if q == "" {
		return sendError(c, http.StatusBadRequest, 1, errors.New("missing query parameter"))
	}

	if len(q) == 64 {
		// This is a hash
		hash, err := chainhash.NewHashFromStr(q)
		if err != nil {
			return sendError(c, http.StatusBadRequest, 2, fmt.Errorf("error reading hash: %w", err))
		}

		// Check if the hash is a block...
		header, _, err := h.repository.GetBlockHeader(c.Request().Context(), hash)
		if err != nil && !errors.Is(err, ubsverrors.ErrNotFound) { // We return an error except if it's a not found error
			return sendError(c, http.StatusBadRequest, 3, fmt.Errorf("error searching for block: %w", err))
		}

		if header != nil {
			// It's a block
			return c.JSONPretty(200, &res{"block", hash.String()}, "  ")
		}

		// Check if it's a subtree
		subtree, err := h.repository.GetSubtreeBytes(c.Request().Context(), hash)
		if err != nil && !errors.Is(err, ubsverrors.ErrNotFound) {
			return sendError(c, http.StatusBadRequest, 4, fmt.Errorf("error searching for subtree: %w", err))
		}

		if subtree != nil {
			// It's a subtree
			return c.JSONPretty(200, &res{"subtree", hash.String()}, "  ")
		}

		// Check if it's a transaction
		tx, err := h.repository.GetTransactionMeta(c.Request().Context(), hash)
		if err != nil && !errors.Is(err, txmeta.NewErrTxmetaNotFound(hash)) {
			return sendError(c, http.StatusBadRequest, 5, fmt.Errorf("error searching for tx: %w", err))
		}

		if tx != nil {
			// It's a transaction
			return c.JSONPretty(200, &res{"tx", hash.String()}, "  ")
		}

		// Check if it's a utxo
		u, err := h.repository.GetUtxo(c.Request().Context(), hash)
		if err != nil && !errors.Is(err, utxo.ErrNotFound) {
			return sendError(c, http.StatusBadRequest, 6, fmt.Errorf("error searching for utxo: %w", err))
		}

		if u != nil {
			// It's a utxo
			return c.JSONPretty(http.StatusOK, &res{"utxo", hash.String()}, "  ")
		}

		return c.String(http.StatusNotFound, "not found")
	}

	// TODO: Check if it's a block height (number)

	return sendError(c, http.StatusBadRequest, 7, errors.New("query must be a valid hash"))
}
