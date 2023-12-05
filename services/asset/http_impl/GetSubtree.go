package http_impl

import (
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetSubtree(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		stat := AssetStat.NewStat("GetSubtree_http")
		defer func() {
			stat.AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetSubtree in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		start2 := gocore.CurrentTime()
		subtree, err := h.repository.GetSubtree(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}
		stat.NewStat("Get Subtree from repository").AddTime(start2)

		prometheusAssetHttpGetSubtree.WithLabelValues("OK", "200").Inc()

		// At this point, the subtree contains all the fees and sizes for the transactions in the subtree.

		if mode == JSON {
			return c.JSONPretty(200, subtree, "  ")
		}

		// If we did not serve JSON, we need to serialize the nodes into a byte slice.  We use SerializeNodes() for this which does NOT include the fees and sizes.

		b, err := subtree.SerializeNodes()
		if err != nil {
			return err
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)

		case HEX:
			return c.String(200, hex.EncodeToString(b))

		default:
			err = errors.New("bad read mode")
			return sendError(c, http.StatusInternalServerError, 52, err)
		}
	}
}

type SubtreeNodesReader struct {
	data      []byte
	pos       int
	itemCount int
	itemsRead int
}

func NewSubtreeNodesReader(subtree []byte) *SubtreeNodesReader {
	// Calculate the position of the first node
	position := 32 // root hash

	f, n := bt.NewVarIntFromBytes(subtree[position:]) // fees
	_ = f
	position += n

	s, n := bt.NewVarIntFromBytes(subtree[position:]) // sizeInBytes
	_ = s
	position += n

	itemCount, n := bt.NewVarIntFromBytes(subtree[position:]) // numberOfLeaves
	position += n

	return &SubtreeNodesReader{
		data:      subtree,
		pos:       position,
		itemCount: int(itemCount),
	}
}

func (r *SubtreeNodesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF // No more data
	}

	if r.itemsRead >= r.itemCount {
		return 0, io.EOF // No more data
	}

	if len(p) < 32 {
		return 0, errors.New("buffer too small")
	}

	// Copy data to p
	n := copy(p, r.data[r.pos:r.pos+32])
	r.pos += 48 // Skip 32 bytes of data and 16 bytes of padding

	r.itemsRead++

	return n, nil
}

func (h *HTTP) GetSubtreeAsReader(c echo.Context) error {
	start := gocore.CurrentTime()
	stat := AssetStat.NewStat("GetSubtree_http")
	defer func() {
		stat.AddTime(start)
	}()

	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return err
	}

	start2 := gocore.CurrentTime()
	subtree, err := h.repository.GetSubtreeBytes(c.Request().Context(), hash)
	if err != nil {
		if strings.HasSuffix(err.Error(), " not found") {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
	}
	stat.NewStat("Get Subtree from repository").AddTime(start2)

	prometheusAssetHttpGetSubtree.WithLabelValues("OK", "200").Inc()

	r := NewSubtreeNodesReader(subtree)

	return c.Stream(200, echo.MIMEOctetStream, r)
}

func (h *HTTP) GetSubtreeStream() func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetSubtreeStream_http").AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetSubtreeStream for %s: %s", c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		subtreeBytes, err := h.repository.GetSubtreeBytes(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		subtree := &util.Subtree{}
		nodeChan, errChan, err := subtree.DeserializeChan(subtreeBytes)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		// Set response type
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEOctetStream)
		c.Response().WriteHeader(http.StatusOK)

		for {
			select {
			case node, ok := <-nodeChan:
				if !ok {
					return nil // Channel closed, end of stream
				}
				if _, err := c.Response().Write(node.Hash[:]); err != nil {
					return echo.NewHTTPError(http.StatusInternalServerError, "Failed to write to response")
				}
				c.Response().Flush()

			case err := <-errChan:
				if err != nil {
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}
		}
	}
}
